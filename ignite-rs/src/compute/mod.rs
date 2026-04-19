use crate::api::OpCode;
use crate::cluster::{parse_uuid_parts, ClusterGroup};
use crate::connection_async::NotificationFrame;
use crate::error::{IgniteError, IgniteResult};
use crate::exec::TokioExec;
use crate::protocol::{read_i64, write_i32, write_i64, write_null, write_string_type_code, write_u8, Flag};
use crate::transport::RequestRoute;
use crate::{ReadableReq, ReadableType, WritableType, WriteableReq};
use std::io::{self, Cursor, Read, Write};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::runtime::Handle;
use tokio::sync::{mpsc, Mutex};

pub mod bulk_put;

const FLAG_NO_FAILOVER: u8 = 0x01;
const FLAG_NO_RESULT_CACHE: u8 = 0x02;
/// Keep the compute task argument as a `BinaryObject` on the server — skips
/// the `arg.deserialize()` call in `ClientExecuteTaskRequest.process()`, which
/// would otherwise require the server to have the POJO class on its classpath.
/// Matches Java's `ClientComputeTask.KEEP_BINARY_FLAG_MASK`.
const FLAG_KEEP_BINARY: u8 = 0x04;

#[derive(Clone)]
pub struct Compute {
    exec: TokioExec,
    cluster_group: ClusterGroup,
    flags: u8,
    timeout_ms: i64,
}

pub struct ComputeTask<R> {
    exec: TokioExec,
    address: String,
    task_id: i64,
    receiver: Mutex<Option<mpsc::UnboundedReceiver<IgniteResult<NotificationFrame>>>>,
    cancelled: AtomicBool,
    _marker: PhantomData<R>,
}

impl Compute {
    /// Invoke `PutAllComputeTask` with a typed `BulkPutParams` argument.
    ///
    /// Convenience wrapper around `execute(task_name, arg)` that uses the
    /// canonical Java FQN for the server-side task and the typed response
    /// decoder. Returns `None` only if the server explicitly returns a null
    /// payload — for a successful compute run the response is always
    /// `Some(BulkPutResponseParams)`.
    pub async fn execute_put_all(
        &self,
        params: &bulk_put::BulkPutParams,
    ) -> IgniteResult<Option<bulk_put::BulkPutResponseParams>> {
        // Register the BulkPutParams / PutParams / IndexContext / SaveStrategy
        // type metadata (plus response-side types) with the server before
        // invoking the task. Without this the grid reports "Failed to resolve
        // class name [typeId=...]" when deserializing the compute-task
        // argument, because these POJOs only exist in the compute-task JAR and
        // are never sent to the grid via normal cache ops.
        let binary = crate::binary::Binary::new(self.exec.clone());
        params.register_types(&binary).await?;

        // The task's `map()` signature is `map(..., BulkPutParams args)`, so
        // the server must deserialize the task argument into a concrete
        // `BulkPutParams` POJO (KEEP_BINARY would leave it as
        // `BinaryObjectImpl` and fail the cast). The nested `PutParams.object`
        // field is declared as `BinaryObject` and is serialized via the
        // `WrappedData` (BINARY_OBJ) envelope in `PutParams::to_binary` so
        // the server keeps the inner value as a `BinaryObject` without
        // loading its class (e.g. `UcpRecord`).
        self.execute::<bulk_put::BulkPutParams, bulk_put::BulkPutResponseParams>(
            bulk_put::PUT_ALL_COMPUTE_TASK,
            Some(params),
        )
        .await
    }

    pub(crate) fn new(exec: TokioExec, cluster_group: Option<ClusterGroup>) -> Self {
        Self {
            cluster_group: cluster_group
                .unwrap_or_else(|| ClusterGroup::default_servers(exec.clone())),
            exec,
            flags: 0,
            timeout_ms: 0,
        }
    }

    pub fn with_cluster_group(&self, cluster_group: ClusterGroup) -> Self {
        Self {
            exec: self.exec.clone(),
            cluster_group,
            flags: self.flags,
            timeout_ms: self.timeout_ms,
        }
    }

    pub fn cluster_group(&self) -> ClusterGroup {
        self.cluster_group.clone()
    }

    pub fn with_timeout_ms(&self, timeout_ms: i64) -> Self {
        Self {
            exec: self.exec.clone(),
            cluster_group: self.cluster_group.clone(),
            flags: self.flags,
            timeout_ms,
        }
    }

    pub fn with_no_failover(&self) -> Self {
        Self {
            exec: self.exec.clone(),
            cluster_group: self.cluster_group.clone(),
            flags: self.flags | FLAG_NO_FAILOVER,
            timeout_ms: self.timeout_ms,
        }
    }

    pub fn with_no_result_cache(&self) -> Self {
        Self {
            exec: self.exec.clone(),
            cluster_group: self.cluster_group.clone(),
            flags: self.flags | FLAG_NO_RESULT_CACHE,
            timeout_ms: self.timeout_ms,
        }
    }

    /// Keep the compute-task argument as a `BinaryObject` on the server (does
    /// not deserialize into a POJO). Required when the POJO class is not
    /// available on the server's classpath — the task implementation must use
    /// `BinaryObject` accessors (e.g. `param.getObject()` returns a
    /// `BinaryObject`) regardless of this flag.
    pub fn with_keep_binary(&self) -> Self {
        Self {
            exec: self.exec.clone(),
            cluster_group: self.cluster_group.clone(),
            flags: self.flags | FLAG_KEEP_BINARY,
            timeout_ms: self.timeout_ms,
        }
    }

    pub async fn execute<A: WritableType, R: ReadableType>(
        &self,
        task_name: &str,
        arg: Option<&A>,
    ) -> IgniteResult<Option<R>> {
        self.execute_async(task_name, arg).await?.wait().await
    }

    pub async fn execute_async<A: WritableType, R: ReadableType>(
        &self,
        task_name: &str,
        arg: Option<&A>,
    ) -> IgniteResult<ComputeTask<R>> {
        let cluster_node_ids = self.cluster_group.node_ids().await?;
        if cluster_node_ids.is_empty() {
            return Err(IgniteError::from("Cluster group is empty."));
        }

        let (response, meta): (ComputeExecuteResponse, crate::transport::ResponseMeta) = self
            .exec
            .send_and_read_with_meta(
                OpCode::ComputeTaskExecute,
                ComputeExecuteRequest {
                    node_ids: cluster_node_ids,
                    flags: self.flags,
                    timeout_ms: self.timeout_ms,
                    task_name: task_name.to_string(),
                    arg,
                },
                RequestRoute::default(),
            )
            .await?;

        let receiver = self
            .exec
            .register_notification_listener(
                &meta.address,
                OpCode::ComputeTaskFinished as i16,
                response.task_id,
            )
            .await?;

        Ok(ComputeTask {
            exec: self.exec.clone(),
            address: meta.address,
            task_id: response.task_id,
            receiver: Mutex::new(Some(receiver)),
            cancelled: AtomicBool::new(false),
            _marker: PhantomData,
        })
    }
}

impl<R: ReadableType> ComputeTask<R> {
    pub fn task_id(&self) -> i64 {
        self.task_id
    }

    pub async fn cancel(&self) -> IgniteResult<()> {
        if self.cancelled.swap(true, Ordering::AcqRel) {
            return Ok(());
        }

        self.exec
            .send_with_route(
                OpCode::QueryClose,
                ResourceCloseRequest {
                    resource_id: self.task_id,
                },
                RequestRoute::pinned(self.address.clone()),
            )
            .await?;
        self.exec
            .remove_notification_listener(
                &self.address,
                OpCode::ComputeTaskFinished as i16,
                self.task_id,
            )
            .await;
        Ok(())
    }

    pub async fn wait(&self) -> IgniteResult<Option<R>> {
        if self.cancelled.load(Ordering::Acquire) {
            return Err(IgniteError::from("Task was cancelled"));
        }

        let mut receiver = self
            .receiver
            .lock()
            .await
            .take()
            .ok_or_else(|| IgniteError::from("Compute task result was already consumed"))?;

        let frame = receiver
            .recv()
            .await
            .ok_or_else(|| IgniteError::from("Compute task notification channel closed"))??;

        self.exec
            .remove_notification_listener(
                &self.address,
                OpCode::ComputeTaskFinished as i16,
                self.task_id,
            )
            .await;

        match frame.flag {
            Flag::Success => {
                if frame.payload_offset >= frame.body.len() {
                    return Ok(None);
                }
                let mut cursor = Cursor::new(&frame.body[frame.payload_offset..]);
                R::read(&mut cursor)
            }
            Flag::Failure { err_msg } => Err(IgniteError::server(err_msg)),
        }
    }
}

impl<R> Drop for ComputeTask<R> {
    fn drop(&mut self) {
        if self.cancelled.load(Ordering::Acquire) {
            return;
        }

        let Ok(handle) = Handle::try_current() else {
            return;
        };
        let address = self.address.clone();
        let task_id = self.task_id;
        let exec = self.exec.clone();
        handle.spawn(async move {
            exec.remove_notification_listener(
                &address,
                OpCode::ComputeTaskFinished as i16,
                task_id,
            )
            .await;
        });
    }
}

struct ComputeExecuteRequest<'a, A> {
    node_ids: Vec<String>,
    flags: u8,
    timeout_ms: i64,
    task_name: String,
    arg: Option<&'a A>,
}

impl<A: WritableType> WriteableReq for ComputeExecuteRequest<'_, A> {
    fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        write_i32(writer, self.node_ids.len() as i32)?;
        for node_id in &self.node_ids {
            let (most, least) = parse_uuid_parts(node_id)
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err.to_string()))?;
            write_i64(writer, most)?;
            write_i64(writer, least)?;
        }
        write_u8(writer, self.flags)?;
        write_i64(writer, self.timeout_ms)?;
        // Java's `BinaryReaderEx.readString()` reads a TypeCode byte and
        // expects STRING (9). `write_string` alone writes just length+bytes
        // which the server parses as a stray type code (observed as
        // "Unexpected field type [pos=39, expected=String, actual=54]").
        write_string_type_code(writer, &self.task_name)?;
        match self.arg {
            Some(arg) => arg.write(writer)?,
            None => write_null(writer)?,
        }
        Ok(())
    }

    fn size(&self) -> usize {
        4 + self.node_ids.len() * 16
            + 1
            + 8
            // TypeCode byte (1) + length (4) + UTF-8 bytes.
            + 1
            + 4
            + self.task_name.len()
            + self.arg.map(WritableType::size).unwrap_or(1)
    }
}

struct ComputeExecuteResponse {
    task_id: i64,
}

impl ReadableReq for ComputeExecuteResponse {
    fn read(reader: &mut impl Read) -> IgniteResult<Self> {
        Ok(Self {
            task_id: read_i64(reader).map_err(IgniteError::from)?,
        })
    }
}

struct ResourceCloseRequest {
    resource_id: i64,
}

impl WriteableReq for ResourceCloseRequest {
    fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        write_i64(writer, self.resource_id)
    }

    fn size(&self) -> usize {
        8
    }
}
