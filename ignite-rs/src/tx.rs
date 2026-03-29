use crate::api::OpCode;
use crate::cache::CacheCore;
use crate::error::{IgniteError, IgniteResult};
use crate::exec::TokioExec;
use crate::transport::RequestRoute;
use crate::{ReadableReq, ReadableType, WritableType, WriteableReq};
use std::io::{self, Read, Write};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionConcurrency {
    Optimistic = 0,
    Pessimistic = 1,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionIsolation {
    ReadCommitted = 0,
    RepeatableRead = 1,
    Serializable = 2,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionOptions {
    pub concurrency: TransactionConcurrency,
    pub isolation: TransactionIsolation,
    pub timeout: Duration,
    pub label: Option<String>,
}

impl Default for TransactionOptions {
    fn default() -> Self {
        Self {
            concurrency: TransactionConcurrency::Pessimistic,
            isolation: TransactionIsolation::RepeatableRead,
            timeout: Duration::ZERO,
            label: None,
        }
    }
}

impl TransactionOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_concurrency(mut self, concurrency: TransactionConcurrency) -> Self {
        self.concurrency = concurrency;
        self
    }

    pub fn with_isolation(mut self, isolation: TransactionIsolation) -> Self {
        self.isolation = isolation;
        self
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn with_label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
        self
    }
}

#[derive(Clone)]
pub struct Transactions {
    exec: TokioExec,
}

impl Transactions {
    pub(crate) fn new(exec: TokioExec) -> Self {
        Self { exec }
    }

    pub async fn tx_start(&self, options: TransactionOptions) -> IgniteResult<Transaction> {
        let (tx_id, meta) = self
            .exec
            .send_and_read_with_meta::<TxStartResponse>(
                OpCode::TxStart,
                TxStartRequest { options: &options },
                RequestRoute::default(),
            )
            .await?;

        Ok(Transaction {
            inner: Arc::new(TransactionInner {
                exec: self.exec.clone(),
                tx_id: tx_id.id,
                pinned_address: meta.address,
                options,
                state: Mutex::new(TransactionState::Active),
            }),
        })
    }
}

#[derive(Clone)]
pub(crate) struct TransactionContext {
    inner: Arc<TransactionInner>,
}

impl TransactionContext {
    pub(crate) fn tx_id(&self) -> IgniteResult<i32> {
        self.inner.tx_id()
    }

    pub(crate) async fn route(&self) -> IgniteResult<RequestRoute> {
        self.inner.ensure_cache_ops_allowed().await?;
        Ok(RequestRoute::pinned(self.inner.pinned_address.clone()))
    }

    pub(crate) async fn mark_lost(&self) {
        *self.inner.state.lock().await = TransactionState::Lost;
    }

    pub(crate) async fn ensure_cache_ops_allowed(&self) -> IgniteResult<()> {
        self.inner.ensure_cache_ops_allowed().await
    }

    pub(crate) async fn ensure_clear_allowed(&self, op_name: &str) -> IgniteResult<()> {
        self.inner.ensure_clear_allowed(op_name).await
    }
}

pub struct Transaction {
    inner: Arc<TransactionInner>,
}

impl Transaction {
    pub async fn commit(&self) -> IgniteResult<()> {
        self.inner.end(true).await
    }

    pub async fn rollback(&self) -> IgniteResult<()> {
        self.inner.end(false).await
    }

    pub fn cache<K: WritableType + ReadableType, V: WritableType + ReadableType>(
        &self,
        name: &str,
    ) -> CacheCore<K, V> {
        let id = crate::utils::string_to_java_hashcode(name);
        CacheCore::new_with_tx(
            id,
            Arc::from(name),
            self.inner.exec.clone(),
            Some(TransactionContext {
                inner: self.inner.clone(),
            }),
        )
    }

    pub fn options(&self) -> &TransactionOptions {
        &self.inner.options
    }

    pub fn tx_id(&self) -> i32 {
        self.inner.tx_id
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if Arc::strong_count(&self.inner) != 1 {
            return;
        }

        let inner = self.inner.clone();
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };

        handle.spawn(async move {
            let should_rollback = matches!(*inner.state.lock().await, TransactionState::Active);
            if !should_rollback {
                return;
            }

            *inner.state.lock().await = TransactionState::RolledBack;
            let _ = inner
                .exec
                .send_with_route(
                    OpCode::TxEnd,
                    TxEndRequest {
                        tx_id: inner.tx_id,
                        committed: false,
                    },
                    RequestRoute::pinned(inner.pinned_address.clone()),
                )
                .await;
        });
    }
}

struct TransactionInner {
    exec: TokioExec,
    tx_id: i32,
    pinned_address: String,
    options: TransactionOptions,
    state: Mutex<TransactionState>,
}

impl TransactionInner {
    fn tx_id(&self) -> IgniteResult<i32> {
        Ok(self.tx_id)
    }

    async fn ensure_cache_ops_allowed(&self) -> IgniteResult<()> {
        match *self.state.lock().await {
            TransactionState::Active => Ok(()),
            TransactionState::Committed | TransactionState::RolledBack => {
                Err(IgniteError::from("The transaction is already closed"))
            }
            TransactionState::Lost => Err(IgniteError::from(
                "Transaction context has been lost due to connection errors. Cache operations are prohibited until current transaction closed.",
            )),
        }
    }

    async fn ensure_clear_allowed(&self, op_name: &str) -> IgniteResult<()> {
        self.ensure_cache_ops_allowed().await?;
        Err(IgniteError::from(
            format!(
                "non-transactional ClientCache {} operation within a transaction.",
                op_name
            )
            .as_str(),
        ))
    }

    async fn end(&self, committed: bool) -> IgniteResult<()> {
        {
            let state = self.state.lock().await;
            match *state {
                TransactionState::Committed | TransactionState::RolledBack => {
                    return Err(IgniteError::from("The transaction is already closed"));
                }
                TransactionState::Lost => {
                    return Err(IgniteError::from(
                        "Transaction context has been lost due to connection errors",
                    ));
                }
                TransactionState::Active => {}
            }
        }

        let result = self
            .exec
            .send_with_route(
                OpCode::TxEnd,
                TxEndRequest {
                    tx_id: self.tx_id,
                    committed,
                },
                RequestRoute::pinned(self.pinned_address.clone()),
            )
            .await;

        let mut state = self.state.lock().await;
        *state = if committed {
            TransactionState::Committed
        } else {
            TransactionState::RolledBack
        };

        result.map_err(|err| {
            IgniteError::from(
                format!(
                    "Transaction context has been lost due to connection errors: {}",
                    err
                )
                .as_str(),
            )
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransactionState {
    Active,
    Committed,
    RolledBack,
    Lost,
}

struct TxStartRequest<'a> {
    options: &'a TransactionOptions,
}

impl WriteableReq for TxStartRequest<'_> {
    fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        writer.write_all(&[self.options.concurrency as u8])?;
        writer.write_all(&[self.options.isolation as u8])?;
        crate::protocol::write_i64(writer, self.options.timeout.as_millis() as i64)?;
        match self.options.label.as_deref() {
            Some(label) => crate::protocol::write_string_type_code(writer, label)?,
            None => crate::protocol::write_null(writer)?,
        }
        Ok(())
    }

    fn size(&self) -> usize {
        1 + 1
            + 8
            + match self.options.label.as_deref() {
                Some(label) => 1 + 4 + label.len(),
                None => 1,
            }
    }
}

struct TxStartResponse {
    id: i32,
}

impl ReadableReq for TxStartResponse {
    fn read(reader: &mut impl Read) -> IgniteResult<Self> {
        Ok(Self {
            id: crate::protocol::read_i32(reader)?,
        })
    }
}

struct TxEndRequest {
    tx_id: i32,
    committed: bool,
}

impl WriteableReq for TxEndRequest {
    fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        crate::protocol::write_i32(writer, self.tx_id)?;
        crate::protocol::write_bool(writer, self.committed)?;
        Ok(())
    }

    fn size(&self) -> usize {
        4 + 1
    }
}
