use crate::api::OpCode;
use crate::cluster::{parse_uuid_parts, ClusterGroup};
use crate::error::{IgniteError, IgniteResult};
use crate::exec::TokioExec;
use crate::protocol::complex_obj::IgniteValue;
use crate::protocol::{
    read_i32, read_string, read_u8, write_i32, write_i64, write_string, write_string_type_code,
    write_u8,
};
use crate::transport::RequestRoute;
use crate::{ReadableReq, ReadableType, WritableType, WriteableReq};
use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServicePlatform {
    Java,
    DotNet,
    Unknown(u8),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceDescriptor {
    pub name: String,
    pub class_name: String,
    pub total_count: i32,
    pub max_per_node_count: i32,
    pub cache_name: String,
    pub origin_node_id: String,
    pub platform: ServicePlatform,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ServiceCallContext {
    values: HashMap<String, IgniteValue>,
}

#[derive(Clone)]
pub struct Services {
    exec: TokioExec,
    cluster_group: ClusterGroup,
    topology_cache: Arc<Mutex<HashMap<String, CachedServiceTopology>>>,
}

#[derive(Clone)]
pub struct ServiceProxy {
    services: Services,
    name: String,
    timeout_ms: i64,
    call_context: Option<ServiceCallContext>,
}

#[derive(Debug, Clone, Default)]
struct CachedServiceTopology {
    topology_version: Option<i64>,
    node_ids: Vec<String>,
}

impl ServiceCallContext {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_attribute(mut self, name: &str, value: &str) -> Self {
        self.values
            .insert(name.to_string(), IgniteValue::from(value.to_string()));
        self
    }

    pub fn with_binary_attribute(mut self, name: &str, value: Vec<u8>) -> Self {
        self.values
            .insert(name.to_string(), IgniteValue::from(value));
        self
    }
}

impl Services {
    pub(crate) fn new(exec: TokioExec, cluster_group: Option<ClusterGroup>) -> Self {
        Self {
            cluster_group: cluster_group
                .unwrap_or_else(|| ClusterGroup::default_servers(exec.clone())),
            exec,
            topology_cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn with_cluster_group(&self, cluster_group: ClusterGroup) -> Self {
        Self {
            exec: self.exec.clone(),
            cluster_group,
            topology_cache: self.topology_cache.clone(),
        }
    }

    pub fn cluster_group(&self) -> ClusterGroup {
        self.cluster_group.clone()
    }

    pub fn service(&self, name: &str) -> ServiceProxy {
        ServiceProxy {
            services: self.clone(),
            name: name.to_string(),
            timeout_ms: 0,
            call_context: None,
        }
    }

    pub async fn service_descriptors(&self) -> IgniteResult<Vec<ServiceDescriptor>> {
        self.exec
            .send_and_read(OpCode::ServiceGetDescriptors, EmptyRequest)
            .await
    }

    pub async fn service_descriptor(&self, name: &str) -> IgniteResult<ServiceDescriptor> {
        self.exec
            .send_and_read(
                OpCode::ServiceGetDescriptor,
                NameRequest {
                    name: name.to_string(),
                },
            )
            .await
    }

    async fn service_topology(&self, name: &str) -> IgniteResult<Vec<String>> {
        let snapshot = self.exec.topology_snapshot().await;
        let current_topology_version = snapshot.topology_version.map(|version| version.major);

        if let Some(cached) = self.topology_cache.lock().await.get(name).cloned() {
            if cached.topology_version == current_topology_version && !cached.node_ids.is_empty() {
                return Ok(cached.node_ids);
            }
        }

        let response: ServiceTopologyResponse = self
            .exec
            .send_and_read(
                OpCode::ServiceGetTopology,
                NameRequest {
                    name: name.to_string(),
                },
            )
            .await?;

        self.topology_cache.lock().await.insert(
            name.to_string(),
            CachedServiceTopology {
                topology_version: current_topology_version,
                node_ids: response.node_ids.clone(),
            },
        );

        Ok(response.node_ids)
    }
}

impl ServiceProxy {
    pub fn with_timeout_ms(&self, timeout_ms: i64) -> Self {
        Self {
            services: self.services.clone(),
            name: self.name.clone(),
            timeout_ms,
            call_context: self.call_context.clone(),
        }
    }

    pub fn with_call_context(&self, call_context: ServiceCallContext) -> Self {
        Self {
            services: self.services.clone(),
            name: self.name.clone(),
            timeout_ms: self.timeout_ms,
            call_context: Some(call_context),
        }
    }

    pub async fn invoke<R: ReadableType>(
        &self,
        method: &str,
        args: &[IgniteValue],
    ) -> IgniteResult<Option<R>> {
        let cluster_node_ids = self.services.cluster_group.node_ids().await?;
        if cluster_node_ids.is_empty() {
            return Err(IgniteError::from("Cluster group is empty."));
        }

        let topology_nodes = self.services.service_topology(&self.name).await?;
        let preferred_node = topology_nodes.into_iter().find(|node_id| {
            cluster_node_ids
                .iter()
                .any(|candidate| candidate == node_id)
        });

        let route = preferred_node
            .map(|s| RequestRoute::preferred_node(Arc::from(s.as_str())))
            .unwrap_or_default();

        let response: NullableValueResponse<R> = self
            .services
            .exec
            .send_and_read_with_route(
                OpCode::ServiceInvoke,
                ServiceInvokeRequest {
                    service_name: self.name.clone(),
                    timeout_ms: self.timeout_ms,
                    cluster_node_ids,
                    method_name: method.to_string(),
                    args,
                    call_context: self.call_context.as_ref(),
                },
                route,
            )
            .await?;

        Ok(response.value)
    }
}

struct EmptyRequest;

impl WriteableReq for EmptyRequest {
    fn write(&self, _writer: &mut dyn Write) -> io::Result<()> {
        Ok(())
    }

    fn size(&self) -> usize {
        0
    }
}

struct NameRequest {
    name: String,
}

impl WriteableReq for NameRequest {
    fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        write_string(writer, &self.name)
    }

    fn size(&self) -> usize {
        4 + self.name.len()
    }
}

struct ServiceInvokeRequest<'a> {
    service_name: String,
    timeout_ms: i64,
    cluster_node_ids: Vec<String>,
    method_name: String,
    args: &'a [IgniteValue],
    call_context: Option<&'a ServiceCallContext>,
}

impl WriteableReq for ServiceInvokeRequest<'_> {
    fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        // FND-046: service_name and method_name are typed strings — Java
        // `writer.writeString(...)` emits `[STRING_CODE (9), i32 len, bytes]`.
        // Raw strings caused the server's `BinaryReaderEx.readString()` to
        // read the length prefix as the type-code byte.
        write_string_type_code(writer, &self.service_name)?;
        write_u8(writer, 0)?;
        write_i64(writer, self.timeout_ms)?;
        write_i32(writer, self.cluster_node_ids.len() as i32)?;
        for node_id in &self.cluster_node_ids {
            let (most, least) = parse_uuid_parts(node_id)
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err.to_string()))?;
            write_i64(writer, most)?;
            write_i64(writer, least)?;
        }
        write_string_type_code(writer, &self.method_name)?;
        write_i32(writer, self.args.len() as i32)?;
        for arg in self.args {
            arg.write(writer)?;
        }
        write_nullable_map(writer, self.call_context.map(|ctx| &ctx.values))?;
        Ok(())
    }

    fn size(&self) -> usize {
        // Typed strings add a 1-byte TypeCode::String prefix.
        1 + 4 + self.service_name.len()
            + 1
            + 8
            + 4
            + self.cluster_node_ids.len() * 16
            + 1 + 4 + self.method_name.len()
            + 4
            + self.args.iter().map(WritableType::size).sum::<usize>()
            + nullable_map_size(self.call_context.map(|ctx| &ctx.values))
    }
}

impl ReadableReq for ServiceDescriptor {
    fn read(reader: &mut impl Read) -> IgniteResult<Self> {
        Ok(Self {
            name: read_string(reader).map_err(IgniteError::from)?,
            class_name: read_string(reader).map_err(IgniteError::from)?,
            total_count: read_i32(reader).map_err(IgniteError::from)?,
            max_per_node_count: read_i32(reader).map_err(IgniteError::from)?,
            cache_name: read_string(reader).map_err(IgniteError::from)?,
            origin_node_id: crate::connection_async::read_uuid_string(reader)?,
            platform: match read_u8(reader).map_err(IgniteError::from)? {
                0 => ServicePlatform::Java,
                1 => ServicePlatform::DotNet,
                other => ServicePlatform::Unknown(other),
            },
        })
    }
}

impl ReadableReq for Vec<ServiceDescriptor> {
    fn read(reader: &mut impl Read) -> IgniteResult<Self> {
        let count = read_i32(reader).map_err(IgniteError::from)?;
        if count < 0 {
            return Err(IgniteError::from("negative service descriptor count"));
        }

        let mut descriptors = Vec::with_capacity(count as usize);
        for _ in 0..count {
            descriptors.push(ServiceDescriptor::read(reader)?);
        }
        Ok(descriptors)
    }
}

struct ServiceTopologyResponse {
    node_ids: Vec<String>,
}

impl ReadableReq for ServiceTopologyResponse {
    fn read(reader: &mut impl Read) -> IgniteResult<Self> {
        let count = read_i32(reader).map_err(IgniteError::from)?;
        if count < 0 {
            return Err(IgniteError::from("negative service topology count"));
        }

        let mut node_ids = Vec::with_capacity(count as usize);
        for _ in 0..count {
            node_ids.push(crate::connection_async::read_uuid_string(reader)?);
        }
        Ok(Self { node_ids })
    }
}

struct NullableValueResponse<T> {
    value: Option<T>,
}

impl<T: ReadableType> ReadableReq for NullableValueResponse<T> {
    fn read(reader: &mut impl Read) -> IgniteResult<Self> {
        Ok(Self {
            value: T::read(reader)?,
        })
    }
}

fn write_nullable_map(
    writer: &mut dyn Write,
    values: Option<&HashMap<String, IgniteValue>>,
) -> io::Result<()> {
    match values {
        Some(values) => {
            write_i32(writer, values.len() as i32)?;
            for (key, value) in values {
                key.clone().write(writer)?;
                value.write(writer)?;
            }
        }
        None => write_i32(writer, -1)?,
    }
    Ok(())
}

fn nullable_map_size(values: Option<&HashMap<String, IgniteValue>>) -> usize {
    match values {
        Some(values) => {
            4 + values
                .iter()
                .map(|(key, value)| key.len() + 1 + 4 + value.size())
                .sum::<usize>()
        }
        None => 4,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::TypeCode;

    const TYPE_CODE_STRING: u8 = TypeCode::String as u8;

    fn build_request<'a>(
        args: &'a [IgniteValue],
        call_context: Option<&'a ServiceCallContext>,
    ) -> ServiceInvokeRequest<'a> {
        ServiceInvokeRequest {
            service_name: "svc".to_string(),
            timeout_ms: 0,
            cluster_node_ids: Vec::new(),
            method_name: "m".to_string(),
            args,
            call_context,
        }
    }

    /// FND-046: Java `writer.writeString(name)` emits
    /// `[STRING_CODE (9), i32 len, bytes]`. The Rust request must write the
    /// service name and method name as typed strings — not raw — so the
    /// server's `BinaryReaderEx.readString()` succeeds.
    #[test]
    fn service_and_method_names_are_typed_strings() {
        let req = build_request(&[], None);
        let mut buf = Vec::new();
        req.write(&mut buf).unwrap();

        // service_name typed string at byte 0.
        assert_eq!(
            buf[0], TYPE_CODE_STRING,
            "service_name must be typed string (TypeCode::String = 9)"
        );
        let svc_len = i32::from_le_bytes(buf[1..5].try_into().unwrap());
        assert_eq!(svc_len as usize, "svc".len());
        assert_eq!(&buf[5..5 + "svc".len()], b"svc");

        // Typed-svc header = 1+4+3 = 8; flags = 1; timeout = 8; node_count = 4.
        // method_name typed string starts at 8+1+8+4 = 21.
        let method_offset = 1 + 4 + "svc".len() + 1 + 8 + 4;
        assert_eq!(
            buf[method_offset], TYPE_CODE_STRING,
            "method_name must be typed string"
        );
    }

    /// FND-046: `size()` must exactly equal the written byte count so the
    /// 4-byte request-length pre-allocation is correct.
    #[test]
    fn service_invoke_size_matches_written_bytes() {
        let args = [IgniteValue::from("ping".to_string())];
        let req = build_request(&args, None);
        let mut buf = Vec::new();
        req.write(&mut buf).unwrap();
        assert_eq!(req.size(), buf.len());
    }
}
