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

/// Java `ClientServicesImpl.FLAG_PARAMETER_TYPES_MASK` (`ClientServicesImpl.java:368@2.17.0`).
/// Always set on 2.17 — parameter type IDs follow each arg (see FND-045).
const FLAG_PARAMETER_TYPES_MASK: u8 = 0x02;

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

    /// Invoke a service method. Per-argument Java parameter type IDs are
    /// derived from the `IgniteValue` variant via [`default_param_type_id`] —
    /// matching Java when the declared parameter type is the standard wrapper
    /// class for that value (e.g. `Integer` for `IgniteValue::Int`).
    ///
    /// For methods with overloaded signatures (e.g. `process(int)` vs
    /// `process(long)`), use [`invoke_with_types`](Self::invoke_with_types)
    /// to supply exact type IDs matching the target overload's declared
    /// parameter class names.
    pub async fn invoke<R: ReadableType>(
        &self,
        method: &str,
        args: &[IgniteValue],
    ) -> IgniteResult<Option<R>> {
        let typed_args: Vec<(i32, &IgniteValue)> = args
            .iter()
            .map(|arg| (default_param_type_id(arg), arg))
            .collect();
        self.invoke_typed(method, &typed_args).await
    }

    /// Invoke a service method with explicit per-argument Java parameter type
    /// IDs. Pairs of `(typeId, argValue)` — `typeId` must match the declared
    /// Java parameter type via `BinaryContext.typeId(parameterType.getName())`
    /// so the server resolves the correct overload (§7.4, FND-048).
    ///
    /// Use [`param_type_id`] to compute a typeId from a Java class name.
    pub async fn invoke_with_types<R: ReadableType>(
        &self,
        method: &str,
        args: &[(i32, IgniteValue)],
    ) -> IgniteResult<Option<R>> {
        let typed_args: Vec<(i32, &IgniteValue)> = args
            .iter()
            .map(|(type_id, value)| (*type_id, value))
            .collect();
        self.invoke_typed(method, &typed_args).await
    }

    async fn invoke_typed<R: ReadableType>(
        &self,
        method: &str,
        args: &[(i32, &IgniteValue)],
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

        // FND-047: trailing callAttrs map is written only when the
        // `SERVICE_INVOKE_CALLCTX` feature bit is negotiated (or an
        // explicit ServiceCallContext was supplied).
        let caps = self.services.exec.service_invoke_capabilities().await;

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
                    callctx_feature_supported: caps.service_invoke_callctx,
                },
                route,
            )
            .await?;

        Ok(response.value)
    }
}

/// Java `BinaryContext.typeId(className)` — for well-known wrapper classes
/// and primitive arrays the server resolves to a fixed
/// `GridBinaryMarshaller` constant (e.g. `Integer` → `INT = 3`); for other
/// class names it falls through to the `SIMPLE_NAME_LOWER_CASE_MAPPER`
/// which hashes `simple_name(name).to_lowercase()` (Java hashcode).
///
/// Used for the `SERVICE_INVOKE` per-argument `paramTypeId` prefix (§7.1).
pub fn param_type_id(java_class_name: &str) -> i32 {
    use crate::utils::string_to_java_hashcode;

    // Predefined mappings from `BinaryContext.registerPredefinedType(...)`
    // — the simple lowercase class name maps to a `GridBinaryMarshaller`
    // constant. Primitives (`int`, `long`, …) are NOT predefined; they
    // hash through to `string_to_java_hashcode` just like user classes.
    let simple_lower = simple_name_lowercase(java_class_name);
    match simple_lower.as_str() {
        "object" => -1,
        "byte" if java_class_name == "java.lang.Byte" => 1,
        "short" if java_class_name == "java.lang.Short" => 2,
        "integer" => 3,
        "long" if java_class_name == "java.lang.Long" => 4,
        "float" if java_class_name == "java.lang.Float" => 5,
        "double" if java_class_name == "java.lang.Double" => 6,
        "character" => 7,
        "boolean" if java_class_name == "java.lang.Boolean" => 8,
        "string" if java_class_name == "java.lang.String" => 9,
        "uuid" if java_class_name == "java.util.UUID" => 10,
        "date" if java_class_name == "java.util.Date" => 11,
        "timestamp" if java_class_name == "java.sql.Timestamp" => 33,
        "time" if java_class_name == "java.sql.Time" => 36,
        "bigdecimal" if java_class_name == "java.math.BigDecimal" => 30,
        "byte[]" => 12,
        "short[]" => 13,
        "int[]" => 14,
        "long[]" => 15,
        "float[]" => 16,
        "double[]" => 17,
        "char[]" => 18,
        "boolean[]" => 19,
        _ => string_to_java_hashcode(&simple_lower),
    }
}

/// Java `SIMPLE_NAME_LOWER_CASE_MAPPER.typeName(clsName)` — strips package
/// then lowercases. For primitive-array class names (`"[B"`, `"[I"`, …)
/// Java returns the canonical `Type[]` form used by `BinaryContext`'s
/// predefined-type table; this helper normalizes the few forms we need.
fn simple_name_lowercase(class_name: &str) -> String {
    let canonical = match class_name {
        "[B" => "byte[]",
        "[S" => "short[]",
        "[I" => "int[]",
        "[J" => "long[]",
        "[F" => "float[]",
        "[D" => "double[]",
        "[C" => "char[]",
        "[Z" => "boolean[]",
        other => other,
    };
    let simple = match canonical.rfind(['.', '$']) {
        Some(idx) => &canonical[idx + 1..],
        None => canonical,
    };
    simple.to_lowercase()
}

/// Derive a Java parameter type ID from an `IgniteValue` variant, assuming
/// the method's declared parameter type is the standard Java wrapper class
/// for that value (`Integer` for `Int`, `String` for `String`, …). For
/// `IgniteValue::Object` the ComplexObject's type_name is used. For
/// `Null` / `Map` / `Collection` / `PreEncoded` / `OpaqueMarshal` this
/// returns `OBJECT = -1` — callers needing overload-precise dispatch must
/// use [`ServiceProxy::invoke_with_types`].
fn default_param_type_id(value: &IgniteValue) -> i32 {
    match value {
        IgniteValue::Byte(_) => 1,
        IgniteValue::Short(_) => 2,
        IgniteValue::Int(_) => 3,
        IgniteValue::Long(_) => 4,
        IgniteValue::Float(_) => 5,
        IgniteValue::Double(_) => 6,
        IgniteValue::Char(_) => 7,
        IgniteValue::Bool(_) => 8,
        IgniteValue::String(_) => 9,
        IgniteValue::Uuid(_, _) => 10,
        IgniteValue::Date(_) => 11,
        IgniteValue::Binary(_) => 12,
        IgniteValue::Array(_) => 23, // Object[] → OBJ_ARR
        IgniteValue::Enum(_) => 28,
        IgniteValue::Timestamp(_, _) => 33,
        IgniteValue::Time(_) => 36,
        IgniteValue::Decimal(_, _) => 30,
        IgniteValue::Object(obj) => param_type_id(obj.type_name()),
        // Null, Map, Collection, PreEncoded, OpaqueMarshal have no single
        // canonical Java parameter-type mapping. Fall back to OBJECT=-1
        // (matches a method declared as `Object`).
        _ => -1,
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
    args: &'a [(i32, &'a IgniteValue)],
    call_context: Option<&'a ServiceCallContext>,
    /// Whether the server negotiated `SERVICE_INVOKE_CALLCTX` (bit 10).
    /// Java `ClientServicesImpl.java:401-404@2.17.0` writes the trailing
    /// `callAttrs` map only when either `callAttrs != null` or this bit
    /// is supported — else the field is omitted entirely. FND-047.
    callctx_feature_supported: bool,
}

impl WriteableReq for ServiceInvokeRequest<'_> {
    fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        // FND-046: service_name and method_name are typed strings — Java
        // `writer.writeString(...)` emits `[STRING_CODE (9), i32 len, bytes]`.
        // Raw strings caused the server's `BinaryReaderEx.readString()` to
        // read the length prefix as the type-code byte.
        write_string_type_code(writer, &self.service_name)?;
        // FND-044: Java always sets FLAG_PARAMETER_TYPES_MASK on 2.17
        // (`ClientServicesImpl.java:368@2.17.0`). With the mask set, the
        // server expects each arg to be prefixed with an i32 paramTypeId
        // (see FND-045); without it, the server falls back to name-only
        // overload matching, so the emitted flag must match Java.
        write_u8(writer, FLAG_PARAMETER_TYPES_MASK)?;
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
        // FND-045: Each arg is prefixed with the declared-parameter typeId.
        // Java `ClientServicesImpl.java:395-398@2.17.0` writes
        // `(i32 paramTypeId, <any-object> argValue)` per arg — the server
        // uses the `(methodName, List<paramTypeId>)` pair to resolve the
        // correct overload (§7.4, FND-048).
        for (type_id, arg) in self.args {
            write_i32(writer, *type_id)?;
            arg.write(writer)?;
        }
        // FND-047: Only write the trailing `callAttrs` map when either
        // `callAttrs != null` or the `SERVICE_INVOKE_CALLCTX` feature bit
        // is negotiated (`ClientServicesImpl.java:401-404@2.17.0`).
        // Otherwise the field is absent from the wire — writing 4 extra
        // bytes would desynchronize the server's frame parser.
        if self.call_context.is_some() || self.callctx_feature_supported {
            write_nullable_map(writer, self.call_context.map(|ctx| &ctx.values))?;
        }
        Ok(())
    }

    fn size(&self) -> usize {
        let trailing = if self.call_context.is_some() || self.callctx_feature_supported {
            nullable_map_size(self.call_context.map(|ctx| &ctx.values))
        } else {
            0
        };
        // Typed strings add a 1-byte TypeCode::String prefix.
        1 + 4 + self.service_name.len()
            + 1
            + 8
            + 4
            + self.cluster_node_ids.len() * 16
            + 1 + 4 + self.method_name.len()
            + 4
            + self
                .args
                .iter()
                .map(|(_, arg)| 4 + arg.size())
                .sum::<usize>()
            + trailing
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
    const TYPE_CODE_INT: u8 = TypeCode::Int as u8;

    fn build_request<'a>(
        args: &'a [(i32, &'a IgniteValue)],
        call_context: Option<&'a ServiceCallContext>,
    ) -> ServiceInvokeRequest<'a> {
        build_request_with_caps(args, call_context, true)
    }

    fn build_request_with_caps<'a>(
        args: &'a [(i32, &'a IgniteValue)],
        call_context: Option<&'a ServiceCallContext>,
        callctx_feature_supported: bool,
    ) -> ServiceInvokeRequest<'a> {
        ServiceInvokeRequest {
            service_name: "svc".to_string(),
            timeout_ms: 0,
            cluster_node_ids: Vec::new(),
            method_name: "m".to_string(),
            args,
            call_context,
            callctx_feature_supported,
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
        let ping = IgniteValue::from("ping".to_string());
        let args: [(i32, &IgniteValue); 1] = [(9, &ping)];
        let req = build_request(&args, None);
        let mut buf = Vec::new();
        req.write(&mut buf).unwrap();
        assert_eq!(req.size(), buf.len());
    }

    /// FND-044: Java always sets `FLAG_PARAMETER_TYPES_MASK = 0x02` on 2.17
    /// (`ClientServicesImpl.java:368@2.17.0`). The flags byte sits at offset
    /// 1+4+name_len (after the typed service-name header).
    #[test]
    fn flags_byte_is_parameter_types_mask() {
        let req = build_request(&[], None);
        let mut buf = Vec::new();
        req.write(&mut buf).unwrap();

        let flags_offset = 1 + 4 + "svc".len();
        assert_eq!(
            buf[flags_offset], FLAG_PARAMETER_TYPES_MASK,
            "flags byte must be 0x02 (FLAG_PARAMETER_TYPES_MASK)"
        );
    }

    /// FND-045: Each arg is prefixed with a 4-byte `paramTypeId`. With an
    /// `Int` arg (value 7), Java sends `typeId=3` (GridBinaryMarshaller.INT
    /// for `Integer.class`) followed by the typed int object.
    #[test]
    fn args_are_prefixed_with_param_type_id() {
        let int_val = IgniteValue::Int(7);
        let args: [(i32, &IgniteValue); 1] = [(3, &int_val)];
        let req = build_request(&args, None);
        let mut buf = Vec::new();
        req.write(&mut buf).unwrap();

        // method_name typed header = 1+4+1 ("m"). Preceded by:
        //   typed svc_name (1+4+3=8) + flags (1) + timeout (8) + nodeCount (4) = 21.
        let method_start = 1 + 4 + "svc".len() + 1 + 8 + 4;
        let after_method = method_start + 1 + 4 + "m".len();
        // arg_count (4) then first arg: typeId (4) + int object (1+4=5).
        let arg_count = i32::from_le_bytes(
            buf[after_method..after_method + 4].try_into().unwrap(),
        );
        assert_eq!(arg_count, 1);
        let type_id_offset = after_method + 4;
        let type_id = i32::from_le_bytes(
            buf[type_id_offset..type_id_offset + 4].try_into().unwrap(),
        );
        assert_eq!(type_id, 3, "Integer typeId must be GridBinaryMarshaller.INT = 3");
        // Arg value: TypeCode::Int then 4 bytes of i32.
        let arg_code_offset = type_id_offset + 4;
        assert_eq!(buf[arg_code_offset], TYPE_CODE_INT);
    }

    /// FND-045 helper: `param_type_id` maps Java class names to
    /// `GridBinaryMarshaller` constants for predefined wrappers, matching
    /// `BinaryContext.typeId(...)` on the Java side.
    #[test]
    fn param_type_id_matches_java_predefined_constants() {
        assert_eq!(param_type_id("java.lang.Byte"), 1);
        assert_eq!(param_type_id("java.lang.Short"), 2);
        assert_eq!(param_type_id("java.lang.Integer"), 3);
        assert_eq!(param_type_id("java.lang.Long"), 4);
        assert_eq!(param_type_id("java.lang.Float"), 5);
        assert_eq!(param_type_id("java.lang.Double"), 6);
        assert_eq!(param_type_id("java.lang.Character"), 7);
        assert_eq!(param_type_id("java.lang.Boolean"), 8);
        assert_eq!(param_type_id("java.lang.String"), 9);
        assert_eq!(param_type_id("java.util.UUID"), 10);
        assert_eq!(param_type_id("java.util.Date"), 11);
        assert_eq!(param_type_id("byte[]"), 12);
        assert_eq!(param_type_id("int[]"), 14);
        assert_eq!(param_type_id("java.sql.Timestamp"), 33);
    }

    /// FND-045 helper: primitives `int`, `long`, etc. are NOT predefined in
    /// `BinaryContext`; they hash through the SIMPLE_NAME_LOWER_CASE_MAPPER.
    /// `BinaryBasicIdMapper.lowerCaseHashCode("int") == "int".hashCode()`
    /// (lowercase hashing leaves ASCII unchanged).
    #[test]
    fn param_type_id_primitive_names_use_hashcode() {
        use crate::utils::string_to_java_hashcode;
        assert_eq!(param_type_id("int"), string_to_java_hashcode("int"));
        assert_eq!(param_type_id("long"), string_to_java_hashcode("long"));
    }

    /// FND-047: The trailing `callAttrs` map must NOT be written when
    /// `callAttrs is None` AND the `SERVICE_INVOKE_CALLCTX` feature bit is
    /// not negotiated. Java gates on `callAttrs != null ||
    /// protocolCtx.isFeatureSupported(SERVICE_INVOKE_CALLCTX)`.
    #[test]
    fn callctx_field_is_omitted_when_feature_not_negotiated() {
        let req = build_request_with_caps(&[], None, false);
        let mut buf = Vec::new();
        req.write(&mut buf).unwrap();

        // Layout: typed svc (8) + flags (1) + timeout (8) + nodeCount (4)
        //       + typed method (6) + arg_count (4) = 31.
        let expected_end = 1 + 4 + "svc".len() + 1 + 8 + 4 + 1 + 4 + "m".len() + 4;
        assert_eq!(
            buf.len(),
            expected_end,
            "no trailing bytes — callctx field omitted"
        );
        // size() must agree.
        assert_eq!(req.size(), buf.len());
    }

    /// FND-047 paired: when the feature bit IS negotiated and callAttrs is
    /// null, Java writes `writeMap(null)` which serializes a null sentinel.
    /// The Rust implementation writes `i32 -1` — legacy behavior preserved.
    /// What matters for this test is the field is PRESENT (not absent).
    #[test]
    fn callctx_field_is_present_when_feature_negotiated() {
        let req = build_request_with_caps(&[], None, true);
        let mut buf = Vec::new();
        req.write(&mut buf).unwrap();

        let prefix_len = 1 + 4 + "svc".len() + 1 + 8 + 4 + 1 + 4 + "m".len() + 4;
        assert!(
            buf.len() > prefix_len,
            "trailing callctx field must be present when SERVICE_INVOKE_CALLCTX negotiated"
        );
        assert_eq!(req.size(), buf.len());
    }

    /// FND-047: callAttrs explicitly supplied by the caller must be written
    /// regardless of the feature bit — Java's `ClientServicesImpl.java:401`
    /// writes the map whenever `callAttrs != null`.
    #[test]
    fn callctx_field_is_written_when_context_supplied() {
        let ctx = ServiceCallContext::new().with_attribute("k", "v");
        let req = build_request_with_caps(&[], Some(&ctx), false);
        let mut buf = Vec::new();
        req.write(&mut buf).unwrap();

        let prefix_len = 1 + 4 + "svc".len() + 1 + 8 + 4 + 1 + 4 + "m".len() + 4;
        assert!(buf.len() > prefix_len, "call context map must be written");
        assert_eq!(req.size(), buf.len());
    }

    /// FND-048 (overload dispatch): sending `process` with `Int(7)` should
    /// emit the typeId for `Integer` (3), while the same method with
    /// `Long(7)` emits typeId `Long` (4). Server uses
    /// `(methodName, List<paramTypeId>)` to pick the overload.
    #[test]
    fn overload_dispatch_uses_distinct_type_ids_per_arg_variant() {
        let int_val = IgniteValue::Int(7);
        let long_val = IgniteValue::Long(7);
        let args_int: [(i32, &IgniteValue); 1] = [(default_param_type_id(&int_val), &int_val)];
        let args_long: [(i32, &IgniteValue); 1] = [(default_param_type_id(&long_val), &long_val)];

        let mut buf_int = Vec::new();
        build_request(&args_int, None).write(&mut buf_int).unwrap();
        let mut buf_long = Vec::new();
        build_request(&args_long, None)
            .write(&mut buf_long)
            .unwrap();

        let method_start = 1 + 4 + "svc".len() + 1 + 8 + 4;
        let after_method = method_start + 1 + 4 + "m".len();
        let type_id_offset = after_method + 4;

        let int_type_id = i32::from_le_bytes(
            buf_int[type_id_offset..type_id_offset + 4]
                .try_into()
                .unwrap(),
        );
        let long_type_id = i32::from_le_bytes(
            buf_long[type_id_offset..type_id_offset + 4]
                .try_into()
                .unwrap(),
        );
        assert_eq!(int_type_id, 3);
        assert_eq!(long_type_id, 4);
        assert_ne!(int_type_id, long_type_id);
    }

    /// FND-049 (coverage): `ServiceTopologyResponse` reads
    /// `i32 count; count × (i64 msb, i64 lsb)` — matching Java
    /// (§7.3, `ClientServicesImpl.java:237-244@2.17.0`). Pinned against
    /// drift — the read-uuid helper should format the pair as a canonical
    /// UUID string.
    #[test]
    fn service_topology_response_matches_java_wire_shape() {
        use std::io::Cursor;
        let mut payload = Vec::new();
        payload.extend_from_slice(&2i32.to_le_bytes());
        // Node 1: (msb=0x11, lsb=0x22)
        payload.extend_from_slice(&0x11i64.to_le_bytes());
        payload.extend_from_slice(&0x22i64.to_le_bytes());
        // Node 2: (msb=0x33, lsb=0x44)
        payload.extend_from_slice(&0x33i64.to_le_bytes());
        payload.extend_from_slice(&0x44i64.to_le_bytes());

        let mut cursor = Cursor::new(payload);
        let resp = ServiceTopologyResponse::read(&mut cursor).unwrap();
        assert_eq!(resp.node_ids.len(), 2);
        // read_uuid_string formats the (msb, lsb) pair as canonical UUID.
        assert!(resp.node_ids[0].contains("-"));
    }
}
