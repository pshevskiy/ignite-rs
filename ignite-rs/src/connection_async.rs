use crate::error::{IgniteError, IgniteResult};
use crate::protocol::Flag::{Failure, Success};
use crate::protocol::{read_i16, read_i32, read_i64, read_string, write_i32, Flag, TypeCode};
use crate::topology::TopologyVersion;
use crate::ClientConfig;
use std::collections::BTreeMap;
use std::convert::TryFrom;
use std::future::Future;
use std::io;
use std::io::Cursor;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{self as tokio_io, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;

pub(crate) type AsyncReadHalf = tokio_io::ReadHalf<AsyncStream>;
pub(crate) type AsyncWriteHalf = tokio_io::WriteHalf<AsyncStream>;

const HANDSHAKE_OP_CODE: u8 = 1;
const CLIENT_CODE: u8 = 2;
const V_MAJOR: i16 = 1;
const V_MINOR: i16 = 7;
const V_PATCH: i16 = 0;
const FLAG_ERROR: i16 = 1;
const FLAG_AFFINITY_TOPOLOGY_CHANGED: i16 = 1 << 1;
const FLAG_NOTIFICATION: i16 = 1 << 2;
const FEATURE_USER_ATTRIBUTES: usize = 0;
const FEATURE_NODE_ENDPOINTS: usize = 3;
const FEATURE_QRY_PARTITIONS_BATCH_SIZE: usize = 7;
const FEATURE_HEARTBEAT: usize = 11;
const FEATURE_DC_AWARE: usize = 22;
const FEATURE_QRY_INITIATOR_ID: usize = 23;
const STATUS_SECURITY_VIOLATION: i32 = 1012;
const STATUS_AUTH_FAILED: i32 = 2000;
const PARTITION_AWARENESS_VERSION: (i16, i16, i16) = (1, 4, 0);
const BITMAP_FEATURES_VERSION: (i16, i16, i16) = (1, 7, 0);

#[derive(Clone, Debug, Default)]
pub(crate) struct ConnectionCapabilities {
    pub(crate) partition_awareness: bool,
    pub(crate) node_endpoints: bool,
    pub(crate) heartbeat: bool,
    pub(crate) dc_aware: bool,
    pub(crate) query_partitions_batch_size: bool,
    pub(crate) query_initiator_id: bool,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ConnectionMetadata {
    pub(crate) capabilities: ConnectionCapabilities,
    pub(crate) server_node_id: Option<String>,
}

pub(crate) struct ResponseFrame {
    pub(crate) correlation_id: i64,
    pub(crate) flag: Flag,
    pub(crate) body: Vec<u8>,
    pub(crate) topology_version: Option<TopologyVersion>,
}

#[derive(Clone, Debug)]
pub(crate) struct NotificationFrame {
    pub(crate) resource_id: i64,
    pub(crate) op_code: i16,
    pub(crate) flag: Flag,
    pub(crate) body: Vec<u8>,
    #[allow(dead_code)]
    pub(crate) topology_version: Option<TopologyVersion>,
}

pub(crate) enum IncomingFrame {
    Response(ResponseFrame),
    Notification(NotificationFrame),
}

pub(crate) enum AsyncStream {
    Plain(TcpStream),
    #[cfg(feature = "ssl")]
    Tls(tokio_rustls::client::TlsStream<TcpStream>),
}

pub(crate) struct AsyncConnection {
    stream: AsyncStream,
    metadata: ConnectionMetadata,
}

impl AsyncConnection {
    pub(crate) async fn connect(
        conf: &ClientConfig,
        address: &str,
    ) -> IgniteResult<AsyncConnection> {
        if address.trim().is_empty() {
            return Err(IgniteError::from("At least one non-empty address expected"));
        }

        let tcp = with_timeout_io(conf.handshake_timeout, TcpStream::connect(address)).await?;

        if let Some(nodelay) = conf.tcp_nodelay {
            tcp.set_nodelay(nodelay)
                .map_err(|err| IgniteError::connection(err.to_string()))?;
        }
        if let Some(ttl) = conf.tcp_ttl {
            tcp.set_ttl(ttl)
                .map_err(|err| IgniteError::connection(err.to_string()))?;
        }

        #[cfg(not(feature = "ssl"))]
        {
            let mut stream = AsyncStream::Plain(tcp);
            let metadata =
                with_timeout_ignite(conf.handshake_timeout, handshake_async(&mut stream, conf))
                    .await?;
            return Ok(AsyncConnection { stream, metadata });
        }

        #[cfg(feature = "ssl")]
        {
            let mut stream = if let Some(ref tls_conf) = conf.tls_conf {
                AsyncStream::Tls(
                    with_timeout_ignite(conf.handshake_timeout, wrap_tls_stream(tls_conf, tcp))
                        .await?,
                )
            } else {
                AsyncStream::Plain(tcp)
            };

            let metadata =
                with_timeout_ignite(conf.handshake_timeout, handshake_async(&mut stream, conf))
                    .await?;
            Ok(AsyncConnection { stream, metadata })
        }
    }

    pub(crate) fn into_parts(self) -> (AsyncReadHalf, AsyncWriteHalf, ConnectionMetadata) {
        let metadata = self.metadata;
        let (reader, writer) = tokio_io::split(self.stream);
        (reader, writer, metadata)
    }
}

impl AsyncRead for AsyncStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match &mut *self {
            AsyncStream::Plain(stream) => Pin::new(stream).poll_read(cx, buf),
            #[cfg(feature = "ssl")]
            AsyncStream::Tls(stream) => Pin::new(stream).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for AsyncStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        match &mut *self {
            AsyncStream::Plain(stream) => Pin::new(stream).poll_write(cx, buf),
            #[cfg(feature = "ssl")]
            AsyncStream::Tls(stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        match &mut *self {
            AsyncStream::Plain(stream) => Pin::new(stream).poll_flush(cx),
            #[cfg(feature = "ssl")]
            AsyncStream::Tls(stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        match &mut *self {
            AsyncStream::Plain(stream) => Pin::new(stream).poll_shutdown(cx),
            #[cfg(feature = "ssl")]
            AsyncStream::Tls(stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }
}

pub(crate) async fn write_request_batch(
    writer: &mut AsyncWriteHalf,
    requests: &[&[u8]],
    request_timeout: Option<Duration>,
    flush: bool,
) -> IgniteResult<()> {
    write_requests(writer, requests, request_timeout, flush).await
}

async fn write_requests<W>(
    writer: &mut W,
    requests: &[&[u8]],
    request_timeout: Option<Duration>,
    flush: bool,
) -> IgniteResult<()>
where
    W: AsyncWrite + Unpin,
{
    with_timeout_ignite(request_timeout, async {
        for request in requests {
            writer
                .write_all(request)
                .await
                .map_err(|err| IgniteError::connection(err.to_string()))?;
        }
        if flush {
            writer
                .flush()
                .await
                .map_err(|err| IgniteError::connection(err.to_string()))?;
        }
        Ok(())
    })
    .await
}

pub(crate) async fn read_incoming_frame(
    reader: &mut AsyncReadHalf,
    metadata: &ConnectionMetadata,
) -> IgniteResult<IncomingFrame> {
    let mut len_buf = [0u8; 4];
    reader
        .read_exact(&mut len_buf)
        .await
        .map_err(|err| IgniteError::connection(err.to_string()))?;
    let mut cur = Cursor::new(len_buf);
    let body_len_i32 = read_i32(&mut cur)?;
    if body_len_i32 < 0 {
        return Err(IgniteError::from("Negative response body length"));
    }
    let body_len = body_len_i32 as usize;

    let mut body = vec![0u8; body_len];
    reader
        .read_exact(&mut body)
        .await
        .map_err(|err| IgniteError::connection(err.to_string()))?;

    let mut rdr = Cursor::new(&body);
    let correlation_id = read_i64(&mut rdr)?;
    let mut topology_version = None;

    let mut notification_op_code = None;
    let flag = if metadata.capabilities.partition_awareness {
        let flags = read_i16(&mut rdr)?;

        if (flags & FLAG_AFFINITY_TOPOLOGY_CHANGED) != 0 {
            topology_version = Some(TopologyVersion::new(
                read_i64(&mut rdr)?,
                read_i32(&mut rdr)?,
            ));
        }

        if (flags & FLAG_NOTIFICATION) != 0 {
            notification_op_code = Some(read_i16(&mut rdr)?);
        }

        if (flags & FLAG_ERROR) != 0 {
            let _status = read_i32(&mut rdr)?;
            let err_msg = read_response_error_string(&mut rdr).map_err(IgniteError::from)?;
            Failure { err_msg }
        } else {
            Success
        }
    } else {
        match read_i32(&mut rdr)? {
            0 => Success,
            _ => {
                let err_msg = read_response_error_string(&mut rdr).map_err(IgniteError::from)?;
                Failure { err_msg }
            }
        }
    };

    let payload = match &flag {
        Success => body.split_off(rdr.position() as usize),
        Failure { .. } => Vec::new(),
    };

    if let Some(op_code) = notification_op_code {
        Ok(IncomingFrame::Notification(NotificationFrame {
            resource_id: correlation_id,
            op_code,
            flag,
            body: payload,
            topology_version,
        }))
    } else {
        Ok(IncomingFrame::Response(ResponseFrame {
            correlation_id,
            flag,
            body: payload,
            topology_version,
        }))
    }
}

#[cfg(feature = "ssl")]
async fn wrap_tls_stream(
    conf: &(rustls::ClientConfig, String),
    stream: TcpStream,
) -> IgniteResult<tokio_rustls::client::TlsStream<TcpStream>> {
    use std::sync::Arc;

    let connector = tokio_rustls::TlsConnector::from(Arc::new(conf.0.clone()));
    let server_name = rustls::pki_types::ServerName::try_from(conf.1.clone())
        .map_err(|e| IgniteError::tls(e.to_string()))?;
    connector
        .connect(server_name, stream)
        .await
        .map_err(|err| IgniteError::tls(err.to_string()))
}

async fn handshake_async<S>(conn: &mut S, conf: &ClientConfig) -> IgniteResult<ConnectionMetadata>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    use crate::protocol::read_u8;

    if conf.username.is_none() && conf.password.is_some() {
        return Err(IgniteError::handshake(
            "Username expected when password is configured!",
        ));
    }

    let buf = build_handshake_request(conf)?;

    conn.write_all(&buf)
        .await
        .map_err(|err| IgniteError::connection(err.to_string()))?;
    conn.flush()
        .await
        .map_err(|err| IgniteError::connection(err.to_string()))?;

    let mut len_buf = [0u8; 4];
    conn.read_exact(&mut len_buf)
        .await
        .map_err(|err| IgniteError::connection(err.to_string()))?;
    let mut cur = Cursor::new(len_buf);
    let body_len_i32 = read_i32(&mut cur)?;
    if body_len_i32 < 0 {
        return Err(IgniteError::from("Negative handshake response length"));
    }

    let mut body = vec![0u8; body_len_i32 as usize];
    conn.read_exact(&mut body)
        .await
        .map_err(|err| IgniteError::connection(err.to_string()))?;
    let mut rdr = Cursor::new(&body);

    match read_u8(&mut rdr)? {
        1 => {
            let features = if version_supports_bitmap_features(V_MAJOR, V_MINOR, V_PATCH) {
                read_typed_byte_array(&mut rdr)?
            } else {
                Vec::new()
            };
            let capabilities = ConnectionCapabilities {
                partition_awareness: version_supports_partition_awareness(
                    V_MAJOR, V_MINOR, V_PATCH,
                ),
                node_endpoints: feature_supported(&features, FEATURE_NODE_ENDPOINTS),
                heartbeat: feature_supported(&features, FEATURE_HEARTBEAT),
                dc_aware: feature_supported(&features, FEATURE_DC_AWARE),
                query_partitions_batch_size: feature_supported(
                    &features,
                    FEATURE_QRY_PARTITIONS_BATCH_SIZE,
                ),
                query_initiator_id: feature_supported(&features, FEATURE_QRY_INITIATOR_ID),
            };
            let server_node_id = if capabilities.partition_awareness {
                Some(read_typed_uuid_string(&mut rdr)?)
            } else {
                None
            };

            Ok(ConnectionMetadata {
                capabilities,
                server_node_id,
            })
        }
        _ => {
            let major_v = read_i16(&mut rdr)?;
            let minor_v = read_i16(&mut rdr)?;
            let patch_v = read_i16(&mut rdr)?;
            let err_msg = read_typed_string(&mut rdr).map_err(IgniteError::from)?;
            let err_code = if rdr.position() < body.len() as u64 {
                Some(read_i32(&mut rdr).map_err(IgniteError::from)?)
            } else {
                None
            };
            Err(classify_handshake_error(
                match err_code {
                    Some(code) => format!(
                        "Handshake error: v{}.{}.{} err: {} (code {})",
                        major_v, minor_v, patch_v, err_msg, code
                    ),
                    None => format!(
                        "Handshake error: v{}.{}.{} err: {}",
                        major_v, minor_v, patch_v, err_msg
                    ),
                },
                err_code,
            ))
        }
    }
}

fn build_handshake_request(conf: &ClientConfig) -> IgniteResult<Vec<u8>> {
    use crate::protocol::{write_i16, write_u8};

    let mut buf = Vec::with_capacity(64);
    write_i32(&mut buf, 0)?;
    write_u8(&mut buf, HANDSHAKE_OP_CODE)?;
    write_i16(&mut buf, V_MAJOR)?;
    write_i16(&mut buf, V_MINOR)?;
    write_i16(&mut buf, V_PATCH)?;
    write_u8(&mut buf, CLIENT_CODE)?;
    write_typed_byte_array(&mut buf, &handshake_features(conf))?;

    if !conf.user_attributes.is_empty() {
        write_user_attributes(&mut buf, &conf.user_attributes)?;
    }

    if let Some(ref user) = conf.username {
        write_typed_string(&mut buf, user)?;
        write_typed_string(&mut buf, conf.password.as_deref().unwrap_or_default())?;
    }

    let payload_len = (buf.len() - 4) as i32;
    buf[..4].copy_from_slice(&payload_len.to_le_bytes());
    Ok(buf)
}

fn classify_handshake_error(message: String, err_code: Option<i32>) -> IgniteError {
    let lower = message.to_ascii_lowercase();
    if matches!(
        err_code,
        Some(STATUS_SECURITY_VIOLATION | STATUS_AUTH_FAILED)
    ) {
        return IgniteError::authentication(message);
    }

    if lower.contains("auth") || lower.contains("credential") {
        return IgniteError::authentication(message);
    }

    if matches!(err_code, Some(1)) && lower.contains("user") {
        return IgniteError::authentication(message);
    }

    IgniteError::handshake(message)
}

fn handshake_features(conf: &ClientConfig) -> Vec<u8> {
    let mut features = Vec::new();
    if !conf.user_attributes.is_empty() {
        set_feature_bit(&mut features, FEATURE_USER_ATTRIBUTES);
    }
    set_feature_bit(&mut features, FEATURE_NODE_ENDPOINTS);
    set_feature_bit(&mut features, FEATURE_QRY_PARTITIONS_BATCH_SIZE);
    set_feature_bit(&mut features, FEATURE_HEARTBEAT);
    set_feature_bit(&mut features, FEATURE_DC_AWARE);
    set_feature_bit(&mut features, FEATURE_QRY_INITIATOR_ID);
    features
}

fn write_user_attributes(
    writer: &mut Vec<u8>,
    attributes: &BTreeMap<String, String>,
) -> io::Result<()> {
    write_i32(writer, attributes.len() as i32)?;
    for (key, value) in attributes {
        crate::protocol::write_string_type_code(writer, key)?;
        crate::protocol::write_string_type_code(writer, value)?;
    }
    Ok(())
}

fn set_feature_bit(features: &mut Vec<u8>, bit: usize) {
    let byte_index = bit / 8;
    if features.len() <= byte_index {
        features.resize(byte_index + 1, 0);
    }
    features[byte_index] |= 1 << (bit % 8);
}

fn feature_supported(features: &[u8], bit: usize) -> bool {
    let byte_index = bit / 8;
    features
        .get(byte_index)
        .map(|byte| (byte & (1 << (bit % 8))) != 0)
        .unwrap_or(false)
}

fn version_supports_partition_awareness(major: i16, minor: i16, patch: i16) -> bool {
    (major, minor, patch) >= PARTITION_AWARENESS_VERSION
}

fn version_supports_bitmap_features(major: i16, minor: i16, patch: i16) -> bool {
    (major, minor, patch) >= BITMAP_FEATURES_VERSION
}

fn write_typed_byte_array(writer: &mut Vec<u8>, value: &[u8]) -> io::Result<()> {
    crate::protocol::write_u8(writer, TypeCode::ArrByte as u8)?;
    write_i32(writer, value.len() as i32)?;
    writer.extend_from_slice(value);
    Ok(())
}

fn read_typed_byte_array(reader: &mut impl io::Read) -> IgniteResult<Vec<u8>> {
    let type_code =
        TypeCode::try_from(crate::protocol::read_u8(reader).map_err(IgniteError::from)?)
            .map_err(IgniteError::from)?;
    if type_code == TypeCode::Null {
        return Ok(Vec::new());
    }
    if type_code != TypeCode::ArrByte {
        return Err(IgniteError::handshake(format!(
            "Expected byte array in handshake, got {:?}",
            type_code
        )));
    }

    let len = read_i32(reader)?;
    if len < 0 {
        return Err(IgniteError::from("Negative byte array length"));
    }

    let mut value = vec![0u8; len as usize];
    reader.read_exact(&mut value).map_err(IgniteError::from)?;
    Ok(value)
}

fn write_typed_string(writer: &mut Vec<u8>, value: &str) -> io::Result<()> {
    crate::protocol::write_string_type_code(writer, value)
}

fn read_typed_string(reader: &mut impl io::Read) -> io::Result<String> {
    read_typed_string_with_context(reader, "handshake")
}

fn read_typed_string_with_context(reader: &mut impl io::Read, context: &str) -> io::Result<String> {
    let type_code = TypeCode::try_from(crate::protocol::read_u8(reader)?)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))?;

    match type_code {
        TypeCode::String => read_string(reader),
        TypeCode::Null => Ok(String::new()),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("Expected string in {}, got {:?}", context, type_code),
        )),
    }
}

fn read_response_error_string(reader: &mut (impl io::Read + io::Seek)) -> io::Result<String> {
    let start = reader.stream_position()?;
    let type_code = TypeCode::try_from(crate::protocol::read_u8(reader)?)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()));

    match type_code {
        Ok(TypeCode::String) => read_string(reader),
        Ok(TypeCode::Null) => Ok(String::new()),
        Ok(_) | Err(_) => {
            reader.seek(io::SeekFrom::Start(start))?;
            read_string(reader)
        }
    }
}

fn read_typed_uuid_string(reader: &mut impl io::Read) -> IgniteResult<String> {
    let type_code =
        TypeCode::try_from(crate::protocol::read_u8(reader).map_err(IgniteError::from)?)
            .map_err(IgniteError::from)?;

    match type_code {
        TypeCode::Uuid => read_uuid_string(reader),
        TypeCode::Null => Err(IgniteError::handshake(
            "Missing server node id in handshake response",
        )),
        _ => Err(IgniteError::handshake(format!(
            "Expected UUID in handshake, got {:?}",
            type_code
        ))),
    }
}

pub(crate) fn read_uuid_string(reader: &mut impl io::Read) -> IgniteResult<String> {
    let most = read_i64(reader)? as u64;
    let least = read_i64(reader)? as u64;

    Ok(format!(
        "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
        (most >> 32) as u32,
        ((most >> 16) & 0xffff) as u16,
        (most & 0xffff) as u16,
        (least >> 48) as u16,
        least & 0x0000_ffff_ffff_ffff,
    ))
}

#[cfg(test)]
mod tests {
    use super::{
        build_handshake_request, classify_handshake_error, read_response_error_string,
        write_requests, CLIENT_CODE, HANDSHAKE_OP_CODE, STATUS_AUTH_FAILED,
        STATUS_SECURITY_VIOLATION, V_MAJOR, V_MINOR, V_PATCH,
    };
    use crate::error::ErrorKind;
    use crate::protocol::{write_i32, write_string, TypeCode};
    use crate::ClientConfig;
    use std::io::Cursor;
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll};
    use tokio::io::{self, AsyncWrite, ReadBuf};

    #[test]
    fn handshake_request_uses_binary_type_codes_for_features_and_credentials() {
        let mut conf = ClientConfig::new("127.0.0.1:10800");
        conf.username = Some("alice".to_string());
        conf.password = Some("secret".to_string());

        let req = build_handshake_request(&conf).expect("handshake request should serialize");

        assert_eq!(req[4], HANDSHAKE_OP_CODE);
        assert_eq!(i16::from_le_bytes([req[5], req[6]]), V_MAJOR);
        assert_eq!(i16::from_le_bytes([req[7], req[8]]), V_MINOR);
        assert_eq!(i16::from_le_bytes([req[9], req[10]]), V_PATCH);
        assert_eq!(req[11], CLIENT_CODE);
        assert_eq!(req[12], TypeCode::ArrByte as u8);

        let features_len = i32::from_le_bytes([req[13], req[14], req[15], req[16]]);
        assert_eq!(features_len, 3);
        assert_ne!(
            req[17] & (1 << 7),
            0,
            "QRY_PARTITIONS_BATCH_SIZE feature bit should be set"
        );
        assert_ne!(
            req[19] & (1 << 7),
            0,
            "QRY_INITIATOR_ID feature bit should be set"
        );

        let username_pos = 17 + features_len as usize;
        assert_eq!(req[username_pos], TypeCode::String as u8);
        let username_len = i32::from_le_bytes([
            req[username_pos + 1],
            req[username_pos + 2],
            req[username_pos + 3],
            req[username_pos + 4],
        ]);
        assert_eq!(username_len, 5);

        let password_pos = username_pos + 1 + 4 + username_len as usize;
        assert_eq!(req[password_pos], TypeCode::String as u8);
    }

    #[test]
    fn handshake_request_serializes_empty_password_when_only_username_is_configured() {
        let mut conf = ClientConfig::new("127.0.0.1:10800");
        conf.username = Some("alice".to_string());

        let req = build_handshake_request(&conf).expect("handshake request should serialize");
        let features_len = i32::from_le_bytes([req[13], req[14], req[15], req[16]]) as usize;
        let username_pos = 17 + features_len;
        let username_len = i32::from_le_bytes([
            req[username_pos + 1],
            req[username_pos + 2],
            req[username_pos + 3],
            req[username_pos + 4],
        ]) as usize;
        let password_pos = username_pos + 1 + 4 + username_len;

        assert_eq!(req[password_pos], TypeCode::String as u8);
        assert_eq!(
            i32::from_le_bytes([
                req[password_pos + 1],
                req[password_pos + 2],
                req[password_pos + 3],
                req[password_pos + 4],
            ]),
            0
        );
    }

    #[derive(Clone, Default)]
    struct Recorder {
        bytes: Arc<Mutex<Vec<u8>>>,
        flushes: Arc<Mutex<usize>>,
    }

    impl Recorder {
        fn take_bytes(&self) -> Vec<u8> {
            self.bytes.lock().unwrap().clone()
        }

        fn flushes(&self) -> usize {
            *self.flushes.lock().unwrap()
        }
    }

    struct RecordingStream {
        recorder: Recorder,
    }

    impl AsyncWrite for RecordingStream {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<Result<usize, io::Error>> {
            self.recorder.bytes.lock().unwrap().extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
            *self.recorder.flushes.lock().unwrap() += 1;
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), io::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    impl tokio::io::AsyncRead for RecordingStream {
        fn poll_read(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &mut ReadBuf<'_>,
        ) -> Poll<Result<(), io::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn write_request_batch_should_skip_flush_when_disabled() {
        let recorder = Recorder::default();
        let mut stream = RecordingStream {
            recorder: recorder.clone(),
        };

        write_requests(&mut stream, &[b"abc", b"def"], None, false)
            .await
            .unwrap();

        assert_eq!(recorder.take_bytes(), b"abcdef");
        assert_eq!(recorder.flushes(), 0);
    }

    #[tokio::test]
    async fn write_request_batch_should_flush_once_when_enabled() {
        let recorder = Recorder::default();
        let mut stream = RecordingStream {
            recorder: recorder.clone(),
        };

        write_requests(&mut stream, &[b"abc", b"def"], None, true)
            .await
            .unwrap();

        assert_eq!(recorder.take_bytes(), b"abcdef");
        assert_eq!(recorder.flushes(), 1);
    }

    #[test]
    fn response_error_string_reads_typed_ignite_string() {
        let mut bytes = Vec::new();
        bytes.push(TypeCode::String as u8);
        write_string(&mut bytes, "Cache does not exist").unwrap();

        let mut cursor = Cursor::new(bytes);
        let message = read_response_error_string(&mut cursor).unwrap();

        assert_eq!(message, "Cache does not exist");
    }

    #[test]
    fn response_error_string_falls_back_to_legacy_raw_string() {
        let mut bytes = Vec::new();
        write_i32(&mut bytes, 12).unwrap();
        bytes.extend_from_slice(b"legacy error");

        let mut cursor = Cursor::new(bytes);
        let message = read_response_error_string(&mut cursor).unwrap();

        assert_eq!(message, "legacy error");
    }

    #[test]
    fn handshake_error_uses_authentication_kind_for_auth_status_codes() {
        let auth_failed =
            classify_handshake_error("Handshake error".to_string(), Some(STATUS_AUTH_FAILED));
        assert_eq!(auth_failed.kind(), ErrorKind::Authentication);

        let security_violation = classify_handshake_error(
            "Handshake error".to_string(),
            Some(STATUS_SECURITY_VIOLATION),
        );
        assert_eq!(security_violation.kind(), ErrorKind::Authentication);
    }
}

async fn with_timeout_io<T, F>(timeout_dur: Option<Duration>, fut: F) -> IgniteResult<T>
where
    F: Future<Output = io::Result<T>>,
{
    match timeout_dur {
        Some(timeout_dur) => match tokio::time::timeout(timeout_dur, fut).await {
            Ok(result) => result.map_err(|err| IgniteError::connection(err.to_string())),
            Err(_) => Err(IgniteError::connection(format!(
                "Operation timed out after {:?}",
                timeout_dur
            ))),
        },
        None => fut
            .await
            .map_err(|err| IgniteError::connection(err.to_string())),
    }
}

async fn with_timeout_ignite<T, F>(timeout_dur: Option<Duration>, fut: F) -> IgniteResult<T>
where
    F: Future<Output = IgniteResult<T>>,
{
    match timeout_dur {
        Some(timeout_dur) => match tokio::time::timeout(timeout_dur, fut).await {
            Ok(result) => result,
            Err(_) => Err(IgniteError::connection(format!(
                "Operation timed out after {:?}",
                timeout_dur
            ))),
        },
        None => fut.await,
    }
}
