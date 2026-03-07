use crate::api::OpCode;
use crate::error::{IgniteError, IgniteResult};
use crate::protocol::Flag::{Failure, Success};
use crate::protocol::{read_i32, read_i64, write_i16, write_i32, write_i64, Flag};
use crate::{ClientConfig, ReadableReq, ReadableType, WriteableReq};
#[cfg(feature = "ssl")]
use std::convert::TryFrom;
use std::io;
use std::io::{Cursor, Write};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

const REQ_HEADER_SIZE_BYTES: i32 = 10;

pub(crate) struct AsyncConnection {
    #[cfg(not(feature = "ssl"))]
    stream: Mutex<TcpStream>,
    #[cfg(feature = "ssl")]
    stream: Mutex<tokio_rustls::client::TlsStream<TcpStream>>,
}

impl AsyncConnection {
    pub(crate) async fn new(conf: &ClientConfig) -> IgniteResult<AsyncConnection> {
        let tcp = TcpStream::connect(&conf.addr)
            .await
            .map_err(IgniteError::from)?;

        // Apply minimal TCP configuration available on Tokio stream
        if let Some(nodelay) = conf.tcp_nodelay {
            tcp.set_nodelay(nodelay).map_err(IgniteError::from)?;
        }
        if let Some(ttl) = conf.tcp_ttl {
            tcp.set_ttl(ttl).map_err(IgniteError::from)?;
        }

        #[cfg(not(feature = "ssl"))]
        {
            let mut stream = tcp;
            // Perform initial handshake over plain TCP
            handshake_async(&mut stream, conf).await?;
            return Ok(AsyncConnection {
                stream: Mutex::new(stream),
            });
        }

        #[cfg(feature = "ssl")]
        {
            let mut tls_stream = wrap_tls_stream(&conf.tls_conf, tcp).await?;
            // Perform initial handshake over TLS
            handshake_async(&mut tls_stream, conf).await?;
            return Ok(AsyncConnection {
                stream: Mutex::new(tls_stream),
            });
        }
    }

    pub(crate) async fn send(&self, op_code: OpCode, data: impl WriteableReq) -> IgniteResult<()> {
        let mut sock = self.stream.lock().await;
        Self::send_safe(&mut *sock, op_code, data).await
    }

    pub(crate) async fn send_and_read<T: ReadableReq>(
        &self,
        op_code: OpCode,
        data: impl WriteableReq,
    ) -> IgniteResult<T> {
        let mut sock = self.stream.lock().await;
        Self::send_and_read_safe(&mut *sock, op_code, data).await
    }

    async fn send_safe<S, P>(stream: &mut S, op_code: OpCode, payload: P) -> IgniteResult<()>
    where
        S: AsyncRead + AsyncWrite + Unpin,
        P: WriteableReq,
    {
        // Build request into a Vec to minimize syscalls
        let mut buf =
            Vec::with_capacity((payload.size() + (REQ_HEADER_SIZE_BYTES as usize)) as usize);
        Self::write_req_header(&mut buf, payload.size(), op_code as i16)?;
        payload.write(&mut buf)?;

        // Write request
        stream.write_all(&buf).await.map_err(IgniteError::from)?;
        stream.flush().await.map_err(IgniteError::from)?;

        // Read response header (size + corr id + status)
        let (flag, _body) = Self::read_resp_header(stream).await?;
        match flag {
            Success => Ok(()),
            Failure { err_msg } => Err(IgniteError::from(err_msg.as_str())),
        }
    }

    async fn send_and_read_safe<T, S, P>(
        stream: &mut S,
        op_code: OpCode,
        payload: P,
    ) -> IgniteResult<T>
    where
        T: ReadableReq,
        S: AsyncRead + AsyncWrite + Unpin,
        P: WriteableReq,
    {
        // Build request into a Vec to minimize syscalls
        let mut buf =
            Vec::with_capacity((payload.size() + (REQ_HEADER_SIZE_BYTES as usize)) as usize);
        Self::write_req_header(&mut buf, payload.size(), op_code as i16)?;
        payload.write(&mut buf)?;

        // Write request
        stream.write_all(&buf).await.map_err(IgniteError::from)?;
        stream.flush().await.map_err(IgniteError::from)?;

        // Read response header and body
        let (flag, body) = Self::read_resp_header(stream).await?;
        match flag {
            Success => {
                // Remaining body contains response payload for T
                let mut cur = Cursor::new(body);
                T::read(&mut cur)
            }
            Failure { err_msg } => Err(IgniteError::from(err_msg.as_str())),
        }
    }

    fn write_req_header(
        writer: &mut dyn Write,
        payload_len: usize,
        op_code: i16,
    ) -> io::Result<()> {
        write_i32(writer, payload_len as i32 + REQ_HEADER_SIZE_BYTES)?;
        write_i16(writer, op_code)?;
        write_i64(writer, 0)?;
        Ok(())
    }

    async fn read_resp_header<S>(stream: &mut S) -> IgniteResult<(Flag, Vec<u8>)>
    where
        S: AsyncRead + Unpin,
    {
        // Read message length
        let mut len_buf = [0u8; 4];
        stream
            .read_exact(&mut len_buf)
            .await
            .map_err(IgniteError::from)?;
        let mut cur = Cursor::new(len_buf);
        let body_len_i32 = read_i32(&mut cur)?;
        if body_len_i32 < 0 {
            return Err(IgniteError::from("Negative response body length"));
        }
        let body_len = body_len_i32 as usize;

        // Read full body into a buffer
        let mut body = vec![0u8; body_len];
        stream
            .read_exact(&mut body)
            .await
            .map_err(IgniteError::from)?;

        // Parse corr id + status
        let mut rdr = Cursor::new(&body);
        let _ = read_i64(&mut rdr)?; // correlation id, ignored for now
        match read_i32(&mut rdr)? {
            0 => Ok((Success, body.split_off(rdr.position() as usize))),
            _ => {
                let err_msg = String::read(&mut rdr)?
                    .unwrap_or_else(|| "Ignite server returned an empty error message".to_string());
                Ok((Failure { err_msg }, Vec::new()))
            }
        }
    }
}

#[cfg(feature = "ssl")]
async fn wrap_tls_stream(
    conf: &(rustls::ClientConfig, String),
    stream: TcpStream,
) -> IgniteResult<tokio_rustls::client::TlsStream<TcpStream>> {
    use std::sync::Arc;
    let connector = tokio_rustls::TlsConnector::from(Arc::new(conf.0.clone()));
    let server_name =
        rustls::pki_types::ServerName::try_from(conf.1.clone()).map_err(|e| IgniteError {
            desc: e.to_string(),
        })?;
    let tls_stream = connector
        .connect(server_name, stream)
        .await
        .map_err(IgniteError::from)?;
    Ok(tls_stream)
}

async fn handshake_async<S>(conn: &mut S, conf: &ClientConfig) -> IgniteResult<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    use crate::protocol::{write_string_type_code, write_u8};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    const MIN_HANDSHAKE_SIZE: usize = 8;
    const CLIENT_CODE: u8 = 2;
    const V_MAJOR: i16 = 1;
    const V_MINOR: i16 = 2;
    const V_PATCH: i16 = 0;

    let mut msg_size = MIN_HANDSHAKE_SIZE;

    if conf.username.is_some() != conf.password.is_some() {
        return Err(IgniteError::from("Both username and password expected!"));
    }

    if let Some(ref user) = conf.username {
        msg_size += user.len() + 4 + 1; // string itself, len, type code
    }
    if let Some(ref pass) = conf.password {
        msg_size += pass.len() + 4 + 1; // string itself, len, type code
    }

    // Build handshake request into Vec<u8> using existing writers
    let mut buf = Vec::with_capacity(msg_size + 4);
    write_i32(&mut buf, msg_size as i32)?;
    write_u8(&mut buf, OpCode::Handshake as u8)?;
    write_i16(&mut buf, V_MAJOR)?;
    write_i16(&mut buf, V_MINOR)?;
    write_i16(&mut buf, V_PATCH)?;
    write_u8(&mut buf, CLIENT_CODE)?;

    if let Some(ref user) = conf.username {
        write_string_type_code(&mut buf, user)?;
    }
    if let Some(ref pass) = conf.password {
        write_string_type_code(&mut buf, pass)?;
    }

    // Send and flush
    conn.write_all(&buf).await.map_err(IgniteError::from)?;
    conn.flush().await.map_err(IgniteError::from)?;

    // Read response len
    let mut len_buf = [0u8; 4];
    conn.read_exact(&mut len_buf)
        .await
        .map_err(IgniteError::from)?;
    let mut cur = Cursor::new(len_buf);
    let body_len_i32 = read_i32(&mut cur)?;
    if body_len_i32 < 0 {
        return Err(IgniteError::from("Negative handshake response length"));
    }
    let body_len = body_len_i32 as usize;

    // Read body
    let mut body = vec![0u8; body_len];
    conn.read_exact(&mut body)
        .await
        .map_err(IgniteError::from)?;
    let mut rdr = Cursor::new(&body);

    // First byte is success flag
    use crate::protocol::read_u8;
    match read_u8(&mut rdr)? {
        1 => Ok(()),
        _ => {
            // On error, payload is version triplet + wrapped error string
            use crate::protocol::read_i16;
            let major_v = read_i16(&mut rdr)?;
            let minor_v = read_i16(&mut rdr)?;
            let patch_v = read_i16(&mut rdr)?;
            let err_msg = String::read(&mut rdr)?
                .unwrap_or_else(|| "Ignite server returned an empty error message".to_string());
            Err(IgniteError::from(
                format!(
                    "Handshake error: v{}.{}.{} err: {}",
                    major_v, minor_v, patch_v, err_msg
                )
                .as_str(),
            ))
        }
    }
}
