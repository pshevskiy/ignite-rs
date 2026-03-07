use crate::api::cache_config::{
    CacheCreateWithConfigReq, CacheCreateWithNameReq, CacheDestroyReq, CacheGetConfigReq,
    CacheGetConfigResp, CacheGetNamesReq, CacheGetNamesResp, CacheGetOrCreateWithConfigReq,
    CacheGetOrCreateWithNameReq,
};
use crate::api::OpCode;

use crate::cache::CacheConfiguration;
use crate::error::IgniteResult;
use crate::exec::TokioExec;
use crate::protocol::{read_wrapped_data, TypeCode};

use std::io;
use std::io::{Read, Write};
use std::sync::Arc;

#[cfg(feature = "ssl")]
use rustls;
use std::time::Duration;

mod api;
pub mod cache;
mod connection_async;
pub mod error;
mod exec;
pub mod protocol;
pub mod utils;

/// Implementations of this trait could be serialized into Ignite byte sequence
/// It is indented to be implemented by structs which represents requests
pub(crate) trait WriteableReq {
    fn write(&self, writer: &mut dyn Write) -> io::Result<()>;
    fn size(&self) -> usize;
}
/// Implementations of this trait could be deserialized from Ignite byte sequence
/// It is indented to be implemented by structs which represents requests. Acts as a closure
/// for response handling
pub(crate) trait ReadableReq: Sized {
    fn read(reader: &mut impl Read) -> IgniteResult<Self>;
}
/// Indicates that a type could be used as cache key/value.
/// Used alongside ReadableType
pub trait WritableType {
    fn write(&self, writer: &mut dyn Write) -> io::Result<()>;
    fn size(&self) -> usize;
}

/// Indicates that a type could be used as cache key/value.
/// Used alongside WritableType
pub trait ReadableType: Sized {
    fn read_unwrapped(type_code: TypeCode, reader: &mut impl Read) -> IgniteResult<Option<Self>>;
    fn read(reader: &mut impl Read) -> IgniteResult<Option<Self>> {
        read_wrapped_data(reader)
    }
}

/// Combines the WritableType and ReadableType crates.
/// Intended to be used in the #[derive(IgniteObj)] attribute to automatically generate
/// serialization/deserialization for the user-defined structs
///
/// use ignite_rs_derive::IgniteObj;
/// #[derive(IgniteObj)]
/// struct MyType {
///     bar: String,
///     foo: i32,
/// }
pub trait IgniteObj: WritableType + ReadableType {}

/// Ignite Client configuration.
/// Allows the configuration of user's credentials, tcp configuration
/// and SSL/TLS, if "ssl" feature is enabled
#[derive(Clone)]
pub struct ClientConfig {
    pub addr: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub tcp_nodelay: Option<bool>,
    pub tcp_nonblocking: Option<bool>,
    pub tcp_read_timeout: Option<Duration>,
    pub tcp_write_timeout: Option<Duration>,
    pub tcp_ttl: Option<u32>,
    pub tcp_read_buff_size: Option<usize>,
    pub tcp_write_buff_size: Option<usize>,
    #[cfg(feature = "ssl")]
    pub tls_conf: (rustls::ClientConfig, String),
}

impl ClientConfig {
    #[cfg(not(feature = "ssl"))]
    pub fn new(addr: &str) -> ClientConfig {
        ClientConfig {
            addr: addr.into(),
            username: None,
            password: None,
            tcp_nodelay: None,
            tcp_nonblocking: None,
            tcp_read_timeout: None,
            tcp_write_timeout: None,
            tcp_ttl: None,
            tcp_read_buff_size: None,
            tcp_write_buff_size: None,
        }
    }

    #[cfg(feature = "ssl")]
    pub fn new(addr: &str, client_conf: rustls::ClientConfig, hostname: String) -> ClientConfig {
        ClientConfig {
            addr: addr.into(),
            username: None,
            password: None,
            tcp_nodelay: None,
            tcp_nonblocking: None,
            tcp_read_timeout: None,
            tcp_write_timeout: None,
            tcp_ttl: None,
            tcp_read_buff_size: None,
            tcp_write_buff_size: None,
            tls_conf: (client_conf, hostname),
        }
    }
}

#[cfg(feature = "ssl")]
/// Build a `ClientConfig` suitable for TLS connections using a CA PEM path and SNI hostname.
/// This avoids consumers/tests depending directly on `rustls`.
pub fn client_config_from_ca_pem(
    addr: &str,
    ca_pem_path: &str,
    sni_hostname: &str,
) -> crate::error::IgniteResult<ClientConfig> {
    use std::fs::File;
    use std::io::BufReader;

    let mut root = rustls::RootCertStore::empty();
    let mut reader = BufReader::new(File::open(ca_pem_path)?);

    // Parse PEM and add all certs to the root store
    let certs = rustls_pemfile::certs(&mut reader)
        .map_err(|_| crate::error::IgniteError::from("invalid CA PEM file"))?;
    if certs.is_empty() {
        return Err(crate::error::IgniteError::from(
            "no CA certs found in PEM file",
        ));
    }
    for cert in certs {
        // add takes ownership of CertificateDer
        let _ = root.add(cert.into());
    }

    let tls_conf = rustls::ClientConfig::builder()
        .with_root_certificates(root)
        .with_no_client_auth();

    Ok(ClientConfig::new(addr, tls_conf, sni_hostname.to_owned()))
}

#[cfg(feature = "ssl")]
/// Build a `ClientConfig` for mutual TLS using CA PEM, client certificate PEM and client key PEM.
/// This avoids consumers/tests depending directly on `rustls`.
pub fn client_config_from_ca_and_client_pem(
    addr: &str,
    ca_pem_path: &str,
    client_cert_pem_path: &str,
    client_key_pem_path: &str,
    sni_hostname: &str,
) -> crate::error::IgniteResult<ClientConfig> {
    use std::fs::File;
    use std::io::BufReader;

    // Root CA
    let mut root = rustls::RootCertStore::empty();
    let mut ca_reader = BufReader::new(File::open(ca_pem_path)?);
    let ca_certs = rustls_pemfile::certs(&mut ca_reader)
        .map_err(|_| crate::error::IgniteError::from("invalid CA PEM file"))?;
    if ca_certs.is_empty() {
        return Err(crate::error::IgniteError::from(
            "no CA certs found in PEM file",
        ));
    }
    for cert in ca_certs {
        let _ = root.add(cert.into());
    }

    // Client certificate chain
    let mut cert_reader = BufReader::new(File::open(client_cert_pem_path)?);
    let client_certs_bytes = rustls_pemfile::certs(&mut cert_reader)
        .map_err(|_| crate::error::IgniteError::from("invalid client cert PEM file"))?;
    if client_certs_bytes.is_empty() {
        return Err(crate::error::IgniteError::from(
            "no client certs found in client PEM file",
        ));
    }
    let client_certs: Vec<rustls::pki_types::CertificateDer<'static>> =
        client_certs_bytes.into_iter().map(|c| c.into()).collect();

    // Client private key (PKCS#8)
    let client_key = {
        use rustls::pki_types::PrivatePkcs8KeyDer;
        let mut key_reader = BufReader::new(File::open(client_key_pem_path)?);
        let keys = rustls_pemfile::pkcs8_private_keys(&mut key_reader)
            .map_err(|_| crate::error::IgniteError::from("invalid client key PEM (pkcs8)"))?;
        if keys.is_empty() {
            return Err(crate::error::IgniteError::from(
                "no PKCS#8 private keys found in client key PEM file",
            ));
        }
        rustls::pki_types::PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(keys[0].clone()))
    };

    let tls_conf = rustls::ClientConfig::builder()
        .with_root_certificates(root)
        .with_client_auth_cert(client_certs, client_key)
        .map_err(|e| crate::error::IgniteError {
            desc: e.to_string(),
        })?;

    Ok(ClientConfig::new(addr, tls_conf, sni_hostname.to_owned()))
}

/// Create new Ignite client using provided configuration
/// Returned client has only one TCP connection with cluster
pub async fn new_client(conf: ClientConfig) -> IgniteResult<Client> {
    Client::new(conf).await
}

/// Ignite Client backed by a single async connection.
pub struct ClientGeneric {
    _conf: ClientConfig,
    exec: TokioExec,
}

impl ClientGeneric {
    async fn get_cache_names_impl(&self) -> IgniteResult<Vec<String>> {
        let resp = self
            .exec
            .send_and_read::<CacheGetNamesResp>(OpCode::CacheGetNames, CacheGetNamesReq {})
            .await?;
        Ok(resp.names)
    }

    async fn create_cache_impl<K: WritableType + ReadableType, V: WritableType + ReadableType>(
        &self,
        name: &str,
    ) -> IgniteResult<crate::cache::CacheCore<K, V>> {
        self.exec
            .send(
                OpCode::CacheCreateWithName,
                CacheCreateWithNameReq::from(name),
            )
            .await?;
        let id = crate::utils::string_to_java_hashcode(name);
        let name = name.to_owned();
        let exec = self.exec.clone();
        Ok(crate::cache::CacheCore::new(id, name, exec))
    }

    async fn get_or_create_cache_impl<
        K: WritableType + ReadableType,
        V: WritableType + ReadableType,
    >(
        &self,
        name: &str,
    ) -> IgniteResult<crate::cache::CacheCore<K, V>> {
        self.exec
            .send(
                OpCode::CacheGetOrCreateWithName,
                CacheGetOrCreateWithNameReq::from(name),
            )
            .await?;
        let id = crate::utils::string_to_java_hashcode(name);
        let name = name.to_owned();
        let exec = self.exec.clone();
        Ok(crate::cache::CacheCore::new(id, name, exec))
    }

    async fn create_cache_with_config_impl<
        K: WritableType + ReadableType,
        V: WritableType + ReadableType,
    >(
        &self,
        config: &CacheConfiguration,
    ) -> IgniteResult<crate::cache::CacheCore<K, V>> {
        self.exec
            .send(
                OpCode::CacheCreateWithConfiguration,
                CacheCreateWithConfigReq { config },
            )
            .await?;
        let id = crate::utils::string_to_java_hashcode(config.name.as_str());
        let name = config.name.clone();
        let exec = self.exec.clone();
        Ok(crate::cache::CacheCore::new(id, name, exec))
    }

    async fn get_or_create_cache_with_config_impl<
        K: WritableType + ReadableType,
        V: WritableType + ReadableType,
    >(
        &self,
        config: &CacheConfiguration,
    ) -> IgniteResult<crate::cache::CacheCore<K, V>> {
        self.exec
            .send(
                OpCode::CacheGetOrCreateWithConfiguration,
                CacheGetOrCreateWithConfigReq { config },
            )
            .await?;
        let id = crate::utils::string_to_java_hashcode(config.name.as_str());
        let name = config.name.clone();
        let exec = self.exec.clone();
        Ok(crate::cache::CacheCore::new(id, name, exec))
    }

    async fn get_cache_config_impl(&self, name: &str) -> IgniteResult<CacheConfiguration> {
        let resp = self
            .exec
            .send_and_read::<CacheGetConfigResp>(
                OpCode::CacheGetConfiguration,
                CacheGetConfigReq::from(name),
            )
            .await?;
        Ok(resp.config)
    }

    async fn destroy_cache_impl(&self, name: &str) -> IgniteResult<()> {
        self.exec
            .send(OpCode::CacheDestroy, CacheDestroyReq::from(name))
            .await
    }

    pub async fn new(conf: ClientConfig) -> IgniteResult<ClientGeneric> {
        let conn = Arc::new(connection_async::AsyncConnection::new(&conf).await?);
        Ok(ClientGeneric {
            _conf: conf,
            exec: TokioExec::new(conn),
        })
    }

    #[allow(dead_code)]
    pub async fn new_async(conf: ClientConfig) -> IgniteResult<ClientGeneric> {
        Self::new(conf).await
    }

    pub async fn get_cache_names(&self) -> IgniteResult<Vec<String>> {
        self.get_cache_names_impl().await
    }

    pub async fn create_cache<K: WritableType + ReadableType, V: WritableType + ReadableType>(
        &self,
        name: &str,
    ) -> IgniteResult<crate::cache::Cache<K, V>> {
        self.create_cache_impl::<K, V>(name).await
    }

    pub async fn get_or_create_cache<
        K: WritableType + ReadableType,
        V: WritableType + ReadableType,
    >(
        &self,
        name: &str,
    ) -> IgniteResult<crate::cache::Cache<K, V>> {
        self.get_or_create_cache_impl::<K, V>(name).await
    }

    pub async fn create_cache_with_config<
        K: WritableType + ReadableType,
        V: WritableType + ReadableType,
    >(
        &self,
        config: &CacheConfiguration,
    ) -> IgniteResult<crate::cache::Cache<K, V>> {
        self.create_cache_with_config_impl::<K, V>(config).await
    }

    pub async fn get_or_create_cache_with_config<
        K: WritableType + ReadableType,
        V: WritableType + ReadableType,
    >(
        &self,
        config: &CacheConfiguration,
    ) -> IgniteResult<crate::cache::Cache<K, V>> {
        self.get_or_create_cache_with_config_impl::<K, V>(config)
            .await
    }

    pub async fn get_cache_config(&self, name: &str) -> IgniteResult<CacheConfiguration> {
        self.get_cache_config_impl(name).await
    }

    pub async fn destroy_cache(&self, name: &str) -> IgniteResult<()> {
        self.destroy_cache_impl(name).await
    }
}
#[derive(Debug, Copy, Clone)]
#[allow(dead_code)]
///Value of an enumerable type. For such types defined only a finite number of named values.
pub struct Enum {
    /// Type id.
    pub type_id: i32,
    /// Enumeration value ordinal.
    pub ordinal: i32,
}
pub type Client = ClientGeneric;
pub type AsyncClient = ClientGeneric;
