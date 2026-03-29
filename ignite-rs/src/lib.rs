use crate::api::cache_config::{
    CacheCreateWithConfigReq, CacheCreateWithNameReq, CacheDestroyReq, CacheGetConfigReq,
    CacheGetConfigResp, CacheGetNamesReq, CacheGetNamesResp, CacheGetOrCreateWithConfigReq,
    CacheGetOrCreateWithNameReq,
};
use crate::api::key_value::CacheInfo;
use crate::api::OpCode;

use crate::cache::CacheConfiguration;
use crate::cursor::SqlFieldsCursor;
use crate::data_structures::{AtomicConfiguration, CollectionConfiguration};
use crate::error::{ErrorKind, IgniteResult};
use crate::events::{EventBus, EventSubscriptions, LifecycleEventKind};
use crate::exec::TokioExec;
use crate::protocol::{read_wrapped_data, TypeCode};
use crate::query::sql::{SqlFieldsOpenResponse, SqlFieldsQueryRequest};
use crate::query::{SqlFieldsQuery, SqlRow};

use std::collections::BTreeMap;
use std::fmt;
use std::io;
use std::io::{Read, Write};
use std::sync::Arc;

#[cfg(feature = "ssl")]
use rustls;
use std::time::Duration;

mod affinity;
mod api;
pub mod binary;
mod binary_registry;
pub mod cache;
pub mod cluster;
pub mod compute;
mod connection_async;
pub mod cursor;
pub mod data_structures;
pub mod error;
pub mod events;
mod exec;
pub mod invoke;
pub mod protocol;
pub mod query;
pub mod replication;
pub mod services;
pub mod streamer;
mod topology;
mod transport;
pub mod tx;
pub mod utils;

const DEFAULT_THIN_CLIENT_PORT: u16 = 10800;
const DEFAULT_THIN_CLIENT_PORT_RANGE: u16 = 100;

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

#[doc(hidden)]
pub fn register_complex_object_schema(schema: &protocol::complex_obj::ComplexObjectSchema) {
    binary_registry::register_complex_schema(schema);
}

/// Ignite Client configuration.
/// Allows the configuration of user's credentials, tcp configuration
/// and SSL/TLS, if "ssl" feature is enabled
pub trait AddressResolver {
    fn addresses(&self) -> Vec<String>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReconnectThrottle {
    pub window: Duration,
    pub max_attempts: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetryContext {
    pub op_code: i16,
    pub attempt: usize,
    pub address: String,
    pub error_kind: ErrorKind,
    pub error_message: String,
    pub read_only: bool,
}

impl RetryContext {
    pub fn is_read_only(&self) -> bool {
        self.read_only
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetryDecision {
    Retry,
    Stop,
}

pub trait RetryPolicyHandler {
    fn decide(&self, ctx: &RetryContext) -> IgniteResult<RetryDecision>;
}

#[derive(Clone)]
pub struct ClientConfig {
    pub addresses: Vec<String>,
    pub address_resolver: Option<Arc<dyn AddressResolver + Send + Sync>>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub user_attributes: BTreeMap<String, String>,
    pub handshake_timeout: Option<Duration>,
    pub request_timeout: Option<Duration>,
    pub heartbeat_enabled: bool,
    pub heartbeat_interval: Option<Duration>,
    pub partition_awareness_enabled: bool,
    pub retry_policy: RetryPolicy,
    pub retry_limit: usize,
    pub reconnect_throttle: Option<ReconnectThrottle>,
    pub reconnect_backoff: Option<Duration>,
    pub event_subscriptions: EventSubscriptions,
    pub tcp_nodelay: Option<bool>,
    pub tcp_nonblocking: Option<bool>,
    pub tcp_read_timeout: Option<Duration>,
    pub tcp_write_timeout: Option<Duration>,
    pub tcp_ttl: Option<u32>,
    pub tcp_read_buff_size: Option<usize>,
    pub tcp_write_buff_size: Option<usize>,
    pub connection_pool_size: usize,
    #[cfg(feature = "ssl")]
    pub tls_conf: Option<(rustls::ClientConfig, String)>,
}

pub enum RetryPolicy {
    Default,
    Never,
    ReadOnly,
    Custom(Arc<dyn RetryPolicyHandler + Send + Sync>),
}

impl Clone for RetryPolicy {
    fn clone(&self) -> Self {
        match self {
            Self::Default => Self::Default,
            Self::Never => Self::Never,
            Self::ReadOnly => Self::ReadOnly,
            Self::Custom(handler) => Self::Custom(handler.clone()),
        }
    }
}

impl fmt::Debug for RetryPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Default => f.write_str("Default"),
            Self::Never => f.write_str("Never"),
            Self::ReadOnly => f.write_str("ReadOnly"),
            Self::Custom(_) => f.write_str("Custom(<handler>)"),
        }
    }
}

impl PartialEq for RetryPolicy {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Default, Self::Default)
            | (Self::Never, Self::Never)
            | (Self::ReadOnly, Self::ReadOnly) => true,
            (Self::Custom(left), Self::Custom(right)) => Arc::ptr_eq(left, right),
            _ => false,
        }
    }
}

impl Eq for RetryPolicy {}

impl RetryPolicy {
    pub fn custom(handler: Arc<dyn RetryPolicyHandler + Send + Sync>) -> Self {
        Self::Custom(handler)
    }
}

impl fmt::Debug for ClientConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientConfig")
            .field("addresses", &self.addresses)
            .field(
                "address_resolver",
                &self.address_resolver.as_ref().map(|_| "<resolver>"),
            )
            .field("username", &self.username)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .field("user_attributes", &self.user_attributes)
            .field("handshake_timeout", &self.handshake_timeout)
            .field("request_timeout", &self.request_timeout)
            .field("heartbeat_enabled", &self.heartbeat_enabled)
            .field("heartbeat_interval", &self.heartbeat_interval)
            .field(
                "partition_awareness_enabled",
                &self.partition_awareness_enabled,
            )
            .field("retry_policy", &self.retry_policy)
            .field("retry_limit", &self.retry_limit)
            .field("reconnect_throttle", &self.reconnect_throttle)
            .field("reconnect_backoff", &self.reconnect_backoff)
            .field("event_subscriptions", &self.event_subscriptions)
            .field("tcp_nodelay", &self.tcp_nodelay)
            .field("tcp_nonblocking", &self.tcp_nonblocking)
            .field("tcp_read_timeout", &self.tcp_read_timeout)
            .field("tcp_write_timeout", &self.tcp_write_timeout)
            .field("tcp_ttl", &self.tcp_ttl)
            .field("tcp_read_buff_size", &self.tcp_read_buff_size)
            .field("tcp_write_buff_size", &self.tcp_write_buff_size)
            .field("connection_pool_size", &self.connection_pool_size)
            .finish_non_exhaustive()
    }
}

impl ClientConfig {
    pub fn new(addr: &str) -> ClientConfig {
        Self::from_addresses(std::iter::once(addr))
    }

    pub fn from_addresses<I, S>(addresses: I) -> ClientConfig
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        ClientConfig {
            addresses: addresses.into_iter().map(Into::into).collect(),
            address_resolver: None,
            username: None,
            password: None,
            user_attributes: BTreeMap::new(),
            handshake_timeout: None,
            request_timeout: None,
            heartbeat_enabled: false,
            heartbeat_interval: None,
            partition_awareness_enabled: true,
            retry_policy: RetryPolicy::Default,
            retry_limit: 1,
            reconnect_throttle: None,
            reconnect_backoff: None,
            event_subscriptions: EventSubscriptions::default(),
            tcp_nodelay: None,
            tcp_nonblocking: None,
            tcp_read_timeout: None,
            tcp_write_timeout: None,
            tcp_ttl: None,
            tcp_read_buff_size: None,
            tcp_write_buff_size: None,
            connection_pool_size: 1,
            #[cfg(feature = "ssl")]
            tls_conf: None,
        }
    }

    #[cfg(feature = "ssl")]
    pub fn new_tls(
        addr: &str,
        client_conf: rustls::ClientConfig,
        hostname: String,
    ) -> ClientConfig {
        Self::from_addresses_tls(std::iter::once(addr), client_conf, hostname)
    }

    #[cfg(feature = "ssl")]
    pub fn from_addresses_tls<I, S>(
        addresses: I,
        client_conf: rustls::ClientConfig,
        hostname: String,
    ) -> ClientConfig
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut conf = Self::from_addresses(addresses);
        conf.tls_conf = Some((client_conf, hostname));
        conf
    }

    pub(crate) fn validate(&self) -> IgniteResult<()> {
        if self.username.is_none() && self.password.is_some() {
            return Err(crate::error::IgniteError::from(
                "Username expected when password is configured!",
            ));
        }
        if self
            .handshake_timeout
            .map(|timeout| timeout.is_zero())
            .unwrap_or(false)
        {
            return Err(crate::error::IgniteError::from(
                "handshake_timeout cannot be zero",
            ));
        }
        if self
            .request_timeout
            .map(|timeout| timeout.is_zero())
            .unwrap_or(false)
        {
            return Err(crate::error::IgniteError::from(
                "request_timeout cannot be zero",
            ));
        }
        if self
            .heartbeat_interval
            .map(|interval| interval.is_zero())
            .unwrap_or(false)
        {
            return Err(crate::error::IgniteError::from(
                "heartbeat_interval cannot be zero",
            ));
        }
        if self
            .reconnect_throttle
            .map(|throttle| throttle.window.is_zero())
            .unwrap_or(false)
        {
            return Err(crate::error::IgniteError::from(
                "reconnect_throttle.window cannot be zero",
            ));
        }
        if self.address_resolver.is_none() {
            let normalized = self.normalized_addresses()?;
            if normalized.is_empty() {
                return Err(crate::error::IgniteError::from(
                    "At least one address expected",
                ));
            }
        }
        Ok(())
    }

    pub fn with_connection_pool_size(mut self, size: usize) -> Self {
        self.connection_pool_size = size.max(1);
        self
    }

    pub(crate) fn normalized_addresses(&self) -> IgniteResult<Vec<String>> {
        let mut normalized = Vec::new();

        for address in self.resolved_addresses()? {
            normalized.extend(parse_address_spec(&address)?);
        }

        Ok(normalized)
    }

    fn resolved_addresses(&self) -> IgniteResult<Vec<String>> {
        let addresses = match &self.address_resolver {
            Some(resolver) => resolver.addresses(),
            None => self.addresses.clone(),
        };

        if addresses.iter().any(|address| address.trim().is_empty()) {
            return Err(crate::error::IgniteError::from(
                "At least one non-empty address expected",
            ));
        }

        Ok(addresses)
    }
}

fn parse_address_spec(spec: &str) -> IgniteResult<Vec<String>> {
    let spec = spec.trim();
    if spec.is_empty() {
        return Err(crate::error::IgniteError::from(
            "At least one non-empty address expected",
        ));
    }

    let (host, port_spec) = split_host_and_port_spec(spec)?;
    let ports = parse_ports(port_spec.as_deref())?;

    Ok(ports
        .into_iter()
        .map(|port| format_endpoint(&host, port))
        .collect())
}

fn split_host_and_port_spec(spec: &str) -> IgniteResult<(String, Option<String>)> {
    if let Some(rest) = spec.strip_prefix('[') {
        let closing = rest.find(']').ok_or_else(|| {
            crate::error::IgniteError::new(format!("Invalid bracketed address: {}", spec))
        })?;
        let host = &rest[..closing];
        let tail = &rest[(closing + 1)..];
        let port_spec = if tail.is_empty() {
            None
        } else if let Some(port_spec) = tail.strip_prefix(':') {
            Some(port_spec.to_string())
        } else {
            return Err(crate::error::IgniteError::new(format!(
                "Invalid bracketed address: {}",
                spec
            )));
        };
        return Ok((host.to_string(), port_spec));
    }

    let colon_count = spec.bytes().filter(|ch| *ch == b':').count();
    if colon_count == 0 {
        return Ok((spec.to_string(), None));
    }

    if colon_count > 1 {
        return Err(crate::error::IgniteError::new(format!(
            "IPv6 addresses must be bracketed: {}",
            spec
        )));
    }

    let (host, port_spec) = spec
        .rsplit_once(':')
        .ok_or_else(|| crate::error::IgniteError::new(format!("Invalid address: {}", spec)))?;

    if host.trim().is_empty() {
        return Err(crate::error::IgniteError::new(format!(
            "Invalid address: {}",
            spec
        )));
    }

    Ok((host.to_string(), Some(port_spec.to_string())))
}

fn parse_ports(port_spec: Option<&str>) -> IgniteResult<Vec<u16>> {
    match port_spec {
        None => Ok((DEFAULT_THIN_CLIENT_PORT
            ..=DEFAULT_THIN_CLIENT_PORT + DEFAULT_THIN_CLIENT_PORT_RANGE)
            .collect()),
        Some(port_spec) if port_spec.contains("..") => {
            let (start, end) = port_spec.split_once("..").ok_or_else(|| {
                crate::error::IgniteError::new(format!("Invalid port range: {}", port_spec))
            })?;
            let start = parse_port(start)?;
            let end = parse_port(end)?;
            if start > end {
                return Err(crate::error::IgniteError::new(format!(
                    "Invalid port range: {}",
                    port_spec
                )));
            }
            Ok((start..=end).collect())
        }
        Some(port_spec) => Ok(vec![parse_port(port_spec)?]),
    }
}

fn parse_port(port_spec: &str) -> IgniteResult<u16> {
    port_spec
        .parse::<u16>()
        .map_err(|_| crate::error::IgniteError::new(format!("Invalid port: {}", port_spec)))
}

fn format_endpoint(host: &str, port: u16) -> String {
    if host.contains(':') {
        format!("[{}]:{}", host, port)
    } else {
        format!("{}:{}", host, port)
    }
}

#[cfg(feature = "ssl")]
#[derive(Clone, Debug, Default)]
pub struct RustlsTlsOptions {
    pub protocols: Option<Vec<String>>,
    pub cipher_suites: Option<Vec<String>>,
}

#[cfg(feature = "ssl")]
impl RustlsTlsOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_protocols<I, S>(mut self, protocols: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.protocols = Some(protocols.into_iter().map(Into::into).collect());
        self
    }

    pub fn with_cipher_suites<I, S>(mut self, cipher_suites: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.cipher_suites = Some(cipher_suites.into_iter().map(Into::into).collect());
        self
    }
}

#[cfg(feature = "ssl")]
fn load_root_store_from_pem(
    ca_pem_path: &str,
) -> crate::error::IgniteResult<rustls::RootCertStore> {
    use std::fs::File;
    use std::io::BufReader;

    let mut root = rustls::RootCertStore::empty();
    let mut reader = BufReader::new(File::open(ca_pem_path)?);
    let certs = rustls_pemfile::certs(&mut reader)
        .map_err(|_| crate::error::IgniteError::tls("invalid CA PEM file"))?;
    if certs.is_empty() {
        return Err(crate::error::IgniteError::tls(
            "no CA certs found in PEM file",
        ));
    }
    for cert in certs {
        let _ = root.add(cert.into());
    }
    Ok(root)
}

#[cfg(feature = "ssl")]
fn parse_rustls_protocol_selector(
    selector: &str,
) -> crate::error::IgniteResult<&'static rustls::SupportedProtocolVersion> {
    match selector {
        "TLSv1.2" => Ok(&rustls::version::TLS12),
        "TLSv1.3" => Ok(&rustls::version::TLS13),
        unsupported if unsupported.eq_ignore_ascii_case("TLS") => {
            Err(crate::error::IgniteError::tls(
                "The generic TLS selector is ambiguous for rustls; use TLSv1.2 or TLSv1.3",
            ))
        }
        unsupported => Err(crate::error::IgniteError::tls(format!(
            "Unsupported rustls protocol selector: {}",
            unsupported
        ))),
    }
}

#[cfg(feature = "ssl")]
fn parse_rustls_cipher_suite_selector(
    selector: &str,
) -> crate::error::IgniteResult<rustls::SupportedCipherSuite> {
    use rustls::crypto::ring::cipher_suite;

    match selector {
        "TLS13_AES_128_GCM_SHA256" => Ok(cipher_suite::TLS13_AES_128_GCM_SHA256),
        "TLS13_AES_256_GCM_SHA384" => Ok(cipher_suite::TLS13_AES_256_GCM_SHA384),
        "TLS13_CHACHA20_POLY1305_SHA256" => Ok(cipher_suite::TLS13_CHACHA20_POLY1305_SHA256),
        "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256" => {
            Ok(cipher_suite::TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256)
        }
        "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384" => {
            Ok(cipher_suite::TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384)
        }
        "TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256" => {
            Ok(cipher_suite::TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256)
        }
        "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256" => {
            Ok(cipher_suite::TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256)
        }
        "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384" => {
            Ok(cipher_suite::TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384)
        }
        "TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256" => {
            Ok(cipher_suite::TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256)
        }
        unsupported => Err(crate::error::IgniteError::tls(format!(
            "Unsupported rustls cipher suite selector: {}",
            unsupported
        ))),
    }
}

#[cfg(feature = "ssl")]
fn build_client_tls_config(
    root: rustls::RootCertStore,
    options: Option<&RustlsTlsOptions>,
) -> crate::error::IgniteResult<rustls::ClientConfig> {
    let mut provider = rustls::crypto::ring::default_provider();
    if let Some(cipher_suites) = options.and_then(|opts| opts.cipher_suites.as_ref()) {
        provider.cipher_suites = cipher_suites
            .iter()
            .map(|suite| parse_rustls_cipher_suite_selector(suite))
            .collect::<crate::error::IgniteResult<Vec<_>>>()?;
    }

    let versions = match options.and_then(|opts| opts.protocols.as_ref()) {
        Some(protocols) => protocols
            .iter()
            .map(|protocol| parse_rustls_protocol_selector(protocol))
            .collect::<crate::error::IgniteResult<Vec<_>>>()?,
        None => rustls::DEFAULT_VERSIONS.to_vec(),
    };

    let builder = rustls::ClientConfig::builder_with_provider(std::sync::Arc::new(provider))
        .with_protocol_versions(&versions)
        .map_err(|err| crate::error::IgniteError::tls(err.to_string()))?;

    Ok(builder.with_root_certificates(root).with_no_client_auth())
}

#[cfg(feature = "ssl")]
/// Build a `ClientConfig` suitable for TLS connections using a CA PEM path and SNI hostname.
/// The optional rustls selectors expose rustls-only protocol and cipher-suite controls.
pub fn client_config_from_ca_pem_with_rustls_options(
    addr: &str,
    ca_pem_path: &str,
    sni_hostname: &str,
    options: RustlsTlsOptions,
) -> crate::error::IgniteResult<ClientConfig> {
    let root = load_root_store_from_pem(ca_pem_path)?;
    let tls_conf = build_client_tls_config(root, Some(&options))?;
    Ok(ClientConfig::new_tls(
        addr,
        tls_conf,
        sni_hostname.to_owned(),
    ))
}

#[cfg(feature = "ssl")]
/// Build a `ClientConfig` suitable for TLS connections using a CA PEM path and SNI hostname.
/// This avoids consumers/tests depending directly on `rustls`.
pub fn client_config_from_ca_pem(
    addr: &str,
    ca_pem_path: &str,
    sni_hostname: &str,
) -> crate::error::IgniteResult<ClientConfig> {
    let root = load_root_store_from_pem(ca_pem_path)?;
    let tls_conf = build_client_tls_config(root, None)?;
    Ok(ClientConfig::new_tls(
        addr,
        tls_conf,
        sni_hostname.to_owned(),
    ))
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
        .map_err(|e| crate::error::IgniteError::tls(e.to_string()))?;

    Ok(ClientConfig::new_tls(
        addr,
        tls_conf,
        sni_hostname.to_owned(),
    ))
}

/// Create new Ignite client using provided configuration
/// Returned client has only one TCP connection with cluster
pub async fn new_client(conf: ClientConfig) -> IgniteResult<Client> {
    Client::new(conf).await
}

/// Create a new Ignite client together with an event receiver that is valid
/// even when startup fails before a `Client` handle exists.
pub async fn new_client_with_events(
    conf: ClientConfig,
) -> (
    IgniteResult<Client>,
    tokio::sync::broadcast::Receiver<events::ClientEvent>,
) {
    Client::new_with_events(conf).await
}

#[derive(Clone)]
pub struct ClientEvents {
    exec: TokioExec,
}

impl ClientEvents {
    pub fn subscribe(&self) -> tokio::sync::broadcast::Receiver<events::ClientEvent> {
        self.exec.subscribe_events()
    }
}

/// Ignite Client backed by the async transport runtime.
pub struct ClientGeneric {
    _conf: ClientConfig,
    exec: TokioExec,
    cluster: cluster::Cluster,
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
        Ok(crate::cache::CacheCore::new(
            id,
            Arc::from(name.as_str()),
            exec,
        ))
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
        Ok(crate::cache::CacheCore::new(
            id,
            Arc::from(name.as_str()),
            exec,
        ))
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
        Ok(crate::cache::CacheCore::new(
            id,
            Arc::from(name.as_str()),
            exec,
        ))
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
        Ok(crate::cache::CacheCore::new(
            id,
            Arc::from(name.as_str()),
            exec,
        ))
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
            .await?;
        self.exec
            .invalidate_affinity_cache(crate::utils::string_to_java_hashcode(name))
            .await;
        Ok(())
    }

    async fn sql_fields_impl<Row: SqlRow>(
        &self,
        query: SqlFieldsQuery<Row>,
    ) -> IgniteResult<SqlFieldsCursor<Row>> {
        query.validate()?;
        let capabilities = self.exec.sql_fields_capabilities().await;
        let (open, meta): (SqlFieldsOpenResponse, crate::transport::ResponseMeta) = self
            .exec
            .send_and_read_with_meta(
                OpCode::QuerySqlFields,
                SqlFieldsQueryRequest {
                    cache_info: CacheInfo::new(0).with_keep_binary(true),
                    query: &query,
                    capabilities,
                },
                crate::transport::RequestRoute::default(),
            )
            .await?;
        SqlFieldsCursor::new(
            self.exec.clone(),
            crate::transport::RequestRoute::pinned(meta.address),
            open,
        )
    }

    pub async fn new(conf: ClientConfig) -> IgniteResult<ClientGeneric> {
        let transport = Arc::new(transport::ChannelManager::new(conf.clone()).await?);
        transport.spawn_background_tasks();
        let exec = TokioExec::new(transport);
        Ok(ClientGeneric {
            _conf: conf,
            cluster: cluster::Cluster::new(exec.clone()),
            exec,
        })
    }

    pub async fn new_with_events(
        conf: ClientConfig,
    ) -> (
        IgniteResult<ClientGeneric>,
        tokio::sync::broadcast::Receiver<events::ClientEvent>,
    ) {
        let event_bus = EventBus::new(conf.event_subscriptions.clone());
        let receiver = event_bus.subscribe();

        match transport::ChannelManager::new_with_event_bus(conf.clone(), event_bus.clone()).await {
            Ok(transport) => {
                let transport = Arc::new(transport);
                transport.spawn_background_tasks();
                let exec = TokioExec::new(transport);
                (
                    Ok(ClientGeneric {
                        _conf: conf,
                        cluster: cluster::Cluster::new(exec.clone()),
                        exec,
                    }),
                    receiver,
                )
            }
            Err(err) => {
                event_bus.emit_lifecycle(LifecycleEventKind::Failed, Some(err.to_string()));
                (Err(err), receiver)
            }
        }
    }

    #[allow(dead_code)]
    pub async fn new_async(conf: ClientConfig) -> IgniteResult<ClientGeneric> {
        Self::new(conf).await
    }

    pub async fn get_cache_names(&self) -> IgniteResult<Vec<String>> {
        self.get_cache_names_impl().await
    }

    pub fn cache<K: WritableType + ReadableType, V: WritableType + ReadableType>(
        &self,
        name: &str,
    ) -> crate::cache::Cache<K, V> {
        let id = crate::utils::string_to_java_hashcode(name);
        crate::cache::CacheCore::new(id, Arc::from(name), self.exec.clone())
    }

    /// Create a cache handle with a pre-computed ID to avoid per-call hashing.
    pub fn cache_with_id<K: WritableType + ReadableType, V: WritableType + ReadableType>(
        &self,
        id: i32,
        name: Arc<str>,
    ) -> crate::cache::Cache<K, V> {
        crate::cache::CacheCore::new(id, name, self.exec.clone())
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

    pub async fn sql_fields<Row: SqlRow>(
        &self,
        query: SqlFieldsQuery<Row>,
    ) -> IgniteResult<SqlFieldsCursor<Row>> {
        self.sql_fields_impl(query).await
    }

    pub fn events(&self) -> ClientEvents {
        ClientEvents {
            exec: self.exec.clone(),
        }
    }

    pub fn transactions(&self) -> tx::Transactions {
        tx::Transactions::new(self.exec.clone())
    }

    pub fn binary(&self) -> binary::Binary {
        binary::Binary::new(self.exec.clone())
    }

    pub fn cluster(&self) -> cluster::Cluster {
        self.cluster.clone()
    }

    pub fn services(&self) -> services::Services {
        services::Services::new(self.exec.clone(), Some(self.cluster.for_servers()))
    }

    pub fn compute(&self) -> compute::Compute {
        compute::Compute::new(self.exec.clone(), Some(self.cluster.for_servers()))
    }

    pub async fn atomic_long(
        &self,
        name: &str,
        initial_value: i64,
        create: bool,
    ) -> IgniteResult<Option<data_structures::AtomicLong>> {
        data_structures::AtomicLong::get_or_create(
            self.exec.clone(),
            name,
            None,
            initial_value,
            create,
        )
        .await
    }

    pub async fn atomic_long_with_config(
        &self,
        name: &str,
        config: &AtomicConfiguration,
        initial_value: i64,
        create: bool,
    ) -> IgniteResult<Option<data_structures::AtomicLong>> {
        data_structures::AtomicLong::get_or_create(
            self.exec.clone(),
            name,
            Some(config.clone()),
            initial_value,
            create,
        )
        .await
    }

    pub async fn set<K: WritableType + ReadableType>(
        &self,
        name: &str,
        config: Option<&CollectionConfiguration>,
    ) -> IgniteResult<Option<data_structures::IgniteSet<K>>> {
        data_structures::IgniteSet::get_or_create(self.exec.clone(), name, config.cloned()).await
    }

    /// Create a data streamer for high-throughput bulk loading into a cache.
    pub fn data_streamer<K: WritableType + ReadableType, V: WritableType + ReadableType>(
        &self,
        cache_name: &str,
        config: streamer::DataStreamerConfig,
    ) -> streamer::DataStreamer<K, V> {
        let cache_id = crate::utils::string_to_java_hashcode(cache_name);
        streamer::DataStreamer::new(self.exec.clone(), cache_id, config)
    }
}
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
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

#[cfg(test)]
mod tests {
    use super::{ClientConfig, DEFAULT_THIN_CLIENT_PORT, DEFAULT_THIN_CLIENT_PORT_RANGE};

    /// Related Apache Ignite address parsing coverage:
    /// `ReliableChannelTest.testAddressWithoutPort`
    /// <https://github.com/apache/ignite/blob/ignite-2.15.0/modules/core/src/test/java/org/apache/ignite/internal/client/thin/ReliableChannelTest.java#L83-L93>
    #[test]
    fn should_expand_address_without_port_to_default_port_range() {
        let conf = ClientConfig::new("127.0.0.1");
        let addresses = conf.normalized_addresses().unwrap();

        assert_eq!(addresses.len(), DEFAULT_THIN_CLIENT_PORT_RANGE as usize + 1);
        assert_eq!(addresses.first().unwrap(), "127.0.0.1:10800");
        assert_eq!(addresses.last().unwrap(), "127.0.0.1:10900");
    }

    /// Related Apache Ignite address parsing coverage:
    /// `ConnectionTest.testIPv6NodeAddresses`
    /// <https://github.com/apache/ignite/blob/ignite-2.15.0/modules/core/src/test/java/org/apache/ignite/client/ConnectionTest.java#L72-L80>
    #[test]
    fn should_preserve_bracketed_ipv6_addresses() {
        let conf = ClientConfig::new("[::1]:10800");
        let addresses = conf.normalized_addresses().unwrap();

        assert_eq!(addresses, vec!["[::1]:10800".to_string()]);
    }

    #[test]
    fn should_reject_invalid_port_range() {
        let conf = ClientConfig::new("127.0.0.1:10802..10800");
        let err = conf.normalized_addresses().unwrap_err();

        assert!(
            err.to_string().contains("Invalid port range"),
            "unexpected invalid range error: {}",
            err
        );
    }

    #[test]
    fn should_expand_explicit_port_range() {
        let conf = ClientConfig::new("127.0.0.1:10800..10802");
        let addresses = conf.normalized_addresses().unwrap();

        assert_eq!(
            addresses,
            vec![
                format!("127.0.0.1:{}", DEFAULT_THIN_CLIENT_PORT),
                "127.0.0.1:10801".to_string(),
                "127.0.0.1:10802".to_string(),
            ]
        );
    }
}
