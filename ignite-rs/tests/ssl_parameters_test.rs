#![cfg(feature = "ssl")]

use ignite_rs::error::ErrorKind;
use ignite_rs::{client_config_from_ca_pem_with_rustls_options, RustlsTlsOptions};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::ring::{self, cipher_suite};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName, UnixTime};
use rustls::{
    ClientConnection, DigitallySignedStruct, Error, ServerConfig, ServerConnection,
    SignatureScheme, SupportedCipherSuite, SupportedProtocolVersion,
};
use std::convert::TryFrom;
use std::fs::File;
use std::io::{BufReader, Cursor};
use std::sync::Arc;

const TEST_SERVER_NAME: &str = "ignite.apache.org";

/// Java parity: org.apache.ignite.client.SslParametersTest#testSameProtocols
#[test]
fn should_handshake_with_same_tls_protocols() {
    assert_tls_handshake_succeeds(
        RustlsTlsOptions::new().with_protocols(["TLSv1.3", "TLSv1.2"]),
        &[&rustls::version::TLS13, &rustls::version::TLS12],
        vec![cipher_suite::TLS13_AES_128_GCM_SHA256],
    );
}

/// Java parity: org.apache.ignite.client.SslParametersTest#testOneCommonProtocol
#[test]
fn should_handshake_with_one_common_tls_protocol() {
    assert_tls_handshake_succeeds(
        RustlsTlsOptions::new().with_protocols(["TLSv1.3"]),
        &[&rustls::version::TLS12, &rustls::version::TLS13],
        vec![cipher_suite::TLS13_AES_128_GCM_SHA256],
    );
}

/// Java parity: org.apache.ignite.client.SslParametersTest#testNoCommonProtocols
#[test]
fn should_fail_when_no_common_tls_protocol_exists() {
    assert_tls_handshake_fails(
        RustlsTlsOptions::new().with_protocols(["TLSv1.3"]),
        &[&rustls::version::TLS12],
        vec![cipher_suite::TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256],
    );
}

/// Java parity: org.apache.ignite.client.SslParametersTest#testNonExistentProtocol
#[test]
fn should_reject_unknown_tls_protocol_selector() {
    let err = client_config_from_ca_pem_with_rustls_options(
        "127.0.0.1:10800",
        ca_pem_path(),
        TEST_SERVER_NAME,
        RustlsTlsOptions::new().with_protocols(["SSLvDoesNotExist"]),
    )
    .expect_err("expected invalid protocol selector to fail");

    assert_eq!(err.kind(), ErrorKind::Tls);
    assert!(err.to_string().contains("SSLvDoesNotExist"));
}

/// Java parity: org.apache.ignite.client.SslParametersTest#testDeprecatedProtocolThrows
#[test]
fn should_reject_legacy_tls_protocol_selector() {
    let err = client_config_from_ca_pem_with_rustls_options(
        "127.0.0.1:10800",
        ca_pem_path(),
        TEST_SERVER_NAME,
        RustlsTlsOptions::new().with_protocols(["TLSv1.1"]),
    )
    .expect_err("expected unsupported legacy protocol selector to fail");

    assert_eq!(err.kind(), ErrorKind::Tls);
    assert!(err.to_string().contains("TLSv1.1"));
}

/// Java parity: org.apache.ignite.client.SslParametersTest#testSameCipherSuite
#[test]
fn should_handshake_with_same_tls_cipher_suite() {
    assert_tls_handshake_succeeds(
        RustlsTlsOptions::new()
            .with_protocols(["TLSv1.3"])
            .with_cipher_suites(["TLS13_AES_128_GCM_SHA256"]),
        &[&rustls::version::TLS13],
        vec![cipher_suite::TLS13_AES_128_GCM_SHA256],
    );
}

/// Java parity: org.apache.ignite.client.SslParametersTest#testOneCommonCipherSuite
#[test]
fn should_handshake_with_one_common_tls_cipher_suite() {
    assert_tls_handshake_succeeds(
        RustlsTlsOptions::new()
            .with_protocols(["TLSv1.3"])
            .with_cipher_suites(["TLS13_CHACHA20_POLY1305_SHA256", "TLS13_AES_128_GCM_SHA256"]),
        &[&rustls::version::TLS13],
        vec![
            cipher_suite::TLS13_AES_256_GCM_SHA384,
            cipher_suite::TLS13_AES_128_GCM_SHA256,
        ],
    );
}

/// Java parity: org.apache.ignite.client.SslParametersTest#testNoCommonCipherSuite
#[test]
fn should_fail_when_no_common_tls_cipher_suite_exists() {
    assert_tls_handshake_fails(
        RustlsTlsOptions::new()
            .with_protocols(["TLSv1.3"])
            .with_cipher_suites(["TLS13_AES_128_GCM_SHA256"]),
        &[&rustls::version::TLS13],
        vec![cipher_suite::TLS13_AES_256_GCM_SHA384],
    );
}

/// Java parity: org.apache.ignite.client.SslParametersTest#testNonExistentCipherSuite
#[test]
fn should_reject_unknown_tls_cipher_suite_selector() {
    let err = client_config_from_ca_pem_with_rustls_options(
        "127.0.0.1:10800",
        ca_pem_path(),
        TEST_SERVER_NAME,
        RustlsTlsOptions::new().with_cipher_suites(["TLC_FAKE_CIPHER"]),
    )
    .expect_err("expected invalid cipher selector to fail");

    assert_eq!(err.kind(), ErrorKind::Tls);
    assert!(err.to_string().contains("TLC_FAKE_CIPHER"));
}

#[derive(Debug)]
struct NoServerCertVerifier;

impl ServerCertVerifier for NoServerCertVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::ECDSA_NISTP521_SHA512,
            SignatureScheme::ED25519,
        ]
    }
}

fn assert_tls_handshake_succeeds(
    client_options: RustlsTlsOptions,
    server_versions: &[&'static SupportedProtocolVersion],
    server_cipher_suites: Vec<SupportedCipherSuite>,
) {
    let client_config = Arc::new(test_client_tls_config(client_options));
    let server_config = Arc::new(build_server_tls_config(
        server_versions,
        server_cipher_suites,
    ));
    drive_tls_handshake(client_config, server_config).expect("expected TLS handshake success");
}

fn assert_tls_handshake_fails(
    client_options: RustlsTlsOptions,
    server_versions: &[&'static SupportedProtocolVersion],
    server_cipher_suites: Vec<SupportedCipherSuite>,
) {
    let client_config = Arc::new(test_client_tls_config(client_options));
    let server_config = Arc::new(build_server_tls_config(
        server_versions,
        server_cipher_suites,
    ));
    let err = drive_tls_handshake(client_config, server_config)
        .expect_err("expected TLS handshake failure");
    assert!(
        !err.to_string().is_empty(),
        "expected TLS failure to carry a message"
    );
}

fn test_client_tls_config(options: RustlsTlsOptions) -> rustls::ClientConfig {
    let mut conf = client_config_from_ca_pem_with_rustls_options(
        "127.0.0.1:10800",
        ca_pem_path(),
        TEST_SERVER_NAME,
        options,
    )
    .expect("failed to build TLS client config");
    let (mut tls_conf, _) = conf
        .tls_conf
        .take()
        .expect("expected TLS config to be present");
    tls_conf
        .dangerous()
        .set_certificate_verifier(Arc::new(NoServerCertVerifier));
    tls_conf
}

fn build_server_tls_config(
    versions: &[&'static SupportedProtocolVersion],
    cipher_suites: Vec<SupportedCipherSuite>,
) -> ServerConfig {
    let mut provider = ring::default_provider();
    provider.cipher_suites = cipher_suites;
    let (certs, key) = load_server_cert_and_key();

    rustls::ServerConfig::builder_with_provider(Arc::new(provider))
        .with_protocol_versions(versions)
        .expect("invalid server protocol selection")
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .expect("failed to build TLS server cert config")
}

fn load_server_cert_and_key() -> (Vec<CertificateDer<'static>>, PrivateKeyDer<'static>) {
    let mut cert_reader =
        BufReader::new(File::open(server_pem_path()).expect("failed to open TLS server PEM"));
    let certs = rustls_pemfile::certs(&mut cert_reader)
        .expect("failed to parse TLS server certs")
        .into_iter()
        .map(CertificateDer::from)
        .collect::<Vec<_>>();

    let mut key_reader =
        BufReader::new(File::open(server_pem_path()).expect("failed to open TLS server PEM"));
    let key = rustls_pemfile::pkcs8_private_keys(&mut key_reader)
        .expect("failed to parse TLS server key")
        .into_iter()
        .next()
        .map(PrivatePkcs8KeyDer::from)
        .map(PrivateKeyDer::Pkcs8)
        .expect("missing TLS server key");

    (certs, key)
}

fn drive_tls_handshake(
    client_config: Arc<rustls::ClientConfig>,
    server_config: Arc<rustls::ServerConfig>,
) -> Result<(), Error> {
    let server_name =
        ServerName::try_from(TEST_SERVER_NAME.to_string()).expect("invalid test server name");
    let mut client =
        ClientConnection::new(client_config, server_name).expect("failed to create rustls client");
    let mut server = ServerConnection::new(server_config).expect("failed to create rustls server");

    for _ in 0..64 {
        let mut progress = false;
        progress |= pump_client_to_server(&mut client, &mut server)?;
        progress |= pump_server_to_client(&mut server, &mut client)?;

        if !client.is_handshaking() && !server.is_handshaking() {
            return Ok(());
        }

        if !progress {
            break;
        }
    }

    Err(Error::General("TLS handshake did not complete".into()))
}

fn pump_client_to_server(
    client: &mut ClientConnection,
    server: &mut ServerConnection,
) -> Result<bool, Error> {
    let mut out = Vec::new();
    let written = client.write_tls(&mut out).expect("client write_tls failed");
    if written == 0 {
        return Ok(false);
    }

    let mut cursor = Cursor::new(out);
    let _ = server
        .read_tls(&mut cursor)
        .expect("server read_tls failed");
    server.process_new_packets()?;
    Ok(true)
}

fn pump_server_to_client(
    server: &mut ServerConnection,
    client: &mut ClientConnection,
) -> Result<bool, Error> {
    let mut out = Vec::new();
    let written = server.write_tls(&mut out).expect("server write_tls failed");
    if written == 0 {
        return Ok(false);
    }

    let mut cursor = Cursor::new(out);
    let _ = client
        .read_tls(&mut cursor)
        .expect("client read_tls failed");
    client.process_new_packets()?;
    Ok(true)
}

fn ca_pem_path() -> &'static str {
    concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../ignite/modules/platforms/cpp/thin-client-test/config/ssl/ca.pem"
    )
}

fn server_pem_path() -> &'static str {
    concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../ignite/modules/platforms/cpp/thin-client-test/config/ssl/client_full.pem"
    )
}
