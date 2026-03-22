#![cfg(not(feature = "ssl"))]

mod common;

use common::{ignite_scope, unused_local_addr, IgniteProfile};
use ignite_rs::{new_client, ClientConfig};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

/// Java parity: org.apache.ignite.client.ConnectionTest#testValidNodeAddresses
#[tokio::test]
async fn should_connect_to_valid_node_address() {
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let client = new_client(scope.client_config().unwrap()).await.unwrap();

    let _ = client.get_cache_names().await.unwrap();
}

/// Java parity: org.apache.ignite.client.ConnectionTest#testValidInvalidNodeAddressesMix
#[tokio::test]
async fn should_connect_with_mixed_valid_and_invalid_node_addresses() {
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let valid_addr = scope
        .single_env()
        .expect("expected single-node Ignite scope")
        .addr()
        .to_string();
    let dead_addr1 = unused_local_addr();
    let dead_addr2 = unused_local_addr();

    let mut conf = ClientConfig::from_addresses([
        dead_addr1.as_str(),
        dead_addr2.as_str(),
        valid_addr.as_str(),
    ]);
    conf.handshake_timeout = Some(Duration::from_secs(5));
    let client = new_client(conf).await.unwrap();

    let _ = client.get_cache_names().await.unwrap();
}

/// Java parity: org.apache.ignite.client.ConnectionTest#testValidBigHandshakeMessage
#[tokio::test]
async fn should_connect_with_valid_big_handshake_message() {
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let mut conf = scope.client_config().unwrap();
    conf.handshake_timeout = Some(Duration::from_secs(20));
    conf.username = Some("a".repeat(65 * 1024));

    let client = new_client(conf).await.unwrap();
    let _ = client.get_cache_names().await.unwrap();
}

/// Java parity: org.apache.ignite.client.ConnectionTest#testInvalidBigHandshakeMessage
#[tokio::test]
async fn should_fail_on_invalid_big_handshake_message() {
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let mut conf = scope.client_config().unwrap();
    conf.handshake_timeout = Some(Duration::from_secs(20));
    conf.username = Some("a".repeat(1024 * 1024 * 128));

    let err = match new_client(conf).await {
        Ok(_) => panic!("expected oversized handshake payload to fail"),
        Err(err) => err,
    };

    let message = err.to_string();
    let lower = message.to_ascii_lowercase();
    assert!(
        lower.contains("connection")
            || lower.contains("too large")
            || lower.contains("closed")
            || lower.contains("failed")
            || lower.contains("broken pipe"),
        "unexpected error for oversized handshake payload: {}",
        message
    );
}

/// Java parity: org.apache.ignite.client.ConnectionTest#testHandshakeTooLargeServerDropsConnection
#[tokio::test]
async fn should_drop_connection_after_too_large_handshake_message() {
    assert_server_drops_connection(&[1, 1, 1, 1]).await;
}

/// Java parity: org.apache.ignite.client.ConnectionTest#testNegativeMessageSizeDropsConnection
#[tokio::test]
async fn should_drop_connection_after_negative_message_size() {
    assert_server_drops_connection(&[255, 255, 255, 255]).await;
}

/// Java parity: org.apache.ignite.client.ConnectionTest#testInvalidHandshakeHeaderDropsConnection
#[tokio::test]
async fn should_drop_connection_after_invalid_handshake_header() {
    assert_server_drops_connection(&[10, 0, 0, 0, 42, 42, 42]).await;
}

async fn assert_server_drops_connection(payload: &[u8]) {
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope
        .single_env()
        .expect("expected single-node Ignite scope")
        .addr()
        .to_string();
    let mut socket = TcpStream::connect(&addr).expect("failed to connect to live Ignite");
    socket
        .set_read_timeout(Some(Duration::from_millis(250)))
        .expect("failed to configure read timeout");
    socket
        .write_all(payload)
        .expect("failed to write malformed handshake payload");
    socket.flush().expect("failed to flush malformed payload");

    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    let mut byte = [0u8; 1];

    loop {
        match socket.read(&mut byte) {
            Ok(0) => break,
            Ok(n) => panic!("expected EOF after malformed payload, read {} bytes", n),
            Err(err)
                if matches!(
                    err.kind(),
                    std::io::ErrorKind::UnexpectedEof
                        | std::io::ErrorKind::ConnectionReset
                        | std::io::ErrorKind::ConnectionAborted
                        | std::io::ErrorKind::BrokenPipe
                ) =>
            {
                break;
            }
            Err(err)
                if matches!(
                    err.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                ) && std::time::Instant::now() < deadline =>
            {
                continue;
            }
            Err(err) => panic!(
                "expected EOF or connection reset after malformed payload: {}",
                err
            ),
        }
    }
}
