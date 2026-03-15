#![cfg(not(feature = "ssl"))]

mod common;

use common::spawn_dummy_tcp_server;
use ignite_rs::ClientConfig;
use std::time::{Duration, Instant};

const TIMEOUT: Duration = Duration::from_millis(500);

/// Java parity: org.apache.ignite.internal.client.thin.TimeoutTest#testClientTimeoutOnHandshake
#[tokio::test]
async fn should_time_out_while_waiting_for_handshake_response() {
    let (addr, handle) = spawn_dummy_tcp_server();

    let mut conf = ClientConfig::new(&addr);
    conf.handshake_timeout = Some(TIMEOUT);

    let started = Instant::now();
    let err = match ignite_rs::new_client(conf).await {
        Ok(_) => panic!("expected handshake timeout against dummy server"),
        Err(err) => err,
    };
    let elapsed = started.elapsed();
    handle.join().unwrap();
    assert!(
        elapsed >= TIMEOUT && elapsed < TIMEOUT * 4,
        "unexpected handshake timeout window: {:?}",
        elapsed
    );
    assert_timeout_error(&err.to_string(), "handshake");
}

/// Java parity: org.apache.ignite.internal.client.thin.TimeoutTest#testConnectionTimeoutIndependentOfRequest
#[tokio::test]
async fn should_apply_handshake_timeout_even_when_request_timeout_is_large() {
    let (addr, handle) = spawn_dummy_tcp_server();

    let mut conf = ClientConfig::new(&addr);
    conf.handshake_timeout = Some(TIMEOUT);
    conf.request_timeout = Some(Duration::MAX);

    let started = Instant::now();
    let err = match ignite_rs::new_client(conf).await {
        Ok(_) => panic!("expected handshake timeout against dummy server"),
        Err(err) => err,
    };
    let elapsed = started.elapsed();
    handle.join().unwrap();
    assert!(
        elapsed >= TIMEOUT && elapsed < TIMEOUT * 4,
        "unexpected handshake-timeout window with large request timeout: {:?}",
        elapsed
    );
    assert_timeout_error(&err.to_string(), "connection");
}

fn assert_timeout_error(msg: &str, expected_kind: &str) {
    let lower = msg.to_ascii_lowercase();
    assert!(
        lower.contains("timeout") || lower.contains("timed out") || lower.contains(expected_kind),
        "unexpected timeout error for {} path: {}",
        expected_kind,
        msg
    );
}
