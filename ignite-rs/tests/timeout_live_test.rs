#![cfg(not(feature = "ssl"))]

mod common;

use common::{ignite_scope, unique_name, IgniteProfile, IgniteScope};
use ignite_rs::cache::{AtomicityMode, CacheConfiguration};
use ignite_rs::tx::TransactionOptions;
use ignite_rs::ClientConfig;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};
use tokio::sync::Barrier;

const TIMEOUT: Duration = Duration::from_millis(500);

/// Java parity: org.apache.ignite.internal.client.thin.TimeoutTest#testServerClosesThinClientConnectionOnHandshakeTimeout
#[tokio::test]
async fn should_server_close_socket_after_handshake_timeout() {
    let _guard = suite_lock().lock().unwrap_or_else(|err| err.into_inner());
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    wait_for_ready_within(&scope).await;

    let addr = scope
        .client_config()
        .unwrap()
        .addresses
        .into_iter()
        .next()
        .expect("missing single-node address");

    let started = Instant::now();
    let mut stream =
        TcpStream::connect(&addr).expect("failed to connect to Ignite thin client port");
    stream
        .set_read_timeout(Some(TIMEOUT * 2))
        .expect("failed to set read timeout");

    stream
        .write_all(&1000i32.to_le_bytes())
        .expect("failed to write partial handshake frame");
    stream
        .flush()
        .expect("failed to flush partial handshake frame");

    let mut buf = [0u8; 1];
    let read = stream
        .read(&mut buf)
        .expect("expected server to close the socket after handshake timeout");

    let elapsed = started.elapsed();
    assert_eq!(read, 0, "expected EOF after server-side handshake timeout");
    assert!(
        elapsed >= TIMEOUT && elapsed < TIMEOUT * 4,
        "unexpected server handshake-timeout window: {:?}",
        elapsed
    );
}

/// Java parity: org.apache.ignite.internal.client.thin.TimeoutTest#testClientTimeoutOnOperation
#[tokio::test]
async fn should_time_out_blocked_transactional_operation_without_dropping_idle_connection() {
    let _guard = suite_lock().lock().unwrap_or_else(|err| err.into_inner());
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    wait_for_ready_within(&scope).await;

    let bootstrap = scope.connect().await.unwrap();
    let cache_name = unique_name("timeout_tx_operation");
    create_transactional_cache::<i32, i32>(&bootstrap, &cache_name).await;

    let client = Arc::new(new_timeout_client(&scope.client_config().unwrap()).await);
    let tx_cache = client.cache::<i32, i32>(&cache_name);

    tokio::time::sleep(TIMEOUT * 2).await;
    tx_cache.put(&0, &0).await.unwrap();

    let barrier = Arc::new(Barrier::new(2));
    let blocker = tokio::spawn({
        let client = client.clone();
        let cache_name = cache_name.clone();
        let barrier = barrier.clone();
        async move {
            let tx = client
                .transactions()
                .tx_start(TransactionOptions::default())
                .await
                .unwrap();
            let cache = tx.cache::<i32, i32>(&cache_name);
            cache.put(&0, &0).await.unwrap();
            barrier.wait().await;
            barrier.wait().await;
            tx.commit().await.unwrap();
        }
    });

    barrier.wait().await;

    let tx = client
        .transactions()
        .tx_start(TransactionOptions::default())
        .await
        .unwrap();
    let cache = tx.cache::<i32, i32>(&cache_name);

    let started = Instant::now();
    let err = tokio::time::timeout(TIMEOUT * 4, cache.put(&0, &0))
        .await
        .expect("timed out waiting for blocked transactional operation to return")
        .unwrap_err();
    let elapsed = started.elapsed();

    barrier.wait().await;
    tokio::time::timeout(Duration::from_secs(10), blocker)
        .await
        .expect("timed out waiting for blocker transaction to finish")
        .unwrap();

    assert!(
        elapsed >= TIMEOUT && elapsed < TIMEOUT * 4,
        "unexpected operation-timeout window: {:?}",
        elapsed
    );
    assert_timeout_error(&err.to_string(), "operation");
    let _ = bootstrap.destroy_cache(&cache_name).await;
}

/// Java parity: org.apache.ignite.internal.client.thin.TimeoutTest#testRequestTimeoutIndependentOfConnection
#[tokio::test]
async fn should_apply_request_timeout_even_when_handshake_timeout_is_large() {
    let _guard = suite_lock().lock().unwrap_or_else(|err| err.into_inner());
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    wait_for_ready_within(&scope).await;

    let bootstrap = scope.connect().await.unwrap();
    let cache_name = unique_name("timeout_normal_cache");
    let tx_cache_name = unique_name("timeout_tx_cache");
    bootstrap
        .get_or_create_cache::<i32, String>(&cache_name)
        .await
        .unwrap();
    create_transactional_cache::<i32, String>(&bootstrap, &tx_cache_name).await;

    let mut conf = scope.client_config().unwrap();
    conf.handshake_timeout = Some(Duration::MAX);
    conf.request_timeout = Some(TIMEOUT);

    let client = Arc::new(
        tokio::time::timeout(Duration::from_secs(10), ignite_rs::new_client(conf))
            .await
            .expect("timed out creating timeout test client")
            .unwrap(),
    );
    let cache = client.cache::<i32, String>(&cache_name);

    let barrier = Arc::new(Barrier::new(2));
    let blocker = tokio::spawn({
        let client = client.clone();
        let tx_cache_name = tx_cache_name.clone();
        let barrier = barrier.clone();
        async move {
            let tx = client
                .transactions()
                .tx_start(TransactionOptions::default())
                .await
                .unwrap();
            let cache = tx.cache::<i32, String>(&tx_cache_name);
            cache.put(&1, &"blocked".to_string()).await.unwrap();
            barrier.wait().await;
            barrier.wait().await;
            tx.commit().await.unwrap();
        }
    });

    barrier.wait().await;

    let tx = client
        .transactions()
        .tx_start(TransactionOptions::default())
        .await
        .unwrap();
    let blocked_cache = tx.cache::<i32, String>(&tx_cache_name);

    let started = Instant::now();
    let err = tokio::time::timeout(
        TIMEOUT * 4,
        blocked_cache.put(&1, &"should timeout".to_string()),
    )
    .await
    .expect("timed out waiting for blocked request-timeout operation to return")
    .unwrap_err();
    let elapsed = started.elapsed();

    barrier.wait().await;
    tokio::time::timeout(Duration::from_secs(10), blocker)
        .await
        .expect("timed out waiting for blocker transaction to finish")
        .unwrap();

    assert!(
        elapsed >= TIMEOUT && elapsed < TIMEOUT * 4,
        "unexpected request-timeout window with large handshake timeout: {:?}",
        elapsed
    );
    assert_timeout_error(&err.to_string(), "request");

    cache.put(&1, &"still works".to_string()).await.unwrap();
    assert_eq!(
        cache.get(&1).await.unwrap(),
        Some("still works".to_string())
    );

    let _ = bootstrap.destroy_cache(&cache_name).await;
    let _ = bootstrap.destroy_cache(&tx_cache_name).await;
}

async fn new_timeout_client(base_conf: &ClientConfig) -> ignite_rs::Client {
    let mut conf = base_conf.clone();
    conf.handshake_timeout = Some(TIMEOUT);
    conf.request_timeout = Some(TIMEOUT);
    tokio::time::timeout(Duration::from_secs(10), ignite_rs::new_client(conf))
        .await
        .expect("timed out creating timeout test client")
        .unwrap()
}

async fn create_transactional_cache<K, V>(
    client: &ignite_rs::Client,
    cache_name: &str,
) -> ignite_rs::cache::Cache<K, V>
where
    K: ignite_rs::WritableType + ignite_rs::ReadableType,
    V: ignite_rs::WritableType + ignite_rs::ReadableType,
{
    let mut cfg = CacheConfiguration::new(cache_name);
    cfg.atomicity_mode = AtomicityMode::Transactional;
    client.create_cache_with_config::<K, V>(&cfg).await.unwrap()
}

async fn wait_for_ready_within(scope: &IgniteScope) {
    tokio::time::timeout(Duration::from_secs(20), scope.wait_for_ready())
        .await
        .expect("timed out waiting for timeout test fixture to become ready")
        .unwrap();
}

fn suite_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
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
