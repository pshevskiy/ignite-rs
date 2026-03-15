#![cfg(not(feature = "ssl"))]

mod common;

use common::{
    encode_cache_partitions_response, encode_typed_payload, spawn_mock_thin_server,
    spawn_mock_thin_server_on_addr, unused_local_addr, MockDiscoveryNode, MockDiscoveryResponse,
    MockResponse, MockThinServerConfig, MockTopologyVersion, MockUuid,
};
use ignite_rs::tx::{TransactionConcurrency, TransactionIsolation, TransactionOptions};
use ignite_rs::{new_client, ClientConfig};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Related Apache Ignite transactional channel-pinning coverage:
/// `BlockingTxOpsTest`
/// Java source: org.apache.ignite.internal.client.thin.BlockingTxOpsTest
#[tokio::test]
async fn should_pin_explicit_transaction_to_start_channel_and_commit_on_same_node() {
    let seed_addr = unused_local_addr();
    let discovered_addr = unused_local_addr();
    let seed_node = MockUuid::new(901, 901);
    let discovered_node = MockUuid::new(902, 902);
    let cache_name = "txPinnedCache";
    let cache_id = ignite_rs::utils::string_to_java_hashcode(cache_name);
    let discovered_port = discovered_addr
        .rsplit_once(':')
        .expect("expected host:port mock address")
        .1
        .parse::<i32>()
        .expect("expected numeric mock port");

    let seed = spawn_mock_thin_server_on_addr(
        &seed_addr,
        MockThinServerConfig {
            node_id: seed_node,
            discovery_response: Some(MockDiscoveryResponse {
                topology_version: 1,
                added_nodes: vec![MockDiscoveryNode {
                    node_id: discovered_node,
                    port: discovered_port,
                    addresses: vec!["127.0.0.1".to_string()],
                }],
                removed_node_ids: Vec::new(),
            }),
            tx_start_responses: Some(Arc::new(Mutex::new(VecDeque::from([
                MockResponse::success(7i32.to_le_bytes().to_vec()),
            ])))),
            cache_partitions_responses: Some(Arc::new(Mutex::new(VecDeque::from([
                MockResponse::success(encode_cache_partitions_response(
                    MockTopologyVersion { major: 1, minor: 0 },
                    cache_id,
                    &[(seed_node, &[0]), (discovered_node, &[1])],
                )),
            ])))),
            cache_get_responses: Some(Arc::new(Mutex::new(VecDeque::from([
                MockResponse::success(encode_typed_payload(&101i32)),
            ])))),
            ..MockThinServerConfig::default()
        },
    );
    let discovered = spawn_mock_thin_server_on_addr(
        &discovered_addr,
        MockThinServerConfig {
            node_id: discovered_node,
            cache_get_responses: Some(Arc::new(Mutex::new(VecDeque::from([
                MockResponse::success(encode_typed_payload(&202i32)),
            ])))),
            ..MockThinServerConfig::default()
        },
    );

    let mut conf = ClientConfig::from_addresses([seed_addr.as_str()]);
    conf.request_timeout = Some(Duration::from_millis(500));
    conf.retry_limit = 1;
    conf.reconnect_backoff = Some(Duration::from_millis(25));

    let client = new_client(conf).await.unwrap();
    let tx = client
        .transactions()
        .tx_start(
            TransactionOptions::new()
                .with_concurrency(TransactionConcurrency::Pessimistic)
                .with_isolation(TransactionIsolation::RepeatableRead)
                .with_timeout(Duration::from_millis(777))
                .with_label("phase3"),
        )
        .await
        .unwrap();

    let cache = tx.cache::<i32, i32>(cache_name);
    let value = cache.get(&1).await.unwrap();
    tx.commit().await.unwrap();

    assert_eq!(value, Some(101));
    assert_eq!(seed.recorded_tx_starts().len(), 1);
    assert_eq!(seed.recorded_tx_starts()[0].timeout_ms, 777);
    assert_eq!(
        seed.recorded_tx_starts()[0].label.as_deref(),
        Some("phase3")
    );
    assert_eq!(seed.recorded_cache_requests().len(), 1);
    assert_eq!(seed.recorded_cache_requests()[0].tx_id, Some(7));
    assert!(discovered.recorded_cache_requests().is_empty());
    assert_eq!(seed.recorded_tx_ends().len(), 1);
    assert_eq!(seed.recorded_tx_ends()[0].tx_id, 7);
    assert!(seed.recorded_tx_ends()[0].committed);
}

/// Related Apache Ignite transactional connection-loss coverage:
/// `BlockingTxOpsTest`
/// Java source: org.apache.ignite.internal.client.thin.BlockingTxOpsTest
#[tokio::test]
async fn should_mark_transaction_lost_when_pinned_channel_drops() {
    let disconnects = Arc::new(Mutex::new(VecDeque::from([true])));
    let server = spawn_mock_thin_server(MockThinServerConfig {
        tx_start_responses: Some(Arc::new(Mutex::new(VecDeque::from([
            MockResponse::success(9i32.to_le_bytes().to_vec()),
        ])))),
        cache_put_disconnects: Some(disconnects),
        ..MockThinServerConfig::default()
    });

    let mut conf = ClientConfig::new(server.addr());
    conf.request_timeout = Some(Duration::from_millis(250));
    let client = new_client(conf).await.unwrap();

    let tx = client
        .transactions()
        .tx_start(TransactionOptions::default())
        .await
        .unwrap();
    let cache = tx.cache::<i32, i32>("txLost");

    let put_err = cache.put(&1, &1).await.unwrap_err();
    assert!(
        put_err
            .to_string()
            .contains("Transaction context has been lost due to connection errors"),
        "unexpected tx-lost error: {}",
        put_err
    );

    let commit_err = tx.commit().await.unwrap_err();
    assert!(
        commit_err
            .to_string()
            .contains("Transaction context has been lost due to connection errors"),
        "unexpected tx commit-after-loss error: {}",
        commit_err
    );
    assert!(server.recorded_tx_ends().is_empty());
}
