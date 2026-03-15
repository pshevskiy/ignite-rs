#![cfg(not(feature = "ssl"))]

mod common;

use common::{
    spawn_mock_thin_server_on_addr, unused_local_addr, MockDiscoveryNode, MockDiscoveryResponse,
    MockThinServerConfig, MockTopologyVersion, MockUuid,
};
use ignite_rs::{new_client, ClientConfig};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Partial migration of Apache Ignite `ReliableChannelDuplicationTest.testDuplicationOnClusterRestart`:
/// Java source: org.apache.ignite.internal.client.thin.ReliableChannelDuplicationTest
#[tokio::test]
async fn should_not_open_duplicate_connections_for_same_discovered_address() {
    let seed_addr = unused_local_addr();
    let discovered_addr = unused_local_addr();
    let discovered_port = discovered_addr
        .rsplit_once(':')
        .expect("expected host:port mock address")
        .1
        .parse::<i32>()
        .expect("expected numeric mock port");

    let discovered =
        spawn_mock_thin_server_on_addr(&discovered_addr, MockThinServerConfig::default());
    let seed = spawn_mock_thin_server_on_addr(
        &seed_addr,
        MockThinServerConfig {
            discovery_response: Some(MockDiscoveryResponse {
                topology_version: 1,
                added_nodes: vec![
                    MockDiscoveryNode {
                        node_id: MockUuid::new(2, 2),
                        port: discovered_port,
                        addresses: vec!["127.0.0.1".to_string()],
                    },
                    MockDiscoveryNode {
                        node_id: MockUuid::new(3, 3),
                        port: discovered_port,
                        addresses: vec!["127.0.0.1".to_string()],
                    },
                ],
                removed_node_ids: Vec::new(),
            }),
            ..MockThinServerConfig::default()
        },
    );

    let client = new_client(ClientConfig::new(&seed_addr)).await.unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while discovered.handshake_count() == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for discovered channel initialization"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        discovered.handshake_count(),
        1,
        "expected exactly one connection for duplicated discovered address"
    );

    let _ = client.get_cache_names().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    assert_eq!(
        discovered.handshake_count(),
        1,
        "expected discovered address dedupe to prevent duplicate channel creation"
    );

    drop(client);
    drop(seed);
    drop(discovered);
}

/// Partial migration of Apache Ignite `ReliableChannelDuplicationTest.testStopAndRestartNode`:
/// Java source: org.apache.ignite.internal.client.thin.ReliableChannelDuplicationTest
#[tokio::test]
async fn should_not_duplicate_channel_when_discovered_node_is_removed_and_readded() {
    let seed_addr = unused_local_addr();
    let discovered_addr = unused_local_addr();
    let seed_node = MockUuid::new(11, 11);
    let discovered_node = MockUuid::new(22, 22);
    let discovered_port = discovered_addr
        .rsplit_once(':')
        .expect("expected host:port mock address")
        .1
        .parse::<i32>()
        .expect("expected numeric mock port");

    let discovery_responses = Arc::new(Mutex::new(VecDeque::from([
        MockDiscoveryResponse {
            topology_version: 1,
            added_nodes: vec![MockDiscoveryNode {
                node_id: discovered_node,
                port: discovered_port,
                addresses: vec!["127.0.0.1".to_string()],
            }],
            removed_node_ids: Vec::new(),
        },
        MockDiscoveryResponse {
            topology_version: 2,
            added_nodes: Vec::new(),
            removed_node_ids: vec![discovered_node],
        },
        MockDiscoveryResponse {
            topology_version: 3,
            added_nodes: vec![MockDiscoveryNode {
                node_id: discovered_node,
                port: discovered_port,
                addresses: vec!["127.0.0.1".to_string()],
            }],
            removed_node_ids: Vec::new(),
        },
    ])));
    let topology_changes = Arc::new(Mutex::new(VecDeque::from([
        MockTopologyVersion { major: 2, minor: 0 },
        MockTopologyVersion { major: 3, minor: 0 },
    ])));

    let discovered = spawn_mock_thin_server_on_addr(
        &discovered_addr,
        MockThinServerConfig {
            node_id: discovered_node,
            ..MockThinServerConfig::default()
        },
    );
    let seed = spawn_mock_thin_server_on_addr(
        &seed_addr,
        MockThinServerConfig {
            node_id: seed_node,
            discovery_responses: Some(discovery_responses),
            topology_changes_on_cache_names: Some(topology_changes),
            ..MockThinServerConfig::default()
        },
    );

    let client = new_client(ClientConfig::new(&seed_addr)).await.unwrap();

    wait_for(
        || discovered.active_connection_count() == 1,
        "expected initial discovered-node connection",
    )
    .await;
    assert_eq!(
        discovered.handshake_count(),
        1,
        "expected exactly one initial discovered-node handshake"
    );

    let _ = client.get_cache_names().await.unwrap();
    wait_for(
        || discovered.active_connection_count() == 0,
        "expected discovered-node channel removal after topology change",
    )
    .await;

    let _ = client.get_cache_names().await.unwrap();
    wait_for(
        || discovered.active_connection_count() == 1,
        "expected discovered-node channel to be recreated after re-add",
    )
    .await;

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        discovered.handshake_count(),
        2,
        "expected one handshake per lifecycle without duplicate recreated channels"
    );

    drop(client);
    drop(seed);
    drop(discovered);
}

/// Migrated from Apache Ignite `ReliableChannelDuplicationTest.testDuplicationOnClusterRestart`:
/// Java source: org.apache.ignite.internal.client.thin.ReliableChannelDuplicationTest
#[tokio::test]
async fn should_not_duplicate_channels_when_all_discovered_nodes_are_removed_and_readded() {
    let seed_addr = unused_local_addr();
    let addr_a = unused_local_addr();
    let addr_b = unused_local_addr();
    let seed_node = MockUuid::new(31, 31);
    let node_a = MockUuid::new(32, 32);
    let node_b = MockUuid::new(33, 33);

    let discovery_responses = Arc::new(Mutex::new(VecDeque::from([
        MockDiscoveryResponse {
            topology_version: 1,
            added_nodes: vec![
                MockDiscoveryNode {
                    node_id: node_a,
                    port: port_of(&addr_a),
                    addresses: vec!["127.0.0.1".to_string()],
                },
                MockDiscoveryNode {
                    node_id: node_b,
                    port: port_of(&addr_b),
                    addresses: vec!["127.0.0.1".to_string()],
                },
            ],
            removed_node_ids: Vec::new(),
        },
        MockDiscoveryResponse {
            topology_version: 2,
            added_nodes: Vec::new(),
            removed_node_ids: vec![node_a, node_b],
        },
        MockDiscoveryResponse {
            topology_version: 3,
            added_nodes: vec![
                MockDiscoveryNode {
                    node_id: node_a,
                    port: port_of(&addr_a),
                    addresses: vec!["127.0.0.1".to_string()],
                },
                MockDiscoveryNode {
                    node_id: node_b,
                    port: port_of(&addr_b),
                    addresses: vec!["127.0.0.1".to_string()],
                },
            ],
            removed_node_ids: Vec::new(),
        },
    ])));
    let topology_changes = Arc::new(Mutex::new(VecDeque::from([
        MockTopologyVersion { major: 2, minor: 0 },
        MockTopologyVersion { major: 3, minor: 0 },
    ])));

    let discovered_a = spawn_mock_thin_server_on_addr(
        &addr_a,
        MockThinServerConfig {
            node_id: node_a,
            ..MockThinServerConfig::default()
        },
    );
    let discovered_b = spawn_mock_thin_server_on_addr(
        &addr_b,
        MockThinServerConfig {
            node_id: node_b,
            ..MockThinServerConfig::default()
        },
    );
    let seed = spawn_mock_thin_server_on_addr(
        &seed_addr,
        MockThinServerConfig {
            node_id: seed_node,
            discovery_responses: Some(discovery_responses),
            topology_changes_on_cache_names: Some(topology_changes),
            ..MockThinServerConfig::default()
        },
    );

    let client = new_client(ClientConfig::new(&seed_addr)).await.unwrap();

    wait_for(
        || {
            discovered_a.active_connection_count() == 1
                && discovered_b.active_connection_count() == 1
        },
        "expected both discovered-node channels to be initialized",
    )
    .await;

    let _ = client.get_cache_names().await.unwrap();
    wait_for(
        || {
            discovered_a.active_connection_count() == 0
                && discovered_b.active_connection_count() == 0
        },
        "expected both discovered-node channels to be removed after restart-style topology change",
    )
    .await;

    let _ = client.get_cache_names().await.unwrap();
    wait_for(
        || {
            discovered_a.active_connection_count() == 1
                && discovered_b.active_connection_count() == 1
        },
        "expected both discovered-node channels to be recreated after re-add",
    )
    .await;

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(discovered_a.handshake_count(), 2);
    assert_eq!(discovered_b.handshake_count(), 2);

    drop(client);
    drop(seed);
    drop(discovered_a);
    drop(discovered_b);
}

/// Migrated from Apache Ignite `ReliableChannelDuplicationTest.testStopSingleNodeDuringOperation`:
/// Java source: org.apache.ignite.internal.client.thin.ReliableChannelDuplicationTest
#[tokio::test]
async fn should_not_duplicate_remaining_channels_when_one_discovered_node_is_removed() {
    let seed_addr = unused_local_addr();
    let addr_a = unused_local_addr();
    let addr_b = unused_local_addr();
    let seed_node = MockUuid::new(41, 41);
    let node_a = MockUuid::new(42, 42);
    let node_b = MockUuid::new(43, 43);

    let discovery_responses = Arc::new(Mutex::new(VecDeque::from([
        MockDiscoveryResponse {
            topology_version: 1,
            added_nodes: vec![
                MockDiscoveryNode {
                    node_id: node_a,
                    port: port_of(&addr_a),
                    addresses: vec!["127.0.0.1".to_string()],
                },
                MockDiscoveryNode {
                    node_id: node_b,
                    port: port_of(&addr_b),
                    addresses: vec!["127.0.0.1".to_string()],
                },
            ],
            removed_node_ids: Vec::new(),
        },
        MockDiscoveryResponse {
            topology_version: 2,
            added_nodes: Vec::new(),
            removed_node_ids: vec![node_a],
        },
    ])));
    let topology_changes = Arc::new(Mutex::new(VecDeque::from([MockTopologyVersion {
        major: 2,
        minor: 0,
    }])));

    let discovered_a = spawn_mock_thin_server_on_addr(
        &addr_a,
        MockThinServerConfig {
            node_id: node_a,
            ..MockThinServerConfig::default()
        },
    );
    let discovered_b = spawn_mock_thin_server_on_addr(
        &addr_b,
        MockThinServerConfig {
            node_id: node_b,
            ..MockThinServerConfig::default()
        },
    );
    let seed = spawn_mock_thin_server_on_addr(
        &seed_addr,
        MockThinServerConfig {
            node_id: seed_node,
            discovery_responses: Some(discovery_responses),
            topology_changes_on_cache_names: Some(topology_changes),
            ..MockThinServerConfig::default()
        },
    );

    let client = new_client(ClientConfig::new(&seed_addr)).await.unwrap();

    wait_for(
        || {
            discovered_a.active_connection_count() == 1
                && discovered_b.active_connection_count() == 1
        },
        "expected both discovered-node channels to be initialized",
    )
    .await;

    let _ = client.get_cache_names().await.unwrap();
    wait_for(
        || {
            discovered_a.active_connection_count() == 0
                && discovered_b.active_connection_count() == 1
        },
        "expected only the removed discovered-node channel to be closed",
    )
    .await;

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(discovered_a.handshake_count(), 1);
    assert_eq!(discovered_b.handshake_count(), 1);

    drop(client);
    drop(seed);
    drop(discovered_a);
    drop(discovered_b);
}

/// Migrated from Apache Ignite `ReliableChannelDuplicationTest.testStopMultipleNodesDuringOperation`:
/// Java source: org.apache.ignite.internal.client.thin.ReliableChannelDuplicationTest
#[tokio::test]
async fn should_not_duplicate_surviving_channel_when_multiple_discovered_nodes_are_removed() {
    let seed_addr = unused_local_addr();
    let addr_a = unused_local_addr();
    let addr_b = unused_local_addr();
    let addr_c = unused_local_addr();
    let seed_node = MockUuid::new(51, 51);
    let node_a = MockUuid::new(52, 52);
    let node_b = MockUuid::new(53, 53);
    let node_c = MockUuid::new(54, 54);

    let discovery_responses = Arc::new(Mutex::new(VecDeque::from([
        MockDiscoveryResponse {
            topology_version: 1,
            added_nodes: vec![
                MockDiscoveryNode {
                    node_id: node_a,
                    port: port_of(&addr_a),
                    addresses: vec!["127.0.0.1".to_string()],
                },
                MockDiscoveryNode {
                    node_id: node_b,
                    port: port_of(&addr_b),
                    addresses: vec!["127.0.0.1".to_string()],
                },
                MockDiscoveryNode {
                    node_id: node_c,
                    port: port_of(&addr_c),
                    addresses: vec!["127.0.0.1".to_string()],
                },
            ],
            removed_node_ids: Vec::new(),
        },
        MockDiscoveryResponse {
            topology_version: 2,
            added_nodes: Vec::new(),
            removed_node_ids: vec![node_a, node_b],
        },
    ])));
    let topology_changes = Arc::new(Mutex::new(VecDeque::from([MockTopologyVersion {
        major: 2,
        minor: 0,
    }])));

    let discovered_a = spawn_mock_thin_server_on_addr(
        &addr_a,
        MockThinServerConfig {
            node_id: node_a,
            ..MockThinServerConfig::default()
        },
    );
    let discovered_b = spawn_mock_thin_server_on_addr(
        &addr_b,
        MockThinServerConfig {
            node_id: node_b,
            ..MockThinServerConfig::default()
        },
    );
    let discovered_c = spawn_mock_thin_server_on_addr(
        &addr_c,
        MockThinServerConfig {
            node_id: node_c,
            ..MockThinServerConfig::default()
        },
    );
    let seed = spawn_mock_thin_server_on_addr(
        &seed_addr,
        MockThinServerConfig {
            node_id: seed_node,
            discovery_responses: Some(discovery_responses),
            topology_changes_on_cache_names: Some(topology_changes),
            ..MockThinServerConfig::default()
        },
    );

    let client = new_client(ClientConfig::new(&seed_addr)).await.unwrap();

    wait_for(
        || {
            discovered_a.active_connection_count() == 1
                && discovered_b.active_connection_count() == 1
                && discovered_c.active_connection_count() == 1
        },
        "expected all discovered-node channels to be initialized",
    )
    .await;

    let _ = client.get_cache_names().await.unwrap();
    wait_for(
        || {
            discovered_a.active_connection_count() == 0
                && discovered_b.active_connection_count() == 0
                && discovered_c.active_connection_count() == 1
        },
        "expected only the surviving discovered-node channel to remain connected",
    )
    .await;

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(discovered_a.handshake_count(), 1);
    assert_eq!(discovered_b.handshake_count(), 1);
    assert_eq!(discovered_c.handshake_count(), 1);

    drop(client);
    drop(seed);
    drop(discovered_a);
    drop(discovered_b);
    drop(discovered_c);
}

fn port_of(address: &str) -> i32 {
    address
        .rsplit_once(':')
        .expect("expected host:port mock address")
        .1
        .parse::<i32>()
        .expect("expected numeric mock port")
}

async fn wait_for(condition: impl Fn() -> bool, failure_message: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);

    while !condition() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "{}",
            failure_message
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}
