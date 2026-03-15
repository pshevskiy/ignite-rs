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

/// Partial migration of Apache Ignite `ThinClientPartitionAwarenessDiscoveryTest.testClientDiscoveryNodesJoin`:
/// Java source: org.apache.ignite.internal.client.thin.ThinClientPartitionAwarenessDiscoveryTest
#[tokio::test]
async fn should_connect_to_newly_discovered_node_after_topology_change() {
    let addr1 = unused_local_addr();
    let addr2 = unused_local_addr();
    let node1 = MockUuid::new(601, 601);
    let node2 = MockUuid::new(602, 602);
    let addr2_port = addr2
        .rsplit_once(':')
        .expect("expected host:port mock address")
        .1
        .parse::<i32>()
        .expect("expected numeric mock port");

    let discovery_responses = Arc::new(Mutex::new(VecDeque::from([
        MockDiscoveryResponse {
            topology_version: 1,
            added_nodes: Vec::new(),
            removed_node_ids: Vec::new(),
        },
        MockDiscoveryResponse {
            topology_version: 2,
            added_nodes: vec![MockDiscoveryNode {
                node_id: node2,
                port: addr2_port,
                addresses: vec!["127.0.0.1".to_string()],
            }],
            removed_node_ids: Vec::new(),
        },
    ])));
    let topology_changes = Arc::new(Mutex::new(VecDeque::from([MockTopologyVersion {
        major: 2,
        minor: 0,
    }])));

    let server2 = spawn_mock_thin_server_on_addr(
        &addr2,
        MockThinServerConfig {
            node_id: node2,
            ..MockThinServerConfig::default()
        },
    );
    let server1 = spawn_mock_thin_server_on_addr(
        &addr1,
        MockThinServerConfig {
            node_id: node1,
            discovery_responses: Some(discovery_responses),
            topology_changes_on_cache_names: Some(topology_changes),
            ..MockThinServerConfig::default()
        },
    );

    let mut conf = ClientConfig::new(&addr1);
    conf.request_timeout = Some(Duration::from_millis(500));
    conf.reconnect_backoff = Some(Duration::from_millis(25));
    conf.retry_limit = 1;

    let client = new_client(conf).await.unwrap();

    assert_eq!(
        server2.active_connection_count(),
        0,
        "unexpected discovered-node connection before topology update"
    );

    let _ = client.get_cache_names().await.unwrap();

    wait_for(
        || server2.active_connection_count() == 1,
        "expected connection to newly discovered node after topology change",
    )
    .await;

    drop(client);
    drop(server1);
    drop(server2);
}

/// Partial migration of Apache Ignite `ThinClientPartitionAwarenessDiscoveryTest.testClientDiscoveryNodesLeave`:
/// Java source: org.apache.ignite.internal.client.thin.ThinClientPartitionAwarenessDiscoveryTest
#[tokio::test]
async fn should_close_removed_discovered_node_after_topology_change() {
    let addr1 = unused_local_addr();
    let addr2 = unused_local_addr();
    let node1 = MockUuid::new(701, 701);
    let node2 = MockUuid::new(702, 702);
    let addr2_port = addr2
        .rsplit_once(':')
        .expect("expected host:port mock address")
        .1
        .parse::<i32>()
        .expect("expected numeric mock port");

    let discovery_responses = Arc::new(Mutex::new(VecDeque::from([
        MockDiscoveryResponse {
            topology_version: 1,
            added_nodes: vec![MockDiscoveryNode {
                node_id: node2,
                port: addr2_port,
                addresses: vec!["127.0.0.1".to_string()],
            }],
            removed_node_ids: Vec::new(),
        },
        MockDiscoveryResponse {
            topology_version: 2,
            added_nodes: Vec::new(),
            removed_node_ids: vec![node2],
        },
    ])));
    let topology_changes = Arc::new(Mutex::new(VecDeque::from([MockTopologyVersion {
        major: 2,
        minor: 0,
    }])));

    let server2 = spawn_mock_thin_server_on_addr(
        &addr2,
        MockThinServerConfig {
            node_id: node2,
            ..MockThinServerConfig::default()
        },
    );
    let server1 = spawn_mock_thin_server_on_addr(
        &addr1,
        MockThinServerConfig {
            node_id: node1,
            discovery_responses: Some(discovery_responses),
            topology_changes_on_cache_names: Some(topology_changes),
            ..MockThinServerConfig::default()
        },
    );

    let mut conf = ClientConfig::new(&addr1);
    conf.request_timeout = Some(Duration::from_millis(500));
    conf.reconnect_backoff = Some(Duration::from_millis(25));
    conf.retry_limit = 1;

    let client = new_client(conf).await.unwrap();
    let _ = client.get_cache_names().await.unwrap();

    wait_for(
        || server2.active_connection_count() == 1,
        "expected discovered node to be connected before removal",
    )
    .await;

    let _ = client.get_cache_names().await.unwrap();

    wait_for(
        || server2.active_connection_count() == 0,
        "expected removed discovered node to close after topology change",
    )
    .await;

    drop(client);
    drop(server1);
    drop(server2);
}

/// Migrated from Apache Ignite `ThinClientPartitionAwarenessDiscoveryTest.testClientDiscoveryFilterNodeJoin`:
/// Java source: org.apache.ignite.internal.client.thin.ThinClientPartitionAwarenessDiscoveryTest
#[tokio::test]
async fn should_not_connect_to_filtered_node_when_topology_update_excludes_it() {
    let seed_addr = unused_local_addr();
    let filtered_addr = unused_local_addr();
    let seed_node = MockUuid::new(711, 711);
    let filtered_node = MockUuid::new(712, 712);

    let discovery_responses = Arc::new(Mutex::new(VecDeque::from([
        MockDiscoveryResponse {
            topology_version: 1,
            added_nodes: Vec::new(),
            removed_node_ids: Vec::new(),
        },
        MockDiscoveryResponse {
            topology_version: 2,
            added_nodes: Vec::new(),
            removed_node_ids: Vec::new(),
        },
    ])));
    let topology_changes = Arc::new(Mutex::new(VecDeque::from([MockTopologyVersion {
        major: 2,
        minor: 0,
    }])));

    let filtered = spawn_mock_thin_server_on_addr(
        &filtered_addr,
        MockThinServerConfig {
            node_id: filtered_node,
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

    let mut conf = ClientConfig::new(&seed_addr);
    conf.request_timeout = Some(Duration::from_millis(500));
    conf.reconnect_backoff = Some(Duration::from_millis(25));
    conf.retry_limit = 1;

    let client = new_client(conf).await.unwrap();
    let _ = client.get_cache_names().await.unwrap();

    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(
        filtered.active_connection_count(),
        0,
        "expected filtered node to remain disconnected when discovery update excludes it",
    );
    assert_eq!(
        filtered.handshake_count(),
        0,
        "expected no channel initialization for the filtered node",
    );

    drop(client);
    drop(seed);
    drop(filtered);
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
