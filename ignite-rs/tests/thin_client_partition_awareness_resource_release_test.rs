#![cfg(not(feature = "ssl"))]

mod common;

use common::{
    encode_cache_partitions_response, spawn_mock_thin_server_on_addr, unused_local_addr,
    MockDiscoveryNode, MockDiscoveryResponse, MockResponse, MockThinServerConfig,
    MockTopologyVersion, MockUuid,
};
use ignite_rs::{new_client, ClientConfig};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Partial migration of Apache Ignite `ThinClientPartitionAwarenessResourceReleaseTest.testResourcesReleasedAfterClientClosed`:
/// Java source: org.apache.ignite.internal.client.thin.ThinClientPartitionAwarenessResourceReleaseTest
#[tokio::test]
async fn should_release_all_discovered_channels_after_client_drop() {
    let addr1 = unused_local_addr();
    let addr2 = unused_local_addr();
    let node1 = MockUuid::new(101, 101);
    let node2 = MockUuid::new(202, 202);
    let addr2_port = addr2
        .rsplit_once(':')
        .expect("expected host:port mock address")
        .1
        .parse::<i32>()
        .expect("expected numeric mock port");

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
            discovery_response: Some(MockDiscoveryResponse {
                topology_version: 1,
                added_nodes: vec![MockDiscoveryNode {
                    node_id: node2,
                    port: addr2_port,
                    addresses: vec!["127.0.0.1".to_string()],
                }],
                removed_node_ids: Vec::new(),
            }),
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
        || server1.active_connection_count() == 1 && server2.active_connection_count() == 1,
        "expected both seed and discovered channels to be connected",
    )
    .await;

    drop(client);

    wait_for(
        || server1.active_connection_count() == 0 && server2.active_connection_count() == 0,
        "expected all partition-aware channels to close after client drop",
    )
    .await;

    drop(server1);
    drop(server2);
}

/// Related original Apache Ignite resource-release coverage:
/// Java source: org.apache.ignite.internal.client.thin.ThinClientPartitionAwarenessResourceReleaseTest
#[tokio::test]
async fn should_release_removed_discovered_channel_after_topology_refresh() {
    let addr1 = unused_local_addr();
    let addr2 = unused_local_addr();
    let node1 = MockUuid::new(303, 303);
    let node2 = MockUuid::new(404, 404);
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
            topology_change_on_cache_names: Some(MockTopologyVersion { major: 2, minor: 0 }),
            discovery_responses: Some(discovery_responses),
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
        "expected discovered channel to connect before removal",
    )
    .await;

    let _ = client.get_cache_names().await.unwrap();
    wait_for(
        || server2.active_connection_count() == 0,
        "expected removed discovered channel to close after topology refresh",
    )
    .await;

    drop(client);
    drop(server1);
    drop(server2);
}

/// Migrated from Apache Ignite `ThinClientPartitionAwarenessResourceReleaseTest.testResourcesReleasedAfterCacheDestroyed`:
/// Java source: org.apache.ignite.internal.client.thin.ThinClientPartitionAwarenessResourceReleaseTest
#[tokio::test]
async fn should_invalidate_affinity_mapping_after_cache_destroy() {
    let seed_addr = unused_local_addr();
    let discovered_addr = unused_local_addr();
    let cache_name = "resource_release_destroyed_cache";
    let cache_id = ignite_rs::utils::string_to_java_hashcode(cache_name);
    let seed_node = MockUuid::new(505, 505);
    let discovered_node = MockUuid::new(606, 606);
    let discovered_port = discovered_addr
        .rsplit_once(':')
        .expect("expected host:port mock address")
        .1
        .parse::<i32>()
        .expect("expected numeric mock port");

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
            discovery_response: Some(MockDiscoveryResponse {
                topology_version: 1,
                added_nodes: vec![MockDiscoveryNode {
                    node_id: discovered_node,
                    port: discovered_port,
                    addresses: vec!["127.0.0.1".to_string()],
                }],
                removed_node_ids: Vec::new(),
            }),
            cache_partitions_responses: Some(Arc::new(Mutex::new(VecDeque::from([
                MockResponse::success(encode_cache_partitions_response(
                    MockTopologyVersion { major: 1, minor: 0 },
                    cache_id,
                    &[(discovered_node, &[0])],
                )),
                MockResponse::success(encode_cache_partitions_response(
                    MockTopologyVersion { major: 2, minor: 0 },
                    cache_id,
                    &[(discovered_node, &[0])],
                )),
            ])))),
            ..MockThinServerConfig::default()
        },
    );

    let mut conf = ClientConfig::new(&seed_addr);
    conf.request_timeout = Some(Duration::from_millis(500));
    conf.reconnect_backoff = Some(Duration::from_millis(25));
    conf.retry_limit = 1;

    let client = new_client(conf).await.unwrap();
    let cache = client.cache::<i32, i32>(cache_name);

    cache.put(&0, &1).await.unwrap();
    assert_eq!(
        seed.recorded_opcode_payloads(1101).len(),
        1,
        "expected the initial partition-aware operation to fetch affinity mapping once",
    );

    client.destroy_cache(cache_name).await.unwrap();
    cache.put(&0, &2).await.unwrap();

    assert_eq!(
        seed.recorded_opcode_payloads(1101).len(),
        2,
        "expected cache destroy to invalidate the cached affinity mapping",
    );
    assert_eq!(
        discovered
            .recorded_cache_requests()
            .iter()
            .filter(|request| request.op_code == 1001)
            .count(),
        2,
        "expected both puts to continue routing through the discovered affinity node",
    );

    drop(client);
    drop(seed);
    drop(discovered);
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
