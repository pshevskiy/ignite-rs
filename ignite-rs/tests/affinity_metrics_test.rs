#![cfg(not(feature = "ssl"))]

mod common;

use common::{
    encode_cache_partitions_response, spawn_mock_thin_server_on_addr, unused_local_addr,
    MockDiscoveryNode, MockDiscoveryResponse, MockResponse, MockThinServerConfig,
    MockTopologyVersion, MockUuid,
};
use ignite_rs::protocol::{write_bool, write_i32, write_i64};
use ignite_rs::query::ScanQuery;
use ignite_rs::{new_client, ClientConfig};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Related Apache Ignite affinity-metrics coverage:
/// `AffinityMetricsTest`
/// Java source: org.apache.ignite.internal.client.thin.AffinityMetricsTest
#[tokio::test]
async fn should_route_partitioned_scan_query_to_partition_owner() {
    let seed_addr = unused_local_addr();
    let discovered_addr = unused_local_addr();
    let seed_node = MockUuid::new(1101, 1101);
    let discovered_node = MockUuid::new(1102, 1102);
    let cache_name = "affinity_scan";
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
            cache_partitions_responses: Some(Arc::new(Mutex::new(VecDeque::from([
                MockResponse::success(encode_cache_partitions_response(
                    MockTopologyVersion { major: 1, minor: 0 },
                    cache_id,
                    &[(seed_node, &[0]), (discovered_node, &[1])],
                )),
            ])))),
            query_scan_responses: Some(Arc::new(Mutex::new(VecDeque::from([
                MockResponse::success(empty_cursor_open_payload()),
            ])))),
            ..MockThinServerConfig::default()
        },
    );
    let discovered_partitions = Arc::new(Mutex::new(VecDeque::from([MockResponse::success(
        encode_cache_partitions_response(
            MockTopologyVersion { major: 1, minor: 0 },
            cache_id,
            &[(seed_node, &[0]), (discovered_node, &[1])],
        ),
    )])));
    let discovered = spawn_mock_thin_server_on_addr(
        &discovered_addr,
        MockThinServerConfig {
            node_id: discovered_node,
            cache_partitions_responses: Some(discovered_partitions),
            query_scan_responses: Some(Arc::new(Mutex::new(VecDeque::from([
                MockResponse::success(empty_cursor_open_payload()),
            ])))),
            ..MockThinServerConfig::default()
        },
    );

    let mut conf = ClientConfig::new(&seed_addr);
    conf.request_timeout = Some(Duration::from_millis(500));
    conf.retry_limit = 1;

    let client = new_client(conf).await.unwrap();
    let cache = client.cache::<i32, i32>(cache_name);

    cache
        .scan_query(ScanQuery::new().with_partition(1))
        .await
        .unwrap()
        .fetch_all()
        .await
        .unwrap();
    cache
        .scan_query(ScanQuery::new().with_partition(0))
        .await
        .unwrap()
        .fetch_all()
        .await
        .unwrap();

    assert_eq!(discovered.recorded_cache_requests().len(), 1);
    assert_eq!(discovered.recorded_cache_requests()[0].op_code, 2000);
    assert_eq!(seed.recorded_cache_requests().len(), 1);
    assert_eq!(seed.recorded_cache_requests()[0].op_code, 2000);
}

fn empty_cursor_open_payload() -> Vec<u8> {
    let mut payload = Vec::new();
    write_i64(&mut payload, 1).unwrap();
    write_i32(&mut payload, 0).unwrap();
    write_bool(&mut payload, false).unwrap();
    payload
}
