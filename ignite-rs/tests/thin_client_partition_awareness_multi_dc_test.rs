#![cfg(not(feature = "ssl"))]

mod common;

use common::{
    encode_cache_partitions_response_with_dc, encode_typed_payload, spawn_mock_thin_server_on_addr,
    unused_local_addr, MockDiscoveryNode, MockDiscoveryResponse, MockResponse,
    MockThinServerConfig, MockTopologyVersion, MockUuid,
};
use ignite_rs::cluster::ClusterState;
use ignite_rs::protocol::{write_bool, write_i32, write_i64};
use ignite_rs::query::ScanQuery;
use ignite_rs::tx::TransactionOptions;
use ignite_rs::{new_client, ClientConfig};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const DC_ATTR: &str = "IGNITE_DATA_CENTER_ID";
const OP_CLUSTER_GET_STATE: i16 = 5000;
const OP_CLUSTER_GROUP_GET_NODE_IDS: i16 = 5100;
const OP_CLUSTER_GROUP_GET_NODE_INFO: i16 = 5101;
const OP_COMPUTE_TASK_EXECUTE: i16 = 6000;
const OP_COMPUTE_TASK_FINISHED: i16 = 6001;

/// Partial migration of Apache Ignite `ThinClientPartitionAwarenessMultiDcTest.testPartitionAwarenessRequests`:
/// Java source: org.apache.ignite.internal.client.thin.ThinClientPartitionAwarenessMultiDcTest
#[tokio::test]
async fn should_route_partition_aware_reads_to_current_data_center_backup() {
    let seed_addr = unused_local_addr();
    let dc_backup_addr = unused_local_addr();
    let other_dc_primary_addr = unused_local_addr();

    let seed_node = MockUuid::new(1201, 1201);
    let dc_backup_node = MockUuid::new(1202, 1202);
    let other_dc_primary_node = MockUuid::new(1203, 1203);
    let cache_name = "mdc_partitioned_cache";
    let cache_id = ignite_rs::utils::string_to_java_hashcode(cache_name);

    let dc_backup_port = port_of(&dc_backup_addr);
    let other_dc_primary_port = port_of(&other_dc_primary_addr);

    let seed = spawn_mock_thin_server_on_addr(
        &seed_addr,
        MockThinServerConfig {
            node_id: seed_node,
            discovery_response: Some(MockDiscoveryResponse {
                topology_version: 1,
                added_nodes: vec![
                    MockDiscoveryNode {
                        node_id: dc_backup_node,
                        port: dc_backup_port,
                        addresses: vec!["127.0.0.1".to_string()],
                    },
                    MockDiscoveryNode {
                        node_id: other_dc_primary_node,
                        port: other_dc_primary_port,
                        addresses: vec!["127.0.0.1".to_string()],
                    },
                ],
                removed_node_ids: Vec::new(),
            }),
            data_center_nodes_response: Some(vec![seed_node, dc_backup_node]),
            cache_partitions_responses: Some(Arc::new(Mutex::new(VecDeque::from([
                MockResponse::success(encode_cache_partitions_response_with_dc(
                    MockTopologyVersion { major: 1, minor: 0 },
                    cache_id,
                    &[(other_dc_primary_node, &[0])],
                    Some(&[(dc_backup_node, &[0])]),
                )),
            ])))),
            ..MockThinServerConfig::default()
        },
    );
    let dc_backup = spawn_mock_thin_server_on_addr(
        &dc_backup_addr,
        MockThinServerConfig {
            node_id: dc_backup_node,
            cache_get_responses: Some(Arc::new(Mutex::new(VecDeque::from([
                MockResponse::success(encode_typed_payload(&42i32)),
            ])))),
            cache_contains_key_responses: Some(Arc::new(Mutex::new(VecDeque::from([
                MockResponse::success(bool_payload(true)),
            ])))),
            query_scan_responses: Some(Arc::new(Mutex::new(VecDeque::from([
                MockResponse::success(empty_cursor_open_payload()),
            ])))),
            ..MockThinServerConfig::default()
        },
    );
    let other_dc_primary = spawn_mock_thin_server_on_addr(
        &other_dc_primary_addr,
        MockThinServerConfig {
            node_id: other_dc_primary_node,
            ..MockThinServerConfig::default()
        },
    );

    let client = new_client(dc_client_config(&seed_addr, "dc1"))
        .await
        .unwrap();
    let cache = client.cache::<i32, i32>(cache_name);

    cache.put(&0, &1).await.unwrap();
    assert_eq!(cache.get(&0).await.unwrap(), Some(42));
    assert!(cache.contains_key(&0).await.unwrap());
    cache
        .scan_query(ScanQuery::new().with_partition(0))
        .await
        .unwrap()
        .fetch_all()
        .await
        .unwrap();

    let primary_ops = other_dc_primary.recorded_cache_requests();
    assert_eq!(
        primary_ops
            .iter()
            .filter(|request| request.op_code == 1001)
            .count(),
        1
    );
    assert_eq!(
        primary_ops
            .iter()
            .filter(|request| request.op_code == 1000
                || request.op_code == 1011
                || request.op_code == 2000)
            .count(),
        0
    );

    let backup_ops = dc_backup.recorded_cache_requests();
    assert_eq!(backup_ops.len(), 3);
    assert_eq!(backup_ops[0].op_code, 1000);
    assert_eq!(backup_ops[1].op_code, 1011);
    assert_eq!(backup_ops[2].op_code, 2000);

    drop(client);
    drop(seed);
    drop(dc_backup);
    drop(other_dc_primary);
}

/// Partial migration of Apache Ignite `ThinClientPartitionAwarenessMultiDcTest.testPartitionAwarenessRequestsNoNodesInDc`:
/// Java source: org.apache.ignite.internal.client.thin.ThinClientPartitionAwarenessMultiDcTest
#[tokio::test]
async fn should_fallback_to_primary_when_client_data_center_has_no_partition_copy() {
    let seed_addr = unused_local_addr();
    let other_dc_primary_addr = unused_local_addr();

    let seed_node = MockUuid::new(1301, 1301);
    let other_dc_primary_node = MockUuid::new(1302, 1302);
    let cache_name = "mdc_no_local_copy";
    let cache_id = ignite_rs::utils::string_to_java_hashcode(cache_name);
    let other_dc_primary_port = port_of(&other_dc_primary_addr);

    let seed = spawn_mock_thin_server_on_addr(
        &seed_addr,
        MockThinServerConfig {
            node_id: seed_node,
            discovery_response: Some(MockDiscoveryResponse {
                topology_version: 1,
                added_nodes: vec![MockDiscoveryNode {
                    node_id: other_dc_primary_node,
                    port: other_dc_primary_port,
                    addresses: vec!["127.0.0.1".to_string()],
                }],
                removed_node_ids: Vec::new(),
            }),
            data_center_nodes_response: Some(Vec::new()),
            cache_partitions_responses: Some(Arc::new(Mutex::new(VecDeque::from([
                MockResponse::success(encode_cache_partitions_response_with_dc(
                    MockTopologyVersion { major: 1, minor: 0 },
                    cache_id,
                    &[(other_dc_primary_node, &[0])],
                    Some(&[]),
                )),
            ])))),
            ..MockThinServerConfig::default()
        },
    );
    let other_dc_primary = spawn_mock_thin_server_on_addr(
        &other_dc_primary_addr,
        MockThinServerConfig {
            node_id: other_dc_primary_node,
            cache_get_responses: Some(Arc::new(Mutex::new(VecDeque::from([
                MockResponse::success(encode_typed_payload(&7i32)),
            ])))),
            query_scan_responses: Some(Arc::new(Mutex::new(VecDeque::from([
                MockResponse::success(empty_cursor_open_payload()),
            ])))),
            ..MockThinServerConfig::default()
        },
    );

    let client = new_client(dc_client_config(&seed_addr, "dc3"))
        .await
        .unwrap();
    let cache = client.cache::<i32, i32>(cache_name);

    cache.put(&0, &1).await.unwrap();
    assert_eq!(cache.get(&0).await.unwrap(), Some(7));
    cache
        .scan_query(ScanQuery::new().with_partition(0))
        .await
        .unwrap()
        .fetch_all()
        .await
        .unwrap();

    let primary_ops = other_dc_primary.recorded_cache_requests();
    assert_eq!(primary_ops.len(), 3);
    assert_eq!(primary_ops[0].op_code, 1001);
    assert_eq!(primary_ops[1].op_code, 1000);
    assert_eq!(primary_ops[2].op_code, 2000);
    assert_eq!(seed.recorded_cache_requests().len(), 0);

    drop(client);
    drop(seed);
    drop(other_dc_primary);
}

/// Partial migration of Apache Ignite `ThinClientPartitionAwarenessMultiDcTest.testNonPartitionAwarenessRequests`:
/// Java source: org.apache.ignite.internal.client.thin.ThinClientPartitionAwarenessMultiDcTest
#[tokio::test]
async fn should_start_transactions_on_current_data_center_default_channel() {
    let seed_addr = unused_local_addr();
    let dc_default_addr = unused_local_addr();
    let other_dc_primary_addr = unused_local_addr();

    let seed_node = MockUuid::new(1401, 1401);
    let dc_default_node = MockUuid::new(1402, 1402);
    let other_dc_primary_node = MockUuid::new(1403, 1403);
    let cache_name = "mdc_tx_cache";
    let cache_id = ignite_rs::utils::string_to_java_hashcode(cache_name);

    let dc_default_port = port_of(&dc_default_addr);
    let other_dc_primary_port = port_of(&other_dc_primary_addr);

    let seed = spawn_mock_thin_server_on_addr(
        &seed_addr,
        MockThinServerConfig {
            node_id: seed_node,
            discovery_response: Some(MockDiscoveryResponse {
                topology_version: 1,
                added_nodes: vec![
                    MockDiscoveryNode {
                        node_id: dc_default_node,
                        port: dc_default_port,
                        addresses: vec!["127.0.0.1".to_string()],
                    },
                    MockDiscoveryNode {
                        node_id: other_dc_primary_node,
                        port: other_dc_primary_port,
                        addresses: vec!["127.0.0.1".to_string()],
                    },
                ],
                removed_node_ids: Vec::new(),
            }),
            data_center_nodes_response: Some(vec![dc_default_node]),
            cache_partitions_responses: Some(Arc::new(Mutex::new(VecDeque::from([
                MockResponse::success(encode_cache_partitions_response_with_dc(
                    MockTopologyVersion { major: 1, minor: 0 },
                    cache_id,
                    &[(other_dc_primary_node, &[0])],
                    Some(&[(dc_default_node, &[0])]),
                )),
            ])))),
            ..MockThinServerConfig::default()
        },
    );
    let dc_default = spawn_mock_thin_server_on_addr(
        &dc_default_addr,
        MockThinServerConfig {
            node_id: dc_default_node,
            tx_start_responses: Some(Arc::new(Mutex::new(VecDeque::from([
                MockResponse::success(9i32.to_le_bytes().to_vec()),
            ])))),
            ..MockThinServerConfig::default()
        },
    );
    let other_dc_primary = spawn_mock_thin_server_on_addr(
        &other_dc_primary_addr,
        MockThinServerConfig {
            node_id: other_dc_primary_node,
            ..MockThinServerConfig::default()
        },
    );

    let client = new_client(dc_client_config(&seed_addr, "dc1"))
        .await
        .unwrap();
    let cache = client.cache::<i32, i32>(cache_name);

    cache.put(&0, &1).await.unwrap();

    let tx = client
        .transactions()
        .tx_start(TransactionOptions::new())
        .await
        .unwrap();
    let tx_cache = tx.cache::<i32, i32>(cache_name);
    tx_cache.put(&1, &2).await.unwrap();
    tx.rollback().await.unwrap();

    assert_eq!(seed.recorded_tx_starts().len(), 0);
    assert_eq!(other_dc_primary.recorded_tx_starts().len(), 0);
    assert_eq!(dc_default.recorded_tx_starts().len(), 1);

    let dc_ops = dc_default.recorded_cache_requests();
    assert_eq!(
        dc_ops
            .iter()
            .filter(|request| request.op_code == 1001 && request.tx_id == Some(9))
            .count(),
        1
    );

    drop(client);
    drop(seed);
    drop(dc_default);
    drop(other_dc_primary);
}

/// Migrated from Apache Ignite `ThinClientPartitionAwarenessMultiDcTest.testNonPartitionAwarenessRequests`:
/// Java source: org.apache.ignite.internal.client.thin.ThinClientPartitionAwarenessMultiDcTest
#[tokio::test]
async fn should_route_cluster_and_compute_requests_to_current_data_center_default_channel() {
    let seed_addr = unused_local_addr();
    let dc_default_addr = unused_local_addr();
    let other_dc_primary_addr = unused_local_addr();

    let seed_node = MockUuid::new(1501, 1501);
    let dc_default_node = MockUuid::new(1502, 1502);
    let other_dc_primary_node = MockUuid::new(1503, 1503);
    let cache_name = "mdc_non_pa_cache";
    let cache_id = ignite_rs::utils::string_to_java_hashcode(cache_name);
    let task_id = 55i64;

    let seed = spawn_mock_thin_server_on_addr(
        &seed_addr,
        MockThinServerConfig {
            node_id: seed_node,
            discovery_response: Some(MockDiscoveryResponse {
                topology_version: 1,
                added_nodes: vec![
                    MockDiscoveryNode {
                        node_id: dc_default_node,
                        port: port_of(&dc_default_addr),
                        addresses: vec!["127.0.0.1".to_string()],
                    },
                    MockDiscoveryNode {
                        node_id: other_dc_primary_node,
                        port: port_of(&other_dc_primary_addr),
                        addresses: vec!["127.0.0.1".to_string()],
                    },
                ],
                removed_node_ids: Vec::new(),
            }),
            data_center_nodes_response: Some(vec![dc_default_node]),
            cache_partitions_responses: Some(Arc::new(Mutex::new(VecDeque::from([
                MockResponse::success(encode_cache_partitions_response_with_dc(
                    MockTopologyVersion { major: 1, minor: 0 },
                    cache_id,
                    &[(other_dc_primary_node, &[0])],
                    Some(&[(dc_default_node, &[0])]),
                )),
            ])))),
            ..MockThinServerConfig::default()
        },
    );
    let dc_default = spawn_mock_thin_server_on_addr(
        &dc_default_addr,
        MockThinServerConfig {
            node_id: dc_default_node,
            opcode_responses: Some(opcode_responses(vec![
                (
                    OP_CLUSTER_GET_STATE,
                    vec![MockResponse::success(vec![ClusterState::Active as u8])],
                ),
                (
                    OP_CLUSTER_GROUP_GET_NODE_IDS,
                    vec![MockResponse::success(encode_node_ids_response(
                        dc_default_node,
                    ))],
                ),
                (
                    OP_CLUSTER_GROUP_GET_NODE_INFO,
                    vec![MockResponse::success(encode_node_info_response(
                        dc_default_node,
                    ))],
                ),
                (
                    OP_COMPUTE_TASK_EXECUTE,
                    vec![MockResponse::success_with_notifications(
                        task_id.to_le_bytes().to_vec(),
                        vec![common::MockNotification::success(
                            OP_COMPUTE_TASK_FINISHED,
                            task_id,
                            encode_typed_payload(&99i32),
                        )],
                    )],
                ),
            ])),
            ..MockThinServerConfig::default()
        },
    );
    let other_dc_primary = spawn_mock_thin_server_on_addr(
        &other_dc_primary_addr,
        MockThinServerConfig {
            node_id: other_dc_primary_node,
            ..MockThinServerConfig::default()
        },
    );

    let client = new_client(dc_client_config(&seed_addr, "dc1"))
        .await
        .unwrap();
    let cache = client.cache::<i32, i32>(cache_name);

    cache.put(&0, &1).await.unwrap();
    assert_eq!(
        client.cluster().state().await.unwrap(),
        ClusterState::Active
    );
    assert_eq!(
        client
            .compute()
            .execute::<i32, i32>("TestTask", Some(&1))
            .await
            .unwrap(),
        Some(99),
    );

    assert_eq!(seed.recorded_opcode_payloads(OP_CLUSTER_GET_STATE).len(), 0);
    assert_eq!(
        seed.recorded_opcode_payloads(OP_CLUSTER_GROUP_GET_NODE_IDS)
            .len(),
        0
    );
    assert_eq!(
        seed.recorded_opcode_payloads(OP_COMPUTE_TASK_EXECUTE).len(),
        0
    );
    assert_eq!(
        dc_default
            .recorded_opcode_payloads(OP_CLUSTER_GET_STATE)
            .len(),
        1
    );
    assert_eq!(
        dc_default
            .recorded_opcode_payloads(OP_CLUSTER_GROUP_GET_NODE_IDS)
            .len(),
        1
    );
    assert_eq!(
        dc_default
            .recorded_opcode_payloads(OP_COMPUTE_TASK_EXECUTE)
            .len(),
        1
    );
    assert_eq!(
        other_dc_primary
            .recorded_opcode_payloads(OP_CLUSTER_GET_STATE)
            .len(),
        0
    );
    assert_eq!(
        other_dc_primary
            .recorded_opcode_payloads(OP_COMPUTE_TASK_EXECUTE)
            .len(),
        0
    );

    drop(client);
    drop(seed);
    drop(dc_default);
    drop(other_dc_primary);
}

/// Migrated from Apache Ignite `ThinClientPartitionAwarenessMultiDcTest.testNonPartitionAwarenessRequestsNoNodesInDc`:
/// Java source: org.apache.ignite.internal.client.thin.ThinClientPartitionAwarenessMultiDcTest
#[tokio::test]
async fn should_fallback_to_active_default_channel_for_non_partition_aware_requests_without_dc_nodes(
) {
    let seed_addr = unused_local_addr();
    let other_dc_primary_addr = unused_local_addr();

    let seed_node = MockUuid::new(1601, 1601);
    let other_dc_primary_node = MockUuid::new(1602, 1602);
    let cache_name = "mdc_non_pa_no_dc_cache";
    let cache_id = ignite_rs::utils::string_to_java_hashcode(cache_name);
    let task_id = 66i64;

    let seed = spawn_mock_thin_server_on_addr(
        &seed_addr,
        MockThinServerConfig {
            node_id: seed_node,
            discovery_response: Some(MockDiscoveryResponse {
                topology_version: 1,
                added_nodes: vec![MockDiscoveryNode {
                    node_id: other_dc_primary_node,
                    port: port_of(&other_dc_primary_addr),
                    addresses: vec!["127.0.0.1".to_string()],
                }],
                removed_node_ids: Vec::new(),
            }),
            data_center_nodes_response: Some(Vec::new()),
            cache_partitions_responses: Some(Arc::new(Mutex::new(VecDeque::from([
                MockResponse::success(encode_cache_partitions_response_with_dc(
                    MockTopologyVersion { major: 1, minor: 0 },
                    cache_id,
                    &[(other_dc_primary_node, &[0])],
                    Some(&[]),
                )),
            ])))),
            tx_start_responses: Some(Arc::new(Mutex::new(VecDeque::from([
                MockResponse::success(19i32.to_le_bytes().to_vec()),
            ])))),
            opcode_responses: Some(opcode_responses(vec![
                (
                    OP_CLUSTER_GET_STATE,
                    vec![MockResponse::success(vec![ClusterState::Active as u8])],
                ),
                (
                    OP_CLUSTER_GROUP_GET_NODE_IDS,
                    vec![MockResponse::success(encode_node_ids_response(seed_node))],
                ),
                (
                    OP_CLUSTER_GROUP_GET_NODE_INFO,
                    vec![MockResponse::success(encode_node_info_response(seed_node))],
                ),
                (
                    OP_COMPUTE_TASK_EXECUTE,
                    vec![MockResponse::success_with_notifications(
                        task_id.to_le_bytes().to_vec(),
                        vec![common::MockNotification::success(
                            OP_COMPUTE_TASK_FINISHED,
                            task_id,
                            encode_typed_payload(&77i32),
                        )],
                    )],
                ),
            ])),
            ..MockThinServerConfig::default()
        },
    );
    let other_dc_primary = spawn_mock_thin_server_on_addr(
        &other_dc_primary_addr,
        MockThinServerConfig {
            node_id: other_dc_primary_node,
            ..MockThinServerConfig::default()
        },
    );

    let client = new_client(dc_client_config(&seed_addr, "dc3"))
        .await
        .unwrap();
    let cache = client.cache::<i32, i32>(cache_name);

    cache.put(&0, &1).await.unwrap();
    assert_eq!(
        client.cluster().state().await.unwrap(),
        ClusterState::Active
    );
    assert_eq!(
        client
            .compute()
            .execute::<i32, i32>("TestTask", Some(&1))
            .await
            .unwrap(),
        Some(77),
    );
    let tx = client
        .transactions()
        .tx_start(TransactionOptions::new())
        .await
        .unwrap();
    tx.rollback().await.unwrap();

    assert_eq!(seed.recorded_opcode_payloads(OP_CLUSTER_GET_STATE).len(), 1);
    assert_eq!(
        seed.recorded_opcode_payloads(OP_CLUSTER_GROUP_GET_NODE_IDS)
            .len(),
        1
    );
    assert_eq!(
        seed.recorded_opcode_payloads(OP_COMPUTE_TASK_EXECUTE).len(),
        1
    );
    assert_eq!(seed.recorded_tx_starts().len(), 1);
    assert_eq!(
        other_dc_primary
            .recorded_opcode_payloads(OP_CLUSTER_GET_STATE)
            .len(),
        0
    );
    assert_eq!(
        other_dc_primary
            .recorded_opcode_payloads(OP_COMPUTE_TASK_EXECUTE)
            .len(),
        0
    );
    assert_eq!(other_dc_primary.recorded_tx_starts().len(), 0);

    drop(client);
    drop(seed);
    drop(other_dc_primary);
}

fn dc_client_config(seed_addr: &str, dc_id: &str) -> ClientConfig {
    let mut conf = ClientConfig::new(seed_addr);
    conf.request_timeout = Some(Duration::from_millis(500));
    conf.reconnect_backoff = Some(Duration::from_millis(25));
    conf.retry_limit = 1;
    conf.user_attributes = BTreeMap::from([(DC_ATTR.to_string(), dc_id.to_string())]);
    conf
}

fn port_of(addr: &str) -> i32 {
    addr.rsplit_once(':')
        .expect("expected host:port mock address")
        .1
        .parse::<i32>()
        .expect("expected numeric mock port")
}

fn bool_payload(value: bool) -> Vec<u8> {
    let mut payload = Vec::new();
    write_bool(&mut payload, value).unwrap();
    payload
}

fn empty_cursor_open_payload() -> Vec<u8> {
    let mut payload = Vec::new();
    write_i64(&mut payload, 1).unwrap();
    write_i32(&mut payload, 0).unwrap();
    write_bool(&mut payload, false).unwrap();
    payload
}

fn opcode_responses(
    entries: Vec<(i16, Vec<MockResponse>)>,
) -> Arc<Mutex<HashMap<i16, VecDeque<MockResponse>>>> {
    Arc::new(Mutex::new(
        entries
            .into_iter()
            .map(|(op, responses)| (op, VecDeque::from(responses)))
            .collect(),
    ))
}

fn encode_node_ids_response(node: MockUuid) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.push(1u8);
    payload.extend_from_slice(&1i64.to_le_bytes());
    payload.extend_from_slice(&1i32.to_le_bytes());
    payload.extend_from_slice(&node.most.to_le_bytes());
    payload.extend_from_slice(&node.least.to_le_bytes());
    payload
}

fn encode_node_info_response(node: MockUuid) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&1i32.to_le_bytes());
    payload.extend_from_slice(&node.most.to_le_bytes());
    payload.extend_from_slice(&node.least.to_le_bytes());
    payload.extend_from_slice(&0i32.to_le_bytes());
    payload.extend_from_slice(&1i32.to_le_bytes());
    write_raw_string(&mut payload, "127.0.0.1");
    payload.extend_from_slice(&1i32.to_le_bytes());
    write_raw_string(&mut payload, "host-a");
    payload.extend_from_slice(&1i64.to_le_bytes());
    payload.push(0);
    payload.push(0);
    payload.push(0);
    encode_typed_to(&mut payload, &"node-a".to_string());
    payload.push(2);
    payload.push(15);
    payload.push(0);
    write_raw_string(&mut payload, "release");
    payload.extend_from_slice(&123i64.to_le_bytes());
    payload.extend_from_slice(&0i32.to_le_bytes());
    payload
}

fn write_raw_string(payload: &mut Vec<u8>, value: &str) {
    payload.extend_from_slice(&(value.len() as i32).to_le_bytes());
    payload.extend_from_slice(value.as_bytes());
}

fn encode_typed_to<T: ignite_rs::WritableType>(payload: &mut Vec<u8>, value: &T) {
    value.write(payload).unwrap();
}
