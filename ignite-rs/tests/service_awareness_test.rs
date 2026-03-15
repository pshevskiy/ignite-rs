#![cfg(not(feature = "ssl"))]

mod common;

use common::{
    encode_typed_payload, spawn_mock_thin_server, spawn_mock_thin_server_on_addr,
    unused_local_addr, MockDiscoveryNode, MockDiscoveryResponse, MockResponse,
    MockThinServerConfig, MockUuid,
};
use ignite_rs::{new_client, ClientConfig, WritableType};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

const OP_CLUSTER_GROUP_GET_NODE_IDS: i16 = 5100;
const OP_CLUSTER_GROUP_GET_NODE_INFO: i16 = 5101;
const OP_SERVICE_INVOKE: i16 = 7000;
const OP_SERVICE_GET_TOPOLOGY: i16 = 7003;

/// Migrated from Apache Ignite `ServiceAwarenessTest.testServiceAwarenessRoutesToServiceNode`:
/// Java source: org.apache.ignite.internal.client.thin.ServiceAwarenessTest
#[tokio::test]
async fn should_route_service_invocation_to_service_topology_node() {
    let seed_addr = unused_local_addr();
    let target_addr = unused_local_addr();
    let seed_node = MockUuid::new(8001, 1);
    let target_node = MockUuid::new(8002, 2);
    let target_port = target_addr
        .rsplit_once(':')
        .unwrap()
        .1
        .parse::<i32>()
        .unwrap();

    let seed = spawn_mock_thin_server_on_addr(
        &seed_addr,
        MockThinServerConfig {
            node_id: seed_node,
            discovery_response: Some(MockDiscoveryResponse {
                topology_version: 1,
                added_nodes: vec![MockDiscoveryNode {
                    node_id: target_node,
                    port: target_port,
                    addresses: vec!["127.0.0.1".into()],
                }],
                removed_node_ids: Vec::new(),
            }),
            opcode_responses: Some(opcode_responses(vec![
                (
                    OP_CLUSTER_GROUP_GET_NODE_IDS,
                    vec![MockResponse::success(encode_node_ids_response(&[
                        seed_node,
                        target_node,
                    ]))],
                ),
                (
                    OP_CLUSTER_GROUP_GET_NODE_INFO,
                    vec![MockResponse::success(encode_node_info_response(&[
                        seed_node,
                        target_node,
                    ]))],
                ),
                (
                    OP_SERVICE_GET_TOPOLOGY,
                    vec![MockResponse::success(encode_service_topology(&[
                        target_node,
                    ]))],
                ),
            ])),
            ..Default::default()
        },
    );

    let target = spawn_mock_thin_server_on_addr(
        &target_addr,
        MockThinServerConfig {
            node_id: target_node,
            opcode_responses: Some(opcode_responses(vec![(
                OP_SERVICE_INVOKE,
                vec![MockResponse::success(encode_typed_payload(
                    &"from-target".to_string(),
                ))],
            )])),
            ..Default::default()
        },
    );

    let client = new_client(ClientConfig::new(seed.addr())).await.unwrap();
    let result = client
        .services()
        .service("svc")
        .invoke::<String>("echo", &[])
        .await
        .unwrap();

    assert_eq!(result, Some("from-target".to_string()));
    assert!(seed.recorded_opcode_payloads(OP_SERVICE_INVOKE).is_empty());
    assert_eq!(target.recorded_opcode_payloads(OP_SERVICE_INVOKE).len(), 1);
}

/// Migrated from Apache Ignite `ServiceAwarenessTest.testFallbackToDefaultChannelWithoutTopology`:
/// Java source: org.apache.ignite.internal.client.thin.ServiceAwarenessTest
#[tokio::test]
async fn should_fall_back_to_default_channel_when_service_topology_is_empty() {
    let seed_node = MockUuid::new(8101, 1);
    let server = spawn_mock_thin_server(MockThinServerConfig {
        node_id: seed_node,
        opcode_responses: Some(opcode_responses(vec![
            (
                OP_CLUSTER_GROUP_GET_NODE_IDS,
                vec![MockResponse::success(encode_node_ids_response(&[
                    seed_node,
                ]))],
            ),
            (
                OP_CLUSTER_GROUP_GET_NODE_INFO,
                vec![MockResponse::success(encode_node_info_response(&[
                    seed_node,
                ]))],
            ),
            (
                OP_SERVICE_GET_TOPOLOGY,
                vec![MockResponse::success(encode_service_topology(&[]))],
            ),
            (
                OP_SERVICE_INVOKE,
                vec![MockResponse::success(encode_typed_payload(
                    &"from-default".to_string(),
                ))],
            ),
        ])),
        ..Default::default()
    });

    let client = new_client(ClientConfig::new(server.addr())).await.unwrap();
    let result = client
        .services()
        .service("svc")
        .invoke::<String>("echo", &[])
        .await
        .unwrap();

    assert_eq!(result, Some("from-default".to_string()));
    assert_eq!(server.recorded_opcode_payloads(OP_SERVICE_INVOKE).len(), 1);
}

fn encode_node_ids_response(nodes: &[MockUuid]) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.push(1u8);
    payload.extend_from_slice(&1i64.to_le_bytes());
    payload.extend_from_slice(&(nodes.len() as i32).to_le_bytes());
    for node in nodes {
        payload.extend_from_slice(&node.most.to_le_bytes());
        payload.extend_from_slice(&node.least.to_le_bytes());
    }
    payload
}

fn encode_node_info_response(nodes: &[MockUuid]) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&(nodes.len() as i32).to_le_bytes());
    for node in nodes {
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
        encode_typed_string(&mut payload, &node.as_string());
        payload.push(2);
        payload.push(15);
        payload.push(0);
        write_raw_string(&mut payload, "release");
        payload.extend_from_slice(&123i64.to_le_bytes());
        payload.extend_from_slice(&0i32.to_le_bytes());
    }
    payload
}

fn encode_service_topology(nodes: &[MockUuid]) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&(nodes.len() as i32).to_le_bytes());
    for node in nodes {
        payload.extend_from_slice(&node.most.to_le_bytes());
        payload.extend_from_slice(&node.least.to_le_bytes());
    }
    payload
}

fn encode_typed_string(payload: &mut Vec<u8>, value: &str) {
    value.to_string().write(payload).unwrap();
}

fn write_raw_string(payload: &mut Vec<u8>, value: &str) {
    payload.extend_from_slice(&(value.len() as i32).to_le_bytes());
    payload.extend_from_slice(value.as_bytes());
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
