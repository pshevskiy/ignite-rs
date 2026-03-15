#![cfg(not(feature = "ssl"))]

mod common;

use common::{spawn_mock_thin_server, MockResponse, MockThinServerConfig, MockUuid};
use ignite_rs::protocol::complex_obj::IgniteValue;
use ignite_rs::{new_client, ClientConfig, WritableType};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

const OP_CLUSTER_GROUP_GET_NODE_IDS: i16 = 5100;
const OP_CLUSTER_GROUP_GET_NODE_INFO: i16 = 5101;
const OP_SERVICE_GET_TOPOLOGY: i16 = 7003;
const OP_SERVICE_INVOKE: i16 = 7000;

/// Migrated from Apache Ignite `ServicesBinaryArraysTests`:
/// Java source: org.apache.ignite.internal.client.thin.ServicesBinaryArraysTests
#[tokio::test]
async fn should_invoke_service_with_binary_array_arguments_and_results() {
    let node = MockUuid::new(7001, 7001);
    let server = spawn_mock_thin_server(MockThinServerConfig {
        opcode_responses: Some(opcode_responses(vec![
            (
                OP_CLUSTER_GROUP_GET_NODE_IDS,
                vec![MockResponse::success(encode_node_ids_response(node))],
            ),
            (
                OP_CLUSTER_GROUP_GET_NODE_INFO,
                vec![MockResponse::success(encode_node_info_response(node))],
            ),
            (
                OP_SERVICE_GET_TOPOLOGY,
                vec![MockResponse::success(encode_service_topology(&[node]))],
            ),
            (
                OP_SERVICE_INVOKE,
                vec![MockResponse::success(encode_typed(&vec![
                    Some(vec![1u8, 2u8]),
                    Some(vec![3u8, 4u8]),
                ]))],
            ),
        ])),
        ..Default::default()
    });

    let client = new_client(ClientConfig::new(server.addr())).await.unwrap();
    let result = client
        .services()
        .service("svc")
        .invoke::<Vec<Option<Vec<u8>>>>(
            "mirror",
            &[IgniteValue::Array(vec![
                IgniteValue::Binary(vec![9, 8]),
                IgniteValue::Binary(vec![7, 6]),
            ])],
        )
        .await
        .unwrap()
        .unwrap();

    assert_eq!(result, vec![Some(vec![1, 2]), Some(vec![3, 4])]);
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
    payload.push(9);
    payload.extend_from_slice(&6i32.to_le_bytes());
    payload.extend_from_slice(b"node-a");
    payload.push(2);
    payload.push(15);
    payload.push(0);
    write_raw_string(&mut payload, "release");
    payload.extend_from_slice(&123i64.to_le_bytes());
    payload.extend_from_slice(&0i32.to_le_bytes());
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

fn write_raw_string(payload: &mut Vec<u8>, value: &str) {
    payload.extend_from_slice(&(value.len() as i32).to_le_bytes());
    payload.extend_from_slice(value.as_bytes());
}

fn encode_typed<T: WritableType>(value: &T) -> Vec<u8> {
    let mut payload = Vec::new();
    value.write(&mut payload).unwrap();
    payload
}
