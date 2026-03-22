#![cfg(not(feature = "ssl"))]

mod common;

use common::{
    encode_node_info_payload, encode_typed_payload, spawn_mock_thin_server, MockNodeInfo,
    MockNotification, MockResponse, MockThinServerConfig, MockUuid,
};
use ignite_rs::{new_client, ClientConfig, ReadableType};
use std::collections::{HashMap, VecDeque};
use std::convert::TryInto;
use std::io::Cursor;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const OP_CLUSTER_GROUP_GET_NODE_IDS: i16 = 5100;
const OP_CLUSTER_GROUP_GET_NODE_INFO: i16 = 5101;
const OP_COMPUTE_TASK_EXECUTE: i16 = 6000;
const OP_COMPUTE_TASK_FINISHED: i16 = 6001;
const OP_RESOURCE_CLOSE: i16 = 0;

/// Migrated from Apache Ignite `ComputeTaskTest.testExecuteTaskAsync` and `testExecuteTaskByName`:
/// Java source: org.apache.ignite.internal.client.thin.ComputeTaskTest
#[tokio::test]
async fn should_execute_compute_task_and_wait_for_notification_result() {
    let node = MockUuid::new(9001, 1);
    let task_id = 77i64;
    let server = spawn_mock_thin_server(MockThinServerConfig {
        opcode_responses: Some(opcode_responses(vec![
            (
                OP_CLUSTER_GROUP_GET_NODE_IDS,
                vec![MockResponse::success(encode_node_ids_response(node))],
            ),
            (
                OP_CLUSTER_GROUP_GET_NODE_INFO,
                vec![MockResponse::success(encode_node_info_payload(&[
                    MockNodeInfo::simple(node),
                ]))],
            ),
            (
                OP_COMPUTE_TASK_EXECUTE,
                vec![MockResponse::success_with_notifications(
                    task_id.to_le_bytes().to_vec(),
                    vec![MockNotification::success(
                        OP_COMPUTE_TASK_FINISHED,
                        task_id,
                        encode_typed_payload(&42i32),
                    )],
                )],
            ),
        ])),
        ..Default::default()
    });

    let client = new_client(ClientConfig::new(server.addr())).await.unwrap();
    let result = client
        .compute()
        .with_timeout_ms(900)
        .with_no_failover()
        .with_no_result_cache()
        .execute::<i32, i32>("TestTask", Some(&1))
        .await
        .unwrap();

    assert_eq!(result, Some(42));

    let payloads = server.recorded_opcode_payloads(OP_COMPUTE_TASK_EXECUTE);
    assert_eq!(payloads.len(), 1);
    let decoded = decode_compute_execute_payload(&payloads[0]);
    assert_eq!(decoded.flags, 0x03);
    assert_eq!(decoded.timeout_ms, 900);
    assert_eq!(decoded.task_name, "TestTask");
    assert_eq!(decoded.node_ids, vec![node.as_string()]);
    assert_eq!(decoded.arg, Some(1));
}

/// Migrated from Apache Ignite `ComputeTaskTest.testExecuteTaskAsync2` cancellation path:
/// Java source: org.apache.ignite.internal.client.thin.ComputeTaskTest
#[tokio::test]
async fn should_cancel_compute_task_with_resource_close() {
    let node = MockUuid::new(9002, 2);
    let task_id = 88i64;
    let server = spawn_mock_thin_server(MockThinServerConfig {
        opcode_responses: Some(opcode_responses(vec![
            (
                OP_CLUSTER_GROUP_GET_NODE_IDS,
                vec![MockResponse::success(encode_node_ids_response(node))],
            ),
            (
                OP_CLUSTER_GROUP_GET_NODE_INFO,
                vec![MockResponse::success(encode_node_info_payload(&[
                    MockNodeInfo::simple(node),
                ]))],
            ),
            (
                OP_COMPUTE_TASK_EXECUTE,
                vec![MockResponse::success_with_notifications(
                    task_id.to_le_bytes().to_vec(),
                    vec![MockNotification::success_with_delay(
                        OP_COMPUTE_TASK_FINISHED,
                        task_id,
                        encode_typed_payload(&11i32),
                        Duration::from_millis(200),
                    )],
                )],
            ),
            (OP_RESOURCE_CLOSE, vec![MockResponse::success(Vec::new())]),
        ])),
        ..Default::default()
    });

    let client = new_client(ClientConfig::new(server.addr())).await.unwrap();
    let task = client
        .compute()
        .execute_async::<i32, i32>("TestTask", Some(&1))
        .await
        .unwrap();

    task.cancel().await.unwrap();
    let err = task.wait().await.unwrap_err();
    assert!(err.to_string().contains("cancelled"));

    let payloads = server.recorded_opcode_payloads(OP_RESOURCE_CLOSE);
    assert_eq!(payloads.len(), 1);
    assert_eq!(
        i64::from_le_bytes(payloads[0].as_slice().try_into().unwrap()),
        task_id
    );
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

#[derive(Debug, PartialEq, Eq)]
struct DecodedComputeRequest {
    node_ids: Vec<String>,
    flags: u8,
    timeout_ms: i64,
    task_name: String,
    arg: Option<i32>,
}

fn decode_compute_execute_payload(payload: &[u8]) -> DecodedComputeRequest {
    let mut cursor = Cursor::new(payload);
    let node_count = ignite_rs::protocol::read_i32(&mut cursor).unwrap();
    let mut node_ids = Vec::new();
    for _ in 0..node_count {
        let most = ignite_rs::protocol::read_i64(&mut cursor).unwrap();
        let least = ignite_rs::protocol::read_i64(&mut cursor).unwrap();
        node_ids.push(MockUuid::new(most, least).as_string());
    }
    let flags = ignite_rs::protocol::read_u8(&mut cursor).unwrap();
    let timeout_ms = ignite_rs::protocol::read_i64(&mut cursor).unwrap();
    let task_name = ignite_rs::protocol::read_string(&mut cursor).unwrap();
    let arg = i32::read(&mut cursor).unwrap();

    DecodedComputeRequest {
        node_ids,
        flags,
        timeout_ms,
        task_name,
        arg,
    }
}
