#![cfg(not(feature = "ssl"))]

mod common;

use common::{
    encode_typed_payload, spawn_mock_thin_server, MockResponse, MockThinServerConfig, MockUuid,
};
use ignite_rs::protocol::complex_obj::IgniteValue;
use ignite_rs::services::ServiceCallContext;
use ignite_rs::{new_client, ClientConfig, ReadableType};
use std::collections::{HashMap, VecDeque};
use std::convert::TryFrom;
use std::io::Cursor;
use std::sync::{Arc, Mutex};

const OP_CLUSTER_GROUP_GET_NODE_IDS: i16 = 5100;
const OP_CLUSTER_GROUP_GET_NODE_INFO: i16 = 5101;
const OP_SERVICE_INVOKE: i16 = 7000;
const OP_SERVICE_GET_DESCRIPTORS: i16 = 7001;
const OP_SERVICE_GET_DESCRIPTOR: i16 = 7002;
const OP_SERVICE_GET_TOPOLOGY: i16 = 7003;

/// Migrated from Apache Ignite `ServicesTest.testServiceDescriptors`:
/// Java source: org.apache.ignite.internal.client.thin.ServicesTest
#[tokio::test]
async fn should_get_service_descriptors() {
    let node = MockUuid::new(5001, 5001);
    let server = spawn_mock_thin_server(MockThinServerConfig {
        opcode_responses: Some(opcode_responses(vec![
            (
                OP_SERVICE_GET_DESCRIPTORS,
                vec![MockResponse::success(encode_service_descriptors(&[
                    ServiceDescriptorSpec {
                        name: "svcA",
                        class_name: "org.example.ServiceA",
                        total_count: 1,
                        max_per_node_count: 1,
                        cache_name: "cacheA",
                        origin: node,
                        platform: 0,
                    },
                    ServiceDescriptorSpec {
                        name: "svcB",
                        class_name: "org.example.ServiceB",
                        total_count: 2,
                        max_per_node_count: 1,
                        cache_name: "cacheB",
                        origin: node,
                        platform: 1,
                    },
                ]))],
            ),
            (
                OP_SERVICE_GET_DESCRIPTOR,
                vec![MockResponse::success(encode_service_descriptor(
                    &ServiceDescriptorSpec {
                        name: "svcA",
                        class_name: "org.example.ServiceA",
                        total_count: 1,
                        max_per_node_count: 1,
                        cache_name: "cacheA",
                        origin: node,
                        platform: 0,
                    },
                ))],
            ),
        ])),
        ..Default::default()
    });

    let mut conf = ClientConfig::new(server.addr());
    conf.partition_awareness_enabled = false;
    let client = new_client(conf).await.unwrap();

    let descriptors = client.services().service_descriptors().await.unwrap();
    assert_eq!(descriptors.len(), 2);
    assert_eq!(descriptors[0].name, "svcA");
    assert_eq!(descriptors[1].name, "svcB");

    let descriptor = client.services().service_descriptor("svcA").await.unwrap();
    assert_eq!(descriptor.class_name, "org.example.ServiceA");
    assert_eq!(descriptor.origin_node_id, node.as_string());
}

/// Migrated from Apache Ignite `ServicesTest.testServiceCallContext` and `testServicesOnClusterGroup`:
/// Java source: org.apache.ignite.internal.client.thin.ServicesTest
#[tokio::test]
async fn should_invoke_service_with_cluster_group_timeout_and_call_context() {
    let node = MockUuid::new(6001, 6001);
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
                vec![MockResponse::success(encode_typed_payload(
                    &"pong".to_string(),
                ))],
            ),
        ])),
        ..Default::default()
    });

    let client = new_client(ClientConfig::new(server.addr())).await.unwrap();
    let group = client.cluster().for_node_id(node.as_string());
    let context = ServiceCallContext::new()
        .with_attribute("name", "alice")
        .with_binary_attribute("bin", vec![1, 2, 3]);

    let result = client
        .services()
        .with_cluster_group(group)
        .service("svc")
        .with_timeout_ms(250)
        .with_call_context(context)
        .invoke::<String>("echo", &[IgniteValue::from("ping")])
        .await
        .unwrap();

    assert_eq!(result, Some("pong".to_string()));

    let payloads = server.recorded_opcode_payloads(OP_SERVICE_INVOKE);
    assert_eq!(payloads.len(), 1);

    let decoded = decode_service_invoke_payload(&payloads[0]);
    assert_eq!(decoded.service_name, "svc");
    assert_eq!(decoded.timeout_ms, 250);
    assert_eq!(decoded.method_name, "echo");
    assert_eq!(decoded.node_ids, vec![node.as_string()]);
    assert_eq!(decoded.arg0, Some("ping".to_string()));
    assert_eq!(
        decoded.call_ctx.get("name"),
        Some(&CtxValue::String("alice".into()))
    );
    assert_eq!(
        decoded.call_ctx.get("bin"),
        Some(&CtxValue::Binary(vec![1, 2, 3]))
    );
}

#[derive(Clone)]
struct ServiceDescriptorSpec {
    name: &'static str,
    class_name: &'static str,
    total_count: i32,
    max_per_node_count: i32,
    cache_name: &'static str,
    origin: MockUuid,
    platform: u8,
}

fn encode_service_descriptors(specs: &[ServiceDescriptorSpec]) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&(specs.len() as i32).to_le_bytes());
    for spec in specs {
        payload.extend_from_slice(&encode_service_descriptor(spec));
    }
    payload
}

fn encode_service_descriptor(spec: &ServiceDescriptorSpec) -> Vec<u8> {
    let mut payload = Vec::new();
    write_raw_string(&mut payload, spec.name);
    write_raw_string(&mut payload, spec.class_name);
    payload.extend_from_slice(&spec.total_count.to_le_bytes());
    payload.extend_from_slice(&spec.max_per_node_count.to_le_bytes());
    write_raw_string(&mut payload, spec.cache_name);
    payload.extend_from_slice(&spec.origin.most.to_le_bytes());
    payload.extend_from_slice(&spec.origin.least.to_le_bytes());
    payload.push(spec.platform);
    payload
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

fn encode_service_topology(nodes: &[MockUuid]) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&(nodes.len() as i32).to_le_bytes());
    for node in nodes {
        payload.extend_from_slice(&node.most.to_le_bytes());
        payload.extend_from_slice(&node.least.to_le_bytes());
    }
    payload
}

fn write_raw_string(payload: &mut Vec<u8>, value: &str) {
    payload.extend_from_slice(&(value.len() as i32).to_le_bytes());
    payload.extend_from_slice(value.as_bytes());
}

fn encode_typed_to<T: ignite_rs::WritableType>(payload: &mut Vec<u8>, value: &T) {
    value.write(payload).unwrap();
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
struct DecodedInvokePayload {
    service_name: String,
    timeout_ms: i64,
    node_ids: Vec<String>,
    method_name: String,
    arg0: Option<String>,
    call_ctx: HashMap<String, CtxValue>,
}

#[derive(Debug, PartialEq, Eq)]
enum CtxValue {
    String(String),
    Binary(Vec<u8>),
}

fn decode_service_invoke_payload(payload: &[u8]) -> DecodedInvokePayload {
    let mut cursor = Cursor::new(payload);
    let service_name = ignite_rs::protocol::read_string(&mut cursor).unwrap();
    let _flags = ignite_rs::protocol::read_u8(&mut cursor).unwrap();
    let timeout_ms = ignite_rs::protocol::read_i64(&mut cursor).unwrap();
    let node_count = ignite_rs::protocol::read_i32(&mut cursor).unwrap();
    let mut node_ids = Vec::new();
    for _ in 0..node_count {
        let most = ignite_rs::protocol::read_i64(&mut cursor).unwrap();
        let least = ignite_rs::protocol::read_i64(&mut cursor).unwrap();
        node_ids.push(MockUuid::new(most, least).as_string());
    }
    let method_name = ignite_rs::protocol::read_string(&mut cursor).unwrap();
    let arg_count = ignite_rs::protocol::read_i32(&mut cursor).unwrap();
    let arg0 = if arg_count > 0 {
        String::read(&mut cursor).unwrap()
    } else {
        None
    };
    let call_ctx_count = ignite_rs::protocol::read_i32(&mut cursor).unwrap();
    let mut call_ctx = HashMap::new();
    if call_ctx_count >= 0 {
        for _ in 0..call_ctx_count {
            let key = String::read(&mut cursor).unwrap().unwrap();
            let type_code = ignite_rs::protocol::read_u8(&mut cursor).unwrap();
            let value = match ignite_rs::protocol::TypeCode::try_from(type_code).unwrap() {
                ignite_rs::protocol::TypeCode::String => {
                    CtxValue::String(ignite_rs::protocol::read_string(&mut cursor).unwrap())
                }
                ignite_rs::protocol::TypeCode::ArrByte => {
                    let len = ignite_rs::protocol::read_i32(&mut cursor).unwrap() as usize;
                    let mut bytes = vec![0u8; len];
                    std::io::Read::read_exact(&mut cursor, &mut bytes).unwrap();
                    CtxValue::Binary(bytes)
                }
                other => panic!("unexpected call context type {:?}", other),
            };
            call_ctx.insert(key, value);
        }
    }

    DecodedInvokePayload {
        service_name,
        timeout_ms,
        node_ids,
        method_name,
        arg0,
        call_ctx,
    }
}
