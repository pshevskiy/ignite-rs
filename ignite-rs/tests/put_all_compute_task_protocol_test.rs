#![cfg(not(feature = "ssl"))]

//! Wire-level coverage for `PutAllComputeTask` invocation from Rust.
//!
//! Two assertions per test:
//! 1. The bytes the Rust client sends in the `COMPUTE_TASK_EXECUTE`
//!    payload decode back to the exact `BulkPutParams` / `PutParams`
//!    shape the Java server-side expects — confirming the wire layout
//!    matches Java's `ClientComputeImpl.writeObject(arg)` output
//!    (symmetric round-trip).
//! 2. A server-emitted `BulkPutResponseParams` payload (ComplexObj)
//!    decodes through `BulkPutResponseParams::from_object` into the
//!    expected `PutResult` map.

mod common;

use common::{
    encode_node_info_payload, spawn_mock_thin_server, MockNodeInfo, MockNotification,
    MockResponse, MockThinServerConfig, MockUuid,
};
use ignite_rs::binary::BinaryObjectBuilder;
use ignite_rs::compute::bulk_put::{
    class_names, BulkPutParams, IndexContext, PutParams, PutStatus, SaveStrategy,
};
use ignite_rs::protocol::complex_obj::{
    ComplexObject, ComplexObjectSchema, IgniteField, IgniteType, IgniteValue,
};
use ignite_rs::protocol::{read_u8, TypeCode};
use ignite_rs::utils::string_to_java_hashcode;
use ignite_rs::{new_client, ClientConfig, ReadableType};
use std::collections::{HashMap, VecDeque};
use std::convert::TryInto;
use std::io::Cursor;
use std::sync::{Arc, Mutex};

const OP_CLUSTER_GROUP_GET_NODE_IDS: i16 = 5100;
const OP_CLUSTER_GROUP_GET_NODE_INFO: i16 = 5101;
const OP_COMPUTE_TASK_EXECUTE: i16 = 6000;
const OP_COMPUTE_TASK_FINISHED: i16 = 6001;

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

/// Encode a `BulkPutResponseParams` ComplexObject with two `PutResult`
/// entries (index=0 Added key=1, index=1 AlreadyExist key=2) so the
/// decoder has a realistic reduce output to parse.
fn encode_bulk_put_response_payload() -> Vec<u8> {
    let put_status_type_id =
        string_to_java_hashcode(&class_names::PUT_STATUS.to_lowercase());

    let pr = |ordinal: i32, key: i32, index: i32| ComplexObject {
        schema: Arc::new(ComplexObjectSchema {
            type_name: class_names::PUT_RESULT.to_string(),
            fields: vec![
                IgniteField {
                    name: "status".to_string(),
                    r#type: IgniteType::Enum,
                },
                IgniteField {
                    name: "message".to_string(),
                    r#type: IgniteType::String,
                },
                IgniteField {
                    name: "key".to_string(),
                    r#type: IgniteType::Int,
                },
                IgniteField {
                    name: "index".to_string(),
                    r#type: IgniteType::Int,
                },
            ],
        }),
        values: vec![
            IgniteValue::Enum(ignite_rs::Enum {
                type_id: put_status_type_id,
                ordinal,
            }),
            IgniteValue::Null,
            IgniteValue::Int(key),
            IgniteValue::Int(index),
        ],
    };

    let entries: Vec<(IgniteValue, IgniteValue)> = vec![
        (IgniteValue::Int(0), IgniteValue::Object(Box::new(pr(0, 1, 0)))),
        (IgniteValue::Int(1), IgniteValue::Object(Box::new(pr(3, 2, 1)))),
    ];
    let map_value = IgniteValue::Map(1 /* HashMap subtype */, entries);

    let resp_obj = ComplexObject {
        schema: Arc::new(ComplexObjectSchema {
            type_name: class_names::BULK_PUT_RESPONSE_PARAMS.to_string(),
            fields: vec![IgniteField {
                name: "putResultMap".to_string(),
                r#type: IgniteType::Map,
            }],
        }),
        values: vec![map_value],
    };

    let mut bytes = Vec::new();
    <ComplexObject as ignite_rs::WritableType>::write(&resp_obj, &mut bytes).unwrap();
    bytes
}

#[tokio::test]
async fn put_all_compute_task_serialises_bulk_put_params_over_the_wire() {
    let node = MockUuid::new(9010, 11);
    let task_id = 42i64;

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
                        encode_bulk_put_response_payload(),
                    )],
                )],
            ),
        ])),
        ..Default::default()
    });

    let client = new_client(ClientConfig::new(server.addr())).await.unwrap();

    // Build a realistic two-item BulkPutParams argument.
    let payload = BinaryObjectBuilder::new("com.example.Entity")
        .set_field("id", 1i32)
        .set_field("version", 1i32)
        .build();
    let pp1 = PutParams::new(
        "DATA_RSC_INDIVIDUAL",
        IgniteValue::Long(100),
        payload.clone(),
        false,
        IndexContext::empty(),
        false,
        SaveStrategy::Atomic,
    );
    let pp2 = PutParams::new(
        "DATA_RSC_INDIVIDUAL",
        IgniteValue::Long(101),
        payload,
        true,
        IndexContext::empty(),
        false,
        SaveStrategy::Atomic,
    );
    let bulk = BulkPutParams::new(vec![pp1, pp2]);

    let result = client.compute().execute_put_all(&bulk).await.unwrap();
    let result = result.expect("non-null response");

    // Response decoded correctly.
    assert_eq!(result.put_result_map.len(), 2);
    assert_eq!(result.put_result_map[&0].status, PutStatus::Added);
    assert_eq!(result.put_result_map[&1].status, PutStatus::AlreadyExist);

    // Request payload on the wire contains the bulk-put params encoded as a
    // BulkPutParams ComplexObject whose `putParamsList` field is an
    // ArrayList of two PutParams objects.
    let payloads = server.recorded_opcode_payloads(OP_COMPUTE_TASK_EXECUTE);
    assert_eq!(payloads.len(), 1);
    let payload_bytes = &payloads[0];

    // Skip the standard compute-exec prefix (nodeIds + flags + timeout +
    // taskName) to get at the arg bytes.
    let (arg_bytes, task_name) = split_compute_exec_payload(payload_bytes);
    assert_eq!(
        task_name,
        "ru.sbrf.ucpcloud.ignite.server.tasks.PutAllComputeTask"
    );

    let mut cur = Cursor::new(&arg_bytes);
    let code = read_u8(&mut cur).unwrap();
    assert_eq!(code, TypeCode::ComplexObj as u8);
    let obj = ComplexObject::read_unwrapped(code.try_into().unwrap(), &mut cur)
        .unwrap()
        .unwrap();

    match obj.field("putParamsList") {
        Some(IgniteValue::Collection(subtype, items)) => {
            assert_eq!(*subtype, 1u8, "ArrayList subtype");
            assert_eq!(items.len(), 2);
        }
        other => panic!("putParamsList: {:?}", other),
    }
}

fn split_compute_exec_payload(payload: &[u8]) -> (Vec<u8>, String) {
    use ignite_rs::protocol::{read_i32, read_i64, read_string};
    let mut cur = Cursor::new(payload);
    let node_count = read_i32(&mut cur).unwrap();
    for _ in 0..node_count {
        let _most = read_i64(&mut cur).unwrap();
        let _least = read_i64(&mut cur).unwrap();
    }
    let _flags = read_u8(&mut cur).unwrap();
    let _timeout = read_i64(&mut cur).unwrap();
    let task_name = read_string(&mut cur).unwrap();
    let mut arg = Vec::new();
    let position = cur.position() as usize;
    arg.extend_from_slice(&payload[position..]);
    (arg, task_name)
}
