#![cfg(not(feature = "ssl"))]

mod common;

use common::{encode_typed_payload, spawn_mock_thin_server, MockResponse, MockThinServerConfig};
use ignite_rs::binary::BinaryObject;
use ignite_rs::invoke::InvokeAllResult;
use ignite_rs::protocol::complex_obj::IgniteValue;
use ignite_rs::{new_client, ClientConfig, ReadableType, WritableType};
use ignite_rs_derive::IgniteObj;
use std::collections::{HashMap, VecDeque};
use std::io::Cursor;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

const OP_CACHE_INVOKE: i16 = 1024;
const OP_CACHE_INVOKE_ALL: i16 = 1025;

#[derive(Debug, Clone, PartialEq, IgniteObj)]
struct Person {
    id: i32,
    name: String,
}

struct CountingKey {
    value: i32,
    writes: Arc<AtomicUsize>,
}

impl CountingKey {
    fn new(value: i32) -> Self {
        Self {
            value,
            writes: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn writes(&self) -> usize {
        self.writes.load(Ordering::Relaxed)
    }
}

impl WritableType for CountingKey {
    fn write(&self, writer: &mut dyn std::io::Write) -> std::io::Result<()> {
        self.writes.fetch_add(1, Ordering::Relaxed);
        self.value.write(writer)
    }

    fn size(&self) -> usize {
        self.value.size()
    }
}

impl ReadableType for CountingKey {
    fn read_unwrapped(
        type_code: ignite_rs::protocol::TypeCode,
        reader: &mut impl std::io::Read,
    ) -> ignite_rs::error::IgniteResult<Option<Self>> {
        Ok(i32::read_unwrapped(type_code, reader)?.map(Self::new))
    }
}

/// Migrated from Apache Ignite `InvokeTest.testInvokeSimpleCase`:
/// Java source: org.apache.ignite.internal.client.thin.InvokeTest
#[tokio::test]
async fn should_invoke_binary_entry_processor_for_single_key() {
    let server = spawn_mock_thin_server(MockThinServerConfig {
        opcode_responses: Some(opcode_responses(vec![(
            OP_CACHE_INVOKE,
            vec![MockResponse::success(encode_typed_payload(&3i32))],
        )])),
        ..Default::default()
    });

    let mut conf = ClientConfig::new(server.addr());
    conf.partition_awareness_enabled = false;

    let ignite = new_client(conf).await.unwrap();
    let cache = ignite.cache::<i32, i32>("invoke-cache");
    let processor = ignite.binary().builder("IncrementProcessor").build();

    let result = cache
        .invoke_binary::<i32>(&1, &processor, &[IgniteValue::Int(1)])
        .await
        .unwrap();

    assert_eq!(result, Some(3));
}

/// Migrated from Apache Ignite `InvokeTest.testExceptionHandling` single-key path:
/// Java source: org.apache.ignite.internal.client.thin.InvokeTest
#[tokio::test]
async fn should_surface_single_invoke_errors() {
    let server = spawn_mock_thin_server(MockThinServerConfig {
        opcode_responses: Some(opcode_responses(vec![(
            OP_CACHE_INVOKE,
            vec![MockResponse::failure("Failed")],
        )])),
        ..Default::default()
    });

    let mut conf = ClientConfig::new(server.addr());
    conf.partition_awareness_enabled = false;

    let ignite = new_client(conf).await.unwrap();
    let cache = ignite.cache::<i32, i32>("invoke-cache");
    let processor = ignite.binary().builder("FailingEntryProcessor").build();

    let err = cache
        .invoke_binary::<i32>(&1, &processor, &[])
        .await
        .unwrap_err();

    assert!(err.to_string().contains("Failed"));
}

/// Migrated from Apache Ignite `InvokeTest.testInvokeAllSimpleCase` and `testExceptionHandling`:
/// Java source: org.apache.ignite.internal.client.thin.InvokeTest
#[tokio::test]
async fn should_decode_invoke_all_results_and_errors() {
    let server = spawn_mock_thin_server(MockThinServerConfig {
        opcode_responses: Some(opcode_responses(vec![(
            OP_CACHE_INVOKE_ALL,
            vec![MockResponse::success(encode_invoke_all_response(&[
                (1, true, Some(7), None),
                (2, false, None, Some("Failed")),
            ]))],
        )])),
        ..Default::default()
    });

    let mut conf = ClientConfig::new(server.addr());
    conf.partition_awareness_enabled = false;

    let ignite = new_client(conf).await.unwrap();
    let cache = ignite.cache::<i32, i32>("invoke-cache");
    let processor = ignite.binary().builder("IncrementProcessor").build();

    let results = cache
        .invoke_all_binary::<i32>(&[1, 2], &processor, &[])
        .await
        .unwrap();

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].0, Some(1));
    assert_eq!(results[0].1, InvokeAllResult::Value(Some(7)));
    assert_eq!(results[1].0, Some(2));
    assert_eq!(results[1].1, InvokeAllResult::Error("Failed".to_string()));
}

#[tokio::test]
async fn should_serialize_first_invoke_all_key_once() {
    let server = spawn_mock_thin_server(MockThinServerConfig {
        opcode_responses: Some(opcode_responses(vec![(
            OP_CACHE_INVOKE_ALL,
            vec![MockResponse::success(encode_invoke_all_response(&[
                (1, true, Some(7), None),
                (2, true, Some(9), None),
            ]))],
        )])),
        ..Default::default()
    });

    let mut conf = ClientConfig::new(server.addr());
    conf.partition_awareness_enabled = false;

    let ignite = new_client(conf).await.unwrap();
    let cache = ignite.cache::<CountingKey, i32>("invoke-cache");
    let processor = ignite.binary().builder("IncrementProcessor").build();
    let keys = [CountingKey::new(1), CountingKey::new(2)];

    let _ = cache
        .invoke_all_binary::<i32>(&keys, &processor, &[])
        .await
        .unwrap();

    assert_eq!(keys[0].writes(), 1);
    assert_eq!(keys[1].writes(), 1);
}

/// Migrated from Apache Ignite `InvokeTest.testSerialization` and `testWithKeepBinary` Rust-native binary path:
/// Java source: org.apache.ignite.internal.client.thin.InvokeTest
#[tokio::test]
async fn should_serialize_binary_object_arguments_and_decode_binary_results() {
    let server = spawn_mock_thin_server(MockThinServerConfig {
        opcode_responses: Some(opcode_responses(vec![(
            OP_CACHE_INVOKE,
            vec![MockResponse::success(encode_typed_payload(
                &person_binary_object(),
            ))],
        )])),
        ..Default::default()
    });

    let mut conf = ClientConfig::new(server.addr());
    conf.partition_awareness_enabled = false;

    let ignite = new_client(conf).await.unwrap();
    let binary = ignite.binary();
    let cache = ignite.cache::<i32, BinaryObject>("invoke-cache");
    let processor = binary.builder("BinaryObjectEntryProcessor").build();
    let person = binary
        .to_binary(Person {
            id: 3,
            name: "Joe".to_string(),
        })
        .unwrap();

    let result = cache
        .invoke_binary::<BinaryObject>(&1, &processor, &[IgniteValue::from(person.clone())])
        .await
        .unwrap()
        .unwrap();

    assert_eq!(result.type_name(), "Person");
    assert_eq!(result.field("id"), Some(&IgniteValue::Int(1)));

    let payload = server
        .recorded_opcode_payloads(OP_CACHE_INVOKE)
        .into_iter()
        .next()
        .unwrap();
    let mut cursor = Cursor::new(payload);
    let _cache_id = ignite_rs::protocol::read_i32(&mut cursor).unwrap();
    let flags = ignite_rs::protocol::read_u8(&mut cursor).unwrap();
    let key = i32::read(&mut cursor).unwrap().unwrap();
    let decoded_processor = BinaryObject::read(&mut cursor).unwrap().unwrap();
    let platform = ignite_rs::protocol::read_u8(&mut cursor).unwrap();
    let arg_count = ignite_rs::protocol::read_i32(&mut cursor).unwrap();
    let decoded_arg = BinaryObject::read(&mut cursor).unwrap().unwrap();

    assert_eq!(flags & 0x01, 0x01);
    assert_eq!(key, 1);
    assert_eq!(decoded_processor.type_name(), "BinaryObjectEntryProcessor");
    assert_eq!(platform, 1);
    assert_eq!(arg_count, 1);
    assert_eq!(decoded_arg.type_name(), "Person");
    assert_eq!(
        decoded_arg.field("name"),
        Some(&IgniteValue::String("Joe".to_string()))
    );
}

fn opcode_responses(
    entries: Vec<(i16, Vec<MockResponse>)>,
) -> Arc<Mutex<HashMap<i16, VecDeque<MockResponse>>>> {
    Arc::new(Mutex::new(
        entries
            .into_iter()
            .map(|(op_code, responses)| (op_code, responses.into_iter().collect()))
            .collect(),
    ))
}

fn encode_invoke_all_response(entries: &[(i32, bool, Option<i32>, Option<&str>)]) -> Vec<u8> {
    let mut payload = Vec::new();
    ignite_rs::protocol::write_i32(&mut payload, entries.len() as i32).unwrap();
    for (key, success, value, err) in entries {
        key.write(&mut payload).unwrap();
        ignite_rs::protocol::write_bool(&mut payload, *success).unwrap();
        if *success {
            value.unwrap().write(&mut payload).unwrap();
        } else {
            let message = err.unwrap_or_default();
            ignite_rs::protocol::write_i32(&mut payload, message.len() as i32).unwrap();
            payload.extend_from_slice(message.as_bytes());
        }
    }
    payload
}

fn person_binary_object() -> BinaryObject {
    ignite_rs::binary::BinaryObjectBuilder::new("Person")
        .set_field("id", 1i32)
        .set_field("name", "Jane")
        .build()
}
