#![cfg(not(feature = "ssl"))]

mod common;

use common::{connect, unique_name};
use ignite_rs::binary::{BinaryEnumVariant, BinaryObject};
use ignite_rs::protocol::complex_obj::IgniteValue;
use ignite_rs::{ReadableType, WritableType};
use ignite_rs_derive::IgniteObj;
use std::io::Cursor;

#[derive(Debug, Clone, PartialEq, IgniteObj)]
struct Person {
    id: i32,
    name: String,
}

/// Related Apache Ignite binary-object cache coverage:
/// org.apache.ignite.client.IgniteBinaryTest#testBinaryObjectPutGet
#[tokio::test]
async fn should_build_and_read_binary_object_fields() {
    let ignite = connect().await.unwrap();
    let cache_name = unique_name("binary_object_cache");
    let cache = ignite
        .get_or_create_cache::<i32, BinaryObject>(&cache_name)
        .await
        .unwrap();

    cache.put(&1, &person_binary_object()).await.unwrap();
    let value = cache.get(&1).await.unwrap().unwrap();

    assert_eq!(value.type_name(), "Person");
    assert_eq!(value.field("id"), Some(&IgniteValue::Int(1)));
    assert_eq!(
        value.field("name"),
        Some(&IgniteValue::String("Jane".to_string()))
    );

    ignite.destroy_cache(&cache_name).await.unwrap();
}

/// Related Apache Ignite binary-object conversion coverage:
/// org.apache.ignite.client.IgniteBinaryTest#testBinaryObjectApi
#[tokio::test]
async fn should_convert_ignite_obj_into_real_binary_object() {
    let ignite = connect().await.unwrap();

    let value = ignite
        .binary()
        .to_binary(Person {
            id: 7,
            name: "Joe".to_string(),
        })
        .unwrap();

    assert_eq!(value.type_name(), "Person");
    assert_eq!(value.field("id"), Some(&IgniteValue::Int(7)));
    assert_eq!(
        value.field("name"),
        Some(&IgniteValue::String("Joe".to_string()))
    );
}

/// Related Apache Ignite enum registration/build coverage:
/// org.apache.ignite.client.IgniteBinaryTest#testBinaryObjectApi
#[tokio::test]
async fn should_register_and_build_binary_enums() {
    let ignite = connect().await.unwrap();
    let binary = ignite.binary();
    let type_name = unique_name("enum_type");

    let meta = binary
        .register_enum(
            &type_name,
            &[
                BinaryEnumVariant {
                    name: "DEFAULT".to_string(),
                    ordinal: 0,
                },
                BinaryEnumVariant {
                    name: "OTHER".to_string(),
                    ordinal: 1,
                },
            ],
        )
        .await
        .unwrap();

    assert!(meta.is_enum);
    assert_eq!(meta.enum_values.len(), 2);
    assert_eq!(binary.build_enum(&type_name, 1).enum_ordinal(), Some(1));
    assert_eq!(
        binary
            .build_enum_name(&type_name, "DEFAULT")
            .unwrap()
            .enum_ordinal(),
        Some(0)
    );
    assert_eq!(
        binary.get_type(binary.type_id(&type_name)).await.unwrap(),
        Some(meta)
    );
}

/// Related Apache Ignite nested binary-object coverage:
/// org.apache.ignite.client.IgniteBinaryTest#testCompactFooterModifiedSchemaRegistration
#[test]
fn should_round_trip_nested_binary_objects_and_object_arrays() {
    let child = person_binary_object();
    let parent = ignite_rs::binary::BinaryObjectBuilder::new("Wrapper")
        .set_field_value("child", IgniteValue::from(child.clone()))
        .set_field_value(
            "children",
            IgniteValue::Array(vec![IgniteValue::from(child.clone())]),
        )
        .build();

    let mut bytes = Vec::new();
    parent.write(&mut bytes).unwrap();

    let mut cursor = Cursor::new(bytes);
    let decoded = BinaryObject::read(&mut cursor).unwrap().unwrap();

    match decoded.field("child") {
        Some(IgniteValue::Object(value)) => {
            assert_eq!(value.type_name(), "Person");
            assert_eq!(value.field("id"), Some(&IgniteValue::Int(1)));
        }
        other => panic!("unexpected nested child value: {:?}", other),
    }

    match decoded.field("children") {
        Some(IgniteValue::Array(values)) => {
            assert_eq!(values.len(), 1);
            match &values[0] {
                IgniteValue::Object(value) => {
                    assert_eq!(value.type_name(), "Person");
                    assert_eq!(
                        value.field("name"),
                        Some(&IgniteValue::String("Jane".to_string()))
                    );
                }
                other => panic!("unexpected array element: {:?}", other),
            }
        }
        other => panic!("unexpected children field: {:?}", other),
    }
}

/// Java parity: org.apache.ignite.client.IgniteBinaryTest#testBinaryObjectNullFields
#[tokio::test]
async fn should_handle_null_fields_in_binary_object() {
    let ignite = connect().await.unwrap();
    let cache_name = unique_name("binary_null_cache");
    let cache = ignite
        .get_or_create_cache::<i32, BinaryObject>(&cache_name)
        .await
        .unwrap();

    let obj = ignite_rs::binary::BinaryObjectBuilder::new("NullablePerson")
        .set_field("id", 1i32)
        .set_field_value("nullable", IgniteValue::Null)
        .set_field("name", "Alice")
        .build();

    cache.put(&1, &obj).await.unwrap();
    let value = cache.get(&1).await.unwrap().unwrap();

    assert_eq!(value.type_name(), "NullablePerson");
    assert_eq!(value.field("id"), Some(&IgniteValue::Int(1)));
    assert_eq!(value.field("nullable"), Some(&IgniteValue::Null));
    assert_eq!(
        value.field("name"),
        Some(&IgniteValue::String("Alice".to_string()))
    );

    ignite.destroy_cache(&cache_name).await.unwrap();
}

/// Java parity: org.apache.ignite.client.IgniteBinaryTest#testBinaryObjectEnumField
#[tokio::test]
async fn should_support_enum_field_in_binary_object() {
    let ignite = connect().await.unwrap();
    let binary = ignite.binary();
    let enum_type_name = unique_name("enum_field_type");
    let cache_name = unique_name("binary_enum_field_cache");

    binary
        .register_enum(
            &enum_type_name,
            &[
                BinaryEnumVariant {
                    name: "ACTIVE".to_string(),
                    ordinal: 0,
                },
                BinaryEnumVariant {
                    name: "INACTIVE".to_string(),
                    ordinal: 1,
                },
            ],
        )
        .await
        .unwrap();

    let enum_value = binary.build_enum(&enum_type_name, 1);
    let obj = ignite_rs::binary::BinaryObjectBuilder::new("EnumHolder")
        .set_field("id", 42i32)
        .set_field_value("status", IgniteValue::from(enum_value.clone()))
        .build();

    let cache = ignite
        .get_or_create_cache::<i32, BinaryObject>(&cache_name)
        .await
        .unwrap();

    cache.put(&1, &obj).await.unwrap();
    let value = cache.get(&1).await.unwrap().unwrap();

    assert_eq!(value.field("id"), Some(&IgniteValue::Int(42)));
    match value.field("status") {
        Some(IgniteValue::Object(inner)) => {
            assert_eq!(inner.enum_ordinal(), Some(1));
        }
        Some(IgniteValue::Enum(e)) => {
            assert_eq!(e.ordinal, 1);
        }
        other => panic!("unexpected status field value: {:?}", other),
    }

    ignite.destroy_cache(&cache_name).await.unwrap();
}

/// Java parity: org.apache.ignite.client.IgniteBinaryTest#testBinaryObjectHashCode
#[test]
fn should_compute_binary_object_hash_code() {
    let obj_a = ignite_rs::binary::BinaryObjectBuilder::new("HashPerson")
        .set_field("id", 1i32)
        .set_field("name", "Jane")
        .build();

    let obj_b = ignite_rs::binary::BinaryObjectBuilder::new("HashPerson")
        .set_field("id", 1i32)
        .set_field("name", "Jane")
        .build();

    let obj_c = ignite_rs::binary::BinaryObjectBuilder::new("HashPerson")
        .set_field("id", 2i32)
        .set_field("name", "Bob")
        .build();

    // Same fields should produce the same serialized bytes and thus the same hash.
    let bytes_a = {
        let mut buf = Vec::new();
        obj_a.write(&mut buf).unwrap();
        buf
    };
    let bytes_b = {
        let mut buf = Vec::new();
        obj_b.write(&mut buf).unwrap();
        buf
    };
    let bytes_c = {
        let mut buf = Vec::new();
        obj_c.write(&mut buf).unwrap();
        buf
    };

    assert_eq!(
        bytes_a, bytes_b,
        "identical objects should serialize identically"
    );
    assert_ne!(
        bytes_a, bytes_c,
        "different objects should serialize differently"
    );

    // Additionally verify type_id is consistent.
    assert_eq!(obj_a.type_name(), obj_b.type_name());
    assert_eq!(obj_a.type_name(), obj_c.type_name());
}

/// Java parity: org.apache.ignite.client.IgniteBinaryTest#testCompactFooterModifiedSchemaRegistration
#[tokio::test]
async fn should_handle_schema_evolution_via_field_addition() {
    let ignite = connect().await.unwrap();
    let cache_name = unique_name("schema_evolution_cache");
    let cache = ignite
        .get_or_create_cache::<i32, BinaryObject>(&cache_name)
        .await
        .unwrap();

    let type_name = unique_name("EvolvingType");

    // Put object with initial schema: {id, name}
    let obj_v1 = ignite_rs::binary::BinaryObjectBuilder::new(&type_name)
        .set_field("id", 1i32)
        .set_field("name", "original")
        .build();
    cache.put(&1, &obj_v1).await.unwrap();

    // Put object with extended schema: {id, name, extra}
    let obj_v2 = ignite_rs::binary::BinaryObjectBuilder::new(&type_name)
        .set_field("id", 2i32)
        .set_field("name", "extended")
        .set_field("extra", "bonus")
        .build();
    cache.put(&2, &obj_v2).await.unwrap();

    // Read back the old entry — it should not have the "extra" field.
    let fetched_v1 = cache.get(&1).await.unwrap().unwrap();
    assert_eq!(fetched_v1.field("id"), Some(&IgniteValue::Int(1)));
    assert_eq!(
        fetched_v1.field("name"),
        Some(&IgniteValue::String("original".to_string()))
    );
    assert_eq!(fetched_v1.field("extra"), None);

    // Read back the new entry — it should have the "extra" field.
    let fetched_v2 = cache.get(&2).await.unwrap().unwrap();
    assert_eq!(fetched_v2.field("id"), Some(&IgniteValue::Int(2)));
    assert_eq!(
        fetched_v2.field("name"),
        Some(&IgniteValue::String("extended".to_string()))
    );
    assert_eq!(
        fetched_v2.field("extra"),
        Some(&IgniteValue::String("bonus".to_string()))
    );

    ignite.destroy_cache(&cache_name).await.unwrap();
}

fn person_binary_object() -> BinaryObject {
    ignite_rs::binary::BinaryObjectBuilder::new("Person")
        .set_field("id", 1i32)
        .set_field("name", "Jane")
        .build()
}
