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
    assert_eq!(binary.get_type(binary.type_id(&type_name)).await.unwrap(), Some(meta));
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

fn person_binary_object() -> BinaryObject {
    ignite_rs::binary::BinaryObjectBuilder::new("Person")
        .set_field("id", 1i32)
        .set_field("name", "Jane")
        .build()
}
