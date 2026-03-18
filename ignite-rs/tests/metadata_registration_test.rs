#![cfg(not(feature = "ssl"))]

mod common;

use common::{connect, unique_name};
use ignite_rs::binary::{BinaryFieldMetadata, BinarySchema, BinaryTypeMetadata};

/// Java parity: org.apache.ignite.internal.client.thin.MetadataRegistrationTest#testBinaryMeta
#[tokio::test]
async fn should_cache_put_binary_type_metadata_locally_after_registration() {
    let client = connect().await.unwrap();
    let binary = client.binary();
    let type_name = unique_name("cached_type_meta");
    let type_id = binary.type_id(&type_name);

    let meta = BinaryTypeMetadata {
        type_id,
        type_name: type_name.clone(),
        affinity_key_field_name: None,
        fields: vec![BinaryFieldMetadata {
            name: "name".to_string(),
            type_id: 9,
            field_id: 12,
        }],
        is_enum: false,
        enum_values: Vec::new(),
        schemas: vec![BinarySchema {
            id: 12,
            field_ids: vec![12],
        }],
    };

    binary.put_type(&meta).await.unwrap();
    let cached = binary.get_type(type_id).await.unwrap().unwrap();

    assert_eq!(cached, meta);
}

/// Java parity: org.apache.ignite.internal.client.thin.MetadataRegistrationTest#testMapping
#[tokio::test]
async fn should_cache_binary_type_name_locally_after_registration() {
    let client = connect().await.unwrap();
    let binary = client.binary();
    let type_name = unique_name("cached_type_name");
    let type_id = binary.type_id(&type_name);

    assert!(binary
        .register_type_name(type_id, &type_name)
        .await
        .unwrap());
    assert_eq!(binary.get_type_name(type_id).await.unwrap(), type_name);
}
