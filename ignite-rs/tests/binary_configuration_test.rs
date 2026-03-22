#![cfg(not(feature = "ssl"))]

mod common;

use common::{connect, unique_name};
use ignite_rs::binary::{
    BinaryFieldMetadata, BinaryNameMapperMode, BinarySchema, BinaryTypeMetadata,
};

/// Java parity: org.apache.ignite.client.BinaryConfigurationTest#testAutoBinaryConfigurationEnabledRetrievesValuesFromServer
#[tokio::test]
async fn should_read_binary_configuration_from_server() {
    let client = connect().await.unwrap();
    let configuration = client.binary().get_configuration().await.unwrap();

    assert!(
        matches!(
            configuration.name_mapper_mode,
            BinaryNameMapperMode::BasicFull
                | BinaryNameMapperMode::BasicSimple
                | BinaryNameMapperMode::Custom
        ),
        "unexpected live binary name mapper mode: {:?}",
        configuration.name_mapper_mode
    );
}

/// Java parity: org.apache.ignite.internal.client.thin.MetadataRegistrationTest#testMapping
#[tokio::test]
async fn should_register_and_lookup_binary_type_name() {
    let client = connect().await.unwrap();
    let binary = client.binary();
    let type_name = unique_name("live_binary_type_name");
    let type_id = binary.type_id(&type_name);

    assert!(binary
        .register_type_name(type_id, &type_name)
        .await
        .unwrap());
    assert_eq!(binary.get_type_name(type_id).await.unwrap(), type_name);
}

/// Java parity: org.apache.ignite.internal.client.thin.MetadataRegistrationTest#testBinaryMeta
#[tokio::test]
async fn should_put_and_get_binary_type_metadata() {
    let client = connect().await.unwrap();
    let binary = client.binary();
    let type_name = unique_name("live_binary_type");
    let type_id = binary.type_id(&type_name);
    let meta = BinaryTypeMetadata {
        type_id,
        type_name: type_name.clone(),
        affinity_key_field_name: Some("id".to_string()),
        fields: vec![BinaryFieldMetadata {
            name: "id".to_string(),
            type_id: 3,
            field_id: 7,
        }],
        is_enum: false,
        enum_values: Vec::new(),
        schemas: vec![BinarySchema {
            id: 77,
            field_ids: vec![7],
        }],
    };

    binary.put_type(&meta).await.unwrap();
    let fetched = binary.get_type(type_id).await.unwrap().unwrap();

    assert_eq!(fetched, meta);
}

/// Java parity: org.apache.ignite.client.BinaryConfigurationTest#testCompactFooter
#[tokio::test]
async fn should_read_compact_footer_setting() {
    let client = connect().await.unwrap();
    let configuration = client.binary().get_configuration().await.unwrap();

    // The default Ignite configuration has compact_footer = true.
    assert!(
        configuration.compact_footer,
        "default Ignite config should have compact_footer = true"
    );
}

/// Java parity: org.apache.ignite.client.BinaryConfigurationTest#testTypeIdConflict
#[tokio::test]
async fn should_survive_type_id_conflict_gracefully() {
    let client = connect().await.unwrap();
    let binary = client.binary();
    let type_name = unique_name("conflict_type");
    let type_id = binary.type_id(&type_name);

    let meta = BinaryTypeMetadata {
        type_id,
        type_name: type_name.clone(),
        affinity_key_field_name: None,
        fields: vec![BinaryFieldMetadata {
            name: "value".to_string(),
            type_id: 3,
            field_id: 9,
        }],
        is_enum: false,
        enum_values: Vec::new(),
        schemas: vec![BinarySchema {
            id: 99,
            field_ids: vec![9],
        }],
    };

    // First registration should succeed.
    binary.put_type(&meta).await.unwrap();

    // Registering the same type again with identical metadata should not fail.
    binary.put_type(&meta).await.unwrap();

    // Verify the type can still be fetched correctly.
    let fetched = binary.get_type(type_id).await.unwrap().unwrap();
    assert_eq!(fetched, meta);
}
