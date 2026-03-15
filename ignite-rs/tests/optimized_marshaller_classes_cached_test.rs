#![cfg(not(feature = "ssl"))]

mod common;

use common::connect;

/// Related Apache Ignite binary type-name cache coverage:
/// org.apache.ignite.internal.client.thin.OptimizedMarshallerClassesCachedTest#testLocalDateTimeMetaCached
#[tokio::test]
async fn should_return_consistent_binary_type_name_across_repeated_lookups() {
    let client = connect().await.unwrap();
    let binary = client.binary();
    let type_name = "java.time.LocalDateTime";
    let type_id = binary.type_id(type_name);

    assert!(binary.register_type_name(type_id, type_name).await.unwrap());
    assert_eq!(binary.get_type_name(type_id).await.unwrap(), type_name);
    assert_eq!(binary.get_type_name(type_id).await.unwrap(), type_name);
}
