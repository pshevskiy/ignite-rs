#![cfg(not(feature = "ssl"))]

mod common;

use common::{connect, unique_name};
use ignite_rs::cache::CacheMode;
use ignite_rs::data_structures::AtomicConfiguration;

/// Java parity: org.apache.ignite.internal.client.thin.AtomicLongTest#testCreateSetsInitialValue
#[tokio::test]
async fn should_create_atomic_long_and_read_initial_value() {
    let client = connect().await.unwrap();
    let atomic = client
        .atomic_long(&unique_name("counter"), 42, true)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(atomic.get().await.unwrap(), 42);
    atomic.close().await.unwrap();
}

/// Java parity: org.apache.ignite.internal.client.thin.AtomicLongTest#testCreateIgnoresInitialValueWhenAlreadyExists
#[tokio::test]
async fn should_ignore_initial_value_when_atomic_long_already_exists() {
    let client = connect().await.unwrap();
    let name = unique_name("counter_existing");
    let atomic = client.atomic_long(&name, 42, true).await.unwrap().unwrap();
    let atomic2 = client.atomic_long(&name, -42, true).await.unwrap().unwrap();

    assert_eq!(atomic.get().await.unwrap(), 42);
    assert_eq!(atomic2.get().await.unwrap(), 42);
    atomic.close().await.unwrap();
}

/// Java parity: org.apache.ignite.internal.client.thin.AtomicLongTest#testRemoved
/// Java parity: org.apache.ignite.internal.client.thin.AtomicLongTest#testIncrementDecrementAdd
#[tokio::test]
async fn should_add_set_compare_and_track_removed_state() {
    let client = connect().await.unwrap();
    let atomic = client
        .atomic_long(&unique_name("counter_ops"), 1, true)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(atomic.increment_and_get().await.unwrap(), 2);
    assert_eq!(atomic.get_and_set(10).await.unwrap(), 2);
    assert!(atomic.compare_and_set(10, 11).await.unwrap());
    assert!(!atomic.removed().await.unwrap());
    atomic.close().await.unwrap();
    assert!(atomic.removed().await.unwrap());
}

/// Java parity: org.apache.ignite.internal.client.thin.AtomicLongTest#testOperationsThrowExceptionWhenAtomicLongDoesNotExist
#[tokio::test]
async fn should_error_on_operations_after_atomic_long_is_removed() {
    let client = connect().await.unwrap();
    let atomic = client
        .atomic_long(&unique_name("counter_removed"), 0, true)
        .await
        .unwrap()
        .unwrap();
    atomic.close().await.unwrap();

    assert!(atomic.get().await.unwrap_err().to_string().contains("does not exist"));
    assert!(atomic
        .increment_and_get()
        .await
        .unwrap_err()
        .to_string()
        .contains("does not exist"));
    assert!(atomic
        .get_and_add(1)
        .await
        .unwrap_err()
        .to_string()
        .contains("does not exist"));
    assert!(atomic
        .get_and_set(1)
        .await
        .unwrap_err()
        .to_string()
        .contains("does not exist"));
    assert!(atomic
        .compare_and_set(1, 2)
        .await
        .unwrap_err()
        .to_string()
        .contains("does not exist"));
}

/// Java parity: org.apache.ignite.internal.client.thin.AtomicLongTest#testCustomConfigurationPropagatesToServer
#[tokio::test]
async fn should_propagate_atomic_long_configuration() {
    let client = connect().await.unwrap();
    let atomic = client
        .atomic_long_with_config(
            &unique_name("counter_cfg"),
            &AtomicConfiguration::new()
                .with_group_name("grp")
                .with_backups(1)
                .with_reserve_size(64)
                .with_cache_mode(CacheMode::Partitioned),
            5,
            true,
        )
        .await
        .unwrap()
        .unwrap();

    assert_eq!(atomic.get().await.unwrap(), 5);
    assert_eq!(atomic.add_and_get(1).await.unwrap(), 6);
    atomic.close().await.unwrap();
}
