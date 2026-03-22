#![cfg(not(feature = "ssl"))]

mod common;

use common::{connect, destroy_cache_if_exists, unique_name};

/// Java parity: org.apache.ignite.client.ClientOrderedCollectionWarnTest#testPutAll
///
/// Rust bulk APIs are slice-based, so there is no Java-style collection warning surface.
/// The parity signal here is that the operations work correctly on real Ignite with ordered input.
#[tokio::test]
async fn should_support_bulk_slice_operations_without_collection_specific_warnings() {
    let client = connect().await.unwrap();
    let cache_name = unique_name("bulk_cache");
    destroy_cache_if_exists(&client, &cache_name).await;

    let cache = client
        .get_or_create_cache::<i32, i32>(&cache_name)
        .await
        .unwrap();
    cache.put_all(&[(1, 10), (2, 20)]).await.unwrap();

    let mut rows = cache.get_all(&[2, 1]).await.unwrap();
    rows.sort_by_key(|(key, _)| key.unwrap_or_default());

    assert_eq!(rows, vec![(Some(1), Some(10)), (Some(2), Some(20))]);
    destroy_cache_if_exists(&client, &cache_name).await;
}

/// Java parity: org.apache.ignite.client.ClientOrderedCollectionWarnTest#testRemoveAll
#[tokio::test]
async fn should_support_bulk_key_removals_via_slices() {
    let client = connect().await.unwrap();
    let cache_name = unique_name("bulk_remove_cache");
    destroy_cache_if_exists(&client, &cache_name).await;

    let cache = client
        .get_or_create_cache::<i32, i32>(&cache_name)
        .await
        .unwrap();
    cache.put_all(&[(1, 10), (2, 20), (3, 30)]).await.unwrap();
    cache.remove_keys(&[1, 3]).await.unwrap();

    // CacheGetAll only returns entries that exist; removed keys are absent.
    let rows = cache.get_all(&[1, 2, 3]).await.unwrap();
    assert_eq!(rows, vec![(Some(2), Some(20))]);
    destroy_cache_if_exists(&client, &cache_name).await;
}

/// Java parity: org.apache.ignite.client.ClientOrderedCollectionWarnTest#testGetAll
#[tokio::test]
async fn should_support_bulk_get_all_via_slices() {
    let client = connect().await.unwrap();
    let cache_name = unique_name("bulk_get_all_cache");
    destroy_cache_if_exists(&client, &cache_name).await;

    let cache = client
        .get_or_create_cache::<i32, i32>(&cache_name)
        .await
        .unwrap();
    cache.put_all(&[(1, 10), (2, 20), (3, 30)]).await.unwrap();

    let mut rows = cache.get_all(&[1, 2, 3]).await.unwrap();
    rows.sort_by_key(|(key, _)| key.unwrap_or_default());

    assert_eq!(
        rows,
        vec![
            (Some(1), Some(10)),
            (Some(2), Some(20)),
            (Some(3), Some(30)),
        ]
    );
    destroy_cache_if_exists(&client, &cache_name).await;
}

/// Java parity: org.apache.ignite.client.ClientOrderedCollectionWarnTest#testContainsKeys
#[tokio::test]
async fn should_support_bulk_contains_keys_via_slices() {
    let client = connect().await.unwrap();
    let cache_name = unique_name("bulk_contains_keys_cache");
    destroy_cache_if_exists(&client, &cache_name).await;

    let cache = client
        .get_or_create_cache::<i32, i32>(&cache_name)
        .await
        .unwrap();
    cache.put_all(&[(1, 10), (2, 20)]).await.unwrap();

    assert!(
        cache.contains_keys(&[1, 2]).await.unwrap(),
        "all present keys should return true"
    );
    assert!(
        !cache.contains_keys(&[1, 2, 3]).await.unwrap(),
        "missing key 3 should cause contains_keys to return false"
    );

    destroy_cache_if_exists(&client, &cache_name).await;
}
