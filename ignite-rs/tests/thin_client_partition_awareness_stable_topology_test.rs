#![cfg(not(feature = "ssl"))]

mod common;

use common::{
    connect_with_cluster3_config, destroy_cache_if_exists, ignite_cluster3_env, unique_name,
    TestClient,
};
use ignite_rs::cache::{AtomicityMode, Cache, CacheConfiguration, CacheMode};
use ignite_rs::data_structures::{AtomicConfiguration, CollectionConfiguration};
use ignite_rs::query::ScanQuery;
use ignite_rs::ClientConfig;

/// Java parity: org.apache.ignite.internal.client.thin.ThinClientPartitionAwarenessStableTopologyTest#testReplicatedCache
#[tokio::test]
async fn should_use_live_replicated_cache_on_stable_cluster() {
    let cache_name = unique_name("stable_replicated");
    let client = stable_cluster_client().await;
    destroy_cache_if_exists(&client, &cache_name).await;

    let mut cfg = CacheConfiguration::new(&cache_name);
    cfg.cache_mode = CacheMode::Replicated;
    let cache = create_cache_with_config_or_get(&client, &cfg).await;

    exercise_not_applicable_cache(&cache).await;
    cleanup_cache(&client, &cache_name).await;
}

/// Java parity: org.apache.ignite.internal.client.thin.ThinClientPartitionAwarenessStableTopologyTest#testPartitionedCachePrimitiveKey
#[tokio::test]
async fn should_use_live_partitioned_cache_with_primitive_keys() {
    let cache_name = unique_name("stable_partitioned_primitive");
    let client = stable_cluster_client().await;
    destroy_cache_if_exists(&client, &cache_name).await;

    let cache = client
        .get_or_create_cache::<i32, i32>(&cache_name)
        .await
        .unwrap();
    exercise_applicable_cache(&cache).await;

    cleanup_cache(&client, &cache_name).await;
}

/// Java parity: org.apache.ignite.internal.client.thin.ThinClientPartitionAwarenessStableTopologyTest#testPartitionedCache0Backups
#[tokio::test]
async fn should_use_live_partitioned_cache_with_zero_backups() {
    let cache_name = unique_name("stable_zero_backups");
    let client = stable_cluster_client().await;
    destroy_cache_if_exists(&client, &cache_name).await;

    let mut cfg = CacheConfiguration::new(&cache_name);
    cfg.num_backup = 0;
    let cache = create_cache_with_config_or_get(&client, &cfg).await;
    exercise_applicable_cache(&cache).await;

    cleanup_cache(&client, &cache_name).await;
}

/// Java parity: org.apache.ignite.internal.client.thin.ThinClientPartitionAwarenessStableTopologyTest#testPartitionedCache1Backups
#[tokio::test]
async fn should_use_live_partitioned_cache_with_one_backup() {
    let cache_name = unique_name("stable_one_backup");
    let client = stable_cluster_client().await;
    destroy_cache_if_exists(&client, &cache_name).await;

    let mut cfg = CacheConfiguration::new(&cache_name);
    cfg.num_backup = 1;
    let cache = create_cache_with_config_or_get(&client, &cfg).await;
    exercise_applicable_cache(&cache).await;

    cleanup_cache(&client, &cache_name).await;
}

/// Java parity: org.apache.ignite.internal.client.thin.ThinClientPartitionAwarenessStableTopologyTest#testPartitionedCache3Backups
#[tokio::test]
async fn should_use_live_partitioned_cache_with_three_backups() {
    let cache_name = unique_name("stable_three_backups");
    let client = stable_cluster_client().await;
    destroy_cache_if_exists(&client, &cache_name).await;

    let mut cfg = CacheConfiguration::new(&cache_name);
    cfg.num_backup = 3;
    let cache = create_cache_with_config_or_get(&client, &cfg).await;
    exercise_applicable_cache(&cache).await;

    cleanup_cache(&client, &cache_name).await;
}

/// Java parity: org.apache.ignite.internal.client.thin.ThinClientPartitionAwarenessStableTopologyTest#testScanQuery
#[tokio::test]
async fn should_execute_live_scan_query_on_stable_cluster() {
    let cache_name = unique_name("stable_scan");
    let client = stable_cluster_client().await;
    destroy_cache_if_exists(&client, &cache_name).await;

    let cache = client
        .get_or_create_cache::<i32, i32>(&cache_name)
        .await
        .unwrap();
    cache.put_all(&[(1, 10), (2, 20), (3, 30)]).await.unwrap();

    let mut rows = cache
        .scan_query(ScanQuery::new().with_page_size(2))
        .await
        .unwrap()
        .fetch_all()
        .await
        .unwrap();
    rows.sort_by_key(|(key, _)| key.unwrap_or_default());
    assert_eq!(
        rows,
        vec![
            (Some(1), Some(10)),
            (Some(2), Some(20)),
            (Some(3), Some(30))
        ]
    );

    let partition_rows = cache
        .scan_query(ScanQuery::new().with_partition(0).with_page_size(1))
        .await
        .unwrap()
        .fetch_all()
        .await
        .unwrap();
    assert!(
        partition_rows.len() <= rows.len(),
        "partition-restricted scan should not produce more rows than full scan"
    );

    cleanup_cache(&client, &cache_name).await;
}

/// Java parity: org.apache.ignite.internal.client.thin.ThinClientPartitionAwarenessStableTopologyTest#testIgniteSet
#[tokio::test]
async fn should_use_live_ignite_set_on_stable_cluster() {
    let client = stable_cluster_client().await;

    exercise_live_set_variant(
        &client,
        &unique_name("stable_set_atomic_default"),
        &CollectionConfiguration::new().with_backups(1),
    )
    .await;
    exercise_live_set_variant(
        &client,
        &unique_name("stable_set_tx_default"),
        &CollectionConfiguration::new()
            .with_atomicity_mode(AtomicityMode::Transactional)
            .with_backups(1),
    )
    .await;
    exercise_live_set_variant(
        &client,
        &unique_name("stable_set_atomic_group"),
        &CollectionConfiguration::new()
            .with_group_name(&unique_name("stable_set_group_atomic"))
            .with_backups(1),
    )
    .await;
    exercise_live_set_variant(
        &client,
        &unique_name("stable_set_tx_group"),
        &CollectionConfiguration::new()
            .with_atomicity_mode(AtomicityMode::Transactional)
            .with_group_name(&unique_name("stable_set_group_tx"))
            .with_backups(1),
    )
    .await;
}

/// Java parity: org.apache.ignite.internal.client.thin.ThinClientPartitionAwarenessStableTopologyTest#testIgniteSetCollocated
#[tokio::test]
async fn should_use_live_collocated_ignite_set_on_stable_cluster() {
    let client = stable_cluster_client().await;

    exercise_live_set_variant(
        &client,
        &unique_name("stable_set_colocated_atomic_default"),
        &CollectionConfiguration::new()
            .with_backups(1)
            .with_colocated(true),
    )
    .await;
    exercise_live_set_variant(
        &client,
        &unique_name("stable_set_colocated_tx_default"),
        &CollectionConfiguration::new()
            .with_atomicity_mode(AtomicityMode::Transactional)
            .with_backups(1)
            .with_colocated(true),
    )
    .await;
    exercise_live_set_variant(
        &client,
        &unique_name("stable_set_colocated_atomic_group"),
        &CollectionConfiguration::new()
            .with_group_name(&unique_name("stable_set_colocated_group_atomic"))
            .with_backups(1)
            .with_colocated(true),
    )
    .await;
    exercise_live_set_variant(
        &client,
        &unique_name("stable_set_colocated_tx_group"),
        &CollectionConfiguration::new()
            .with_atomicity_mode(AtomicityMode::Transactional)
            .with_group_name(&unique_name("stable_set_colocated_group_tx"))
            .with_backups(1)
            .with_colocated(true),
    )
    .await;
}

/// Java parity: org.apache.ignite.internal.client.thin.ThinClientPartitionAwarenessStableTopologyTest#testAtomicLong
#[tokio::test]
async fn should_use_live_atomic_long_on_stable_cluster() {
    let client = stable_cluster_client().await;

    exercise_live_atomic_long_variant(
        &client,
        &unique_name("stable_atomic_default_partitioned"),
        None,
        CacheMode::Partitioned,
    )
    .await;
    exercise_live_atomic_long_variant(
        &client,
        &unique_name("stable_atomic_default_replicated"),
        None,
        CacheMode::Replicated,
    )
    .await;
    exercise_live_atomic_long_variant(
        &client,
        &unique_name("stable_atomic_group_partitioned"),
        Some(&unique_name("stable_atomic_group_partitioned")),
        CacheMode::Partitioned,
    )
    .await;
    exercise_live_atomic_long_variant(
        &client,
        &unique_name("stable_atomic_group_replicated"),
        Some(&unique_name("stable_atomic_group_replicated")),
        CacheMode::Replicated,
    )
    .await;
}

async fn stable_cluster_client() -> TestClient {
    let env = ignite_cluster3_env();
    env.wait_for_ready().await.unwrap();
    connect_with_cluster3_config(ClientConfig::from_addresses(
        env.addresses().iter().cloned(),
    ))
    .await
    .unwrap()
}

async fn cleanup_cache(client: &TestClient, cache_name: &str) {
    let _ = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        client.destroy_cache(cache_name),
    )
    .await;
}

async fn create_cache_with_config_or_get(
    client: &TestClient,
    cfg: &CacheConfiguration,
) -> Cache<i32, i32> {
    match client.create_cache_with_config::<i32, i32>(cfg).await {
        Ok(cache) => cache,
        Err(err) if err.to_string().contains("same name is already started") => client
            .get_or_create_cache::<i32, i32>(&cfg.name)
            .await
            .unwrap(),
        Err(err) => panic!("failed to create cache {}: {}", cfg.name, err),
    }
}

async fn exercise_not_applicable_cache(cache: &Cache<i32, i32>) {
    cache.put(&0, &0).await.unwrap();
    assert_eq!(cache.get(&0).await.unwrap(), Some(0));

    for idx in 1..8 {
        cache.put(&idx, &idx).await.unwrap();
        assert_eq!(cache.get(&idx).await.unwrap(), Some(idx));
    }
}

async fn exercise_applicable_cache(cache: &Cache<i32, i32>) {
    for idx in 0..8 {
        assert_eq!(cache.get_and_put(&idx, &idx).await.unwrap(), None);
        assert_eq!(cache.get(&idx).await.unwrap(), Some(idx));
        assert!(cache.contains_key(&idx).await.unwrap());
        assert!(!cache.put_if_absent(&idx, &(idx + 100)).await.unwrap());
        assert!(cache.replace(&idx, &(idx + 1)).await.unwrap());
        assert_eq!(
            cache.get_and_replace(&idx, &(idx + 2)).await.unwrap(),
            Some(idx + 1)
        );
        assert_eq!(
            cache.get_and_put_if_absent(&idx, &(idx + 3)).await.unwrap(),
            Some(idx + 2)
        );
        assert_eq!(cache.get_and_remove(&idx).await.unwrap(), Some(idx + 2));
        assert!(cache.put_if_absent(&idx, &idx).await.unwrap());
        cache.clear_key(&idx).await.unwrap();
        assert!(!cache.contains_key(&idx).await.unwrap());
    }
}

async fn exercise_live_set_variant(
    client: &TestClient,
    set_name: &str,
    config: &CollectionConfiguration,
) {
    let set = client
        .set::<String>(set_name, Some(config))
        .await
        .unwrap()
        .unwrap();

    let first = format!("{set_name}-a");
    let second = format!("{set_name}-b");
    assert!(set.add(&first).await.unwrap());
    assert!(set.add(&second).await.unwrap());
    assert!(set.contains(&first).await.unwrap());
    assert_eq!(set.size().await.unwrap(), 2);

    let mut values = set.iter().await.unwrap().fetch_all().await.unwrap();
    values.sort();
    assert_eq!(values, vec![first.clone(), second.clone()]);
    set.close().await.unwrap();
}

async fn exercise_live_atomic_long_variant(
    client: &TestClient,
    atomic_name: &str,
    group_name: Option<&str>,
    cache_mode: CacheMode,
) {
    let mut config = AtomicConfiguration::new().with_cache_mode(cache_mode);
    if let Some(group_name) = group_name {
        config = config.with_group_name(group_name);
    }

    let atomic = client
        .atomic_long_with_config(atomic_name, &config, 1, true)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(atomic.get().await.unwrap(), 1);
    assert_eq!(atomic.increment_and_get().await.unwrap(), 2);
    assert_eq!(atomic.add_and_get(3).await.unwrap(), 5);
    atomic.close().await.unwrap();
}
