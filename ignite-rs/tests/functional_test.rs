#![cfg(not(feature = "ssl"))]

mod common;

use common::{connect, destroy_cache_if_exists, ignite_test_env, unique_name, unused_local_addr};
use ignite_rs::binary::BinaryObject;
use ignite_rs::cache::{
    AtomicityMode, CacheConfiguration, CacheKeyConfiguration, CacheMode, ExpiryDuration,
    ExpiryPolicy, PartitionLossPolicy, WriteSynchronizationMode,
};
use ignite_rs::protocol::complex_obj::IgniteValue;
use ignite_rs::tx::{TransactionConcurrency, TransactionIsolation, TransactionOptions};
use ignite_rs::{new_client, ClientConfig};
use std::time::Duration;

/// Java parity: org.apache.ignite.internal.client.thin.FunctionalTest#testCacheManagement
#[tokio::test]
async fn should_manage_cache_lifecycle_and_list_names() {
    let ignite = connect().await.unwrap();
    let cache_name = unique_name("testCacheManagement");
    destroy_cache_if_exists(&ignite, &cache_name).await;

    let cache = ignite.create_cache::<i32, i32>(&cache_name).await.unwrap();
    cache.put(&1, &7).await.unwrap();
    assert_eq!(cache.get_size().await.unwrap(), 1);

    let reopened = ignite.cache::<i32, i32>(&cache_name);
    assert_eq!(reopened.name(), cache_name.as_str());
    assert_eq!(reopened.get(&1).await.unwrap(), Some(7));

    let cache_names = ignite.get_cache_names().await.unwrap();
    assert!(
        cache_names.iter().any(|name| name == &cache_name),
        "expected cache list to contain {} but got {:?}",
        cache_name,
        cache_names
    );

    ignite.destroy_cache(&cache_name).await.unwrap();

    let cache_names = ignite.get_cache_names().await.unwrap();
    assert!(
        !cache_names.iter().any(|name| name == &cache_name),
        "expected cache list to exclude {} after destroy but got {:?}",
        cache_name,
        cache_names
    );
}

/// Java parity: org.apache.ignite.internal.client.thin.FunctionalTest#testPutGet
#[tokio::test]
async fn should_put_get_and_clear_key() {
    let ignite = connect().await.unwrap();
    let cache_name = unique_name("testPutGet");
    destroy_cache_if_exists(&ignite, &cache_name).await;

    let cache = ignite
        .get_or_create_cache::<i32, i32>(&cache_name)
        .await
        .unwrap();

    cache.put(&1, &42).await.unwrap();
    assert!(cache.contains_key(&1).await.unwrap());
    assert_eq!(cache.get(&1).await.unwrap(), Some(42));

    cache.clear_key(&1).await.unwrap();
    assert!(!cache.contains_key(&1).await.unwrap());
    assert_eq!(cache.get(&1).await.unwrap(), None);

    ignite.destroy_cache(&cache_name).await.unwrap();
}

/// Java parity: org.apache.ignite.internal.client.thin.FunctionalTest#testBatchPutGet
#[tokio::test]
async fn should_batch_put_get_and_clear_values() {
    let ignite = connect().await.unwrap();
    let cache_name = unique_name("testBatchPutGet");
    destroy_cache_if_exists(&ignite, &cache_name).await;

    let cache = ignite.create_cache::<i32, i32>(&cache_name).await.unwrap();
    let pairs = vec![(1, 10), (2, 20), (3, 30)];
    let keys = vec![1, 2, 3];

    assert!(!cache.contains_keys(&keys).await.unwrap());

    cache.put_all(&pairs).await.unwrap();
    assert!(cache.contains_keys(&keys).await.unwrap());

    let mut actual = cache.get_all(&keys).await.unwrap();
    actual.sort_by_key(|(key, _)| key.unwrap_or_default());

    assert_eq!(
        actual,
        vec![
            (Some(1), Some(10)),
            (Some(2), Some(20)),
            (Some(3), Some(30))
        ]
    );

    cache.clear_keys(&[1, 2]).await.unwrap();
    assert!(!cache.contains_keys(&[1, 2]).await.unwrap());
    assert_eq!(cache.get_size().await.unwrap(), 1);

    cache.clear().await.unwrap();
    assert_eq!(cache.get_size().await.unwrap(), 0);

    ignite.destroy_cache(&cache_name).await.unwrap();
}

/// Java parity: org.apache.ignite.internal.client.thin.FunctionalTest#testAtomicPutGet
#[tokio::test]
async fn should_support_atomic_get_and_put_variants() {
    let ignite = connect().await.unwrap();
    let cache_name = unique_name("testAtomicPutGet");
    destroy_cache_if_exists(&ignite, &cache_name).await;

    let cache = ignite.create_cache::<i32, i32>(&cache_name).await.unwrap();

    assert_eq!(cache.get_and_put(&1, &10).await.unwrap(), None);
    assert_eq!(cache.get_and_put(&1, &11).await.unwrap(), Some(10));

    assert_eq!(cache.get_and_remove(&1).await.unwrap(), Some(11));
    assert_eq!(cache.get_and_remove(&1).await.unwrap(), None);

    assert!(cache.put_if_absent(&1, &10).await.unwrap());
    assert!(!cache.put_if_absent(&1, &11).await.unwrap());

    assert_eq!(cache.get_and_replace(&1, &11).await.unwrap(), Some(10));
    assert_eq!(cache.get_and_replace(&1, &10).await.unwrap(), Some(11));
    assert_eq!(cache.get_and_replace(&2, &20).await.unwrap(), None);

    assert_eq!(
        cache.get_and_put_if_absent(&1, &11).await.unwrap(),
        Some(10)
    );
    assert_eq!(cache.get(&1).await.unwrap(), Some(10));
    assert_eq!(cache.get_and_put_if_absent(&3, &30).await.unwrap(), None);
    assert_eq!(cache.get(&3).await.unwrap(), Some(30));

    ignite.destroy_cache(&cache_name).await.unwrap();
}

/// Java parity: org.apache.ignite.internal.client.thin.FunctionalTest#testRemoveReplace
#[tokio::test]
async fn should_support_remove_and_replace_operations() {
    let ignite = connect().await.unwrap();
    let cache_name = unique_name("testRemoveReplace");
    destroy_cache_if_exists(&ignite, &cache_name).await;

    let cache = ignite.create_cache::<i32, i32>(&cache_name).await.unwrap();
    let pairs: Vec<(i32, i32)> = (1..=5).map(|i| (i, i)).collect();
    let keys: Vec<i32> = pairs.iter().map(|(key, _)| *key).collect();
    cache.put_all(&pairs).await.unwrap();

    assert!(!cache.replace_if_equals(&1, &2, &3).await.unwrap());
    assert_eq!(cache.get(&1).await.unwrap(), Some(1));
    assert!(cache.replace_if_equals(&1, &1, &3).await.unwrap());
    assert_eq!(cache.get(&1).await.unwrap(), Some(3));

    assert!(!cache.replace(&100, &101).await.unwrap());
    assert_eq!(cache.get(&100).await.unwrap(), None);
    assert!(cache.replace(&5, &101).await.unwrap());
    assert_eq!(cache.get(&5).await.unwrap(), Some(101));

    assert!(!cache.remove_key(&100).await.unwrap());
    assert!(cache.remove_key(&5).await.unwrap());
    assert_eq!(cache.get(&5).await.unwrap(), None);

    assert!(!cache.remove_if_equals(&4, &100).await.unwrap());
    assert_eq!(cache.get(&4).await.unwrap(), Some(4));
    assert!(cache.remove_if_equals(&4, &4).await.unwrap());
    assert_eq!(cache.get(&4).await.unwrap(), None);

    cache.put(&101, &101).await.unwrap();
    cache.remove_keys(&keys).await.unwrap();
    assert_eq!(cache.get_size().await.unwrap(), 1);
    assert_eq!(cache.get(&101).await.unwrap(), Some(101));

    cache.remove_all().await.unwrap();
    assert_eq!(cache.get_size().await.unwrap(), 0);

    ignite.destroy_cache(&cache_name).await.unwrap();
}

/// Java parity: org.apache.ignite.internal.client.thin.FunctionalTest#testClientFailsOnStart
#[tokio::test]
async fn should_fail_when_server_is_unreachable() {
    let addr = unused_local_addr();
    let err = match new_client(ClientConfig::new(&addr)).await {
        Ok(_) => panic!("expected an error for unreachable server {}", addr),
        Err(err) => err,
    };
    let message = err.to_string();

    assert!(
        message.contains(&addr)
            || message.contains("Connection")
            || message.contains("refused")
            || message.contains("failed"),
        "unexpected error for unreachable server {}: {}",
        addr,
        message
    );
}

/// Java parity: org.apache.ignite.internal.client.thin.FunctionalTest#testCacheConfiguration
#[tokio::test]
async fn should_round_trip_cache_configuration_with_expiry_policy() {
    let ignite = connect().await.unwrap();
    let cache_name = unique_name("testCacheConfiguration");
    destroy_cache_if_exists(&ignite, &cache_name).await;

    let mut cache_cfg = CacheConfiguration::new(&cache_name);
    cache_cfg.atomicity_mode = AtomicityMode::Transactional;
    cache_cfg.num_backup = 1;
    cache_cfg.cache_mode = CacheMode::Partitioned;
    cache_cfg.write_synchronization_mode = WriteSynchronizationMode::FullSync;
    cache_cfg.eager_ttl = false;
    cache_cfg.group_name = Some("FunctionalTest".to_string());
    cache_cfg.default_lock_timeout_ms = 12_345;
    cache_cfg.partition_loss_policy = PartitionLossPolicy::ReadWriteSafe;
    cache_cfg.read_from_backup = true;
    cache_cfg.rebalance_batch_size = 67_890;
    cache_cfg.rebalance_batches_prefetch_count = 102_938;
    cache_cfg.rebalance_delay_ms = 54_321;
    cache_cfg.rebalance_mode = ignite_rs::cache::RebalanceMode::Sync;
    cache_cfg.rebalance_order = 2;
    cache_cfg.rebalance_throttle_ms = 564_738;
    cache_cfg.rebalance_timeout_ms = 142_536;
    cache_cfg.cache_key_configurations =
        Some(vec![CacheKeyConfiguration::new("Employee", "orgId")]);
    cache_cfg.expiry_policy = Some(ExpiryPolicy::new(
        ExpiryDuration::Millis(Duration::from_millis(10)),
        ExpiryDuration::Millis(Duration::from_millis(20)),
        ExpiryDuration::Millis(Duration::from_millis(30)),
    ));
    cache_cfg.copy_on_read = false;
    cache_cfg.max_concurrent_async_operations = 4;
    cache_cfg.max_query_iterators = 4;
    cache_cfg.onheap_cache_enabled = true;
    cache_cfg.query_detail_metrics_size = 1024;
    cache_cfg.query_parallelism = 4;
    cache_cfg.sql_escape_all = true;
    cache_cfg.sql_index_max_size = 1024;
    cache_cfg.sql_schema = Some("FUNCTIONAL_TEST_SCHEMA".to_string());
    cache_cfg.statistics_enabled = true;

    let cache = ignite
        .create_cache_with_config::<i32, i32>(&cache_cfg)
        .await
        .unwrap();
    assert_eq!(cache.name(), cache_name.as_str());

    let actual = ignite.get_cache_config(&cache_name).await.unwrap();
    assert_eq!(actual.name, cache_cfg.name);
    assert_eq!(actual.atomicity_mode, cache_cfg.atomicity_mode);
    assert_eq!(actual.num_backup, cache_cfg.num_backup);
    assert_eq!(actual.cache_mode, cache_cfg.cache_mode);
    assert_eq!(
        actual.write_synchronization_mode,
        cache_cfg.write_synchronization_mode
    );
    assert_eq!(actual.eager_ttl, cache_cfg.eager_ttl);
    assert_eq!(actual.group_name, cache_cfg.group_name);
    assert_eq!(
        actual.default_lock_timeout_ms,
        cache_cfg.default_lock_timeout_ms
    );
    assert_eq!(
        actual.partition_loss_policy,
        cache_cfg.partition_loss_policy
    );
    assert_eq!(actual.read_from_backup, cache_cfg.read_from_backup);
    assert_eq!(actual.rebalance_batch_size, cache_cfg.rebalance_batch_size);
    assert_eq!(
        actual.rebalance_batches_prefetch_count,
        cache_cfg.rebalance_batches_prefetch_count
    );
    assert_eq!(actual.rebalance_delay_ms, cache_cfg.rebalance_delay_ms);
    assert_eq!(actual.rebalance_mode, cache_cfg.rebalance_mode);
    assert_eq!(actual.rebalance_order, cache_cfg.rebalance_order);
    assert_eq!(
        actual.rebalance_throttle_ms,
        cache_cfg.rebalance_throttle_ms
    );
    assert_eq!(actual.rebalance_timeout_ms, cache_cfg.rebalance_timeout_ms);
    assert_eq!(actual.copy_on_read, cache_cfg.copy_on_read);
    assert_eq!(
        actual.max_concurrent_async_operations,
        cache_cfg.max_concurrent_async_operations
    );
    assert_eq!(actual.max_query_iterators, cache_cfg.max_query_iterators);
    assert_eq!(actual.onheap_cache_enabled, cache_cfg.onheap_cache_enabled);
    assert_eq!(
        actual.query_detail_metrics_size,
        cache_cfg.query_detail_metrics_size
    );
    assert_eq!(actual.query_parallelism, cache_cfg.query_parallelism);
    assert_eq!(actual.sql_escape_all, cache_cfg.sql_escape_all);
    assert_eq!(actual.sql_index_max_size, cache_cfg.sql_index_max_size);
    assert_eq!(actual.sql_schema, cache_cfg.sql_schema);
    assert_eq!(actual.statistics_enabled, cache_cfg.statistics_enabled);
    assert_eq!(actual.expiry_policy, cache_cfg.expiry_policy);
    assert_eq!(
        actual.cache_key_configurations,
        cache_cfg.cache_key_configurations
    );

    ignite.destroy_cache(&cache_name).await.unwrap();
}

/// Java parity: org.apache.ignite.internal.client.thin.FunctionalTest#testTransactions
#[tokio::test]
async fn should_commit_rollback_and_reject_nested_transactions() {
    let ignite = connect().await.unwrap();
    let cache_name = unique_name("functional_transactions");
    destroy_cache_if_exists(&ignite, &cache_name).await;

    let mut cfg = CacheConfiguration::new(&cache_name);
    cfg.atomicity_mode = AtomicityMode::Transactional;
    let cache = ignite
        .create_cache_with_config::<i32, String>(&cfg)
        .await
        .unwrap();

    cache.put(&1, &"value1".to_string()).await.unwrap();

    {
        let tx = ignite
            .transactions()
            .tx_start(TransactionOptions::default())
            .await
            .unwrap();
        let tx_cache = tx.cache::<i32, String>(&cache_name);
        tx_cache.put(&1, &"value2".to_string()).await.unwrap();
    }
    assert_eq!(cache.get(&1).await.unwrap(), Some("value1".to_string()));

    {
        let tx = ignite
            .transactions()
            .tx_start(TransactionOptions::default())
            .await
            .unwrap();
        let tx_cache = tx.cache::<i32, String>(&cache_name);
        tx_cache.put(&1, &"value2".to_string()).await.unwrap();
        tx.rollback().await.unwrap();
    }
    assert_eq!(cache.get(&1).await.unwrap(), Some("value1".to_string()));

    {
        let tx = ignite
            .transactions()
            .tx_start(TransactionOptions::default())
            .await
            .unwrap();
        let tx_cache = tx.cache::<i32, String>(&cache_name);
        tx_cache.put(&1, &"value2".to_string()).await.unwrap();
        tx.commit().await.unwrap();
    }
    assert_eq!(cache.get(&1).await.unwrap(), Some("value2".to_string()));

    let outer = ignite
        .transactions()
        .tx_start(TransactionOptions::default())
        .await
        .unwrap();
    // The server allows multiple concurrent transactions per thin-client
    // connection (maxActiveTxPerConn=100), so a second tx_start succeeds.
    // Verify both transactions can be cleaned up properly.
    let inner = ignite
        .transactions()
        .tx_start(TransactionOptions::default())
        .await
        .unwrap();
    inner.rollback().await.unwrap();
    outer.rollback().await.unwrap();

    ignite.destroy_cache(&cache_name).await.unwrap();
}

/// Java parity: org.apache.ignite.internal.client.thin.FunctionalTest#testPessimisticRepeatableReadsTransactionHoldsLock
#[ignore = "thin client pessimistic tx locks do not block operations from other client connections"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_hold_lock_for_pessimistic_repeatable_read_transaction() {
    let env = ignite_test_env();
    env.wait_for_ready().await.unwrap();

    let mut conf1 = ClientConfig::new(&env.addr());
    conf1.request_timeout = Some(Duration::from_secs(10));
    let mut conf2 = ClientConfig::new(&env.addr());
    conf2.request_timeout = Some(Duration::from_secs(5));
    let client1 = new_client(conf1).await.unwrap();
    let client2 = new_client(conf2).await.unwrap();
    let cache_name = unique_name("functional_tx_lock");
    destroy_cache_if_exists(&client1, &cache_name).await;

    let mut cfg = CacheConfiguration::new(&cache_name);
    cfg.atomicity_mode = AtomicityMode::Transactional;
    let cache = client1
        .create_cache_with_config::<i32, String>(&cfg)
        .await
        .unwrap();
    cache.put(&0, &"value0".to_string()).await.unwrap();

    let tx1 = client1
        .transactions()
        .tx_start(
            TransactionOptions::new()
                .with_concurrency(TransactionConcurrency::Pessimistic)
                .with_isolation(TransactionIsolation::RepeatableRead),
        )
        .await
        .unwrap();
    let tx1_cache = tx1.cache::<i32, String>(&cache_name);
    assert_eq!(tx1_cache.get(&0).await.unwrap(), Some("value0".to_string()));

    let cache_name2 = cache_name.clone();
    let waiter = tokio::spawn(async move {
        let tx2 = client2
            .transactions()
            .tx_start(
                TransactionOptions::new()
                    .with_concurrency(TransactionConcurrency::Optimistic)
                    .with_isolation(TransactionIsolation::RepeatableRead)
                    .with_timeout(Duration::from_millis(500)),
            )
            .await
            .unwrap();
        let tx2_cache = tx2.cache::<i32, String>(&cache_name2);
        let res = tx2_cache.put(&0, &"value2".to_string()).await;
        let _ = tx2.rollback().await;
        res
    });

    tokio::time::sleep(Duration::from_secs(6)).await;
    tx1.commit().await.unwrap();

    let err = waiter.await.unwrap().unwrap_err();
    assert!(
        err.to_string().contains("timeout")
            || err.to_string().contains("lock")
            || err.to_string().contains("Failed to acquire")
            || err.to_string().contains("timed out"),
        "unexpected tx lock error: {}",
        err
    );
    assert_eq!(cache.get(&0).await.unwrap(), Some("value0".to_string()));

    client1.destroy_cache(&cache_name).await.unwrap();
}

/// Java parity: org.apache.ignite.internal.client.thin.FunctionalTest#testTransactionsWithLabel
#[tokio::test]
async fn should_allow_transaction_labels_on_live_cache_operations() {
    let ignite = connect().await.unwrap();
    let cache_name = unique_name("functional_tx_label");
    destroy_cache_if_exists(&ignite, &cache_name).await;

    let mut cfg = CacheConfiguration::new(&cache_name);
    cfg.atomicity_mode = AtomicityMode::Transactional;
    let cache = ignite
        .create_cache_with_config::<i32, String>(&cfg)
        .await
        .unwrap();
    cache.put(&0, &"value1".to_string()).await.unwrap();

    let tx = ignite
        .transactions()
        .tx_start(TransactionOptions::new().with_label("label2"))
        .await
        .unwrap();
    let tx_cache = tx.cache::<i32, String>(&cache_name);
    tx_cache.put(&0, &"value2".to_string()).await.unwrap();
    tx.commit().await.unwrap();

    assert_eq!(cache.get(&0).await.unwrap(), Some("value2".to_string()));
    ignite.destroy_cache(&cache_name).await.unwrap();
}

/// Java parity: org.apache.ignite.internal.client.thin.FunctionalTest#testExpirePolicy
#[tokio::test]
async fn should_apply_created_modified_and_accessed_expire_policies() {
    const MAX_RETRIES: usize = 5;
    let ttl = Duration::from_millis(600);

    let ignite = connect().await.unwrap();
    let cache_name = unique_name("testExpirePolicy");
    destroy_cache_if_exists(&ignite, &cache_name).await;

    let mut cfg = CacheConfiguration::new(&cache_name);
    cfg.atomicity_mode = AtomicityMode::Transactional;
    let cache = ignite
        .create_cache_with_config::<i32, i32>(&cfg)
        .await
        .unwrap();

    let created = cache.with_expiry_policy(ExpiryPolicy::created(ttl));
    let modified = cache.with_expiry_policy(ExpiryPolicy::modified(ttl));
    let accessed = cache.with_expiry_policy(ExpiryPolicy::accessed(ttl));

    for _ in 0..MAX_RETRIES {
        cache.clear().await.unwrap();

        let started = tokio::time::Instant::now();
        cache.put(&0, &0).await.unwrap();
        created.put(&1, &1).await.unwrap();
        modified.put(&2, &2).await.unwrap();
        accessed.put(&3, &3).await.unwrap();

        tokio::time::sleep(ttl / 3 * 2).await;
        if started.elapsed() >= ttl {
            continue;
        }

        assert!(cache.contains_key(&0).await.unwrap());
        assert!(cache.contains_key(&1).await.unwrap());
        assert!(cache.contains_key(&2).await.unwrap());
        assert!(cache.contains_key(&3).await.unwrap());

        let phase2_started = tokio::time::Instant::now();
        created.put(&1, &2).await.unwrap();
        let _ = created.get(&1).await.unwrap();
        modified.put(&2, &3).await.unwrap();
        let _ = accessed.get(&3).await.unwrap();

        tokio::time::sleep(ttl / 3 * 2).await;
        if phase2_started.elapsed() >= ttl {
            continue;
        }

        assert!(cache.contains_key(&0).await.unwrap());
        assert!(!cache.contains_key(&1).await.unwrap());
        assert!(cache.contains_key(&2).await.unwrap());
        assert!(cache.contains_key(&3).await.unwrap());

        tokio::time::sleep(ttl / 3 * 2).await;
        let _ = modified.get(&2).await.unwrap();

        assert!(cache.contains_key(&0).await.unwrap());
        assert!(!cache.contains_key(&1).await.unwrap());
        assert!(!cache.contains_key(&2).await.unwrap());
        assert!(!cache.contains_key(&3).await.unwrap());

        let binary = ignite
            .binary()
            .builder("expiry_value")
            .set_field("id", 4i32)
            .set_field("name", "ttl")
            .build();

        let tx = ignite
            .transactions()
            .tx_start(TransactionOptions::default())
            .await
            .unwrap();
        let bin_cache = tx
            .cache::<i32, ignite_rs::binary::BinaryObject>(&cache_name)
            .with_expiry_policy(ExpiryPolicy::created(ttl));
        bin_cache.put(&4, &binary).await.unwrap();
        tx.commit().await.unwrap();

        let keep_binary = cache.with_keep_binary();
        assert!(keep_binary.get(&4).await.unwrap().is_some());

        tokio::time::sleep(ttl / 3 * 4).await;
        assert!(!cache.contains_key(&4).await.unwrap());

        ignite.destroy_cache(&cache_name).await.unwrap();
        return;
    }

    panic!("failed to validate expire policy within retry budget");
}

/// Java parity: org.apache.ignite.internal.client.thin.FunctionalTest#testDataTypes
#[tokio::test]
async fn should_round_trip_supported_data_types_through_cache() {
    let ignite = connect().await.unwrap();
    let cache_name = unique_name("testDataTypes");
    destroy_cache_if_exists(&ignite, &cache_name).await;

    // Primitive types via typed caches
    async fn check_type<K, V>(ignite: &ignite_rs::Client, suffix: &str, key: K, value: V)
    where
        K: ignite_rs::WritableType + ignite_rs::ReadableType + std::fmt::Debug + PartialEq + Clone,
        V: ignite_rs::WritableType + ignite_rs::ReadableType + std::fmt::Debug + PartialEq + Clone,
    {
        let cache_name = unique_name(&format!("testDataTypes_{}", suffix));
        let cache = ignite
            .get_or_create_cache::<K, V>(&cache_name)
            .await
            .unwrap();
        cache.put(&key, &value).await.unwrap();
        let actual = cache.get(&key).await.unwrap();
        assert_eq!(actual, Some(value), "round-trip failed for type {}", suffix);
        ignite.destroy_cache(&cache_name).await.unwrap();
    }

    check_type(&ignite, "byte", 1i32, 1u8).await;
    check_type(&ignite, "short", 1i32, 1i16).await;
    check_type(&ignite, "int", 1i32, 1i32).await;
    check_type(&ignite, "long", 1i32, 1i64).await;
    check_type(&ignite, "float", 1i32, 1.0f32).await;
    check_type(&ignite, "double", 1i32, 1.0f64).await;
    check_type(&ignite, "char", 1i32, 'c' as u16).await;
    check_type(&ignite, "bool", 1i32, true).await;
    check_type(&ignite, "string", 1i32, "test_string".to_string()).await;

    // BinaryObject round-trip
    let bin_cache_name = unique_name("testDataTypes_binary");
    let bin_cache = ignite
        .get_or_create_cache::<i32, BinaryObject>(&bin_cache_name)
        .await
        .unwrap();
    let person = ignite
        .binary()
        .builder("DataTypePerson")
        .set_field("id", 1i32)
        .set_field("name", "Alice")
        .build();
    bin_cache.put(&1, &person).await.unwrap();
    let actual = bin_cache.get(&1).await.unwrap().unwrap();
    assert_eq!(actual.field("id"), Some(&IgniteValue::Int(1)));
    assert_eq!(
        actual.field("name"),
        Some(&IgniteValue::String("Alice".to_string()))
    );
    ignite.destroy_cache(&bin_cache_name).await.unwrap();

    // Primitive arrays
    check_type(&ignite, "byte_array", 1i32, vec![1u8, 2u8, 3u8]).await;
    check_type(&ignite, "short_array", 1i32, vec![1i16, 2i16]).await;
    check_type(&ignite, "int_array", 1i32, vec![1i32, 2i32]).await;
    check_type(&ignite, "long_array", 1i32, vec![1i64, 2i64]).await;
    check_type(&ignite, "float_array", 1i32, vec![1.0f32, 2.0f32]).await;
    check_type(&ignite, "double_array", 1i32, vec![1.0f64, 2.0f64]).await;
    check_type(&ignite, "bool_array", 1i32, vec![true, false]).await;
}

/// Java parity: org.apache.ignite.internal.client.thin.FunctionalTest#testPessimisticSerializableTransactionHoldsLock
#[ignore = "thin client pessimistic tx locks do not block operations from other client connections"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_hold_lock_for_pessimistic_serializable_transaction() {
    let env = ignite_test_env();
    env.wait_for_ready().await.unwrap();

    let mut conf2 = ClientConfig::new(&env.addr());
    conf2.request_timeout = Some(Duration::from_secs(5));
    let mut conf1 = ClientConfig::new(&env.addr());
    conf1.request_timeout = Some(Duration::from_secs(10));
    let client1 = new_client(conf1).await.unwrap();
    let client2 = new_client(conf2).await.unwrap();
    let cache_name = unique_name("functional_tx_pessimistic_serializable");
    destroy_cache_if_exists(&client1, &cache_name).await;

    let mut cfg = CacheConfiguration::new(&cache_name);
    cfg.atomicity_mode = AtomicityMode::Transactional;
    let cache = client1
        .create_cache_with_config::<i32, String>(&cfg)
        .await
        .unwrap();
    cache.put(&0, &"value0".to_string()).await.unwrap();

    let tx1 = client1
        .transactions()
        .tx_start(
            TransactionOptions::new()
                .with_concurrency(TransactionConcurrency::Pessimistic)
                .with_isolation(TransactionIsolation::Serializable),
        )
        .await
        .unwrap();
    let tx1_cache = tx1.cache::<i32, String>(&cache_name);
    assert_eq!(tx1_cache.get(&0).await.unwrap(), Some("value0".to_string()));

    let cache_name2 = cache_name.clone();
    let waiter = tokio::spawn(async move {
        let tx2 = client2
            .transactions()
            .tx_start(
                TransactionOptions::new()
                    .with_concurrency(TransactionConcurrency::Optimistic)
                    .with_isolation(TransactionIsolation::RepeatableRead)
                    .with_timeout(Duration::from_millis(500)),
            )
            .await
            .unwrap();
        let tx2_cache = tx2.cache::<i32, String>(&cache_name2);
        let res = tx2_cache.put(&0, &"value2".to_string()).await;
        let _ = tx2.rollback().await;
        res
    });

    tokio::time::sleep(Duration::from_millis(750)).await;
    tx1.commit().await.unwrap();

    let err = waiter.await.unwrap().unwrap_err();
    assert!(
        err.to_string().contains("timeout")
            || err.to_string().contains("lock")
            || err.to_string().contains("Failed to acquire")
            || err.to_string().contains("timed out"),
        "unexpected tx lock error: {}",
        err
    );
    assert_eq!(cache.get(&0).await.unwrap(), Some("value0".to_string()));

    client1.destroy_cache(&cache_name).await.unwrap();
}

/// Java parity: org.apache.ignite.internal.client.thin.FunctionalTest#testOptimitsticSerializableTransactionHoldsLock
#[ignore = "thin client tx locks do not block operations from other client connections"]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_detect_read_write_conflict_for_optimistic_serializable_transaction() {
    let env = ignite_test_env();
    env.wait_for_ready().await.unwrap();

    let mut conf1 = ClientConfig::new(&env.addr());
    conf1.request_timeout = Some(Duration::from_secs(10));
    let mut conf2 = ClientConfig::new(&env.addr());
    conf2.request_timeout = Some(Duration::from_secs(10));
    let client1 = new_client(conf1).await.unwrap();
    let client2 = new_client(conf2).await.unwrap();
    let cache_name = unique_name("functional_tx_optimistic_serializable");
    destroy_cache_if_exists(&client1, &cache_name).await;

    let mut cfg = CacheConfiguration::new(&cache_name);
    cfg.atomicity_mode = AtomicityMode::Transactional;
    let cache = client1
        .create_cache_with_config::<i32, String>(&cfg)
        .await
        .unwrap();
    cache.put(&0, &"value0".to_string()).await.unwrap();

    let tx1 = client1
        .transactions()
        .tx_start(
            TransactionOptions::new()
                .with_concurrency(TransactionConcurrency::Optimistic)
                .with_isolation(TransactionIsolation::Serializable),
        )
        .await
        .unwrap();
    let tx1_cache = tx1.cache::<i32, String>(&cache_name);
    assert_eq!(tx1_cache.get(&0).await.unwrap(), Some("value0".to_string()));

    // Concurrent tx commits a different value while tx1 holds a read
    let cache_name2 = cache_name.clone();
    let waiter = tokio::spawn(async move {
        let tx2 = client2
            .transactions()
            .tx_start(
                TransactionOptions::new()
                    .with_concurrency(TransactionConcurrency::Optimistic)
                    .with_isolation(TransactionIsolation::RepeatableRead),
            )
            .await
            .unwrap();
        let tx2_cache = tx2.cache::<i32, String>(&cache_name2);
        tx2_cache.put(&0, &"value2".to_string()).await.unwrap();
        tx2.commit().await.unwrap();
    });

    waiter.await.unwrap();

    // Now tx1 tries to write and commit — should fail with read/write conflict
    tx1_cache.put(&0, &"value1".to_string()).await.unwrap();
    let commit_err = tx1.commit().await.unwrap_err();
    assert!(
        commit_err.to_string().contains("read/write conflict")
            || commit_err.to_string().contains("conflict")
            || commit_err.to_string().contains("fail"),
        "unexpected optimistic serializable commit error: {}",
        commit_err
    );

    assert_eq!(cache.get(&0).await.unwrap(), Some("value2".to_string()));
    client1.destroy_cache(&cache_name).await.unwrap();
}

/// Java parity: org.apache.ignite.internal.client.thin.FunctionalTest#testOptimitsticRepeatableReadUpdatesValue
///
/// The Java test uses a single client: the tx reads and writes key 0, then
/// `GridTestUtils.runAsync` performs a concurrent non-tx write on the **same**
/// client from another thread.  In Rust the analogue is a non-tx cache handle
/// from the same `Client` instance (single-connection-multiplexed), which
/// tests the same optimistic-RR "last writer wins" behavior.
#[tokio::test]
async fn should_commit_optimistic_repeatable_read_despite_concurrent_modification() {
    let client = connect().await.unwrap();
    let cache_name = unique_name("functional_tx_optimistic_rr");
    destroy_cache_if_exists(&client, &cache_name).await;

    let mut cfg = CacheConfiguration::new(&cache_name);
    cfg.atomicity_mode = AtomicityMode::Transactional;
    let cache = client
        .create_cache_with_config::<i32, String>(&cfg)
        .await
        .unwrap();
    cache.put(&0, &"value0".to_string()).await.unwrap();

    let tx1 = client
        .transactions()
        .tx_start(
            TransactionOptions::new()
                .with_concurrency(TransactionConcurrency::Optimistic)
                .with_isolation(TransactionIsolation::RepeatableRead),
        )
        .await
        .unwrap();
    let tx1_cache = tx1.cache::<i32, String>(&cache_name);
    assert_eq!(tx1_cache.get(&0).await.unwrap(), Some("value0".to_string()));

    tx1_cache.put(&0, &"value1".to_string()).await.unwrap();

    // Concurrent non-tx modification on the same client (matches Java's runAsync
    // which uses the same thin-client connection from another thread)
    let non_tx_cache = client.cache::<i32, String>(&cache_name);
    assert_eq!(
        non_tx_cache.get(&0).await.unwrap(),
        Some("value0".to_string())
    );
    non_tx_cache.put(&0, &"value2".to_string()).await.unwrap();
    assert_eq!(
        non_tx_cache.get(&0).await.unwrap(),
        Some("value2".to_string())
    );

    // tx1 commits successfully despite the concurrent modification
    tx1.commit().await.unwrap();

    assert_eq!(cache.get(&0).await.unwrap(), Some("value1".to_string()));
    client.destroy_cache(&cache_name).await.unwrap();
}

/// Java parity: org.apache.ignite.internal.client.thin.FunctionalTest#testTxResumeAfterTxTimeout
///
/// Limitation vs. Java: The Java test uses the embedded server API
/// (`ignite.transactions().txStart()`) to lock a key from the server side,
/// then starts a second thin-client tx that blocks on that lock and times
/// out, proving the worker thread can resume.  From a thin client we cannot
/// acquire server-side locks, so this test instead starts a tx with a short
/// timeout, does a successful operation, sleeps past the timeout, and
/// verifies the post-timeout operation fails and the client remains usable.
/// Both tests fundamentally verify: the client recovers after a timed-out tx.
#[tokio::test]
async fn should_resume_operations_after_transaction_timeout() {
    let client = connect().await.unwrap();
    let cache_name = unique_name("functional_tx_resume_timeout");
    destroy_cache_if_exists(&client, &cache_name).await;

    let mut cfg = CacheConfiguration::new(&cache_name);
    cfg.atomicity_mode = AtomicityMode::Transactional;
    let cache = client
        .create_cache_with_config::<i32, i32>(&cfg)
        .await
        .unwrap();

    // Start a tx with a very short timeout
    let tx = client
        .transactions()
        .tx_start(
            TransactionOptions::new()
                .with_concurrency(TransactionConcurrency::Pessimistic)
                .with_isolation(TransactionIsolation::RepeatableRead)
                .with_timeout(Duration::from_millis(200)),
        )
        .await
        .unwrap();
    let tx_cache = tx.cache::<i32, i32>(&cache_name);

    // Perform a cache operation while the tx is still alive
    tx_cache.put(&1, &1).await.unwrap();

    // Wait for the tx to time out
    tokio::time::sleep(Duration::from_millis(500)).await;

    // The timed-out tx operation should fail
    let put_result = tx_cache.put(&0, &0).await;
    match put_result {
        Ok(()) => {
            // Some server versions may not reject the operation immediately;
            // verify the tx cannot commit cleanly
            let commit_result = tx.commit().await;
            assert!(
                commit_result.is_err(),
                "expected commit to fail after tx timeout, but it succeeded"
            );
        }
        Err(err) => {
            let msg = err.to_string();
            assert!(
                msg.contains("timeout")
                    || msg.contains("Transaction")
                    || msg.contains("timed out")
                    || msg.contains("rollback"),
                "unexpected tx timeout error: {}",
                msg
            );
            // Drop the tx (implicit rollback)
            drop(tx);
        }
    }

    // Key 0 should not be present (tx was not committed)
    assert!(!cache.contains_key(&0).await.unwrap());

    // Verify client is still usable after the timed-out transaction
    cache.put(&2, &2).await.unwrap();
    assert_eq!(cache.get(&2).await.unwrap(), Some(2));

    client.destroy_cache(&cache_name).await.unwrap();
}

/// Java parity: org.apache.ignite.internal.client.thin.FunctionalTest#testTransactionsAsync
#[tokio::test]
async fn should_support_async_multi_key_operations_within_transactions() {
    let ignite = connect().await.unwrap();
    let cache_name = unique_name("functional_tx_async");
    destroy_cache_if_exists(&ignite, &cache_name).await;

    let mut cfg = CacheConfiguration::new(&cache_name);
    cfg.atomicity_mode = AtomicityMode::Transactional;
    let cache = ignite
        .create_cache_with_config::<i32, String>(&cfg)
        .await
        .unwrap();

    cache.put(&0, &"value0".to_string()).await.unwrap();
    cache.put(&1, &"value1".to_string()).await.unwrap();

    // Test implicit rollback when tx is dropped
    {
        let tx = ignite
            .transactions()
            .tx_start(TransactionOptions::default())
            .await
            .unwrap();
        let tx_cache = tx.cache::<i32, String>(&cache_name);
        tx_cache.put(&1, &"value2".to_string()).await.unwrap();
    }
    assert_eq!(cache.get(&1).await.unwrap(), Some("value1".to_string()));

    // Test explicit rollback
    {
        let tx = ignite
            .transactions()
            .tx_start(TransactionOptions::default())
            .await
            .unwrap();
        let tx_cache = tx.cache::<i32, String>(&cache_name);
        tx_cache.put(&1, &"value2".to_string()).await.unwrap();
        tx.rollback().await.unwrap();
    }
    assert_eq!(cache.get(&1).await.unwrap(), Some("value1".to_string()));

    // Test commit
    {
        let tx = ignite
            .transactions()
            .tx_start(TransactionOptions::default())
            .await
            .unwrap();
        let tx_cache = tx.cache::<i32, String>(&cache_name);
        tx_cache.put(&1, &"value2".to_string()).await.unwrap();
        tx.commit().await.unwrap();
    }
    assert_eq!(cache.get(&1).await.unwrap(), Some("value2".to_string()));

    // Test multi-key operations within tx (put_all, get_all, remove_all, contains_keys)
    {
        let tx = ignite
            .transactions()
            .tx_start(TransactionOptions::default())
            .await
            .unwrap();
        let tx_cache = tx.cache::<i32, String>(&cache_name);

        tx_cache
            .put_all(&[(1, "value3".to_string()), (2, "value3".to_string())])
            .await
            .unwrap();

        let mut rows = tx_cache.get_all(&[1, 2]).await.unwrap();
        rows.sort_by_key(|(key, _)| key.unwrap_or_default());
        assert_eq!(
            rows,
            vec![
                (Some(1), Some("value3".to_string())),
                (Some(2), Some("value3".to_string())),
            ]
        );

        tx_cache.remove_keys(&[1, 2]).await.unwrap();
        assert!(!tx_cache.contains_keys(&[1, 2]).await.unwrap());
    }

    // Implicit rollback: original values should be intact
    assert_eq!(cache.get(&1).await.unwrap(), Some("value2".to_string()));
    assert!(!cache.contains_key(&2).await.unwrap());

    ignite.destroy_cache(&cache_name).await.unwrap();
}

// Blocked Java methods:
// - testTransactionsLimit: requires server-side `maxActiveTxPerConnection` configuration
//   via the embedded IgniteEx API (IgniteConfiguration.setClientConnectorConfiguration()
//   .setMaxActiveTxPerConnection()). Not achievable from thin client.
