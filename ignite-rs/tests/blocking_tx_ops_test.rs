#![cfg(not(feature = "ssl"))]

mod common;

use common::{connect, destroy_cache_if_exists, unique_name};
use ignite_rs::cache::{AtomicityMode, CacheConfiguration};
use ignite_rs::tx::{TransactionConcurrency, TransactionIsolation, TransactionOptions};

/// Java parity: org.apache.ignite.internal.client.thin.BlockingTxOpsTest#testBlockingOps
///
/// Verifies that basic transactional operations (put, get, commit) work correctly
/// against a live single-node Ignite instance with a TRANSACTIONAL cache.
#[tokio::test]
async fn should_execute_blocking_tx_operations_on_live_cache() {
    let client = connect().await.unwrap();
    let cache_name = unique_name("blocking_tx_live");
    destroy_cache_if_exists(&client, &cache_name).await;

    let mut cfg = CacheConfiguration::new(&cache_name);
    cfg.atomicity_mode = AtomicityMode::Transactional;
    let cache = client
        .create_cache_with_config::<i32, i32>(&cfg)
        .await
        .unwrap();

    // Pessimistic RepeatableRead transaction
    let tx = client
        .transactions()
        .tx_start(
            TransactionOptions::new()
                .with_concurrency(TransactionConcurrency::Pessimistic)
                .with_isolation(TransactionIsolation::RepeatableRead),
        )
        .await
        .unwrap();
    let tx_cache = tx.cache::<i32, i32>(&cache_name);
    tx_cache.put(&1, &10).await.unwrap();
    tx_cache.put(&2, &20).await.unwrap();
    assert_eq!(tx_cache.get(&1).await.unwrap(), Some(10));
    tx.commit().await.unwrap();

    assert_eq!(cache.get(&1).await.unwrap(), Some(10));
    assert_eq!(cache.get(&2).await.unwrap(), Some(20));

    client.destroy_cache(&cache_name).await.unwrap();
}

/// Java parity: org.apache.ignite.internal.client.thin.BlockingTxOpsTest#testTransactionalConsistency
///
/// Verifies transactional consistency: rollback undoes all operations.
#[tokio::test]
async fn should_rollback_transactional_operations_atomically() {
    let client = connect().await.unwrap();
    let cache_name = unique_name("blocking_tx_rollback");
    destroy_cache_if_exists(&client, &cache_name).await;

    let mut cfg = CacheConfiguration::new(&cache_name);
    cfg.atomicity_mode = AtomicityMode::Transactional;
    let cache = client
        .create_cache_with_config::<i32, i32>(&cfg)
        .await
        .unwrap();
    cache.put(&1, &100).await.unwrap();

    let tx = client
        .transactions()
        .tx_start(TransactionOptions::default())
        .await
        .unwrap();
    let tx_cache = tx.cache::<i32, i32>(&cache_name);
    tx_cache.put(&1, &200).await.unwrap();
    tx_cache.put(&2, &300).await.unwrap();
    tx.rollback().await.unwrap();

    // Rollback should undo both operations
    assert_eq!(cache.get(&1).await.unwrap(), Some(100));
    assert!(!cache.contains_key(&2).await.unwrap());

    client.destroy_cache(&cache_name).await.unwrap();
}

/// Java parity: org.apache.ignite.internal.client.thin.BlockingTxOpsTest#testCommitFutureChaining
///
/// Verifies that commit completes and subsequent operations on the same cache work.
#[tokio::test]
async fn should_chain_operations_after_commit() {
    let client = connect().await.unwrap();
    let cache_name = unique_name("blocking_tx_chain");
    destroy_cache_if_exists(&client, &cache_name).await;

    let mut cfg = CacheConfiguration::new(&cache_name);
    cfg.atomicity_mode = AtomicityMode::Transactional;
    let cache = client
        .create_cache_with_config::<i32, i32>(&cfg)
        .await
        .unwrap();

    // First transaction
    let tx1 = client
        .transactions()
        .tx_start(TransactionOptions::default())
        .await
        .unwrap();
    tx1.cache::<i32, i32>(&cache_name)
        .put(&1, &10)
        .await
        .unwrap();
    tx1.commit().await.unwrap();

    // Second transaction builds on the first
    let tx2 = client
        .transactions()
        .tx_start(TransactionOptions::default())
        .await
        .unwrap();
    let tx2_cache = tx2.cache::<i32, i32>(&cache_name);
    let val = tx2_cache.get(&1).await.unwrap();
    assert_eq!(val, Some(10));
    tx2_cache.put(&1, &20).await.unwrap();
    tx2.commit().await.unwrap();

    assert_eq!(cache.get(&1).await.unwrap(), Some(20));

    client.destroy_cache(&cache_name).await.unwrap();
}
