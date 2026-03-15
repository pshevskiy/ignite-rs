#![cfg(not(feature = "ssl"))]

mod common;

use common::{connect_cluster3, destroy_cache_if_exists, unique_name};
use ignite_rs::cache::{AtomicityMode, Cache, CacheConfiguration};
use ignite_rs::tx::TransactionOptions;
use ignite_rs::Client;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{oneshot, Barrier};

/// Java parity: org.apache.ignite.client.AsyncChannelTest#testAsyncRequests
#[tokio::test]
async fn should_allow_later_async_request_to_finish_before_blocked_one() {
    let ignite = connect_cluster3().await.unwrap();
    let blocker = connect_cluster3().await.unwrap();
    let cache_name = unique_name("async_channel_tx_requests");
    destroy_cache_if_exists(&ignite, &cache_name).await;

    let cache = create_transactional_cache(&ignite, &cache_name).await;

    let tx = blocker
        .transactions()
        .tx_start(TransactionOptions::default())
        .await
        .unwrap();
    let tx_cache = tx.cache::<i32, i32>(&cache_name);

    // Hold the key lock on `0` so the first request on the shared client blocks.
    tx_cache.put(&0, &0).await.unwrap();

    let slow_cache = ignite.cache::<i32, i32>(&cache_name);
    let (done_tx, mut done_rx) = oneshot::channel();
    tokio::spawn(async move {
        let res = async {
            slow_cache.put(&0, &0).await?;
            slow_cache.put(&1, &1).await?;
            slow_cache.get_size().await
        }
        .await;

        let _ = done_tx.send(res);
    });

    tokio::time::sleep(Duration::from_millis(150)).await;

    for key in 2..10 {
        tokio::time::timeout(Duration::from_secs(2), cache.put(&key, &key))
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for parallel put({})", key))
            .unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), cache.get(&key))
                .await
                .unwrap_or_else(|_| panic!("timed out waiting for parallel get({})", key))
                .unwrap(),
            Some(key)
        );
    }

    assert!(
        !tokio::time::timeout(Duration::from_secs(2), cache.contains_key(&1))
            .await
            .expect("timed out waiting for contains_key(1) while the blocked request is pending")
            .unwrap(),
        "parallel request should still be blocked behind the lock on key 0"
    );

    tx.commit().await.unwrap();

    let size = tokio::time::timeout(Duration::from_secs(5), &mut done_rx)
        .await
        .expect("timed out waiting for blocked async request to finish")
        .expect("blocked async request sender dropped unexpectedly")
        .unwrap();

    assert_eq!(size, 10);
    assert!(cache.contains_key(&1).await.unwrap());
    ignite.destroy_cache(&cache_name).await.unwrap();
}

/// Java parity: org.apache.ignite.client.AsyncChannelTest#testConcurrentRequests
#[tokio::test]
async fn should_complete_concurrent_cache_requests_on_single_client() {
    let ignite = Arc::new(connect_cluster3().await.unwrap());
    let cache_name = unique_name("async_channel_concurrent_requests");
    destroy_cache_if_exists(&ignite, &cache_name).await;

    let cache = create_transactional_cache(&ignite, &cache_name).await;
    let barrier = Arc::new(Barrier::new(25));
    let next_key = Arc::new(AtomicI32::new(0));
    let mut tasks = Vec::new();

    for _ in 0..25 {
        let barrier = barrier.clone();
        let client = ignite.clone();
        let cache_name = cache_name.clone();
        let next_key = next_key.clone();

        tasks.push(tokio::spawn(async move {
            barrier.wait().await;

            let cache = client.cache::<i32, i32>(&cache_name);
            for _ in 0..100 {
                let key = next_key.fetch_add(1, Ordering::SeqCst) + 1;
                cache.put(&key, &key).await.unwrap();
                assert_eq!(cache.get(&key).await.unwrap(), Some(key));
            }
        }));
    }

    for task in tasks {
        task.await.unwrap();
    }

    assert_eq!(cache.get_size().await.unwrap(), 2_500);
    ignite.destroy_cache(&cache_name).await.unwrap();
}

/// Java parity: org.apache.ignite.client.AsyncChannelTest#testConcurrentQueries
#[tokio::test]
async fn should_complete_concurrent_scan_queries_on_single_client() {
    let ignite = Arc::new(connect_cluster3().await.unwrap());
    let cache_name = unique_name("async_channel_concurrent_queries");
    destroy_cache_if_exists(&ignite, &cache_name).await;

    let cache = create_transactional_cache(&ignite, &cache_name).await;
    for key in 0..10 {
        cache.put(&key, &key).await.unwrap();
    }

    let barrier = Arc::new(Barrier::new(25));
    let mut tasks = Vec::new();

    for _ in 0..25 {
        let barrier = barrier.clone();
        let client = ignite.clone();
        let cache_name = cache_name.clone();

        tasks.push(tokio::spawn(async move {
            barrier.wait().await;

            let cache = client.cache::<i32, i32>(&cache_name);
            for _ in 0..10 {
                let rows = cache.query_scan(1).await.unwrap();
                assert_eq!(rows.len(), 10);
            }
        }));
    }

    for task in tasks {
        task.await.unwrap();
    }

    ignite.destroy_cache(&cache_name).await.unwrap();
}

async fn create_transactional_cache(client: &Client, cache_name: &str) -> Cache<i32, i32> {
    let mut cfg = CacheConfiguration::new(cache_name);
    cfg.atomicity_mode = AtomicityMode::Transactional;

    client
        .create_cache_with_config::<i32, i32>(&cfg)
        .await
        .unwrap()
}
