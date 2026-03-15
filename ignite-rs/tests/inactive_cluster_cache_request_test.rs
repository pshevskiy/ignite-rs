#![cfg(not(feature = "ssl"))]

mod common;

use common::{destroy_cache_if_exists, ignite_scope, unique_name, IgniteProfile};
use ignite_rs::cluster::ClusterState;
use ignite_rs::new_client;
use std::sync::OnceLock;

static INACTIVE_CLUSTER_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

/// Java parity: org.apache.ignite.internal.client.thin.InactiveClusterCacheRequestTest#testCacheOperationReturnErrorOnInactiveCluster
#[tokio::test]
async fn should_return_inactive_cluster_error_for_cache_get_without_partition_awareness() {
    let _guard = inactive_cluster_lock().lock().await;
    assert_inactive_cluster_error(false).await;
}

/// Java parity: org.apache.ignite.internal.client.thin.InactiveClusterCacheRequestTest#testCacheOperationReturnErrorOnInactiveCluster
#[tokio::test]
async fn should_return_inactive_cluster_error_for_cache_get_with_partition_awareness() {
    let _guard = inactive_cluster_lock().lock().await;
    assert_inactive_cluster_error(true).await;
}

async fn assert_inactive_cluster_error(partition_awareness_enabled: bool) {
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();

    let cache_name = unique_name("inactive_cluster");
    let mut cfg = scope.client_config().unwrap();
    cfg.partition_awareness_enabled = partition_awareness_enabled;
    cfg.retry_limit = 0;

    let client = new_client(cfg).await.unwrap();
    destroy_cache_if_exists(&client, &cache_name).await;
    client
        .get_or_create_cache::<i32, i32>(&cache_name)
        .await
        .unwrap();

    client.cluster().set_state(ClusterState::Inactive).await.unwrap();

    let err = client
        .cache::<i32, i32>(&cache_name)
        .get(&0)
        .await
        .unwrap_err()
        .to_string();

    client.cluster().set_state(ClusterState::Active).await.unwrap();
    destroy_cache_if_exists(&client, &cache_name).await;

    let lower = err.to_ascii_lowercase();
    assert!(
        lower.contains("inactive") || lower.contains("not active"),
        "unexpected inactive cluster error: {}",
        err
    );
}

fn inactive_cluster_lock() -> &'static tokio::sync::Mutex<()> {
    INACTIVE_CLUSTER_LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}
