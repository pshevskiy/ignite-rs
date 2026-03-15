#![cfg(not(feature = "ssl"))]

mod common;

use common::{ignite_scope, IgniteProfile};
use ignite_rs::cluster::ClusterState;
use std::sync::OnceLock;

static CLUSTER_API_TEST_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

/// Related Apache Ignite cluster-state coverage:
/// org.apache.ignite.internal.client.thin.ClusterApiTest#testClusterState
#[tokio::test]
async fn should_get_and_change_cluster_state_live() {
    let _guard = cluster_api_test_lock().lock().await;
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    scope.context().ensure_cluster_active().await.unwrap();

    let client = scope.connect().await.unwrap();
    let cluster = client.cluster();
    assert_eq!(cluster.state().await.unwrap(), ClusterState::Active);

    cluster
        .set_state_with_force(ClusterState::Active, false)
        .await
        .unwrap();
    assert_eq!(cluster.state().await.unwrap(), ClusterState::Active);
}

fn cluster_api_test_lock() -> &'static tokio::sync::Mutex<()> {
    CLUSTER_API_TEST_LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}
