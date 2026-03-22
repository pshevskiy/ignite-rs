#![cfg(not(feature = "ssl"))]

mod common;

use common::ignite_cluster3_churn_env;
use ignite_rs::{new_client, ClientConfig};
use std::sync::OnceLock;

static CLUSTER_RESTART_LOCK: OnceLock<std::sync::Mutex<()>> = OnceLock::new();

/// Java parity: org.apache.ignite.internal.client.thin.ClusterGroupClusterRestartTest#testClusterGroupAfterRestart
///
/// Verifies that cluster node information refreshes after a node restart.
#[tokio::test]
async fn should_refresh_cluster_nodes_after_restart() {
    let _guard = restart_lock()
        .lock()
        .expect("cluster restart lock poisoned");
    let env = ignite_cluster3_churn_env();
    if !env.is_managed() {
        return;
    }
    env.restart_all();
    env.wait_for_ready().await.unwrap();

    let mut conf = ClientConfig::from_addresses(env.addresses().iter().cloned());
    conf.retry_limit = 4;
    conf.reconnect_backoff = Some(std::time::Duration::from_millis(200));
    let client = new_client(conf).await.unwrap();
    let cluster = client.cluster();

    // Get initial node list
    let initial_nodes = cluster.nodes().await.unwrap();
    assert!(
        initial_nodes.len() >= 2,
        "expected at least 2 nodes, got {}",
        initial_nodes.len()
    );
    let initial_ids: Vec<String> = initial_nodes.iter().map(|n| n.id.clone()).collect();

    // Restart a node — give the container time to fully stop and start.
    env.stop_node(0);
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    env.start_node(0);
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    env.wait_for_ready().await.unwrap();

    // Give the client time to detect topology change
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // The refreshed node list may differ (restarted node gets new UUID)
    let refreshed_nodes = cluster.nodes().await.unwrap();
    assert!(
        refreshed_nodes.len() >= 2,
        "expected at least 2 nodes after restart, got {}",
        refreshed_nodes.len()
    );

    // At least some node should still be present (the ones that didn't restart)
    let refreshed_ids: Vec<String> = refreshed_nodes.iter().map(|n| n.id.clone()).collect();
    let surviving = initial_ids
        .iter()
        .filter(|id| refreshed_ids.contains(id))
        .count();
    assert!(
        surviving >= 1,
        "expected at least 1 surviving node after single-node restart"
    );
}

fn restart_lock() -> &'static std::sync::Mutex<()> {
    CLUSTER_RESTART_LOCK.get_or_init(|| std::sync::Mutex::new(()))
}
