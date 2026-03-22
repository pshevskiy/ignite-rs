#![cfg(not(feature = "ssl"))]

mod common;

use common::{connect, ignite_single_node_churn_env};
use std::time::Duration;

#[tokio::test]
async fn should_stop_and_restart_single_node_churn_fixture() {
    let env = ignite_single_node_churn_env();
    env.wait_for_ready().await.unwrap();

    env.stop();
    tokio::time::sleep(Duration::from_millis(200)).await;
    env.start();
    env.wait_for_ready().await.unwrap();

    let client = connect().await.unwrap();
    let names = client.get_cache_names().await.unwrap();
    assert!(
        !names.iter().any(|name| name == "__unexpected__"),
        "unexpected cache names payload after single-node churn restart: {:?}",
        names
    );
}
