#![cfg(not(feature = "ssl"))]

mod common;

use common::{connect_cluster3_churn, ignite_cluster3_churn_env};

#[tokio::test]
async fn should_restart_cluster3_churn_node_and_recover_fixture() {
    let env = ignite_cluster3_churn_env();
    env.wait_for_ready().await.unwrap();

    env.restart_node(0);
    env.wait_for_ready().await.unwrap();

    let client = connect_cluster3_churn().await.unwrap();
    let names = client.get_cache_names().await.unwrap();
    assert!(
        !names.iter().any(|name| name == "__unexpected__"),
        "unexpected cache names payload after cluster churn node restart: {:?}",
        names
    );
}
