#![cfg(not(feature = "ssl"))]

mod common;

use common::{connect_cluster3, ignite_cluster3_env};

#[tokio::test]
async fn should_start_cluster3_fixture_and_accept_multi_address_connections() {
    let env = ignite_cluster3_env();
    env.wait_for_ready().await.unwrap();
    assert_eq!(
        env.addresses().len(),
        3,
        "cluster fixture should expose three thin-client addresses",
    );

    let client = connect_cluster3().await.unwrap();
    let names = client.get_cache_names().await.unwrap();

    assert!(
        !names.iter().any(|name| name == "__unexpected__"),
        "unexpected cache names payload from cluster fixture: {:?}",
        names
    );
}
