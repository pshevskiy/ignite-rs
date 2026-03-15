#![cfg(feature = "ssl")]

mod common;

use common::{connect_auth, ignite_auth_env};

#[tokio::test]
async fn should_start_auth_fixture_and_accept_authenticated_thin_client_connections() {
    let env = ignite_auth_env();
    env.wait_for_ready().await.unwrap();
    assert_eq!(env.username(), Some("ignite"));
    assert_eq!(env.password(), Some("ignite"));

    let client = connect_auth().await.unwrap();
    let names = client.get_cache_names().await.unwrap();

    assert!(
        !names.iter().any(|name| name == "__unexpected__"),
        "unexpected cache names payload from auth fixture: {:?}",
        names
    );
}
