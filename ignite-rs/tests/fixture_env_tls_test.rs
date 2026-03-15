#![cfg(feature = "ssl")]

mod common;

use common::{connect_tls, ignite_tls_env};

#[tokio::test]
async fn should_start_tls_fixture_and_accept_server_authenticated_connections() {
    let env = ignite_tls_env();
    env.wait_for_ready().await.unwrap();
    assert_eq!(env.tls_server_name(), Some("ignite.apache.org"));
    assert!(
        env.ca_pem().is_some(),
        "expected CA PEM path for TLS fixture"
    );

    let client = connect_tls().await.unwrap();
    let names = client.get_cache_names().await.unwrap();

    assert!(
        !names.iter().any(|name| name == "__unexpected__"),
        "unexpected cache names payload from TLS fixture: {:?}",
        names
    );
}
