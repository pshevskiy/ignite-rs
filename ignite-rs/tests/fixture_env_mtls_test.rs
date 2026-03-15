#![cfg(feature = "ssl")]

mod common;

use common::{connect_mtls, ignite_mtls_env};

#[tokio::test]
async fn should_start_mtls_fixture_and_accept_mutual_tls_connections() {
    let env = ignite_mtls_env();
    env.wait_for_ready().await.unwrap();
    assert_eq!(env.tls_server_name(), Some("ignite.apache.org"));
    assert!(
        env.ca_pem().is_some(),
        "expected CA PEM path for mTLS fixture"
    );
    assert!(
        env.client_cert_pem().is_some() && env.client_key_pem().is_some(),
        "expected client cert/key PEM paths for mTLS fixture"
    );

    let client = connect_mtls().await.unwrap();
    let names = client.get_cache_names().await.unwrap();

    assert!(
        !names.iter().any(|name| name == "__unexpected__"),
        "unexpected cache names payload from mTLS fixture: {:?}",
        names
    );
}
