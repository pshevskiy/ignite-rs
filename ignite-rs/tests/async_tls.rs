#![cfg(feature = "ssl")]

use ignite_rs::{client_config_from_ca_pem, new_client};
use std::env;

#[tokio::test]
async fn async_tls_should_list_caches() -> Result<(), Box<dyn std::error::Error>> {
    let addr = env::var("IGNITE_TLS_ADDR").expect("IGNITE_TLS_ADDR must be set for TLS test");
    let sni = env::var("IGNITE_TLS_SERVER_NAME")
        .expect("IGNITE_TLS_SERVER_NAME must be set for TLS test");
    let ca_pem = env::var("IGNITE_TLS_CA_PEM").expect("IGNITE_TLS_CA_PEM must be set for TLS test");

    let conf = client_config_from_ca_pem(&addr, &ca_pem, &sni)?;
    let client = new_client(conf).await?;
    let names = client.get_cache_names().await?;

    if let Ok(expected) = env::var("IGNITE_EXPECTED_CACHE_NAMES") {
        let expected_names: Vec<String> =
            expected.split(',').map(|s| s.trim().to_string()).collect();
        assert_eq!(names, expected_names);
    } else {
        // At least must be non-empty to prove connectivity.
        assert!(!names.is_empty(), "ignite returned no caches over TLS");
    }

    Ok(())
}

#[tokio::test]
async fn async_mtls_should_list_caches() -> Result<(), Box<dyn std::error::Error>> {
    use std::env;

    let addr = env::var("IGNITE_TLS_ADDR").expect("IGNITE_TLS_ADDR must be set for mTLS test");
    let sni = env::var("IGNITE_TLS_SERVER_NAME")
        .expect("IGNITE_TLS_SERVER_NAME must be set for mTLS test");
    let ca_pem =
        env::var("IGNITE_TLS_CA_PEM").expect("IGNITE_TLS_CA_PEM must be set for mTLS test");
    let client_cert_pem = env::var("IGNITE_TLS_CLIENT_CERT_PEM")
        .expect("IGNITE_TLS_CLIENT_CERT_PEM must be set for mTLS test");
    let client_key_pem = env::var("IGNITE_TLS_CLIENT_KEY_PEM")
        .expect("IGNITE_TLS_CLIENT_KEY_PEM must be set for mTLS test");

    let conf = ignite_rs::client_config_from_ca_and_client_pem(
        &addr,
        &ca_pem,
        &client_cert_pem,
        &client_key_pem,
        &sni,
    )?;
    let client = new_client(conf).await?;
    let names = client.get_cache_names().await?;
    assert!(!names.is_empty(), "ignite returned no caches over mTLS");
    Ok(())
}
