#![cfg(feature = "ssl")]

mod common;

use common::connect_mtls;

/// Java parity: org.apache.ignite.client.SecurityTest#testEncryption
#[tokio::test]
async fn should_list_caches_over_mutual_tls() -> Result<(), Box<dyn std::error::Error>> {
    let client = connect_mtls().await?;
    // Verify mTLS connection works by issuing a request.
    // A vanilla Ignite node may have no user caches — the important thing
    // is that the request succeeds over mutual TLS without error.
    let _names = client.get_cache_names().await?;
    Ok(())
}
