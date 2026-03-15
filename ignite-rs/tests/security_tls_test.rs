#![cfg(feature = "ssl")]

mod common;

use common::connect_mtls;

/// Java parity: org.apache.ignite.client.SecurityTest#testEncryption
#[tokio::test]
async fn should_list_caches_over_mutual_tls() -> Result<(), Box<dyn std::error::Error>> {
    let client = connect_mtls().await?;
    let names = client.get_cache_names().await?;
    assert!(!names.is_empty(), "ignite returned no caches over mTLS");
    Ok(())
}
