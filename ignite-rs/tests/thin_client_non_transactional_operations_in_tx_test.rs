#![cfg(not(feature = "ssl"))]

mod common;

use common::{connect, destroy_cache_if_exists, unique_name};
use ignite_rs::tx::TransactionOptions;

/// Java parity: org.apache.ignite.internal.client.thin.ThinClientNonTransactionalOperationsInTxTest#testClearNotAllowedInTx
#[tokio::test]
async fn should_reject_clear_operations_inside_explicit_transaction() {
    let client = connect().await.unwrap();
    let cache_name = unique_name("tx_clear");
    destroy_cache_if_exists(&client, &cache_name).await;
    client
        .get_or_create_cache::<i32, i32>(&cache_name)
        .await
        .unwrap();

    let tx = client
        .transactions()
        .tx_start(TransactionOptions::default())
        .await
        .unwrap();

    let cache = tx.cache::<i32, i32>(&cache_name);
    let err = cache.clear().await.unwrap_err();
    let msg = err.to_string();

    let _ = tx.rollback().await;
    destroy_cache_if_exists(&client, &cache_name).await;

    assert!(
        msg.contains("non-transactional ClientCache clear operation within a transaction"),
        "unexpected non-transactional-in-tx error: {}",
        msg
    );
}
