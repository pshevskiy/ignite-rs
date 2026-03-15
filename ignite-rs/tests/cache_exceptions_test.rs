#![cfg(not(feature = "ssl"))]

mod common;

use common::{connect, destroy_cache_if_exists, unique_name};
use ignite_rs::query::{ScanQuery, SqlFieldsQuery, SqlQuery};

/// Java parity: org.apache.ignite.internal.client.thin.CacheExceptionsTest#testCacheExceptionWrapped
#[tokio::test]
async fn should_include_missing_cache_context_in_errors() {
    let ignite = connect().await.unwrap();
    let cache_name = unique_name("missing_cache");
    destroy_cache_if_exists(&ignite, &cache_name).await;
    let _ = ignite
        .get_or_create_cache::<i32, i32>(&cache_name)
        .await
        .unwrap();
    ignite.destroy_cache(&cache_name).await.unwrap();

    let cache = ignite.cache::<i32, i32>(&cache_name);

    let get_err = cache.get(&0).await.unwrap_err().to_string();
    assert!(
        get_err.contains(&cache_name) || get_err.contains("Cache does not exist"),
        "unexpected get error for missing cache {}: {}",
        cache_name,
        get_err
    );

    let size_err = cache.get_size().await.unwrap_err().to_string();
    assert!(
        size_err.contains(&cache_name) || size_err.contains("Cache does not exist"),
        "unexpected size error for missing cache {}: {}",
        cache_name,
        size_err
    );

    let query_err = cache.query_scan(1).await.unwrap_err().to_string();
    assert!(
        query_err.contains(&cache_name) || query_err.contains("Cache does not exist"),
        "unexpected query error for missing cache {}: {}",
        cache_name,
        query_err
    );

    let affinity_query_err = match cache.scan_query(ScanQuery::new().with_partition(0)).await {
        Ok(_) => panic!(
            "expected partition scan query to fail for missing cache {}",
            cache_name
        ),
        Err(err) => err.to_string(),
    };
    assert!(
        affinity_query_err.contains(&cache_name)
            || affinity_query_err.contains("Cache does not exist"),
        "unexpected affinity query error for missing cache {}: {}",
        cache_name,
        affinity_query_err
    );

    let sql_fields_err = match cache
        .sql_fields(SqlFieldsQuery::<i64>::new("SELECT 1"))
        .await
    {
        Ok(_) => panic!(
            "expected sql fields query to fail for missing cache {}",
            cache_name
        ),
        Err(err) => err.to_string(),
    };
    assert!(
        sql_fields_err.contains(&cache_name) || sql_fields_err.contains("Cache does not exist"),
        "unexpected sql fields error for missing cache {}: {}",
        cache_name,
        sql_fields_err
    );

    let sql_query_err = match cache
        .sql_query(SqlQuery::new("MissingType", "id = 1"))
        .await
    {
        Ok(_) => panic!(
            "expected sql query to fail for missing cache {}",
            cache_name
        ),
        Err(err) => err.to_string(),
    };
    assert!(
        sql_query_err.contains(&cache_name) || sql_query_err.contains("Cache does not exist"),
        "unexpected sql query error for missing cache {}: {}",
        cache_name,
        sql_query_err
    );
}

/// Java parity: org.apache.ignite.internal.client.thin.CacheExceptionsTest#testCacheExceptionWrapped
#[tokio::test]
async fn should_preserve_missing_cache_context_for_tx_scoped_errors() {
    let ignite = connect().await.unwrap();
    let cache_name = unique_name("missing_cache_tx");
    destroy_cache_if_exists(&ignite, &cache_name).await;
    let _ = ignite
        .get_or_create_cache::<i32, i32>(&cache_name)
        .await
        .unwrap();
    ignite.destroy_cache(&cache_name).await.unwrap();
    let tx = ignite
        .transactions()
        .tx_start(Default::default())
        .await
        .unwrap();
    let cache = tx.cache::<i32, i32>(&cache_name);

    let err = cache.get(&0).await.unwrap_err().to_string();
    assert!(
        err.contains(&cache_name) || err.contains("Cache does not exist"),
        "unexpected tx-scoped missing-cache error for {}: {}",
        cache_name,
        err
    );
    assert!(
        !err.contains("Transaction context has been lost due to connection errors"),
        "unexpected tx-lost classification for missing-cache error: {}",
        err
    );
}
