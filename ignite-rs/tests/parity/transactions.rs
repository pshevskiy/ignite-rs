//! Tier-2 parity — transactions.
//!
//! Each tx case needs a `TRANSACTIONAL` cache — we create it via the Java
//! driver's `get_or_create_tx_cache` op. Without it, the server rejects
//! tx_start on an ATOMIC cache with "Failed to start transaction".

use super::common::{ignite_scope, IgniteProfile};
use super::parity::{driver_jar_built, jd_call_ok, jd_connected};
use ignite_rs::cache::Cache;
use ignite_rs::tx::{TransactionConcurrency, TransactionIsolation, TransactionOptions};
use serde_json::json;
use std::time::Duration;

fn tx_cache_name(suffix: &str) -> String {
    format!("PARITY_TX_{}_{}", suffix, std::process::id())
}

macro_rules! require_jar {
    () => {
        if !driver_jar_built() {
            eprintln!("[parity] driver JAR missing — skipping");
            return;
        }
    };
}

/// Java tx commit propagates across the wire. Rust reads the committed
/// value outside the tx.
#[tokio::test]
async fn tx_commit_visible_cross_client() {
    require_jar!();
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    let name = tx_cache_name("COMMIT");
    jd_call_ok(
        &jd,
        "c1",
        "get_or_create_tx_cache",
        json!({"cache": name}),
    )
    .await;
    let cache: Cache<String, String> = rs
        .get_or_create_cache::<String, String>(&name)
        .await
        .expect("rs get_or_create_cache");

    let r = jd_call_ok(
        &jd,
        "t1",
        "tx_put_commit",
        json!({
            "cache": name,
            "key": "k",
            "value": "tx_val",
            "concurrency": "PESSIMISTIC",
            "isolation": "REPEATABLE_READ",
        }),
    )
    .await;
    assert!(r.ok);
    assert_eq!(
        cache.get(&"k".to_string()).await.unwrap().as_deref(),
        Some("tx_val")
    );

    jd_call_ok(&jd, "d1", "destroy_cache", json!({"cache": name})).await;
    jd.shutdown().await;
}

/// Java tx rollback leaves no trace. Rust sees no value.
#[tokio::test]
async fn tx_rollback_invisible_cross_client() {
    require_jar!();
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    let name = tx_cache_name("RB");
    jd_call_ok(
        &jd,
        "c1",
        "get_or_create_tx_cache",
        json!({"cache": name}),
    )
    .await;
    let cache: Cache<String, String> = rs
        .get_or_create_cache::<String, String>(&name)
        .await
        .expect("rs get_or_create_cache");

    let r = jd_call_ok(
        &jd,
        "t1",
        "tx_put_rollback",
        json!({
            "cache": name,
            "key": "k",
            "value": "lost",
            "concurrency": "PESSIMISTIC",
            "isolation": "REPEATABLE_READ",
        }),
    )
    .await;
    assert!(r.ok);
    assert!(cache.get(&"k".to_string()).await.unwrap().is_none());

    jd_call_ok(&jd, "d1", "destroy_cache", json!({"cache": name})).await;
    jd.shutdown().await;
}

/// Rust tx commit propagates to Java (mirror direction).
#[tokio::test]
async fn rust_tx_commit_visible_to_java() {
    require_jar!();
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    let name = tx_cache_name("RS_COMMIT");
    jd_call_ok(
        &jd,
        "c1",
        "get_or_create_tx_cache",
        json!({"cache": name}),
    )
    .await;
    let cache: Cache<String, String> = rs
        .get_or_create_cache::<String, String>(&name)
        .await
        .expect("rs get_or_create_cache");

    let txs = rs.transactions();
    let tx = txs
        .tx_start(TransactionOptions::new())
        .await
        .expect("rs tx_start");
    let tx_cache = tx.cache::<String, String>(&name);
    tx_cache
        .put(&"k".to_string(), &"rs_committed".to_string())
        .await
        .expect("rs put in tx");
    tx.commit().await.expect("rs commit");
    drop(tx);

    let r = jd_call_ok(&jd, "g1", "get", json!({"cache": name, "key": "k"})).await;
    assert_eq!(
        r.body.get("value").and_then(|v| v.as_str()),
        Some("rs_committed")
    );
    // Post-commit: Rust sees it too.
    assert_eq!(
        cache.get(&"k".to_string()).await.unwrap().as_deref(),
        Some("rs_committed")
    );

    jd_call_ok(&jd, "d1", "destroy_cache", json!({"cache": name})).await;
    jd.shutdown().await;
}

/// Rust tx rollback invisible to Java (mirror).
#[tokio::test]
async fn rust_tx_rollback_invisible_to_java() {
    require_jar!();
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    let name = tx_cache_name("RS_RB");
    jd_call_ok(
        &jd,
        "c1",
        "get_or_create_tx_cache",
        json!({"cache": name}),
    )
    .await;

    let txs = rs.transactions();
    let tx = txs
        .tx_start(TransactionOptions::new())
        .await
        .expect("rs tx_start");
    let tx_cache = tx.cache::<String, String>(&name);
    tx_cache
        .put(&"k".to_string(), &"rolled".to_string())
        .await
        .expect("rs put in tx");
    tx.rollback().await.expect("rs rollback");
    drop(tx);

    let r = jd_call_ok(&jd, "g1", "get", json!({"cache": name, "key": "k"})).await;
    assert!(r.body.get("value").map(|v| v.is_null()).unwrap_or(false));

    jd_call_ok(&jd, "d1", "destroy_cache", json!({"cache": name})).await;
    jd.shutdown().await;
}

/// OPTIMISTIC/SERIALIZABLE concurrency + isolation combo — commit works
/// across both clients.
#[tokio::test]
async fn tx_optimistic_serializable_commit() {
    require_jar!();
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    let name = tx_cache_name("OS");
    jd_call_ok(
        &jd,
        "c1",
        "get_or_create_tx_cache",
        json!({"cache": name}),
    )
    .await;
    let cache: Cache<String, String> = rs
        .get_or_create_cache::<String, String>(&name)
        .await
        .expect("rs get_or_create_cache");

    let r = jd_call_ok(
        &jd,
        "t1",
        "tx_put_commit",
        json!({
            "cache": name,
            "key": "k",
            "value": "v",
            "concurrency": "OPTIMISTIC",
            "isolation": "SERIALIZABLE",
        }),
    )
    .await;
    assert!(r.ok, "optimistic-serializable commit should succeed: {:?}", r.error);
    assert_eq!(
        cache.get(&"k".to_string()).await.unwrap().as_deref(),
        Some("v")
    );

    jd_call_ok(&jd, "d1", "destroy_cache", json!({"cache": name})).await;
    jd.shutdown().await;
}

/// PESSIMISTIC/READ_COMMITTED combo — commit works.
#[tokio::test]
async fn tx_pessimistic_read_committed_commit() {
    require_jar!();
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    let name = tx_cache_name("PRC");
    jd_call_ok(
        &jd,
        "c1",
        "get_or_create_tx_cache",
        json!({"cache": name}),
    )
    .await;
    let cache: Cache<String, String> = rs
        .get_or_create_cache::<String, String>(&name)
        .await
        .expect("rs get_or_create_cache");

    jd_call_ok(
        &jd,
        "t1",
        "tx_put_commit",
        json!({
            "cache": name,
            "key": "k",
            "value": "v",
            "concurrency": "PESSIMISTIC",
            "isolation": "READ_COMMITTED",
        }),
    )
    .await;
    assert_eq!(
        cache.get(&"k".to_string()).await.unwrap().as_deref(),
        Some("v")
    );

    jd_call_ok(&jd, "d1", "destroy_cache", json!({"cache": name})).await;
    jd.shutdown().await;
}

/// OPTIMISTIC/REPEATABLE_READ combo — commit works.
#[tokio::test]
async fn tx_optimistic_repeatable_read_commit() {
    require_jar!();
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    let name = tx_cache_name("ORR");
    jd_call_ok(
        &jd,
        "c1",
        "get_or_create_tx_cache",
        json!({"cache": name}),
    )
    .await;
    let cache: Cache<String, String> = rs
        .get_or_create_cache::<String, String>(&name)
        .await
        .expect("rs get_or_create_cache");

    jd_call_ok(
        &jd,
        "t1",
        "tx_put_commit",
        json!({
            "cache": name,
            "key": "k",
            "value": "v",
            "concurrency": "OPTIMISTIC",
            "isolation": "REPEATABLE_READ",
        }),
    )
    .await;
    assert_eq!(
        cache.get(&"k".to_string()).await.unwrap().as_deref(),
        Some("v")
    );

    jd_call_ok(&jd, "d1", "destroy_cache", json!({"cache": name})).await;
    jd.shutdown().await;
}

/// Java uncommitted tx is invisible to Rust; once Java commits, Rust sees it.
/// Uses Java's persistent-tx handle so the commit is delayed until we inspect
/// the cache from Rust during the open window.
#[tokio::test]
async fn tx_pending_invisible_until_commit() {
    require_jar!();
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    let name = tx_cache_name("PEND");
    jd_call_ok(
        &jd,
        "c1",
        "get_or_create_tx_cache",
        json!({"cache": name}),
    )
    .await;
    let cache: Cache<String, String> = rs
        .get_or_create_cache::<String, String>(&name)
        .await
        .expect("rs get_or_create_cache");

    // Open a tx on Java side, put a value, DON'T commit yet.
    let r = jd_call_ok(
        &jd,
        "ts",
        "tx_start_persist",
        json!({
            "concurrency": "PESSIMISTIC",
            "isolation": "REPEATABLE_READ",
        }),
    )
    .await;
    let handle = r
        .body
        .get("handle")
        .and_then(|v| v.as_str())
        .expect("tx handle")
        .to_string();
    jd_call_ok(
        &jd,
        "tp",
        "tx_put_in_persist",
        json!({"handle": handle, "cache": name, "key": "k", "value": "pending"}),
    )
    .await;

    // Rust shouldn't see the uncommitted value (other tx, different read-view).
    assert!(cache.get(&"k".to_string()).await.unwrap().is_none());

    // Commit from Java.
    jd_call_ok(
        &jd,
        "te",
        "tx_end_persist",
        json!({"handle": handle, "commit": true}),
    )
    .await;
    // Now visible.
    assert_eq!(
        cache.get(&"k".to_string()).await.unwrap().as_deref(),
        Some("pending")
    );

    jd_call_ok(&jd, "d1", "destroy_cache", json!({"cache": name})).await;
    jd.shutdown().await;
}

/// Rust tx with an explicit timeout (non-zero) still commits successfully
/// when the work is fast.
#[tokio::test]
async fn rust_tx_with_timeout_commits() {
    require_jar!();
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    let name = tx_cache_name("RTO");
    jd_call_ok(
        &jd,
        "c1",
        "get_or_create_tx_cache",
        json!({"cache": name}),
    )
    .await;

    let txs = rs.transactions();
    let tx = txs
        .tx_start(
            TransactionOptions::new()
                .with_concurrency(TransactionConcurrency::Pessimistic)
                .with_isolation(TransactionIsolation::RepeatableRead)
                .with_timeout(Duration::from_secs(5)),
        )
        .await
        .expect("rs tx_start with timeout");
    tx.cache::<String, String>(&name)
        .put(&"k".to_string(), &"done".to_string())
        .await
        .expect("put in tx");
    tx.commit().await.expect("commit");
    drop(tx);

    let r = jd_call_ok(&jd, "g1", "get", json!({"cache": name, "key": "k"})).await;
    assert_eq!(r.body.get("value").and_then(|v| v.as_str()), Some("done"));
    jd_call_ok(&jd, "d1", "destroy_cache", json!({"cache": name})).await;
    jd.shutdown().await;
}
