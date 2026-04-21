//! Tier-2 parity — cache CRUD ops cross-client.

use super::common::{ignite_scope, IgniteProfile};
use super::parity::{driver_jar_built, jd_call_ok, jd_connected};
use ignite_rs::cache::Cache;
use serde_json::json;

/// Rust put → Java get; Java put → Rust get. Simple string K/V round-trip.
#[tokio::test]
async fn put_get_cross_client() {
    if !driver_jar_built() {
        eprintln!("[parity] driver JAR missing — skipping");
        return;
    }
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    let cache_name = "PARITY_PUT_GET";
    // Create cache via Java side so Rust side can use the same name with
    // the expected default cache configuration. (Rust's `get_or_create_cache`
    // also works; either is fine.)
    jd_call_ok(&jd, "c1", "get_or_create_cache", json!({ "cache": cache_name })).await;

    let cache: Cache<String, String> = rs
        .get_or_create_cache::<String, String>(cache_name)
        .await
        .expect("rs get_or_create_cache");

    // Rust puts, Java reads.
    cache
        .put(&"k1".to_string(), &"v1".to_string())
        .await
        .expect("rs put");
    let r = jd_call_ok(
        &jd,
        "g1",
        "get",
        json!({"cache": cache_name, "key": "k1"}),
    )
    .await;
    assert_eq!(r.body.get("value").and_then(|v| v.as_str()), Some("v1"));

    // Java puts, Rust reads.
    jd_call_ok(
        &jd,
        "p2",
        "put",
        json!({"cache": cache_name, "key": "k2", "value": "v2"}),
    )
    .await;
    let v = cache.get(&"k2".to_string()).await.expect("rs get");
    assert_eq!(v.as_deref(), Some("v2"));

    // Cleanup to keep the fixture clean for other tests.
    jd_call_ok(&jd, "d1", "destroy_cache", json!({"cache": cache_name})).await;
    jd.shutdown().await;
}

/// Rust puts value; Java's `putIfAbsent` returns `inserted=false`.
/// Then Rust removes; Java's `putIfAbsent` returns `inserted=true`.
#[tokio::test]
async fn put_if_absent_cross_client() {
    if !driver_jar_built() {
        eprintln!("[parity] driver JAR missing — skipping");
        return;
    }
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    let cache_name = "PARITY_PIA";
    jd_call_ok(&jd, "c1", "get_or_create_cache", json!({ "cache": cache_name })).await;
    let cache: Cache<String, String> = rs
        .get_or_create_cache::<String, String>(cache_name)
        .await
        .expect("rs get_or_create_cache");

    // Rust pre-populates.
    cache
        .put(&"k".to_string(), &"v".to_string())
        .await
        .expect("rs put");
    // Java putIfAbsent → false.
    let r = jd_call_ok(
        &jd,
        "p1",
        "put_if_absent",
        json!({"cache": cache_name, "key": "k", "value": "other"}),
    )
    .await;
    assert_eq!(
        r.body.get("inserted").and_then(|v| v.as_bool()),
        Some(false)
    );
    let v = cache.get(&"k".to_string()).await.expect("rs get");
    assert_eq!(v.as_deref(), Some("v"));

    // Rust removes, Java putIfAbsent now → true.
    cache
        .remove_key(&"k".to_string())
        .await
        .expect("rs remove");
    let r = jd_call_ok(
        &jd,
        "p2",
        "put_if_absent",
        json!({"cache": cache_name, "key": "k", "value": "other"}),
    )
    .await;
    assert_eq!(
        r.body.get("inserted").and_then(|v| v.as_bool()),
        Some(true)
    );
    let v = cache.get(&"k".to_string()).await.expect("rs get");
    assert_eq!(v.as_deref(), Some("other"));

    jd_call_ok(&jd, "d1", "destroy_cache", json!({"cache": cache_name})).await;
    jd.shutdown().await;
}

/// Java's `replace(k, expected, new)` (CAS) — Rust writes a different current
/// value and Java's CAS should return `replaced=false`.
#[tokio::test]
async fn cas_replace_cross_client() {
    if !driver_jar_built() {
        eprintln!("[parity] driver JAR missing — skipping");
        return;
    }
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    let cache_name = "PARITY_CAS";
    jd_call_ok(&jd, "c1", "get_or_create_cache", json!({ "cache": cache_name })).await;
    let cache: Cache<String, String> = rs
        .get_or_create_cache::<String, String>(cache_name)
        .await
        .expect("rs get_or_create_cache");

    cache
        .put(&"k".to_string(), &"actual".to_string())
        .await
        .expect("rs put");

    // CAS with wrong expected — should not replace.
    let r = jd_call_ok(
        &jd,
        "r1",
        "replace_if_equals",
        json!({"cache": cache_name, "key": "k", "old": "wrong", "new": "next"}),
    )
    .await;
    assert_eq!(
        r.body.get("replaced").and_then(|v| v.as_bool()),
        Some(false)
    );
    let v = cache.get(&"k".to_string()).await.expect("rs get");
    assert_eq!(v.as_deref(), Some("actual"));

    // CAS with correct expected — should replace.
    let r = jd_call_ok(
        &jd,
        "r2",
        "replace_if_equals",
        json!({"cache": cache_name, "key": "k", "old": "actual", "new": "next"}),
    )
    .await;
    assert_eq!(
        r.body.get("replaced").and_then(|v| v.as_bool()),
        Some(true)
    );
    let v = cache.get(&"k".to_string()).await.expect("rs get");
    assert_eq!(v.as_deref(), Some("next"));

    jd_call_ok(&jd, "d1", "destroy_cache", json!({"cache": cache_name})).await;
    jd.shutdown().await;
}
