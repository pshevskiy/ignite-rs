//! Tier-2 parity — cache CRUD ops cross-client.
//!
//! Each case sets up one parity cache per-test, exercises a single operation
//! group with both clients, and tears down. Caches are namespaced by PID
//! to avoid cross-test interference when parity tests run in parallel.

use super::common::{ignite_scope, IgniteProfile};
use super::parity::{driver_jar_built, jd_call_ok, jd_connected};
use ignite_rs::cache::Cache;
use serde_json::json;

/// Per-test cache name — unique per test + PID so runs don't collide.
fn cache_name(suffix: &str) -> String {
    format!("PARITY_CACHE_{}_{}", suffix, std::process::id())
}

// Small macro to skip the test when the JAR's not built — matches the rest
// of the file's pattern without duplicating the three lines in every test.
macro_rules! require_jar {
    () => {
        if !driver_jar_built() {
            eprintln!("[parity] driver JAR missing — skipping");
            return;
        }
    };
}

/// Rust put → Java get; Java put → Rust get. Simple string K/V round-trip.
#[tokio::test]
async fn put_get_cross_client() {
    require_jar!();
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    let name = cache_name("PUT_GET");
    jd_call_ok(&jd, "c1", "get_or_create_cache", json!({"cache": name})).await;
    let cache: Cache<String, String> = rs
        .get_or_create_cache::<String, String>(&name)
        .await
        .expect("rs get_or_create_cache");

    cache.put(&"k1".to_string(), &"v1".to_string()).await.unwrap();
    let r = jd_call_ok(&jd, "g1", "get", json!({"cache": name, "key": "k1"})).await;
    assert_eq!(r.body.get("value").and_then(|v| v.as_str()), Some("v1"));

    jd_call_ok(
        &jd,
        "p2",
        "put",
        json!({"cache": name, "key": "k2", "value": "v2"}),
    )
    .await;
    let v = cache.get(&"k2".to_string()).await.unwrap();
    assert_eq!(v.as_deref(), Some("v2"));

    jd_call_ok(&jd, "d1", "destroy_cache", json!({"cache": name})).await;
    jd.shutdown().await;
}

/// Rust puts value; Java's `putIfAbsent` returns `inserted=false`.
/// After Rust removes, Java's `putIfAbsent` returns `inserted=true`.
#[tokio::test]
async fn put_if_absent_cross_client() {
    require_jar!();
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    let name = cache_name("PIA");
    jd_call_ok(&jd, "c1", "get_or_create_cache", json!({"cache": name})).await;
    let cache: Cache<String, String> = rs
        .get_or_create_cache::<String, String>(&name)
        .await
        .expect("rs get_or_create_cache");

    cache.put(&"k".to_string(), &"v".to_string()).await.unwrap();
    let r = jd_call_ok(
        &jd,
        "p1",
        "put_if_absent",
        json!({"cache": name, "key": "k", "value": "other"}),
    )
    .await;
    assert_eq!(r.body.get("inserted").and_then(|v| v.as_bool()), Some(false));
    assert_eq!(
        cache.get(&"k".to_string()).await.unwrap().as_deref(),
        Some("v")
    );

    cache.remove_key(&"k".to_string()).await.unwrap();
    let r = jd_call_ok(
        &jd,
        "p2",
        "put_if_absent",
        json!({"cache": name, "key": "k", "value": "other"}),
    )
    .await;
    assert_eq!(r.body.get("inserted").and_then(|v| v.as_bool()), Some(true));
    assert_eq!(
        cache.get(&"k".to_string()).await.unwrap().as_deref(),
        Some("other")
    );

    jd_call_ok(&jd, "d1", "destroy_cache", json!({"cache": name})).await;
    jd.shutdown().await;
}

/// Rust's `put_if_absent` mirror: Java writes; Rust's put_if_absent → false.
#[tokio::test]
async fn put_if_absent_mirror_rust_sees_existing() {
    require_jar!();
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    let name = cache_name("PIA_M");
    jd_call_ok(&jd, "c1", "get_or_create_cache", json!({"cache": name})).await;
    let cache: Cache<String, String> = rs
        .get_or_create_cache::<String, String>(&name)
        .await
        .expect("rs get_or_create_cache");

    // Java puts first.
    jd_call_ok(
        &jd,
        "p1",
        "put",
        json!({"cache": name, "key": "k", "value": "existing"}),
    )
    .await;
    // Rust's put_if_absent → false.
    let inserted = cache
        .put_if_absent(&"k".to_string(), &"mine".to_string())
        .await
        .unwrap();
    assert!(!inserted, "Rust put_if_absent on existing key should return false");
    assert_eq!(
        cache.get(&"k".to_string()).await.unwrap().as_deref(),
        Some("existing")
    );
    jd_call_ok(&jd, "d1", "destroy_cache", json!({"cache": name})).await;
    jd.shutdown().await;
}

/// Java's `replace(k, expected, new)` (CAS) — Rust writes a different current
/// value and Java's CAS returns `replaced=false`.
#[tokio::test]
async fn cas_replace_cross_client() {
    require_jar!();
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    let name = cache_name("CAS");
    jd_call_ok(&jd, "c1", "get_or_create_cache", json!({"cache": name})).await;
    let cache: Cache<String, String> = rs
        .get_or_create_cache::<String, String>(&name)
        .await
        .expect("rs get_or_create_cache");

    cache.put(&"k".to_string(), &"actual".to_string()).await.unwrap();
    let r = jd_call_ok(
        &jd,
        "r1",
        "replace_if_equals",
        json!({"cache": name, "key": "k", "old": "wrong", "new": "next"}),
    )
    .await;
    assert_eq!(r.body.get("replaced").and_then(|v| v.as_bool()), Some(false));
    assert_eq!(
        cache.get(&"k".to_string()).await.unwrap().as_deref(),
        Some("actual")
    );
    let r = jd_call_ok(
        &jd,
        "r2",
        "replace_if_equals",
        json!({"cache": name, "key": "k", "old": "actual", "new": "next"}),
    )
    .await;
    assert_eq!(r.body.get("replaced").and_then(|v| v.as_bool()), Some(true));
    assert_eq!(
        cache.get(&"k".to_string()).await.unwrap().as_deref(),
        Some("next")
    );

    jd_call_ok(&jd, "d1", "destroy_cache", json!({"cache": name})).await;
    jd.shutdown().await;
}

/// Mirror: Rust's replace_if_equals agrees with Java-set state.
#[tokio::test]
async fn cas_replace_mirror_rust_side() {
    require_jar!();
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    let name = cache_name("CAS_M");
    jd_call_ok(&jd, "c1", "get_or_create_cache", json!({"cache": name})).await;
    let cache: Cache<String, String> = rs
        .get_or_create_cache::<String, String>(&name)
        .await
        .expect("rs get_or_create_cache");

    jd_call_ok(
        &jd,
        "p1",
        "put",
        json!({"cache": name, "key": "k", "value": "one"}),
    )
    .await;

    // Wrong-expected Rust CAS fails.
    let swapped = cache
        .replace_if_equals(&"k".to_string(), &"two".to_string(), &"three".to_string())
        .await
        .unwrap();
    assert!(!swapped);
    // Correct-expected Rust CAS succeeds.
    let swapped = cache
        .replace_if_equals(&"k".to_string(), &"one".to_string(), &"two".to_string())
        .await
        .unwrap();
    assert!(swapped);
    let r = jd_call_ok(&jd, "g1", "get", json!({"cache": name, "key": "k"})).await;
    assert_eq!(r.body.get("value").and_then(|v| v.as_str()), Some("two"));

    jd_call_ok(&jd, "d1", "destroy_cache", json!({"cache": name})).await;
    jd.shutdown().await;
}

/// `get_and_put` returns prior value. Both clients must return the same thing.
#[tokio::test]
async fn get_and_put_cross_client() {
    require_jar!();
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    let name = cache_name("GAP");
    jd_call_ok(&jd, "c1", "get_or_create_cache", json!({"cache": name})).await;
    let cache: Cache<String, String> = rs
        .get_or_create_cache::<String, String>(&name)
        .await
        .expect("rs get_or_create_cache");

    // Rust pre-populates, Java get_and_put returns the prior.
    cache.put(&"k".to_string(), &"first".to_string()).await.unwrap();
    let r = jd_call_ok(
        &jd,
        "g1",
        "get_and_put",
        json!({"cache": name, "key": "k", "value": "second"}),
    )
    .await;
    assert_eq!(
        r.body.get("previous").and_then(|v| v.as_str()),
        Some("first")
    );
    // Now Rust's get_and_put returns "second" and writes "third".
    let prev = cache
        .get_and_put(&"k".to_string(), &"third".to_string())
        .await
        .unwrap();
    assert_eq!(prev.as_deref(), Some("second"));
    let r = jd_call_ok(&jd, "v1", "get", json!({"cache": name, "key": "k"})).await;
    assert_eq!(r.body.get("value").and_then(|v| v.as_str()), Some("third"));

    jd_call_ok(&jd, "d1", "destroy_cache", json!({"cache": name})).await;
    jd.shutdown().await;
}

/// `remove_if_equals` — conditional remove. Java removes if current matches.
#[tokio::test]
async fn remove_if_equals_cross_client() {
    require_jar!();
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    let name = cache_name("RIE");
    jd_call_ok(&jd, "c1", "get_or_create_cache", json!({"cache": name})).await;
    let cache: Cache<String, String> = rs
        .get_or_create_cache::<String, String>(&name)
        .await
        .expect("rs get_or_create_cache");

    cache.put(&"k".to_string(), &"v".to_string()).await.unwrap();
    // Java remove_if_equals with wrong value → false.
    let r = jd_call_ok(
        &jd,
        "r1",
        "remove_if_equals",
        json!({"cache": name, "key": "k", "value": "wrong"}),
    )
    .await;
    assert_eq!(r.body.get("removed").and_then(|v| v.as_bool()), Some(false));
    // Mirror: Rust.
    let removed = cache
        .remove_if_equals(&"k".to_string(), &"wrong".to_string())
        .await
        .unwrap();
    assert!(!removed);
    // Java remove_if_equals with right value → true.
    let r = jd_call_ok(
        &jd,
        "r2",
        "remove_if_equals",
        json!({"cache": name, "key": "k", "value": "v"}),
    )
    .await;
    assert_eq!(r.body.get("removed").and_then(|v| v.as_bool()), Some(true));

    jd_call_ok(&jd, "d1", "destroy_cache", json!({"cache": name})).await;
    jd.shutdown().await;
}

/// Rust `put_all`; Java `get_all` reads same entries back.
#[tokio::test]
async fn put_all_get_all_cross_client() {
    require_jar!();
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    let name = cache_name("PA_GA");
    jd_call_ok(&jd, "c1", "get_or_create_cache", json!({"cache": name})).await;
    let cache: Cache<String, String> = rs
        .get_or_create_cache::<String, String>(&name)
        .await
        .expect("rs get_or_create_cache");

    let pairs: Vec<(String, String)> = (0..5)
        .map(|i| (format!("k{}", i), format!("v{}", i)))
        .collect();
    cache.put_all(&pairs).await.unwrap();

    let keys: Vec<String> = (0..5).map(|i| format!("k{}", i)).collect();
    let r = jd_call_ok(
        &jd,
        "ga1",
        "get_all",
        json!({"cache": name, "keys": keys}),
    )
    .await;
    let entries = r
        .body
        .get("entries")
        .and_then(|v| v.as_object())
        .expect("entries object");
    for i in 0..5 {
        let k = format!("k{}", i);
        let v = entries
            .get(&k)
            .and_then(|v| v.as_str())
            .unwrap_or_else(|| panic!("missing key {}", k));
        assert_eq!(v, &format!("v{}", i));
    }
    jd_call_ok(&jd, "d1", "destroy_cache", json!({"cache": name})).await;
    jd.shutdown().await;
}

/// Inverse: Java `put_all`; Rust `get_all` reads same entries back.
#[tokio::test]
async fn put_all_get_all_inverse() {
    require_jar!();
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    let name = cache_name("PA_GA_INV");
    jd_call_ok(&jd, "c1", "get_or_create_cache", json!({"cache": name})).await;
    let cache: Cache<String, String> = rs
        .get_or_create_cache::<String, String>(&name)
        .await
        .expect("rs get_or_create_cache");

    let entries: Vec<serde_json::Value> = (0..4)
        .map(|i| json!({"key": format!("k{}", i), "value": format!("v{}", i)}))
        .collect();
    jd_call_ok(
        &jd,
        "pa",
        "put_all",
        json!({"cache": name, "entries": entries}),
    )
    .await;

    let keys: Vec<String> = (0..4).map(|i| format!("k{}", i)).collect();
    let rs_pairs = cache.get_all(&keys).await.unwrap();
    assert_eq!(rs_pairs.len(), 4);
    let mut by_k: std::collections::HashMap<String, String> = Default::default();
    for (k, v) in rs_pairs {
        by_k.insert(k.unwrap_or_default(), v.unwrap_or_default());
    }
    for i in 0..4 {
        assert_eq!(
            by_k.get(&format!("k{}", i)).map(|s| s.as_str()),
            Some(format!("v{}", i).as_str())
        );
    }

    jd_call_ok(&jd, "d1", "destroy_cache", json!({"cache": name})).await;
    jd.shutdown().await;
}

/// `contains_key` behaviour parity — both clients agree on key presence.
#[tokio::test]
async fn contains_key_cross_client() {
    require_jar!();
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    let name = cache_name("CK");
    jd_call_ok(&jd, "c1", "get_or_create_cache", json!({"cache": name})).await;
    let cache: Cache<String, String> = rs
        .get_or_create_cache::<String, String>(&name)
        .await
        .expect("rs get_or_create_cache");

    cache.put(&"present".to_string(), &"v".to_string()).await.unwrap();
    let r = jd_call_ok(
        &jd,
        "c2",
        "contains_key",
        json!({"cache": name, "key": "present"}),
    )
    .await;
    assert_eq!(r.body.get("contains").and_then(|v| v.as_bool()), Some(true));
    assert!(cache.contains_key(&"present".to_string()).await.unwrap());

    let r = jd_call_ok(
        &jd,
        "c3",
        "contains_key",
        json!({"cache": name, "key": "absent"}),
    )
    .await;
    assert_eq!(r.body.get("contains").and_then(|v| v.as_bool()), Some(false));
    assert!(!cache.contains_key(&"absent".to_string()).await.unwrap());

    jd_call_ok(&jd, "d1", "destroy_cache", json!({"cache": name})).await;
    jd.shutdown().await;
}

/// `size` — Rust populates, Java reports same count.
#[tokio::test]
async fn size_cross_client() {
    require_jar!();
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    let name = cache_name("SZ");
    jd_call_ok(&jd, "c1", "get_or_create_cache", json!({"cache": name})).await;
    let cache: Cache<String, String> = rs
        .get_or_create_cache::<String, String>(&name)
        .await
        .expect("rs get_or_create_cache");

    for i in 0..7 {
        cache
            .put(&format!("k{}", i), &"v".to_string())
            .await
            .unwrap();
    }
    let r = jd_call_ok(&jd, "s1", "size", json!({"cache": name})).await;
    assert_eq!(r.body.get("size").and_then(|v| v.as_i64()), Some(7));
    assert_eq!(cache.get_size().await.unwrap(), 7);

    jd_call_ok(&jd, "d1", "destroy_cache", json!({"cache": name})).await;
    jd.shutdown().await;
}

/// `clear` — Java clears the cache, Rust sees size 0.
#[tokio::test]
async fn clear_cross_client() {
    require_jar!();
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    let name = cache_name("CLR");
    jd_call_ok(&jd, "c1", "get_or_create_cache", json!({"cache": name})).await;
    let cache: Cache<String, String> = rs
        .get_or_create_cache::<String, String>(&name)
        .await
        .expect("rs get_or_create_cache");

    for i in 0..3 {
        cache.put(&format!("k{}", i), &"v".to_string()).await.unwrap();
    }
    jd_call_ok(&jd, "clr", "clear", json!({"cache": name})).await;
    assert_eq!(cache.get_size().await.unwrap(), 0);

    jd_call_ok(&jd, "d1", "destroy_cache", json!({"cache": name})).await;
    jd.shutdown().await;
}

/// `destroy_cache` — Java destroys, Rust's cache-names listing no longer
/// contains the cache name.
#[tokio::test]
async fn destroy_cache_cross_client() {
    require_jar!();
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    let name = cache_name("DC");
    rs.get_or_create_cache::<String, String>(&name)
        .await
        .expect("rs get_or_create_cache");
    // Sanity.
    assert!(rs
        .get_cache_names()
        .await
        .unwrap()
        .iter()
        .any(|n| n == &name));
    jd_call_ok(&jd, "d1", "destroy_cache", json!({"cache": name})).await;
    assert!(!rs
        .get_cache_names()
        .await
        .unwrap()
        .iter()
        .any(|n| n == &name));
    jd.shutdown().await;
}

/// `get_and_remove` — Java calls it; the prior value matches Rust's put,
/// and Rust's subsequent contains_key returns false.
#[tokio::test]
async fn get_and_remove_cross_client() {
    require_jar!();
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    let name = cache_name("GAR");
    jd_call_ok(&jd, "c1", "get_or_create_cache", json!({"cache": name})).await;
    let cache: Cache<String, String> = rs
        .get_or_create_cache::<String, String>(&name)
        .await
        .expect("rs get_or_create_cache");

    cache.put(&"k".to_string(), &"taken".to_string()).await.unwrap();
    let r = jd_call_ok(
        &jd,
        "gr",
        "get_and_remove",
        json!({"cache": name, "key": "k"}),
    )
    .await;
    assert_eq!(
        r.body.get("previous").and_then(|v| v.as_str()),
        Some("taken")
    );
    assert!(!cache.contains_key(&"k".to_string()).await.unwrap());

    jd_call_ok(&jd, "d1", "destroy_cache", json!({"cache": name})).await;
    jd.shutdown().await;
}

/// `replace` (unconditional) — Rust puts, Java's replace overwrites.
/// Then Java's replace on a missing key returns false (not inserted).
#[tokio::test]
async fn replace_cross_client() {
    require_jar!();
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    let name = cache_name("RP");
    jd_call_ok(&jd, "c1", "get_or_create_cache", json!({"cache": name})).await;
    let cache: Cache<String, String> = rs
        .get_or_create_cache::<String, String>(&name)
        .await
        .expect("rs get_or_create_cache");

    cache.put(&"k".to_string(), &"a".to_string()).await.unwrap();
    // Replace existing key.
    let r = jd_call_ok(
        &jd,
        "r1",
        "replace",
        json!({"cache": name, "key": "k", "value": "b"}),
    )
    .await;
    assert_eq!(r.body.get("replaced").and_then(|v| v.as_bool()), Some(true));
    assert_eq!(
        cache.get(&"k".to_string()).await.unwrap().as_deref(),
        Some("b")
    );
    // Replace non-existent key.
    let r = jd_call_ok(
        &jd,
        "r2",
        "replace",
        json!({"cache": name, "key": "nope", "value": "c"}),
    )
    .await;
    assert_eq!(r.body.get("replaced").and_then(|v| v.as_bool()), Some(false));
    assert!(cache.get(&"nope".to_string()).await.unwrap().is_none());

    jd_call_ok(&jd, "d1", "destroy_cache", json!({"cache": name})).await;
    jd.shutdown().await;
}

/// Remove with bulk keys (`remove_keys`) — Java removes a subset; Rust
/// observes the surviving keys.
#[tokio::test]
async fn remove_keys_cross_client() {
    require_jar!();
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    let name = cache_name("RK");
    jd_call_ok(&jd, "c1", "get_or_create_cache", json!({"cache": name})).await;
    let cache: Cache<String, String> = rs
        .get_or_create_cache::<String, String>(&name)
        .await
        .expect("rs get_or_create_cache");

    for i in 0..5 {
        cache.put(&format!("k{}", i), &"v".to_string()).await.unwrap();
    }
    let keys_to_remove = vec!["k1", "k3"];
    jd_call_ok(
        &jd,
        "rk",
        "remove_keys",
        json!({"cache": name, "keys": keys_to_remove}),
    )
    .await;
    assert!(cache.contains_key(&"k0".to_string()).await.unwrap());
    assert!(!cache.contains_key(&"k1".to_string()).await.unwrap());
    assert!(cache.contains_key(&"k2".to_string()).await.unwrap());
    assert!(!cache.contains_key(&"k3".to_string()).await.unwrap());
    assert!(cache.contains_key(&"k4".to_string()).await.unwrap());

    jd_call_ok(&jd, "d1", "destroy_cache", json!({"cache": name})).await;
    jd.shutdown().await;
}
