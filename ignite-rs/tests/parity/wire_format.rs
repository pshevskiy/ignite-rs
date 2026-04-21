//! Tier-2 parity — wire format / handshake.
//!
//! Each case boots an Ignite fixture, connects both clients, and asserts
//! an invariant of the binary thin-client wire format by observing
//! behaviour from both client implementations.

use super::common::{ignite_scope, IgniteProfile};
use super::parity::{driver_jar_built, jd_call_ok, jd_connected};
use serde_json::json;

/// Both Rust and Java drivers can complete a handshake against the same
/// live fixture. If both `connect` ops succeed, the feature-bitmask
/// negotiation produced a compatible session for each client.
#[tokio::test]
async fn handshake_feature_negotiation_succeeds_for_both_clients() {
    if !driver_jar_built() {
        eprintln!("[parity] driver JAR missing — skipping");
        return;
    }
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    // Rust side: full handshake via `new_client`.
    let rs = super::parity::rs_client(&addr).await;
    // Sanity ping: get_cache_names round-trips post-handshake.
    let _ = rs.get_cache_names().await.expect("get_cache_names");

    // Java side: connect via driver JAR.
    let jd = jd_connected(&addr).await.expect("jd connected");
    let probe = jd
        .call(super::parity::Request {
            id: "h1".into(),
            op: "handshake_features",
            extra: json!({}),
        })
        .await;
    assert!(probe.ok, "driver probe failed: {:?}", probe.error);
    assert_eq!(
        probe.body.get("alive").and_then(|v| v.as_bool()),
        Some(true)
    );
    jd.shutdown().await;
}

/// Both clients observe the same post-handshake cache-names listing.
/// Rust creates a cache, Java's `cacheNames()` call must include it.
#[tokio::test]
async fn post_handshake_cache_names_visible_to_both() {
    if !driver_jar_built() {
        eprintln!("[parity] driver JAR missing — skipping");
        return;
    }
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    let name = format!("PARITY_WF_CN_{}", std::process::id());
    rs.get_or_create_cache::<String, String>(&name)
        .await
        .expect("rs get_or_create_cache");

    let r = jd_call_ok(&jd, "n1", "cache_names", json!({})).await;
    let names: Vec<String> = r
        .body
        .get("names")
        .and_then(|v| v.as_array())
        .expect("names array")
        .iter()
        .map(|v| v.as_str().unwrap_or_default().to_string())
        .collect();
    assert!(
        names.iter().any(|n| n == &name),
        "Java listing missing Rust-created cache: {:?}",
        names
    );

    rs.destroy_cache(&name).await.expect("rs destroy");
    jd.shutdown().await;
}

/// After Java destroys a cache, Rust's cache-names listing no longer
/// contains it. Mirrors I1 (post-handshake visibility equivalence).
#[tokio::test]
async fn destroyed_cache_disappears_for_both() {
    if !driver_jar_built() {
        eprintln!("[parity] driver JAR missing — skipping");
        return;
    }
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    let name = format!("PARITY_WF_DN_{}", std::process::id());
    jd_call_ok(&jd, "c1", "get_or_create_cache", json!({"cache": name})).await;
    // Sanity: Rust sees it.
    let rs_names = rs.get_cache_names().await.expect("rs get_cache_names");
    assert!(rs_names.iter().any(|n| n == &name));

    jd_call_ok(&jd, "d1", "destroy_cache", json!({"cache": name})).await;
    let rs_names = rs.get_cache_names().await.expect("rs get_cache_names");
    assert!(
        !rs_names.iter().any(|n| n == &name),
        "Rust still sees destroyed cache: {:?}",
        rs_names
    );
    jd.shutdown().await;
}

/// Request id wrap-around: Rust sends many requests back-to-back; Java can
/// still communicate on the same fixture. Not a direct id-space check but
/// exercises framing stability under volume.
#[tokio::test]
async fn sustained_request_volume_keeps_framing_stable() {
    if !driver_jar_built() {
        eprintln!("[parity] driver JAR missing — skipping");
        return;
    }
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    let name = format!("PARITY_WF_VOL_{}", std::process::id());
    let cache = rs
        .get_or_create_cache::<String, String>(&name)
        .await
        .expect("rs get_or_create_cache");
    for i in 0..200 {
        cache
            .put(&format!("k{}", i), &format!("v{}", i))
            .await
            .expect("rs put");
    }
    // Java must be able to read the most-recent one without framing desync.
    let r = jd_call_ok(&jd, "g1", "get", json!({"cache": name, "key": "k199"})).await;
    assert_eq!(
        r.body.get("value").and_then(|v| v.as_str()),
        Some("v199"),
        "framing desync or data loss"
    );
    rs.destroy_cache(&name).await.ok();
    jd.shutdown().await;
}

/// Java writes through and Rust reads back: round-trip of a UTF-8 value
/// with high-byte characters. Exercises Ignite string-type framing
/// (TypeCode::String, i32 length, UTF-8 bytes).
#[tokio::test]
async fn utf8_string_round_trip_java_to_rust() {
    if !driver_jar_built() {
        eprintln!("[parity] driver JAR missing — skipping");
        return;
    }
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    let name = format!("PARITY_WF_UTF_{}", std::process::id());
    let cache = rs
        .get_or_create_cache::<String, String>(&name)
        .await
        .expect("rs get_or_create_cache");

    let val = "λ∑πé — naïve 日本語";
    jd_call_ok(
        &jd,
        "p1",
        "put",
        json!({"cache": name, "key": "k", "value": val}),
    )
    .await;
    let v = cache.get(&"k".to_string()).await.expect("rs get");
    assert_eq!(v.as_deref(), Some(val), "UTF-8 framing mismatch");
    rs.destroy_cache(&name).await.ok();
    jd.shutdown().await;
}

/// Rust writes a 16 KiB string value; Java reads it back unchanged.
/// Verifies large-payload framing doesn't truncate or corrupt.
#[tokio::test]
async fn large_string_value_round_trip_rust_to_java() {
    if !driver_jar_built() {
        eprintln!("[parity] driver JAR missing — skipping");
        return;
    }
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    let name = format!("PARITY_WF_BIG_{}", std::process::id());
    let cache = rs
        .get_or_create_cache::<String, String>(&name)
        .await
        .expect("rs get_or_create_cache");

    let big: String = "a".repeat(16 * 1024);
    cache
        .put(&"k".to_string(), &big)
        .await
        .expect("rs put");
    let r = jd_call_ok(&jd, "g1", "get", json!({"cache": name, "key": "k"})).await;
    let got = r.body.get("value").and_then(|v| v.as_str()).unwrap_or("");
    assert_eq!(got.len(), big.len(), "length mismatch on large payload");
    assert_eq!(got, big.as_str(), "content mismatch on large payload");
    rs.destroy_cache(&name).await.ok();
    jd.shutdown().await;
}

/// Rust puts a missing-value key; Java must observe a null. Tests TypeCode::Null
/// propagation via the absent-key path.
#[tokio::test]
async fn missing_key_returns_null_for_java() {
    if !driver_jar_built() {
        eprintln!("[parity] driver JAR missing — skipping");
        return;
    }
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let _rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    let name = format!("PARITY_WF_NULL_{}", std::process::id());
    jd_call_ok(&jd, "c1", "get_or_create_cache", json!({"cache": name})).await;
    let r = jd_call_ok(&jd, "g1", "get", json!({"cache": name, "key": "absent"})).await;
    assert!(
        r.body.get("value").map(|v| v.is_null()).unwrap_or(false),
        "expected null for absent key: {:?}",
        r.body
    );
    jd_call_ok(&jd, "d1", "destroy_cache", json!({"cache": name})).await;
    jd.shutdown().await;
}

/// Cross-client size reporting: Rust puts 3 entries, Java observes size==3.
/// Tests both size-opcode framing + commit-before-return ordering.
#[tokio::test]
async fn cache_size_consistent_across_clients() {
    if !driver_jar_built() {
        eprintln!("[parity] driver JAR missing — skipping");
        return;
    }
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    let name = format!("PARITY_WF_SZ_{}", std::process::id());
    let cache = rs
        .get_or_create_cache::<String, String>(&name)
        .await
        .expect("rs get_or_create_cache");
    for i in 0..3 {
        cache
            .put(&format!("k{}", i), &"v".to_string())
            .await
            .expect("rs put");
    }
    let r = jd_call_ok(&jd, "s1", "size", json!({"cache": name})).await;
    let sz = r.body.get("size").and_then(|v| v.as_i64()).unwrap_or(-1);
    assert_eq!(sz, 3, "size mismatch: java={} rust should have written 3", sz);
    rs.destroy_cache(&name).await.ok();
    jd.shutdown().await;
}
