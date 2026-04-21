//! Tier-2 parity — distributed data structures (AtomicLong, IgniteSet).
//!
//! Each case opens the structure on one client, mutates via the other,
//! and verifies the mutation is observed on the original side.

use super::common::{ignite_scope, IgniteProfile};
use super::parity::{driver_jar_built, jd_call_ok, jd_connected};
use ignite_rs::data_structures::CollectionConfiguration;
use serde_json::json;

fn unique_name(suffix: &str) -> String {
    format!("parity_ds_{}_{}", suffix, std::process::id())
}

macro_rules! require_jar {
    () => {
        if !driver_jar_built() {
            eprintln!("[parity] driver JAR missing — skipping");
            return;
        }
    };
}

/// Rust creates an AtomicLong; Java reads its value.
#[tokio::test]
async fn atomic_long_create_via_rust_read_via_java() {
    require_jar!();
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    let name = unique_name("al_read");
    let atomic = rs
        .atomic_long(&name, 42, true)
        .await
        .expect("rs atomic_long")
        .expect("Some(AtomicLong)");
    assert_eq!(atomic.get().await.expect("rs get"), 42);

    let r = jd_call_ok(&jd, "al", "atomic_long_get", json!({"name": name})).await;
    assert_eq!(r.body.get("value").and_then(|v| v.as_i64()), Some(42));

    atomic.close().await.expect("rs close");
    jd.shutdown().await;
}

/// Java increments; Rust observes the new value.
#[tokio::test]
async fn atomic_long_increment_via_java_read_via_rust() {
    require_jar!();
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    let name = unique_name("al_inc");
    let atomic = rs
        .atomic_long(&name, 10, true)
        .await
        .expect("rs atomic_long")
        .expect("Some");
    // Java increments twice.
    let r = jd_call_ok(&jd, "i1", "atomic_long_inc", json!({"name": name})).await;
    assert_eq!(r.body.get("value").and_then(|v| v.as_i64()), Some(11));
    let r = jd_call_ok(&jd, "i2", "atomic_long_inc", json!({"name": name})).await;
    assert_eq!(r.body.get("value").and_then(|v| v.as_i64()), Some(12));
    // Rust reads same value.
    assert_eq!(atomic.get().await.expect("get"), 12);

    atomic.close().await.ok();
    jd.shutdown().await;
}

/// Java compare-and-set: wrong expected → no swap; correct → swap.
#[tokio::test]
async fn atomic_long_cas_cross_client() {
    require_jar!();
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    let name = unique_name("al_cas");
    let atomic = rs
        .atomic_long(&name, 100, true)
        .await
        .expect("rs atomic_long")
        .expect("Some");

    // Wrong expected → no swap.
    let r = jd_call_ok(
        &jd,
        "c1",
        "atomic_long_cas",
        json!({"name": name, "expected": 99, "value": 0}),
    )
    .await;
    assert_eq!(r.body.get("swapped").and_then(|v| v.as_bool()), Some(false));
    assert_eq!(atomic.get().await.expect("get"), 100);
    // Correct expected → swap to 200.
    let r = jd_call_ok(
        &jd,
        "c2",
        "atomic_long_cas",
        json!({"name": name, "expected": 100, "value": 200}),
    )
    .await;
    assert_eq!(r.body.get("swapped").and_then(|v| v.as_bool()), Some(true));
    assert_eq!(atomic.get().await.expect("get"), 200);

    atomic.close().await.ok();
    jd.shutdown().await;
}

/// Rust adds to IgniteSet; Java observes membership.
#[tokio::test]
async fn set_add_via_rust_contains_via_java() {
    require_jar!();
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    let name = unique_name("set_add");
    let set = rs
        .set::<String>(
            &name,
            Some(&CollectionConfiguration::new().with_backups(1)),
        )
        .await
        .expect("rs set")
        .expect("Some");

    assert!(set.add(&"hello".to_string()).await.expect("add"));
    assert_eq!(set.size().await.expect("size"), 1);

    let r = jd_call_ok(
        &jd,
        "c1",
        "set_contains",
        json!({"name": name, "value": "hello"}),
    )
    .await;
    assert_eq!(r.body.get("contains").and_then(|v| v.as_bool()), Some(true));
    let r = jd_call_ok(
        &jd,
        "c2",
        "set_contains",
        json!({"name": name, "value": "bye"}),
    )
    .await;
    assert_eq!(r.body.get("contains").and_then(|v| v.as_bool()), Some(false));

    set.close().await.ok();
    jd.shutdown().await;
}

/// Rust closes the set; Java's subsequent operations fail ("set not found").
#[tokio::test]
async fn set_close_propagates_cross_client() {
    require_jar!();
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    let name = unique_name("set_close");
    let set = rs
        .set::<String>(
            &name,
            Some(&CollectionConfiguration::new().with_backups(1)),
        )
        .await
        .expect("rs set")
        .expect("Some");
    set.add(&"item".to_string()).await.expect("add");
    set.close().await.expect("close");

    // Java: set operations on a closed/removed set fail.
    let r = jd
        .call(super::parity::Request {
            id: "j1".into(),
            op: "set_contains",
            extra: json!({"name": name, "value": "item"}),
        })
        .await;
    assert!(
        !r.ok,
        "expected Java set_contains to fail on closed set, got {:?}",
        r
    );

    jd.shutdown().await;
}
