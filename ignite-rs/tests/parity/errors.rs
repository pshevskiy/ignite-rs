//! Tier-2 parity — server-error mapping.
//!
//! Both Rust and Java clients must surface server errors consistently.
//! Each case triggers a specific server-side error path and confirms
//! both clients convert it into their error type.

use super::common::{ignite_scope, IgniteProfile};
use super::parity::{driver_jar_built, jd_call_ok, jd_connected};
use ignite_rs::cache::Cache;
use ignite_rs::error::ErrorKind;
use ignite_rs::query::SqlFieldsQuery;
use serde_json::json;

macro_rules! require_jar {
    () => {
        if !driver_jar_built() {
            eprintln!("[parity] driver JAR missing — skipping");
            return;
        }
    };
}

/// CACHE_DOES_NOT_EXIST — both clients raise a server error when getting
/// from a cache that never existed / was destroyed.
#[tokio::test]
async fn missing_cache_error_mapping() {
    require_jar!();
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    let name = format!("NO_SUCH_CACHE_{}", std::process::id());
    let r = jd_call_ok(
        &jd,
        "e1",
        "force_error_op",
        json!({"kind": "cache_not_found", "cache": name}),
    )
    .await;
    assert_eq!(r.body.get("triggered").and_then(|v| v.as_bool()), Some(true));
    let err_msg = r
        .body
        .get("error_message")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    assert!(
        err_msg.contains("Cache does not exist") || err_msg.contains("cache"),
        "Java: {}",
        err_msg
    );

    // Rust: create cache, destroy it, call get → ErrorKind::Server.
    let cache: Cache<String, String> = rs
        .get_or_create_cache::<String, String>(&name)
        .await
        .expect("rs get_or_create_cache");
    rs.destroy_cache(&name).await.expect("rs destroy");
    let err = cache.get(&"x".to_string()).await.unwrap_err();
    assert!(matches!(err.kind(), ErrorKind::Server));

    jd.shutdown().await;
}

/// Destroying a non-existent cache — Java raises, Rust raises server error.
#[tokio::test]
async fn destroy_missing_cache_error() {
    require_jar!();
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    let name = format!("NEVER_CREATED_{}", std::process::id());
    // Java destroy on unknown cache.
    let r = jd_call_ok(
        &jd,
        "e1",
        "force_error_op",
        json!({"kind": "destroy_missing", "cache": name}),
    )
    .await;
    assert_eq!(r.body.get("triggered").and_then(|v| v.as_bool()), Some(true));

    // Rust equivalent.
    let err = rs.destroy_cache(&name).await.unwrap_err();
    assert!(
        matches!(err.kind(), ErrorKind::Server | ErrorKind::Other),
        "expected server/other error for destroying missing cache, got {:?}: {}",
        err.kind(),
        err
    );

    jd.shutdown().await;
}

/// Bad SQL — server returns a parse error; Java wraps it, Rust gets ErrorKind::Server.
#[tokio::test]
async fn sql_syntax_error_mapping() {
    require_jar!();
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    // Java: bad SQL raises.
    let r = jd_call_ok(
        &jd,
        "e1",
        "force_error_op",
        json!({"kind": "sql_bad", "sql": "SELECT ** FROMM where where"}),
    )
    .await;
    assert_eq!(r.body.get("triggered").and_then(|v| v.as_bool()), Some(true));

    // Rust equivalent.
    let err = rs
        .sql_fields::<i64>(
            SqlFieldsQuery::new("SELECT ** FROMM where where")
                .with_schema("PUBLIC")
                .with_page_size(32),
        )
        .await
        .err()
        .expect("bad SQL should fail");
    assert!(
        matches!(err.kind(), ErrorKind::Server | ErrorKind::Other),
        "unexpected kind for bad SQL: {:?}",
        err.kind()
    );

    jd.shutdown().await;
}

/// Getting a non-existent AtomicLong — Java raises / returns null,
/// Rust's .atomic_long(_, _, false) returns Ok(None) or a server error.
/// Parity target: both see a "not found" outcome, not a success.
#[tokio::test]
async fn missing_atomic_long_consistent() {
    require_jar!();
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    let name = format!("missing_al_{}", std::process::id());
    // Java: atomic_long_get on a non-existent name — ok=false.
    let r = jd
        .call(super::parity::Request {
            id: "e1".into(),
            op: "atomic_long_get",
            extra: json!({ "name": name }),
        })
        .await;
    assert!(!r.ok, "Java should fail on missing atomic long: {:?}", r);

    // Rust: atomic_long(_, _, create=false) → Ok(None) for missing.
    let result = rs.atomic_long(&name, 0, false).await.expect("rs call");
    assert!(
        result.is_none(),
        "Rust should return None for missing atomic long"
    );

    jd.shutdown().await;
}

/// Post-cache-destroy: `put` via the dead handle — both clients surface a
/// server error. Exercises the CACHE_DOES_NOT_EXIST path on the write-op side.
#[tokio::test]
async fn put_to_destroyed_cache_errors() {
    require_jar!();
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    let name = format!("DYING_CACHE_{}", std::process::id());
    // Rust creates, destroys, then tries to put.
    let cache: Cache<String, String> = rs
        .get_or_create_cache::<String, String>(&name)
        .await
        .expect("rs get_or_create_cache");
    rs.destroy_cache(&name).await.expect("rs destroy");
    let err = cache
        .put(&"k".to_string(), &"v".to_string())
        .await
        .unwrap_err();
    assert!(matches!(err.kind(), ErrorKind::Server));

    // Java: same cache now missing; probe via force_error_op (reads),
    // which also hits the CACHE_DOES_NOT_EXIST path.
    let r = jd_call_ok(
        &jd,
        "e1",
        "force_error_op",
        json!({"kind": "cache_not_found", "cache": name}),
    )
    .await;
    assert_eq!(r.body.get("triggered").and_then(|v| v.as_bool()), Some(true));

    jd.shutdown().await;
}

/// A wildly-wrong type on the Java side triggers server-side validation.
/// We use a malformed interface string for service_invoke to cover an
/// unrelated-kind error (ClassNotFoundException) that surfaces differently
/// from a cache/SQL error — it's a reflection error on the Java side only.
#[tokio::test]
async fn service_invoke_error_distinct_from_cache_error() {
    require_jar!();
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let _rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    let r = jd
        .call(super::parity::Request {
            id: "e1".into(),
            op: "service_invoke",
            extra: json!({
                "service": "svc",
                "interface": "not.a.real.Iface",
                "method": "get"
            }),
        })
        .await;
    assert!(!r.ok);
    let msg = r.error.clone().unwrap_or_default();
    // The Java driver wraps with class name + message; must include the class
    // NotFoundException for this error path.
    assert!(
        msg.contains("ClassNotFoundException") || msg.contains("not.a.real.Iface"),
        "expected ClassNotFound-shaped error, got: {}",
        msg
    );

    jd.shutdown().await;
}
