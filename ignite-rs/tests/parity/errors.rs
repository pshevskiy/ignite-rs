//! Tier-2 parity — server-error mapping.
//!
//! Both Rust and Java clients must surface the same "cache does not exist"
//! error when you call `get()` on a cache that's never been created.
//! Java driver's `force_error` op captures the error class + message.

use super::common::{ignite_scope, IgniteProfile};
use super::parity::{driver_jar_built, jd_call_ok, jd_connected};
use ignite_rs::cache::Cache;
use ignite_rs::error::ErrorKind;
use serde_json::json;

#[tokio::test]
async fn missing_cache_error_mapping() {
    if !driver_jar_built() {
        eprintln!("[parity] driver JAR missing — skipping");
        return;
    }
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    let cache_name = format!("NO_SUCH_CACHE_{}", std::process::id());

    // Java side: force error — use `.cache(name).get()` which throws if
    // the cache doesn't exist.
    let r = jd_call_ok(
        &jd,
        "e1",
        "force_error",
        json!({"cache": cache_name}),
    )
    .await;
    let triggered = r
        .body
        .get("triggered")
        .and_then(|v| v.as_bool())
        .expect("triggered field");
    assert!(triggered, "Java didn't see an error: {:?}", r.body);
    let err_msg = r
        .body
        .get("error_message")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    assert!(
        err_msg.contains("Cache does not exist") || err_msg.contains("cache"),
        "Java error was not 'cache does not exist': {}",
        err_msg
    );

    // Rust side: create cache, destroy it, then call get — the server returns
    // CACHE_DOES_NOT_EXIST (ErrorKind::Server). This isolates the "server-side
    // error mapping" pathway, which is what this parity case targets.
    let cache: Cache<String, String> = rs
        .get_or_create_cache::<String, String>(&cache_name)
        .await
        .expect("rs get_or_create_cache");
    rs.destroy_cache(&cache_name).await.expect("rs destroy");
    let rs_err = cache.get(&"x".to_string()).await;
    match rs_err {
        Err(e) => {
            // Cache-does-not-exist is a server status → ErrorKind::Server.
            assert!(
                matches!(e.kind(), ErrorKind::Server),
                "unexpected Rust error kind: {:?} (msg: {})",
                e.kind(),
                e
            );
        }
        Ok(v) => panic!("Rust get() succeeded on destroyed cache: {:?}", v),
    }

    jd.shutdown().await;
}
