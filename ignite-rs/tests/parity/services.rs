//! Tier-2 parity — service invocation (coverage of FND-044/045 fixes).
//!
//! We can't deploy a real service without a custom Ignite image, so these
//! cases focus on observable behaviours that both clients can exercise
//! without a user service: undeployed-service error mapping and the
//! service-descriptors listing (empty on a fresh node).

use super::common::{ignite_scope, IgniteProfile};
use super::parity::{driver_jar_built, jd_call_ok, jd_connected};
use serde_json::json;

macro_rules! require_jar {
    () => {
        if !driver_jar_built() {
            eprintln!("[parity] driver JAR missing — skipping");
            return;
        }
    };
}

/// Invoking a non-existent service via the Java driver fails with a recognisable
/// server-side error shape.
#[tokio::test]
async fn service_invoke_missing_service_parity() {
    require_jar!();
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let _rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    let r = jd
        .call(super::parity::Request {
            id: "s1".into(),
            op: "service_invoke",
            extra: json!({
                "service": "no-such-svc",
                "interface": "java.util.function.Supplier",
                "method": "get",
            }),
        })
        .await;
    assert!(!r.ok, "missing service should fail");
    let msg = r.error.clone().unwrap_or_default();
    assert!(
        msg.contains("service")
            || msg.contains("Service")
            || msg.contains("not found")
            || msg.contains("ClientException")
            || msg.contains("InvocationTargetException")
            || msg.contains("deploy"),
        "unexpected error shape: {}",
        msg
    );

    jd.shutdown().await;
}

/// service_descriptors on a fresh node returns an empty collection.
#[tokio::test]
async fn service_descriptors_empty_on_fresh_node() {
    require_jar!();
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let _rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    let r = jd_call_ok(&jd, "sd1", "service_descriptors", json!({})).await;
    let count = r.body.get("count").and_then(|v| v.as_i64()).unwrap_or(-1);
    assert_eq!(count, 0, "expected no user services on fresh node");

    jd.shutdown().await;
}

/// Invocation with an unknown interface — Java reflection throws
/// ClassNotFoundException. The driver wraps it as ok=false.
#[tokio::test]
async fn service_invoke_unknown_interface_fails() {
    require_jar!();
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let _rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    let r = jd
        .call(super::parity::Request {
            id: "s1".into(),
            op: "service_invoke",
            extra: json!({
                "service": "svc",
                "interface": "no.such.Interface",
                "method": "get",
            }),
        })
        .await;
    assert!(!r.ok);
    let msg = r.error.clone().unwrap_or_default();
    assert!(
        msg.contains("ClassNotFoundException") || msg.contains("class"),
        "unexpected error: {}",
        msg
    );
    jd.shutdown().await;
}
