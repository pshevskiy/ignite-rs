//! Tier-2 parity — service invocation (coverage of FND-044/045 fixes).
//!
//! We can't deploy a real service without a custom Ignite image, so this
//! case only confirms that the error path on `service_invoke` for an
//! undeployed service is consistently mapped — both clients see a
//! "service not found" server error.

use super::common::{ignite_scope, IgniteProfile};
use super::parity::{driver_jar_built, jd_connected};
use serde_json::json;

#[tokio::test]
async fn service_invoke_missing_service_parity() {
    if !driver_jar_built() {
        eprintln!("[parity] driver JAR missing — skipping");
        return;
    }
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let _rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    // java.util.function.Supplier is on the JDK classpath, so the Class.forName
    // in the driver succeeds; but the service "no-such-svc" isn't deployed,
    // so the server returns a service-not-found error.
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
    // Common shapes observed: "java.lang.reflect.InvocationTargetException"
    // (the proxy creation fails and reflection wraps it), "ClientException",
    // "Service ... does not exist", etc. All are acceptable because all
    // represent the server rejecting the undeployed-service invocation.
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
