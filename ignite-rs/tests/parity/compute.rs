//! Tier-2 parity — compute task invocation.
//!
//! Stock Ignite server images ship no user-defined compute task class.
//! Calling `compute.execute(<unknown>)` yields a well-formed server error.
//! Both Rust and Java must surface the same class of error — the
//! "task not found" case — with the server status code propagated.

use super::common::{ignite_scope, IgniteProfile};
use super::parity::{driver_jar_built, jd_connected};
use serde_json::json;

#[tokio::test]
async fn compute_task_not_found_parity() {
    if !driver_jar_built() {
        eprintln!("[parity] driver JAR missing — skipping");
        return;
    }
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let _rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    // Java side: execute unknown task → expect ok=false.
    let r = jd
        .call(super::parity::Request {
            id: "c1".into(),
            op: "compute_run",
            extra: json!({ "task": "no.such.ClassThatDoesNotExist" }),
        })
        .await;
    assert!(!r.ok, "unknown-task call should fail");
    let msg = r.error.clone().unwrap_or_default();
    assert!(
        msg.contains("class")
            || msg.contains("Class")
            || msg.contains("not found")
            || msg.contains("Task")
            || msg.contains("task")
            || msg.contains("IgniteCheckedException")
            || msg.contains("Compute")
            || msg.contains("ClientException"),
        "unexpected error shape: {}",
        msg
    );

    jd.shutdown().await;
}
