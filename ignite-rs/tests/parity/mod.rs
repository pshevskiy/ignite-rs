//! Tier-2 cross-client parity suite.
//!
//! Each test:
//!   1. Uses ignite-rs to perform an operation against a live Ignite fixture.
//!   2. Uses the Java parity driver (subprocess) to perform the dual/mirror op
//!      against the same fixture.
//!   3. Asserts logical agreement of the observed state.
//!
//! If the Java driver JAR isn't built, tests print a hint and pass
//! without failing — this keeps the bucket usable when Maven is absent.

use ignite_rs::{new_client, ClientConfig};
use serde_json::json;
use std::time::Duration;

pub use super::driver_client::{driver_jar_built, JavaDriver, Request, Response};

/// Connect ignite-rs to the addr the fixture published. Uses handshake + request
/// timeouts so a misbehaving fixture can't hang the test process.
pub async fn rs_client(addr: &str) -> ignite_rs::Client {
    let mut cfg = ClientConfig::new(addr);
    cfg.partition_awareness_enabled = false;
    cfg.handshake_timeout = Some(Duration::from_secs(10));
    cfg.request_timeout = Some(Duration::from_secs(30));
    new_client(cfg).await.expect("ignite-rs connect")
}

/// Spawn Java driver and perform its `connect` op against `addr`. Returns
/// `None` if the JAR isn't built — callers should treat that as "skip with
/// warning" so that the bucket still compiles + runs other tests.
pub async fn jd_connected(addr: &str) -> Option<JavaDriver> {
    if !driver_jar_built() {
        eprintln!(
            "[parity] Java driver JAR not built — skipping driver-backed assertions. \
             Build with `cd tests/java-parity-driver && mvn -q -e package`."
        );
        return None;
    }
    let jd = JavaDriver::start().await;
    let resp = jd
        .call(Request {
            id: "c0".into(),
            op: "connect",
            extra: json!({ "addr": addr }),
        })
        .await;
    assert!(resp.ok, "java driver connect failed: {:?}", resp.error);
    Some(jd)
}

/// Convenience: call a driver op and assert ok==true.
pub async fn jd_call_ok(jd: &JavaDriver, id: &str, op: &str, extra: serde_json::Value) -> Response {
    let resp = jd
        .call(Request {
            id: id.into(),
            op,
            extra,
        })
        .await;
    assert!(resp.ok, "driver op {} failed: {:?}", op, resp.error);
    resp
}
