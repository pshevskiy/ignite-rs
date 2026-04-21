//! Tier-2 parity — wire format / handshake.

use super::common::{ignite_scope, IgniteProfile};
use super::parity::{driver_jar_built, jd_connected};

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
    let names = rs.get_cache_names().await.expect("get_cache_names");
    drop(names);

    // Java side: connect via driver JAR.
    let jd = jd_connected(&addr).await.expect("jd connected");
    // probe op confirms the driver's client handle is alive after handshake
    let probe = jd
        .call(super::parity::Request {
            id: "h1".into(),
            op: "handshake_features",
            extra: serde_json::json!({}),
        })
        .await;
    assert!(probe.ok, "driver probe failed: {:?}", probe.error);
    assert_eq!(
        probe.body.get("alive").and_then(|v| v.as_bool()),
        Some(true)
    );
    jd.shutdown().await;
}
