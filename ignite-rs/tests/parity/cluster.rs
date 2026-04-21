//! Tier-2 parity — cluster/topology observations.
//!
//! Both clients observe the same topology against a single-node fixture.

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

/// Single-node fixture: both clients report exactly 1 server node.
#[tokio::test]
async fn single_node_topology_agrees() {
    require_jar!();
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    let r = jd_call_ok(&jd, "n1", "cluster_nodes_count", json!({})).await;
    let java_count = r.body.get("count").and_then(|v| v.as_i64()).unwrap_or(-1);
    assert_eq!(java_count, 1, "Java should see 1 node");

    let rs_nodes = rs
        .cluster()
        .for_servers()
        .nodes()
        .await
        .expect("rs cluster nodes");
    assert_eq!(rs_nodes.len(), 1, "Rust should see 1 server node");

    jd.shutdown().await;
}

/// After both clients connect, cluster-state query is consistent.
/// Specifically the node set is stable across repeated queries.
#[tokio::test]
async fn cluster_state_stable_across_queries() {
    require_jar!();
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    let rs_first = rs
        .cluster()
        .for_servers()
        .nodes()
        .await
        .expect("rs first")
        .len();
    let r1 = jd_call_ok(&jd, "n1", "cluster_nodes_count", json!({})).await;
    let rs_second = rs
        .cluster()
        .for_servers()
        .nodes()
        .await
        .expect("rs second")
        .len();
    let r2 = jd_call_ok(&jd, "n2", "cluster_nodes_count", json!({})).await;

    assert_eq!(rs_first, rs_second, "Rust node count should be stable");
    assert_eq!(
        r1.body.get("count").and_then(|v| v.as_i64()),
        r2.body.get("count").and_then(|v| v.as_i64()),
        "Java node count should be stable"
    );
    assert_eq!(
        rs_first as i64,
        r1.body.get("count").and_then(|v| v.as_i64()).unwrap_or(-1),
        "Rust and Java should agree on node count"
    );

    jd.shutdown().await;
}

/// cache_partition_count probe — can Java see a cache created by Rust?
/// Exercises the partition map bootstrap path on both clients.
#[tokio::test]
async fn partition_map_bootstrap_cross_client() {
    require_jar!();
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    let name = format!("PARITY_CL_PM_{}", std::process::id());
    let cache = rs
        .get_or_create_cache::<String, String>(&name)
        .await
        .expect("rs cache");
    // Seed one key so partition map gets built.
    cache
        .put(&"bootstrap".to_string(), &"v".to_string())
        .await
        .expect("rs put");

    let r = jd_call_ok(
        &jd,
        "pm1",
        "cache_partition_count",
        json!({"cache": name}),
    )
    .await;
    assert_eq!(
        r.body.get("exists").and_then(|v| v.as_bool()),
        Some(true),
        "Java should have seen the cache: {:?}",
        r.body
    );

    rs.destroy_cache(&name).await.ok();
    jd.shutdown().await;
}

/// Cluster state: repeated Rust calls and Java calls interleaved must
/// both succeed (no lock/race between topology reads).
#[tokio::test]
async fn interleaved_topology_reads_succeed() {
    require_jar!();
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    for i in 0..4 {
        let _ = rs
            .cluster()
            .for_servers()
            .nodes()
            .await
            .expect("rs loop nodes");
        let r = jd_call_ok(
            &jd,
            &format!("jd_{}", i),
            "cluster_nodes_count",
            json!({}),
        )
        .await;
        assert_eq!(r.body.get("count").and_then(|v| v.as_i64()), Some(1));
    }

    jd.shutdown().await;
}
