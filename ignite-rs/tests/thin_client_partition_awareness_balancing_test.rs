#![cfg(not(feature = "ssl"))]

mod common;

use common::{ignite_context, FixtureScope, IgniteContext, IgniteProfile};
use ignite_rs::events::{ClientEvent, EventSubscriptions, RequestEventKind};
use ignite_rs::{new_client_with_events, ClientConfig};
use std::collections::HashSet;
use std::time::Duration;

const CACHE_GET_NAMES_OP_CODE: i16 = 1050;

/// Java parity: org.apache.ignite.internal.client.thin.ThinClientPartitionAwarenessBalancingTest#testConnectionDistribution
#[tokio::test]
async fn should_distribute_non_affinity_requests_across_live_cluster_channels() {
    let ctx = balancing_context();
    ctx.wait_for_ready().await.unwrap();
    let env = ctx
        .cluster_env()
        .expect("expected three-node cluster context")
        .clone();

    let mut conf = ClientConfig::from_addresses(env.addresses().iter().cloned());
    conf.event_subscriptions = EventSubscriptions {
        request: true,
        ..EventSubscriptions::default()
    };
    conf.request_timeout = Some(Duration::from_millis(500));
    conf.retry_limit = 1;

    let (client_res, mut events) = new_client_with_events(conf).await;
    let client = client_res.unwrap();

    let mut used_addresses = HashSet::new();
    let mut successes = 0usize;

    for _ in 0..300 {
        let _ = client.get_cache_names().await.unwrap();
        drain_request_addresses(
            &mut events,
            CACHE_GET_NAMES_OP_CODE,
            &mut used_addresses,
            &mut successes,
        );
    }

    drain_request_addresses(
        &mut events,
        CACHE_GET_NAMES_OP_CODE,
        &mut used_addresses,
        &mut successes,
    );

    // With containerized clusters, partition awareness discovers internal
    // container IPs in addition to the host-mapped addresses, so we may
    // see up to 6 unique channels (3 internal + 3 external).
    assert!(
        used_addresses.len() >= 3,
        "expected non-affinity requests to be distributed across at least three live cluster channels, saw {:?}",
        used_addresses
    );
    assert_eq!(
        successes, 300,
        "expected one successful request event per cache-names call, saw {}",
        successes
    );
}

fn drain_request_addresses(
    events: &mut tokio::sync::broadcast::Receiver<ClientEvent>,
    op_code: i16,
    addresses: &mut HashSet<String>,
    successes: &mut usize,
) {
    while let Ok(event) = events.try_recv() {
        if let ClientEvent::Request(event) = event {
            if event.kind == RequestEventKind::Succeeded && event.op_code == op_code {
                addresses.insert(event.address);
                *successes += 1;
            }
        }
    }
}

fn balancing_context() -> std::sync::Arc<IgniteContext> {
    ignite_context(IgniteProfile::ThreeNodeCluster, FixtureScope::CargoSession)
}
