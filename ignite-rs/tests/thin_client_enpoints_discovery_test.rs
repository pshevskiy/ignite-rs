#![cfg(not(feature = "ssl"))]

mod common;

use common::{ignite_context, IgniteClusterEnv, IgniteContext, FixtureScope, IgniteProfile};
use ignite_rs::events::{ClientEvent, ConnectionEventKind, EventSubscriptions, RequestEventKind};
use ignite_rs::{new_client, new_client_with_events, AddressResolver, Client, ClientConfig};
use std::collections::HashSet;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

const CACHE_GET_NAMES_OP_CODE: i16 = 1050;
const DISCOVERY_CACHE_NAME: &str = "endpoint_discovery_cache";

/// Java parity: org.apache.ignite.internal.client.thin.ThinClientEnpointsDiscoveryTest#testEndpointsDiscovery
#[tokio::test]
async fn should_fail_over_to_discovered_node_after_seed_node_stops() {
    let _guard = discovery_churn_lock().lock().await;
    debug_phase("resetting discovery churn context");
    let ctx = discovery_churn_context();
    ctx.reset_profile_state().await.unwrap();
    let env = ctx
        .cluster_env()
        .expect("expected cluster churn discovery context")
        .clone();
    let addrs = env.addresses().to_vec();
    let new_node_addr = env.node_addr(3);
    debug_phase("ensuring discovery cache exists");
    ctx.ensure_cache(DISCOVERY_CACHE_NAME).await.unwrap();

    debug_phase("creating discovery client");
    let (client_res, mut events) =
        new_client_with_events(discovery_client_config(
            [addrs[0].as_str()],
            discovery_resolved_addresses(&env),
        ))
        .await;
    let client = client_res.unwrap();

    debug_phase("waiting for initial 3 channels");
    wait_for_connected_count(&client, &mut events, 3).await;

    debug_phase(&format!("stopping seed node {}", addrs[0]));
    env.stop_node(0);
    debug_phase("waiting for cache-name request to succeed via discovered live nodes");
    wait_for_cache_names_success(&client).await;

    let cache_names = client.get_cache_names().await.unwrap();
    assert!(
        cache_names.iter().any(|name| name == DISCOVERY_CACHE_NAME),
        "expected discovered live nodes to keep cache-name request working after seed node shutdown; got {:?}",
        cache_names
    );

    debug_phase(&format!(
        "starting seed node {} and new node {}",
        addrs[0], new_node_addr
    ));
    env.start_node(0);
    env.start_node(3);
    debug_phase("waiting for restarted seed and new node readiness");
    wait_for_node_ready(&addrs[0]).await;
    wait_for_node_ready(&new_node_addr).await;
    debug_phase("waiting for seed reconnect");
    wait_for_connection_event(&client, &mut events, &addrs[0], ConnectionEventKind::Connected)
        .await;
    debug_phase("waiting for new node connect");
    wait_for_connection_event(&client, &mut events, &new_node_addr, ConnectionEventKind::Connected)
        .await;
}

/// Java parity: org.apache.ignite.internal.client.thin.ThinClientEnpointsDiscoveryTest#testEndpointsDiscoveryDisabled
#[tokio::test]
async fn should_ignore_discovered_nodes_when_partition_awareness_is_disabled() {
    let _guard = discovery_churn_lock().lock().await;
    let ctx = discovery_churn_context();
    ctx.reset_profile_state().await.unwrap();
    let env = ctx
        .cluster_env()
        .expect("expected cluster churn discovery context")
        .clone();
    let addrs = env.addresses().to_vec();

    let mut conf = discovery_client_config([addrs[0].as_str()], discovery_resolved_addresses(&env));
    conf.partition_awareness_enabled = false;
    conf.event_subscriptions = EventSubscriptions {
        connection: true,
        request: true,
        lifecycle: false,
    };

    let (client_res, mut events) = new_client_with_events(conf).await;
    let client = client_res.unwrap();

    let mut seen_request_addresses = HashSet::new();
    let mut connected_addresses = HashSet::new();

    for _ in 0..16 {
        let _ = client.get_cache_names().await.unwrap();
    }

    collect_request_and_connection_addresses(&mut events, &mut seen_request_addresses, &mut connected_addresses)
        .await;

    assert_eq!(
        seen_request_addresses.len(),
        1,
        "expected discovery-disabled client to use only one connection address, saw {:?}",
        seen_request_addresses
    );
    assert!(
        connected_addresses.len() <= 1,
        "expected discovery-disabled client to avoid eager connections to discovered nodes, saw {:?}",
        connected_addresses
    );
}

/// Java parity: org.apache.ignite.internal.client.thin.ThinClientEnpointsDiscoveryTest#testDiscoveryAfterAllNodesFailed
#[tokio::test]
async fn should_rediscover_seed_address_after_all_known_nodes_fail() {
    let _guard = discovery_churn_lock().lock().await;
    let ctx = discovery_churn_context();
    ctx.reset_profile_state().await.unwrap();
    let env = ctx
        .cluster_env()
        .expect("expected cluster churn discovery context")
        .clone();
    env.stop_node(2);
    env.wait_for_ready().await.unwrap();
    ctx.ensure_cache(DISCOVERY_CACHE_NAME).await.unwrap();

    let addrs = env.addresses().to_vec();
    let (client_res, mut events) = new_client_with_events(discovery_client_config(
        [addrs[0].as_str()],
        discovery_resolved_addresses(&env),
    ))
    .await;
    let client = client_res.unwrap();

    wait_for_connected_count(&client, &mut events, 2).await;

    env.stop_node(0);
    wait_for_cache_names_success(&client).await;

    let via_discovered = client.get_cache_names().await.unwrap();
    assert!(
        via_discovered
            .iter()
            .any(|name| name == DISCOVERY_CACHE_NAME),
        "expected client to continue via discovered live node after configured seed stopped; got {:?}",
        via_discovered
    );

    env.stop_node(1);
    tokio::time::sleep(Duration::from_millis(250)).await;

    let err = client.get_cache_names().await.unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("Failed to connect")
            || msg.contains("refused")
            || msg.contains("closed")
            || msg.contains("reset"),
        "unexpected all-nodes-down error: {}",
        msg
    );

    env.start_node(0);
    wait_for_node_ready(&addrs[0]).await;
    wait_for_connection_event(&client, &mut events, &addrs[0], ConnectionEventKind::Connected)
        .await;
    wait_for_cache_names_success(&client).await;

    env.start_node(1);
    env.start_node(2);
}

/// Java parity: org.apache.ignite.internal.client.thin.ThinClientEnpointsDiscoveryTest#testUnreachableAddressDiscoveredDoesNotPreventClientInit
#[tokio::test]
async fn should_ignore_unreachable_extra_address_during_client_init() {
    let _guard = discovery_churn_lock().lock().await;
    let ctx = discovery_churn_context();
    ctx.reset_profile_state().await.unwrap();
    let env = ctx
        .cluster_env()
        .expect("expected cluster churn discovery context")
        .clone();
    ctx.ensure_cache(DISCOVERY_CACHE_NAME).await.unwrap();

    let dead_addr = common::unused_local_addr();
    let client = new_client(ClientConfig::from_addresses([
        env.addr(),
        dead_addr.as_str(),
    ]))
    .await
    .unwrap();
    let cache_names = client.get_cache_names().await.unwrap();

    assert!(
        cache_names.iter().any(|name| name == DISCOVERY_CACHE_NAME),
        "expected reachable live seed node to initialize client despite unreachable extra address; got {:?}",
        cache_names
    );
}

async fn wait_for_connected_count(
    client: &Client,
    events: &mut tokio::sync::broadcast::Receiver<ClientEvent>,
    expected_count: usize,
) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    let mut connected = HashSet::new();
    let mut failed = HashSet::new();

    loop {
        while let Some(event) = try_recv_event(events).await {
            if let ClientEvent::Connection(event) = event {
                match event.kind {
                    ConnectionEventKind::Connected => {
                        connected.insert(event.address);
                    }
                    ConnectionEventKind::ConnectFailed => {
                        failed.insert(format!(
                            "{} ({})",
                            event.address,
                            event.detail.unwrap_or_else(|| "no detail".to_string())
                        ));
                    }
                    _ => {}
                }
            }
        }

        if connected.len() >= expected_count {
            return;
        }

        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {} connected live channels; connected {:?}; failed {:?}",
            expected_count,
            connected
            ,
            failed
        );

        let _ = trigger_topology_change(client).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_connection_event(
    client: &Client,
    events: &mut tokio::sync::broadcast::Receiver<ClientEvent>,
    address: &str,
    kind: ConnectionEventKind,
) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    let mut seen = Vec::new();

    loop {
        while let Some(event) = try_recv_event(events).await {
            if let ClientEvent::Connection(event) = event {
                seen.push(format!(
                    "{:?} {}{}",
                    event.kind,
                    event.address,
                    event
                        .detail
                        .as_deref()
                        .map(|detail| format!(" ({detail})"))
                        .unwrap_or_default()
                ));
                if event.kind == kind && event.address == address {
                    return;
                }
            }
        }

        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {:?} on {}; seen connection events {:?}",
            kind, address, seen
        );

        let _ = trigger_topology_change(client).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_cache_names_success(client: &Client) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);

    loop {
        if client.get_cache_names().await.is_ok() {
            return;
        }

        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for cache-names request to recover"
        );

        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_node_ready(addr: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);

    loop {
        let mut conf = ClientConfig::from_addresses([addr]);
        conf.partition_awareness_enabled = false;
        conf.handshake_timeout = Some(Duration::from_millis(500));
        conf.request_timeout = Some(Duration::from_millis(500));

        if let Ok(client) = new_client(conf).await {
            if client.get_cache_names().await.is_ok() {
                return;
            }
        }

        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for live node readiness at {}",
            addr
        );

        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn trigger_topology_change(client: &Client) {
    let _ = client
        .get_or_create_cache::<i32, i32>(DISCOVERY_CACHE_NAME)
        .await;
}

async fn collect_request_and_connection_addresses(
    events: &mut tokio::sync::broadcast::Receiver<ClientEvent>,
    request_addresses: &mut HashSet<String>,
    connected_addresses: &mut HashSet<String>,
) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);

    loop {
        match tokio::time::timeout(Duration::from_millis(100), events.recv()).await {
            Ok(Ok(ClientEvent::Request(event)))
                if event.kind == RequestEventKind::Succeeded
                    && event.op_code == CACHE_GET_NAMES_OP_CODE =>
            {
                request_addresses.insert(event.address);
            }
            Ok(Ok(ClientEvent::Connection(event))) if event.kind == ConnectionEventKind::Connected => {
                connected_addresses.insert(event.address);
            }
            Ok(Ok(_)) | Ok(Err(_)) => {}
            Err(_) if tokio::time::Instant::now() >= deadline => return,
            Err(_) => {}
        }
    }
}

async fn try_recv_event(
    events: &mut tokio::sync::broadcast::Receiver<ClientEvent>,
) -> Option<ClientEvent> {
    match tokio::time::timeout(Duration::from_millis(50), events.recv()).await {
        Ok(Ok(event)) => Some(event),
        Ok(Err(_)) | Err(_) => None,
    }
}

#[derive(Clone)]
struct StaticAddressResolver {
    addresses: Vec<String>,
}

impl AddressResolver for StaticAddressResolver {
    fn addresses(&self) -> Vec<String> {
        self.addresses.clone()
    }
}

fn discovery_resolved_addresses(env: &IgniteClusterEnv) -> Vec<String> {
    (0..4).map(|index| env.node_addr(index)).collect()
}

fn discovery_client_config<'a>(
    addresses: impl IntoIterator<Item = &'a str>,
    resolved_addresses: Vec<String>,
) -> ClientConfig {
    let mut conf = ClientConfig::from_addresses(addresses);
    conf.address_resolver = Some(Arc::new(StaticAddressResolver {
        addresses: resolved_addresses,
    }));
    conf.handshake_timeout = Some(Duration::from_millis(500));
    conf.request_timeout = Some(Duration::from_millis(500));
    conf.reconnect_backoff = Some(Duration::from_millis(50));
    conf.retry_limit = 8;
    conf.event_subscriptions = EventSubscriptions {
        connection: true,
        request: true,
        lifecycle: false,
    };
    conf
}

fn discovery_churn_context() -> std::sync::Arc<IgniteContext> {
    ignite_context(IgniteProfile::ThreeNodeClusterChurn, FixtureScope::CargoSession)
}

fn discovery_churn_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

fn debug_phase(message: &str) {
    if std::env::var_os("IGNITE_TEST_DEBUG").is_some() {
        eprintln!("discovery test: {message}");
    }
}
