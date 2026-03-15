#![cfg(not(feature = "ssl"))]

mod common;

use common::{
    recv_event, spawn_mock_thin_server_on_addr, unused_local_addr, MockDiscoveryNode,
    MockDiscoveryResponse, MockThinServerConfig, MockTopologyVersion, MockUuid,
};
use ignite_rs::events::{ClientEvent, ConnectionEventKind, EventSubscriptions};
use ignite_rs::{new_client, ClientConfig};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Migrated from Apache Ignite `RecoveryModeTest.testNodeInRecovery`:
/// Java source: org.apache.ignite.internal.client.thin.RecoveryModeTest
#[tokio::test]
async fn should_reject_normal_client_handshake_when_node_is_in_recovery_mode() {
    let addr = unused_local_addr();
    let server = spawn_mock_thin_server_on_addr(
        &addr,
        MockThinServerConfig {
            recovery_mode: true,
            ..MockThinServerConfig::default()
        },
    );

    let err = match new_client(ClientConfig::new(&addr)).await {
        Ok(_) => panic!("expected recovery-mode handshake to fail without management attribute"),
        Err(err) => err,
    };
    assert!(
        err.to_string().contains("Node in recovery mode"),
        "unexpected recovery-mode handshake error: {}",
        err
    );

    drop(server);
}

/// Migrated from Apache Ignite `RecoveryModeTest.testNodeInRecovery`:
/// Java source: org.apache.ignite.internal.client.thin.RecoveryModeTest
#[tokio::test]
async fn should_allow_management_client_to_connect_to_recovery_node() {
    let addr = unused_local_addr();
    let server = spawn_mock_thin_server_on_addr(
        &addr,
        MockThinServerConfig {
            recovery_mode: true,
            ..MockThinServerConfig::default()
        },
    );

    let mut conf = ClientConfig::new(&addr);
    conf.user_attributes.insert(
        "ignite.internal.management-client".to_string(),
        "true".to_string(),
    );

    let client = new_client(conf).await.unwrap();
    let err = client.get_cache_names().await.unwrap_err();
    assert!(
        err.to_string().contains("Node in recovery mode"),
        "unexpected management-client recovery-mode error: {}",
        err
    );

    drop(client);
    drop(server);
}

/// Migrated from Apache Ignite `RecoveryModeTest.testFirstNodeInRecovery`:
/// Java source: org.apache.ignite.internal.client.thin.RecoveryModeTest
#[tokio::test]
async fn should_fall_back_to_next_address_when_first_node_is_in_recovery_mode() {
    let recovery_addr = unused_local_addr();
    let healthy_addr = unused_local_addr();
    let recovery = spawn_mock_thin_server_on_addr(
        &recovery_addr,
        MockThinServerConfig {
            recovery_mode: true,
            ..MockThinServerConfig::default()
        },
    );
    let healthy = spawn_mock_thin_server_on_addr(&healthy_addr, MockThinServerConfig::default());

    let mut conf = ClientConfig::from_addresses([recovery_addr.as_str(), healthy_addr.as_str()]);
    conf.event_subscriptions = EventSubscriptions {
        connection: true,
        ..EventSubscriptions::default()
    };

    let client = new_client(conf).await.unwrap();
    let _ = client.get_cache_names().await.unwrap();

    let mut events = client.events().subscribe();
    let mut saw_recovery_failure = false;
    let mut saw_healthy_connect = false;

    for _ in 0..8 {
        if let ClientEvent::Connection(event) = recv_event(&mut events).await {
            if event.kind == ConnectionEventKind::ConnectFailed && event.address == recovery_addr {
                saw_recovery_failure = true;
            }
            if event.kind == ConnectionEventKind::Connected && event.address == healthy_addr {
                saw_healthy_connect = true;
                break;
            }
        }
    }

    assert!(
        saw_recovery_failure,
        "expected a failed connect event for recovery-mode node {}",
        recovery_addr
    );
    assert!(
        saw_healthy_connect,
        "expected a successful connect event for healthy node {}",
        healthy_addr
    );

    drop(client);
    drop(healthy);
    drop(recovery);
}

/// Migrated from Apache Ignite `RecoveryModeTest.testConnectToNodeInRecovery`:
/// Java source: org.apache.ignite.internal.client.thin.RecoveryModeTest
#[tokio::test]
async fn should_allow_multiple_management_clients_to_connect_to_recovery_node() {
    let addr = unused_local_addr();
    let server = spawn_mock_thin_server_on_addr(
        &addr,
        MockThinServerConfig {
            recovery_mode: true,
            ..MockThinServerConfig::default()
        },
    );

    let mut conf1 = ClientConfig::new(&addr);
    conf1.user_attributes.insert(
        "ignite.internal.management-client".to_string(),
        "true".to_string(),
    );

    let mut conf2 = ClientConfig::new(&addr);
    conf2.user_attributes.insert(
        "ignite.internal.management-client".to_string(),
        "true".to_string(),
    );

    let client1 = new_client(conf1).await.unwrap();
    let client2 = new_client(conf2).await.unwrap();

    assert_eq!(
        server.handshake_count(),
        2,
        "expected both management clients to connect independently"
    );

    let err1 = client1.get_cache_names().await.unwrap_err();
    let err2 = client2.get_cache_names().await.unwrap_err();
    assert!(
        err1.to_string().contains("Node in recovery mode"),
        "unexpected first management-client recovery-mode error: {}",
        err1
    );
    assert!(
        err2.to_string().contains("Node in recovery mode"),
        "unexpected second management-client recovery-mode error: {}",
        err2
    );

    drop(client2);
    drop(client1);
    drop(server);
}

/// Migrated from Apache Ignite `RecoveryModeTest.testFirstNodeInRecovery`:
/// Java source: org.apache.ignite.internal.client.thin.RecoveryModeTest
#[tokio::test]
async fn should_connect_to_recovered_node_after_it_is_discovered_healthy() {
    let recovery_addr = unused_local_addr();
    let healthy_addr = unused_local_addr();
    let recovery_node = MockUuid::new(91, 91);
    let healthy_node = MockUuid::new(92, 92);
    let recovery_port = recovery_addr
        .rsplit_once(':')
        .expect("expected host:port mock address")
        .1
        .parse::<i32>()
        .expect("expected numeric mock port");

    let discovery_responses = Arc::new(Mutex::new(VecDeque::from([
        MockDiscoveryResponse {
            topology_version: 1,
            added_nodes: Vec::new(),
            removed_node_ids: Vec::new(),
        },
        MockDiscoveryResponse {
            topology_version: 2,
            added_nodes: vec![MockDiscoveryNode {
                node_id: recovery_node,
                port: recovery_port,
                addresses: vec!["127.0.0.1".to_string()],
            }],
            removed_node_ids: Vec::new(),
        },
    ])));
    let topology_changes = Arc::new(Mutex::new(VecDeque::from([MockTopologyVersion {
        major: 2,
        minor: 0,
    }])));

    let recovery = spawn_mock_thin_server_on_addr(
        &recovery_addr,
        MockThinServerConfig {
            node_id: recovery_node,
            recovery_mode: true,
            ..MockThinServerConfig::default()
        },
    );
    let healthy = spawn_mock_thin_server_on_addr(
        &healthy_addr,
        MockThinServerConfig {
            node_id: healthy_node,
            discovery_responses: Some(discovery_responses),
            topology_changes_on_cache_names: Some(topology_changes),
            ..MockThinServerConfig::default()
        },
    );

    let client = new_client(ClientConfig::from_addresses([
        recovery_addr.as_str(),
        healthy_addr.as_str(),
    ]))
    .await
    .unwrap();

    drop(recovery);

    let recovered = spawn_mock_thin_server_on_addr(
        &recovery_addr,
        MockThinServerConfig {
            node_id: recovery_node,
            ..MockThinServerConfig::default()
        },
    );

    let _ = client.get_cache_names().await.unwrap();

    wait_for(
        || recovered.handshake_count() == 1,
        "expected recovered node to be connected after discovery refresh",
    )
    .await;

    drop(client);
    drop(recovered);
    drop(healthy);
}

async fn wait_for(condition: impl Fn() -> bool, failure_message: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);

    while !condition() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "{}",
            failure_message
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}
