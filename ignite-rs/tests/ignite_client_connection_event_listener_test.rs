#![cfg(not(feature = "ssl"))]

mod common;

use common::{ignite_scope, recv_event, unused_local_addr, IgniteProfile};
use ignite_rs::events::{ClientEvent, ConnectionEvent, ConnectionEventKind, EventSubscriptions};
use ignite_rs::{new_client_with_events, ClientConfig};
use std::sync::OnceLock;

static CONNECTION_EVENT_TEST_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

/// Java parity: org.apache.ignite.internal.client.thin.events.IgniteClientConnectionEventListenerTest#testBasic
#[tokio::test]
async fn should_replay_initial_connection_events() {
    let _guard = connection_event_test_lock().lock().await;
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().unwrap().addr().to_string();

    let mut conf = scope.client_config().unwrap();
    conf.event_subscriptions = EventSubscriptions {
        connection: true,
        ..EventSubscriptions::default()
    };

    let (client, mut events) = new_client_with_events(conf).await;
    let client = client.unwrap();

    let start = recv_event(&mut events).await;
    let connected = recv_event(&mut events).await;

    assert_eq!(
        start,
        ClientEvent::Connection(ConnectionEvent {
            kind: ConnectionEventKind::ConnectAttempt,
            address: addr.clone(),
            detail: None,
        })
    );
    assert_eq!(
        connected,
        ClientEvent::Connection(ConnectionEvent {
            kind: ConnectionEventKind::Connected,
            address: addr.clone(),
            detail: None,
        })
    );

    drop(client);

    assert_eq!(
        recv_event(&mut events).await,
        ClientEvent::Connection(ConnectionEvent {
            kind: ConnectionEventKind::Closed,
            address: addr,
            detail: None,
        })
    );
}

/// Related Apache Ignite connection-listener failure coverage:
/// org.apache.ignite.internal.client.thin.events.IgniteClientConnectionEventListenerTest
#[tokio::test]
async fn should_emit_connect_failed_event_on_startup_failure() {
    let _guard = connection_event_test_lock().lock().await;
    let dead_addr = unused_local_addr();

    let mut conf = ClientConfig::new(&dead_addr);
    conf.retry_limit = 0;
    conf.event_subscriptions = EventSubscriptions {
        connection: true,
        ..EventSubscriptions::default()
    };

    let (client, mut events) = new_client_with_events(conf).await;
    let err = match client {
        Ok(_) => panic!("expected startup connect failure"),
        Err(err) => err,
    };
    assert!(
        !err.to_string().is_empty(),
        "expected startup connect failure detail"
    );

    assert_eq!(
        recv_event(&mut events).await,
        ClientEvent::Connection(ConnectionEvent {
            kind: ConnectionEventKind::ConnectAttempt,
            address: dead_addr.clone(),
            detail: None,
        })
    );

    let failed = recv_event(&mut events).await;

    assert!(matches!(
        failed,
        ClientEvent::Connection(ConnectionEvent {
            kind: ConnectionEventKind::ConnectFailed,
            address,
            detail: Some(_),
        }) if address == dead_addr
    ));
}

fn connection_event_test_lock() -> &'static tokio::sync::Mutex<()> {
    CONNECTION_EVENT_TEST_LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}
