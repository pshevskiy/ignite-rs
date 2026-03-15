#![cfg(not(feature = "ssl"))]

mod common;

use common::{ignite_scope, recv_event, unique_name, IgniteProfile};
use ignite_rs::events::{ClientEvent, EventSubscriptions, RequestEvent, RequestEventKind};
use ignite_rs::{new_client, ClientConfig};
use std::sync::OnceLock;

const CACHE_GET_NAMES_OP_CODE: i16 = 1050;
const CACHE_PUT_OP_CODE: i16 = 1001;

static REQUEST_EVENT_TEST_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

/// Java parity: org.apache.ignite.internal.client.thin.events.IgniteClientRequestEventListenerTest#testQuerySuccessEvents
#[tokio::test]
async fn should_emit_request_start_and_success_events() {
    let _guard = request_event_test_lock().lock().await;
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().unwrap().addr().to_string();

    let mut conf = scope.client_config().unwrap();
    conf.event_subscriptions = EventSubscriptions {
        request: true,
        ..EventSubscriptions::default()
    };

    let client = new_client(conf).await.unwrap();
    let mut events = client.events().subscribe();

    let _ = client.get_cache_names().await.unwrap();

    let started = recv_request_event(&mut events).await;
    let succeeded = recv_request_event(&mut events).await;

    assert_eq!(started.kind, RequestEventKind::Started);
    assert_eq!(started.op_code, CACHE_GET_NAMES_OP_CODE);
    assert_eq!(started.address, addr);
    assert_eq!(succeeded.kind, RequestEventKind::Succeeded);
    assert_eq!(succeeded.op_code, CACHE_GET_NAMES_OP_CODE);
    assert_eq!(succeeded.address, started.address);
    assert_eq!(succeeded.correlation_id, started.correlation_id);
}

/// Java parity: org.apache.ignite.internal.client.thin.events.IgniteClientRequestEventListenerTest#testQueryFailEvents
#[tokio::test]
async fn should_emit_request_start_and_fail_events() {
    let _guard = request_event_test_lock().lock().await;
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().unwrap().addr().to_string();

    let mut conf: ClientConfig = scope.client_config().unwrap();
    conf.retry_limit = 0;
    conf.partition_awareness_enabled = false;
    conf.event_subscriptions = EventSubscriptions {
        request: true,
        ..EventSubscriptions::default()
    };

    let client = new_client(conf).await.unwrap();
    let mut events = client.events().subscribe();
    let cache_name = unique_name("request_listener_missing_cache");

    let err = client
        .cache::<i32, i32>(&cache_name)
        .put(&1, &1)
        .await
        .unwrap_err();
    let err_msg = err.to_string();
    assert!(
        err_msg.contains("does not exist") || err_msg.contains(&cache_name),
        "unexpected request failure: {}",
        err_msg
    );

    let started = recv_request_event(&mut events).await;
    let failed = recv_request_event(&mut events).await;

    assert_eq!(started.kind, RequestEventKind::Started);
    assert_eq!(started.op_code, CACHE_PUT_OP_CODE);
    assert_eq!(failed.kind, RequestEventKind::Failed);
    assert_eq!(failed.op_code, CACHE_PUT_OP_CODE);
    assert_eq!(failed.address, addr);
    assert_eq!(failed.correlation_id, started.correlation_id);
    assert!(failed.detail.as_deref().is_some());
}

async fn recv_request_event(
    events: &mut tokio::sync::broadcast::Receiver<ClientEvent>,
) -> RequestEvent {
    loop {
        if let ClientEvent::Request(event) = recv_event(events).await {
            return event;
        }
    }
}

fn request_event_test_lock() -> &'static tokio::sync::Mutex<()> {
    REQUEST_EVENT_TEST_LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}
