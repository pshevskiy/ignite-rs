#![cfg(not(feature = "ssl"))]

mod common;

use common::{ignite_scope, recv_event, unused_local_addr, IgniteProfile};
use ignite_rs::events::{ClientEvent, EventSubscriptions, LifecycleEventKind};
use ignite_rs::{new_client_with_events, ClientConfig};

/// Java parity: org.apache.ignite.internal.client.thin.events.IgniteClientLifecycleEventListenerTest#testClientLifecycleEvents
#[tokio::test]
async fn should_emit_created_closed_and_failed_lifecycle_events() {
    let scope = ignite_scope(IgniteProfile::ThreeNodeCluster);
    let mut conf = scope.client_config().unwrap();
    conf.event_subscriptions = EventSubscriptions {
        lifecycle: true,
        ..EventSubscriptions::default()
    };

    let (client, mut events) = new_client_with_events(conf).await;
    let client = client.unwrap();

    let created = recv_event(&mut events).await;
    assert_eq!(
        created,
        ClientEvent::Lifecycle(ignite_rs::events::LifecycleEvent {
            kind: LifecycleEventKind::Created,
            detail: None,
        })
    );

    drop(client);

    let closed = recv_event(&mut events).await;
    assert_eq!(
        closed,
        ClientEvent::Lifecycle(ignite_rs::events::LifecycleEvent {
            kind: LifecycleEventKind::Closed,
            detail: None,
        })
    );

    let mut failed_conf = ClientConfig::new(&unused_local_addr());
    failed_conf.retry_limit = 0;
    failed_conf.event_subscriptions = EventSubscriptions {
        lifecycle: true,
        ..EventSubscriptions::default()
    };

    let (failed_client, mut failed_events) = new_client_with_events(failed_conf).await;
    let err = match failed_client {
        Ok(_) => panic!("expected lifecycle startup failure"),
        Err(err) => err,
    };
    assert!(
        !err.to_string().is_empty(),
        "expected startup failure for lifecycle fail event"
    );

    let failed = recv_event(&mut failed_events).await;
    assert!(matches!(
        failed,
        ClientEvent::Lifecycle(ignite_rs::events::LifecycleEvent {
            kind: LifecycleEventKind::Failed,
            detail: Some(_),
        })
    ));
}
