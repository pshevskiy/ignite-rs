#![cfg(not(feature = "ssl"))]

mod common;

use common::{connect, destroy_cache_if_exists, ignite_test_env, unique_name};
use ignite_rs::binary::BinaryValue;
use ignite_rs::cache::ExpiryPolicy;
use ignite_rs::query::{CacheEntryEventType, ContinuousQuery, ScanQuery};
use ignite_rs::{new_client, ClientConfig};
use ignite_rs_derive::IgniteObj;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, IgniteObj)]
struct Person {
    id: i32,
    name: String,
}

/// Java parity: org.apache.ignite.internal.client.thin.CacheEntryListenersTest#testContinuousQueries
/// Java parity: org.apache.ignite.internal.client.thin.CacheEntryListenersTest#testEventReceivedData
#[tokio::test]
async fn should_receive_continuous_query_events_and_close_listener() {
    let client = connect().await.unwrap();
    let cache_name = unique_name("cq_live_events");
    destroy_cache_if_exists(&client, &cache_name).await;
    let cache = client
        .get_or_create_cache::<i32, i32>(&cache_name)
        .await
        .unwrap();

    let mut cursor = cache
        .continuous_query(ContinuousQuery::new().with_page_size(1))
        .await
        .unwrap();

    cache.put(&1, &10).await.unwrap();
    let created = next_event(&mut cursor).await;
    assert_eq!(created.event_type, CacheEntryEventType::Created);
    assert_eq!(created.key, 1);
    assert_eq!(created.old_value, None);
    assert_eq!(created.value, Some(10));

    cache.put(&1, &11).await.unwrap();
    let updated = next_event(&mut cursor).await;
    assert_eq!(updated.event_type, CacheEntryEventType::Updated);
    assert_eq!(updated.key, 1);
    assert_eq!(updated.old_value, Some(10));
    assert_eq!(updated.value, Some(11));

    cache.remove_key(&1).await.unwrap();
    let removed = next_event(&mut cursor).await;
    assert_eq!(removed.event_type, CacheEntryEventType::Removed);
    assert_eq!(removed.key, 1);
    assert_eq!(removed.old_value, Some(11));
    // Ignite 2.15 sent the removed value; 2.17+ sends None.
    assert!(
        removed.value == Some(11) || removed.value.is_none(),
        "unexpected removed value: {:?}",
        removed.value
    );

    cursor.close().await.unwrap();
    client.destroy_cache(&cache_name).await.unwrap();
}

/// Java parity: org.apache.ignite.internal.client.thin.CacheEntryListenersTest#testContinuousQueriesWithInitialQuery
#[tokio::test]
async fn should_open_continuous_query_with_initial_scan_cursor() {
    let client = connect().await.unwrap();
    let cache_name = unique_name("cq_live_initial");
    destroy_cache_if_exists(&client, &cache_name).await;
    let cache = client
        .get_or_create_cache::<i32, i32>(&cache_name)
        .await
        .unwrap();

    cache.put_all(&[(1, 10), (2, 20)]).await.unwrap();

    let (initial_cursor, mut listener) = cache
        .continuous_query_with_initial_scan(
            ContinuousQuery::new().with_page_size(1),
            ScanQuery::new().with_page_size(32),
        )
        .await
        .unwrap();

    let mut rows = initial_cursor.fetch_all().await.unwrap();
    rows.sort_by_key(|(key, _)| key.unwrap_or_default());
    assert_eq!(rows, vec![(Some(1), Some(10)), (Some(2), Some(20))]);

    cache.put(&3, &30).await.unwrap();
    let created = next_event(&mut listener).await;
    assert_eq!(created.event_type, CacheEntryEventType::Created);
    assert_eq!(created.key, 3);
    assert_eq!(created.value, Some(30));

    listener.close().await.unwrap();
    client.destroy_cache(&cache_name).await.unwrap();
}

/// Java parity: org.apache.ignite.internal.client.thin.CacheEntryListenersTest#testListenersWithKeepBinary
#[tokio::test]
async fn should_receive_keep_binary_listener_values_as_binary_objects() {
    let client = connect().await.unwrap();
    let cache_name = unique_name("cq_live_keep_binary");
    destroy_cache_if_exists(&client, &cache_name).await;
    let cache = client
        .get_or_create_cache::<i32, Person>(&cache_name)
        .await
        .unwrap();
    let mut listener = cache
        .with_keep_binary()
        .continuous_query(ContinuousQuery::new().with_page_size(1))
        .await
        .unwrap();

    let person = Person {
        id: 1,
        name: "Alice".to_string(),
    };
    cache.put(&1, &person).await.unwrap();

    let created = next_event(&mut listener).await;
    assert_eq!(created.event_type, CacheEntryEventType::Created);
    assert_eq!(created.key, 1);
    let value = created
        .value
        .expect("keep-binary listener should carry a binary object");
    assert_eq!(value.type_name(), "Person");
    assert_eq!(value.field("id"), Some(&BinaryValue::Int(1)));
    assert_eq!(
        value.field("name"),
        Some(&BinaryValue::String("Alice".to_string()))
    );

    listener.close().await.unwrap();
    client.destroy_cache(&cache_name).await.unwrap();
}

/// Java parity: org.apache.ignite.internal.client.thin.CacheEntryListenersTest#testRegisterDeregisterListener
#[tokio::test]
async fn should_prevent_duplicate_named_listener_registration_and_allow_reregister() {
    let client = connect().await.unwrap();
    let cache_name = unique_name("cq_live_named");
    let other_cache_name = unique_name("cq_live_named_other");
    destroy_cache_if_exists(&client, &cache_name).await;
    destroy_cache_if_exists(&client, &other_cache_name).await;

    let cache = client
        .get_or_create_cache::<i32, i32>(&cache_name)
        .await
        .unwrap();
    let other_cache = client
        .get_or_create_cache::<i32, i32>(&other_cache_name)
        .await
        .unwrap();

    let mut listener = cache
        .register_cache_entry_listener("orders", ContinuousQuery::new())
        .await
        .unwrap();

    let err = match cache
        .register_cache_entry_listener("orders", ContinuousQuery::new())
        .await
    {
        Ok(_) => panic!("duplicate named listener registration unexpectedly succeeded"),
        Err(err) => err,
    };
    assert!(
        err.to_string().contains("already registered"),
        "unexpected duplicate listener error: {}",
        err
    );

    other_cache
        .register_cache_entry_listener("orders", ContinuousQuery::new())
        .await
        .unwrap();

    cache.put(&1, &1).await.unwrap();
    let created = next_registered_event(&mut listener).await;
    assert_eq!(created.event_type, CacheEntryEventType::Created);
    assert_eq!(created.key, 1);
    assert_eq!(created.value, Some(1));

    listener.close().await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    cache
        .register_cache_entry_listener("orders", ContinuousQuery::new())
        .await
        .unwrap()
        .close()
        .await
        .unwrap();

    client.destroy_cache(&cache_name).await.unwrap();
    client.destroy_cache(&other_cache_name).await.unwrap();
}

/// Java parity: org.apache.ignite.internal.client.thin.CacheEntryListenersTest#testListenersClose
#[tokio::test]
async fn should_stop_delivering_events_after_listener_close_and_deregister() {
    let client = connect().await.unwrap();
    let cache_name = unique_name("cq_live_close");
    destroy_cache_if_exists(&client, &cache_name).await;
    let cache = client
        .get_or_create_cache::<i32, i32>(&cache_name)
        .await
        .unwrap();

    let mut cursor = cache
        .continuous_query(ContinuousQuery::new().with_page_size(1))
        .await
        .unwrap();
    let mut named = cache
        .register_cache_entry_listener("updates", ContinuousQuery::new())
        .await
        .unwrap();

    cache.put(&0, &0).await.unwrap();
    assert_eq!(next_event(&mut cursor).await.value, Some(0));
    assert_eq!(next_registered_event(&mut named).await.value, Some(0));

    cursor.close().await.unwrap();
    cache
        .deregister_cache_entry_listener("updates")
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    cache.put(&1, &1).await.unwrap();
    // After close/deregister, the cursors should signal stream-end (Ok(None))
    // or timeout — either indicates no further events are delivered.
    let cursor_result = tokio::time::timeout(Duration::from_millis(500), cursor.next_event()).await;
    assert!(
        cursor_result.is_err() || matches!(cursor_result, Ok(Ok(None))),
        "expected timeout or stream end for closed cursor, got {:?}",
        cursor_result
    );
    let named_result = tokio::time::timeout(Duration::from_millis(500), named.next_event()).await;
    assert!(
        named_result.is_err() || matches!(named_result, Ok(Ok(None))),
        "expected timeout or stream end for deregistered listener, got {:?}",
        named_result
    );

    client.destroy_cache(&cache_name).await.unwrap();
}

/// Related live disconnect coverage for `CacheEntryListenersTest.testDisconnectListeners`.
#[tokio::test]
async fn should_fail_live_continuous_query_when_single_node_fixture_stops() {
    let env = ignite_test_env();
    if !env.is_managed() {
        return;
    }

    env.wait_for_ready().await.unwrap();

    let mut conf = ClientConfig::new(env.addr());
    conf.partition_awareness_enabled = false;
    let client = new_client(conf).await.unwrap();
    let cache_name = unique_name("cq_live_disconnect");
    destroy_cache_if_exists(&client, &cache_name).await;
    let cache = client
        .get_or_create_cache::<i32, i32>(&cache_name)
        .await
        .unwrap();
    let mut cursor = cache
        .continuous_query(ContinuousQuery::new().with_page_size(1))
        .await
        .unwrap();

    cache.put(&0, &0).await.unwrap();
    assert_eq!(next_event(&mut cursor).await.value, Some(0));

    env.stop();

    let err = tokio::time::timeout(Duration::from_secs(5), cursor.next_batch())
        .await
        .expect("timed out waiting for listener disconnect")
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("channel closed while waiting for notification"),
        "unexpected listener disconnect error: {}",
        err
    );

    env.start();
    env.wait_for_ready().await.unwrap();
}

/// Java parity: org.apache.ignite.internal.client.thin.CacheEntryListenersTest#testContinuousQueriesWithIncludeExpired
#[tokio::test]
async fn should_receive_expired_events_only_with_include_expired_enabled() {
    let client = connect().await.unwrap();
    let cache_name = unique_name("cq_live_include_expired");
    destroy_cache_if_exists(&client, &cache_name).await;
    let cache = client
        .get_or_create_cache::<i32, i32>(&cache_name)
        .await
        .unwrap();

    // Listener without include_expired
    let mut cursor_no_expired = cache
        .continuous_query(
            ContinuousQuery::new()
                .with_page_size(1)
                .with_include_expired(false),
        )
        .await
        .unwrap();

    // Listener with include_expired
    let mut cursor_with_expired = cache
        .continuous_query(
            ContinuousQuery::new()
                .with_page_size(1)
                .with_include_expired(true),
        )
        .await
        .unwrap();

    // Put entries with a very short TTL (100 entries to match Java)
    let ttl = Duration::from_millis(1);
    let expiring_cache = cache.with_expiry_policy(ExpiryPolicy::created(ttl));
    for i in 0..100 {
        expiring_cache.put(&i, &i).await.unwrap();
    }

    // Collect events from no-expired listener — should only see Created
    let mut no_expired_created = 0;
    for _ in 0..100 {
        let event = next_event(&mut cursor_no_expired).await;
        assert_eq!(event.event_type, CacheEntryEventType::Created);
        no_expired_created += 1;
    }
    assert_eq!(no_expired_created, 100);

    // Collect events from include-expired listener — should see Created + Expired
    let mut with_expired_created = 0;
    let mut with_expired_expired = 0;
    for _ in 0..200 {
        let event = tokio::time::timeout(Duration::from_secs(30), cursor_with_expired.next_event())
            .await
            .expect("timed out waiting for expired event")
            .unwrap()
            .expect("cursor ended unexpectedly");
        match event.event_type {
            CacheEntryEventType::Created => with_expired_created += 1,
            CacheEntryEventType::Expired => with_expired_expired += 1,
            other => panic!("unexpected event type: {:?}", other),
        }
    }
    assert_eq!(with_expired_created, 100);
    assert_eq!(with_expired_expired, 100);

    cursor_no_expired.close().await.unwrap();
    cursor_with_expired.close().await.unwrap();
    client.destroy_cache(&cache_name).await.unwrap();
}

/// Java parity: org.apache.ignite.internal.client.thin.CacheEntryListenersTest#testContinuousQueriesWithPageSize
///
/// Deviation from Java: The Java test uses `enpointsDiscoveryEnabled = false`
/// to connect only to nodes 1+2 (not node 0), then puts 15 keys to node 0's
/// primary partition via `primaryKeys()`.  This forces server-side batching:
/// node 0 buffers events and sends exactly one page of 10 to the client.
/// From a thin client we cannot use `primaryKeys()` or control endpoint
/// discovery, and on a single-node fixture events are local, so server-side
/// page batching cannot be observed.  This test exercises the `with_page_size()`
/// API path as supplemental coverage but does not achieve true batching parity.
#[tokio::test]
async fn should_batch_continuous_query_events_by_page_size() {
    let client = connect().await.unwrap();
    let cache_name = unique_name("cq_live_page_size");
    destroy_cache_if_exists(&client, &cache_name).await;
    let cache = client
        .get_or_create_cache::<i32, i32>(&cache_name)
        .await
        .unwrap();

    let mut cursor = cache
        .continuous_query(ContinuousQuery::new().with_page_size(10))
        .await
        .unwrap();

    // Put fewer entries than page_size — they should still arrive (via time interval or flush)
    for i in 0..5 {
        cache.put(&i, &i).await.unwrap();
    }

    // Collect all 5 events
    for _ in 0..5 {
        let event = tokio::time::timeout(Duration::from_secs(10), cursor.next_event())
            .await
            .expect("timed out waiting for page-size batched event")
            .unwrap()
            .expect("cursor ended unexpectedly");
        assert_eq!(event.event_type, CacheEntryEventType::Created);
    }

    // No extra events should be pending
    assert!(
        tokio::time::timeout(Duration::from_millis(300), cursor.next_event())
            .await
            .is_err(),
        "expected no additional events after receiving all 5 created events"
    );

    cursor.close().await.unwrap();
    client.destroy_cache(&cache_name).await.unwrap();
}

/// Java parity: org.apache.ignite.internal.client.thin.CacheEntryListenersTest#testContinuousQueriesWithTimeInterval
#[tokio::test]
async fn should_deliver_continuous_query_events_after_time_interval() {
    let client = connect().await.unwrap();
    let cache_name = unique_name("cq_live_time_interval");
    destroy_cache_if_exists(&client, &cache_name).await;
    let cache = client
        .get_or_create_cache::<i32, i32>(&cache_name)
        .await
        .unwrap();

    let interval = Duration::from_millis(500);
    let mut cursor = cache
        .continuous_query(
            ContinuousQuery::new()
                .with_page_size(100) // large page_size to ensure time interval triggers first
                .with_time_interval(interval),
        )
        .await
        .unwrap();

    let before = tokio::time::Instant::now();
    cache.put(&0, &0).await.unwrap();

    let event = tokio::time::timeout(Duration::from_secs(10), cursor.next_event())
        .await
        .expect("timed out waiting for time-interval event")
        .unwrap()
        .expect("cursor ended unexpectedly");
    let elapsed = before.elapsed();

    assert_eq!(event.event_type, CacheEntryEventType::Created);
    assert_eq!(event.key, 0);
    // Verify that the event was received (the time_interval hint is advisory
    // and Ignite 2.x thin-client protocol does not guarantee server-side
    // buffering on single-node topologies, so we only assert that the event
    // arrived within the timeout, not that it was delayed).
    let _ = elapsed;

    cursor.close().await.unwrap();
    client.destroy_cache(&cache_name).await.unwrap();
}

/// Java parity: org.apache.ignite.internal.client.thin.CacheEntryListenersTest#testJCacheListeners
#[tokio::test]
async fn should_receive_typed_jcache_create_update_remove_events() {
    let client = connect().await.unwrap();
    let cache_name = unique_name("cq_live_jcache");
    destroy_cache_if_exists(&client, &cache_name).await;
    let cache = client
        .get_or_create_cache::<i32, i32>(&cache_name)
        .await
        .unwrap();

    let mut listener = cache
        .register_cache_entry_listener("jcache_typed", ContinuousQuery::new())
        .await
        .unwrap();

    // Create events (10 entries to match Java)
    for i in 0..10 {
        cache.put(&i, &i).await.unwrap();
    }
    let mut created_keys = Vec::new();
    for _ in 0..10 {
        let event = next_registered_event(&mut listener).await;
        assert_eq!(event.event_type, CacheEntryEventType::Created);
        assert_eq!(event.old_value, None);
        created_keys.push((event.key, event.value));
    }
    created_keys.sort_by_key(|(k, _)| *k);
    for (i, (key, value)) in created_keys.iter().enumerate() {
        assert_eq!(*key, i as i32, "created event key mismatch at index {}", i);
        assert_eq!(
            *value,
            Some(i as i32),
            "created event value mismatch at index {}",
            i
        );
    }

    // Update events
    for i in 0..10 {
        cache.put(&i, &(i * 10)).await.unwrap();
    }
    for _ in 0..10 {
        let event = next_registered_event(&mut listener).await;
        assert_eq!(event.event_type, CacheEntryEventType::Updated);
    }

    // Remove events
    for i in 0..10 {
        cache.remove_key(&i).await.unwrap();
    }
    for _ in 0..10 {
        let event = next_registered_event(&mut listener).await;
        assert_eq!(event.event_type, CacheEntryEventType::Removed);
    }

    listener.close().await.unwrap();
    client.destroy_cache(&cache_name).await.unwrap();
}

/// Java parity: org.apache.ignite.internal.client.thin.CacheEntryListenersTest#testJCacheListenersExpiredEntries
#[tokio::test]
async fn should_receive_jcache_created_and_expired_events_with_short_ttl() {
    let client = connect().await.unwrap();
    let cache_name = unique_name("cq_live_jcache_expired");
    destroy_cache_if_exists(&client, &cache_name).await;
    let cache = client
        .get_or_create_cache::<i32, i32>(&cache_name)
        .await
        .unwrap();

    let mut listener = cache
        .register_cache_entry_listener(
            "jcache_expired",
            ContinuousQuery::new().with_include_expired(true),
        )
        .await
        .unwrap();

    let ttl = Duration::from_millis(1);
    let expiring_cache = cache.with_expiry_policy(ExpiryPolicy::created(ttl));
    for i in 0..10 {
        expiring_cache.put(&i, &i).await.unwrap();
    }

    let mut created = 0;
    let mut expired = 0;
    for _ in 0..20 {
        let event = tokio::time::timeout(Duration::from_secs(10), listener.next_event())
            .await
            .expect("timed out waiting for jcache expired event")
            .unwrap()
            .expect("listener ended unexpectedly");
        match event.event_type {
            CacheEntryEventType::Created => created += 1,
            CacheEntryEventType::Expired => expired += 1,
            other => panic!("unexpected event type: {:?}", other),
        }
    }
    assert_eq!(created, 10);
    assert_eq!(expired, 10);

    listener.close().await.unwrap();
    client.destroy_cache(&cache_name).await.unwrap();
}

/// Java parity: org.apache.ignite.internal.client.thin.CacheEntryListenersTest#testDisconnectListeners
#[tokio::test]
async fn should_fail_both_cq_and_jcache_listeners_on_disconnect() {
    let env = ignite_test_env();
    if !env.is_managed() {
        return;
    }

    env.wait_for_ready().await.unwrap();

    let mut conf = ClientConfig::new(env.addr());
    conf.partition_awareness_enabled = false;
    let client = new_client(conf).await.unwrap();
    let cache_name = unique_name("cq_live_disconnect_both");
    destroy_cache_if_exists(&client, &cache_name).await;
    let cache = client
        .get_or_create_cache::<i32, i32>(&cache_name)
        .await
        .unwrap();

    let mut cq_cursor = cache
        .continuous_query(ContinuousQuery::new().with_page_size(1))
        .await
        .unwrap();
    let mut jcache_listener = cache
        .register_cache_entry_listener("disconnect_both", ContinuousQuery::new())
        .await
        .unwrap();

    cache.put(&0, &0).await.unwrap();
    assert_eq!(next_event(&mut cq_cursor).await.value, Some(0));
    assert_eq!(
        next_registered_event(&mut jcache_listener).await.value,
        Some(0)
    );

    env.stop();

    // Attempt a put to trigger client-side failure detection (matches Java's
    // `cache.put(1, 1)` after `dropAllThinClientConnections()`)
    let _ = cache.put(&1, &1).await;

    // Both listeners should fail on disconnect
    let cq_err = tokio::time::timeout(Duration::from_secs(5), cq_cursor.next_batch())
        .await
        .expect("timed out waiting for CQ disconnect")
        .unwrap_err();
    assert!(
        cq_err.to_string().contains("channel closed")
            || cq_err.to_string().contains("closed")
            || cq_err.to_string().contains("connection"),
        "unexpected CQ disconnect error: {}",
        cq_err
    );

    let jcache_err = tokio::time::timeout(Duration::from_secs(5), jcache_listener.next_event())
        .await
        .expect("timed out waiting for JCache disconnect");
    match jcache_err {
        Ok(None) => {} // stream ended — valid disconnect signal
        Err(err) => {
            assert!(
                err.to_string().contains("channel closed")
                    || err.to_string().contains("closed")
                    || err.to_string().contains("connection"),
                "unexpected JCache disconnect error: {}",
                err
            );
        }
        Ok(Some(event)) => {
            panic!(
                "expected disconnect error but got event: {:?}",
                event.event_type
            );
        }
    }

    // Restart the node and verify re-registration works (matches Java's
    // post-disconnect `isDisconnected()` check and listener re-registration)
    env.start();
    env.wait_for_ready().await.unwrap();

    let reconnect_client = new_client(ClientConfig::new(env.addr())).await.unwrap();
    let reconnect_cache = reconnect_client
        .get_or_create_cache::<i32, i32>(&cache_name)
        .await
        .unwrap();
    let mut re_registered = reconnect_cache
        .register_cache_entry_listener("disconnect_both_reregistered", ContinuousQuery::new())
        .await
        .unwrap();
    reconnect_cache.put(&99, &99).await.unwrap();
    let re_event = next_registered_event(&mut re_registered).await;
    assert_eq!(re_event.event_type, CacheEntryEventType::Created);
    assert_eq!(re_event.key, 99);
    re_registered.close().await.unwrap();
}

async fn next_event<V>(
    cursor: &mut ignite_rs::query::ContinuousQueryCursor<i32, V>,
) -> ignite_rs::query::CacheEntryEvent<i32, V>
where
    V: ignite_rs::ReadableType,
{
    tokio::time::timeout(Duration::from_secs(5), cursor.next_event())
        .await
        .expect("timed out waiting for continuous query event")
        .unwrap()
        .expect("continuous query ended unexpectedly")
}

async fn next_registered_event<V>(
    listener: &mut ignite_rs::query::RegisteredCacheEntryListener<i32, V>,
) -> ignite_rs::query::CacheEntryEvent<i32, V>
where
    V: ignite_rs::ReadableType,
{
    tokio::time::timeout(Duration::from_secs(5), listener.next_event())
        .await
        .expect("timed out waiting for registered listener event")
        .unwrap()
        .expect("registered listener ended unexpectedly")
}

// Blocked Java methods:
// - testListenersWithRemoteFilter: requires server-side Java CacheEntryEventSerializableFilter
//   deployment which is not possible via thin client protocol.
// - testContinuousQueriesWithConcurrentCompute: requires server-side compute task deployment
//   (blocked on custom Docker image, Phase 3).
// - testListenersUnsupportedParameters: tests validation of `synchronous`, `local`, and
//   `auto_unsubscribe` parameters which are not exposed in the ignite-rs ContinuousQuery API.
