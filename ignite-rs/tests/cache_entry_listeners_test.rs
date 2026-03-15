#![cfg(not(feature = "ssl"))]

mod common;

use common::{connect, destroy_cache_if_exists, ignite_test_env, unique_name};
use ignite_rs::binary::BinaryValue;
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
    assert_eq!(removed.value, None);

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
    assert_eq!(value.type_name(), "person");
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
    tokio::time::sleep(Duration::from_millis(50)).await;

    cache.put(&1, &1).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(500), cursor.next_event())
            .await
            .is_err()
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(500), named.next_event())
            .await
            .is_err()
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
