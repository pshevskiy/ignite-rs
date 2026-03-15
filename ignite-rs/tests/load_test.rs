#![cfg(not(feature = "ssl"))]

mod common;

use common::{connect, destroy_cache_if_exists, unique_name};
use ignite_rs::query::ScanQuery;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;

const THREAD_CNT: usize = 8;
const ITERATION_CNT: usize = 20;
const BATCH_SIZE: i32 = 1000;
const PAGE_CNT: i32 = 3;

fn expected_batch(range_start: i32, range_end: i32) -> BTreeMap<i32, String> {
    (range_start..range_end)
        .map(|key| (key, format!("String {}", key)))
        .collect()
}

fn decode_entries(rows: Vec<(Option<i32>, Option<String>)>) -> BTreeMap<i32, String> {
    rows.into_iter()
        .filter_map(|(key, value)| match (key, value) {
            (Some(key), Some(value)) => Some((key, value)),
            _ => None,
        })
        .collect()
}

/// Java parity: org.apache.ignite.client.LoadTest#testMultithreading
///
/// `ignite-rs` does not expose the Java thin client's remote `ScanQuery` predicate hook,
/// so this live equivalent verifies each inserted batch via a real `get_all` round trip and
/// then exercises a real paged `scan_query` after the concurrent workload completes.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn should_handle_live_multithreaded_load_on_one_shared_client() {
    let client = Arc::new(connect().await.unwrap());
    let cache_name = unique_name("testMultithreading");

    destroy_cache_if_exists(&client, &cache_name).await;
    client
        .get_or_create_cache::<i32, String>(&cache_name)
        .await
        .unwrap();

    let next_range = Arc::new(AtomicI32::new(1));
    let mut handles = Vec::with_capacity(THREAD_CNT);

    for _ in 0..THREAD_CNT {
        let client = Arc::clone(&client);
        let cache_name = cache_name.clone();
        let next_range = Arc::clone(&next_range);

        handles.push(tokio::spawn(async move {
            let cache = client.cache::<i32, String>(&cache_name);

            for _ in 0..ITERATION_CNT {
                let range_start = next_range.fetch_add(BATCH_SIZE, Ordering::SeqCst);
                let range_end = range_start + BATCH_SIZE;
                let expected = expected_batch(range_start, range_end);
                let pairs = expected
                    .iter()
                    .map(|(key, value)| (*key, value.clone()))
                    .collect::<Vec<_>>();
                let keys = pairs.iter().map(|(key, _)| *key).collect::<Vec<_>>();

                cache.put_all(&pairs).await.unwrap();

                let rows = cache.get_all(&keys).await.unwrap();

                let actual = decode_entries(rows);

                assert_eq!(
                    expected.len(),
                    actual.len(),
                    "unexpected number of entries for range [{range_start}, {range_end})",
                );
                assert_eq!(
                    expected, actual,
                    "unexpected entries for range [{range_start}, {range_end})",
                );
            }
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let expected_total_entries = (THREAD_CNT * ITERATION_CNT) as i32 * BATCH_SIZE;
    let final_rows = client
        .cache::<i32, String>(&cache_name)
        .scan_query(ScanQuery::new().with_page_size((BATCH_SIZE / PAGE_CNT).max(1)))
        .await
        .unwrap()
        .fetch_all()
        .await
        .unwrap();
    let final_entries = decode_entries(final_rows);

    assert_eq!(
        expected_total_entries as usize,
        final_entries.len(),
        "expected final paged scan to return the full concurrent workload",
    );

    client.destroy_cache(&cache_name).await.unwrap();
}
