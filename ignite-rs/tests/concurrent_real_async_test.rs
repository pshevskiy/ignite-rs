#![cfg(not(feature = "ssl"))]

mod common;

use common::{connect, destroy_cache_if_exists, unique_name, SqlTableFixture, SQL_SCHEMA};
use ignite_rs::cache::CachePeekMode;
use ignite_rs::protocol::complex_obj::{ComplexObject, IgniteValue};
use ignite_rs::query::{ScanQuery, SqlFieldsQuery};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

fn decode_pairs(rows: Vec<(Option<i32>, Option<i32>)>) -> BTreeMap<i32, i32> {
    rows.into_iter()
        .filter_map(|(key, value)| match (key, value) {
            (Some(key), Some(value)) => Some((key, value)),
            _ => None,
        })
        .collect()
}

fn decode_scan_keys(rows: Vec<(Option<ComplexObject>, Option<ComplexObject>)>) -> Vec<i64> {
    let mut keys = rows
        .into_iter()
        .map(
            |(key, _)| match key.expect("expected scan key").values.as_slice() {
                [IgniteValue::Long(big)] => *big,
                other => panic!("unexpected scan key layout: {:?}", other),
            },
        )
        .collect::<Vec<_>>();
    keys.sort_unstable();
    keys
}

/// Supplemental live async coverage for the core cache API.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn should_complete_concurrent_live_core_cache_operations_on_one_shared_client() {
    let ignite = Arc::new(connect().await.unwrap());

    let single_cache_name = unique_name("async_core_single");
    let conditional_cache_name = unique_name("async_core_conditional");
    let bulk_cache_name = unique_name("async_core_bulk");
    let size_cache_name = unique_name("async_core_size");

    for cache_name in [
        &single_cache_name,
        &conditional_cache_name,
        &bulk_cache_name,
        &size_cache_name,
    ] {
        destroy_cache_if_exists(&ignite, cache_name).await;
    }

    ignite
        .get_or_create_cache::<i32, i32>(&single_cache_name)
        .await
        .unwrap();
    ignite
        .get_or_create_cache::<i32, i32>(&conditional_cache_name)
        .await
        .unwrap();
    ignite
        .get_or_create_cache::<i32, i32>(&bulk_cache_name)
        .await
        .unwrap();
    ignite
        .get_or_create_cache::<i32, i32>(&size_cache_name)
        .await
        .unwrap();

    let single_cache_name_for_single = single_cache_name.clone();
    let conditional_cache_name_for_conditional = conditional_cache_name.clone();
    let bulk_cache_name_for_bulk = bulk_cache_name.clone();
    let size_cache_name_for_size = size_cache_name.clone();

    let single = {
        let ignite = Arc::clone(&ignite);
        async move {
            let cache = ignite.cache::<i32, i32>(&single_cache_name_for_single);
            cache.put(&1, &10).await.unwrap();
            assert!(cache.contains_key(&1).await.unwrap());
            assert_eq!(cache.get(&1).await.unwrap(), Some(10));
            assert_eq!(cache.get_and_put(&1, &11).await.unwrap(), Some(10));
            assert_eq!(cache.get(&1).await.unwrap(), Some(11));
            assert_eq!(cache.get_and_replace(&1, &12).await.unwrap(), Some(11));
            assert_eq!(cache.get_and_remove(&1).await.unwrap(), Some(12));
            assert_eq!(cache.get(&1).await.unwrap(), None);
            cache.put(&2, &20).await.unwrap();
            cache.clear_key(&2).await.unwrap();
            assert_eq!(cache.get(&2).await.unwrap(), None);
        }
    };

    let conditional = {
        let ignite = Arc::clone(&ignite);
        async move {
            let cache = ignite.cache::<i32, i32>(&conditional_cache_name_for_conditional);
            assert!(cache.put_if_absent(&10, &100).await.unwrap());
            assert!(!cache.put_if_absent(&10, &101).await.unwrap());
            assert_eq!(
                cache.get_and_put_if_absent(&10, &102).await.unwrap(),
                Some(100)
            );
            assert!(cache.replace(&10, &103).await.unwrap());
            assert!(!cache.replace_if_equals(&10, &100, &104).await.unwrap());
            assert!(cache.replace_if_equals(&10, &103, &104).await.unwrap());
            assert!(!cache.remove_if_equals(&10, &103).await.unwrap());
            assert!(cache.remove_if_equals(&10, &104).await.unwrap());
            assert!(!cache.remove_key(&999).await.unwrap());
            cache.put(&11, &110).await.unwrap();
            assert!(cache.remove_key(&11).await.unwrap());
            assert_eq!(cache.get(&11).await.unwrap(), None);
        }
    };

    let bulk = {
        let ignite = Arc::clone(&ignite);
        async move {
            let cache = ignite.cache::<i32, i32>(&bulk_cache_name_for_bulk);
            let pairs = vec![(21, 210), (22, 220), (23, 230), (24, 240)];
            let keys = vec![21, 22, 23, 24];
            cache.put_all(&pairs).await.unwrap();
            assert!(cache.contains_keys(&keys).await.unwrap());
            assert_eq!(
                decode_pairs(cache.get_all(&keys).await.unwrap()),
                pairs.iter().cloned().collect()
            );

            cache.remove_keys(&[21, 22]).await.unwrap();
            cache.clear_keys(&[23]).await.unwrap();
            assert_eq!(cache.get_size().await.unwrap(), 1);
            assert_eq!(cache.get(&24).await.unwrap(), Some(240));

            cache.remove_all().await.unwrap();
            assert_eq!(cache.get_size().await.unwrap(), 0);
        }
    };

    let sizing = {
        let ignite = Arc::clone(&ignite);
        async move {
            let cache = ignite.cache::<i32, i32>(&size_cache_name_for_size);
            cache
                .put_all(&[(31, 310), (32, 320), (33, 330)])
                .await
                .unwrap();
            assert_eq!(cache.get_size().await.unwrap(), 3);
            assert_eq!(
                cache.get_size_peek_mode(CachePeekMode::All).await.unwrap(),
                3
            );
            assert_eq!(
                cache
                    .get_size_peek_modes(vec![CachePeekMode::Primary, CachePeekMode::Backup])
                    .await
                    .unwrap(),
                3
            );
            cache.clear().await.unwrap();
            assert_eq!(cache.get_size().await.unwrap(), 0);
        }
    };

    tokio::join!(single, conditional, bulk, sizing);

    for cache_name in [
        &single_cache_name,
        &conditional_cache_name,
        &bulk_cache_name,
        &size_cache_name,
    ] {
        ignite.destroy_cache(cache_name).await.unwrap();
    }
}

/// Supplemental live async coverage for the real query surface on one shared client.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn should_complete_concurrent_live_query_operations_on_one_shared_client() {
    let ignite = Arc::new(connect().await.unwrap());
    let fixture = SqlTableFixture::create_seeded(&ignite).await.unwrap();
    fixture.insert_row(2).await.unwrap();
    fixture.insert_row(3).await.unwrap();

    let fixture_cache_name_for_scan = fixture.cache_name.clone();
    let fixture_cache_name_for_query_scan = fixture.cache_name.clone();
    let bootstrap_cache_name_for_sql = fixture.bootstrap_cache_name.clone();
    let fixture_table_name_for_client_sql = fixture.table_name.clone();
    let fixture_table_name_for_cache_sql = fixture.table_name.clone();

    let scan = {
        let ignite = Arc::clone(&ignite);
        async move {
            let cache = ignite.cache::<ComplexObject, ComplexObject>(&fixture_cache_name_for_scan);
            let rows = cache
                .scan_query(ScanQuery::new().with_page_size(1))
                .await
                .unwrap()
                .fetch_all()
                .await
                .unwrap();
            assert_eq!(decode_scan_keys(rows), vec![1, 2, 3]);
        }
    };

    let query_scan = {
        let ignite = Arc::clone(&ignite);
        async move {
            let cache =
                ignite.cache::<ComplexObject, ComplexObject>(&fixture_cache_name_for_query_scan);
            let rows = cache.query_scan(1).await.unwrap();
            assert_eq!(decode_scan_keys(rows), vec![1, 2, 3]);
        }
    };

    let client_sql = {
        let ignite = Arc::clone(&ignite);
        async move {
            let rows = ignite
                .sql_fields(
                    SqlFieldsQuery::<i64>::new(&format!(
                        "SELECT big FROM {} ORDER BY big",
                        fixture_table_name_for_client_sql
                    ))
                    .with_schema(SQL_SCHEMA)
                    .with_page_size(1),
                )
                .await
                .unwrap()
                .fetch_all()
                .await
                .unwrap();
            assert_eq!(rows, vec![1, 2, 3]);
        }
    };

    let cache_sql = {
        let ignite = Arc::clone(&ignite);
        async move {
            let bootstrap_cache = ignite.cache::<i32, i32>(&bootstrap_cache_name_for_sql);
            let rows = bootstrap_cache
                .sql_fields(
                    SqlFieldsQuery::<i64>::new(&format!(
                        "SELECT COUNT(*) FROM {}",
                        fixture_table_name_for_cache_sql
                    ))
                    .with_schema(SQL_SCHEMA)
                    .with_page_size(1),
                )
                .await
                .unwrap()
                .fetch_all()
                .await
                .unwrap();
            assert_eq!(rows, vec![3]);
        }
    };

    tokio::join!(scan, query_scan, client_sql, cache_sql);

    fixture.cleanup(&ignite).await;
}

/// Supplemental live async coverage that mixes cache and SQL-table operations on one shared client.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn should_complete_concurrent_live_cache_and_sql_table_operations_on_one_shared_client() {
    let ignite = Arc::new(connect().await.unwrap());
    let fixture = SqlTableFixture::create_seeded(&ignite).await.unwrap();
    let bootstrap_cache_name = fixture.bootstrap_cache_name.clone();
    let cache_name = fixture.cache_name.clone();
    let table_name = fixture.table_name.clone();
    let barrier = Arc::new(tokio::sync::Barrier::new(2));

    let sql_insert = {
        let ignite = Arc::clone(&ignite);
        let barrier = Arc::clone(&barrier);
        async move {
            barrier.wait().await;
            let bootstrap_cache = ignite.cache::<i32, i32>(&bootstrap_cache_name);
            bootstrap_cache
                .sql_fields::<i64>(
                    SqlFieldsQuery::new(&format!(
                        "INSERT INTO {} (big, bool, dec, int, null_int, small, char, var, ts) \
                         VALUES (2, true, 2.0, 4, null, 4, 'c', 'varchar2', \
                         timestamp '2023-06-21 12:34:56 UTC')",
                        table_name
                    ))
                    .with_schema(SQL_SCHEMA),
                )
                .await
                .unwrap()
                .fetch_all()
                .await
                .unwrap();
        }
    };

    let cache_read = {
        let ignite = Arc::clone(&ignite);
        let barrier = Arc::clone(&barrier);
        async move {
            barrier.wait().await;
            let cache = ignite.cache::<ComplexObject, ComplexObject>(&cache_name);
            let deadline = Instant::now() + Duration::from_secs(2);

            loop {
                let rows = cache.query_scan(1).await.unwrap();
                if decode_scan_keys(rows) == vec![1, 2] {
                    break;
                }

                assert!(
                    Instant::now() < deadline,
                    "timed out waiting for cache view to observe SQL insert"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    };

    tokio::join!(sql_insert, cache_read);

    let rows = ignite
        .sql_fields(
            SqlFieldsQuery::<i64>::new(&format!(
                "SELECT big FROM {} ORDER BY big",
                fixture.table_name
            ))
            .with_schema(SQL_SCHEMA),
        )
        .await
        .unwrap()
        .fetch_all()
        .await
        .unwrap();
    assert_eq!(rows, vec![1, 2]);

    fixture.cleanup(&ignite).await;
}
