#![cfg(not(feature = "ssl"))]

mod common;

use common::{connect, execute_sql, unique_name, SqlTableFixture, SQL_SCHEMA};
use ignite_rs::binary::BinaryObject;
use ignite_rs::error::IgniteResult;
use ignite_rs::protocol::complex_obj::{ComplexObject, IgniteValue};
use ignite_rs::query::{ScanQuery, SqlDecimal, SqlFieldsQuery, SqlQuery, SqlTimestamp, SqlValue};
use ignite_rs::Client;

/// Java parity: org.apache.ignite.client.FunctionalQueryTest#testGettingEmptyResultWhenQueryingEmptyTable
#[tokio::test]
async fn should_return_empty_results_when_querying_empty_sql_table() {
    let ignite = connect().await.unwrap();
    let fixture = SqlTableFixture::create_empty(&ignite).await.unwrap();

    let sql_rows = execute_sql(
        &fixture.bootstrap_cache,
        &format!("SELECT big FROM {} ORDER BY big", fixture.table_name),
    )
    .await
    .unwrap();

    let cache = ignite.cache::<ComplexObject, ComplexObject>(&fixture.cache_name);
    let scan_rows = cache
        .scan_query(ScanQuery::new().with_page_size(1))
        .await
        .unwrap()
        .fetch_all()
        .await
        .unwrap();
    fixture.cleanup(&ignite).await;

    assert!(
        sql_rows.is_empty(),
        "expected empty SQL rows, got {:?}",
        sql_rows
    );
    assert!(
        scan_rows.is_empty(),
        "expected empty scan rows, got {:?}",
        scan_rows
    );
}

/// Java parity: org.apache.ignite.client.FunctionalQueryTest#testSql
#[tokio::test]
async fn should_query_sql_fields_over_public_schema() {
    let ignite = connect().await.unwrap();
    let fixture = SqlTableFixture::create_seeded(&ignite).await.unwrap();

    let rows = ignite
        .sql_fields(
            SqlFieldsQuery::<i64>::new(&format!(
                "SELECT big FROM {} WHERE big >= 1 ORDER BY big",
                fixture.table_name
            ))
            .with_schema(SQL_SCHEMA)
            .with_page_size(1),
        )
        .await
        .unwrap();
    let rows = rows.fetch_all().await.unwrap();
    fixture.cleanup(&ignite).await;

    assert_eq!(rows, vec![1]);
}

/// Java parity: org.apache.ignite.client.FunctionalQueryTest#testMixedQueryAndCacheApiOperations
#[tokio::test]
async fn should_mix_client_global_sql_and_cache_api_operations() {
    let ignite = connect().await.unwrap();
    let table_name = unique_name("PERSON");
    let cache_name = unique_name("person_cache");
    let type_name = unique_name("MixedQueryPerson");

    ignite
        .sql_fields::<Vec<SqlValue>>(
            SqlFieldsQuery::new(&format!(
                "CREATE TABLE {} (key INT PRIMARY KEY, name VARCHAR) \
                 WITH \"CACHE_NAME={},VALUE_TYPE={}\"",
                table_name, cache_name, type_name
            ))
            .with_schema(SQL_SCHEMA),
        )
        .await
        .unwrap()
        .fetch_all()
        .await
        .unwrap();

    ignite
        .sql_fields::<Vec<SqlValue>>(
            SqlFieldsQuery::new(&format!(
                "INSERT INTO {} (key, name) VALUES (?, ?)",
                table_name
            ))
            .with_schema(SQL_SCHEMA)
            .with_args(vec![
                IgniteValue::Int(1),
                IgniteValue::String("Person 1".to_string()),
            ]),
        )
        .await
        .unwrap()
        .fetch_all()
        .await
        .unwrap();

    let cache = ignite.cache::<i32, BinaryObject>(&cache_name);
    let person2 = ignite
        .binary()
        .builder(&type_name)
        .set_field("name", "Person 2")
        .build();
    cache.put(&2, &person2).await.unwrap();

    let cached_name = match cache
        .get(&1)
        .await
        .unwrap()
        .and_then(|person| person.field("name").cloned())
    {
        Some(IgniteValue::String(name)) => name,
        other => panic!("unexpected cached value from SQL insert: {:?}", other),
    };

    let rows = ignite
        .sql_fields(
            SqlFieldsQuery::<String>::new(&format!(
                "SELECT name FROM {} WHERE key = 2",
                table_name
            ))
            .with_schema(SQL_SCHEMA),
        )
        .await
        .unwrap()
        .fetch_all()
        .await
        .unwrap();

    let _ = ignite
        .sql_fields::<Vec<SqlValue>>(
            SqlFieldsQuery::new(&format!("DROP TABLE {}", table_name)).with_schema(SQL_SCHEMA),
        )
        .await;
    let _ = ignite.destroy_cache(&cache_name).await;

    assert_eq!(cached_name, "Person 1".to_string());
    assert_eq!(rows, vec!["Person 2".to_string()]);
}

/// Java parity: org.apache.ignite.client.FunctionalQueryTest#testMixedQueryAndCacheApiOperations
#[tokio::test]
async fn should_read_data_from_sql_backed_cache() {
    let ignite = connect().await.unwrap();
    let fixture = SqlTableFixture::create_seeded(&ignite).await.unwrap();

    let cache = ignite.cache::<ComplexObject, ComplexObject>(&fixture.cache_name);
    let actual = cache
        .scan_query(ScanQuery::new().with_page_size(100))
        .await
        .unwrap()
        .fetch_all()
        .await
        .unwrap();
    fixture.cleanup(&ignite).await;

    assert_eq!(actual.len(), 1);

    let (key, value) = &actual[0];
    let key = key.as_ref().expect("expected key");
    let value = value.as_ref().expect("expected value");

    assert_eq!(key.values, vec![IgniteValue::Long(1)]);
    assert_eq!(
        value.values,
        vec![
            IgniteValue::Bool(true),
            IgniteValue::Decimal(1, vec![20]),
            IgniteValue::Int(3),
            IgniteValue::Null,
            IgniteValue::Short(4),
            IgniteValue::String("c".to_string()),
            IgniteValue::String("varchar".to_string()),
            IgniteValue::Timestamp(1687350896000, 0),
        ]
    );
}

/// Java parity: org.apache.ignite.client.FunctionalQueryTest#testQueries
#[tokio::test]
async fn should_paginate_scan_results_across_pages() {
    let ignite = connect().await.unwrap();
    let fixture = SqlTableFixture::create_seeded(&ignite).await.unwrap();
    fixture.insert_row(2).await.unwrap();
    fixture.insert_row(3).await.unwrap();

    let cache = ignite.cache::<ComplexObject, ComplexObject>(&fixture.cache_name);
    let actual = cache
        .scan_query(ScanQuery::new().with_page_size(1))
        .await
        .unwrap()
        .fetch_all()
        .await
        .unwrap();
    fixture.cleanup(&ignite).await;

    let mut keys: Vec<i64> = actual
        .into_iter()
        .map(|(key, _)| match key.unwrap().values.as_slice() {
            [IgniteValue::Long(big)] => *big,
            other => panic!("unexpected key layout: {:?}", other),
        })
        .collect();
    keys.sort_unstable();

    assert_eq!(keys, vec![1, 2, 3]);
}

/// Java parity: org.apache.ignite.client.FunctionalQueryTest#testQueries
#[tokio::test]
async fn should_paginate_cache_sql_query_results_across_pages() {
    let ignite = connect().await.unwrap();
    let fixture = PersonQueryCacheFixture::create(&ignite).await.unwrap();
    fixture.put_person(&ignite, 1, "Person 1").await.unwrap();
    fixture.put_person(&ignite, 2, "Person 2").await.unwrap();
    fixture.put_person(&ignite, 3, "Person 3").await.unwrap();

    let rows = fixture
        .cache
        .sql_query(
            SqlQuery::new(&fixture.type_name, "id >= ?")
                .with_args(vec![IgniteValue::Int(2)])
                .with_page_size(1),
        )
        .await
        .unwrap()
        .fetch_all()
        .await
        .unwrap();
    fixture.cleanup(&ignite).await;

    let mut actual = rows
        .into_iter()
        .map(|(key, value)| {
            let key = key.expect("expected SQL query key");
            let value = value.expect("expected SQL query value");
            let id = match value.field("id") {
                Some(IgniteValue::Int(id)) => *id,
                other => panic!("unexpected id field: {:?}", other),
            };
            let name = match value.field("name") {
                Some(IgniteValue::String(name)) => name.clone(),
                other => panic!("unexpected name field: {:?}", other),
            };
            (key, id, name)
        })
        .collect::<Vec<_>>();
    actual.sort_unstable_by_key(|(key, _, _)| *key);

    assert_eq!(
        actual,
        vec![
            (2, 2, "Person 2".to_string()),
            (3, 3, "Person 3".to_string()),
        ]
    );
}

/// Java parity: org.apache.ignite.client.FunctionalQueryTest#testQueries
/// Java parity: org.apache.ignite.client.FunctionalQueryTest#testSql
#[tokio::test]
async fn should_paginate_sql_fields_results_across_pages() {
    let ignite = connect().await.unwrap();
    let fixture = SqlTableFixture::create_seeded(&ignite).await.unwrap();
    fixture.insert_row(2).await.unwrap();
    fixture.insert_row(3).await.unwrap();

    let cursor = ignite
        .sql_fields(
            SqlFieldsQuery::<i64>::new(&format!(
                "SELECT big FROM {} ORDER BY big",
                fixture.table_name
            ))
            .with_schema(SQL_SCHEMA)
            .with_page_size(1),
        )
        .await
        .unwrap();
    let actual = cursor.fetch_all().await.unwrap();
    fixture.cleanup(&ignite).await;

    assert_eq!(actual, vec![1, 2, 3]);
}

/// Java parity: org.apache.ignite.client.FunctionalQueryTest#testQueries
/// Java parity: org.apache.ignite.client.FunctionalQueryTest#testSql
#[tokio::test]
async fn should_decode_typed_sql_fields_rows() {
    let ignite = connect().await.unwrap();
    let fixture = SqlTableFixture::create_seeded(&ignite).await.unwrap();

    let rows = ignite
        .sql_fields(
            SqlFieldsQuery::<(i64, bool, Option<i32>, String)>::new(&format!(
                "SELECT big, bool, null_int, var FROM {} ORDER BY big",
                fixture.table_name
            ))
            .with_schema(SQL_SCHEMA)
            .with_page_size(1),
        )
        .await
        .unwrap()
        .fetch_all()
        .await
        .unwrap();
    fixture.cleanup(&ignite).await;

    assert_eq!(rows, vec![(1, true, None, "varchar".to_string())]);
}

/// Java parity: org.apache.ignite.client.FunctionalQueryTest#testQueries
/// Java parity: org.apache.ignite.client.FunctionalQueryTest#testSql
#[tokio::test]
async fn should_decode_generic_sql_values_for_supported_types() {
    let ignite = connect().await.unwrap();
    let fixture = SqlTableFixture::create_seeded(&ignite).await.unwrap();

    let rows = ignite
        .sql_fields(
            SqlFieldsQuery::<Vec<SqlValue>>::new(&format!(
                "SELECT big, bool, dec, int, null_int, small, char, var, ts FROM {} ORDER BY big",
                fixture.table_name
            ))
            .with_schema(SQL_SCHEMA)
            .with_page_size(1),
        )
        .await
        .unwrap()
        .fetch_all()
        .await
        .unwrap();
    fixture.cleanup(&ignite).await;

    assert_eq!(
        rows,
        vec![vec![
            SqlValue::Long(1),
            SqlValue::Bool(true),
            SqlValue::Decimal(SqlDecimal {
                scale: 1,
                magnitude: vec![20],
            }),
            SqlValue::Int(3),
            SqlValue::Null,
            SqlValue::Short(4),
            SqlValue::String("c".to_string()),
            SqlValue::String("varchar".to_string()),
            SqlValue::Timestamp(SqlTimestamp {
                millis: 1_687_350_896_000,
                nanos: 0,
            }),
        ]]
    );
}

/// Java parity: org.apache.ignite.client.FunctionalQueryTest#testGettingEmptyResultWhenQueryingEmptyTable
#[tokio::test]
async fn should_return_empty_results_when_querying_empty_cache_sql_type() {
    let ignite = connect().await.unwrap();
    let fixture = PersonQueryCacheFixture::create(&ignite).await.unwrap();

    let rows = fixture
        .cache
        .sql_query(SqlQuery::new(&fixture.type_name, "1 = 1").with_page_size(1))
        .await
        .unwrap()
        .fetch_all()
        .await
        .unwrap();
    fixture.cleanup(&ignite).await;

    assert!(
        rows.is_empty(),
        "expected empty cache SQL query rows, got {:?}",
        rows
    );
}

/// Java parity: org.apache.ignite.client.FunctionalQueryTest#testSqlParameterValidation
#[tokio::test]
async fn should_validate_sql_fields_parameters() {
    let ignite = connect().await.unwrap();

    let batch_err = match ignite
        .sql_fields::<Vec<SqlValue>>(SqlFieldsQuery::new("SELECT 1").with_update_batch_size(0))
        .await
    {
        Ok(_) => panic!("expected invalid updateBatchSize to fail"),
        Err(err) => err,
    };
    assert!(
        batch_err
            .to_string()
            .contains("updateBatchSize cannot be lower than 1"),
        "unexpected updateBatchSize error: {}",
        batch_err
    );

    let partition_err = match ignite
        .sql_fields::<Vec<SqlValue>>(SqlFieldsQuery::new("SELECT 1").with_partitions([0, -1]))
        .await
    {
        Ok(_) => panic!("expected invalid partitions to fail"),
        Err(err) => err,
    };
    assert!(
        partition_err.to_string().contains("Illegal partition"),
        "unexpected partition validation error: {}",
        partition_err
    );
}

/// Java parity: org.apache.ignite.client.FunctionalQueryTest#testEmptyQuery
#[tokio::test]
async fn should_reject_empty_sql_fields_query() {
    let ignite = connect().await.unwrap();

    let err = match ignite
        .sql_fields::<Vec<SqlValue>>(SqlFieldsQuery::new(""))
        .await
    {
        Ok(_) => panic!("expected empty SQL query to fail"),
        Err(err) => err,
    };
    assert!(
        err.to_string().contains("Failed to parse SQL"),
        "unexpected empty-query error: {}",
        err
    );
}

/// Java parity: org.apache.ignite.client.FunctionalQueryTest#testQueryInitiatorId
#[tokio::test]
#[ignore = "stock apacheignite/ignite Docker images do not advertise QRY_INITIATOR_ID; requires a custom Ignite build"]
async fn should_round_trip_query_initiator_id_via_sys_sql_queries() {
    let ignite = connect().await.unwrap();
    let cache_name = unique_name("functional_query_initiator");
    let cache = ignite
        .get_or_create_cache::<i32, i32>(&cache_name)
        .await
        .unwrap();

    let client_rows = ignite
        .sql_fields(
            SqlFieldsQuery::<String>::new("SELECT INITIATOR_ID FROM SYS.SQL_QUERIES")
                .with_query_initiator_id("client-initiator"),
        )
        .await
        .unwrap()
        .fetch_all()
        .await
        .unwrap();

    let cache_rows = cache
        .sql_fields(
            SqlFieldsQuery::<String>::new("SELECT INITIATOR_ID FROM SYS.SQL_QUERIES")
                .with_schema(SQL_SCHEMA)
                .with_query_initiator_id("cache-initiator"),
        )
        .await
        .unwrap()
        .fetch_all()
        .await
        .unwrap();
    let _ = ignite.destroy_cache(&cache_name).await;

    assert_eq!(client_rows, vec!["client-initiator".to_string()]);
    assert_eq!(cache_rows, vec!["cache-initiator".to_string()]);
}

struct PersonQueryCacheFixture {
    bootstrap_cache_name: String,
    bootstrap_cache: ignite_rs::cache::Cache<i32, i32>,
    table_name: String,
    cache_name: String,
    type_name: String,
    cache: ignite_rs::cache::Cache<i32, BinaryObject>,
}

impl PersonQueryCacheFixture {
    async fn create(ignite: &Client) -> IgniteResult<Self> {
        let bootstrap_cache_name = unique_name("__INT_TEST_SQL_QUERY_BOOTSTRAP");
        let bootstrap_cache = ignite
            .get_or_create_cache::<i32, i32>(&bootstrap_cache_name)
            .await?;
        let table_name = unique_name("FUNCTIONAL_QUERY_PERSON");
        let cache_name = unique_name("functional_query_sql");
        let type_name = unique_name("FunctionalQueryPerson");
        execute_sql(
            &bootstrap_cache,
            &format!(
                "CREATE TABLE {} (key INT PRIMARY KEY, id INT, name VARCHAR) \
                 WITH \"CACHE_NAME={},VALUE_TYPE={}\"",
                table_name, cache_name, type_name
            ),
        )
        .await?;

        Ok(Self {
            bootstrap_cache_name,
            bootstrap_cache,
            table_name,
            cache_name: cache_name.clone(),
            type_name,
            cache: ignite.cache::<i32, BinaryObject>(&cache_name),
        })
    }

    async fn put_person(&self, ignite: &Client, id: i32, name: &str) -> IgniteResult<()> {
        let person = ignite
            .binary()
            .builder(&self.type_name)
            .set_field("id", id)
            .set_field("name", name)
            .build();
        self.cache.put(&id, &person).await
    }

    async fn cleanup(&self, ignite: &Client) {
        let _ = execute_sql(
            &self.bootstrap_cache,
            &format!("DROP TABLE {}", self.table_name),
        )
        .await;
        let _ = ignite.destroy_cache(&self.bootstrap_cache_name).await;
        let _ = ignite.destroy_cache(&self.cache_name).await;
    }
}
