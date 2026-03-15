#![cfg(not(feature = "ssl"))]

mod common;

use common::{connect, execute_sql, unique_name};
use ignite_rs::binary::BinaryObject;
use ignite_rs::error::IgniteResult;
use ignite_rs::protocol::complex_obj::ComplexObject;
use ignite_rs::query::SqlQuery;
use ignite_rs::Client;

/// Related Apache Ignite binary SQL-query coverage:
/// org.apache.ignite.client.IgniteBinaryQueryTest#testBinaryQueries
#[tokio::test]
async fn should_decode_binary_object_field_values_from_sql_query_results() {
    let ignite = connect().await.unwrap();
    let fixture = BinaryQueryFixture::create(&ignite).await.unwrap();
    fixture.put_person(&ignite, 1, "Person 1").await.unwrap();

    let cache = ignite
        .cache::<i32, ComplexObject>(&fixture.cache_name)
        .with_keep_binary();
    let rows = cache
        .sql_query(SqlQuery::new(&fixture.type_name, "id = 1"))
        .await
        .unwrap()
        .fetch_all()
        .await
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, Some(1));
    let actual = rows[0].1.as_ref().expect("expected binary value");
    assert_eq!(actual.type_name(), &fixture.type_name);
    assert_eq!(
        actual
            .schema
            .fields
            .iter()
            .map(|field| field.name.as_str())
            .collect::<Vec<_>>(),
        vec!["id", "name"]
    );
    assert_eq!(actual.field("id"), Some(&ignite_rs::protocol::complex_obj::IgniteValue::Int(1)));
    assert_eq!(
        actual.field("name"),
        Some(&ignite_rs::protocol::complex_obj::IgniteValue::String(
            "Person 1".to_string()
        ))
    );

    fixture.cleanup(&ignite).await;
}

/// Related Apache Ignite keep-binary query-view coverage:
/// org.apache.ignite.client.IgniteBinaryQueryTest#testBinaryQueries
#[tokio::test]
async fn should_expose_keep_binary_cache_view_for_binary_sql_queries() {
    let ignite = connect().await.unwrap();
    let fixture = BinaryQueryFixture::create(&ignite).await.unwrap();
    fixture.put_person(&ignite, 7, "Person 7").await.unwrap();

    let cache = ignite
        .cache::<i32, ComplexObject>(&fixture.cache_name)
        .with_keep_binary();
    let rows = cache
        .sql_query(SqlQuery::new(&fixture.type_name, "id = 7"))
        .await
        .unwrap()
        .fetch_all()
        .await
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, Some(7));
    assert_eq!(rows[0].1.as_ref().unwrap().type_name(), &fixture.type_name);

    fixture.cleanup(&ignite).await;
}

struct BinaryQueryFixture {
    bootstrap_cache_name: String,
    bootstrap_cache: ignite_rs::cache::Cache<i32, i32>,
    table_name: String,
    cache_name: String,
    type_name: String,
    cache: ignite_rs::cache::Cache<i32, BinaryObject>,
}

impl BinaryQueryFixture {
    async fn create(ignite: &Client) -> IgniteResult<Self> {
        let bootstrap_cache_name = unique_name("__INT_TEST_BINARY_QUERY_BOOTSTRAP");
        let bootstrap_cache = ignite
            .get_or_create_cache::<i32, i32>(&bootstrap_cache_name)
            .await?;
        let table_name = unique_name("BINARY_QUERY_PERSON");
        let cache_name = unique_name("ignite_binary_query");
        let type_name = unique_name("IgniteBinaryQueryPerson");
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
