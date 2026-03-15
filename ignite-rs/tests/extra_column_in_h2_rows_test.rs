#![cfg(not(feature = "ssl"))]

mod common;

use common::{connect, SqlTableFixture, SQL_SCHEMA};
use ignite_rs::query::{SqlFieldsQuery, SqlValue};

/// Java parity: org.apache.ignite.client.thin.ExtraColumnInH2RowsTest#testExtraColumnIgnored
#[tokio::test]
async fn should_ignore_extra_h2_columns_when_decoding_visible_sql_fields() {
    let ignite = connect().await.unwrap();
    let fixture = SqlTableFixture::create_seeded(&ignite).await.unwrap();

    let rows = fixture
        .bootstrap_cache
        .sql_fields(
            SqlFieldsQuery::<Vec<SqlValue>>::new(&format!(
                "SELECT _key, _val FROM {} ORDER BY big ASC",
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

    let empty_rows = fixture
        .bootstrap_cache
        .sql_fields(
            SqlFieldsQuery::<Vec<SqlValue>>::new(&format!(
                "SELECT _key, _val FROM {} WHERE var IS NULL ORDER BY big ASC",
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

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].len(), 2);
    assert!(matches!(rows[0][0], SqlValue::ComplexObject(_)));
    assert!(matches!(rows[0][1], SqlValue::ComplexObject(_)));
    assert!(empty_rows.is_empty());
}
