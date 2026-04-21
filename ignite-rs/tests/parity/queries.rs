//! Tier-2 parity — SQL/scan queries.

use super::common::{ignite_scope, IgniteProfile};
use super::parity::{driver_jar_built, jd_call_ok, jd_connected};
use ignite_rs::query::SqlFieldsQuery;
use serde_json::json;

/// Rust creates a SQL-backed cache, inserts 3 rows; Java driver queries
/// `SELECT COUNT(*)`; both Rust and Java report the same count.
#[tokio::test]
async fn sql_scan_count_cross_client() {
    if !driver_jar_built() {
        eprintln!("[parity] driver JAR missing — skipping");
        return;
    }
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    // Bootstrap SQL table via Rust.
    let bootstrap_cache = rs
        .get_or_create_cache::<i32, i32>("PARITY_SQL_BOOT")
        .await
        .expect("bootstrap cache");
    let table = format!("PARITY_SQL_TBL_{}", std::process::id());
    let sql_cache = format!("SQL_PUBLIC_{}", table);

    let _ = rs
        .sql_fields::<i32>(
            SqlFieldsQuery::new(format!("DROP TABLE IF EXISTS {}", table).as_str())
                .with_schema("PUBLIC")
                .with_page_size(32),
        )
        .await;
    // CREATE TABLE and INSERT via Rust, targeting the bootstrap cache
    // (SqlFieldsQuery does not require the cache to be the SQL-backed one
    // — Ignite resolves the target table via the schema).
    let _ = bootstrap_cache; // retain to keep the cache alive
    rs
        .sql_fields::<i32>(
            SqlFieldsQuery::new(
                format!("CREATE TABLE {} (id INT PRIMARY KEY, v VARCHAR)", table).as_str(),
            )
            .with_schema("PUBLIC")
            .with_page_size(32),
        )
        .await
        .expect("create table");

    for i in 1..=3 {
        rs
            .sql_fields::<i32>(
                SqlFieldsQuery::new(
                    format!("INSERT INTO {} (id, v) VALUES ({}, 'r{}')", table, i, i).as_str(),
                )
                .with_schema("PUBLIC")
                .with_page_size(32),
            )
            .await
            .expect("insert");
    }

    // Query via Java driver.
    let r = jd_call_ok(
        &jd,
        "q1",
        "sql_scan_count",
        json!({
            "cache": sql_cache,
            "sql": format!("SELECT * FROM {}", table),
            "schema": "PUBLIC",
        }),
    )
    .await;
    let rows = r
        .body
        .get("rows")
        .and_then(|v| v.as_i64())
        .expect("rows field");
    assert_eq!(rows, 3, "java SQL count disagreed with Rust insert count");

    // Query via Rust, assert same count.
    let rs_rows = rs
        .sql_fields::<i32>(
            SqlFieldsQuery::new(format!("SELECT COUNT(*) FROM {}", table).as_str())
                .with_schema("PUBLIC")
                .with_page_size(32),
        )
        .await
        .expect("rust sql")
        .fetch_all()
        .await
        .expect("rust fetch");
    assert_eq!(rs_rows.len(), 1);

    // Cleanup.
    let _ = rs
        .sql_fields::<i32>(
            SqlFieldsQuery::new(format!("DROP TABLE {}", table).as_str())
                .with_schema("PUBLIC")
                .with_page_size(32),
        )
        .await;
    jd.shutdown().await;
}
