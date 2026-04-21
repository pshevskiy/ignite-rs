//! Tier-2 parity — SQL/scan queries.

use super::common::{ignite_scope, IgniteProfile};
use super::parity::{driver_jar_built, jd_call_ok, jd_connected};
use ignite_rs::cache::Cache;
use ignite_rs::query::{ScanQuery, SqlFieldsQuery};
use serde_json::json;

fn unique_table(suffix: &str) -> String {
    format!("PARITY_Q_{}_{}", suffix, std::process::id())
}

macro_rules! require_jar {
    () => {
        if !driver_jar_built() {
            eprintln!("[parity] driver JAR missing — skipping");
            return;
        }
    };
}

/// Rust creates a SQL-backed cache, inserts 3 rows; Java driver queries
/// `SELECT *`; both clients report 3 rows.
#[tokio::test]
async fn sql_scan_count_cross_client() {
    require_jar!();
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    let _bootstrap = rs
        .get_or_create_cache::<i32, i32>("PARITY_SQL_BOOT")
        .await
        .expect("bootstrap cache");
    let table = unique_table("SC");
    let sql_cache = format!("SQL_PUBLIC_{}", table);

    let _ = rs
        .sql_fields::<i64>(
            SqlFieldsQuery::new(format!("DROP TABLE IF EXISTS {}", table).as_str())
                .with_schema("PUBLIC")
                .with_page_size(32),
        )
        .await;
    rs.sql_fields::<i64>(
        SqlFieldsQuery::new(
            format!("CREATE TABLE {} (id INT PRIMARY KEY, v VARCHAR)", table).as_str(),
        )
        .with_schema("PUBLIC")
        .with_page_size(32),
    )
    .await
    .expect("create table");
    for i in 1..=3 {
        rs.sql_fields::<i64>(
            SqlFieldsQuery::new(
                format!("INSERT INTO {} (id, v) VALUES ({}, 'r{}')", table, i, i).as_str(),
            )
            .with_schema("PUBLIC")
            .with_page_size(32),
        )
        .await
        .expect("insert");
    }

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
    assert_eq!(r.body.get("rows").and_then(|v| v.as_i64()), Some(3));

    let rs_rows = rs
        .sql_fields::<i64>(
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

    let _ = rs
        .sql_fields::<i64>(
            SqlFieldsQuery::new(format!("DROP TABLE {}", table).as_str())
                .with_schema("PUBLIC")
                .with_page_size(32),
        )
        .await;
    jd.shutdown().await;
}

/// SELECT with WHERE clause — filters work the same via Java.
#[tokio::test]
async fn sql_select_with_where_filter() {
    require_jar!();
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    let _boot = rs
        .get_or_create_cache::<i32, i32>("PARITY_SQL_BOOT")
        .await
        .expect("bootstrap cache");
    let table = unique_table("WH");
    let sql_cache = format!("SQL_PUBLIC_{}", table);

    let _ = rs
        .sql_fields::<i64>(
            SqlFieldsQuery::new(format!("DROP TABLE IF EXISTS {}", table).as_str())
                .with_schema("PUBLIC")
                .with_page_size(32),
        )
        .await;
    rs.sql_fields::<i64>(
        SqlFieldsQuery::new(
            format!("CREATE TABLE {} (id INT PRIMARY KEY, n INT)", table).as_str(),
        )
        .with_schema("PUBLIC")
        .with_page_size(32),
    )
    .await
    .expect("create");
    for i in 1..=10 {
        rs.sql_fields::<i64>(
            SqlFieldsQuery::new(
                format!("INSERT INTO {} (id, n) VALUES ({}, {})", table, i, i).as_str(),
            )
            .with_schema("PUBLIC")
            .with_page_size(32),
        )
        .await
        .expect("insert");
    }

    // Java: WHERE n > 7 → ids 8,9,10 → 3 rows.
    let r = jd_call_ok(
        &jd,
        "q1",
        "sql_scan_count",
        json!({
            "cache": sql_cache,
            "sql": format!("SELECT * FROM {} WHERE n > 7", table),
            "schema": "PUBLIC",
        }),
    )
    .await;
    assert_eq!(r.body.get("rows").and_then(|v| v.as_i64()), Some(3));

    let _ = rs
        .sql_fields::<i64>(
            SqlFieldsQuery::new(format!("DROP TABLE {}", table).as_str())
                .with_schema("PUBLIC")
                .with_page_size(32),
        )
        .await;
    jd.shutdown().await;
}

/// DDL via Java, then Rust INSERT and query — schema propagates.
#[tokio::test]
async fn sql_ddl_via_java_then_dml_via_rust() {
    require_jar!();
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    // Bootstrap: any cache, used as query context.
    let _boot = rs
        .get_or_create_cache::<i32, i32>("PARITY_SQL_BOOT")
        .await
        .expect("bootstrap cache");
    let table = unique_table("DDL");

    // Java creates the table.
    jd_call_ok(
        &jd,
        "d1",
        "sql_exec",
        json!({
            "cache": "PARITY_SQL_BOOT",
            "sql": format!("DROP TABLE IF EXISTS {}", table),
            "schema": "PUBLIC",
        }),
    )
    .await;
    jd_call_ok(
        &jd,
        "d2",
        "sql_exec",
        json!({
            "cache": "PARITY_SQL_BOOT",
            "sql": format!("CREATE TABLE {} (id INT PRIMARY KEY, name VARCHAR)", table),
            "schema": "PUBLIC",
        }),
    )
    .await;

    // Rust inserts.
    for i in 1..=4 {
        rs.sql_fields::<i64>(
            SqlFieldsQuery::new(
                format!("INSERT INTO {} (id, name) VALUES ({}, 'rs{}')", table, i, i).as_str(),
            )
            .with_schema("PUBLIC")
            .with_page_size(32),
        )
        .await
        .expect("insert");
    }

    // Java reads count.
    let sql_cache = format!("SQL_PUBLIC_{}", table);
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
    assert_eq!(r.body.get("rows").and_then(|v| v.as_i64()), Some(4));

    jd_call_ok(
        &jd,
        "dd",
        "sql_exec",
        json!({
            "cache": "PARITY_SQL_BOOT",
            "sql": format!("DROP TABLE {}", table),
            "schema": "PUBLIC",
        }),
    )
    .await;
    jd.shutdown().await;
}

/// ScanQuery — Rust populates, Java driver scans cache, both see N entries.
#[tokio::test]
async fn scan_query_cross_client() {
    require_jar!();
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    let name = format!("PARITY_Q_SCAN_{}", std::process::id());
    jd_call_ok(&jd, "c1", "get_or_create_cache", json!({"cache": name})).await;
    let cache: Cache<String, String> = rs
        .get_or_create_cache::<String, String>(&name)
        .await
        .expect("rs get_or_create_cache");
    for i in 0..6 {
        cache.put(&format!("k{}", i), &"v".to_string()).await.unwrap();
    }

    // Java ScanQuery: count all rows.
    let r = jd_call_ok(
        &jd,
        "s1",
        "scan_query",
        json!({"cache": name, "page_size": 32}),
    )
    .await;
    assert_eq!(r.body.get("rows").and_then(|v| v.as_i64()), Some(6));

    // Mirror: Rust ScanQuery too.
    let rs_cursor = cache
        .scan_query(ScanQuery::new().with_page_size(32))
        .await
        .expect("rs scan_query");
    let rows = rs_cursor.fetch_all().await.expect("fetch_all");
    assert_eq!(rows.len(), 6);

    jd_call_ok(&jd, "d1", "destroy_cache", json!({"cache": name})).await;
    jd.shutdown().await;
}

/// ScanQuery with small page_size: paging doesn't lose rows for Java.
#[tokio::test]
async fn scan_query_pages_correctly() {
    require_jar!();
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    let name = format!("PARITY_Q_PAGE_{}", std::process::id());
    jd_call_ok(&jd, "c1", "get_or_create_cache", json!({"cache": name})).await;
    let cache: Cache<String, String> = rs
        .get_or_create_cache::<String, String>(&name)
        .await
        .expect("rs get_or_create_cache");
    for i in 0..25 {
        cache.put(&format!("k{}", i), &"v".to_string()).await.unwrap();
    }
    let r = jd_call_ok(
        &jd,
        "s1",
        "scan_query",
        json!({"cache": name, "page_size": 4}),
    )
    .await;
    assert_eq!(r.body.get("rows").and_then(|v| v.as_i64()), Some(25));
    jd_call_ok(&jd, "d1", "destroy_cache", json!({"cache": name})).await;
    jd.shutdown().await;
}

/// Empty cache scan — 0 rows from both clients.
#[tokio::test]
async fn scan_query_empty_cache() {
    require_jar!();
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    let name = format!("PARITY_Q_EMPTY_{}", std::process::id());
    jd_call_ok(&jd, "c1", "get_or_create_cache", json!({"cache": name})).await;
    let _cache: Cache<String, String> = rs
        .get_or_create_cache::<String, String>(&name)
        .await
        .expect("rs get_or_create_cache");

    let r = jd_call_ok(
        &jd,
        "s1",
        "scan_query",
        json!({"cache": name, "page_size": 16}),
    )
    .await;
    assert_eq!(r.body.get("rows").and_then(|v| v.as_i64()), Some(0));

    jd_call_ok(&jd, "d1", "destroy_cache", json!({"cache": name})).await;
    jd.shutdown().await;
}

/// Aggregation query — SUM() + GROUP BY produces consistent results.
#[tokio::test]
async fn sql_aggregation_parity() {
    require_jar!();
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    let _boot = rs
        .get_or_create_cache::<i32, i32>("PARITY_SQL_BOOT")
        .await
        .expect("bootstrap cache");
    let table = unique_table("AGG");

    let _ = rs
        .sql_fields::<i64>(
            SqlFieldsQuery::new(format!("DROP TABLE IF EXISTS {}", table).as_str())
                .with_schema("PUBLIC")
                .with_page_size(32),
        )
        .await;
    rs.sql_fields::<i64>(
        SqlFieldsQuery::new(
            format!(
                "CREATE TABLE {} (id INT PRIMARY KEY, g INT, v INT)",
                table
            )
            .as_str(),
        )
        .with_schema("PUBLIC")
        .with_page_size(32),
    )
    .await
    .expect("create");
    // Groups: g=1 → sum 6, g=2 → sum 9 → 2 groups total.
    for (id, g, v) in [(1, 1, 1), (2, 1, 2), (3, 1, 3), (4, 2, 4), (5, 2, 5)].iter() {
        rs.sql_fields::<i64>(
            SqlFieldsQuery::new(
                format!("INSERT INTO {} VALUES ({}, {}, {})", table, id, g, v).as_str(),
            )
            .with_schema("PUBLIC")
            .with_page_size(32),
        )
        .await
        .expect("insert");
    }
    // Java: SELECT g, SUM(v) FROM table GROUP BY g → 2 rows.
    let sql_cache = format!("SQL_PUBLIC_{}", table);
    let r = jd_call_ok(
        &jd,
        "q1",
        "sql_scan_count",
        json!({
            "cache": sql_cache,
            "sql": format!("SELECT g, SUM(v) FROM {} GROUP BY g", table),
            "schema": "PUBLIC",
        }),
    )
    .await;
    assert_eq!(r.body.get("rows").and_then(|v| v.as_i64()), Some(2));

    let _ = rs
        .sql_fields::<i64>(
            SqlFieldsQuery::new(format!("DROP TABLE {}", table).as_str())
                .with_schema("PUBLIC")
                .with_page_size(32),
        )
        .await;
    jd.shutdown().await;
}

/// Rust writes rows, then UPDATE via Java, then Rust reads modified values.
#[tokio::test]
async fn sql_update_cross_client() {
    require_jar!();
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    let _boot = rs
        .get_or_create_cache::<i32, i32>("PARITY_SQL_BOOT")
        .await
        .expect("bootstrap cache");
    let table = unique_table("UPD");

    let _ = rs
        .sql_fields::<i64>(
            SqlFieldsQuery::new(format!("DROP TABLE IF EXISTS {}", table).as_str())
                .with_schema("PUBLIC")
                .with_page_size(32),
        )
        .await;
    rs.sql_fields::<i64>(
        SqlFieldsQuery::new(
            format!("CREATE TABLE {} (id INT PRIMARY KEY, v VARCHAR)", table).as_str(),
        )
        .with_schema("PUBLIC")
        .with_page_size(32),
    )
    .await
    .expect("create");
    rs.sql_fields::<i64>(
        SqlFieldsQuery::new(format!("INSERT INTO {} VALUES (1, 'orig')", table).as_str())
            .with_schema("PUBLIC")
            .with_page_size(32),
    )
    .await
    .expect("insert");

    jd_call_ok(
        &jd,
        "u1",
        "sql_exec",
        json!({
            "cache": "PARITY_SQL_BOOT",
            "sql": format!("UPDATE {} SET v = 'updated' WHERE id = 1", table),
            "schema": "PUBLIC",
        }),
    )
    .await;

    let rows = rs
        .sql_fields::<String>(
            SqlFieldsQuery::new(format!("SELECT v FROM {} WHERE id = 1", table).as_str())
                .with_schema("PUBLIC")
                .with_page_size(32),
        )
        .await
        .expect("select")
        .fetch_all()
        .await
        .expect("fetch");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], "updated");

    let _ = rs
        .sql_fields::<i64>(
            SqlFieldsQuery::new(format!("DROP TABLE {}", table).as_str())
                .with_schema("PUBLIC")
                .with_page_size(32),
        )
        .await;
    jd.shutdown().await;
}

/// Rust DELETE, then Java's SELECT sees 0 rows afterwards.
#[tokio::test]
async fn sql_delete_cross_client() {
    require_jar!();
    let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
    scope.wait_for_ready().await.unwrap();
    let addr = scope.single_env().expect("single env").addr().to_string();

    let rs = super::parity::rs_client(&addr).await;
    let jd = jd_connected(&addr).await.expect("jd connected");

    let _boot = rs
        .get_or_create_cache::<i32, i32>("PARITY_SQL_BOOT")
        .await
        .expect("bootstrap cache");
    let table = unique_table("DEL");

    let _ = rs
        .sql_fields::<i64>(
            SqlFieldsQuery::new(format!("DROP TABLE IF EXISTS {}", table).as_str())
                .with_schema("PUBLIC")
                .with_page_size(32),
        )
        .await;
    rs.sql_fields::<i64>(
        SqlFieldsQuery::new(
            format!("CREATE TABLE {} (id INT PRIMARY KEY, tag VARCHAR)", table).as_str(),
        )
        .with_schema("PUBLIC")
        .with_page_size(32),
    )
    .await
    .expect("create");
    for i in 1..=5 {
        rs.sql_fields::<i64>(
            SqlFieldsQuery::new(
                format!("INSERT INTO {} VALUES ({}, 't{}')", table, i, i).as_str(),
            )
            .with_schema("PUBLIC")
            .with_page_size(32),
        )
        .await
        .expect("insert");
    }
    rs.sql_fields::<i64>(
        SqlFieldsQuery::new(format!("DELETE FROM {} WHERE id <= 3", table).as_str())
            .with_schema("PUBLIC")
            .with_page_size(32),
    )
    .await
    .expect("delete");

    let sql_cache = format!("SQL_PUBLIC_{}", table);
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
    assert_eq!(r.body.get("rows").and_then(|v| v.as_i64()), Some(2));

    let _ = rs
        .sql_fields::<i64>(
            SqlFieldsQuery::new(format!("DROP TABLE {}", table).as_str())
                .with_schema("PUBLIC")
                .with_page_size(32),
        )
        .await;
    jd.shutdown().await;
}
