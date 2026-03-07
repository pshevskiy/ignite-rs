#[cfg(test)]
mod int_test {
    use ignite_rs::cache::Cache;
    use ignite_rs::error::{IgniteError, IgniteResult};
    use ignite_rs::protocol::complex_obj::{
        ComplexObject, ComplexObjectSchema, IgniteField, IgniteType, IgniteValue,
    };
    use ignite_rs::{new_client, Client, ClientConfig};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    const IGNITE_ADDR: &str = "127.0.0.1:10800";
    const SQL_SCHEMA: &str = "PUBLIC";
    const SQL_PAGE_SIZE: i32 = 32;
    static NEXT_FIXTURE_ID: AtomicUsize = AtomicUsize::new(0);

    struct RainbowFixture {
        bootstrap_cache_name: String,
        bootstrap_cache: Cache<i32, i32>,
        table_name: String,
        cache_name: String,
    }

    impl RainbowFixture {
        async fn create(client: &Client) -> IgniteResult<Self> {
            let fixture_id = NEXT_FIXTURE_ID.fetch_add(1, Ordering::Relaxed);
            let bootstrap_cache_name = format!(
                "__INT_TEST_SQL_BOOTSTRAP_{}_{}",
                std::process::id(),
                fixture_id
            );
            let bootstrap_cache = client
                .get_or_create_cache::<i32, i32>(&bootstrap_cache_name)
                .await?;
            let table_name = format!("RAINBOW_{}_{}", std::process::id(), fixture_id);
            let cache_name = format!("SQL_PUBLIC_{}", table_name);

            execute_sql(
                &bootstrap_cache,
                &format!(
                    "CREATE TABLE {} (\
                         big BIGINT,\
                         bool BOOLEAN,\
                         dec DECIMAL,\
                         int INT,\
                         null_int INT,\
                         small SMALLINT,\
                         char CHAR,\
                         var VARCHAR,\
                         ts TIMESTAMP,\
                         PRIMARY KEY (big)\
                     )",
                    table_name
                ),
            )
            .await?;

            execute_sql(
                &bootstrap_cache,
                &format!(
                    "INSERT INTO {} (big, bool, dec, int, null_int, small, char, var, ts) \
                     VALUES (1, true, 2.0, 3, null, 4, 'c', 'varchar', \
                     timestamp '2023-06-21 12:34:56 UTC')",
                    table_name
                ),
            )
            .await?;

            let row_count = execute_sql(
                &bootstrap_cache,
                &format!("SELECT COUNT(*) FROM {}", table_name),
            )
            .await?;
            if row_count.as_slice() != [1] {
                return Err(IgniteError::from(
                    format!("fixture seed failed for {}", table_name).as_str(),
                ));
            }

            Ok(Self {
                bootstrap_cache_name,
                bootstrap_cache,
                table_name,
                cache_name,
            })
        }

        async fn cleanup(&self, client: &Client) {
            let _ = execute_sql(
                &self.bootstrap_cache,
                &format!("DROP TABLE {}", self.table_name),
            )
            .await;
            let _ = client.destroy_cache(&self.bootstrap_cache_name).await;
        }
    }

    async fn connect() -> IgniteResult<Client> {
        new_client(ClientConfig::new(IGNITE_ADDR)).await
    }

    async fn execute_sql(cache: &Cache<i32, i32>, sql: &str) -> IgniteResult<Vec<i64>> {
        cache
            .query_sql_fields_long_fetch_all_with_args_schema(SQL_SCHEMA, sql, SQL_PAGE_SIZE, &[])
            .await
    }

    #[test]
    fn sanity_test() {
        assert_eq!(true, true, "CI works");
    }

    #[tokio::test]
    async fn should_list_caches() {
        let ignite = connect().await.unwrap();
        let fixture = RainbowFixture::create(&ignite).await.unwrap();

        let actual = ignite.get_cache_names().await.unwrap();
        fixture.cleanup(&ignite).await;

        assert!(
            actual.iter().any(|name| name == &fixture.cache_name),
            "expected cache list to contain {} but got {:?}",
            fixture.cache_name,
            actual
        );
    }

    #[tokio::test]
    async fn should_read_schema() {
        let ignite = connect().await.unwrap();
        let fixture = RainbowFixture::create(&ignite).await.unwrap();

        let cfg = ignite.get_cache_config(&fixture.cache_name).await.unwrap();
        let entities = cfg.query_entities.unwrap();
        let entity = entities.last().unwrap().clone();
        fixture.cleanup(&ignite).await;

        assert_eq!(entities.len(), 1);
        let (ks, vs) = ComplexObjectSchema::infer_schemas(&entity).unwrap();

        assert_eq!(
            *ks,
            ComplexObjectSchema {
                type_name: "java.lang.Long".to_string(),
                fields: vec![IgniteField {
                    name: "BIG".to_string(),
                    r#type: IgniteType::Long
                }]
            }
        );

        assert_eq!(
            *vs,
            ComplexObjectSchema {
                type_name: vs.type_name().to_string(),
                fields: vec![
                    IgniteField {
                        name: "BOOL".to_string(),
                        r#type: IgniteType::Bool
                    },
                    IgniteField {
                        name: "DEC".to_string(),
                        r#type: IgniteType::Decimal(-1, -1)
                    },
                    IgniteField {
                        name: "INT".to_string(),
                        r#type: IgniteType::Int
                    },
                    IgniteField {
                        name: "NULL_INT".to_string(),
                        r#type: IgniteType::Int
                    },
                    IgniteField {
                        name: "SMALL".to_string(),
                        r#type: IgniteType::Short
                    },
                    IgniteField {
                        name: "CHAR".to_string(),
                        r#type: IgniteType::String
                    },
                    IgniteField {
                        name: "VAR".to_string(),
                        r#type: IgniteType::String
                    },
                    IgniteField {
                        name: "TS".to_string(),
                        r#type: IgniteType::Timestamp
                    }
                ]
            }
        );
    }

    #[tokio::test]
    async fn should_read_data() {
        let ignite = connect().await.unwrap();
        let fixture = RainbowFixture::create(&ignite).await.unwrap();

        // read a row
        let cache = ignite
            .get_or_create_cache::<ComplexObject, ComplexObject>(&fixture.cache_name)
            .await
            .unwrap();
        let actual = cache.query_scan(100).await.unwrap();
        fixture.cleanup(&ignite).await;
        let expected = vec![(
            Some(ComplexObject {
                schema: Arc::new(ComplexObjectSchema {
                    type_name: "".to_string(),
                    fields: vec![],
                }),
                values: vec![IgniteValue::Long(1)],
            }),
            Some(ComplexObject {
                schema: Arc::new(ComplexObjectSchema {
                    type_name: "".to_string(),
                    fields: vec![],
                }),
                values: vec![
                    IgniteValue::Bool(true),
                    IgniteValue::Decimal(1, vec![20]),
                    IgniteValue::Int(3),
                    IgniteValue::Null,
                    IgniteValue::Short(4),
                    IgniteValue::String("c".to_string()),
                    IgniteValue::String("varchar".to_string()),
                    IgniteValue::Timestamp(1687350896000, 0),
                ],
            }),
        )];
        assert_eq!(actual, expected);
    }
}
