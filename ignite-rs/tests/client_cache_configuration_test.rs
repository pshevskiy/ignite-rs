#![cfg(not(feature = "ssl"))]

mod common;

use common::{connect, destroy_cache_if_exists, unique_name, SqlTableFixture};
use ignite_rs::cache::{
    AtomicityMode, CacheConfiguration, CacheKeyConfiguration, CacheMode, IndexType,
    PartitionLossPolicy, QueryEntity, QueryField, QueryIndex, RebalanceMode,
    WriteSynchronizationMode,
};
use ignite_rs::protocol::complex_obj::{ComplexObjectSchema, IgniteField, IgniteType};

/// Additional live cache-configuration round-trip coverage over a real Ignite node.
#[tokio::test]
async fn should_round_trip_cache_configuration() {
    let ignite = connect().await.unwrap();
    let cache_name = unique_name("testCacheConfiguration");
    destroy_cache_if_exists(&ignite, &cache_name).await;

    let mut config = CacheConfiguration::new(&cache_name);
    config.atomicity_mode = AtomicityMode::Transactional;
    config.num_backup = 1;
    config.cache_mode = CacheMode::Partitioned;
    config.copy_on_read = false;
    config.eager_ttl = false;
    config.statistics_enabled = true;
    config.group_name = Some("FunctionalTest".to_string());
    config.default_lock_timeout_ms = 12_345;
    config.max_concurrent_async_operations = 4;
    config.max_query_iterators = 4;
    config.onheap_cache_enabled = true;
    config.partition_loss_policy = PartitionLossPolicy::ReadWriteSafe;
    config.query_detail_metrics_size = 1_024;
    config.query_parallelism = 4;
    config.read_from_backup = true;
    config.rebalance_batch_size = 67_890;
    config.rebalance_batches_prefetch_count = 102_938;
    config.rebalance_delay_ms = 54_321;
    config.rebalance_mode = RebalanceMode::Sync;
    config.rebalance_order = 2;
    config.rebalance_throttle_ms = 564_738;
    config.rebalance_timeout_ms = 142_536;
    config.sql_escape_all = true;
    config.sql_index_max_size = 1_024;
    config.sql_schema = Some("functional_test_schema".to_string());
    config.write_synchronization_mode = WriteSynchronizationMode::FullSync;
    config.cache_key_configurations = Some(vec![CacheKeyConfiguration::new("Employee", "orgId")]);
    config.query_entities = Some(vec![QueryEntity::new("java.lang.Integer", "Employee")
        .set_table_name("EMPLOYEE")
        .set_key_field_name("id")
        .add_query_field(
            QueryField::new("id", "java.lang.Integer")
                .set_key_field(true)
                .set_not_null_constraint(true),
        )
        .add_query_field(QueryField::new("orgId", "java.lang.Integer"))
        .add_field_alias("id", "ID")
        .add_field_alias("orgId", "ORGID")
        .add_query_index(
            QueryIndex::new("IDX_EMPLOYEE_ID", IndexType::Sorted).add_field("id", false),
        )]);

    let cache = ignite
        .create_cache_with_config::<i32, i32>(&config)
        .await
        .unwrap();
    assert_eq!(cache.name(), cache_name.as_str());

    let actual = ignite.get_cache_config(&cache_name).await.unwrap();
    let entity = actual
        .query_entities
        .as_ref()
        .and_then(|entities| entities.first())
        .unwrap();

    assert_eq!(actual.atomicity_mode, config.atomicity_mode);
    assert_eq!(actual.num_backup, config.num_backup);
    assert_eq!(actual.cache_mode, config.cache_mode);
    assert_eq!(actual.copy_on_read, config.copy_on_read);
    assert_eq!(actual.eager_ttl, config.eager_ttl);
    assert_eq!(actual.group_name, config.group_name);
    assert_eq!(
        actual.default_lock_timeout_ms,
        config.default_lock_timeout_ms
    );
    assert_eq!(
        actual.max_concurrent_async_operations,
        config.max_concurrent_async_operations
    );
    assert_eq!(actual.max_query_iterators, config.max_query_iterators);
    assert_eq!(actual.onheap_cache_enabled, config.onheap_cache_enabled);
    assert_eq!(actual.partition_loss_policy, config.partition_loss_policy);
    assert_eq!(
        actual.query_detail_metrics_size,
        config.query_detail_metrics_size
    );
    assert_eq!(actual.query_parallelism, config.query_parallelism);
    assert_eq!(actual.read_from_backup, config.read_from_backup);
    assert_eq!(actual.rebalance_batch_size, config.rebalance_batch_size);
    assert_eq!(
        actual.rebalance_batches_prefetch_count,
        config.rebalance_batches_prefetch_count
    );
    assert_eq!(actual.rebalance_delay_ms, config.rebalance_delay_ms);
    assert_eq!(actual.rebalance_mode, config.rebalance_mode);
    assert_eq!(actual.rebalance_order, config.rebalance_order);
    assert_eq!(actual.rebalance_throttle_ms, config.rebalance_throttle_ms);
    assert_eq!(actual.rebalance_timeout_ms, config.rebalance_timeout_ms);
    assert_eq!(actual.sql_escape_all, config.sql_escape_all);
    assert_eq!(actual.sql_index_max_size, config.sql_index_max_size);
    assert_eq!(actual.sql_schema, config.sql_schema);
    assert_eq!(
        actual.write_synchronization_mode,
        config.write_synchronization_mode
    );
    assert_eq!(
        actual.cache_key_configurations,
        config.cache_key_configurations
    );
    assert_eq!(entity.key_type(), "java.lang.Integer");
    assert_eq!(entity.value_type(), "Employee");
    assert_eq!(entity.table_name(), "EMPLOYEE");
    assert_eq!(entity.key_field_name(), "id");
    assert_eq!(entity.query_fields().len(), 2);
    assert_eq!(entity.query_fields()[0].name(), "id");
    assert_eq!(entity.query_fields()[0].type_name(), "java.lang.Integer");
    assert!(entity.query_fields()[0].is_key_field());
    assert!(entity.query_fields()[0].has_not_null_constraint());
    assert_eq!(entity.query_fields()[1].name(), "orgId");
    assert_eq!(entity.field_aliases().len(), 2);
    let mut sorted_aliases = entity.field_aliases().to_vec();
    sorted_aliases.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(sorted_aliases[0], ("id".to_string(), "ID".to_string()));
    assert_eq!(
        sorted_aliases[1],
        ("orgId".to_string(), "ORGID".to_string())
    );
    assert_eq!(entity.query_indexes().len(), 1);
    assert_eq!(entity.query_indexes()[0].index_name(), "IDX_EMPLOYEE_ID");
    assert_eq!(entity.query_indexes()[0].index_type(), &IndexType::Sorted);
    assert_eq!(
        entity.query_indexes()[0].fields(),
        &[("id".to_string(), false)]
    );

    ignite.destroy_cache(&cache_name).await.unwrap();
}

/// Additional live schema-inference coverage for SQL-created cache configuration.
#[tokio::test]
async fn should_read_schema_from_sql_cache_configuration() {
    let ignite = connect().await.unwrap();
    let fixture = SqlTableFixture::create_seeded(&ignite).await.unwrap();

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
                },
            ],
        }
    );
}

/// Java parity: org.apache.ignite.client.ClientCacheConfigurationTest#testSerialization
#[test]
fn should_clone_cache_configuration_losslessly() {
    let mut config = CacheConfiguration::new("Person");
    config.atomicity_mode = AtomicityMode::Transactional;
    config.num_backup = 3;
    config.cache_mode = CacheMode::Partitioned;
    config.write_synchronization_mode = WriteSynchronizationMode::FullSync;
    config.eager_ttl = false;
    config.group_name = Some("FunctionalTest".to_string());
    config.default_lock_timeout_ms = 12_345;
    config.partition_loss_policy = PartitionLossPolicy::ReadWriteAll;
    config.read_from_backup = true;
    config.rebalance_batch_size = 67_890;
    config.rebalance_batches_prefetch_count = 102_938;
    config.rebalance_delay_ms = 54_321;
    config.rebalance_mode = RebalanceMode::Sync;
    config.rebalance_order = 2;
    config.rebalance_throttle_ms = 564_738;
    config.rebalance_timeout_ms = 142_536;
    config.cache_key_configurations = Some(vec![CacheKeyConfiguration::new("Employee", "orgId")]);
    config.query_entities = Some(vec![QueryEntity::new("java.lang.Integer", "Employee")
        .set_table_name("EMPLOYEE")
        .set_key_field_name("id")
        .add_query_field(
            QueryField::new("id", "java.lang.Integer")
                .set_key_field(true)
                .set_not_null_constraint(true),
        )
        .add_query_field(QueryField::new("orgId", "java.lang.Integer"))
        .add_field_alias("id", "ID")
        .add_field_alias("orgId", "ORGID")
        .add_query_index(
            QueryIndex::new("IDX_EMPLOYEE_ID", IndexType::Sorted).add_field("id", false),
        )]);

    assert_eq!(config.clone(), config);
}

/// Java parity: org.apache.ignite.client.ClientCacheConfigurationTest#testDifferentSizeCacheConfiguration
#[tokio::test]
async fn should_fetch_cache_configuration_for_many_different_name_lengths() {
    let ignite = connect().await.unwrap();
    let mut cache_names = Vec::new();
    let mut current = String::new();

    for _ in 0..32 {
        current.push('a');
        let cache_name = format!("{}_{}", unique_name("cfg_len"), current);
        let mut config = CacheConfiguration::new(&cache_name);
        config.group_name = Some("CacheGroupName".to_string());
        config.query_entities = Some(vec![QueryEntity::new("java.lang.Integer", "QueryEntity0")
            .set_table_name("ENTITY0")
            .add_query_field(QueryField::new("id", "java.lang.Integer"))
            .add_query_field(QueryField::new("name", "java.lang.String"))]);
        ignite
            .create_cache_with_config::<i32, i32>(&config)
            .await
            .unwrap();
        cache_names.push(cache_name);
    }

    for cache_name in &cache_names {
        let config = ignite.get_cache_config(cache_name).await.unwrap();
        assert_eq!(&config.name, cache_name);
        assert_eq!(config.group_name.as_deref(), Some("CacheGroupName"));
    }

    for cache_name in cache_names {
        let _ = ignite.destroy_cache(&cache_name).await;
    }
}
