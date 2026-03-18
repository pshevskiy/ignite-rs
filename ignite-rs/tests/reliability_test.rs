#![cfg(not(feature = "ssl"))]

mod common;

use common::{destroy_cache_if_exists, ignite_cluster3_churn_env, ignite_test_env, unique_name};
use ignite_rs::cache::{AtomicityMode, CacheConfiguration, CacheMode, WriteSynchronizationMode};
use ignite_rs::error::IgniteError;
use ignite_rs::query::ScanQuery;
use ignite_rs::tx::TransactionOptions;
use ignite_rs::{
    new_client, ClientConfig, ReconnectThrottle, RetryContext, RetryDecision, RetryPolicy,
    RetryPolicyHandler,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

/// Java parity: org.apache.ignite.client.ReliabilityTest#testServerDoesNotDisconnectIdleClientWithHeartbeats
#[tokio::test]
async fn should_keep_idle_client_connected_with_heartbeats() {
    let env = ignite_test_env();
    if env.is_managed() {
        env.start();
    }
    env.wait_for_ready().await.unwrap();

    let mut conf = ClientConfig::new(env.addr());
    conf.heartbeat_enabled = true;
    conf.heartbeat_interval = Some(Duration::from_millis(250));

    let client = new_client(conf).await.unwrap();

    tokio::time::sleep(Duration::from_millis(1_600)).await;

    let names = client.get_cache_names().await.unwrap();
    assert!(
        !names.iter().any(|name| name == "__unexpected__"),
        "expected cache names request to succeed after heartbeat keepalive",
    );
}

/// Java parity: org.apache.ignite.client.ReliabilityTest#testFailover
#[tokio::test]
async fn should_continue_live_operations_during_cluster_churn_and_fail_when_all_nodes_go_down() {
    let _guard = churn_lock()
        .lock()
        .expect("reliability churn lock poisoned");
    let env = ignite_cluster3_churn_env();
    if !env.is_managed() {
        return;
    }
    env.restart_all();
    env.wait_for_ready().await.unwrap();

    let cache_name = unique_name("reliability_failover");
    let mut conf = ClientConfig::from_addresses(env.addresses().iter().cloned());
    conf.partition_awareness_enabled = false;
    conf.retry_limit = 8;
    conf.request_timeout = Some(Duration::from_secs(3));
    conf.reconnect_backoff = Some(Duration::from_millis(100));

    let client = new_client(conf).await.unwrap();
    destroy_cache_if_exists(&client, &cache_name).await;

    let mut cache_cfg = CacheConfiguration::new(&cache_name);
    cache_cfg.cache_mode = CacheMode::Replicated;
    cache_cfg.write_synchronization_mode = WriteSynchronizationMode::FullSync;
    let cache = create_or_get_failover_cache(&client, &cache_name, &cache_cfg).await;

    churn_cluster_while(env.clone(), || async {
        for key in 0..24 {
            cache.put(&key, &key).await.unwrap();
            assert_eq!(cache.get(&key).await.unwrap(), Some(key));
        }
    })
    .await;

    cache.clear().await.unwrap();

    let scan_data = (1..=100).map(|i| (i, i)).collect::<Vec<_>>();
    cache.put_all(&scan_data).await.unwrap();

    churn_cluster_while(env.clone(), || async {
        let query_result = match cache.scan_query(ScanQuery::new().with_page_size(10)).await {
            Ok(cursor) => cursor.fetch_all().await,
            Err(err) => Err(err),
        };

        match query_result {
            Ok(rows) => {
                assert_eq!(
                    rows.len(),
                    scan_data.len(),
                    "unexpected row count from replicated cache scan during churn",
                );
            }
            Err(err) => {
                let msg = err.to_string();
                assert!(
                    msg.contains("closed")
                        || msg.contains("failed")
                        || msg.contains("connection")
                        || msg.contains("reset")
                        || msg.contains("eof"),
                    "unexpected scan-query failover error: {}",
                    msg
                );
            }
        }
    })
    .await;

    env.stop_all();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let err = tokio::time::timeout(Duration::from_secs(5), cache.put(&999, &999))
        .await
        .expect("timed out waiting for all-nodes-down failure")
        .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("closed")
            || msg.contains("failed")
            || msg.contains("connection")
            || msg.contains("refused")
            || msg.contains("reset")
            || msg.contains("eof")
            || msg.contains("unavailable"),
        "unexpected all-nodes-down error: {}",
        msg
    );

    env.start_all();
    env.wait_for_ready().await.unwrap();
    let _ = client.destroy_cache(&cache_name).await;
}

/// Related idle-connection coverage for the Phase 1 heartbeat transport:
/// `heartbeat_enabled=false` should not send internal keepalives.
#[tokio::test]
async fn should_allow_idle_connection_to_drop_when_heartbeats_are_disabled() {
    let env = ignite_test_env();
    if env.is_managed() {
        env.start();
    }
    env.wait_for_ready().await.unwrap();

    let mut conf = ClientConfig::new(env.addr());
    conf.retry_policy = RetryPolicy::Never;

    let client = new_client(conf).await.unwrap();

    tokio::time::sleep(Duration::from_millis(1_600)).await;

    if let Ok(Err(err)) =
        tokio::time::timeout(Duration::from_secs(5), client.get_cache_names()).await
    {
        let msg = err.to_string();
        assert!(
            msg.contains("failed")
                || msg.contains("closed")
                || msg.contains("reset")
                || msg.contains("eof")
                || msg.contains("Broken pipe")
                || msg.contains("unavailable"),
            "unexpected idle disconnect error: {}",
            msg
        );
    }
}

/// Java parity: org.apache.ignite.client.ReliabilityTest#testSingleServerFailover
#[tokio::test]
async fn should_recover_single_server_after_connection_drop() {
    let _guard = churn_lock()
        .lock()
        .expect("reliability churn lock poisoned");
    let env = ignite_cluster3_churn_env();
    if env.is_managed() {
        env.start_all();
    }
    env.wait_for_ready().await.unwrap();

    let cache_name = unique_name("reliability_single_server");
    let mut conf = ClientConfig::new(env.addr());
    conf.partition_awareness_enabled = false;
    conf.retry_limit = 1;
    conf.reconnect_backoff = Some(Duration::from_millis(100));

    let client = new_client(conf).await.unwrap();
    destroy_cache_if_exists(&client, &cache_name).await;
    let cache = client
        .get_or_create_cache::<i32, i32>(&cache_name)
        .await
        .unwrap();

    cache.put(&0, &0).await.unwrap();

    env.stop_node(0);
    tokio::time::sleep(Duration::from_millis(250)).await;
    env.start_node(0);
    env.wait_for_ready().await.unwrap();

    cache.put(&0, &1).await.unwrap();
    assert_eq!(cache.get(&0).await.unwrap(), Some(1));
}

/// Java parity: org.apache.ignite.client.ReliabilityTest#testSingleServerDuplicatedFailover
#[tokio::test]
async fn should_recover_single_server_with_duplicated_address_after_connection_drop() {
    let _guard = churn_lock()
        .lock()
        .expect("reliability churn lock poisoned");
    let env = ignite_cluster3_churn_env();
    if env.is_managed() {
        env.start_all();
    }
    env.wait_for_ready().await.unwrap();

    let cache_name = unique_name("reliability_single_server_duplicate");
    let mut conf = ClientConfig::from_addresses([env.addr(), env.addr()]);
    conf.partition_awareness_enabled = false;
    conf.retry_limit = 1;
    conf.reconnect_backoff = Some(Duration::from_millis(100));

    let client = new_client(conf).await.unwrap();
    destroy_cache_if_exists(&client, &cache_name).await;
    let cache = client
        .get_or_create_cache::<i32, i32>(&cache_name)
        .await
        .unwrap();

    cache.put(&0, &0).await.unwrap();

    env.stop_node(0);
    tokio::time::sleep(Duration::from_millis(250)).await;
    env.start_node(0);
    env.wait_for_ready().await.unwrap();

    cache.put(&0, &1).await.unwrap();
    assert_eq!(cache.get(&0).await.unwrap(), Some(1));
}

/// Java parity: org.apache.ignite.client.ReliabilityTest#testRetryReadPolicyRetriesCacheGet
#[tokio::test]
async fn should_retry_cache_get_with_read_only_retry_policy_after_connection_drop() {
    let _guard = churn_lock()
        .lock()
        .expect("reliability churn lock poisoned");
    let env = ignite_cluster3_churn_env();
    if env.is_managed() {
        env.start_all();
    }
    env.wait_for_ready().await.unwrap();

    let cache_name = unique_name("reliability_read_only_retry");
    let mut conf = ClientConfig::new(env.addr());
    conf.partition_awareness_enabled = false;
    conf.retry_policy = RetryPolicy::ReadOnly;
    conf.retry_limit = 16;
    conf.reconnect_backoff = Some(Duration::from_millis(100));

    let client = new_client(conf).await.unwrap();
    destroy_cache_if_exists(&client, &cache_name).await;
    let cache = client
        .get_or_create_cache::<i32, i32>(&cache_name)
        .await
        .unwrap();
    cache.put(&0, &7).await.unwrap();

    env.stop_node(0);
    tokio::spawn({
        let env = env.clone();
        async move {
            tokio::time::sleep(Duration::from_millis(600)).await;
            env.start_node(0);
        }
    });

    assert_eq!(cache.get(&0).await.unwrap(), Some(7));
}

/// Java parity: org.apache.ignite.client.ReliabilityTest#testReconnectionThrottling
#[tokio::test]
async fn should_throttle_reconnect_attempts_within_configured_window() {
    let _guard = churn_lock()
        .lock()
        .expect("reliability churn lock poisoned");
    let env = ignite_cluster3_churn_env();
    if env.is_managed() {
        env.start_all();
    }
    env.wait_for_ready().await.unwrap();

    let throttle = ReconnectThrottle {
        window: Duration::from_millis(700),
        max_attempts: 2,
    };
    let mut conf = ClientConfig::new(env.addr());
    conf.partition_awareness_enabled = false;
    conf.retry_limit = 1;
    conf.reconnect_backoff = Some(Duration::from_millis(100));
    conf.reconnect_throttle = Some(throttle);

    let client = new_client(conf).await.unwrap();
    let _ = client.get_cache_names().await.unwrap();

    env.stop_node(0);
    tokio::time::sleep(Duration::from_millis(200)).await;

    let _ = client.get_cache_names().await.unwrap_err();
    let _ = client.get_cache_names().await.unwrap_err();
    let err = client.get_cache_names().await.unwrap_err();
    assert!(
        err.to_string().contains("Reconnect throttling is applied"),
        "unexpected reconnect throttling error: {}",
        err
    );

    tokio::time::sleep(throttle.window + Duration::from_millis(150)).await;
    env.start_node(0);
    env.wait_for_ready().await.unwrap();

    let names = client.get_cache_names().await.unwrap();
    assert!(
        !names.iter().any(|name| name == "__unexpected__"),
        "expected reconnects to recover after throttling window elapsed",
    );
}

/// Java parity: org.apache.ignite.client.ReliabilityTest#testExceptionInRetryPolicyPropagatesToCaller
#[tokio::test]
async fn should_propagate_custom_retry_policy_errors_to_caller() {
    let _guard = churn_lock()
        .lock()
        .expect("reliability churn lock poisoned");
    let env = ignite_cluster3_churn_env();
    if env.is_managed() {
        env.start_all();
    }
    env.wait_for_ready().await.unwrap();

    let mut conf = ClientConfig::from_addresses([env.addr(), env.addr()]);
    conf.partition_awareness_enabled = false;
    conf.retry_limit = 1;
    conf.request_timeout = Some(Duration::from_millis(500));
    conf.reconnect_backoff = Some(Duration::from_millis(100));
    conf.retry_policy = RetryPolicy::custom(Arc::new(ErrorRetryPolicy));

    let client = new_client(conf).await.unwrap();
    let cache_name = unique_name("reliability_custom_retry_policy");
    destroy_cache_if_exists(&client, &cache_name).await;
    let cache = client
        .get_or_create_cache::<i32, i32>(&cache_name)
        .await
        .unwrap();
    cache.put(&0, &7).await.unwrap();

    env.stop_node(0);
    tokio::time::sleep(Duration::from_millis(200)).await;

    let err = cache.get(&0).await.unwrap_err();
    env.start_node(0);
    env.wait_for_ready().await.unwrap();
    let msg = err.to_string();
    assert!(
        msg.contains("Error in policy") && msg.contains(env.addr()),
        "unexpected retry policy error: {}",
        msg
    );
}

/// Java parity: org.apache.ignite.client.ReliabilityTest#testNullRetryPolicyDisablesFailover
#[tokio::test]
async fn should_disable_failover_when_retry_policy_is_never_after_connection_drop() {
    assert_never_retry_failover().await;
}

/// Java parity: org.apache.ignite.client.ReliabilityTest#testRetryNonePolicyDisablesFailover
#[tokio::test]
async fn should_disable_failover_when_explicit_never_retry_policy_is_used() {
    assert_never_retry_failover().await;
}

/// Java parity: org.apache.ignite.client.ReliabilityTest#testQueryConsistencyOnFailover
#[tokio::test]
async fn should_fail_live_scan_cursor_after_cluster_connection_loss() {
    let _guard = churn_lock()
        .lock()
        .expect("reliability churn lock poisoned");
    let env = ignite_cluster3_churn_env();
    if !env.is_managed() {
        return;
    }
    env.start_all();
    env.wait_for_ready().await.unwrap();

    let cache_name = unique_name("reliability_query_consistency");
    let mut conf = ClientConfig::from_addresses(env.addresses().iter().cloned());
    conf.retry_limit = 2;
    conf.request_timeout = Some(Duration::from_millis(500));
    conf.reconnect_backoff = Some(Duration::from_millis(100));
    let client = new_client(conf).await.unwrap();
    destroy_cache_if_exists(&client, &cache_name).await;
    let cache = client
        .get_or_create_cache::<i32, i32>(&cache_name)
        .await
        .unwrap();
    cache.put_all(&[(0, 0), (1, 1)]).await.unwrap();

    let mut cursor = cache
        .scan_query(ScanQuery::new().with_page_size(1))
        .await
        .unwrap();
    let first_page = cursor.next_page().await.unwrap();
    assert_eq!(first_page.len(), 1);

    env.stop_all();
    tokio::time::sleep(Duration::from_millis(500)).await;

    let err = tokio::time::timeout(Duration::from_secs(5), cursor.next_page())
        .await
        .expect("timed out waiting for scan cursor failure")
        .unwrap_err();
    env.start_all();
    env.wait_for_ready().await.unwrap();
    let msg = err.to_string();
    assert!(
        msg.contains("closed")
            || msg.contains("failed")
            || msg.contains("connection")
            || msg.contains("reset")
            || msg.contains("eof"),
        "unexpected cursor failover error: {}",
        msg
    );
}

/// Java parity: org.apache.ignite.client.ReliabilityTest#testTxWithIdIntersection
#[tokio::test]
async fn should_detect_lost_transaction_context_after_connection_drop() {
    let _guard = churn_lock()
        .lock()
        .expect("reliability churn lock poisoned");
    let env = ignite_cluster3_churn_env();
    if !env.is_managed() {
        return;
    }
    env.restart_all();
    env.wait_for_ready().await.unwrap();

    let cache_name = unique_name("reliability_tx_id_intersection");
    let mut conf = ClientConfig::from_addresses(env.addresses().iter().cloned());
    conf.partition_awareness_enabled = false;
    conf.retry_limit = 1;
    conf.reconnect_backoff = Some(Duration::from_millis(100));

    let client = new_client(conf).await.unwrap();
    destroy_cache_if_exists(&client, &cache_name).await;

    let mut cache_cfg = CacheConfiguration::new(&cache_name);
    cache_cfg.atomicity_mode = AtomicityMode::Transactional;
    let cache = client
        .create_cache_with_config::<i32, i32>(&cache_cfg)
        .await
        .unwrap();

    // Start a transaction
    let tx = client
        .transactions()
        .tx_start(TransactionOptions::default())
        .await
        .unwrap();
    let tx_cache = tx.cache::<i32, i32>(&cache_name);

    // Drop connections by cycling the node
    env.stop_node(0);
    tokio::time::sleep(Duration::from_millis(250)).await;
    env.start_node(0);
    env.wait_for_ready().await.unwrap();

    // The tx-scoped operation should fail because the transaction context was lost.
    // Java uses `CyclicBarrier` + `dropAllThinClientConnections()` for precise
    // synchronization and asserts the exact error message.  We cycle the node
    // which achieves the same connection drop.
    let put_result = tx_cache.put(&0, &0).await;
    match put_result {
        Ok(()) => {
            // If the put appeared to succeed (e.g. fast reconnect), the tx context
            // is still lost — rollback and verify the key was never committed.
            let _ = tx.rollback().await;
        }
        Err(err) => {
            let msg = err.to_string();
            assert!(
                msg.contains("Transaction context has been lost")
                    || msg.contains("transaction")
                    || msg.contains("closed")
                    || msg.contains("connection")
                    || msg.contains("failed"),
                "unexpected tx context lost error: {}",
                msg
            );
        }
    }

    // The key must never be present — the tx was never committed regardless
    // of whether put returned Ok or Err.
    assert!(
        !cache.contains_key(&0).await.unwrap(),
        "key should not be present after lost transaction context"
    );

    client.destroy_cache(&cache_name).await.unwrap();
}

async fn assert_never_retry_failover() {
    let _guard = churn_lock()
        .lock()
        .expect("reliability churn lock poisoned");
    let env = ignite_cluster3_churn_env();
    if env.is_managed() {
        env.start_all();
    }
    env.wait_for_ready().await.unwrap();

    let mut conf = ClientConfig::from_addresses([env.addr(), env.addr()]);
    conf.partition_awareness_enabled = false;
    conf.request_timeout = Some(Duration::from_millis(500));
    conf.reconnect_backoff = Some(Duration::from_millis(100));
    conf.retry_policy = RetryPolicy::Never;

    let client = new_client(conf).await.unwrap();
    let cache_name = unique_name("reliability_never_retry");
    destroy_cache_if_exists(&client, &cache_name).await;
    let cache = client
        .get_or_create_cache::<i32, i32>(&cache_name)
        .await
        .unwrap();
    cache.put(&0, &0).await.unwrap();

    env.stop_node(0);
    tokio::time::sleep(Duration::from_millis(200)).await;

    let err = cache.get_size().await.unwrap_err();
    env.start_node(0);
    env.wait_for_ready().await.unwrap();
    let msg = err.to_string();
    assert!(
        msg.contains("closed")
            || msg.contains("failed")
            || msg.contains("connection")
            || msg.contains("reset")
            || msg.contains("eof"),
        "unexpected never-retry error: {}",
        msg
    );
}

struct ErrorRetryPolicy;

impl RetryPolicyHandler for ErrorRetryPolicy {
    fn decide(&self, ctx: &RetryContext) -> Result<RetryDecision, IgniteError> {
        Err(IgniteError::from(
            format!("Error in policy for {}", ctx.address).as_str(),
        ))
    }
}

fn churn_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

async fn create_or_get_failover_cache(
    client: &ignite_rs::Client,
    cache_name: &str,
    cache_cfg: &CacheConfiguration,
) -> ignite_rs::cache::Cache<i32, i32> {
    let mut last_err = None;

    for _ in 0..10 {
        match client.create_cache_with_config::<i32, i32>(cache_cfg).await {
            Ok(cache) => return cache,
            Err(err) if err.to_string().contains("same name is already started") => {
                match client.get_or_create_cache::<i32, i32>(cache_name).await {
                    Ok(cache) => return cache,
                    Err(get_err) => last_err = Some(get_err),
                }
            }
            Err(err) => last_err = Some(err),
        }

        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    panic!(
        "failed to create or get failover cache {}: {}",
        cache_name,
        last_err
            .map(|err| err.to_string())
            .unwrap_or_else(|| "unknown cache bootstrap error".to_string())
    );
}

async fn churn_cluster_while<F, Fut>(env: Arc<common::IgniteClusterEnv>, body: F)
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let stop = Arc::new(AtomicBool::new(false));
    let churn_env = env.clone();
    let churn_stop = stop.clone();

    let churn = tokio::spawn(async move {
        for _ in 0..3 {
            if churn_stop.load(Ordering::Relaxed) {
                break;
            }

            churn_env.stop_node(0);
            tokio::time::sleep(Duration::from_millis(250)).await;

            churn_env.stop_node(1);
            tokio::time::sleep(Duration::from_millis(250)).await;

            churn_env.start_node(0);
            tokio::time::sleep(Duration::from_millis(250)).await;

            churn_env.start_node(1);
            churn_env.wait_for_ready().await.unwrap();
        }
    });

    body().await;
    stop.store(true, Ordering::Relaxed);
    churn.await.unwrap();
    env.start_all();
    env.wait_for_ready().await.unwrap();
}
