#![cfg(not(feature = "ssl"))]

mod common;

use common::{
    connect_with_cluster3_churn_config, destroy_cache_if_exists, ignite_cluster3_churn_env,
    unique_name,
};
use ignite_rs::cache::CacheConfiguration;
use ignite_rs::{new_client, ClientConfig};
use std::net::{TcpListener, TcpStream};
use std::sync::{Mutex, MutexGuard, OnceLock};
use std::thread;
use std::time::Duration;

/// Java parity: org.apache.ignite.internal.client.thin.ThinClientPartitionAwarenessUnstableTopologyTest#testPartitionAwarenessOnNodeJoin
#[tokio::test]
async fn should_continue_live_partition_aware_operations_after_node_join() {
    let _guard = lock_churn();
    let env = ignite_cluster3_churn_env();
    if !env.is_managed() {
        return;
    }

    env.stop_node(2);
    tokio::time::sleep(Duration::from_millis(500)).await;
    env.wait_for_ready().await.unwrap();

    let cache_name = unique_name("unstable_live_join");
    let mut conf = ClientConfig::from_addresses(env.addresses().iter().cloned());
    conf.request_timeout = Some(Duration::from_millis(500));
    conf.retry_limit = 8;
    conf.reconnect_backoff = Some(Duration::from_millis(100));
    let client = connect_with_cluster3_churn_config(conf).await.unwrap();
    destroy_cache_if_exists(&client, &cache_name).await;

    let mut cache_cfg = CacheConfiguration::new(&cache_name);
    cache_cfg.num_backup = 1;
    let cache = match client
        .create_cache_with_config::<i32, i32>(&cache_cfg)
        .await
    {
        Ok(cache) => cache,
        Err(err) if err.to_string().contains("same name is already started") => client
            .get_or_create_cache::<i32, i32>(&cache_name)
            .await
            .unwrap(),
        Err(err) => panic!(
            "failed to create restart test cache {}: {}",
            cache_name, err
        ),
    };
    cache.put(&0, &0).await.unwrap();
    assert_eq!(cache.get(&0).await.unwrap(), Some(0));

    env.start_node(2);
    env.wait_for_ready().await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    for idx in 1..=16 {
        cache.put(&idx, &idx).await.unwrap();
        assert_eq!(cache.get(&idx).await.unwrap(), Some(idx));
    }

    let _ = client.destroy_cache(&cache_name).await;
}

/// Java parity: org.apache.ignite.internal.client.thin.ThinClientPartitionAwarenessUnstableTopologyTest#testPartitionAwarenessOnNodeLeft
#[tokio::test]
async fn should_continue_live_partition_aware_operations_after_node_left() {
    let _guard = lock_churn();
    let env = ignite_cluster3_churn_env();
    if !env.is_managed() {
        return;
    }

    env.wait_for_ready().await.unwrap();

    let cache_name = unique_name("unstable_live_left");
    let mut conf = ClientConfig::from_addresses(env.addresses().iter().cloned());
    conf.request_timeout = Some(Duration::from_millis(500));
    conf.retry_limit = 8;
    conf.reconnect_backoff = Some(Duration::from_millis(100));
    let client = connect_with_cluster3_churn_config(conf).await.unwrap();
    destroy_cache_if_exists(&client, &cache_name).await;

    let mut cache_cfg = CacheConfiguration::new(&cache_name);
    cache_cfg.num_backup = 1;
    let cache = match client
        .create_cache_with_config::<i32, i32>(&cache_cfg)
        .await
    {
        Ok(cache) => cache,
        Err(err) if err.to_string().contains("same name is already started") => client
            .get_or_create_cache::<i32, i32>(&cache_name)
            .await
            .unwrap(),
        Err(err) => panic!(
            "failed to create restart test cache {}: {}",
            cache_name, err
        ),
    };

    for idx in 0..8 {
        cache.put(&idx, &idx).await.unwrap();
    }

    env.stop_node(2);
    tokio::time::sleep(Duration::from_millis(500)).await;

    for idx in 8..=24 {
        cache.put(&idx, &idx).await.unwrap();
        assert_eq!(cache.get(&idx).await.unwrap(), Some(idx));
    }

    env.start_node(2);
    env.wait_for_ready().await.unwrap();
    let _ = client.destroy_cache(&cache_name).await;
}

/// Java parity: org.apache.ignite.internal.client.thin.ThinClientPartitionAwarenessUnstableTopologyTest#testConnectionLoss
#[tokio::test]
async fn should_keep_live_cache_usable_after_connection_loss() {
    let _guard = lock_churn();
    let env = ignite_cluster3_churn_env();
    if !env.is_managed() {
        return;
    }

    env.wait_for_ready().await.unwrap();

    let cache_name = unique_name("unstable_live_connection_loss");
    let mut conf = ClientConfig::from_addresses(env.addresses().iter().cloned());
    conf.request_timeout = Some(Duration::from_millis(500));
    conf.retry_limit = 16;
    conf.reconnect_backoff = Some(Duration::from_millis(100));
    let client = connect_with_cluster3_churn_config(conf).await.unwrap();
    destroy_cache_if_exists(&client, &cache_name).await;

    let mut cache_cfg = CacheConfiguration::new(&cache_name);
    cache_cfg.num_backup = 1;
    let cache = match client
        .create_cache_with_config::<i32, i32>(&cache_cfg)
        .await
    {
        Ok(cache) => cache,
        Err(err) if err.to_string().contains("same name is already started") => client
            .get_or_create_cache::<i32, i32>(&cache_name)
            .await
            .unwrap(),
        Err(err) => panic!(
            "failed to create restart test cache {}: {}",
            cache_name, err
        ),
    };
    cache.put(&0, &0).await.unwrap();

    env.stop_node(0);
    tokio::time::sleep(Duration::from_millis(500)).await;

    cache.put(&1, &1).await.unwrap();
    assert_eq!(cache.get(&0).await.unwrap(), Some(0));
    assert_eq!(cache.get(&1).await.unwrap(), Some(1));

    env.start_node(0);
    env.wait_for_ready().await.unwrap();
    let _ = client.destroy_cache(&cache_name).await;
}

/// Java parity: org.apache.ignite.internal.client.thin.ThinClientPartitionAwarenessUnstableTopologyTest#testPartitionAwarenessOnClusterRestartWithLowerTopologyVersion
#[tokio::test]
async fn should_continue_live_partition_aware_operations_after_cluster_restart_with_lower_topology_version(
) {
    assert_live_partition_awareness_after_cluster_restart(2).await;
}

/// Java parity: org.apache.ignite.internal.client.thin.ThinClientPartitionAwarenessUnstableTopologyTest#testPartitionAwarenessOnClusterRestartWithSameTopologyVersion
#[tokio::test]
async fn should_continue_live_partition_aware_operations_after_cluster_restart_with_same_topology_version(
) {
    assert_live_partition_awareness_after_cluster_restart(3).await;
}

/// Java parity: org.apache.ignite.internal.client.thin.ThinClientPartitionAwarenessUnstableTopologyTest#testSessionCloseBeforeHandshake
#[tokio::test]
async fn should_fail_live_startup_when_proxy_closes_socket_before_handshake() {
    let _guard = lock_churn();
    let env = ignite_cluster3_churn_env();
    if !env.is_managed() {
        return;
    }

    env.wait_for_ready().await.unwrap();
    let (proxy_addr, handle) = spawn_live_handshake_close_proxy(env.addr().to_string());

    let mut conf = ClientConfig::new(&proxy_addr);
    conf.handshake_timeout = Some(Duration::from_millis(500));
    conf.request_timeout = Some(Duration::from_millis(500));

    let err = match connect_with_cluster3_churn_config(conf).await {
        Ok(_) => panic!("expected startup to fail when the proxy closes during handshake"),
        Err(err) => err,
    };
    let msg = err.to_string().to_lowercase();

    assert!(
        msg.contains("closed")
            || msg.contains("eof")
            || msg.contains("failed")
            || msg.contains("reset"),
        "unexpected handshake-close error: {}",
        err
    );

    handle
        .join()
        .expect("handshake-close proxy thread should exit cleanly");
}

/// Java parity: org.apache.ignite.internal.client.thin.ThinClientPartitionAwarenessUnstableTopologyTest#testCreateSessionAfterClose
#[tokio::test]
async fn should_create_live_session_after_close_and_cluster_restart() {
    let _guard = lock_churn();
    let env = ignite_cluster3_churn_env();
    if !env.is_managed() {
        return;
    }

    env.wait_for_ready().await.unwrap();
    let mut conf = ClientConfig::from_addresses(env.addresses().iter().cloned());
    conf.request_timeout = Some(Duration::from_millis(500));

    {
        let client = connect_with_cluster3_churn_config(conf.clone())
            .await
            .unwrap();
        let names = client.get_cache_names().await.unwrap();
        assert!(
            !names.iter().any(|name| name == "__unexpected__"),
            "unexpected cache names before restart: {:?}",
            names
        );
    }

    env.stop_all();
    tokio::time::sleep(Duration::from_secs(1)).await;
    env.start_all();
    env.wait_for_ready().await.unwrap();

    let client = connect_with_cluster3_churn_config(conf).await.unwrap();
    let names = client.get_cache_names().await.unwrap();
    assert!(
        !names.iter().any(|name| name == "__unexpected__"),
        "unexpected cache names after restart: {:?}",
        names
    );
}

fn churn_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

async fn assert_live_partition_awareness_after_cluster_restart(restarted_cluster_size: usize) {
    let _guard = lock_churn();
    let env = ignite_cluster3_churn_env();
    if !env.is_managed() {
        return;
    }

    env.start_all();
    env.wait_for_ready().await.unwrap();
    wait_for_cluster_operations_ready(env.addresses()).await;

    let cache_name = unique_name("unstable_live_cluster_restart");
    let mut conf = ClientConfig::from_addresses(env.addresses().iter().cloned());
    conf.request_timeout = Some(Duration::from_millis(500));
    conf.retry_limit = 16;
    conf.reconnect_backoff = Some(Duration::from_millis(100));

    let client = connect_with_cluster3_churn_config(conf).await.unwrap();
    destroy_cache_if_exists(&client, &cache_name).await;

    let mut cache_cfg = CacheConfiguration::new(&cache_name);
    cache_cfg.num_backup = 1;
    let cache = match client
        .create_cache_with_config::<i32, i32>(&cache_cfg)
        .await
    {
        Ok(cache) => cache,
        Err(err) if err.to_string().contains("same name is already started") => client
            .get_or_create_cache::<i32, i32>(&cache_name)
            .await
            .unwrap(),
        Err(err) => panic!(
            "failed to create restart test cache {}: {}",
            cache_name, err
        ),
    };
    assert_live_round_trips(&cache, 0..16).await;

    env.stop_all();
    tokio::time::sleep(Duration::from_secs(1)).await;
    for index in 0..restarted_cluster_size {
        env.start_node(index);
    }
    env.wait_for_ready().await.unwrap();
    wait_for_cluster_operations_ready(env.addresses()).await;

    let cache = client
        .get_or_create_cache::<i32, i32>(&cache_name)
        .await
        .unwrap();
    assert_live_round_trips(&cache, 16..32).await;

    let names = client.get_cache_names().await.unwrap();
    assert!(
        names.iter().any(|name| name == &cache_name),
        "expected restarted cluster to expose recreated cache {}",
        cache_name
    );

    let _ = client.destroy_cache(&cache_name).await;
    env.start_all();
    env.wait_for_ready().await.unwrap();
}

async fn assert_live_round_trips(
    cache: &ignite_rs::cache::Cache<i32, i32>,
    keys: std::ops::Range<i32>,
) {
    for key in keys {
        cache.put(&key, &key).await.unwrap();
        assert_eq!(cache.get(&key).await.unwrap(), Some(key));
    }
}

fn spawn_live_handshake_close_proxy(target_addr: String) -> (String, thread::JoinHandle<()>) {
    let listener =
        TcpListener::bind("127.0.0.1:0").expect("failed to bind live handshake-close proxy");
    let addr = listener
        .local_addr()
        .expect("failed to read live handshake-close proxy address")
        .to_string();

    let handle = thread::spawn(move || {
        if let Ok((_client, _peer)) = listener.accept() {
            let _ = TcpStream::connect(target_addr);
            // Drop both sockets immediately to emulate the server side closing before handshake completes.
        }
    });

    (addr, handle)
}

fn lock_churn() -> MutexGuard<'static, ()> {
    churn_lock()
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
}

async fn wait_for_cluster_operations_ready(addresses: &[String]) {
    let mut conf = ClientConfig::from_addresses(addresses.iter().cloned());
    conf.request_timeout = Some(Duration::from_millis(500));
    conf.retry_limit = 8;
    conf.reconnect_backoff = Some(Duration::from_millis(100));

    let mut last_err = None;
    for _ in 0..20 {
        match new_client(conf.clone()).await {
            Ok(client) => match client.get_cache_names().await {
                Ok(_) => return,
                Err(err) => last_err = Some(err),
            },
            Err(err) => last_err = Some(err),
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    panic!(
        "cluster churn fixture did not become operation-ready: {}",
        last_err
            .map(|err| err.to_string())
            .unwrap_or_else(|| "unknown readiness error".to_string())
    );
}
