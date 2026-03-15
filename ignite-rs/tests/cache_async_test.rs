#![cfg(not(feature = "ssl"))]

mod common;

use common::{
    connect, destroy_cache_if_exists, ignite_scope, unique_name, IgniteProfile, IgniteScope,
};
use ignite_rs::cache::{AtomicityMode, CacheConfiguration};
use ignite_rs::error::IgniteError;
use ignite_rs::protocol::TypeCode;
use ignite_rs::{ClientConfig, ReadableType, WritableType};
use std::collections::BTreeMap;
use std::io::{self, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReadFailsString(String);

impl WritableType for ReadFailsString {
    fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        self.0.write(writer)
    }

    fn size(&self) -> usize {
        self.0.size()
    }
}

impl ReadableType for ReadFailsString {
    fn read_unwrapped(
        type_code: TypeCode,
        reader: &mut impl Read,
    ) -> ignite_rs::error::IgniteResult<Option<Self>> {
        match String::read_unwrapped(type_code, reader)? {
            Some(value) => Err(IgniteError::from(
                format!("Failed to deserialize object [_read_]: {}", value).as_str(),
            )),
            None => Ok(None),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WriteFailsString(String);

impl WritableType for WriteFailsString {
    fn write(&self, _writer: &mut dyn Write) -> io::Result<()> {
        Err(io::Error::other(
            "Failed to serialize object [typeName=WriteFailsString, cause=_write_]",
        ))
    }

    fn size(&self) -> usize {
        self.0.size()
    }
}

impl ReadableType for WriteFailsString {
    fn read_unwrapped(
        type_code: TypeCode,
        reader: &mut impl Read,
    ) -> ignite_rs::error::IgniteResult<Option<Self>> {
        Ok(String::read_unwrapped(type_code, reader)?.map(Self))
    }
}

struct DelayedResponseProxy {
    addr: String,
    _scope: IgniteScope,
    join: Option<thread::JoinHandle<()>>,
}

impl DelayedResponseProxy {
    async fn start(delay: Duration) -> io::Result<Self> {
        let scope = ignite_scope(IgniteProfile::DefaultSingleNode);
        scope
            .wait_for_ready()
            .await
            .map_err(|err| io::Error::other(err.to_string()))?;
        let target_addr = scope
            .client_config()
            .map_err(|err| io::Error::other(err.to_string()))?
            .addresses
            .into_iter()
            .next()
            .ok_or_else(|| io::Error::other("missing single-node fixture address"))?;
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?.to_string();
        let join = thread::spawn(move || {
            let Ok((mut client_stream, _)) = listener.accept() else {
                return;
            };
            let Ok(mut server_stream) = TcpStream::connect(&target_addr) else {
                return;
            };
            let Ok(mut client_reader) = client_stream.try_clone() else {
                return;
            };
            let Ok(mut server_writer) = server_stream.try_clone() else {
                return;
            };

            let upstream = thread::spawn(move || {
                let _ = io::copy(&mut client_reader, &mut server_writer);
                let _ = server_writer.shutdown(Shutdown::Write);
            });

            let mut buf = [0u8; 16 * 1024];
            loop {
                match server_stream.read(&mut buf) {
                    Ok(0) => break,
                    Ok(n) => {
                        thread::sleep(delay);
                        if client_stream.write_all(&buf[..n]).is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }

            let _ = client_stream.shutdown(Shutdown::Write);
            let _ = upstream.join();
        });

        Ok(Self {
            addr,
            _scope: scope,
            join: Some(join),
        })
    }

    fn addr(&self) -> &str {
        &self.addr
    }
}

impl Drop for DelayedResponseProxy {
    fn drop(&mut self) {
        let _ = TcpStream::connect(self.addr());
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

async fn connect_via_delayed_proxy(delay: Duration) -> (DelayedResponseProxy, ignite_rs::Client) {
    let proxy = DelayedResponseProxy::start(delay).await.unwrap();
    let mut conf = ClientConfig::new(proxy.addr());
    conf.handshake_timeout = Some(Duration::from_secs(5));
    conf.request_timeout = Some(Duration::from_secs(5));
    let client = ignite_rs::new_client(conf).await.unwrap();
    (proxy, client)
}

fn suite_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

/// Java parity: org.apache.ignite.internal.client.thin.CacheAsyncTest#testCreateCacheAsyncByNameCreatesCacheWhenNotExists
#[tokio::test]
async fn should_create_cache_async_by_name_when_it_does_not_exist() {
    let _guard = suite_lock()
        .lock()
        .expect("cache_async suite lock poisoned");
    let ignite = connect().await.unwrap();
    let cache_name = unique_name("cache_async_create_by_name");
    destroy_cache_if_exists(&ignite, &cache_name).await;

    ignite.create_cache::<i32, i32>(&cache_name).await.unwrap();

    let cache_names = ignite.get_cache_names().await.unwrap();
    assert!(cache_names.iter().any(|name| name == &cache_name));

    ignite.destroy_cache(&cache_name).await.unwrap();
}

/// Java parity: org.apache.ignite.internal.client.thin.CacheAsyncTest#testCreateCacheAsyncByCfgCreatesCacheWhenNotExists
#[tokio::test]
async fn should_create_cache_async_by_config_when_it_does_not_exist() {
    let _guard = suite_lock()
        .lock()
        .expect("cache_async suite lock poisoned");
    let ignite = connect().await.unwrap();
    let cache_name = unique_name("cache_async_create_by_cfg");
    destroy_cache_if_exists(&ignite, &cache_name).await;

    let mut config = CacheConfiguration::new(&cache_name);
    config.num_backup = 1;
    config.atomicity_mode = AtomicityMode::Transactional;

    ignite
        .create_cache_with_config::<i32, i32>(&config)
        .await
        .unwrap();

    let actual = ignite.get_cache_config(&cache_name).await.unwrap();
    assert_eq!(actual.num_backup, 1);
    assert_eq!(actual.atomicity_mode, AtomicityMode::Transactional);

    ignite.destroy_cache(&cache_name).await.unwrap();
}

/// Java parity: org.apache.ignite.internal.client.thin.CacheAsyncTest#testCreateCacheAsyncByNameThrowsExceptionWhenCacheExists
#[tokio::test]
async fn should_return_error_when_create_cache_async_by_name_hits_existing_cache() {
    let _guard = suite_lock()
        .lock()
        .expect("cache_async suite lock poisoned");
    let ignite = connect().await.unwrap();
    let cache_name = unique_name("cache_async_create_by_name_exists");
    destroy_cache_if_exists(&ignite, &cache_name).await;

    ignite.create_cache::<i32, i32>(&cache_name).await.unwrap();

    let err = match ignite.create_cache::<i32, i32>(&cache_name).await {
        Ok(_) => panic!("expected duplicate create_cache by name to fail"),
        Err(err) => err,
    };
    let message = err.to_string();

    assert!(
        message.contains("already started")
            || message.contains("already exists")
            || message.contains(&cache_name),
        "unexpected duplicate create-cache error for {}: {}",
        cache_name,
        message
    );

    ignite.destroy_cache(&cache_name).await.unwrap();
}

/// Java parity: org.apache.ignite.internal.client.thin.CacheAsyncTest#testCreateCacheAsyncByCfgThrowsExceptionWhenCacheExists
#[tokio::test]
async fn should_return_error_when_create_cache_async_by_config_hits_existing_cache() {
    let _guard = suite_lock()
        .lock()
        .expect("cache_async suite lock poisoned");
    let ignite = connect().await.unwrap();
    let cache_name = unique_name("cache_async_create_by_cfg_exists");
    destroy_cache_if_exists(&ignite, &cache_name).await;

    ignite.create_cache::<i32, i32>(&cache_name).await.unwrap();

    let mut config = CacheConfiguration::new(&cache_name);
    config.num_backup = 3;

    let err = match ignite.create_cache_with_config::<i32, i32>(&config).await {
        Ok(_) => panic!("expected duplicate create_cache_with_config to fail"),
        Err(err) => err,
    };
    let message = err.to_string();

    assert!(
        message.contains("already started")
            || message.contains("already exists")
            || message.contains(&cache_name),
        "unexpected duplicate create-cache-with-config error for {}: {}",
        cache_name,
        message
    );

    ignite.destroy_cache(&cache_name).await.unwrap();
}

/// Java parity: org.apache.ignite.internal.client.thin.CacheAsyncTest#testGetOrCreateCacheAsyncByCfgCreatesCacheWhenNotExists
#[tokio::test]
async fn should_create_cache_when_get_or_create_async_by_config_misses_existing_cache() {
    let _guard = suite_lock()
        .lock()
        .expect("cache_async suite lock poisoned");
    let ignite = connect().await.unwrap();
    let cache_name = unique_name("cache_async_get_or_create_cfg");
    destroy_cache_if_exists(&ignite, &cache_name).await;

    let mut config = CacheConfiguration::new(&cache_name);
    config.num_backup = 5;

    let cache = ignite
        .get_or_create_cache_with_config::<i32, i32>(&config)
        .await
        .unwrap();
    let actual = ignite.get_cache_config(&cache_name).await.unwrap();

    assert_eq!(cache.name(), cache_name.as_str());
    assert_eq!(actual.num_backup, 5);

    ignite.destroy_cache(&cache_name).await.unwrap();
}

/// Java parity: org.apache.ignite.internal.client.thin.CacheAsyncTest#testGetOrCreateCacheAsyncByNameCreatesCacheWhenNotExists
#[tokio::test]
async fn should_create_cache_when_get_or_create_async_by_name_misses_existing_cache() {
    let _guard = suite_lock()
        .lock()
        .expect("cache_async suite lock poisoned");
    let ignite = connect().await.unwrap();
    let cache_name = unique_name("cache_async_get_or_create_name");
    destroy_cache_if_exists(&ignite, &cache_name).await;

    let cache = ignite
        .get_or_create_cache::<i32, i32>(&cache_name)
        .await
        .unwrap();
    let cache_names = ignite.get_cache_names().await.unwrap();

    assert_eq!(cache.name(), cache_name.as_str());
    assert!(cache_names.iter().any(|name| name == &cache_name));

    ignite.destroy_cache(&cache_name).await.unwrap();
}

/// Java parity: org.apache.ignite.internal.client.thin.CacheAsyncTest#testDestroyCacheAsyncSucceedsWhenCacheExists
#[tokio::test]
async fn should_destroy_cache_async_when_it_exists() {
    let _guard = suite_lock()
        .lock()
        .expect("cache_async suite lock poisoned");
    let ignite = connect().await.unwrap();
    let cache_name = unique_name("cache_async_destroy");
    destroy_cache_if_exists(&ignite, &cache_name).await;

    ignite.create_cache::<i32, i32>(&cache_name).await.unwrap();
    ignite.destroy_cache(&cache_name).await.unwrap();

    let cache_names = ignite.get_cache_names().await.unwrap();
    assert!(!cache_names.iter().any(|name| name == &cache_name));
}

/// Java parity: org.apache.ignite.internal.client.thin.CacheAsyncTest#testCacheNamesAsync
#[tokio::test]
async fn should_list_cache_names_after_async_mutations() {
    let _guard = suite_lock()
        .lock()
        .expect("cache_async suite lock poisoned");
    let ignite = connect().await.unwrap();
    let cache_name = unique_name("cache_async_names");
    destroy_cache_if_exists(&ignite, &cache_name).await;

    let before = ignite.get_cache_names().await.unwrap();
    ignite.create_cache::<i32, i32>(&cache_name).await.unwrap();
    let after = ignite.get_cache_names().await.unwrap();

    assert!(
        after.len() >= before.len(),
        "cache-name listing unexpectedly shrank from {} to {}",
        before.len(),
        after.len()
    );
    assert!(after.iter().any(|name| name == &cache_name));

    ignite.destroy_cache(&cache_name).await.unwrap();
}

/// Java parity: org.apache.ignite.internal.client.thin.CacheAsyncTest#testGetAsyncReportsCorrectIgniteFutureStates
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_report_rust_async_task_states_for_cache_get() {
    let _guard = suite_lock()
        .lock()
        .expect("cache_async suite lock poisoned");
    let direct = connect().await.unwrap();
    let cache_name = unique_name("cache_async_future_states");
    destroy_cache_if_exists(&direct, &cache_name).await;

    let cache = direct
        .get_or_create_cache::<i32, String>(&cache_name)
        .await
        .unwrap();
    cache.put(&1, &"1".to_string()).await.unwrap();

    let (_proxy, proxied) = connect_via_delayed_proxy(Duration::from_millis(250)).await;
    let proxied_cache = proxied.cache::<i32, String>(&cache_name);

    let get_task = tokio::spawn(async move { proxied_cache.get(&1).await });
    tokio::time::sleep(Duration::from_millis(25)).await;
    assert!(
        !get_task.is_finished(),
        "spawned cache get completed before the delayed live response arrived"
    );

    let completion_thread = Arc::new(Mutex::new(None::<String>));
    let completion_thread_clone = completion_thread.clone();
    let observer = tokio::spawn(async move {
        let result = get_task.await.expect("cache get task should join cleanly");
        *completion_thread_clone.lock().unwrap() = Some(
            std::thread::current()
                .name()
                .unwrap_or("unnamed")
                .to_string(),
        );
        result
    });

    let result = observer.await.unwrap().unwrap();
    assert_eq!(result, Some("1".to_string()));

    let completion_name = completion_thread
        .lock()
        .unwrap()
        .clone()
        .expect("completion observer should record a thread name");
    assert!(
        !completion_name.starts_with("thin-client-channel"),
        "completion observer should not run on an internal thin-client thread: {}",
        completion_name
    );

    direct.destroy_cache(&cache_name).await.unwrap();
}

/// Java parity: org.apache.ignite.internal.client.thin.CacheAsyncTest#testGetAsyncCanBeCancelled
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn should_allow_rust_async_cache_get_task_to_be_cancelled() {
    let _guard = suite_lock()
        .lock()
        .expect("cache_async suite lock poisoned");
    let direct = connect().await.unwrap();
    let cache_name = unique_name("cache_async_cancel");
    destroy_cache_if_exists(&direct, &cache_name).await;

    let cache = direct
        .get_or_create_cache::<i32, String>(&cache_name)
        .await
        .unwrap();
    cache.put(&1, &"2".to_string()).await.unwrap();

    let (_proxy, proxied) = connect_via_delayed_proxy(Duration::from_millis(250)).await;
    let proxied_cache = proxied.cache::<i32, String>(&cache_name);

    let get_task = tokio::spawn(async move { proxied_cache.get(&1).await });
    tokio::time::sleep(Duration::from_millis(25)).await;
    assert!(
        !get_task.is_finished(),
        "spawned cache get completed before cancellation could be observed"
    );

    get_task.abort();
    let err = get_task
        .await
        .expect_err("aborted cache get task should report cancellation");
    assert!(
        err.is_cancelled(),
        "expected task cancellation, got {}",
        err
    );

    direct.destroy_cache(&cache_name).await.unwrap();
}

/// Java parity: org.apache.ignite.internal.client.thin.CacheAsyncTest#testGetAsyncThrowsExceptionOnBadCacheName
#[tokio::test]
async fn should_return_error_for_async_operation_on_missing_cache() {
    let _guard = suite_lock()
        .lock()
        .expect("cache_async suite lock poisoned");
    let ignite = connect().await.unwrap();
    let cache_name = unique_name("cache_async_missing");
    destroy_cache_if_exists(&ignite, &cache_name).await;

    let cache = ignite.cache::<i32, i32>(&cache_name);
    let err = cache.put(&1, &2).await.unwrap_err();
    let message = err.to_string();

    assert!(
        message.contains(&cache_name) || message.contains("Cache does not exist"),
        "unexpected error for missing cache {}: {}",
        cache_name,
        message
    );
}

/// Java parity: org.apache.ignite.internal.client.thin.CacheAsyncTest#testGetAsyncThrowsExceptionOnFailedDeserialization
#[tokio::test]
async fn should_return_error_for_async_cache_get_when_value_deserialization_fails() {
    let _guard = suite_lock()
        .lock()
        .expect("cache_async suite lock poisoned");
    let ignite = connect().await.unwrap();
    let cache_name = unique_name("cache_async_deser_fail");
    destroy_cache_if_exists(&ignite, &cache_name).await;

    let cache = ignite
        .get_or_create_cache::<i32, String>(&cache_name)
        .await
        .unwrap();
    cache.put(&1, &"1".to_string()).await.unwrap();

    let broken_cache = ignite.cache::<i32, ReadFailsString>(&cache_name);
    let err = broken_cache.get(&1).await.unwrap_err().to_string();

    assert!(
        err.contains("Failed to deserialize object") && err.contains("_read_"),
        "unexpected deserialization error: {}",
        err
    );

    ignite.destroy_cache(&cache_name).await.unwrap();
}

/// Java parity: org.apache.ignite.internal.client.thin.CacheAsyncTest#testGetOrCreateCacheAsyncByNameReturnsExistingWhenCacheExists
#[tokio::test]
async fn should_return_existing_cache_when_get_or_create_async_by_name_hits_existing_cache() {
    let _guard = suite_lock()
        .lock()
        .expect("cache_async suite lock poisoned");
    let ignite = connect().await.unwrap();
    let cache_name = unique_name("cache_async_existing_name");
    destroy_cache_if_exists(&ignite, &cache_name).await;

    let mut config = CacheConfiguration::new(&cache_name);
    config.num_backup = 7;
    ignite
        .create_cache_with_config::<i32, i32>(&config)
        .await
        .unwrap();

    let cache = ignite
        .get_or_create_cache::<i32, i32>(&cache_name)
        .await
        .unwrap();
    let actual = ignite.get_cache_config(&cache_name).await.unwrap();

    assert_eq!(cache.name(), cache_name.as_str());
    assert_eq!(actual.num_backup, 7);

    ignite.destroy_cache(&cache_name).await.unwrap();
}

/// Java parity: org.apache.ignite.internal.client.thin.CacheAsyncTest#testGetOrCreateCacheAsyncByCfgIgnoresCfgWhenCacheExists
#[tokio::test]
async fn should_ignore_new_config_when_get_or_create_async_by_config_hits_existing_cache() {
    let _guard = suite_lock()
        .lock()
        .expect("cache_async suite lock poisoned");
    let ignite = connect().await.unwrap();
    let cache_name = unique_name("cache_async_existing_cfg");
    destroy_cache_if_exists(&ignite, &cache_name).await;

    ignite.create_cache::<i32, i32>(&cache_name).await.unwrap();

    let mut config = CacheConfiguration::new(&cache_name);
    config.num_backup = 7;

    let cache = ignite
        .get_or_create_cache_with_config::<i32, i32>(&config)
        .await
        .unwrap();
    let actual = ignite.get_cache_config(&cache_name).await.unwrap();

    assert_eq!(cache.name(), cache_name.as_str());
    assert_eq!(actual.num_backup, 0);

    ignite.destroy_cache(&cache_name).await.unwrap();
}

/// Java parity: org.apache.ignite.internal.client.thin.CacheAsyncTest#testDestroyCacheAsyncThrowsWhenCacheDoesNotExist
#[tokio::test]
async fn should_return_error_when_destroying_missing_cache_async() {
    let _guard = suite_lock()
        .lock()
        .expect("cache_async suite lock poisoned");
    let ignite = connect().await.unwrap();
    let cache_name = unique_name("cache_async_destroy_missing");
    destroy_cache_if_exists(&ignite, &cache_name).await;

    let err = ignite
        .destroy_cache(&cache_name)
        .await
        .unwrap_err()
        .to_string();

    assert!(
        err.contains("Cache does not exist") || err.contains(&cache_name),
        "unexpected destroy error for missing cache {}: {}",
        cache_name,
        err
    );
}

/// Java parity: org.apache.ignite.internal.client.thin.CacheAsyncTest#testPutAsyncThrowsExceptionOnFailedSerialization
#[tokio::test]
async fn should_return_error_for_async_cache_put_when_value_serialization_fails() {
    let _guard = suite_lock()
        .lock()
        .expect("cache_async suite lock poisoned");
    let ignite = connect().await.unwrap();
    let cache_name = unique_name("cache_async_ser_fail");
    destroy_cache_if_exists(&ignite, &cache_name).await;

    let cache = ignite
        .get_or_create_cache::<i32, WriteFailsString>(&cache_name)
        .await
        .unwrap();
    let err = cache
        .put(&1, &WriteFailsString("1".to_string()))
        .await
        .unwrap_err()
        .to_string();

    assert!(
        err.contains("Failed to serialize object") && err.contains("_write_"),
        "unexpected serialization error: {}",
        err
    );

    ignite.destroy_cache(&cache_name).await.unwrap();
}

/// Java parity: org.apache.ignite.internal.client.thin.CacheAsyncTest#testAsyncCacheOperations
#[tokio::test]
async fn should_support_async_first_cache_operations_across_cache_api_surface() {
    let _guard = suite_lock()
        .lock()
        .expect("cache_async suite lock poisoned");
    let ignite = connect().await.unwrap();
    let cache_name = unique_name("cache_async_ops");
    destroy_cache_if_exists(&ignite, &cache_name).await;

    let cache = ignite
        .get_or_create_cache::<i32, String>(&cache_name)
        .await
        .unwrap();

    cache.put(&1, &"1".to_string()).await.unwrap();
    assert_eq!(cache.get(&1).await.unwrap(), Some("1".to_string()));
    assert_eq!(cache.get(&2).await.unwrap(), None);

    cache.put(&11, &"2".to_string()).await.unwrap();
    assert_eq!(cache.get(&11).await.unwrap(), Some("2".to_string()));

    assert!(cache.contains_key(&1).await.unwrap());
    assert!(!cache.contains_key(&2).await.unwrap());
    assert_eq!(
        ignite.get_cache_config(&cache_name).await.unwrap().name,
        cache_name
    );

    cache.put(&2, &"2".to_string()).await.unwrap();
    assert_eq!(cache.get_size().await.unwrap(), 3);

    cache.put(&3, &"3".to_string()).await.unwrap();
    let actual = cache.get_all(&[2, 3, 4, 5]).await.unwrap();
    let actual = actual
        .into_iter()
        .filter_map(|(key, value)| key.zip(value))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        actual,
        BTreeMap::from([(2, "2".to_string()), (3, "3".to_string())])
    );

    cache
        .put_all(&[(4, "4".to_string()), (5, "5".to_string())])
        .await
        .unwrap();
    assert_eq!(cache.get(&4).await.unwrap(), Some("4".to_string()));
    assert_eq!(cache.get(&5).await.unwrap(), Some("5".to_string()));

    assert!(cache.contains_keys(&[4, 5]).await.unwrap());
    assert!(!cache.contains_keys(&[4, 5, 6]).await.unwrap());
    assert!(cache.contains_keys(&[]).await.unwrap());

    assert!(cache.replace(&4, &"6".to_string()).await.unwrap());
    assert_eq!(cache.get(&4).await.unwrap(), Some("6".to_string()));
    assert!(!cache.replace(&-1, &"1".to_string()).await.unwrap());

    assert!(cache
        .replace_if_equals(&4, &"6".to_string(), &"7".to_string())
        .await
        .unwrap());
    assert_eq!(cache.get(&4).await.unwrap(), Some("7".to_string()));
    assert!(!cache
        .replace_if_equals(&-1, &"1".to_string(), &"2".to_string())
        .await
        .unwrap());
    assert!(!cache
        .replace_if_equals(&4, &"1".to_string(), &"2".to_string())
        .await
        .unwrap());

    assert!(!cache.remove_key(&-1).await.unwrap());
    assert!(cache.remove_key(&4).await.unwrap());
    assert!(!cache.contains_key(&4).await.unwrap());

    assert!(!cache.remove_if_equals(&2, &"0".to_string()).await.unwrap());
    assert!(cache.remove_if_equals(&2, &"2".to_string()).await.unwrap());
    assert!(!cache.contains_key(&2).await.unwrap());

    cache.remove_all().await.unwrap();
    assert_eq!(cache.get_size().await.unwrap(), 0);

    cache
        .put_all(&[
            (1, "1".to_string()),
            (2, "2".to_string()),
            (3, "3".to_string()),
        ])
        .await
        .unwrap();
    cache.remove_keys(&[2, 3]).await.unwrap();
    assert_eq!(cache.get_size().await.unwrap(), 1);
    assert_eq!(cache.get(&1).await.unwrap(), Some("1".to_string()));

    assert_eq!(cache.get_and_put(&2, &"2".to_string()).await.unwrap(), None);
    assert_eq!(
        cache.get_and_put(&2, &"3".to_string()).await.unwrap(),
        Some("2".to_string())
    );
    assert_eq!(cache.get_and_remove(&-1).await.unwrap(), None);
    assert_eq!(
        cache.get_and_remove(&2).await.unwrap(),
        Some("3".to_string())
    );
    assert_eq!(
        cache.get_and_replace(&-1, &"1".to_string()).await.unwrap(),
        None
    );
    assert_eq!(
        cache.get_and_replace(&1, &"2".to_string()).await.unwrap(),
        Some("1".to_string())
    );

    assert!(!cache.put_if_absent(&1, &"3".to_string()).await.unwrap());
    assert_eq!(cache.get(&1).await.unwrap(), Some("2".to_string()));
    assert!(cache.put_if_absent(&5, &"5".to_string()).await.unwrap());
    assert_eq!(cache.get(&5).await.unwrap(), Some("5".to_string()));

    cache.clear().await.unwrap();
    assert_eq!(cache.get_size().await.unwrap(), 0);

    cache
        .put_all(&[
            (1, "1".to_string()),
            (2, "2".to_string()),
            (3, "3".to_string()),
        ])
        .await
        .unwrap();
    assert_eq!(
        cache
            .get_and_put_if_absent(&1, &"2".to_string())
            .await
            .unwrap(),
        Some("1".to_string())
    );
    assert_eq!(cache.get(&1).await.unwrap(), Some("1".to_string()));
    assert_eq!(
        cache
            .get_and_put_if_absent(&4, &"4".to_string())
            .await
            .unwrap(),
        None
    );
    assert_eq!(cache.get(&4).await.unwrap(), Some("4".to_string()));

    cache.clear_key(&1).await.unwrap();
    assert_eq!(cache.get(&1).await.unwrap(), None);
    cache.clear_keys(&[2, 3, 4]).await.unwrap();
    assert_eq!(cache.get_size().await.unwrap(), 0);

    ignite.destroy_cache(&cache_name).await.unwrap();
}
