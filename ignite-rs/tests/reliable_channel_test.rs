#![cfg(not(feature = "ssl"))]

mod common;

use common::{connect_with_config, ignite_cluster3_churn_env, ignite_test_env, unused_local_addr};
use ignite_rs::{new_client, AddressResolver, ClientConfig};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

/// Migrated from Apache Ignite `ReliableChannelTest.testDuplicatedAddressesAreValid`:
/// Java source: org.apache.ignite.internal.client.thin.ReliableChannelTest
#[tokio::test]
async fn should_allow_duplicated_addresses_against_live_node() {
    let env = ignite_test_env();
    env.wait_for_ready().await.unwrap();

    let conf = ClientConfig::from_addresses([env.addr(), env.addr()]);
    let client = connect_with_config(conf).await.unwrap();
    let names = client.get_cache_names().await.unwrap();

    assert!(
        !names.iter().any(|name| name == "__unexpected__"),
        "expected duplicated configured addresses to remain usable",
    );
}

/// Migrated from Apache Ignite `ReliableChannelTest.testAddressWithoutPort`:
/// Java source: org.apache.ignite.internal.client.thin.ReliableChannelTest
#[tokio::test]
async fn should_connect_using_live_port_range_address() {
    let env = ignite_test_env();
    env.wait_for_ready().await.unwrap();

    let port = env
        .addr()
        .rsplit_once(':')
        .expect("expected host:port live Ignite address")
        .1
        .parse::<u16>()
        .expect("expected numeric live Ignite port");
    let range_addr = format!("127.0.0.1:{}..{}", port.saturating_sub(1), port);

    let client = new_client(ClientConfig::new(&range_addr)).await.unwrap();
    let names = client.get_cache_names().await.unwrap();

    assert!(
        !names.iter().any(|name| name == "__unexpected__"),
        "expected live port-range configuration to connect successfully",
    );
}

/// Migrated from Apache Ignite `ReliableChannelTest.testDynamicAddressReinitializedCorrectly`:
/// Java source: org.apache.ignite.internal.client.thin.ReliableChannelTest
#[tokio::test]
async fn should_reconnect_using_live_refreshed_dynamic_address_set() {
    let _guard = churn_lock()
        .lock()
        .expect("reliable-channel churn lock poisoned");
    let env = ignite_cluster3_churn_env();
    env.wait_for_ready().await.unwrap();

    let resolver = SequenceAddressResolver::new([
        vec![env.addresses()[0].clone()],
        vec![env.addresses()[1].clone()],
    ]);

    let mut conf = ClientConfig::from_addresses(std::iter::empty::<&str>());
    conf.address_resolver = Some(Arc::new(resolver));
    conf.partition_awareness_enabled = false;
    conf.retry_limit = 8;
    conf.reconnect_backoff = Some(Duration::from_millis(100));

    let client = new_client(conf).await.unwrap();
    let _ = client.get_cache_names().await.unwrap();

    env.stop_node(0);
    tokio::time::sleep(Duration::from_millis(250)).await;

    let names = client.get_cache_names().await.unwrap();
    assert!(
        !names.iter().any(|name| name == "__unexpected__"),
        "expected retry on refreshed live address set to succeed",
    );

    env.start_node(0);
    env.wait_for_ready().await.unwrap();
}

/// Migrated from Apache Ignite `ReliableChannelTest.testChannelsNotReinitForStableDynamicAddressConfiguration`:
/// Java source: org.apache.ignite.internal.client.thin.ReliableChannelTest
#[tokio::test]
async fn should_reconnect_to_same_live_address_when_dynamic_address_set_is_stable() {
    let _guard = churn_lock()
        .lock()
        .expect("reliable-channel churn lock poisoned");
    let env = ignite_cluster3_churn_env();
    env.wait_for_ready().await.unwrap();

    let resolver = SequenceAddressResolver::new([
        vec![env.addresses()[0].clone()],
        vec![env.addresses()[0].clone()],
    ]);

    let mut conf = ClientConfig::from_addresses(std::iter::empty::<&str>());
    conf.address_resolver = Some(Arc::new(resolver));
    conf.partition_awareness_enabled = false;
    conf.retry_limit = 8;
    conf.reconnect_backoff = Some(Duration::from_millis(100));

    let client = new_client(conf).await.unwrap();
    let _ = client.get_cache_names().await.unwrap();

    env.stop_node(0);
    tokio::time::sleep(Duration::from_millis(250)).await;
    env.start_node(0);
    env.wait_for_ready().await.unwrap();

    let names = client.get_cache_names().await.unwrap();
    assert!(
        !names.iter().any(|name| name == "__unexpected__"),
        "expected stable dynamic live address refresh to reconnect to the same endpoint",
    );
}

/// Related original Apache Ignite reliable-channel coverage:
/// Java source: org.apache.ignite.internal.client.thin.ReliableChannelTest
#[tokio::test]
async fn should_connect_using_next_configured_address_when_first_is_unreachable() {
    let env = ignite_test_env();
    let addr = env.addr();
    let dead_addr = unused_local_addr();
    let conf = ClientConfig::from_addresses([dead_addr.as_str(), addr.as_str()]);

    let client = connect_with_config(conf).await.unwrap();
    let _ = client.get_cache_names().await.unwrap();
    assert_ne!(dead_addr, addr, "expected failover addresses to differ");
}

#[derive(Clone)]
struct SequenceAddressResolver {
    remaining: Arc<Mutex<VecDeque<Vec<String>>>>,
    last: Arc<Mutex<Vec<String>>>,
}

impl SequenceAddressResolver {
    fn new<const N: usize>(responses: [Vec<String>; N]) -> Self {
        Self {
            remaining: Arc::new(Mutex::new(VecDeque::from(responses))),
            last: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl AddressResolver for SequenceAddressResolver {
    fn addresses(&self) -> Vec<String> {
        let mut remaining = self
            .remaining
            .lock()
            .expect("address resolver mutex poisoned");
        if let Some(next) = remaining.pop_front() {
            *self
                .last
                .lock()
                .expect("address resolver last mutex poisoned") = next.clone();
            return next;
        }

        self.last
            .lock()
            .expect("address resolver last mutex poisoned")
            .clone()
    }
}

fn churn_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}
