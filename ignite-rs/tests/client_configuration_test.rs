#![cfg(not(feature = "ssl"))]

use ignite_rs::{AddressResolver, ClientConfig, ReconnectThrottle, RetryPolicy};
use std::sync::Arc;
use std::time::Duration;

// `org.apache.ignite.client.ClientConfigurationTest#testRebalanceThreadPoolSize`
// exercises embedded-node client mode and has no thin-client analogue in ignite-rs.

#[derive(Debug)]
struct StaticResolver;

impl AddressResolver for StaticResolver {
    fn addresses(&self) -> Vec<String> {
        vec!["127.0.0.1:10800".to_string()]
    }
}

/// Java parity: org.apache.ignite.client.ClientConfigurationTest#testSerialization
#[test]
fn should_preserve_configured_addresses() {
    let single = ClientConfig::new("127.0.0.1:10800");
    assert_eq!(single.addresses, vec!["127.0.0.1:10800".to_string()]);
    assert!(
        single.partition_awareness_enabled,
        "partition awareness should be enabled by default"
    );

    let multi = ClientConfig::from_addresses(["127.0.0.1:10800", "127.0.0.1:10801"]);
    assert_eq!(
        multi.addresses,
        vec!["127.0.0.1:10800".to_string(), "127.0.0.1:10801".to_string()]
    );
    assert!(
        multi.partition_awareness_enabled,
        "partition awareness should be enabled by default"
    );
}

/// Java parity: org.apache.ignite.client.ClientConfigurationTest#testSerialization
#[test]
fn should_preserve_transport_fields_when_cloned() {
    let mut conf = ClientConfig::from_addresses(["127.0.0.1:10800", "127.0.0.1:10801"]);
    conf.username = Some("user".to_string());
    conf.password = Some("pass".to_string());
    conf.user_attributes.insert(
        "ignite.internal.management-client".to_string(),
        "true".to_string(),
    );
    conf.handshake_timeout = Some(Duration::from_millis(123));
    conf.request_timeout = Some(Duration::from_millis(456));
    conf.heartbeat_enabled = true;
    conf.heartbeat_interval = Some(Duration::from_millis(789));
    conf.partition_awareness_enabled = false;
    conf.retry_policy = RetryPolicy::ReadOnly;
    conf.retry_limit = 4;
    conf.reconnect_throttle = Some(ReconnectThrottle {
        window: Duration::from_millis(654),
        max_attempts: 3,
    });
    conf.reconnect_backoff = Some(Duration::from_millis(321));
    conf.address_resolver = Some(Arc::new(StaticResolver));
    conf.tcp_nodelay = Some(true);
    conf.tcp_nonblocking = Some(false);

    let cloned = conf.clone();

    assert_eq!(cloned.addresses, conf.addresses);
    assert_eq!(cloned.username, conf.username);
    assert_eq!(cloned.password, conf.password);
    assert_eq!(cloned.user_attributes, conf.user_attributes);
    assert_eq!(cloned.handshake_timeout, conf.handshake_timeout);
    assert_eq!(cloned.request_timeout, conf.request_timeout);
    assert_eq!(cloned.heartbeat_enabled, conf.heartbeat_enabled);
    assert_eq!(cloned.heartbeat_interval, conf.heartbeat_interval);
    assert_eq!(
        cloned.partition_awareness_enabled,
        conf.partition_awareness_enabled
    );
    assert_eq!(cloned.retry_policy, conf.retry_policy);
    assert_eq!(cloned.retry_limit, conf.retry_limit);
    assert_eq!(cloned.reconnect_throttle, conf.reconnect_throttle);
    assert_eq!(cloned.reconnect_backoff, conf.reconnect_backoff);
    assert!(cloned.address_resolver.is_some());
    assert_eq!(cloned.tcp_nodelay, conf.tcp_nodelay);
    assert_eq!(cloned.tcp_nonblocking, conf.tcp_nonblocking);
}

/// Java parity: org.apache.ignite.client.ClientConfigurationTest#testInvalidHeartbeatIntervalThrows
#[tokio::test]
async fn should_reject_zero_heartbeat_interval() {
    let mut conf = ClientConfig::new("127.0.0.1:10800");
    conf.heartbeat_enabled = true;
    conf.heartbeat_interval = Some(Duration::ZERO);

    let err = match ignite_rs::new_client(conf).await {
        Ok(_) => panic!("expected zero heartbeat interval validation to fail"),
        Err(err) => err,
    };
    assert!(
        err.to_string()
            .contains("heartbeat_interval cannot be zero"),
        "unexpected zero-heartbeat validation error: {}",
        err
    );
}

#[tokio::test]
async fn should_reject_zero_handshake_timeout() {
    let mut conf = ClientConfig::new("127.0.0.1:10800");
    conf.handshake_timeout = Some(Duration::ZERO);

    let err = match ignite_rs::new_client(conf).await {
        Ok(_) => panic!("expected zero handshake timeout validation to fail"),
        Err(err) => err,
    };
    assert!(
        err.to_string().contains("handshake_timeout cannot be zero"),
        "unexpected zero-handshake validation error: {}",
        err
    );
}

#[tokio::test]
async fn should_reject_zero_request_timeout() {
    let mut conf = ClientConfig::new("127.0.0.1:10800");
    conf.request_timeout = Some(Duration::ZERO);

    let err = match ignite_rs::new_client(conf).await {
        Ok(_) => panic!("expected zero request timeout validation to fail"),
        Err(err) => err,
    };
    assert!(
        err.to_string().contains("request_timeout cannot be zero"),
        "unexpected zero-request validation error: {}",
        err
    );
}

#[tokio::test]
async fn should_reject_zero_reconnect_throttle_window() {
    let mut conf = ClientConfig::new("127.0.0.1:10800");
    conf.reconnect_throttle = Some(ReconnectThrottle {
        window: Duration::ZERO,
        max_attempts: 1,
    });

    let err = match ignite_rs::new_client(conf).await {
        Ok(_) => panic!("expected zero reconnect throttle window validation to fail"),
        Err(err) => err,
    };
    assert!(
        err.to_string()
            .contains("reconnect_throttle.window cannot be zero"),
        "unexpected zero reconnect throttle validation error: {}",
        err
    );
}
