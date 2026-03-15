#![cfg(not(feature = "ssl"))]

mod common;

use common::unused_local_addr;
use ignite_rs::{new_client, ClientConfig};

/// Java parity: org.apache.ignite.client.ConnectionTest#testEmptyNodeAddress
#[tokio::test]
async fn should_fail_on_empty_node_address() {
    let err = match new_client(ClientConfig::new("")).await {
        Ok(_) => panic!("expected an error for an empty node address"),
        Err(err) => err,
    };

    assert!(
        !err.to_string().is_empty(),
        "expected a connection error for an empty node address"
    );
}

/// Java parity: org.apache.ignite.client.ConnectionTest#testNullNodeAddresses
#[tokio::test]
async fn should_fail_when_no_node_addresses_are_configured() {
    let err = match new_client(ClientConfig::from_addresses(Vec::<String>::new())).await {
        Ok(_) => panic!("expected an error for an empty address list"),
        Err(err) => err,
    };

    assert!(
        err.to_string().contains("At least one address expected"),
        "unexpected error for empty address list: {}",
        err
    );
}

/// Java parity: org.apache.ignite.client.ConnectionTest#testNullNodeAddress
#[tokio::test]
async fn should_fail_on_null_node_address_analogue() {
    let err = match new_client(ClientConfig::from_addresses(vec![String::new()])).await {
        Ok(_) => panic!("expected an error for an empty address entry"),
        Err(err) => err,
    };

    assert!(
        err.to_string()
            .contains("At least one non-empty address expected"),
        "unexpected error for empty address entry: {}",
        err
    );
}

/// Java parity: org.apache.ignite.client.ConnectionTest#testInvalidNodeAddresses
#[tokio::test]
async fn should_fail_on_invalid_node_address() {
    let dead_addr1 = unused_local_addr();
    let dead_addr2 = unused_local_addr();

    let err = match new_client(ClientConfig::from_addresses([
        dead_addr1.as_str(),
        dead_addr2.as_str(),
    ]))
    .await
    {
        Ok(_) => panic!("expected an error for invalid node addresses"),
        Err(err) => err,
    };

    let message = err.to_string();
    assert!(
        message.contains("Connection")
            || message.contains("refused")
            || message.contains("failed")
            || message.contains(&dead_addr1)
            || message.contains(&dead_addr2),
        "unexpected error for invalid node addresses: {}",
        message
    );
}

/// Related local coverage for the Rust client password-only preflight path.
#[tokio::test]
async fn should_reject_password_without_username_before_handshake_write() {
    let mut conf = ClientConfig::new("127.0.0.1:10800");
    conf.password = Some("secret".to_string());

    let err = match new_client(conf).await {
        Ok(_) => panic!("expected an error for invalid credential pairing"),
        Err(err) => err,
    };

    assert!(
        err.to_string()
            .contains("Username expected when password is configured"),
        "unexpected error for password-only credential pairing: {}",
        err
    );
}
