#![cfg(not(feature = "ssl"))]

mod common;

use common::{
    connect, connect_profile, delayed_handshake_env, ensure_rainbow_table, ignite_test_env,
    IgniteProfile,
};
use ignite_rs::query::SqlFieldsQuery;
use ignite_rs::{new_client, ClientConfig, RetryPolicy};
use std::io::{Cursor, Read, Write};
use std::net::TcpStream;
use std::time::Duration;

const HANDSHAKE_OP_CODE: u8 = 1;
const CLIENT_CODE: u8 = 2;
const FEATURE_QRY_PARTITIONS_BATCH_SIZE: usize = 7;
const FEATURE_QRY_INITIATOR_ID: usize = 23;

#[tokio::test]
async fn should_start_single_node_fixture_and_accept_thin_client_connections() {
    let env = ignite_test_env();
    env.wait_for_ready().await.unwrap();

    let client = connect_profile(IgniteProfile::DefaultSingleNode)
        .await
        .unwrap();
    let names = client.get_cache_names().await.unwrap();

    assert!(
        !names.iter().any(|name| name == "__unexpected__"),
        "unexpected cache names payload from single-node fixture: {:?}",
        names
    );
}

#[tokio::test]
async fn should_bootstrap_rainbow_sql_fixture_idempotently() {
    let client = connect().await.unwrap();

    ensure_rainbow_table(&client).await.unwrap();
    ensure_rainbow_table(&client).await.unwrap();

    let rows = client
        .sql_fields::<i64>(SqlFieldsQuery::new("SELECT COUNT(*) FROM rainbow"))
        .await
        .unwrap()
        .fetch_all()
        .await
        .unwrap();

    assert_eq!(rows, vec![1]);
}

#[tokio::test]
#[ignore = "diagnostic probe: the managed apacheignite/ignite:2.15.0 fixture does not advertise QRY_INITIATOR_ID; run manually when validating an external source-matched Ignite build"]
async fn should_advertise_query_initiator_features_in_live_handshake() {
    let env = ignite_test_env();
    env.wait_for_ready().await.unwrap();

    let mut stream = TcpStream::connect(env.addr()).expect("failed to connect to Ignite fixture");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("failed to set read timeout");
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .expect("failed to set write timeout");

    let request =
        build_handshake_request(&[FEATURE_QRY_PARTITIONS_BATCH_SIZE, FEATURE_QRY_INITIATOR_ID]);
    stream
        .write_all(&request)
        .expect("failed to write handshake request");
    stream.flush().expect("failed to flush handshake request");

    let mut len_buf = [0u8; 4];
    stream
        .read_exact(&mut len_buf)
        .expect("failed to read handshake length");
    let body_len = i32::from_le_bytes(len_buf) as usize;
    let mut body = vec![0u8; body_len];
    stream
        .read_exact(&mut body)
        .expect("failed to read handshake body");

    let mut cursor = Cursor::new(body);
    let mut success = [0u8; 1];
    cursor
        .read_exact(&mut success)
        .expect("failed to read handshake status");
    assert_eq!(success[0], 1, "expected successful handshake response");

    let feature_type = read_u8(&mut cursor);
    assert_eq!(
        feature_type,
        ignite_rs::protocol::TypeCode::ArrByte as u8,
        "expected byte-array feature bitmap in handshake response"
    );
    let feature_len = read_i32(&mut cursor) as usize;
    let mut features = vec![0u8; feature_len];
    cursor
        .read_exact(&mut features)
        .expect("failed to read handshake feature bitmap");

    assert!(
        feature_supported(&features, FEATURE_QRY_PARTITIONS_BATCH_SIZE),
        "live fixture did not advertise QRY_PARTITIONS_BATCH_SIZE"
    );
    assert!(
        feature_supported(&features, FEATURE_QRY_INITIATOR_ID),
        "live fixture did not advertise QRY_INITIATOR_ID"
    );
}

#[tokio::test]
async fn should_expose_delayed_handshake_proxy_against_live_single_node() {
    let env = ignite_test_env();
    env.wait_for_ready().await.unwrap();

    let delayed = delayed_handshake_env(Duration::from_millis(750)).unwrap();
    let mut conf = ClientConfig::new(delayed.addr());
    conf.handshake_timeout = Some(Duration::from_millis(150));
    conf.retry_policy = RetryPolicy::Never;

    let err = match new_client(conf).await {
        Ok(_) => panic!("expected delayed-handshake proxy to exceed handshake timeout"),
        Err(err) => err,
    };
    let msg = err.to_string();
    assert!(
        msg.contains("timeout") || msg.contains("timed out") || msg.contains("handshake"),
        "unexpected delayed-handshake failure: {}",
        msg
    );
}

fn build_handshake_request(feature_bits: &[usize]) -> Vec<u8> {
    let mut features = Vec::new();
    for bit in feature_bits {
        set_feature_bit(&mut features, *bit);
    }

    let mut req = Vec::new();
    req.extend_from_slice(&0i32.to_le_bytes());
    req.push(HANDSHAKE_OP_CODE);
    req.extend_from_slice(&1i16.to_le_bytes());
    req.extend_from_slice(&7i16.to_le_bytes());
    req.extend_from_slice(&0i16.to_le_bytes());
    req.push(CLIENT_CODE);
    req.push(ignite_rs::protocol::TypeCode::ArrByte as u8);
    req.extend_from_slice(&(features.len() as i32).to_le_bytes());
    req.extend_from_slice(&features);

    let payload_len = (req.len() - 4) as i32;
    req[..4].copy_from_slice(&payload_len.to_le_bytes());
    req
}

fn set_feature_bit(features: &mut Vec<u8>, bit: usize) {
    let byte_index = bit / 8;
    if features.len() <= byte_index {
        features.resize(byte_index + 1, 0);
    }
    features[byte_index] |= 1 << (bit % 8);
}

fn feature_supported(features: &[u8], bit: usize) -> bool {
    let byte_index = bit / 8;
    features
        .get(byte_index)
        .map(|byte| (byte & (1 << (bit % 8))) != 0)
        .unwrap_or(false)
}

fn read_u8(reader: &mut impl Read) -> u8 {
    let mut buf = [0u8; 1];
    reader.read_exact(&mut buf).expect("failed to read u8");
    buf[0]
}

fn read_i32(reader: &mut impl Read) -> i32 {
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf).expect("failed to read i32");
    i32::from_le_bytes(buf)
}
