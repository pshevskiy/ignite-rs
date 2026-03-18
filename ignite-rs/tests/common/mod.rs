#![allow(dead_code)]

use ignite_rs::cache::Cache;
use ignite_rs::error::{IgniteError, IgniteResult};
use ignite_rs::events::ClientEvent;
use ignite_rs::protocol::{write_bool, write_i32, write_u8, TypeCode};
use ignite_rs::query::CacheEntryEventType;
use ignite_rs::query::SqlFieldsQuery;
use ignite_rs::{Client, WritableType};
use std::collections::{HashMap, VecDeque};
use std::convert::TryInto;
use std::io::{self, Read, Write};
use std::net::TcpListener;
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

mod fixtures;

#[allow(unused_imports)]
pub use fixtures::{
    connect, connect_auth, connect_cluster3, connect_cluster3_churn, connect_profile,
    connect_with_cluster3_churn_config, connect_with_cluster3_config, connect_with_config,
    delayed_handshake_env, ensure_rainbow_table, ignite_auth_env, ignite_cluster3_churn_env,
    ignite_cluster3_env, ignite_context, ignite_scope, ignite_single_node_churn_env,
    ignite_test_env, FixtureScope, IgniteClusterEnv, IgniteContext, IgniteProfile, IgniteScope,
    TestClient,
};
#[cfg(feature = "ssl")]
#[allow(unused_imports)]
pub use fixtures::{connect_mtls, connect_tls, ignite_mtls_env, ignite_tls_env};
#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use fixtures::{
    debug_context_registry_contains, debug_context_registry_key, debug_profile_descriptor,
    debug_prune_context_registry,
};

pub const SQL_SCHEMA: &str = "PUBLIC";
pub const SQL_PAGE_SIZE: i32 = 32;

static NEXT_FIXTURE_ID: AtomicUsize = AtomicUsize::new(0);

pub struct SqlTableFixture {
    pub bootstrap_cache_name: String,
    pub bootstrap_cache: Cache<i32, i32>,
    pub table_name: String,
    pub cache_name: String,
}

impl SqlTableFixture {
    pub async fn create_seeded(client: &Client) -> IgniteResult<Self> {
        Self::create_impl(client, true).await
    }

    pub async fn create_empty(client: &Client) -> IgniteResult<Self> {
        Self::create_impl(client, false).await
    }

    async fn create_impl(client: &Client, seed_first_row: bool) -> IgniteResult<Self> {
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

        if seed_first_row {
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
        }

        let row_count = execute_sql(
            &bootstrap_cache,
            &format!("SELECT COUNT(*) FROM {}", table_name),
        )
        .await?;
        let expected_rows = if seed_first_row { 1 } else { 0 };
        if row_count.as_slice() != [expected_rows] {
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

    pub async fn insert_row(&self, big: i64) -> IgniteResult<()> {
        execute_sql(
            &self.bootstrap_cache,
            &format!(
                "INSERT INTO {} (big, bool, dec, int, null_int, small, char, var, ts) \
                 VALUES ({}, true, 2.0, {}, null, 4, 'c', 'varchar{}', \
                 timestamp '2023-06-21 12:34:56 UTC')",
                self.table_name,
                big,
                big + 2,
                big
            ),
        )
        .await?;
        Ok(())
    }

    pub async fn cleanup(&self, client: &Client) {
        let _ = execute_sql(
            &self.bootstrap_cache,
            &format!("DROP TABLE {}", self.table_name),
        )
        .await;
        let _ = client.destroy_cache(&self.bootstrap_cache_name).await;
    }
}

pub fn unique_name(prefix: &str) -> String {
    format!(
        "{}_{}_{}",
        prefix,
        std::process::id(),
        NEXT_FIXTURE_ID.fetch_add(1, Ordering::Relaxed)
    )
}

pub async fn destroy_cache_if_exists(client: &Client, cache_name: &str) {
    let _ = client.destroy_cache(cache_name).await;
}

pub async fn execute_sql(cache: &Cache<i32, i32>, sql: &str) -> IgniteResult<Vec<i64>> {
    cache
        .sql_fields(
            SqlFieldsQuery::<i64>::new(sql)
                .with_schema(SQL_SCHEMA)
                .with_page_size(SQL_PAGE_SIZE),
        )
        .await?
        .fetch_all()
        .await
}

pub fn encode_typed_payload(value: &impl WritableType) -> Vec<u8> {
    let mut bytes = Vec::new();
    value
        .write(&mut bytes)
        .expect("failed to encode typed mock payload");
    bytes
}

pub fn encode_null_payload() -> Vec<u8> {
    vec![TypeCode::Null as u8]
}

pub fn encode_continuous_query_events<K: WritableType, V: WritableType>(
    events: &[(K, Option<V>, Option<V>, CacheEntryEventType)],
) -> Vec<u8> {
    let mut payload = Vec::new();
    write_i32(&mut payload, events.len() as i32).expect("failed to encode event count");

    for (key, old_value, value, event_type) in events {
        key.write(&mut payload)
            .expect("failed to encode continuous query key");
        old_value
            .write(&mut payload)
            .expect("failed to encode continuous query old value");
        value
            .write(&mut payload)
            .expect("failed to encode continuous query value");

        let code = match event_type {
            CacheEntryEventType::Created => 0,
            CacheEntryEventType::Updated => 1,
            CacheEntryEventType::Removed => 2,
            CacheEntryEventType::Expired => 3,
        };
        write_u8(&mut payload, code).expect("failed to encode continuous query event type");
    }

    payload
}

pub fn encode_entry_cursor_open<K: WritableType, V: WritableType>(
    cursor_id: i64,
    rows: &[(K, V)],
    has_more: bool,
) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&cursor_id.to_le_bytes());
    write_i32(&mut payload, rows.len() as i32).expect("failed to encode cursor row count");

    for (key, value) in rows {
        key.write(&mut payload)
            .expect("failed to encode cursor row key");
        value
            .write(&mut payload)
            .expect("failed to encode cursor row value");
    }

    write_bool(&mut payload, has_more).expect("failed to encode cursor has_more");
    payload
}

pub fn encode_cache_partitions_response(
    topology_version: MockTopologyVersion,
    cache_id: i32,
    node_partitions: &[(MockUuid, &[i32])],
) -> Vec<u8> {
    encode_cache_partitions_response_with_dc(
        topology_version,
        cache_id,
        node_partitions,
        Some(node_partitions),
    )
}

pub fn encode_cache_partitions_response_with_dc(
    topology_version: MockTopologyVersion,
    cache_id: i32,
    primary_node_partitions: &[(MockUuid, &[i32])],
    dc_node_partitions: Option<&[(MockUuid, &[i32])]>,
) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&topology_version.major.to_le_bytes());
    payload.extend_from_slice(&topology_version.minor.to_le_bytes());
    payload.extend_from_slice(&1i32.to_le_bytes());
    write_bool(&mut payload, true).expect("failed to encode applicable flag");
    payload.extend_from_slice(&1i32.to_le_bytes());
    payload.extend_from_slice(&cache_id.to_le_bytes());
    payload.extend_from_slice(&0i32.to_le_bytes());
    encode_partition_map(&mut payload, primary_node_partitions);
    encode_partition_map(&mut payload, dc_node_partitions.unwrap_or(&[]));

    payload
}

fn encode_partition_map(payload: &mut Vec<u8>, node_partitions: &[(MockUuid, &[i32])]) {
    payload.extend_from_slice(&(node_partitions.len() as i32).to_le_bytes());

    for (node_id, partitions) in node_partitions {
        payload.extend_from_slice(&node_id.most.to_le_bytes());
        payload.extend_from_slice(&node_id.least.to_le_bytes());
        payload.extend_from_slice(&(partitions.len() as i32).to_le_bytes());
        for partition in *partitions {
            payload.extend_from_slice(&partition.to_le_bytes());
        }
    }
}

pub fn unused_local_addr() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("failed to reserve a local port");
    let addr = listener
        .local_addr()
        .expect("failed to read reserved local address")
        .to_string();
    drop(listener);
    addr
}

pub fn spawn_dummy_tcp_server() -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("failed to bind dummy TCP server");
    let addr = listener
        .local_addr()
        .expect("failed to read dummy TCP server address")
        .to_string();
    let handle = thread::spawn(move || {
        if let Ok((_stream, _peer)) = listener.accept() {
            thread::sleep(Duration::from_secs(2));
        }
    });
    (addr, handle)
}

pub async fn recv_event(
    receiver: &mut tokio::sync::broadcast::Receiver<ClientEvent>,
) -> ClientEvent {
    tokio::time::timeout(Duration::from_secs(1), receiver.recv())
        .await
        .expect("timed out waiting for client event")
        .expect("client event channel closed unexpectedly")
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MockUuid {
    pub most: i64,
    pub least: i64,
}

impl MockUuid {
    pub const fn new(most: i64, least: i64) -> Self {
        Self { most, least }
    }

    pub fn as_string(self) -> String {
        let most = self.most as u64;
        let least = self.least as u64;
        format!(
            "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
            (most >> 32) as u32,
            ((most >> 16) & 0xffff) as u16,
            (most & 0xffff) as u16,
            (least >> 48) as u16,
            least & 0x0000_ffff_ffff_ffff,
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MockTopologyVersion {
    pub major: i64,
    pub minor: i32,
}

#[derive(Clone, Debug)]
pub struct MockDiscoveryNode {
    pub node_id: MockUuid,
    pub port: i32,
    pub addresses: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct MockDiscoveryResponse {
    pub topology_version: i64,
    pub added_nodes: Vec<MockDiscoveryNode>,
    pub removed_node_ids: Vec<MockUuid>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordedCacheRequest {
    pub op_code: i16,
    pub cache_id: i32,
    pub flags: u8,
    pub tx_id: Option<i32>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordedTxStartRequest {
    pub concurrency: u8,
    pub isolation: u8,
    pub timeout_ms: i64,
    pub label: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordedTxEndRequest {
    pub tx_id: i32,
    pub committed: bool,
}

#[derive(Clone, Debug)]
pub enum MockResponse {
    Success {
        payload: Vec<u8>,
        topology_change: Option<MockTopologyVersion>,
        notifications: Vec<MockNotification>,
    },
    Failure(String),
}

impl MockResponse {
    pub fn success(payload: Vec<u8>) -> Self {
        Self::Success {
            payload,
            topology_change: None,
            notifications: Vec::new(),
        }
    }

    pub fn success_with_topology(payload: Vec<u8>, topology_change: MockTopologyVersion) -> Self {
        Self::Success {
            payload,
            topology_change: Some(topology_change),
            notifications: Vec::new(),
        }
    }

    pub fn success_with_notifications(
        payload: Vec<u8>,
        notifications: Vec<MockNotification>,
    ) -> Self {
        Self::Success {
            payload,
            topology_change: None,
            notifications,
        }
    }

    pub fn failure(message: impl Into<String>) -> Self {
        Self::Failure(message.into())
    }
}

#[derive(Clone, Debug)]
pub struct MockNotification {
    pub op_code: i16,
    pub resource_id: i64,
    pub payload: Vec<u8>,
    pub error: Option<String>,
    pub delay: Option<Duration>,
}

impl MockNotification {
    pub fn success(op_code: i16, resource_id: i64, payload: Vec<u8>) -> Self {
        Self {
            op_code,
            resource_id,
            payload,
            error: None,
            delay: None,
        }
    }

    pub fn success_with_delay(
        op_code: i16,
        resource_id: i64,
        payload: Vec<u8>,
        delay: Duration,
    ) -> Self {
        Self {
            op_code,
            resource_id,
            payload,
            error: None,
            delay: Some(delay),
        }
    }

    pub fn failure(op_code: i16, resource_id: i64, message: impl Into<String>) -> Self {
        Self {
            op_code,
            resource_id,
            payload: Vec::new(),
            error: Some(message.into()),
            delay: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct MockThinServerConfig {
    pub server_idle_timeout: Option<Duration>,
    pub advertised_idle_timeout: Option<Duration>,
    pub accept_after_idle_close: bool,
    pub handshake_failure: Option<(String, i32)>,
    pub expected_username: Option<String>,
    pub expected_password: Option<String>,
    pub cache_names_delay: Option<Duration>,
    pub cache_names_delays: Option<Arc<Mutex<VecDeque<Duration>>>>,
    pub cache_names_disconnects: Option<Arc<Mutex<VecDeque<bool>>>>,
    pub cache_get_error: Option<String>,
    pub cache_get_responses: Option<Arc<Mutex<VecDeque<MockResponse>>>>,
    pub cache_get_disconnects: Option<Arc<Mutex<VecDeque<bool>>>>,
    pub cache_contains_key_responses: Option<Arc<Mutex<VecDeque<MockResponse>>>>,
    pub cache_put_delays: Option<Arc<Mutex<VecDeque<Duration>>>>,
    pub cache_put_disconnects: Option<Arc<Mutex<VecDeque<bool>>>>,
    pub cache_put_error: Option<String>,
    pub cache_put_responses: Option<Arc<Mutex<VecDeque<MockResponse>>>>,
    pub node_id: MockUuid,
    pub recovery_mode: bool,
    pub topology_change_on_cache_names: Option<MockTopologyVersion>,
    pub topology_changes_on_cache_names: Option<Arc<Mutex<VecDeque<MockTopologyVersion>>>>,
    pub discovery_response: Option<MockDiscoveryResponse>,
    pub discovery_responses: Option<Arc<Mutex<VecDeque<MockDiscoveryResponse>>>>,
    pub data_center_nodes_response: Option<Vec<MockUuid>>,
    pub data_center_nodes_responses: Option<Arc<Mutex<VecDeque<Vec<MockUuid>>>>>,
    pub cache_partitions_responses: Option<Arc<Mutex<VecDeque<MockResponse>>>>,
    pub tx_start_responses: Option<Arc<Mutex<VecDeque<MockResponse>>>>,
    pub tx_end_responses: Option<Arc<Mutex<VecDeque<MockResponse>>>>,
    pub query_scan_responses: Option<Arc<Mutex<VecDeque<MockResponse>>>>,
    pub query_scan_page_responses: Option<Arc<Mutex<VecDeque<MockResponse>>>>,
    pub query_sql_responses: Option<Arc<Mutex<VecDeque<MockResponse>>>>,
    pub query_sql_page_responses: Option<Arc<Mutex<VecDeque<MockResponse>>>>,
    pub query_sql_fields_responses: Option<Arc<Mutex<VecDeque<MockResponse>>>>,
    pub query_sql_fields_page_responses: Option<Arc<Mutex<VecDeque<MockResponse>>>>,
    pub opcode_responses: Option<Arc<Mutex<HashMap<i16, VecDeque<MockResponse>>>>>,
}

impl Default for MockThinServerConfig {
    fn default() -> Self {
        Self {
            server_idle_timeout: None,
            advertised_idle_timeout: None,
            accept_after_idle_close: true,
            handshake_failure: None,
            expected_username: None,
            expected_password: None,
            cache_names_delay: None,
            cache_names_delays: None,
            cache_names_disconnects: None,
            cache_get_error: None,
            cache_get_responses: None,
            cache_get_disconnects: None,
            cache_contains_key_responses: None,
            cache_put_delays: None,
            cache_put_disconnects: None,
            cache_put_error: None,
            cache_put_responses: None,
            node_id: MockUuid::new(1, 1),
            recovery_mode: false,
            topology_change_on_cache_names: None,
            topology_changes_on_cache_names: None,
            discovery_response: None,
            discovery_responses: None,
            data_center_nodes_response: None,
            data_center_nodes_responses: None,
            cache_partitions_responses: None,
            tx_start_responses: None,
            tx_end_responses: None,
            query_scan_responses: None,
            query_scan_page_responses: None,
            query_sql_responses: None,
            query_sql_page_responses: None,
            query_sql_fields_responses: None,
            query_sql_fields_page_responses: None,
            opcode_responses: None,
        }
    }
}

pub struct MockThinServer {
    addr: String,
    handshake_count: Arc<AtomicUsize>,
    heartbeat_count: Arc<AtomicUsize>,
    query_close_count: Arc<AtomicUsize>,
    active_connection_count: Arc<AtomicUsize>,
    recorded_handshake_credentials: Arc<Mutex<Vec<(Option<String>, Option<String>)>>>,
    recorded_opcode_payloads: Arc<Mutex<HashMap<i16, Vec<Vec<u8>>>>>,
    recorded_cache_requests: Arc<Mutex<Vec<RecordedCacheRequest>>>,
    recorded_tx_starts: Arc<Mutex<Vec<RecordedTxStartRequest>>>,
    recorded_tx_ends: Arc<Mutex<Vec<RecordedTxEndRequest>>>,
    stop: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}

impl MockThinServer {
    pub fn addr(&self) -> &str {
        &self.addr
    }

    pub fn handshake_count(&self) -> usize {
        self.handshake_count.load(Ordering::Relaxed)
    }

    pub fn heartbeat_count(&self) -> usize {
        self.heartbeat_count.load(Ordering::Relaxed)
    }

    pub fn query_close_count(&self) -> usize {
        self.query_close_count.load(Ordering::Relaxed)
    }

    pub fn active_connection_count(&self) -> usize {
        self.active_connection_count.load(Ordering::Relaxed)
    }

    pub fn recorded_handshake_credentials(&self) -> Vec<(Option<String>, Option<String>)> {
        self.recorded_handshake_credentials
            .lock()
            .expect("mock recorded_handshake_credentials mutex poisoned")
            .clone()
    }

    pub fn recorded_opcode_payloads(&self, op_code: i16) -> Vec<Vec<u8>> {
        self.recorded_opcode_payloads
            .lock()
            .expect("mock recorded_opcode_payloads mutex poisoned")
            .get(&op_code)
            .cloned()
            .unwrap_or_default()
    }

    pub fn recorded_cache_requests(&self) -> Vec<RecordedCacheRequest> {
        self.recorded_cache_requests
            .lock()
            .expect("mock recorded_cache_requests mutex poisoned")
            .clone()
    }

    pub fn recorded_tx_starts(&self) -> Vec<RecordedTxStartRequest> {
        self.recorded_tx_starts
            .lock()
            .expect("mock recorded_tx_starts mutex poisoned")
            .clone()
    }

    pub fn recorded_tx_ends(&self) -> Vec<RecordedTxEndRequest> {
        self.recorded_tx_ends
            .lock()
            .expect("mock recorded_tx_ends mutex poisoned")
            .clone()
    }
}

impl Drop for MockThinServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        let _ = TcpStream::connect(&self.addr);

        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

pub fn spawn_mock_thin_server(config: MockThinServerConfig) -> MockThinServer {
    let listener = TcpListener::bind("127.0.0.1:0").expect("failed to bind mock thin server");
    spawn_mock_thin_server_with_listener(listener, config)
}

pub fn spawn_mock_thin_server_on_addr(addr: &str, config: MockThinServerConfig) -> MockThinServer {
    let listener =
        TcpListener::bind(addr).expect("failed to bind mock thin server on requested address");
    spawn_mock_thin_server_with_listener(listener, config)
}

pub fn spawn_mock_thin_server_with_listener(
    listener: TcpListener,
    config: MockThinServerConfig,
) -> MockThinServer {
    listener
        .set_nonblocking(true)
        .expect("failed to set mock thin server listener nonblocking");
    let addr = listener
        .local_addr()
        .expect("failed to read mock thin server local address")
        .to_string();

    let heartbeat_count = Arc::new(AtomicUsize::new(0));
    let handshake_count = Arc::new(AtomicUsize::new(0));
    let query_close_count = Arc::new(AtomicUsize::new(0));
    let active_connection_count = Arc::new(AtomicUsize::new(0));
    let recorded_handshake_credentials = Arc::new(Mutex::new(Vec::new()));
    let recorded_opcode_payloads = Arc::new(Mutex::new(HashMap::new()));
    let recorded_cache_requests = Arc::new(Mutex::new(Vec::new()));
    let recorded_tx_starts = Arc::new(Mutex::new(Vec::new()));
    let recorded_tx_ends = Arc::new(Mutex::new(Vec::new()));
    let stop = Arc::new(AtomicBool::new(false));
    let thread_heartbeat_count = heartbeat_count.clone();
    let thread_handshake_count = handshake_count.clone();
    let thread_query_close_count = query_close_count.clone();
    let thread_active_connection_count = active_connection_count.clone();
    let thread_recorded_handshake_credentials = recorded_handshake_credentials.clone();
    let thread_recorded_opcode_payloads = recorded_opcode_payloads.clone();
    let thread_recorded_cache_requests = recorded_cache_requests.clone();
    let thread_recorded_tx_starts = recorded_tx_starts.clone();
    let thread_recorded_tx_ends = recorded_tx_ends.clone();
    let thread_stop = stop.clone();

    let handle = thread::spawn(move || {
        while !thread_stop.load(Ordering::Acquire) {
            match listener.accept() {
                Ok((mut stream, _peer)) => {
                    thread_active_connection_count.fetch_add(1, Ordering::Relaxed);
                    let conn_config = config.clone();
                    let conn_handshake_count = thread_handshake_count.clone();
                    let conn_heartbeat_count = thread_heartbeat_count.clone();
                    let conn_query_close_count = thread_query_close_count.clone();
                    let conn_active_connection_count = thread_active_connection_count.clone();
                    let conn_recorded_handshake_credentials =
                        thread_recorded_handshake_credentials.clone();
                    let conn_recorded_opcode_payloads = thread_recorded_opcode_payloads.clone();
                    let conn_recorded_cache_requests = thread_recorded_cache_requests.clone();
                    let conn_recorded_tx_starts = thread_recorded_tx_starts.clone();
                    let conn_recorded_tx_ends = thread_recorded_tx_ends.clone();
                    let conn_stop = thread_stop.clone();

                    thread::spawn(move || {
                        let _ = handle_mock_connection(
                            &mut stream,
                            conn_config,
                            &conn_handshake_count,
                            &conn_heartbeat_count,
                            &conn_query_close_count,
                            &conn_recorded_handshake_credentials,
                            &conn_recorded_opcode_payloads,
                            &conn_recorded_cache_requests,
                            &conn_recorded_tx_starts,
                            &conn_recorded_tx_ends,
                            &conn_stop,
                        );

                        conn_active_connection_count.fetch_sub(1, Ordering::Relaxed);
                    });
                }
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(25));
                }
                Err(_) => break,
            }
        }
    });

    MockThinServer {
        addr,
        handshake_count,
        heartbeat_count,
        query_close_count,
        active_connection_count,
        recorded_handshake_credentials,
        recorded_opcode_payloads,
        recorded_cache_requests,
        recorded_tx_starts,
        recorded_tx_ends,
        stop,
        handle: Some(handle),
    }
}

fn handle_mock_connection(
    stream: &mut TcpStream,
    config: MockThinServerConfig,
    handshake_count: &Arc<AtomicUsize>,
    heartbeat_count: &Arc<AtomicUsize>,
    query_close_count: &Arc<AtomicUsize>,
    recorded_handshake_credentials: &Arc<Mutex<Vec<(Option<String>, Option<String>)>>>,
    recorded_opcode_payloads: &Arc<Mutex<HashMap<i16, Vec<Vec<u8>>>>>,
    recorded_cache_requests: &Arc<Mutex<Vec<RecordedCacheRequest>>>,
    recorded_tx_starts: &Arc<Mutex<Vec<RecordedTxStartRequest>>>,
    recorded_tx_ends: &Arc<Mutex<Vec<RecordedTxEndRequest>>>,
    stop: &Arc<AtomicBool>,
) -> bool {
    stream
        .set_read_timeout(Some(Duration::from_millis(50)))
        .expect("failed to set mock thin server read timeout");

    let mut handshake_len_bytes = [0u8; 4];
    if !read_frame_prefix(stream, &mut handshake_len_bytes, stop) {
        return true;
    }
    let handshake_len = i32::from_le_bytes(handshake_len_bytes) as usize;
    let mut handshake = vec![0u8; handshake_len];
    if !read_frame_body(stream, &mut handshake, stop) {
        return true;
    }

    let handshake_attrs = parse_handshake_user_attributes(&handshake)
        .expect("failed to parse mock thin client handshake attributes");
    let (username, password) = parse_handshake_credentials(&handshake)
        .expect("failed to parse mock thin client handshake credentials");
    recorded_handshake_credentials
        .lock()
        .expect("mock recorded_handshake_credentials mutex poisoned")
        .push((username.clone(), password.clone()));

    if let Some((msg, err_code)) = &config.handshake_failure {
        write_handshake_failure(stream, msg, *err_code)
            .expect("failed to write mock handshake failure");
        return true;
    }

    if config.expected_username.is_some() || config.expected_password.is_some() {
        if username != config.expected_username || password != config.expected_password {
            write_handshake_failure(stream, "Authentication failed", 1)
                .expect("failed to write mock auth handshake failure");
            return true;
        }
    }

    if config.recovery_mode
        && handshake_attrs.get("ignite.internal.management-client") != Some(&"true".to_string())
    {
        write_handshake_failure(stream, "Node in recovery mode.", 11)
            .expect("failed to write mock recovery handshake failure");
        return true;
    }

    write_handshake_success(stream, config.node_id)
        .expect("failed to write mock handshake response");
    handshake_count.fetch_add(1, Ordering::Relaxed);

    let advertised_idle_timeout = config
        .advertised_idle_timeout
        .or(config.server_idle_timeout)
        .unwrap_or(Duration::ZERO);
    let mut last_activity = std::time::Instant::now();
    loop {
        if stop.load(Ordering::Acquire) {
            return false;
        }

        if let Some(idle_timeout) = config.server_idle_timeout {
            if last_activity.elapsed() > idle_timeout {
                return config.accept_after_idle_close;
            }
        }

        let mut len_buf = [0u8; 4];
        match read_exact_or_timeout(stream, &mut len_buf, stop) {
            Ok(ReadOutcome::Data) => {}
            Ok(ReadOutcome::Timeout) => continue,
            Ok(ReadOutcome::Closed) => return true,
            Err(_) => return true,
        }

        let frame_len = i32::from_le_bytes(len_buf) as usize;
        let mut frame = vec![0u8; frame_len];
        if !read_frame_body(stream, &mut frame, stop) {
            return true;
        }

        last_activity = std::time::Instant::now();

        let op_code = i16::from_le_bytes([frame[0], frame[1]]);
        let corr_id = i64::from_le_bytes([
            frame[2], frame[3], frame[4], frame[5], frame[6], frame[7], frame[8], frame[9],
        ]);
        let payload = &frame[10..];

        recorded_opcode_payloads
            .lock()
            .expect("mock recorded_opcode_payloads mutex poisoned")
            .entry(op_code)
            .or_default()
            .push(payload.to_vec());

        if op_code != 0 {
            if let Some(response) = next_opcode_response(&config, op_code) {
                write_mock_response(stream, corr_id, response)
                    .expect("failed to write queued opcode response");
                continue;
            }
        }

        match op_code {
            1 => {
                heartbeat_count.fetch_add(1, Ordering::Relaxed);
                write_success_response(stream, corr_id, &[], None)
                    .expect("failed to write heartbeat");
            }
            2 => {
                write_success_response(
                    stream,
                    corr_id,
                    &(advertised_idle_timeout.as_millis() as i64).to_le_bytes(),
                    None,
                )
                .expect("failed to write idle timeout");
            }
            5102 => {
                let discovery = next_discovery_response(&config);
                write_discovery_response(stream, corr_id, &discovery)
                    .expect("failed to write discovery response");
            }
            5103 => {
                let node_ids = next_data_center_nodes_response(&config);
                write_data_center_nodes_response(stream, corr_id, &node_ids)
                    .expect("failed to write data center nodes response");
            }
            1101 => {
                write_mock_response(
                    stream,
                    corr_id,
                    next_mock_response(&config.cache_partitions_responses).unwrap_or_else(|| {
                        MockResponse::failure("unsupported cache partitions opcode")
                    }),
                )
                .expect("failed to write cache partitions response");
            }
            4000 => {
                if let Some(request) = parse_tx_start_request(payload) {
                    recorded_tx_starts
                        .lock()
                        .expect("mock recorded_tx_starts mutex poisoned")
                        .push(request);
                }

                write_mock_response(
                    stream,
                    corr_id,
                    next_mock_response(&config.tx_start_responses)
                        .unwrap_or_else(|| MockResponse::success(1i32.to_le_bytes().to_vec())),
                )
                .expect("failed to write tx start response");
            }
            4001 => {
                if let Some(request) = parse_tx_end_request(payload) {
                    recorded_tx_ends
                        .lock()
                        .expect("mock recorded_tx_ends mutex poisoned")
                        .push(request);
                }

                write_mock_response(
                    stream,
                    corr_id,
                    next_mock_response(&config.tx_end_responses)
                        .unwrap_or_else(|| MockResponse::success(Vec::new())),
                )
                .expect("failed to write tx end response");
            }
            2000 => {
                record_cache_request(recorded_cache_requests, op_code, payload);
                write_mock_response(
                    stream,
                    corr_id,
                    next_mock_response(&config.query_scan_responses)
                        .unwrap_or_else(|| MockResponse::failure("unsupported query scan opcode")),
                )
                .expect("failed to write query scan response");
            }
            2001 => {
                write_mock_response(
                    stream,
                    corr_id,
                    next_mock_response(&config.query_scan_page_responses).unwrap_or_else(|| {
                        MockResponse::failure("unsupported query scan page opcode")
                    }),
                )
                .expect("failed to write query scan page response");
            }
            2002 => {
                record_cache_request(recorded_cache_requests, op_code, payload);
                write_mock_response(
                    stream,
                    corr_id,
                    next_mock_response(&config.query_sql_responses)
                        .unwrap_or_else(|| MockResponse::failure("unsupported query sql opcode")),
                )
                .expect("failed to write query sql response");
            }
            2003 => {
                write_mock_response(
                    stream,
                    corr_id,
                    next_mock_response(&config.query_sql_page_responses).unwrap_or_else(|| {
                        MockResponse::failure("unsupported query sql page opcode")
                    }),
                )
                .expect("failed to write query sql page response");
            }
            1050 => {
                if config.recovery_mode {
                    write_failure_response(stream, corr_id, "Node in recovery mode.")
                        .expect("failed to write recovery-mode error response");
                } else {
                    if should_disconnect_on_cache_names(&config) {
                        return true;
                    }
                    if let Some(delay) = next_cache_names_delay(&config) {
                        thread::sleep(delay);
                    }
                    write_success_response(
                        stream,
                        corr_id,
                        &0i32.to_le_bytes(),
                        next_topology_change_on_cache_names(&config),
                    )
                    .expect("failed to write cache names response");
                }
            }
            1000 => {
                record_cache_request(recorded_cache_requests, op_code, payload);
                if should_disconnect_on_cache_get(&config) {
                    return true;
                }
                if let Some(response) = next_mock_response(&config.cache_get_responses) {
                    write_mock_response(stream, corr_id, response)
                        .expect("failed to write cache get response");
                } else if let Some(err_msg) = &config.cache_get_error {
                    write_failure_response(stream, corr_id, err_msg)
                        .expect("failed to write cache get error response");
                } else {
                    write_failure_response(stream, corr_id, "unsupported cache get opcode")
                        .expect("failed to write mock error response");
                }
            }
            1001 => {
                record_cache_request(recorded_cache_requests, op_code, payload);
                if should_disconnect_on_cache_put(&config) {
                    return true;
                }
                if let Some(delay) = next_cache_put_delay(&config) {
                    let mut writer = stream
                        .try_clone()
                        .expect("failed to clone mock stream for delayed cache put");
                    let err_msg = config.cache_put_error.clone();
                    thread::spawn(move || {
                        thread::sleep(delay);
                        if let Some(err_msg) = err_msg {
                            write_failure_response(&mut writer, corr_id, &err_msg)
                                .expect("failed to write delayed cache put error response");
                        } else {
                            write_success_response(&mut writer, corr_id, &[], None)
                                .expect("failed to write delayed cache put response");
                        }
                    });
                    continue;
                }

                if let Some(response) = next_mock_response(&config.cache_put_responses) {
                    write_mock_response(stream, corr_id, response)
                        .expect("failed to write cache put response");
                } else if let Some(err_msg) = &config.cache_put_error {
                    write_failure_response(stream, corr_id, err_msg)
                        .expect("failed to write cache put error response");
                } else {
                    write_success_response(stream, corr_id, &[], None)
                        .expect("failed to write cache put response");
                }
            }
            2004 => {
                record_cache_request(recorded_cache_requests, op_code, payload);
                write_mock_response(
                    stream,
                    corr_id,
                    next_mock_response(&config.query_sql_fields_responses).unwrap_or_else(|| {
                        MockResponse::failure("unsupported query sql fields opcode")
                    }),
                )
                .expect("failed to write query sql fields response");
            }
            1011 => {
                record_cache_request(recorded_cache_requests, op_code, payload);
                write_mock_response(
                    stream,
                    corr_id,
                    next_mock_response(&config.cache_contains_key_responses).unwrap_or_else(|| {
                        MockResponse::failure("unsupported cache contains key opcode")
                    }),
                )
                .expect("failed to write cache contains key response");
            }
            1003 | 1004 | 1005 | 1006 | 1007 | 1008 | 1009 | 1010 | 1012 | 1013 | 1014 | 1015
            | 1016 | 1017 | 1018 | 1019 | 1020 | 1056 => {
                record_cache_request(recorded_cache_requests, op_code, payload);
                write_success_response(stream, corr_id, &[], None)
                    .expect("failed to write generic cache response");
            }
            2005 => {
                write_mock_response(
                    stream,
                    corr_id,
                    next_mock_response(&config.query_sql_fields_page_responses).unwrap_or_else(
                        || MockResponse::failure("unsupported query sql fields page opcode"),
                    ),
                )
                .expect("failed to write query sql fields page response");
            }
            0 => {
                query_close_count.fetch_add(1, Ordering::Relaxed);
                write_success_response(stream, corr_id, &[], None)
                    .expect("failed to write query close response");
            }
            _ => {
                write_failure_response(stream, corr_id, "unsupported opcode")
                    .expect("failed to write mock error response");
            }
        }
    }
}

enum ReadOutcome {
    Data,
    Timeout,
    Closed,
}

fn read_frame_prefix(
    stream: &mut TcpStream,
    len_buf: &mut [u8; 4],
    stop: &Arc<AtomicBool>,
) -> bool {
    matches!(
        read_exact_or_timeout(stream, len_buf, stop),
        Ok(ReadOutcome::Data)
    )
}

fn read_frame_body(stream: &mut TcpStream, buf: &mut [u8], stop: &Arc<AtomicBool>) -> bool {
    matches!(
        read_exact_or_timeout(stream, buf, stop),
        Ok(ReadOutcome::Data)
    )
}

fn read_exact_or_timeout(
    stream: &mut TcpStream,
    buf: &mut [u8],
    stop: &Arc<AtomicBool>,
) -> io::Result<ReadOutcome> {
    let mut read = 0usize;

    while read < buf.len() {
        if stop.load(Ordering::Acquire) {
            return Ok(ReadOutcome::Closed);
        }

        match stream.read(&mut buf[read..]) {
            Ok(0) if read == 0 => return Ok(ReadOutcome::Closed),
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "unexpected eof",
                ))
            }
            Ok(n) => read += n,
            Err(err)
                if matches!(
                    err.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) && read == 0 =>
            {
                return Ok(ReadOutcome::Timeout);
            }
            Err(err)
                if matches!(
                    err.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) =>
            {
                continue;
            }
            Err(err) => return Err(err),
        }
    }

    Ok(ReadOutcome::Data)
}

fn write_handshake_success(stream: &mut TcpStream, node_id: MockUuid) -> io::Result<()> {
    let features = mock_feature_bytes();
    let body_len = 1 + 1 + 4 + features.len() + 1 + 16;
    stream.write_all(&(body_len as i32).to_le_bytes())?;
    stream.write_all(&[1u8])?;
    stream.write_all(&[12u8])?;
    stream.write_all(&(features.len() as i32).to_le_bytes())?;
    stream.write_all(&features)?;
    stream.write_all(&[10u8])?;
    stream.write_all(&node_id.most.to_le_bytes())?;
    stream.write_all(&node_id.least.to_le_bytes())?;
    stream.flush()?;
    Ok(())
}

fn write_handshake_failure(stream: &mut TcpStream, msg: &str, err_code: i32) -> io::Result<()> {
    let body_len = 1 + 2 + 2 + 2 + 1 + 4 + msg.len() + 4;
    stream.write_all(&(body_len as i32).to_le_bytes())?;
    stream.write_all(&[0u8])?;
    stream.write_all(&1i16.to_le_bytes())?;
    stream.write_all(&7i16.to_le_bytes())?;
    stream.write_all(&0i16.to_le_bytes())?;
    stream.write_all(&[9u8])?;
    stream.write_all(&(msg.len() as i32).to_le_bytes())?;
    stream.write_all(msg.as_bytes())?;
    stream.write_all(&err_code.to_le_bytes())?;
    stream.flush()?;
    Ok(())
}

fn write_success_response(
    stream: &mut TcpStream,
    corr_id: i64,
    payload: &[u8],
    topology_change: Option<MockTopologyVersion>,
) -> io::Result<()> {
    let mut body = Vec::with_capacity(10 + payload.len());
    body.extend_from_slice(&corr_id.to_le_bytes());
    let mut flags = 0i16;
    if topology_change.is_some() {
        flags |= 1 << 1;
    }
    body.extend_from_slice(&flags.to_le_bytes());

    if let Some(version) = topology_change {
        body.extend_from_slice(&version.major.to_le_bytes());
        body.extend_from_slice(&version.minor.to_le_bytes());
    }

    body.extend_from_slice(payload);

    stream.write_all(&(body.len() as i32).to_le_bytes())?;
    stream.write_all(&body)?;
    stream.flush()?;
    Ok(())
}

fn write_failure_response(stream: &mut TcpStream, corr_id: i64, msg: &str) -> io::Result<()> {
    let mut body = Vec::with_capacity(14 + 4 + msg.len());
    body.extend_from_slice(&corr_id.to_le_bytes());
    body.extend_from_slice(&1i16.to_le_bytes());
    body.extend_from_slice(&1i32.to_le_bytes());
    body.extend_from_slice(&(msg.len() as i32).to_le_bytes());
    body.extend_from_slice(msg.as_bytes());

    stream.write_all(&(body.len() as i32).to_le_bytes())?;
    stream.write_all(&body)?;
    stream.flush()?;
    Ok(())
}

fn write_mock_response(
    stream: &mut TcpStream,
    corr_id: i64,
    response: MockResponse,
) -> io::Result<()> {
    match response {
        MockResponse::Success {
            payload,
            topology_change,
            notifications,
        } => {
            write_success_response(stream, corr_id, &payload, topology_change)?;
            write_mock_notifications(stream, notifications)
        }
        MockResponse::Failure(message) => write_failure_response(stream, corr_id, &message),
    }
}

fn write_mock_notifications(
    stream: &mut TcpStream,
    notifications: Vec<MockNotification>,
) -> io::Result<()> {
    for notification in notifications {
        if let Some(delay) = notification.delay {
            let mut writer = stream
                .try_clone()
                .expect("failed to clone mock stream for notification");
            thread::spawn(move || {
                thread::sleep(delay);
                write_notification_frame(&mut writer, &notification)
                    .expect("failed to write delayed notification");
            });
            continue;
        }

        write_notification_frame(stream, &notification)?;
    }

    Ok(())
}

fn write_notification_frame(
    stream: &mut TcpStream,
    notification: &MockNotification,
) -> io::Result<()> {
    let mut body = Vec::new();
    body.extend_from_slice(&notification.resource_id.to_le_bytes());
    let mut flags = 1i16 << 2;
    if notification.error.is_some() {
        flags |= 1;
    }
    body.extend_from_slice(&flags.to_le_bytes());
    body.extend_from_slice(&notification.op_code.to_le_bytes());
    if let Some(message) = &notification.error {
        body.extend_from_slice(&1i32.to_le_bytes());
        body.extend_from_slice(&(message.len() as i32).to_le_bytes());
        body.extend_from_slice(message.as_bytes());
    } else {
        body.extend_from_slice(&notification.payload);
    }

    stream.write_all(&(body.len() as i32).to_le_bytes())?;
    stream.write_all(&body)?;
    stream.flush()?;
    Ok(())
}

fn write_discovery_response(
    stream: &mut TcpStream,
    corr_id: i64,
    response: &MockDiscoveryResponse,
) -> io::Result<()> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&response.topology_version.to_le_bytes());
    payload.extend_from_slice(&(response.added_nodes.len() as i32).to_le_bytes());

    for node in &response.added_nodes {
        payload.extend_from_slice(&node.node_id.most.to_le_bytes());
        payload.extend_from_slice(&node.node_id.least.to_le_bytes());
        payload.extend_from_slice(&node.port.to_le_bytes());
        payload.extend_from_slice(&(node.addresses.len() as i32).to_le_bytes());

        for address in &node.addresses {
            payload.extend_from_slice(&(address.len() as i32).to_le_bytes());
            payload.extend_from_slice(address.as_bytes());
        }
    }

    payload.extend_from_slice(&(response.removed_node_ids.len() as i32).to_le_bytes());
    for node_id in &response.removed_node_ids {
        payload.extend_from_slice(&node_id.most.to_le_bytes());
        payload.extend_from_slice(&node_id.least.to_le_bytes());
    }

    write_success_response(stream, corr_id, &payload, None)
}

fn write_data_center_nodes_response(
    stream: &mut TcpStream,
    corr_id: i64,
    node_ids: &[MockUuid],
) -> io::Result<()> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&(node_ids.len() as i32).to_le_bytes());
    for node_id in node_ids {
        payload.extend_from_slice(&node_id.most.to_le_bytes());
        payload.extend_from_slice(&node_id.least.to_le_bytes());
    }
    write_success_response(stream, corr_id, &payload, None)
}

fn record_cache_request(
    recorded_cache_requests: &Arc<Mutex<Vec<RecordedCacheRequest>>>,
    op_code: i16,
    payload: &[u8],
) {
    if let Some(request) = parse_cache_request(op_code, payload) {
        recorded_cache_requests
            .lock()
            .expect("mock recorded_cache_requests mutex poisoned")
            .push(request);
    }
}

fn parse_cache_request(op_code: i16, payload: &[u8]) -> Option<RecordedCacheRequest> {
    if payload.len() < 5 {
        return None;
    }

    let cache_id = i32::from_le_bytes(payload.get(0..4)?.try_into().ok()?);
    let flags = *payload.get(4)?;
    let tx_id = if (flags & 0x02) != 0 {
        Some(i32::from_le_bytes(payload.get(5..9)?.try_into().ok()?))
    } else {
        None
    };

    Some(RecordedCacheRequest {
        op_code,
        cache_id,
        flags,
        tx_id,
    })
}

fn parse_tx_start_request(payload: &[u8]) -> Option<RecordedTxStartRequest> {
    if payload.len() < 14 {
        return None;
    }

    let concurrency = payload[0];
    let isolation = payload[1];
    let timeout_ms = i64::from_le_bytes(payload.get(2..10)?.try_into().ok()?);
    let label_len = i32::from_le_bytes(payload.get(10..14)?.try_into().ok()?);
    let label = if label_len < 0 {
        None
    } else {
        let start = 14usize;
        let end = start + label_len as usize;
        Some(String::from_utf8(payload.get(start..end)?.to_vec()).ok()?)
    };

    Some(RecordedTxStartRequest {
        concurrency,
        isolation,
        timeout_ms,
        label,
    })
}

fn parse_tx_end_request(payload: &[u8]) -> Option<RecordedTxEndRequest> {
    if payload.len() < 5 {
        return None;
    }

    Some(RecordedTxEndRequest {
        tx_id: i32::from_le_bytes(payload.get(0..4)?.try_into().ok()?),
        committed: *payload.get(4)? != 0,
    })
}

fn next_discovery_response(config: &MockThinServerConfig) -> MockDiscoveryResponse {
    if let Some(queue) = &config.discovery_responses {
        let mut queue = queue
            .lock()
            .expect("mock discovery_responses mutex poisoned");
        if let Some(response) = queue.pop_front() {
            return response;
        }
    }

    config
        .discovery_response
        .clone()
        .unwrap_or(MockDiscoveryResponse {
            topology_version: 0,
            added_nodes: Vec::new(),
            removed_node_ids: Vec::new(),
        })
}

fn next_data_center_nodes_response(config: &MockThinServerConfig) -> Vec<MockUuid> {
    if let Some(queue) = &config.data_center_nodes_responses {
        let mut queue = queue
            .lock()
            .expect("mock data_center_nodes_responses mutex poisoned");
        if let Some(node_ids) = queue.pop_front() {
            return node_ids;
        }
    }

    config
        .data_center_nodes_response
        .clone()
        .unwrap_or_default()
}

fn next_mock_response(queue: &Option<Arc<Mutex<VecDeque<MockResponse>>>>) -> Option<MockResponse> {
    queue.as_ref().and_then(|queue| {
        queue
            .lock()
            .expect("mock response queue mutex poisoned")
            .pop_front()
    })
}

fn next_opcode_response(config: &MockThinServerConfig, op_code: i16) -> Option<MockResponse> {
    config.opcode_responses.as_ref().and_then(|queue| {
        queue
            .lock()
            .expect("mock opcode_responses mutex poisoned")
            .get_mut(&op_code)
            .and_then(VecDeque::pop_front)
    })
}

fn next_topology_change_on_cache_names(
    config: &MockThinServerConfig,
) -> Option<MockTopologyVersion> {
    if let Some(queue) = &config.topology_changes_on_cache_names {
        let mut queue = queue
            .lock()
            .expect("mock topology_changes_on_cache_names mutex poisoned");
        if let Some(version) = queue.pop_front() {
            return Some(version);
        }
    }

    config.topology_change_on_cache_names
}

fn next_cache_names_delay(config: &MockThinServerConfig) -> Option<Duration> {
    if let Some(queue) = &config.cache_names_delays {
        let mut queue = queue
            .lock()
            .expect("mock cache_names_delays mutex poisoned");
        if let Some(delay) = queue.pop_front() {
            return Some(delay);
        }
    }

    config.cache_names_delay
}

fn next_cache_put_delay(config: &MockThinServerConfig) -> Option<Duration> {
    let Some(queue) = &config.cache_put_delays else {
        return None;
    };

    let mut queue = queue.lock().expect("mock cache_put_delays mutex poisoned");
    queue.pop_front()
}

fn should_disconnect_on_cache_put(config: &MockThinServerConfig) -> bool {
    let Some(queue) = &config.cache_put_disconnects else {
        return false;
    };

    let mut queue = queue
        .lock()
        .expect("mock cache_put_disconnects mutex poisoned");
    queue.pop_front().unwrap_or(false)
}

fn should_disconnect_on_cache_get(config: &MockThinServerConfig) -> bool {
    let Some(queue) = &config.cache_get_disconnects else {
        return false;
    };

    let mut queue = queue
        .lock()
        .expect("mock cache_get_disconnects mutex poisoned");
    queue.pop_front().unwrap_or(false)
}

fn should_disconnect_on_cache_names(config: &MockThinServerConfig) -> bool {
    let Some(queue) = &config.cache_names_disconnects else {
        return false;
    };

    let mut queue = queue
        .lock()
        .expect("mock cache_names_disconnects mutex poisoned");
    queue.pop_front().unwrap_or(false)
}

fn mock_feature_bytes() -> Vec<u8> {
    let mut features = vec![0u8; 3];
    features[0] |= 1 << 0;
    features[0] |= 1 << 3;
    features[1] |= 1 << 3;
    features[2] |= 1 << 6;
    features
}

fn parse_handshake_user_attributes(
    handshake: &[u8],
) -> io::Result<std::collections::BTreeMap<String, String>> {
    let mut cursor = std::io::Cursor::new(handshake);

    let features = read_handshake_prefix(&mut cursor)?;

    let mut attrs = std::collections::BTreeMap::new();
    if features
        .get(0)
        .map(|byte| (byte & (1 << 0)) != 0)
        .unwrap_or(false)
    {
        let mut count_bytes = [0u8; 4];
        cursor.read_exact(&mut count_bytes)?;
        let count = i32::from_le_bytes(count_bytes);
        if count < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "negative handshake attributes count",
            ));
        }

        for _ in 0..count {
            let key = read_typed_string(&mut cursor)?;
            let value = read_typed_string(&mut cursor)?;
            attrs.insert(key, value);
        }
    }

    Ok(attrs)
}

fn parse_handshake_credentials(handshake: &[u8]) -> io::Result<(Option<String>, Option<String>)> {
    let mut cursor = std::io::Cursor::new(handshake);
    let features = read_handshake_prefix(&mut cursor)?;

    if features
        .get(0)
        .map(|byte| (byte & (1 << 0)) != 0)
        .unwrap_or(false)
    {
        let mut count_bytes = [0u8; 4];
        cursor.read_exact(&mut count_bytes)?;
        let count = i32::from_le_bytes(count_bytes);
        if count < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "negative handshake attributes count",
            ));
        }

        for _ in 0..count {
            let _ = read_typed_string(&mut cursor)?;
            let _ = read_typed_string(&mut cursor)?;
        }
    }

    if cursor.position() >= handshake.len() as u64 {
        return Ok((None, None));
    }

    let username = read_typed_string(&mut cursor)?;
    let password = if cursor.position() < handshake.len() as u64 {
        Some(read_typed_string(&mut cursor)?)
    } else {
        None
    };

    Ok((Some(username), password))
}

fn read_handshake_prefix(reader: &mut impl Read) -> io::Result<Vec<u8>> {
    let mut opcode = [0u8; 1];
    reader.read_exact(&mut opcode)?;

    let mut version = [0u8; 6];
    reader.read_exact(&mut version)?;

    let mut client_code = [0u8; 1];
    reader.read_exact(&mut client_code)?;

    let mut features_type_code = [0u8; 1];
    reader.read_exact(&mut features_type_code)?;
    if features_type_code[0] != 12 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "unexpected handshake feature type code {}",
                features_type_code[0]
            ),
        ));
    }

    let mut features_len = [0u8; 4];
    reader.read_exact(&mut features_len)?;
    let features_len = i32::from_le_bytes(features_len);
    if features_len < 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "negative handshake feature length",
        ));
    }

    let mut features = vec![0u8; features_len as usize];
    reader.read_exact(&mut features)?;
    Ok(features)
}

fn read_typed_string(reader: &mut impl Read) -> io::Result<String> {
    let mut type_code = [0u8; 1];
    reader.read_exact(&mut type_code)?;
    if type_code[0] != 9 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unexpected handshake type code {}", type_code[0]),
        ));
    }

    let mut len_bytes = [0u8; 4];
    reader.read_exact(&mut len_bytes)?;
    let len = i32::from_le_bytes(len_bytes);
    if len < 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "negative handshake string length",
        ));
    }

    let mut data = vec![0u8; len as usize];
    reader.read_exact(&mut data)?;
    String::from_utf8(data).map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))
}
