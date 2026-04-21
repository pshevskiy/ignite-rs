use crate::affinity::{AffinityCache, CachePartitionsRequest, CachePartitionsResponse};
use crate::api::OpCode;
use crate::connection_async::{
    read_incoming_frame, write_request_batch, AsyncConnection, AsyncReadHalf, AsyncWriteHalf,
    ConnectionMetadata, IncomingFrame, NotificationFrame, ResponseFrame,
};
use crate::error::{ErrorKind, IgniteError, IgniteResult};
use crate::events::{ConnectionEventKind, EventBus, LifecycleEventKind, RequestEventKind};
use crate::protocol::Flag::{Failure, Success};
use crate::protocol::{
    read_i32, read_i64, read_string, read_u8, write_i16, write_i32, write_i64, write_string, Flag,
    TypeCode,
};
use crate::topology::{DiscoveredNode, TopologyCache, TopologySnapshot, TopologyVersion};
use crate::{ClientConfig, ReadableReq, RetryContext, RetryDecision, RetryPolicy, WriteableReq};
use arc_swap::ArcSwap;
use std::cmp;
use std::collections::{HashMap, HashSet, VecDeque};
use std::convert::TryFrom;
use std::io;
use std::io::{Cursor, Write};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, mpsc, oneshot, Mutex, RwLock};

const REQ_HEADER_SIZE_BYTES: i32 = 10;
const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
const MIN_RECOMMENDED_HEARTBEAT_INTERVAL: Duration = Duration::from_millis(500);
const IGNITE_DATA_CENTER_ID_ATTR: &str = "IGNITE_DATA_CENTER_ID";
static NEXT_DEFAULT_START_INDEX: AtomicI64 = AtomicI64::new(0);

struct EmptyReq;

impl WriteableReq for EmptyReq {
    fn write(&self, _: &mut dyn Write) -> io::Result<()> {
        Ok(())
    }

    fn size(&self) -> usize {
        0
    }
}

struct LongResp {
    value: i64,
}

impl ReadableReq for LongResp {
    fn read(reader: &mut impl io::Read) -> IgniteResult<Self> {
        Ok(Self {
            value: read_i64(reader).map_err(IgniteError::from)?,
        })
    }
}

struct RawPayload {
    body: Vec<u8>,
}

impl ReadableReq for RawPayload {
    fn read(reader: &mut impl io::Read) -> IgniteResult<Self> {
        let mut body = Vec::new();
        reader.read_to_end(&mut body).map_err(IgniteError::from)?;
        Ok(Self { body })
    }
}

struct NodeEndpointsReq {
    start_topology_version: i64,
    end_topology_version: i64,
}

impl WriteableReq for NodeEndpointsReq {
    fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        write_i64(writer, self.start_topology_version)?;
        write_i64(writer, self.end_topology_version)?;
        Ok(())
    }

    fn size(&self) -> usize {
        16
    }
}

struct NodeEndpointsResp {
    topology_version: i64,
    added_nodes: Vec<DiscoveredNode>,
    removed_node_ids: Vec<String>,
}

fn dedupe_endpoints(endpoints: Vec<String>) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut deduped = Vec::with_capacity(endpoints.len());
    for endpoint in endpoints {
        if seen.insert(endpoint.clone()) {
            deduped.push(endpoint);
        }
    }
    deduped
}

impl ReadableReq for NodeEndpointsResp {
    fn read(reader: &mut impl io::Read) -> IgniteResult<Self> {
        let topology_version = read_i64(reader).map_err(IgniteError::from)?;
        let added_count = read_i32(reader).map_err(IgniteError::from)?;
        if added_count < 0 {
            return Err(IgniteError::from("negative node additions count"));
        }

        let mut added_nodes = Vec::with_capacity(added_count as usize);
        for _ in 0..added_count {
            let node_id = crate::connection_async::read_uuid_string(reader)?;
            let port = read_i32(reader).map_err(IgniteError::from)?;
            let addr_count = read_i32(reader).map_err(IgniteError::from)?;
            if addr_count < 0 {
                return Err(IgniteError::from("negative node address count"));
            }

            let mut endpoints = Vec::with_capacity(addr_count as usize);
            for _ in 0..addr_count {
                let host = read_flexible_string(reader)?;
                endpoints.push(format!("{}:{}", host, port));
            }

            added_nodes.push(DiscoveredNode { node_id, endpoints });
        }

        let removed_count = read_i32(reader).map_err(IgniteError::from)?;
        if removed_count < 0 {
            return Err(IgniteError::from("negative node removals count"));
        }

        let mut removed_node_ids = Vec::with_capacity(removed_count as usize);
        for _ in 0..removed_count {
            removed_node_ids.push(crate::connection_async::read_uuid_string(reader)?);
        }

        Ok(Self {
            topology_version,
            added_nodes,
            removed_node_ids,
        })
    }
}

struct DataCenterNodesReq<'a> {
    dc_id: &'a str,
}

impl WriteableReq for DataCenterNodesReq<'_> {
    fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        write_string(writer, self.dc_id)
    }

    fn size(&self) -> usize {
        4 + self.dc_id.len()
    }
}

struct DataCenterNodesResp {
    node_ids: Vec<String>,
}

impl ReadableReq for DataCenterNodesResp {
    fn read(reader: &mut impl io::Read) -> IgniteResult<Self> {
        let count = read_i32(reader).map_err(IgniteError::from)?;
        if count < 0 {
            return Err(IgniteError::from("negative data center nodes count"));
        }

        let mut node_ids = Vec::with_capacity(count as usize);
        for _ in 0..count {
            node_ids.push(crate::connection_async::read_uuid_string(reader)?);
        }

        Ok(Self { node_ids })
    }
}

struct Channel {
    address: String,
    request_timeout: Option<Duration>,
    metadata: ConnectionMetadata,
    writer_tx: mpsc::UnboundedSender<OutboundRequest>,
    /// Sharded inflight map — reduces Mutex contention under concurrent load.
    /// Shard selected by corr_id % SHARD_COUNT.
    inflight_shards: Box<[StdMutex<HashMap<i64, oneshot::Sender<IgniteResult<ResponseFrame>>>>]>,
    notification_listeners:
        Mutex<HashMap<(i16, i64), mpsc::UnboundedSender<IgniteResult<NotificationFrame>>>>,
    pending_notifications: Mutex<HashMap<(i16, i64), Vec<NotificationFrame>>>,
    closed: AtomicBool,
    last_error: Mutex<Option<String>>,
    last_send_at_ms: AtomicU64,
    created_at: Instant,
    writer_pump: StdMutex<Option<tokio::task::JoinHandle<()>>>,
    response_pump: StdMutex<Option<tokio::task::JoinHandle<()>>>,
}

struct OutboundRequest {
    request: Vec<u8>,
}

impl Channel {
    fn new(
        address: String,
        request_timeout: Option<Duration>,
        metadata: ConnectionMetadata,
        writer_tx: mpsc::UnboundedSender<OutboundRequest>,
    ) -> Arc<Self> {
        Arc::new(Self {
            address,
            request_timeout,
            metadata,
            writer_tx,
            inflight_shards: (0..16)
                .map(|_| StdMutex::new(HashMap::new()))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            notification_listeners: Mutex::new(HashMap::new()),
            pending_notifications: Mutex::new(HashMap::new()),
            closed: AtomicBool::new(false),
            last_error: Mutex::new(None),
            last_send_at_ms: AtomicU64::new(0),
            created_at: Instant::now(),
            writer_pump: StdMutex::new(None),
            response_pump: StdMutex::new(None),
        })
    }

    #[inline]
    fn inflight_shard(
        &self,
        corr_id: i64,
    ) -> &StdMutex<HashMap<i64, oneshot::Sender<IgniteResult<ResponseFrame>>>> {
        &self.inflight_shards[(corr_id as usize) % self.inflight_shards.len()]
    }

    async fn connect(conf: &ClientConfig, address: String) -> IgniteResult<Arc<Self>> {
        let connection = AsyncConnection::connect(conf, &address).await?;
        let (reader, writer, metadata) = connection.into_parts();
        let (writer_tx, writer_rx) = mpsc::unbounded_channel();
        let channel = Self::new(address, conf.request_timeout, metadata, writer_tx);
        channel.attach_writer_pump(writer, writer_rx, channel_requires_flush(conf));
        channel.attach_response_pump(reader);
        Ok(channel)
    }

    fn address(&self) -> &str {
        &self.address
    }

    fn is_available(&self) -> bool {
        !self.closed.load(Ordering::Acquire)
    }

    fn supports_heartbeat(&self) -> bool {
        self.metadata.capabilities.heartbeat
    }

    fn supports_node_endpoints(&self) -> bool {
        self.metadata.capabilities.node_endpoints
    }

    fn supports_dc_aware(&self) -> bool {
        self.metadata.capabilities.dc_aware
    }

    fn supports_query_partitions_batch_size(&self) -> bool {
        self.metadata.capabilities.query_partitions_batch_size
    }

    fn supports_query_initiator_id(&self) -> bool {
        self.metadata.capabilities.query_initiator_id
    }

    fn supports_index_query_limit(&self) -> bool {
        self.metadata.capabilities.index_query_limit
    }

    fn supports_service_invoke_callctx(&self) -> bool {
        self.metadata.capabilities.service_invoke_callctx
    }

    fn supports_transactions(&self) -> bool {
        self.metadata.capabilities.transactions
    }

    fn supports_all_affinity_mappings(&self) -> bool {
        self.metadata.capabilities.all_affinity_mappings
    }

    fn supports_force_deactivation_flag(&self) -> bool {
        self.metadata.capabilities.force_deactivation_flag
    }

    fn server_node_id(&self) -> Option<&str> {
        self.metadata.server_node_id.as_deref()
    }

    fn idle_for(&self) -> Duration {
        let ms = self.last_send_at_ms.load(Ordering::Relaxed);
        let last_send = self.created_at + Duration::from_millis(ms);
        last_send.elapsed()
    }

    fn mark_sent(&self) {
        let ms = self.created_at.elapsed().as_millis() as u64;
        self.last_send_at_ms.store(ms, Ordering::Relaxed);
    }

    async fn request(&self, corr_id: i64, request: Vec<u8>) -> IgniteResult<ResponseFrame> {
        if !self.is_available() {
            return Err(self.closed_error().await);
        }

        let (tx, rx) = oneshot::channel();
        self.inflight_shard(corr_id)
            .lock()
            .unwrap()
            .insert(corr_id, tx);

        if self.writer_tx.send(OutboundRequest { request }).is_err() {
            self.inflight_shard(corr_id)
                .lock()
                .unwrap()
                .remove(&corr_id);
            return Err(self.closed_error().await);
        }

        let frame = match self.await_response(corr_id, rx).await {
            Ok(frame) => frame,
            Err(err) => {
                self.inflight_shard(corr_id)
                    .lock()
                    .unwrap()
                    .remove(&corr_id);
                return Err(err);
            }
        };

        if frame.correlation_id != corr_id {
            return Err(IgniteError::connection(format!(
                "Unexpected response correlation id {}, expected {}",
                frame.correlation_id, corr_id
            )));
        }

        Ok(frame)
    }

    async fn await_response(
        &self,
        corr_id: i64,
        rx: oneshot::Receiver<IgniteResult<ResponseFrame>>,
    ) -> IgniteResult<ResponseFrame> {
        match self.request_timeout {
            Some(timeout) => match tokio::time::timeout(timeout, rx).await {
                Ok(result) => resolve_response(corr_id, result),
                Err(_) => Err(IgniteError::connection(format!(
                    "Operation timed out after {:?}",
                    timeout
                ))),
            },
            None => resolve_response(corr_id, rx.await),
        }
    }

    async fn register_notification_listener(
        &self,
        op_code: i16,
        resource_id: i64,
    ) -> IgniteResult<mpsc::UnboundedReceiver<IgniteResult<NotificationFrame>>> {
        if !self.is_available() {
            return Err(self.closed_error().await);
        }

        let key = (op_code, resource_id);
        let (tx, rx) = mpsc::unbounded_channel();
        self.notification_listeners
            .lock()
            .await
            .insert(key, tx.clone());

        if let Some(pending) = self.pending_notifications.lock().await.remove(&key) {
            for frame in pending {
                let _ = tx.send(Ok(frame));
            }
        }

        Ok(rx)
    }

    async fn remove_notification_listener(&self, op_code: i16, resource_id: i64) {
        let key = (op_code, resource_id);
        self.notification_listeners.lock().await.remove(&key);
        self.pending_notifications.lock().await.remove(&key);
    }

    async fn dispatch_notification(&self, frame: NotificationFrame) {
        let key = (frame.op_code, frame.resource_id);
        if let Some(listener) = self.notification_listeners.lock().await.get(&key).cloned() {
            if listener.send(Ok(frame.clone())).is_ok() {
                return;
            }
        }

        self.pending_notifications
            .lock()
            .await
            .entry(key)
            .or_default()
            .push(frame);
    }

    fn attach_writer_pump(
        self: &Arc<Self>,
        mut writer: AsyncWriteHalf,
        mut receiver: mpsc::UnboundedReceiver<OutboundRequest>,
        flush: bool,
    ) {
        let channel = self.clone();
        let handle = tokio::spawn(async move {
            while let Some(first) = receiver.recv().await {
                let mut batch = vec![first.request];
                while let Ok(next) = receiver.try_recv() {
                    batch.push(next.request);
                }
                let slices = batch.iter().map(Vec::as_slice).collect::<Vec<_>>();
                if let Err(err) =
                    write_request_batch(&mut writer, &slices, channel.request_timeout, flush).await
                {
                    channel.mark_broken(err.to_string()).await;
                    channel.abort_response_pump();
                    break;
                }
                channel.mark_sent();
            }
        });
        *self
            .writer_pump
            .lock()
            .expect("channel writer_pump mutex poisoned") = Some(handle);
    }

    fn attach_response_pump(self: &Arc<Self>, mut reader: AsyncReadHalf) {
        let channel = self.clone();
        let handle = tokio::spawn(async move {
            loop {
                match read_incoming_frame(&mut reader, &channel.metadata).await {
                    Ok(IncomingFrame::Response(frame)) => {
                        let waiter = channel
                            .inflight_shard(frame.correlation_id)
                            .lock()
                            .unwrap()
                            .remove(&frame.correlation_id);
                        if let Some(waiter) = waiter {
                            let _ = waiter.send(Ok(frame));
                        }
                    }
                    Ok(IncomingFrame::Notification(frame)) => {
                        channel.dispatch_notification(frame).await;
                    }
                    Err(err) => {
                        channel.mark_broken(err.to_string()).await;
                        channel.abort_writer_pump();
                        break;
                    }
                }
            }
        });
        *self
            .response_pump
            .lock()
            .expect("channel response_pump mutex poisoned") = Some(handle);
    }

    async fn mark_broken(&self, detail: String) {
        let was_closed = self.closed.swap(true, Ordering::AcqRel);
        *self.last_error.lock().await = Some(detail.clone());

        if was_closed {
            return;
        }

        // Drain all inflight shards
        for shard in self.inflight_shards.iter() {
            let pending = {
                let mut shard = shard.lock().unwrap();
                std::mem::take(&mut *shard)
            };
            for (_, waiter) in pending {
                let _ = waiter.send(Err(IgniteError::connection(detail.as_str())));
            }
        }

        let listeners = {
            let mut listeners = self.notification_listeners.lock().await;
            std::mem::take(&mut *listeners)
        };
        self.pending_notifications.lock().await.clear();

        for ((op_code, resource_id), listener) in listeners {
            let _ = listener.send(Err(IgniteError::from(
                format!(
                    "channel closed while waiting for notification {}:{}: {}",
                    op_code, resource_id, detail
                )
                .as_str(),
            )));
        }
    }

    async fn close(&self, detail: String) {
        self.mark_broken(detail).await;
        self.abort_writer_pump();
        self.abort_response_pump();
    }

    fn abort_writer_pump(&self) {
        if let Some(handle) = self
            .writer_pump
            .lock()
            .expect("channel writer_pump mutex poisoned")
            .take()
        {
            handle.abort();
        }
    }

    fn abort_response_pump(&self) {
        if let Some(handle) = self
            .response_pump
            .lock()
            .expect("channel response_pump mutex poisoned")
            .take()
        {
            handle.abort();
        }
    }

    async fn closed_error(&self) -> IgniteError {
        match self.last_error.lock().await.clone() {
            Some(detail) => IgniteError::connection(detail),
            None => IgniteError::connection("channel is closed"),
        }
    }
}

fn resolve_response(
    corr_id: i64,
    response: Result<IgniteResult<ResponseFrame>, oneshot::error::RecvError>,
) -> IgniteResult<ResponseFrame> {
    match response {
        Ok(result) => result,
        Err(_) => Err(IgniteError::connection(format!(
            "response pump stopped while waiting for correlation id {}",
            corr_id
        ))),
    }
}

fn classify_server_error(message: &str) -> IgniteError {
    let lower = message.to_ascii_lowercase();
    if lower.contains("auth") || lower.contains("credential") {
        IgniteError::authentication(message)
    } else {
        IgniteError::server(message)
    }
}

fn aggregate_connect_errors(errors: Vec<IgniteError>) -> IgniteError {
    let detail = errors
        .iter()
        .map(|err| err.to_string())
        .collect::<Vec<_>>()
        .join(" | ");
    let message = format!("Failed to connect to any configured address: {}", detail);

    if errors
        .iter()
        .any(|err| err.kind() == ErrorKind::Authentication)
    {
        IgniteError::authentication(message)
    } else if errors.iter().any(|err| err.kind() == ErrorKind::Tls) {
        IgniteError::tls(message)
    } else if errors.iter().any(|err| err.kind() == ErrorKind::Handshake) {
        IgniteError::handshake(message)
    } else {
        IgniteError::connection(message)
    }
}

fn build_channels_by_address(
    channels: &HashMap<String, Arc<Channel>>,
) -> HashMap<Arc<str>, Vec<Arc<Channel>>> {
    let mut by_address: HashMap<Arc<str>, Vec<Arc<Channel>>> = HashMap::new();
    for channel in channels.values() {
        by_address
            .entry(Arc::from(channel.address()))
            .or_default()
            .push(channel.clone());
    }
    by_address
}

pub(crate) struct ChannelManager {
    conf: ClientConfig,
    affinity: AffinityCache,
    topology: TopologyCache,
    event_bus: EventBus,
    channels: RwLock<HashMap<String, Arc<Channel>>>,
    channels_by_address: ArcSwap<HashMap<Arc<str>, Vec<Arc<Channel>>>>,
    /// Lock-free node_id → channel index for partition-aware routing.
    /// Uses ArcSwap for zero-cost reads (equivalent to Java volatile).
    node_channels: ArcSwap<HashMap<Arc<str>, Arc<Channel>>>,
    active: RwLock<Arc<Channel>>,
    cached_active_tx: tokio::sync::watch::Sender<Arc<Channel>>,
    cached_active_rx: tokio::sync::watch::Receiver<Arc<Channel>>,
    reconnect_guard: Mutex<()>,
    discovery_refresh_guard: Mutex<()>,
    channel_connect_guard: Mutex<()>,
    next_correlation_id: AtomicI64,
    next_channel_key: AtomicI64,
    next_default_channel_index: AtomicI64,
    reconnect_attempts: StdMutex<VecDeque<Instant>>,
    shutdown: Arc<AtomicBool>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct RequestRoute {
    pinned_address: Option<String>,
    preferred_node_id: Option<Arc<str>>,
    allow_retry: bool,
}

impl RequestRoute {
    pub(crate) fn pinned(address: String) -> Self {
        Self {
            pinned_address: Some(address),
            preferred_node_id: None,
            allow_retry: false,
        }
    }

    pub(crate) fn preferred_node(node_id: Arc<str>) -> Self {
        Self {
            pinned_address: None,
            preferred_node_id: Some(node_id),
            allow_retry: true,
        }
    }

    fn is_default(&self) -> bool {
        self.pinned_address.is_none() && self.preferred_node_id.is_none()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ResponseMeta {
    pub(crate) address: String,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct SqlFieldsCapabilities {
    pub(crate) partitions_batch_size: bool,
    pub(crate) query_initiator_id: bool,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct IndexQueryCapabilities {
    /// Server advertises `INDEX_QUERY_LIMIT` (bit 15). When false, the writer
    /// must omit the `limit` field — Java `TcpClientCache.indexQuery` gates on
    /// this bit and throws rather than emit it.
    pub(crate) index_query_limit: bool,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ServiceInvokeCapabilities {
    /// Server advertises `SERVICE_INVOKE_CALLCTX` (bit 10). When false, the
    /// `SERVICE_INVOKE` writer must omit the trailing `callAttrs` map field
    /// entirely — see `ClientServicesImpl.java:401-404@2.17.0`. FND-047.
    pub(crate) service_invoke_callctx: bool,
}

impl ChannelManager {
    pub(crate) async fn new(conf: ClientConfig) -> IgniteResult<Self> {
        let event_bus = EventBus::new(conf.event_subscriptions.clone());
        Self::new_with_event_bus(conf, event_bus).await
    }

    pub(crate) async fn new_with_event_bus(
        conf: ClientConfig,
        event_bus: EventBus,
    ) -> IgniteResult<Self> {
        conf.validate()?;

        let seed_endpoints = conf.normalized_addresses()?;
        let start_index = initial_start_index(&seed_endpoints);
        let affinity = AffinityCache::new();
        let topology = TopologyCache::new(seed_endpoints);
        let active =
            Self::connect_any_initial(&conf, &topology, &event_bus, start_index, false).await?;
        let address = active.address().to_string();
        let mut channels = HashMap::new();
        channels.insert(format!("{}#pool0", address), active.clone());

        for i in 1..conf.connection_pool_size {
            match Channel::connect(&conf, address.clone()).await {
                Ok(ch) => {
                    channels.insert(format!("{}#pool{}", address, i), ch);
                }
                Err(_) => {
                    // Silently skip — the pool will have fewer connections
                    // than requested, but at least one is guaranteed.
                }
            }
        }
        let channels_by_address = build_channels_by_address(&channels);
        let next_channel_key = conf.connection_pool_size as i64;

        let (cached_active_tx, cached_active_rx) = tokio::sync::watch::channel(active.clone());

        let manager = Self {
            conf,
            affinity,
            topology,
            event_bus,
            channels: RwLock::new(channels),
            channels_by_address: ArcSwap::from_pointee(channels_by_address),
            node_channels: ArcSwap::from_pointee(HashMap::new()),
            active: RwLock::new(active.clone()),
            cached_active_tx,
            cached_active_rx,
            reconnect_guard: Mutex::new(()),
            discovery_refresh_guard: Mutex::new(()),
            channel_connect_guard: Mutex::new(()),
            next_correlation_id: AtomicI64::new(1),
            next_channel_key: AtomicI64::new(next_channel_key),
            next_default_channel_index: AtomicI64::new(0),
            reconnect_attempts: StdMutex::new(VecDeque::new()),
            shutdown: Arc::new(AtomicBool::new(false)),
        };

        manager.on_channel_connected(active).await;
        manager
            .event_bus
            .emit_lifecycle(LifecycleEventKind::Created, None);
        Ok(manager)
    }

    pub(crate) fn spawn_background_tasks(self: &Arc<Self>) {
        if !self.conf.heartbeat_enabled {
            return;
        }

        let weak = Arc::downgrade(self);
        let shutdown = self.shutdown.clone();

        tokio::spawn(async move {
            let Some(manager) = weak.upgrade() else {
                return;
            };
            let interval = manager.resolve_heartbeat_interval().await;
            drop(manager);

            let Some(interval) = interval else {
                return;
            };

            loop {
                if shutdown.load(Ordering::Acquire) {
                    break;
                }

                tokio::time::sleep(interval).await;

                if shutdown.load(Ordering::Acquire) {
                    break;
                }

                let Some(manager) = weak.upgrade() else {
                    break;
                };
                manager.heartbeat_tick(interval).await;
            }
        });
    }

    fn store_channels_by_address(&self, channels: &HashMap<String, Arc<Channel>>) {
        self.channels_by_address
            .store(Arc::new(build_channels_by_address(channels)));
    }

    async fn insert_channel(&self, channel: Arc<Channel>) {
        let key = format!(
            "{}#ch{}",
            channel.address(),
            self.next_channel_key.fetch_add(1, Ordering::Relaxed)
        );
        let mut channels = self.channels.write().await;
        channels.insert(key, channel);
        self.store_channels_by_address(&channels);
    }

    fn available_channels_from_index(&self, addresses: &[String]) -> Vec<Arc<Channel>> {
        let indexed = self.channels_by_address.load();
        let mut available = Vec::new();
        let mut seen = HashSet::new();

        for address in addresses {
            if let Some(group) = indexed.get(address.as_str()) {
                for channel in group {
                    if !channel.is_available() {
                        continue;
                    }
                    let ptr = Arc::as_ptr(channel) as usize;
                    if seen.insert(ptr) {
                        available.push(channel.clone());
                    }
                }
            }
        }

        available
    }

    pub(crate) fn subscribe_events(&self) -> broadcast::Receiver<crate::events::ClientEvent> {
        self.event_bus.subscribe()
    }

    pub(crate) async fn register_notification_listener(
        &self,
        address: &str,
        op_code: i16,
        resource_id: i64,
    ) -> IgniteResult<mpsc::UnboundedReceiver<IgniteResult<NotificationFrame>>> {
        let channel = self
            .channel_for_address(address)
            .await
            .ok_or_else(|| IgniteError::from("Notification channel is not available"))?;
        channel
            .register_notification_listener(op_code, resource_id)
            .await
    }

    pub(crate) async fn remove_notification_listener(
        &self,
        address: &str,
        op_code: i16,
        resource_id: i64,
    ) {
        if let Some(channel) = self.channel_for_address(address).await {
            channel
                .remove_notification_listener(op_code, resource_id)
                .await;
        }
    }

    pub(crate) async fn topology_snapshot(&self) -> TopologySnapshot {
        self.topology.snapshot().await
    }

    pub(crate) async fn invalidate_affinity_cache(&self, cache_id: i32) {
        self.affinity.invalidate_cache(cache_id).await;
    }

    pub(crate) async fn send(&self, op_code: OpCode, data: impl WriteableReq) -> IgniteResult<()> {
        self.send_with_route(op_code, data, RequestRoute::default())
            .await
    }

    pub(crate) async fn send_with_route(
        &self,
        op_code: OpCode,
        data: impl WriteableReq,
        route: RequestRoute,
    ) -> IgniteResult<()> {
        let corr_id = self.next_correlation_id.fetch_add(1, Ordering::Relaxed);
        let request = Self::encode_request(op_code as i16, corr_id, &data)?;
        let (flag, _body, _offset) = self
            .round_trip_no_meta(op_code as i16, corr_id, request, true, route)
            .await?;
        match flag {
            Success => Ok(()),
            Failure { err_msg } => Err(classify_server_error(&err_msg)),
        }
    }

    pub(crate) async fn send_and_read<T: ReadableReq>(
        &self,
        op_code: OpCode,
        data: impl WriteableReq,
    ) -> IgniteResult<T> {
        self.send_and_read_with_route(op_code, data, RequestRoute::default())
            .await
    }

    pub(crate) async fn send_and_read_with_route<T: ReadableReq>(
        &self,
        op_code: OpCode,
        data: impl WriteableReq,
        route: RequestRoute,
    ) -> IgniteResult<T> {
        let corr_id = self.next_correlation_id.fetch_add(1, Ordering::Relaxed);
        let request = Self::encode_request(op_code as i16, corr_id, &data)?;
        let (flag, body, payload_offset) = self
            .round_trip_no_meta(op_code as i16, corr_id, request, true, route)
            .await?;
        match flag {
            Success => {
                let mut cur = Cursor::new(&body[payload_offset..]);
                Ok(T::read(&mut cur)?)
            }
            Failure { err_msg } => Err(classify_server_error(&err_msg)),
        }
    }

    pub(crate) async fn send_and_read_with_meta<T: ReadableReq>(
        &self,
        op_code: OpCode,
        data: impl WriteableReq,
        route: RequestRoute,
    ) -> IgniteResult<(T, ResponseMeta)> {
        let corr_id = self.next_correlation_id.fetch_add(1, Ordering::Relaxed);
        let request = Self::encode_request(op_code as i16, corr_id, &data)?;
        let (flag, body, payload_offset, meta) = self
            .round_trip_with_route(op_code as i16, corr_id, request, true, route)
            .await?;
        match flag {
            Success => {
                let mut cur = Cursor::new(&body[payload_offset..]);
                Ok((T::read(&mut cur)?, meta))
            }
            Failure { err_msg } => Err(classify_server_error(&err_msg)),
        }
    }

    async fn send_internal(&self, op_code: OpCode) -> IgniteResult<()> {
        let channel = self.active.read().await.clone();
        self.send_internal_on_channel(channel, op_code, EmptyReq)
            .await
    }

    async fn send_internal_on_channel(
        &self,
        channel: Arc<Channel>,
        op_code: OpCode,
        data: impl WriteableReq,
    ) -> IgniteResult<()> {
        let corr_id = self.next_correlation_id.fetch_add(1, Ordering::Relaxed);
        let request = Self::encode_request(op_code as i16, corr_id, &data)?;
        let frame = channel.request(corr_id, request).await?;
        match frame.flag {
            Success => Ok(()),
            Failure { err_msg } => Err(classify_server_error(&err_msg)),
        }
    }

    async fn send_internal_and_read<T: ReadableReq>(&self, op_code: OpCode) -> IgniteResult<T> {
        let channel = self.active.read().await.clone();
        self.send_internal_and_read_on_channel(channel, op_code, EmptyReq)
            .await
    }

    async fn send_internal_and_read_on_channel<T: ReadableReq>(
        &self,
        channel: Arc<Channel>,
        op_code: OpCode,
        data: impl WriteableReq,
    ) -> IgniteResult<T> {
        let corr_id = self.next_correlation_id.fetch_add(1, Ordering::Relaxed);
        let request = Self::encode_request(op_code as i16, corr_id, &data)?;
        let frame = channel.request(corr_id, request).await?;
        match frame.flag {
            Success => {
                let mut cur = Cursor::new(&frame.body[frame.payload_offset..]);
                T::read(&mut cur)
            }
            Failure { err_msg } => Err(classify_server_error(&err_msg)),
        }
    }

    async fn resolve_heartbeat_interval(&self) -> Option<Duration> {
        if !self.active.read().await.supports_heartbeat() {
            return None;
        }

        let configured = self
            .conf
            .heartbeat_interval
            .unwrap_or(DEFAULT_HEARTBEAT_INTERVAL);

        let server_idle_timeout = match self
            .send_internal_and_read::<LongResp>(OpCode::GetIdleTimeout)
            .await
        {
            Ok(resp) if resp.value > 0 => Some(Duration::from_millis(resp.value as u64)),
            _ => None,
        };

        server_idle_timeout.map_or(Some(configured), |idle_timeout| {
            let recommended = cmp::max(idle_timeout / 3, MIN_RECOMMENDED_HEARTBEAT_INTERVAL);
            Some(cmp::min(configured, recommended))
        })
    }

    async fn heartbeat_tick(&self, interval: Duration) {
        let channel = self.active.read().await.clone();

        if !channel.is_available() {
            let _ = self.reconnect_from(channel.address()).await;
            return;
        }

        if channel.idle_for() <= interval {
            return;
        }

        let failed_addr = channel.address().to_string();

        if self.send_internal(OpCode::Heartbeat).await.is_err() {
            let _ = self.reconnect_from(&failed_addr).await;
        }
    }

    async fn round_trip_internal(
        &self,
        op_code: i16,
        corr_id: i64,
        mut request: Vec<u8>,
        emit_request_events: bool,
    ) -> IgniteResult<(Flag, Vec<u8>, usize, ResponseMeta)> {
        let max_attempts = self.max_attempts();
        let mut attempt = 0usize;
        let mut started_emitted = false;
        let track_events = emit_request_events && self.event_bus.has_request_subscribers();

        loop {
            let channel = self.default_channel().await;

            if track_events && !started_emitted {
                self.event_bus.emit_request(
                    RequestEventKind::Started,
                    op_code,
                    corr_id,
                    channel.address().to_string(),
                    None,
                );
                started_emitted = true;
            }

            // Avoid cloning request on last attempt (pass by ownership).
            let is_last = attempt + 1 >= max_attempts;
            let req = if is_last {
                std::mem::take(&mut request)
            } else {
                request.clone()
            };
            match channel.request(corr_id, req).await {
                Ok(response) => {
                    if let Some(version) = response.topology_version {
                        self.handle_topology_change(version).await;
                    }

                    if track_events {
                        let address = channel.address().to_string();
                        match &response.flag {
                            Success => self.event_bus.emit_request(
                                RequestEventKind::Succeeded,
                                op_code,
                                corr_id,
                                address,
                                None,
                            ),
                            Failure { err_msg } => self.event_bus.emit_request(
                                RequestEventKind::Failed,
                                op_code,
                                corr_id,
                                address,
                                Some(err_msg.clone()),
                            ),
                        }
                    }

                    // Lazy address: only allocate when caller needs ResponseMeta
                    let address = channel.address().to_string();
                    return Ok((response.flag, response.body, response.payload_offset, ResponseMeta { address }));
                }
                Err(err) => {
                    let address = channel.address().to_string();
                    let err_desc = err.to_string();

                    if track_events {
                        self.event_bus.emit_request(
                            RequestEventKind::Failed,
                            op_code,
                            corr_id,
                            address.clone(),
                            Some(err_desc.clone()),
                        );
                    }

                    if !self.should_retry(op_code, attempt + 1, &address, &err)? {
                        return Err(err);
                    }

                    self.event_bus.emit_connection(
                        ConnectionEventKind::Closed,
                        address.clone(),
                        Some(err_desc.clone()),
                    );

                    if attempt >= max_attempts {
                        return Err(err);
                    }

                    if track_events {
                        self.event_bus.emit_request(
                            RequestEventKind::Retried,
                            op_code,
                            corr_id,
                            address.clone(),
                            Some(err_desc),
                        );
                    }

                    self.reconnect_from(&address).await?;
                    attempt += 1;
                }
            }
        }
    }

    /// Fast round-trip — skips ResponseMeta allocation, optimized for both default and preferred_node routes.
    async fn round_trip_no_meta(
        &self,
        op_code: i16,
        corr_id: i64,
        request: Vec<u8>,
        emit_request_events: bool,
        route: RequestRoute,
    ) -> IgniteResult<(Flag, Vec<u8>, usize)> {
        let (flag, body, payload_offset, _meta) = self
            .round_trip_with_route(op_code, corr_id, request, emit_request_events, route)
            .await?;
        Ok((flag, body, payload_offset))
    }

    async fn round_trip_with_route(
        &self,
        op_code: i16,
        corr_id: i64,
        request: Vec<u8>,
        emit_request_events: bool,
        route: RequestRoute,
    ) -> IgniteResult<(Flag, Vec<u8>, usize, ResponseMeta)> {
        if route.is_default() {
            return self
                .round_trip_internal(op_code, corr_id, request, emit_request_events)
                .await;
        }

        let channel = match self.select_channel_for_route(&route).await {
            Some(channel) => channel,
            None if route.pinned_address.is_none() => {
                return self
                    .round_trip_internal(op_code, corr_id, request, emit_request_events)
                    .await
            }
            None => {
                return Err(IgniteError::from(
                    "Transaction context has been lost due to connection errors",
                ))
            }
        };
        // Avoid allocating address String unless events are subscribed.
        let track_events = emit_request_events && self.event_bus.has_request_subscribers();
        let address = if track_events {
            Some(channel.address().to_string())
        } else {
            None
        };

        if let Some(ref addr) = address {
            self.event_bus.emit_request(
                RequestEventKind::Started,
                op_code,
                corr_id,
                addr.clone(),
                None,
            );
        }

        // Pass request by ownership — only clone for the retry path.
        let retry_request = if route.allow_retry {
            Some(request.clone())
        } else {
            None
        };
        match channel.request(corr_id, request).await {
            Ok(response) => {
                if let Some(version) = response.topology_version {
                    self.handle_topology_change(version).await;
                }

                if let Some(ref addr) = address {
                    match &response.flag {
                        Success => self.event_bus.emit_request(
                            RequestEventKind::Succeeded,
                            op_code,
                            corr_id,
                            addr.clone(),
                            None,
                        ),
                        Failure { err_msg } => self.event_bus.emit_request(
                            RequestEventKind::Failed,
                            op_code,
                            corr_id,
                            addr.clone(),
                            Some(err_msg.clone()),
                        ),
                    }
                }

                let meta_addr = address.unwrap_or_else(|| channel.address().to_string());
                Ok((
                    response.flag,
                    response.body,
                    response.payload_offset,
                    ResponseMeta { address: meta_addr },
                ))
            }
            Err(err) => {
                let addr_str = address.unwrap_or_else(|| channel.address().to_string());
                if track_events {
                    self.event_bus.emit_request(
                        RequestEventKind::Failed,
                        op_code,
                        corr_id,
                        addr_str.clone(),
                        Some(err.to_string()),
                    );
                }
                self.event_bus.emit_connection(
                    ConnectionEventKind::Closed,
                    addr_str,
                    Some(err.to_string()),
                );

                if let Some(retry_req) = retry_request {
                    self.affinity.invalidate().await;
                    self.round_trip_internal(op_code, corr_id, retry_req, emit_request_events)
                        .await
                } else {
                    Err(err)
                }
            }
        }
    }

    async fn reconnect_from(&self, failed_address: &str) -> IgniteResult<Arc<Channel>> {
        let _guard = self.reconnect_guard.lock().await;

        let current = self.active.read().await.clone();
        if current.address() != failed_address && current.is_available() {
            return Ok(current);
        }

        self.record_reconnect_attempt()?;
        let removed_seed_endpoints = self.refresh_seed_endpoints().await?;
        let start_index = self.topology.next_index_after(failed_address).await;
        let channel = self.connect_any(start_index, true).await?;
        *self.active.write().await = channel.clone();
        let _ = self.cached_active_tx.send(channel.clone());
        self.prune_removed_seed_channels(&removed_seed_endpoints, channel.address())
            .await;
        Ok(channel)
    }

    async fn connect_any(&self, start_index: usize, reconnect: bool) -> IgniteResult<Arc<Channel>> {
        let addresses = self.topology.endpoints().await;
        let len = addresses.len();
        let mut errors = Vec::new();

        for step in 0..len {
            let index = (start_index + step) % len;
            let address = addresses[index].clone();
            let connect_result = {
                let _connect_guard = self.channel_connect_guard.lock().await;
                let existing = self.channel_for_address(&address).await;

                if let Some(existing) = existing {
                    if existing.is_available() {
                        Ok((existing, false))
                    } else {
                        self.event_bus.emit_connection(
                            if reconnect {
                                ConnectionEventKind::ReconnectAttempt
                            } else {
                                ConnectionEventKind::ConnectAttempt
                            },
                            address.clone(),
                            None,
                        );

                        match Channel::connect(&self.conf, address.clone()).await {
                            Ok(channel) => {
                                self.insert_channel(channel.clone()).await;
                                Ok((channel, true))
                            }
                            Err(err) => Err(err),
                        }
                    }
                } else {
                    self.event_bus.emit_connection(
                        if reconnect {
                            ConnectionEventKind::ReconnectAttempt
                        } else {
                            ConnectionEventKind::ConnectAttempt
                        },
                        address.clone(),
                        None,
                    );

                    match Channel::connect(&self.conf, address.clone()).await {
                        Ok(channel) => {
                            self.insert_channel(channel.clone()).await;
                            Ok((channel, true))
                        }
                        Err(err) => Err(err),
                    }
                }
            };

            match connect_result {
                Ok((channel, inserted_new)) => {
                    if inserted_new {
                        self.on_channel_connected(channel.clone()).await;
                    }
                    self.topology.mark_active(&address).await;
                    self.event_bus.emit_connection(
                        if reconnect {
                            ConnectionEventKind::Reconnected
                        } else {
                            ConnectionEventKind::Connected
                        },
                        address,
                        None,
                    );
                    return Ok(channel);
                }
                Err(err) => {
                    let err_desc = err.to_string();
                    self.event_bus.emit_connection(
                        ConnectionEventKind::ConnectFailed,
                        address,
                        Some(err_desc.clone()),
                    );
                    errors.push(err);

                    if let Some(backoff) = self.conf.reconnect_backoff {
                        tokio::time::sleep(backoff).await;
                    }
                }
            }
        }

        Err(aggregate_connect_errors(errors))
    }

    async fn connect_any_initial(
        conf: &ClientConfig,
        topology: &TopologyCache,
        event_bus: &EventBus,
        start_index: usize,
        reconnect: bool,
    ) -> IgniteResult<Arc<Channel>> {
        let mut errors = Vec::new();

        for _ in 0..=conf.retry_limit {
            match Self::connect_any_initial_once(conf, topology, event_bus, start_index, reconnect)
                .await
            {
                Ok(channel) => return Ok(channel),
                Err(err) => errors.push(err),
            }
        }

        Err(aggregate_connect_errors(errors))
    }

    fn max_attempts(&self) -> usize {
        self.conf.retry_limit
    }

    fn record_reconnect_attempt(&self) -> IgniteResult<()> {
        let Some(throttle) = self.conf.reconnect_throttle else {
            return Ok(());
        };

        let mut attempts = self
            .reconnect_attempts
            .lock()
            .expect("reconnect attempts mutex poisoned");
        let now = Instant::now();

        while attempts
            .front()
            .map(|attempt| now.duration_since(*attempt) >= throttle.window)
            .unwrap_or(false)
        {
            attempts.pop_front();
        }

        if attempts.len() >= throttle.max_attempts {
            return Err(IgniteError::connection(format!(
                "Reconnect throttling is applied: more than {} reconnect attempts within {:?}",
                throttle.max_attempts, throttle.window
            )));
        }

        attempts.push_back(now);
        Ok(())
    }

    fn should_retry(
        &self,
        op_code: i16,
        attempt: usize,
        address: &str,
        err: &IgniteError,
    ) -> IgniteResult<bool> {
        match &self.conf.retry_policy {
            RetryPolicy::Default => Ok(true),
            RetryPolicy::Never => Ok(false),
            RetryPolicy::ReadOnly => Ok(is_read_only_op(op_code)),
            RetryPolicy::Custom(handler) => {
                let ctx = RetryContext {
                    op_code,
                    attempt,
                    address: address.to_owned(),
                    error_kind: err.kind(),
                    error_message: err.to_string(),
                    read_only: is_read_only_op(op_code),
                };
                match handler.decide(&ctx) {
                    Ok(RetryDecision::Retry) => Ok(true),
                    Ok(RetryDecision::Stop) => Ok(false),
                    Err(policy_err) => Err(IgniteError::new(format!(
                        "retry policy for {} failed: {}",
                        address, policy_err
                    ))),
                }
            }
        }
    }

    async fn refresh_seed_endpoints(&self) -> IgniteResult<Vec<String>> {
        if self.conf.address_resolver.is_none() {
            return Ok(Vec::new());
        }

        let next = self.conf.normalized_addresses()?;
        Ok(self.topology.replace_seed_endpoints(next).await)
    }

    fn data_center_id(&self) -> Option<&str> {
        self.conf
            .user_attributes
            .get(IGNITE_DATA_CENTER_ID_ATTR)
            .map(String::as_str)
    }

    async fn default_channel(&self) -> Arc<Channel> {
        let candidates = self.default_channel_candidates().await;
        if candidates.is_empty() {
            let cached = self.cached_active_rx.borrow().clone();
            if cached.is_available() {
                return cached;
            }
            return self.active.read().await.clone();
        }
        if candidates.len() == 1 {
            return candidates[0].clone();
        }

        let index = self
            .next_default_channel_index
            .fetch_add(1, Ordering::Relaxed) as usize
            % candidates.len();
        candidates[index].clone()
    }

    async fn select_channel_for_route(&self, route: &RequestRoute) -> Option<Arc<Channel>> {
        if let Some(address) = route.pinned_address.as_deref() {
            return self.channel_for_address(address).await;
        }

        if let Some(ref node_id) = route.preferred_node_id {
            if let Some(channel) = self.channel_for_node_id(node_id).await {
                return Some(channel);
            }
        }

        None
    }

    async fn default_channel_candidates(&self) -> Vec<Arc<Channel>> {
        let current_dc_channels = self.current_dc_channels().await;
        if !current_dc_channels.is_empty() {
            return current_dc_channels;
        }

        let available = self
            .available_channels_by_addresses(self.topology.endpoints().await)
            .await;
        if !available.is_empty() {
            return available;
        }

        vec![self.active.read().await.clone()]
    }

    async fn current_dc_channels(&self) -> Vec<Arc<Channel>> {
        if !self.conf.partition_awareness_enabled || self.data_center_id().is_none() {
            return Vec::new();
        }

        let node_ids = self.topology.current_dc_nodes().await;
        if node_ids.is_empty() {
            return Vec::new();
        }

        let mut addresses = Vec::new();
        for node_id in node_ids {
            addresses.extend(self.topology.endpoints_for_node(&node_id).await);
        }

        self.dc_channels_by_addresses(addresses).await
    }

    /// Returns available channels for the given addresses without injecting the
    /// active channel.  This is used for DC-aware routing where only channels
    /// belonging to the current data-center should be considered.
    async fn dc_channels_by_addresses(&self, addresses: Vec<String>) -> Vec<Arc<Channel>> {
        self.available_channels_from_index(&addresses)
    }

    async fn available_channels_by_addresses(&self, addresses: Vec<String>) -> Vec<Arc<Channel>> {
        let active = self.active.read().await.clone();
        let mut available = self.available_channels_from_index(&addresses);
        let mut seen_active = false;

        for channel in &available {
            if Arc::ptr_eq(channel, &active) {
                seen_active = true;
                break;
            }
        }

        if !seen_active && active.is_available() {
            available.push(active);
        }

        available
    }

    async fn channel_for_address(&self, address: &str) -> Option<Arc<Channel>> {
        let indexed = self.channels_by_address.load();
        indexed
            .get(address)
            .and_then(|group| group.iter().find(|ch| ch.is_available()).cloned())
    }

    async fn channel_for_node_id(&self, node_id: &Arc<str>) -> Option<Arc<Channel>> {
        // Fast path: lock-free O(1) lookup via ArcSwap
        {
            let nc = self.node_channels.load();
            if let Some(ch) = nc.get(node_id.as_ref()) {
                if ch.is_available() {
                    return Some(ch.clone());
                }
            }
        }
        // Slow path: linear scan of all channels
        let found = {
            let channels = self.channels.read().await;
            channels
                .values()
                .find(|channel| {
                    channel.is_available() && channel.server_node_id() == Some(node_id.as_ref())
                })
                .cloned()
        };
        if let Some(existing) = found {
            let mut new_map: HashMap<Arc<str>, Arc<Channel>> =
                (**self.node_channels.load()).clone();
            new_map.insert(node_id.clone(), existing.clone());
            self.node_channels.store(new_map.into());
            return Some(existing);
        }

        let endpoints = self.topology.endpoints_for_node(node_id).await;
        for endpoint in endpoints {
            if let Some(existing) = self.channel_for_address(&endpoint).await {
                return Some(existing);
            }

            if let Ok(channel) = self.connect_specific(endpoint.clone()).await {
                return Some(channel);
            }
        }

        None
    }

    pub(crate) async fn sql_fields_capabilities(&self) -> SqlFieldsCapabilities {
        let channel = self.default_channel().await;
        SqlFieldsCapabilities {
            partitions_batch_size: channel.supports_query_partitions_batch_size(),
            query_initiator_id: channel.supports_query_initiator_id(),
        }
    }

    pub(crate) async fn index_query_capabilities(&self) -> IndexQueryCapabilities {
        let channel = self.default_channel().await;
        IndexQueryCapabilities {
            index_query_limit: channel.supports_index_query_limit(),
        }
    }

    pub(crate) async fn service_invoke_capabilities(&self) -> ServiceInvokeCapabilities {
        let channel = self.default_channel().await;
        ServiceInvokeCapabilities {
            service_invoke_callctx: channel.supports_service_invoke_callctx(),
        }
    }

    /// True if the default channel negotiated the TRANSACTIONS protocol-version feature
    /// (Java `ProtocolVersionFeature.TRANSACTIONS`, V1_5_0+).
    pub(crate) async fn supports_transactions(&self) -> bool {
        self.default_channel().await.supports_transactions()
    }

    /// True if the default channel negotiated `FORCE_DEACTIVATION_FLAG` (bit 19).
    /// Gates the trailing `bool forceDeactivation` on `CLUSTER_CHANGE_STATE`
    /// (Java §9, FND-056).
    pub(crate) async fn supports_force_deactivation_flag(&self) -> bool {
        self.default_channel().await.supports_force_deactivation_flag()
    }

    async fn connect_specific(&self, address: String) -> IgniteResult<Arc<Channel>> {
        let channel = {
            let _connect_guard = self.channel_connect_guard.lock().await;

            if let Some(existing) = self.channel_for_address(&address).await {
                return Ok(existing);
            }

            self.event_bus.emit_connection(
                ConnectionEventKind::ConnectAttempt,
                address.clone(),
                None,
            );

            let channel = Channel::connect(&self.conf, address.clone()).await?;
            self.insert_channel(channel.clone()).await;
            channel
        };
        self.on_channel_connected(channel.clone()).await;
        self.event_bus
            .emit_connection(ConnectionEventKind::Connected, address, None);
        Ok(channel)
    }

    pub(crate) async fn affinity_node_for_key(
        &self,
        cache_id: i32,
        marshaled_key: &[u8],
        primary: bool,
    ) -> Option<Arc<str>> {
        if !self.conf.partition_awareness_enabled {
            return None;
        }
        // Fast path: single-node cluster — no routing needed
        if self.affinity.is_single_node() {
            return None;
        }
        // Lock-free lookup via ArcSwap: freshness check + partition resolve in ONE atomic load.
        if let Some(node) = self
            .affinity
            .resolve_node_for_key(cache_id, marshaled_key, primary)
        {
            return Some(node);
        }
        // Slow path: mapping was stale — refresh and retry
        self.ensure_affinity_mapping(cache_id).await.ok()?;
        self.affinity
            .node_for_marshaled_key(cache_id, marshaled_key, primary)
            .await
    }

    pub(crate) async fn affinity_node_for_partition(
        &self,
        cache_id: i32,
        partition: i32,
        primary: bool,
    ) -> Option<Arc<str>> {
        if !self.conf.partition_awareness_enabled {
            return None;
        }

        self.ensure_affinity_mapping(cache_id).await.ok()?;
        self.affinity
            .node_for_partition(cache_id, partition, primary)
            .await
    }

    async fn ensure_affinity_mapping(&self, cache_id: i32) -> IgniteResult<()> {
        if !self.affinity.needs_refresh(cache_id).await {
            return Ok(());
        }

        let active = self.active.read().await.clone();
        let dc_aware_request = active.supports_dc_aware();
        let all_affinity_mappings = active.supports_all_affinity_mappings();
        let dc_id = self.data_center_id().map(str::to_owned);
        let (raw, meta): (RawPayload, ResponseMeta) = match self
            .send_and_read_with_meta(
                OpCode::CachePartitions,
                CachePartitionsRequest {
                    all_affinity_mappings,
                    custom_mappings_required: false,
                    include_dc_id: dc_aware_request,
                    dc_id,
                    cache_ids: vec![cache_id],
                },
                RequestRoute::default(),
            )
            .await
        {
            Ok(response) => response,
            Err(err) => {
                self.affinity.invalidate_cache(cache_id).await;
                return Err(err);
            }
        };

        let response_channel = self.channel_for_address(&meta.address).await;
        let dc_aware = response_channel
            .as_ref()
            .map(|channel| channel.supports_dc_aware())
            .unwrap_or(false);
        let response_all_affinity_mappings = response_channel
            .as_ref()
            .map(|channel| channel.supports_all_affinity_mappings())
            .unwrap_or(all_affinity_mappings);

        let response = match CachePartitionsResponse::read_with_flags(
            &mut Cursor::new(&raw.body),
            dc_aware,
            response_all_affinity_mappings,
        ) {
            Ok(response) => response,
            Err(err) => {
                self.affinity.invalidate_cache(cache_id).await;
                return Err(err);
            }
        };

        self.affinity
            .apply(response.topology_version, response.caches)
            .await;

        if dc_aware {
            let _ = self.refresh_data_center_nodes().await;
        }

        Ok(())
    }

    async fn on_channel_connected(&self, channel: Arc<Channel>) {
        self.cache_channel_identity(channel.clone()).await;

        if !self.conf.partition_awareness_enabled {
            return;
        }

        if channel.supports_node_endpoints() {
            match self.refresh_topology_from_channel(channel.clone()).await {
                Ok(()) => return,
                Err(err) => {
                    self.event_bus.emit_connection(
                        ConnectionEventKind::ConnectFailed,
                        channel.address().to_string(),
                        Some(format!("failed to refresh discovered endpoints: {}", err)),
                    );
                }
            }
        }

        self.prime_discovered_channels(channel.address()).await;
    }

    async fn cache_channel_identity(&self, channel: Arc<Channel>) {
        if let Some(node_id) = channel.server_node_id() {
            self.topology.record_node(node_id.to_string()).await;
            self.topology
                .record_node_endpoint(node_id.to_string(), channel.address().to_string())
                .await;
            // Cache node_id → channel for O(1) routing (lock-free via ArcSwap)
            let mut new_map: HashMap<Arc<str>, Arc<Channel>> =
                (**self.node_channels.load()).clone();
            new_map.insert(Arc::from(node_id), channel.clone());
            self.node_channels.store(new_map.into());
        }
    }

    async fn handle_topology_change(&self, version: TopologyVersion) {
        if !self.conf.partition_awareness_enabled {
            return;
        }

        self.affinity.invalidate().await;
        self.topology.clear_current_dc_nodes().await;

        let channel = self.active.read().await.clone();
        if !channel.supports_node_endpoints() {
            self.topology.set_topology_version(version).await;
            return;
        }

        let _ = self.refresh_topology_from_channel(channel).await;
    }

    async fn refresh_topology_from_channel(&self, channel: Arc<Channel>) -> IgniteResult<()> {
        if !self.conf.partition_awareness_enabled || !channel.supports_node_endpoints() {
            return Ok(());
        }

        {
            let _guard = self.discovery_refresh_guard.lock().await;
            let start_topology_version = self
                .topology
                .topology_version()
                .await
                .map(|version| version.major)
                .unwrap_or(-1);

            let raw = self
                .send_internal_and_read_on_channel::<RawPayload>(
                    channel.clone(),
                    OpCode::ClusterGroupGetNodeEndpoints,
                    NodeEndpointsReq {
                        start_topology_version,
                        end_topology_version: -1,
                    },
                )
                .await?;
            let response = NodeEndpointsResp::read(&mut Cursor::new(&raw.body)).map_err(|err| {
                IgniteError::from(
                    format!(
                        "failed to decode discovered endpoints response ({} bytes, prefix {}): {}",
                        raw.body.len(),
                        hex_prefix(&raw.body, 32),
                        err
                    )
                    .as_str(),
                )
            })?;

            let added_nodes = self.normalize_discovered_nodes(response.added_nodes)?;

            self.topology
                .apply_discovery_update(
                    TopologyVersion::new(response.topology_version, 0),
                    added_nodes,
                    &response.removed_node_ids,
                )
                .await;
            self.prune_removed_node_channels(&response.removed_node_ids)
                .await;
        }
        // prime_discovered_channels is called after releasing the guard to avoid
        // re-entrant deadlock: connecting to a discovered node triggers
        // on_channel_connected → refresh_topology_from_channel which needs the same guard.
        self.prime_discovered_channels(channel.address()).await;

        Ok(())
    }

    fn normalize_discovered_nodes(
        &self,
        added_nodes: Vec<DiscoveredNode>,
    ) -> IgniteResult<Vec<DiscoveredNode>> {
        if self.conf.address_resolver.is_none() {
            return Ok(added_nodes);
        }

        let mut resolved_by_port: HashMap<u16, Vec<String>> = HashMap::new();
        for endpoint in self.conf.normalized_addresses()? {
            if let Some(port) = endpoint_port(&endpoint) {
                resolved_by_port.entry(port).or_default().push(endpoint);
            }
        }

        Ok(added_nodes
            .into_iter()
            .map(|node| {
                let mut endpoints = Vec::new();
                for endpoint in node.endpoints {
                    if let Some(port) = endpoint_port(&endpoint) {
                        if let Some(mapped) = resolved_by_port.get(&port) {
                            endpoints.extend(mapped.iter().cloned());
                            continue;
                        }
                    }
                    endpoints.push(endpoint);
                }

                DiscoveredNode {
                    node_id: node.node_id,
                    endpoints: dedupe_endpoints(endpoints),
                }
            })
            .collect())
    }

    async fn refresh_data_center_nodes(&self) -> IgniteResult<()> {
        let Some(dc_id) = self.data_center_id() else {
            self.topology.clear_current_dc_nodes().await;
            return Ok(());
        };

        let channel = self.active.read().await.clone();
        if !channel.supports_dc_aware() {
            self.topology.clear_current_dc_nodes().await;
            return Ok(());
        }

        let response = self
            .send_internal_and_read_on_channel::<DataCenterNodesResp>(
                channel,
                OpCode::ClusterGetDataCenterNodes,
                DataCenterNodesReq { dc_id },
            )
            .await?;

        self.topology.set_current_dc_nodes(response.node_ids).await;
        Ok(())
    }

    async fn prime_discovered_channels(&self, active_address: &str) {
        let endpoints = self.topology.endpoints().await;

        for address in endpoints {
            if address == active_address {
                continue;
            }

            let connect_result = {
                let _connect_guard = self.channel_connect_guard.lock().await;
                if self.channel_for_address(&address).await.is_some() {
                    continue;
                }

                self.event_bus.emit_connection(
                    ConnectionEventKind::ConnectAttempt,
                    address.clone(),
                    None,
                );

                match Channel::connect(&self.conf, address.clone()).await {
                    Ok(channel) => {
                        self.insert_channel(channel.clone()).await;
                        Ok(channel)
                    }
                    Err(err) => Err(err),
                }
            };

            match connect_result {
                Ok(channel) => {
                    self.cache_channel_identity(channel.clone()).await;
                    self.event_bus
                        .emit_connection(ConnectionEventKind::Connected, address, None);
                }
                Err(err) => {
                    self.event_bus.emit_connection(
                        ConnectionEventKind::ConnectFailed,
                        address,
                        Some(err.to_string()),
                    );
                }
            }
        }
    }

    async fn prune_removed_node_channels(&self, removed_node_ids: &[String]) {
        if removed_node_ids.is_empty() {
            return;
        }

        // Prune node_channels cache (lock-free swap)
        {
            let current = self.node_channels.load();
            let new_map: HashMap<Arc<str>, Arc<Channel>> = current
                .iter()
                .filter(|(node_id, _)| {
                    !removed_node_ids
                        .iter()
                        .any(|removed| removed.as_str() == node_id.as_ref())
                })
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            if new_map.len() != current.len() {
                self.node_channels.store(new_map.into());
            }
        }

        let removed_channels = {
            let mut channels = self.channels.write().await;
            let mut removed_addresses = Vec::new();

            for (address, channel) in channels.iter() {
                if channel
                    .server_node_id()
                    .map(|node_id| removed_node_ids.iter().any(|removed| removed == node_id))
                    .unwrap_or(false)
                {
                    removed_addresses.push(address.clone());
                }
            }

            let mut removed_channels = Vec::with_capacity(removed_addresses.len());
            for address in removed_addresses {
                if let Some(channel) = channels.remove(&address) {
                    removed_channels.push(channel);
                }
            }
            self.store_channels_by_address(&channels);

            removed_channels
        };

        for channel in removed_channels {
            channel
                .close("channel removed from discovered topology".to_string())
                .await;
        }
    }

    async fn prune_removed_seed_channels(
        &self,
        removed_addresses: &[String],
        active_address: &str,
    ) {
        if removed_addresses.is_empty() {
            return;
        }

        let snapshot = self.topology.snapshot().await;
        let retained = snapshot
            .discovered_endpoints
            .into_iter()
            .collect::<HashSet<_>>();

        let removed_channels = {
            let mut channels = self.channels.write().await;
            let mut removed_channels = Vec::new();

            for address in removed_addresses {
                if address == active_address || retained.contains(address) {
                    continue;
                }

                let keys_to_remove: Vec<String> = channels
                    .iter()
                    .filter(|(_, channel)| channel.address() == address)
                    .map(|(key, _)| key.clone())
                    .collect();
                for key in keys_to_remove {
                    if let Some(channel) = channels.remove(&key) {
                        removed_channels.push(channel);
                    }
                }
            }
            self.store_channels_by_address(&channels);

            removed_channels
        };

        for channel in removed_channels {
            channel
                .close("channel removed from configured address set".to_string())
                .await;
        }
    }

    async fn connect_any_initial_once(
        conf: &ClientConfig,
        topology: &TopologyCache,
        event_bus: &EventBus,
        start_index: usize,
        reconnect: bool,
    ) -> IgniteResult<Arc<Channel>> {
        let addresses = topology.endpoints().await;
        let len = addresses.len();
        let mut errors = Vec::new();

        for step in 0..len {
            let index = (start_index + step) % len;
            let address = addresses[index].clone();

            event_bus.emit_connection(
                if reconnect {
                    ConnectionEventKind::ReconnectAttempt
                } else {
                    ConnectionEventKind::ConnectAttempt
                },
                address.clone(),
                None,
            );

            match Channel::connect(conf, address.clone()).await {
                Ok(channel) => {
                    topology.mark_active(&address).await;
                    event_bus.emit_connection(
                        if reconnect {
                            ConnectionEventKind::Reconnected
                        } else {
                            ConnectionEventKind::Connected
                        },
                        address,
                        None,
                    );
                    return Ok(channel);
                }
                Err(err) => {
                    let err_desc = err.to_string();
                    event_bus.emit_connection(
                        ConnectionEventKind::ConnectFailed,
                        address,
                        Some(err_desc.clone()),
                    );
                    errors.push(err);

                    if let Some(backoff) = conf.reconnect_backoff {
                        tokio::time::sleep(backoff).await;
                    }
                }
            }
        }

        Err(aggregate_connect_errors(errors))
    }

    // FND-007: Java's `TcpClientChannel.java:394@2.17.0` back-patches the
    // frame length (`req.writeInt(0, req.position() - 4)`) after writing
    // the payload, so mismatches between `WriteableReq::size()` and actual
    // written bytes can't corrupt the length prefix. Rust now mirrors
    // Java: reserve 4 bytes up-front, write header and payload, then
    // patch `buf.len() - 4` back at offset 0.
    pub(super) fn encode_request(
        op_code: i16,
        corr_id: i64,
        payload: &impl WriteableReq,
    ) -> IgniteResult<Vec<u8>> {
        let mut buf = Vec::with_capacity(payload.size() + (REQ_HEADER_SIZE_BYTES as usize));
        // Reserve 4 bytes for length prefix (back-patched after write).
        write_i32(&mut buf, 0)?;
        write_i16(&mut buf, op_code)?;
        write_i64(&mut buf, corr_id)?;
        payload.write(&mut buf).map_err(IgniteError::from)?;
        let payload_len = (buf.len() - 4) as i32;
        buf[..4].copy_from_slice(&payload_len.to_le_bytes());
        Ok(buf)
    }
}

impl Drop for ChannelManager {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);

        if let Ok(active) = self.active.try_read() {
            self.event_bus.emit_connection(
                ConnectionEventKind::Closed,
                active.address().to_string(),
                None,
            );
        }

        let mut channels_to_abort = Vec::new();

        if let Ok(channels) = self.channels.try_read() {
            channels_to_abort.extend(channels.values().cloned());
        }

        if let Ok(active) = self.active.try_read() {
            if !channels_to_abort
                .iter()
                .any(|channel| channel.address() == active.address())
            {
                channels_to_abort.push(active.clone());
            }
        }

        for channel in channels_to_abort {
            channel.abort_writer_pump();
            channel.abort_response_pump();
        }

        self.event_bus
            .emit_lifecycle(LifecycleEventKind::Closed, None);
    }
}

fn initial_start_index(addresses: &[String]) -> usize {
    let candidates = lowest_port_indices(addresses);
    if candidates.is_empty() {
        return 0;
    }

    let cursor = NEXT_DEFAULT_START_INDEX.fetch_add(1, Ordering::Relaxed) as usize;
    candidates[cursor % candidates.len()]
}

fn channel_requires_flush(conf: &ClientConfig) -> bool {
    #[cfg(feature = "ssl")]
    {
        conf.tls_conf.is_some()
    }

    #[cfg(not(feature = "ssl"))]
    {
        let _ = conf;
        false
    }
}

fn lowest_port_indices(addresses: &[String]) -> Vec<usize> {
    let mut min_port = None;
    let mut indices = Vec::new();

    for (index, address) in addresses.iter().enumerate() {
        let Some(port) = endpoint_port(address) else {
            continue;
        };

        match min_port {
            None => {
                min_port = Some(port);
                indices.push(index);
            }
            Some(current_min) if port < current_min => {
                min_port = Some(port);
                indices.clear();
                indices.push(index);
            }
            Some(current_min) if port == current_min => indices.push(index),
            Some(_) => {}
        }
    }

    indices
}

fn endpoint_port(address: &str) -> Option<u16> {
    if let Some(host) = address.strip_prefix('[') {
        let closing = host.find(']')?;
        let tail = &host[(closing + 1)..];
        return tail.strip_prefix(':')?.parse().ok();
    }

    address.rsplit_once(':')?.1.parse().ok()
}

fn hex_prefix(bytes: &[u8], limit: usize) -> String {
    bytes
        .iter()
        .take(limit)
        .map(|byte| format!("{byte:02x}"))
        .collect::<Vec<_>>()
        .join("")
}

fn read_flexible_string(reader: &mut impl io::Read) -> IgniteResult<String> {
    let first = read_u8(reader).map_err(IgniteError::from)?;
    match TypeCode::try_from(first) {
        Ok(TypeCode::String) => read_string(reader).map_err(IgniteError::from),
        Ok(TypeCode::Null) => Ok(String::new()),
        Ok(other) => Err(IgniteError::from(
            format!("expected string, got {:?}", other).as_str(),
        )),
        Err(_) => {
            let mut len_tail = [0u8; 3];
            reader
                .read_exact(&mut len_tail)
                .map_err(IgniteError::from)?;
            let str_len = i32::from_le_bytes([first, len_tail[0], len_tail[1], len_tail[2]]);
            if str_len < 0 {
                return Err(IgniteError::from("negative string length"));
            }

            let mut bytes = vec![0u8; str_len as usize];
            reader.read_exact(&mut bytes).map_err(IgniteError::from)?;
            String::from_utf8(bytes).map_err(|err| IgniteError::from(err.to_string().as_str()))
        }
    }
}

fn is_read_only_op(op_code: i16) -> bool {
    matches!(
        op_code,
        x if x == OpCode::GetIdleTimeout as i16
            || x == OpCode::CacheGetNames as i16
            || x == OpCode::CacheGetConfiguration as i16
            || x == OpCode::CacheGet as i16
            || x == OpCode::CacheGetAll as i16
            || x == OpCode::CacheContainsKey as i16
            || x == OpCode::CacheContainsKeys as i16
            || x == OpCode::CacheGetSize as i16
            || x == OpCode::ClusterGetState as i16
            || x == OpCode::ClusterGetWalState as i16
            || x == OpCode::ServiceGetDescriptors as i16
            || x == OpCode::ServiceGetDescriptor as i16
            || x == OpCode::ServiceGetTopology as i16
            || x == OpCode::GetBinaryTypeName as i16
            || x == OpCode::GetBinaryType as i16
            || x == OpCode::GetBinaryConfiguration as i16
    )
}

#[cfg(test)]
mod tests {
    use super::{
        initial_start_index, lowest_port_indices, ChannelManager, NEXT_DEFAULT_START_INDEX,
        REQ_HEADER_SIZE_BYTES,
    };
    use crate::WriteableReq;
    use std::io::{self, Write};
    use std::sync::atomic::Ordering;

    /// FND-007: Request frame's length prefix must be written as the actual
    /// payload size, not `size()` estimate. Java's `TcpClientChannel.java:394@2.17.0`
    /// back-patches `position - 4` after writing, so mismatches between
    /// `size()` and `write()` can't corrupt the length prefix.
    ///
    /// This test constructs a payload whose `size()` lies (reports 3 but
    /// writes 7 bytes). A correct implementation writes the length prefix
    /// from the *actual* written bytes, so the receiver can parse the
    /// frame. A lazy implementation using `size()` will emit the wrong
    /// length prefix, and the server drops the frame.
    struct LyingPayload;

    impl WriteableReq for LyingPayload {
        fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
            writer.write_all(b"0123456") // 7 bytes
        }

        fn size(&self) -> usize {
            3 // lies: claims 3, writes 7
        }
    }

    #[test]
    fn encode_request_backpatches_length_on_size_mismatch() {
        let buf = ChannelManager::encode_request(0x42i16, 0x11_22_33_44_55_66_77_88i64, &LyingPayload)
            .expect("encode");
        // Frame layout:
        //   [0..4]  i32 length (should equal buf.len() - 4 per Java)
        //   [4..6]  i16 op_code
        //   [6..14] i64 corr_id
        //   [14..]  payload
        let written_len = i32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
        let header_and_payload = (buf.len() - 4) as i32;
        assert_eq!(
            written_len, header_and_payload,
            "length prefix must match actual bytes written after the prefix \
             (Java TcpClientChannel.java:394 back-patches `position - 4`)"
        );
        assert_eq!(
            written_len,
            REQ_HEADER_SIZE_BYTES + 7,
            "op_code (2) + corr_id (8) + 7 payload bytes = 10 + 7 = 17"
        );
    }

    #[test]
    fn encode_request_backpatches_length_on_size_match() {
        // Baseline: when size() is honest, the length is still right.
        struct HonestPayload;
        impl WriteableReq for HonestPayload {
            fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
                writer.write_all(b"abc")
            }
            fn size(&self) -> usize {
                3
            }
        }
        let buf = ChannelManager::encode_request(0x01i16, 42i64, &HonestPayload).expect("encode");
        let written_len = i32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
        assert_eq!(written_len, (buf.len() - 4) as i32);
    }

    /// Migrated from Apache Ignite `ReliableChannelTest.testDefaultChannelBalancing`:
    /// <https://github.com/apache/ignite/blob/ignite-2.15.0/modules/core/src/test/java/org/apache/ignite/internal/client/thin/ReliableChannelTest.java#L95-L118>
    #[test]
    fn should_rotate_initial_start_index_across_min_port_addresses() {
        NEXT_DEFAULT_START_INDEX.store(0, Ordering::Relaxed);
        let addresses = vec![
            "127.0.0.1:10801".to_string(),
            "127.0.0.2:10800".to_string(),
            "127.0.0.3:10800".to_string(),
            "127.0.0.4:10805".to_string(),
        ];

        assert_eq!(initial_start_index(&addresses), 1);
        assert_eq!(initial_start_index(&addresses), 2);
        assert_eq!(initial_start_index(&addresses), 1);
    }

    #[test]
    fn should_collect_only_lowest_port_indices() {
        let addresses = vec![
            "127.0.0.1:10801".to_string(),
            "127.0.0.2:10800".to_string(),
            "127.0.0.3:10800".to_string(),
            "127.0.0.4:10805".to_string(),
        ];

        assert_eq!(lowest_port_indices(&addresses), vec![1, 2]);
    }
}
