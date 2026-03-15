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
    read_i32, read_i64, read_string, read_u8, write_i16, write_i32, write_i64, write_string,
    Flag, TypeCode,
};
use crate::topology::{DiscoveredNode, TopologyCache, TopologySnapshot, TopologyVersion};
use crate::{ClientConfig, ReadableReq, RetryContext, RetryDecision, RetryPolicy, WriteableReq};
use std::cmp;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet, VecDeque};
use std::convert::TryFrom;
use std::io;
use std::io::{Cursor, Write};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
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
    inflight: Mutex<HashMap<i64, oneshot::Sender<IgniteResult<ResponseFrame>>>>,
    notification_listeners:
        Mutex<HashMap<(i16, i64), mpsc::UnboundedSender<IgniteResult<NotificationFrame>>>>,
    pending_notifications: Mutex<HashMap<(i16, i64), Vec<NotificationFrame>>>,
    closed: AtomicBool,
    last_error: Mutex<Option<String>>,
    last_send_at: StdMutex<Instant>,
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
            inflight: Mutex::new(HashMap::new()),
            notification_listeners: Mutex::new(HashMap::new()),
            pending_notifications: Mutex::new(HashMap::new()),
            closed: AtomicBool::new(false),
            last_error: Mutex::new(None),
            last_send_at: StdMutex::new(Instant::now()),
            writer_pump: StdMutex::new(None),
            response_pump: StdMutex::new(None),
        })
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

    fn server_node_id(&self) -> Option<&str> {
        self.metadata.server_node_id.as_deref()
    }

    fn idle_for(&self) -> Duration {
        let last_send = *self
            .last_send_at
            .lock()
            .expect("channel last_send_at mutex poisoned");
        last_send.elapsed()
    }

    fn mark_sent(&self) {
        *self
            .last_send_at
            .lock()
            .expect("channel last_send_at mutex poisoned") = Instant::now();
    }

    async fn request(&self, corr_id: i64, request: Vec<u8>) -> IgniteResult<ResponseFrame> {
        if !self.is_available() {
            return Err(self.closed_error().await);
        }

        let (tx, rx) = oneshot::channel();
        self.inflight.lock().await.insert(corr_id, tx);

        if self.writer_tx.send(OutboundRequest { request }).is_err() {
            self.inflight.lock().await.remove(&corr_id);
            return Err(self.closed_error().await);
        }

        let frame = match self.await_response(corr_id, rx).await {
            Ok(frame) => frame,
            Err(err) => {
                self.inflight.lock().await.remove(&corr_id);
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
                        let waiter = channel.inflight.lock().await.remove(&frame.correlation_id);
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

        let pending = {
            let mut inflight = self.inflight.lock().await;
            std::mem::take(&mut *inflight)
        };

        for (_, waiter) in pending {
            let _ = waiter.send(Err(IgniteError::connection(detail.as_str())));
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

pub(crate) struct ChannelManager {
    conf: ClientConfig,
    affinity: AffinityCache,
    topology: TopologyCache,
    event_bus: EventBus,
    channels: RwLock<HashMap<String, Arc<Channel>>>,
    active: RwLock<Arc<Channel>>,
    reconnect_guard: Mutex<()>,
    discovery_refresh_guard: Mutex<()>,
    channel_connect_guard: Mutex<()>,
    next_correlation_id: AtomicI64,
    next_default_channel_index: AtomicI64,
    reconnect_attempts: StdMutex<VecDeque<Instant>>,
    shutdown: Arc<AtomicBool>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct RequestRoute {
    pinned_address: Option<String>,
    preferred_node_id: Option<String>,
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

    pub(crate) fn preferred_node(node_id: String) -> Self {
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
        let mut channels = HashMap::new();
        channels.insert(active.address().to_string(), active.clone());

        let manager = Self {
            conf,
            affinity,
            topology,
            event_bus,
            channels: RwLock::new(channels),
            active: RwLock::new(active.clone()),
            reconnect_guard: Mutex::new(()),
            discovery_refresh_guard: Mutex::new(()),
            channel_connect_guard: Mutex::new(()),
            next_correlation_id: AtomicI64::new(1),
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
        let (flag, _body, _meta) = self
            .round_trip_with_route(op_code as i16, corr_id, request, true, route)
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
        let (value, _meta) = self.send_and_read_with_meta(op_code, data, route).await?;
        Ok(value)
    }

    pub(crate) async fn send_and_read_with_meta<T: ReadableReq>(
        &self,
        op_code: OpCode,
        data: impl WriteableReq,
        route: RequestRoute,
    ) -> IgniteResult<(T, ResponseMeta)> {
        let corr_id = self.next_correlation_id.fetch_add(1, Ordering::Relaxed);
        let request = Self::encode_request(op_code as i16, corr_id, &data)?;
        let (flag, body, meta) = self
            .round_trip_with_route(op_code as i16, corr_id, request, true, route)
            .await?;
        match flag {
            Success => {
                let mut cur = Cursor::new(body);
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
                let mut cur = Cursor::new(frame.body);
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
        request: Vec<u8>,
        emit_request_events: bool,
    ) -> IgniteResult<(Flag, Vec<u8>, ResponseMeta)> {
        let max_attempts = self.max_attempts();
        let mut attempt = 0usize;
        let mut started_emitted = false;

        loop {
            let channel = self.default_channel().await;
            let address = channel.address().to_string();

            if emit_request_events && !started_emitted {
                self.event_bus.emit_request(
                    RequestEventKind::Started,
                    op_code,
                    corr_id,
                    address.clone(),
                    None,
                );
                started_emitted = true;
            }

            match channel.request(corr_id, request.clone()).await {
                Ok(response) => {
                    if let Some(version) = response.topology_version {
                        self.handle_topology_change(version).await;
                    }

                    if emit_request_events {
                        match &response.flag {
                            Success => self.event_bus.emit_request(
                                RequestEventKind::Succeeded,
                                op_code,
                                corr_id,
                                address.clone(),
                                None,
                            ),
                            Failure { err_msg } => self.event_bus.emit_request(
                                RequestEventKind::Failed,
                                op_code,
                                corr_id,
                                address.clone(),
                                Some(err_msg.clone()),
                            ),
                        }
                    }

                    return Ok((response.flag, response.body, ResponseMeta { address }));
                }
                Err(err) => {
                    let err_desc = err.to_string();

                    if emit_request_events {
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

                    if emit_request_events {
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

    async fn round_trip_with_route(
        &self,
        op_code: i16,
        corr_id: i64,
        request: Vec<u8>,
        emit_request_events: bool,
        route: RequestRoute,
    ) -> IgniteResult<(Flag, Vec<u8>, ResponseMeta)> {
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
        let address = channel.address().to_string();

        if emit_request_events {
            self.event_bus.emit_request(
                RequestEventKind::Started,
                op_code,
                corr_id,
                address.clone(),
                None,
            );
        }

        match channel.request(corr_id, request.clone()).await {
            Ok(response) => {
                if let Some(version) = response.topology_version {
                    self.handle_topology_change(version).await;
                }

                if emit_request_events {
                    match &response.flag {
                        Success => self.event_bus.emit_request(
                            RequestEventKind::Succeeded,
                            op_code,
                            corr_id,
                            address.clone(),
                            None,
                        ),
                        Failure { err_msg } => self.event_bus.emit_request(
                            RequestEventKind::Failed,
                            op_code,
                            corr_id,
                            address.clone(),
                            Some(err_msg.clone()),
                        ),
                    }
                }

                Ok((response.flag, response.body, ResponseMeta { address }))
            }
            Err(err) => {
                if emit_request_events {
                    self.event_bus.emit_request(
                        RequestEventKind::Failed,
                        op_code,
                        corr_id,
                        address.clone(),
                        Some(err.to_string()),
                    );
                }

                self.event_bus.emit_connection(
                    ConnectionEventKind::Closed,
                    address.clone(),
                    Some(err.to_string()),
                );

                if route.allow_retry {
                    self.affinity.invalidate().await;
                    self.round_trip_internal(op_code, corr_id, request, emit_request_events)
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
                let existing = {
                    let channels = self.channels.read().await;
                    channels.get(&address).cloned()
                };

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
                                match self.channels.write().await.entry(address.clone()) {
                                    Entry::Vacant(entry) => {
                                        entry.insert(channel.clone());
                                    }
                                    Entry::Occupied(mut entry) => {
                                        entry.insert(channel.clone());
                                    }
                                }
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
                            self.channels
                                .write()
                                .await
                                .insert(address.clone(), channel.clone());
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

        if let Some(node_id) = route.preferred_node_id.as_deref() {
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

        self.available_channels_by_addresses(addresses).await
    }

    async fn available_channels_by_addresses(&self, addresses: Vec<String>) -> Vec<Arc<Channel>> {
        let active = self.active.read().await.clone();
        let channels = self.channels.read().await;
        let mut available = Vec::new();
        let mut seen = HashSet::new();

        for address in addresses {
            let Some(channel) = channels.get(&address) else {
                continue;
            };
            if !channel.is_available() || !seen.insert(address) {
                continue;
            }
            available.push(channel.clone());
        }

        if active.is_available() && seen.insert(active.address().to_string()) {
            available.push(active);
        }

        available
    }

    async fn channel_for_address(&self, address: &str) -> Option<Arc<Channel>> {
        self.channels
            .read()
            .await
            .get(address)
            .cloned()
            .filter(|channel| channel.is_available())
    }

    async fn channel_for_node_id(&self, node_id: &str) -> Option<Arc<Channel>> {
        {
            let channels = self.channels.read().await;
            if let Some(existing) = channels
                .values()
                .find(|channel| channel.is_available() && channel.server_node_id() == Some(node_id))
            {
                return Some(existing.clone());
            }
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

    async fn connect_specific(&self, address: String) -> IgniteResult<Arc<Channel>> {
        let channel = {
            let _connect_guard = self.channel_connect_guard.lock().await;

            if let Some(existing) = self.channels.read().await.get(&address).cloned() {
                if existing.is_available() {
                    return Ok(existing);
                }
            }

            self.event_bus.emit_connection(
                ConnectionEventKind::ConnectAttempt,
                address.clone(),
                None,
            );

            let channel = Channel::connect(&self.conf, address.clone()).await?;
            self.channels
                .write()
                .await
                .insert(address.clone(), channel.clone());
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
    ) -> Option<String> {
        if !self.conf.partition_awareness_enabled {
            return None;
        }

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
    ) -> Option<String> {
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

        let dc_aware_request = self.active.read().await.supports_dc_aware();
        let dc_id = self.data_center_id().map(str::to_owned);
        let (raw, meta): (RawPayload, ResponseMeta) = match self
            .send_and_read_with_meta(
                OpCode::CachePartitions,
                CachePartitionsRequest {
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

        let dc_aware = self
            .channel_for_address(&meta.address)
            .await
            .map(|channel| channel.supports_dc_aware())
            .unwrap_or(false);

        let response = match CachePartitionsResponse::read_with_dc_aware(
            &mut Cursor::new(&raw.body),
            dc_aware,
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
        if let Some(node_id) = channel.server_node_id() {
            self.topology.record_node(node_id.to_string()).await;
            self.topology
                .record_node_endpoint(node_id.to_string(), channel.address().to_string())
                .await;
        }

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
                let existing = {
                    let channels = self.channels.read().await;
                    channels.get(&address).cloned()
                };

                if let Some(existing) = existing {
                    if existing.is_available() {
                        continue;
                    }
                }

                self.event_bus.emit_connection(
                    ConnectionEventKind::ConnectAttempt,
                    address.clone(),
                    None,
                );

                match Channel::connect(&self.conf, address.clone()).await {
                    Ok(channel) => {
                        match self.channels.write().await.entry(address.clone()) {
                            Entry::Vacant(entry) => {
                                entry.insert(channel.clone());
                            }
                            Entry::Occupied(mut entry) => {
                                entry.insert(channel.clone());
                            }
                        }
                        Ok(channel)
                    }
                    Err(err) => Err(err),
                }
            };

            match connect_result {
                Ok(channel) => {
                    if let Some(node_id) = channel.server_node_id() {
                        self.topology.record_node(node_id.to_string()).await;
                    }

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

                if let Some(channel) = channels.remove(address) {
                    removed_channels.push(channel);
                }
            }

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

    fn encode_request(
        op_code: i16,
        corr_id: i64,
        payload: &impl WriteableReq,
    ) -> IgniteResult<Vec<u8>> {
        let mut buf = Vec::with_capacity(payload.size() + (REQ_HEADER_SIZE_BYTES as usize));
        Self::write_req_header(&mut buf, payload.size(), op_code, corr_id)?;
        payload.write(&mut buf).map_err(IgniteError::from)?;
        Ok(buf)
    }

    fn write_req_header(
        writer: &mut dyn Write,
        payload_len: usize,
        op_code: i16,
        corr_id: i64,
    ) -> io::Result<()> {
        write_i32(writer, payload_len as i32 + REQ_HEADER_SIZE_BYTES)?;
        write_i16(writer, op_code)?;
        write_i64(writer, corr_id)?;
        Ok(())
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
    bytes.iter()
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
            String::from_utf8(bytes)
                .map_err(|err| IgniteError::from(err.to_string().as_str()))
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
    use super::{initial_start_index, lowest_port_indices, NEXT_DEFAULT_START_INDEX};
    use std::sync::atomic::Ordering;

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
