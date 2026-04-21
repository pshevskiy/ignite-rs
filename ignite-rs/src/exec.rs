use crate::api::OpCode;
use crate::error::IgniteResult;
use crate::query::continuous::CacheListenerRegistry;
use crate::topology::TopologySnapshot;
use crate::transport::{
    ChannelManager, IndexQueryCapabilities, RequestRoute, ResponseMeta, SqlFieldsCapabilities,
};
use crate::{ReadableReq, WriteableReq};
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::sync::mpsc;

#[derive(Clone)]
pub(crate) struct TokioExec {
    pub(crate) transport: Arc<ChannelManager>,
    pub(crate) cache_listener_registry: Arc<CacheListenerRegistry>,
}

impl TokioExec {
    pub(crate) fn new(transport: Arc<ChannelManager>) -> Self {
        Self {
            transport,
            cache_listener_registry: CacheListenerRegistry::new(),
        }
    }

    pub(crate) fn subscribe_events(&self) -> broadcast::Receiver<crate::events::ClientEvent> {
        self.transport.subscribe_events()
    }

    pub(crate) async fn send(&self, op_code: OpCode, data: impl WriteableReq) -> IgniteResult<()> {
        self.transport.send(op_code, data).await
    }

    pub(crate) async fn send_with_route(
        &self,
        op_code: OpCode,
        data: impl WriteableReq,
        route: RequestRoute,
    ) -> IgniteResult<()> {
        self.transport.send_with_route(op_code, data, route).await
    }

    pub(crate) async fn send_and_read<T: ReadableReq>(
        &self,
        op_code: OpCode,
        data: impl WriteableReq,
    ) -> IgniteResult<T> {
        self.transport.send_and_read::<T>(op_code, data).await
    }

    pub(crate) async fn send_and_read_with_route<T: ReadableReq>(
        &self,
        op_code: OpCode,
        data: impl WriteableReq,
        route: RequestRoute,
    ) -> IgniteResult<T> {
        self.transport
            .send_and_read_with_route::<T>(op_code, data, route)
            .await
    }

    pub(crate) async fn send_and_read_with_meta<T: ReadableReq>(
        &self,
        op_code: OpCode,
        data: impl WriteableReq,
        route: RequestRoute,
    ) -> IgniteResult<(T, ResponseMeta)> {
        self.transport
            .send_and_read_with_meta::<T>(op_code, data, route)
            .await
    }

    pub(crate) async fn register_notification_listener(
        &self,
        address: &str,
        op_code: i16,
        resource_id: i64,
    ) -> IgniteResult<
        mpsc::UnboundedReceiver<IgniteResult<crate::connection_async::NotificationFrame>>,
    > {
        self.transport
            .register_notification_listener(address, op_code, resource_id)
            .await
    }

    pub(crate) async fn remove_notification_listener(
        &self,
        address: &str,
        op_code: i16,
        resource_id: i64,
    ) {
        self.transport
            .remove_notification_listener(address, op_code, resource_id)
            .await
    }

    pub(crate) async fn topology_snapshot(&self) -> TopologySnapshot {
        self.transport.topology_snapshot().await
    }

    pub(crate) async fn affinity_node_for_key(
        &self,
        cache_id: i32,
        marshaled_key: &[u8],
        primary: bool,
    ) -> Option<Arc<str>> {
        self.transport
            .affinity_node_for_key(cache_id, marshaled_key, primary)
            .await
    }

    pub(crate) async fn affinity_node_for_partition(
        &self,
        cache_id: i32,
        partition: i32,
        primary: bool,
    ) -> Option<Arc<str>> {
        self.transport
            .affinity_node_for_partition(cache_id, partition, primary)
            .await
    }

    pub(crate) async fn invalidate_affinity_cache(&self, cache_id: i32) {
        self.transport.invalidate_affinity_cache(cache_id).await;
    }

    pub(crate) async fn sql_fields_capabilities(&self) -> SqlFieldsCapabilities {
        self.transport.sql_fields_capabilities().await
    }

    pub(crate) async fn index_query_capabilities(&self) -> IndexQueryCapabilities {
        self.transport.index_query_capabilities().await
    }

    pub(crate) async fn supports_transactions(&self) -> bool {
        self.transport.supports_transactions().await
    }

    pub(crate) fn register_cache_listener_name(
        &self,
        cache_id: i32,
        name: &str,
        close_tx: mpsc::UnboundedSender<()>,
    ) -> IgniteResult<()> {
        self.cache_listener_registry
            .register(cache_id, name, close_tx)
    }

    pub(crate) fn deregister_cache_listener_name(
        &self,
        cache_id: i32,
        name: &str,
    ) -> Option<mpsc::UnboundedSender<()>> {
        self.cache_listener_registry.deregister(cache_id, name)
    }
}
