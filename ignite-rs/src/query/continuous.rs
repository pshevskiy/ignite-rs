use crate::api::key_value::{CacheInfo, CacheReq, KEEP_BINARY_FLAG_MASK};
use crate::api::OpCode;
use crate::connection_async::NotificationFrame;
use crate::error::{IgniteError, IgniteResult};
use crate::exec::TokioExec;
use crate::protocol::{
    read_i32, read_i64, read_u8, write_bool, write_i32, write_i64, write_null, write_u8,
};
use crate::{ReadableReq, ReadableType, WriteableReq};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::io::{self, Read, Write};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContinuousQuery {
    page_size: i32,
    time_interval: Duration,
    include_expired: bool,
}

impl Default for ContinuousQuery {
    fn default() -> Self {
        Self {
            page_size: 1,
            time_interval: Duration::ZERO,
            include_expired: false,
        }
    }
}

impl ContinuousQuery {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_page_size(mut self, page_size: i32) -> Self {
        self.page_size = page_size;
        self
    }

    pub fn with_time_interval(mut self, time_interval: Duration) -> Self {
        self.time_interval = time_interval;
        self
    }

    pub fn with_include_expired(mut self, include_expired: bool) -> Self {
        self.include_expired = include_expired;
        self
    }

    pub fn page_size(&self) -> i32 {
        self.page_size
    }

    pub fn time_interval(&self) -> Duration {
        self.time_interval
    }

    pub fn include_expired(&self) -> bool {
        self.include_expired
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CacheEntryEventType {
    Created,
    Updated,
    Removed,
    Expired,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CacheEntryEvent<K, V> {
    pub event_type: CacheEntryEventType,
    pub key: K,
    pub old_value: Option<V>,
    pub value: Option<V>,
}

pub(crate) struct ContinuousQueryRequest {
    pub(crate) cache_info: CacheInfo,
    pub(crate) query: ContinuousQuery,
}

impl WriteableReq for ContinuousQueryRequest {
    fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        // Java `ClientCacheEntryListenerHandler.java:92-143@2.17.0` — continuous
        // query is not transactional; prefix is `cacheId + keepBinary flag`
        // only. `write_cache_info` would emit expiry-policy / tx-id bytes that
        // mis-align the rest of the frame (FND-020).
        write_i32(writer, self.cache_info.cache_id)?;
        write_u8(writer, self.cache_info.flags & KEEP_BINARY_FLAG_MASK)?;
        write_i32(writer, self.query.page_size)?;
        write_i64(writer, self.query.time_interval.as_millis() as i64)?;
        write_bool(writer, self.query.include_expired)?;
        write_null(writer)?;
        Ok(())
    }

    fn size(&self) -> usize {
        // cacheId(4) + flags(1) + pageSize(4) + timeInterval(8) +
        // includeExpired(1) + null filter-factory(1).
        4 + 1 + 4 + 8 + 1 + 1
    }
}

pub(crate) struct ContinuousQueryResponse {
    pub(crate) resource_id: i64,
}

impl ReadableReq for ContinuousQueryResponse {
    fn read(reader: &mut impl Read) -> IgniteResult<Self> {
        Ok(Self {
            resource_id: read_i64(reader)?,
        })
    }
}

pub struct ContinuousQueryCursor<K: ReadableType, V: ReadableType> {
    exec: TokioExec,
    address: String,
    resource_id: i64,
    receiver: mpsc::UnboundedReceiver<IgniteResult<NotificationFrame>>,
    pending: VecDeque<CacheEntryEvent<K, V>>,
    closed: bool,
}

impl<K: ReadableType, V: ReadableType> ContinuousQueryCursor<K, V> {
    pub(crate) fn new(
        exec: TokioExec,
        address: String,
        resource_id: i64,
        receiver: mpsc::UnboundedReceiver<IgniteResult<NotificationFrame>>,
    ) -> Self {
        Self {
            exec,
            address,
            resource_id,
            receiver,
            pending: VecDeque::new(),
            closed: false,
        }
    }

    pub fn resource_id(&self) -> i64 {
        self.resource_id
    }

    pub async fn next_batch(&mut self) -> IgniteResult<Option<Vec<CacheEntryEvent<K, V>>>> {
        if !self.pending.is_empty() {
            return Ok(Some(self.pending.drain(..).collect()));
        }

        match self.receiver.recv().await {
            Some(Ok(frame)) => {
                let events = decode_notification_batch::<K, V>(&frame)?;
                Ok(Some(events))
            }
            Some(Err(err)) => Err(err),
            None => Ok(None),
        }
    }

    pub async fn next_event(&mut self) -> IgniteResult<Option<CacheEntryEvent<K, V>>> {
        if let Some(event) = self.pending.pop_front() {
            return Ok(Some(event));
        }

        let Some(events) = self.next_batch().await? else {
            return Ok(None);
        };

        self.pending = events.into();
        Ok(self.pending.pop_front())
    }

    pub async fn close(&mut self) -> IgniteResult<()> {
        if self.closed {
            return Ok(());
        }

        self.exec
            .remove_notification_listener(
                &self.address,
                OpCode::QueryContinuousEvent as i16,
                self.resource_id,
            )
            .await;
        self.exec
            .send(
                OpCode::ResourceClose,
                CacheReq::CursorClose::<i32, i32>(self.resource_id),
            )
            .await?;
        self.closed = true;
        Ok(())
    }
}

impl<K: ReadableType, V: ReadableType> Drop for ContinuousQueryCursor<K, V> {
    fn drop(&mut self) {
        if self.closed {
            return;
        }

        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let exec = self.exec.clone();
            let address = self.address.clone();
            let resource_id = self.resource_id;
            handle.spawn(async move {
                exec.remove_notification_listener(
                    &address,
                    OpCode::QueryContinuousEvent as i16,
                    resource_id,
                )
                .await;
                let _ = exec
                    .send(
                        OpCode::ResourceClose,
                        CacheReq::CursorClose::<i32, i32>(resource_id),
                    )
                    .await;
            });
        }
    }
}

#[derive(Default)]
pub(crate) struct CacheListenerRegistry {
    listeners: Mutex<HashMap<(i32, String), mpsc::UnboundedSender<()>>>,
}

impl CacheListenerRegistry {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub(crate) fn register(
        &self,
        cache_id: i32,
        name: &str,
        close_tx: mpsc::UnboundedSender<()>,
    ) -> IgniteResult<()> {
        let mut listeners = self
            .listeners
            .lock()
            .expect("cache listener registry mutex poisoned");
        let key = (cache_id, name.to_string());

        if listeners.contains_key(&key) {
            return Err(IgniteError::from(
                format!(
                    "cache entry listener `{}` is already registered for cache {}",
                    name, cache_id
                )
                .as_str(),
            ));
        }

        listeners.insert(key, close_tx);
        Ok(())
    }

    pub(crate) fn deregister(
        &self,
        cache_id: i32,
        name: &str,
    ) -> Option<mpsc::UnboundedSender<()>> {
        self.listeners
            .lock()
            .expect("cache listener registry mutex poisoned")
            .remove(&(cache_id, name.to_string()))
    }
}

pub struct RegisteredCacheEntryListener<K: ReadableType, V: ReadableType> {
    cache_id: i32,
    name: String,
    registry: Arc<CacheListenerRegistry>,
    events: mpsc::UnboundedReceiver<IgniteResult<CacheEntryEvent<K, V>>>,
    closed: bool,
}

impl<K: ReadableType, V: ReadableType> RegisteredCacheEntryListener<K, V> {
    pub(crate) fn new(
        cache_id: i32,
        name: String,
        registry: Arc<CacheListenerRegistry>,
        events: mpsc::UnboundedReceiver<IgniteResult<CacheEntryEvent<K, V>>>,
    ) -> Self {
        Self {
            cache_id,
            name,
            registry,
            events,
            closed: false,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub async fn next_event(&mut self) -> IgniteResult<Option<CacheEntryEvent<K, V>>> {
        match self.events.recv().await {
            Some(Ok(event)) => Ok(Some(event)),
            Some(Err(err)) => Err(err),
            None => Ok(None),
        }
    }

    pub async fn close(&mut self) -> IgniteResult<()> {
        if self.closed {
            return Ok(());
        }

        if let Some(close_tx) = self.registry.deregister(self.cache_id, &self.name) {
            let _ = close_tx.send(());
        }
        self.closed = true;
        Ok(())
    }
}

impl<K: ReadableType, V: ReadableType> Drop for RegisteredCacheEntryListener<K, V> {
    fn drop(&mut self) {
        if self.closed {
            return;
        }

        if let Some(close_tx) = self.registry.deregister(self.cache_id, &self.name) {
            let _ = close_tx.send(());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::key_value::KEEP_BINARY_FLAG_MASK;
    use crate::cache::{ExpiryDuration, ExpiryPolicy};

    /// FND-020: `ClientCacheEntryListenerHandler.java:92-143@2.17.0` expects the
    /// continuous-query prefix to be `cacheId (i32) + flags (i8)` followed by
    /// `pageSize (i32) + timeInterval (i64) + includeExpired (bool) + null
    /// filter-factory`. No expiry-policy triple, no tx-id — those fields are
    /// part of `write_cache_info` but continuous-query is not transactional.
    /// Rust previously called `write_cache_info`, so any caller that set an
    /// expiry policy or tx id mis-aligned the rest of the frame.
    #[test]
    fn continuous_query_request_uses_cache_id_and_flags_only() {
        let cache_info = CacheInfo::new(0x11223344)
            .with_keep_binary(true)
            .with_expiry_policy(Some(ExpiryPolicy::new(
                ExpiryDuration::Zero,
                ExpiryDuration::Unchanged,
                ExpiryDuration::Zero,
            )));
        let req = ContinuousQueryRequest {
            cache_info,
            query: ContinuousQuery::default()
                .with_page_size(32)
                .with_time_interval(Duration::from_millis(1_000))
                .with_include_expired(true),
        };
        let mut actual = Vec::new();
        req.write(&mut actual).unwrap();

        // Expected Java wire shape: cacheId + flags + pageSize + timeInterval
        // + includeExpired + null filter-factory.
        let mut expected = Vec::new();
        expected.extend_from_slice(&0x11223344i32.to_le_bytes());
        expected.push(KEEP_BINARY_FLAG_MASK);
        expected.extend_from_slice(&32i32.to_le_bytes());
        expected.extend_from_slice(&1_000i64.to_le_bytes());
        expected.push(1u8); // true
        expected.push(crate::protocol::TypeCode::Null as u8);

        assert_eq!(actual, expected);
        assert_eq!(req.size(), actual.len());
    }
}

fn decode_notification_batch<K: ReadableType, V: ReadableType>(
    frame: &NotificationFrame,
) -> IgniteResult<Vec<CacheEntryEvent<K, V>>> {
    let mut reader = std::io::Cursor::new(&frame.body[frame.payload_offset..]);
    let count = read_i32(&mut reader)?;
    if count < 0 {
        return Err(IgniteError::from("negative continuous query event count"));
    }

    let mut events = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let key = K::read(&mut reader)?
            .ok_or_else(|| IgniteError::from("continuous query event key was null"))?;
        let old_value = V::read(&mut reader)?;
        let value = V::read(&mut reader)?;
        let event_type = match read_u8(&mut reader)? {
            0 => CacheEntryEventType::Created,
            1 => CacheEntryEventType::Updated,
            2 => CacheEntryEventType::Removed,
            3 => CacheEntryEventType::Expired,
            other => {
                return Err(IgniteError::from(
                    format!("unknown continuous query event type {}", other).as_str(),
                ))
            }
        };

        events.push(CacheEntryEvent {
            event_type,
            key,
            old_value,
            value,
        });
    }

    Ok(events)
}
