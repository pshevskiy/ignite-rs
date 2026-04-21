use std::convert::TryFrom;
use std::sync::Arc;

use crate::affinity::marshal_key;
use crate::api::key_value::{
    CacheBoolResp, CacheDataObjectResp, CacheInfo, CachePairsResp, CacheReq, CacheSizeResp,
    CursorOpenResp,
};
use crate::cache::AtomicityMode::{Atomic, Transactional};
use crate::cache::CacheMode::{Local, Partitioned, Replicated};
use crate::cache::IndexType::{Fulltext, GeoSpatial, Sorted};
use crate::cache::PartitionLossPolicy::{
    Ignore, ReadOnlyAll, ReadOnlySafe, ReadWriteAll, ReadWriteSafe,
};
use crate::cache::RebalanceMode::Async;
use crate::cache::WriteSynchronizationMode::{FullAsync, FullSync, PrimarySync};
use crate::cursor::{EntryCursor, SqlFieldsCursor};
use crate::error::{IgniteError, IgniteResult};

use crate::api::OpCode;
use crate::exec::TokioExec;
use crate::invoke::{
    InvokeAllPreparedFirstRequest, InvokeAllResponse, InvokeAllResult, InvokeRequest,
};
use crate::protocol::complex_obj::IgniteValue;
use crate::query::continuous::{
    ContinuousQuery, ContinuousQueryCursor, ContinuousQueryRequest, ContinuousQueryResponse,
    RegisteredCacheEntryListener,
};
use crate::query::index::{IndexQuery, IndexQueryRequest};
use crate::query::scan::{ScanQuery, ScanQueryRequest};
use crate::query::sql::{
    SqlFieldsOpenResponse, SqlFieldsQuery, SqlFieldsQueryRequest, SqlQuery, SqlQueryRequest, SqlRow,
};
use crate::replication::{
    CacheVersion, ConflictEntry, PutAllConflictRequest, RemoveAllConflictRequest,
};
use crate::transport::RequestRoute;
use crate::tx::TransactionContext;
use crate::{ReadableType, WritableType};
use std::io::{self, Write};
use std::marker::PhantomData;
use std::time::Duration;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AtomicityMode {
    Transactional = 0,
    Atomic = 1,
}

impl TryFrom<i32> for AtomicityMode {
    type Error = IgniteError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Transactional),
            1 => Ok(Atomic),
            _ => Err(IgniteError::from("Cannot read AtomicityMode")),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CacheMode {
    Local = 0,
    Replicated = 1,
    Partitioned = 2,
}

impl TryFrom<i32> for CacheMode {
    type Error = IgniteError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Local),
            1 => Ok(Replicated),
            2 => Ok(Partitioned),
            _ => Err(IgniteError::from("Cannot read CacheMode")),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PartitionLossPolicy {
    ReadOnlySafe = 0,
    ReadOnlyAll = 1,
    ReadWriteSafe = 2,
    ReadWriteAll = 3,
    Ignore = 4,
}

impl TryFrom<i32> for PartitionLossPolicy {
    type Error = IgniteError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(ReadOnlySafe),
            1 => Ok(ReadOnlyAll),
            2 => Ok(ReadWriteSafe),
            3 => Ok(ReadWriteAll),
            4 => Ok(Ignore),
            _ => Err(IgniteError::from("Cannot read PartitionLossPolicy")),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RebalanceMode {
    Sync = 0,
    Async = 1,
    None = 2,
}

impl TryFrom<i32> for RebalanceMode {
    type Error = IgniteError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(RebalanceMode::Sync),
            1 => Ok(Async),
            2 => Ok(RebalanceMode::None),
            _ => Err(IgniteError::from("Cannot read RebalanceMode")),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WriteSynchronizationMode {
    FullSync = 0,
    FullAsync = 1,
    PrimarySync = 2,
}

impl TryFrom<i32> for WriteSynchronizationMode {
    type Error = IgniteError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(FullSync),
            1 => Ok(FullAsync),
            2 => Ok(PrimarySync),
            _ => Err(IgniteError::from("Cannot read WriteSynchronizationMode")),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CachePeekMode {
    All = 0,
    Near = 1,
    Primary = 2,
    Backup = 3,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExpiryDuration {
    Unchanged,
    Eternal,
    Zero,
    Millis(Duration),
}

impl ExpiryDuration {
    pub const fn unchanged() -> Self {
        Self::Unchanged
    }

    pub const fn eternal() -> Self {
        Self::Eternal
    }

    pub const fn zero() -> Self {
        Self::Zero
    }

    pub const fn millis(duration: Duration) -> Self {
        Self::Millis(duration)
    }

    pub(crate) fn to_wire(self) -> i64 {
        match self {
            Self::Unchanged => -2,
            Self::Eternal => -1,
            Self::Zero => 0,
            Self::Millis(duration) => duration.as_millis() as i64,
        }
    }

    pub(crate) fn from_wire(value: i64) -> Self {
        match value {
            -2 => Self::Unchanged,
            -1 => Self::Eternal,
            0 => Self::Zero,
            millis if millis > 0 => Self::Millis(Duration::from_millis(millis as u64)),
            _ => Self::Unchanged,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExpiryPolicy {
    pub create: ExpiryDuration,
    pub update: ExpiryDuration,
    pub access: ExpiryDuration,
}

impl ExpiryPolicy {
    pub const fn new(
        create: ExpiryDuration,
        update: ExpiryDuration,
        access: ExpiryDuration,
    ) -> Self {
        Self {
            create,
            update,
            access,
        }
    }

    pub const fn created(duration: Duration) -> Self {
        Self::new(
            ExpiryDuration::Millis(duration),
            ExpiryDuration::Unchanged,
            ExpiryDuration::Unchanged,
        )
    }

    pub const fn modified(duration: Duration) -> Self {
        Self::new(
            ExpiryDuration::Unchanged,
            ExpiryDuration::Millis(duration),
            ExpiryDuration::Unchanged,
        )
    }

    pub const fn accessed(duration: Duration) -> Self {
        Self::new(
            ExpiryDuration::Unchanged,
            ExpiryDuration::Unchanged,
            ExpiryDuration::Millis(duration),
        )
    }
}

impl Into<u8> for CachePeekMode {
    fn into(self) -> u8 {
        self as u8
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IndexType {
    Sorted = 0,
    Fulltext = 1,
    GeoSpatial = 2,
}

impl TryFrom<u8> for IndexType {
    type Error = IgniteError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Sorted),
            1 => Ok(Fulltext),
            2 => Ok(GeoSpatial),
            _ => Err(IgniteError::from("Cannot read IndexType")),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CacheConfiguration {
    pub atomicity_mode: AtomicityMode,
    pub num_backup: i32,
    pub cache_mode: CacheMode,
    pub copy_on_read: bool,
    pub data_region_name: Option<String>,
    pub eager_ttl: bool,
    pub statistics_enabled: bool,
    pub group_name: Option<String>,
    pub default_lock_timeout_ms: i64,
    pub max_concurrent_async_operations: i32,
    pub max_query_iterators: i32,
    pub name: String,
    pub onheap_cache_enabled: bool,
    pub partition_loss_policy: PartitionLossPolicy,
    pub query_detail_metrics_size: i32,
    pub query_parallelism: i32,
    pub read_from_backup: bool,
    pub rebalance_batch_size: i32,
    pub rebalance_batches_prefetch_count: i64,
    pub rebalance_delay_ms: i64,
    pub rebalance_mode: RebalanceMode,
    pub rebalance_order: i32,
    pub rebalance_throttle_ms: i64,
    pub rebalance_timeout_ms: i64,
    pub sql_escape_all: bool,
    pub sql_index_max_size: i32,
    pub sql_schema: Option<String>,
    pub write_synchronization_mode: WriteSynchronizationMode,
    pub cache_key_configurations: Option<Vec<CacheKeyConfiguration>>,
    pub query_entities: Option<Vec<QueryEntity>>,
    pub expiry_policy: Option<ExpiryPolicy>,
}

impl CacheConfiguration {
    pub fn new(name: &str) -> CacheConfiguration {
        CacheConfiguration {
            name: name.to_owned(),
            ..Self::default()
        }
    }

    fn default() -> CacheConfiguration {
        CacheConfiguration {
            atomicity_mode: AtomicityMode::Atomic,
            num_backup: 0,
            cache_mode: CacheMode::Partitioned,
            copy_on_read: true,
            data_region_name: None,
            eager_ttl: true,
            statistics_enabled: true,
            group_name: None,
            default_lock_timeout_ms: 0,
            max_concurrent_async_operations: 500,
            max_query_iterators: 1024,
            name: String::new(),
            onheap_cache_enabled: false,
            partition_loss_policy: PartitionLossPolicy::Ignore,
            query_detail_metrics_size: 0,
            query_parallelism: 1,
            read_from_backup: true,
            rebalance_batch_size: 512 * 1024, //512K
            rebalance_batches_prefetch_count: 2,
            rebalance_delay_ms: 0,
            rebalance_mode: RebalanceMode::Async,
            rebalance_order: 0,
            rebalance_throttle_ms: 0,
            rebalance_timeout_ms: 10000, //1sec
            sql_escape_all: false,
            sql_index_max_size: -1,
            sql_schema: None,
            write_synchronization_mode: WriteSynchronizationMode::PrimarySync,
            cache_key_configurations: None,
            query_entities: None,
            expiry_policy: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CacheKeyConfiguration {
    pub type_name: String,
    pub affinity_key_field_name: String,
}

impl CacheKeyConfiguration {
    pub fn new(type_name: &str, affinity_key_field_name: &str) -> Self {
        Self {
            type_name: type_name.to_owned(),
            affinity_key_field_name: affinity_key_field_name.to_owned(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryEntity {
    pub(crate) key_type: String,
    pub(crate) value_type: String,
    pub(crate) table: String,
    pub(crate) key_field: String,
    pub(crate) value_field: String,
    pub(crate) query_fields: Vec<QueryField>,
    pub(crate) field_aliases: Vec<(String, String)>,
    pub(crate) query_indexes: Vec<QueryIndex>,
    pub(crate) _default_value: Option<String>, //TODO: find the issue where this field is listed
}

impl QueryEntity {
    pub fn new(key_type: &str, value_type: &str) -> Self {
        Self {
            key_type: key_type.to_owned(),
            value_type: value_type.to_owned(),
            table: String::new(),
            key_field: String::new(),
            value_field: String::new(),
            query_fields: Vec::new(),
            field_aliases: Vec::new(),
            query_indexes: Vec::new(),
            _default_value: None,
        }
    }

    pub fn set_table_name(mut self, table: &str) -> Self {
        self.table = table.to_owned();
        self
    }

    pub fn set_key_field_name(mut self, key_field: &str) -> Self {
        self.key_field = key_field.to_owned();
        self
    }

    pub fn set_value_field_name(mut self, value_field: &str) -> Self {
        self.value_field = value_field.to_owned();
        self
    }

    pub fn set_query_fields(mut self, query_fields: Vec<QueryField>) -> Self {
        self.query_fields = query_fields;
        self
    }

    pub fn add_query_field(mut self, field: QueryField) -> Self {
        self.query_fields.push(field);
        self
    }

    pub fn set_field_aliases(mut self, field_aliases: Vec<(String, String)>) -> Self {
        self.field_aliases = field_aliases;
        self
    }

    pub fn add_field_alias(mut self, field: &str, alias: &str) -> Self {
        self.field_aliases
            .push((field.to_owned(), alias.to_owned()));
        self
    }

    pub fn set_query_indexes(mut self, query_indexes: Vec<QueryIndex>) -> Self {
        self.query_indexes = query_indexes;
        self
    }

    pub fn add_query_index(mut self, index: QueryIndex) -> Self {
        self.query_indexes.push(index);
        self
    }

    pub fn key_type(&self) -> &str {
        &self.key_type
    }

    pub fn value_type(&self) -> &str {
        &self.value_type
    }

    pub fn table_name(&self) -> &str {
        &self.table
    }

    pub fn key_field_name(&self) -> &str {
        &self.key_field
    }

    pub fn value_field_name(&self) -> &str {
        &self.value_field
    }

    pub fn query_fields(&self) -> &[QueryField] {
        &self.query_fields
    }

    pub fn field_aliases(&self) -> &[(String, String)] {
        &self.field_aliases
    }

    pub fn query_indexes(&self) -> &[QueryIndex] {
        &self.query_indexes
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryField {
    pub(crate) name: String,
    pub(crate) type_name: String,
    pub(crate) key_field: bool,
    pub(crate) not_null_constraint: bool,
    pub(crate) precision: i32,
    pub(crate) scale: i32,
}

impl QueryField {
    pub fn new(name: &str, type_name: &str) -> Self {
        Self {
            name: name.to_owned(),
            type_name: type_name.to_owned(),
            key_field: false,
            not_null_constraint: false,
            precision: -1,
            scale: -1,
        }
    }

    pub fn set_key_field(mut self, key_field: bool) -> Self {
        self.key_field = key_field;
        self
    }

    pub fn set_not_null_constraint(mut self, not_null_constraint: bool) -> Self {
        self.not_null_constraint = not_null_constraint;
        self
    }

    pub fn set_precision(mut self, precision: i32) -> Self {
        self.precision = precision;
        self
    }

    pub fn set_scale(mut self, scale: i32) -> Self {
        self.scale = scale;
        self
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn type_name(&self) -> &str {
        &self.type_name
    }

    pub fn is_key_field(&self) -> bool {
        self.key_field
    }

    pub fn has_not_null_constraint(&self) -> bool {
        self.not_null_constraint
    }

    pub fn precision(&self) -> i32 {
        self.precision
    }

    pub fn scale(&self) -> i32 {
        self.scale
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryIndex {
    pub(crate) index_name: String,
    pub(crate) index_type: IndexType,
    pub(crate) inline_size: i32,
    pub(crate) fields: Vec<(String, bool)>,
}

impl QueryIndex {
    pub fn new(index_name: &str, index_type: IndexType) -> Self {
        Self {
            index_name: index_name.to_owned(),
            index_type,
            inline_size: -1,
            fields: Vec::new(),
        }
    }

    pub fn set_inline_size(mut self, inline_size: i32) -> Self {
        self.inline_size = inline_size;
        self
    }

    pub fn set_fields(mut self, fields: Vec<(String, bool)>) -> Self {
        self.fields = fields;
        self
    }

    pub fn add_field(mut self, name: &str, is_descending: bool) -> Self {
        self.fields.push((name.to_owned(), is_descending));
        self
    }

    pub fn index_name(&self) -> &str {
        &self.index_name
    }

    pub fn index_type(&self) -> &IndexType {
        &self.index_type
    }

    pub fn inline_size(&self) -> i32 {
        self.inline_size
    }

    pub fn fields(&self) -> &[(String, bool)] {
        &self.fields
    }
}

/// Ignite key-value cache. This cache is strongly typed and reading/writing some other
/// types leads to errors.
/// All caches created from the single IgniteClient shares the common TCP connection
pub struct CacheCore<K: WritableType + ReadableType, V: WritableType + ReadableType> {
    id: i32,
    pub _name: Arc<str>,
    exec: TokioExec,
    tx: Option<TransactionContext>,
    expiry_policy: Option<ExpiryPolicy>,
    k_phantom: PhantomData<K>,
    v_phantom: PhantomData<V>,
}

struct PreparedKey {
    bytes: Vec<u8>,
}

impl PreparedKey {
    fn new(key: &impl WritableType) -> IgniteResult<Self> {
        Ok(Self {
            bytes: marshal_key(key)?,
        })
    }

    fn as_marshaled(&self) -> &[u8] {
        &self.bytes
    }
}

impl WritableType for PreparedKey {
    fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        writer.write_all(&self.bytes)
    }

    fn size(&self) -> usize {
        self.bytes.len()
    }
}

impl<K: WritableType + ReadableType, V: WritableType + ReadableType> CacheCore<K, V> {
    pub(crate) fn new(id: i32, name: Arc<str>, exec: TokioExec) -> CacheCore<K, V> {
        Self::new_with_tx(id, name, exec, None)
    }

    pub(crate) fn new_with_tx(
        id: i32,
        name: Arc<str>,
        exec: TokioExec,
        tx: Option<TransactionContext>,
    ) -> CacheCore<K, V> {
        CacheCore {
            id,
            _name: name,
            exec,
            tx,
            expiry_policy: None,
            k_phantom: PhantomData,
            v_phantom: PhantomData,
        }
    }

    pub fn with_keep_binary(&self) -> CacheCore<K, crate::binary::BinaryObject> {
        let mut cache = CacheCore::new_with_tx(
            self.id,
            self._name.clone(),
            self.exec.clone(),
            self.tx.clone(),
        );
        cache.expiry_policy = self.expiry_policy;
        cache
    }

    pub fn with_expiry_policy(&self, expiry_policy: ExpiryPolicy) -> Self {
        let mut cache = Self::new_with_tx(
            self.id,
            self._name.clone(),
            self.exec.clone(),
            self.tx.clone(),
        );
        cache.expiry_policy = Some(expiry_policy);
        cache
    }

    fn cache_info(&self, keep_binary: bool) -> IgniteResult<CacheInfo> {
        let info = CacheInfo::new(self.id)
            .with_keep_binary(keep_binary)
            .with_expiry_policy(self.expiry_policy);
        match &self.tx {
            Some(tx) => Ok(info.with_tx_id(Some(tx.tx_id()?))),
            None => Ok(info),
        }
    }

    async fn tx_route(&self) -> IgniteResult<Option<RequestRoute>> {
        match &self.tx {
            Some(tx) => Ok(Some(tx.route().await?)),
            None => Ok(None),
        }
    }

    async fn ensure_tx_cache_ops_allowed(&self) -> IgniteResult<()> {
        match &self.tx {
            Some(tx) => tx.ensure_cache_ops_allowed().await,
            None => Ok(()),
        }
    }

    async fn ensure_clear_allowed(&self, op_name: &str) -> IgniteResult<()> {
        match &self.tx {
            Some(tx) => tx.ensure_clear_allowed(op_name).await,
            None => Ok(()),
        }
    }

    async fn prepare_key_route(
        &self,
        key: &K,
        primary: bool,
    ) -> IgniteResult<(PreparedKey, RequestRoute)> {
        let prepared_key = PreparedKey::new(key)?;
        let route = match self.tx_route().await? {
            Some(route) => route,
            None => {
                match self
                    .exec
                    .affinity_node_for_key(self.id, prepared_key.as_marshaled(), primary)
                    .await
                {
                    Some(node_id) => RequestRoute::preferred_node(node_id),
                    None => RequestRoute::default(),
                }
            }
        };
        Ok((prepared_key, route))
    }

    async fn route_for_partition(
        &self,
        partition: i32,
        primary: bool,
    ) -> IgniteResult<Option<RequestRoute>> {
        if let Some(route) = self.tx_route().await? {
            return Ok(Some(route));
        }

        Ok(self
            .exec
            .affinity_node_for_partition(self.id, partition, primary)
            .await
            .map(RequestRoute::preferred_node))
    }

    async fn map_tx_err<T>(&self, result: IgniteResult<T>) -> IgniteResult<T> {
        match (&self.tx, result) {
            (Some(tx), Err(err)) => {
                if err.is_connection_related() {
                    tx.mark_lost().await;
                    Err(IgniteError::from(
                        format!(
                            "Transaction context has been lost due to connection errors. Cache operations are prohibited until current transaction closed. Cause: {}",
                            err
                        )
                        .as_str(),
                    ))
                } else {
                    Err(err)
                }
            }
            (_, result) => result,
        }
    }

    /// https://ignite.apache.org/docs/latest/binary-client-protocol/sql-and-scan-queries#op_query_scan
    async fn scan_query_impl(&self, query: ScanQuery) -> IgniteResult<EntryCursor<K, V>> {
        self.ensure_tx_cache_ops_allowed().await?;
        let route = match query.partition() {
            Some(partition) => self.route_for_partition(partition, false).await?,
            None => self.tx_route().await?,
        };
        let (open, meta): (CursorOpenResp<K, V>, crate::transport::ResponseMeta) = self
            .map_tx_err(
                self.exec
                    .send_and_read_with_meta(
                        OpCode::QueryScan,
                        ScanQueryRequest {
                            cache_info: self.cache_info(true)?,
                            query,
                        },
                        route.unwrap_or_default(),
                    )
                    .await,
            )
            .await?;

        Ok(EntryCursor::new(
            self.exec.clone(),
            crate::transport::RequestRoute::pinned(meta.address),
            open.cursor_id,
            OpCode::QueryScanCursorGetPage,
            false,
            open.rows,
            open.has_more,
        ))
    }

    async fn index_query_impl(&self, query: IndexQuery) -> IgniteResult<EntryCursor<K, V>> {
        self.ensure_tx_cache_ops_allowed().await?;
        let capabilities = self.exec.index_query_capabilities().await;
        // FND-034: Java throws `ClientFeatureNotSupportedByServerException` rather
        // than emit `limit` without the bit.
        if !capabilities.index_query_limit && query.limit().is_some_and(|v| v > 0) {
            return Err(IgniteError::from(
                "IndexQuery.limit > 0 requires server feature INDEX_QUERY_LIMIT",
            ));
        }
        let route = match query.partition() {
            Some(partition) => self.route_for_partition(partition, false).await?,
            None => self.tx_route().await?,
        };
        let (open, meta): (CursorOpenResp<K, V>, crate::transport::ResponseMeta) = self
            .map_tx_err(
                self.exec
                    .send_and_read_with_meta(
                        OpCode::QueryIndex,
                        IndexQueryRequest {
                            cache_info: self.cache_info(true)?,
                            query: &query,
                            capabilities,
                        },
                        route.unwrap_or_default(),
                    )
                    .await,
            )
            .await?;

        Ok(EntryCursor::new(
            self.exec.clone(),
            crate::transport::RequestRoute::pinned(meta.address),
            open.cursor_id,
            OpCode::QueryIndexCursorGetPage,
            false,
            open.rows,
            open.has_more,
        ))
    }

    async fn continuous_query_impl(
        &self,
        query: ContinuousQuery,
    ) -> IgniteResult<ContinuousQueryCursor<K, V>> {
        self.ensure_tx_cache_ops_allowed().await?;
        let (response, meta): (ContinuousQueryResponse, crate::transport::ResponseMeta) = self
            .map_tx_err(
                self.exec
                    .send_and_read_with_meta(
                        OpCode::QueryContinuous,
                        ContinuousQueryRequest {
                            cache_info: self.cache_info(false)?,
                            query,
                        },
                        self.tx_route().await?.unwrap_or_default(),
                    )
                    .await,
            )
            .await?;

        let receiver = self
            .exec
            .register_notification_listener(
                &meta.address,
                OpCode::QueryContinuousEvent as i16,
                response.resource_id,
            )
            .await?;

        Ok(ContinuousQueryCursor::new(
            self.exec.clone(),
            meta.address,
            response.resource_id,
            receiver,
        ))
    }

    async fn query_scan_impl(&self, page_size: i32) -> IgniteResult<Vec<(Option<K>, Option<V>)>> {
        self.scan_query_impl(ScanQuery::new().with_page_size(page_size))
            .await?
            .fetch_all()
            .await
    }

    async fn sql_query_impl(&self, query: SqlQuery<K, V>) -> IgniteResult<EntryCursor<K, V>> {
        self.ensure_tx_cache_ops_allowed().await?;
        let (open, meta): (CursorOpenResp<K, V>, crate::transport::ResponseMeta) = self
            .map_tx_err(
                self.exec
                    .send_and_read_with_meta(
                        OpCode::QuerySql,
                        SqlQueryRequest {
                            cache_info: self.cache_info(true)?,
                            query: &query,
                        },
                        self.tx_route().await?.unwrap_or_default(),
                    )
                    .await,
            )
            .await?;

        Ok(EntryCursor::new(
            self.exec.clone(),
            crate::transport::RequestRoute::pinned(meta.address),
            open.cursor_id,
            OpCode::QuerySqlCursorGetPage,
            true,
            open.rows,
            open.has_more,
        ))
    }

    async fn sql_fields_open_response_impl<Row>(
        &self,
        query: &SqlFieldsQuery<Row>,
    ) -> IgniteResult<(SqlFieldsOpenResponse, crate::transport::ResponseMeta)> {
        query.validate()?;
        self.ensure_tx_cache_ops_allowed().await?;
        let capabilities = self.exec.sql_fields_capabilities().await;
        self.map_tx_err(
            self.exec
                .send_and_read_with_meta(
                    OpCode::QuerySqlFields,
                    SqlFieldsQueryRequest {
                        cache_info: self.cache_info(true)?,
                        query,
                        capabilities,
                    },
                    self.tx_route().await?.unwrap_or_default(),
                )
                .await,
        )
        .await
    }

    async fn sql_fields_impl<Row: SqlRow>(
        &self,
        query: SqlFieldsQuery<Row>,
    ) -> IgniteResult<SqlFieldsCursor<Row>> {
        let (open, meta) = self.sql_fields_open_response_impl(&query).await?;
        SqlFieldsCursor::new(
            self.exec.clone(),
            crate::transport::RequestRoute::pinned(meta.address),
            open,
        )
    }

    async fn query_close_impl(&self, cursor_id: i64) -> IgniteResult<()> {
        self.map_tx_err(
            self.exec
                .send_with_route(
                    OpCode::ResourceClose,
                    CacheReq::CursorClose::<K, V>(cursor_id),
                    self.tx_route().await?.unwrap_or_default(),
                )
                .await,
        )
        .await
    }

    async fn get_impl(&self, key: &K) -> IgniteResult<Option<V>> {
        self.ensure_tx_cache_ops_allowed().await?;
        let (prepared_key, route) = self.prepare_key_route(key, false).await?;
        let resp: CacheDataObjectResp<V> = self
            .map_tx_err(
                self.exec
                    .send_and_read_with_route(
                        OpCode::CacheGet,
                        CacheReq::Get::<PreparedKey, V>(self.cache_info(false)?, &prepared_key),
                        route,
                    )
                    .await,
            )
            .await?;
        Ok(resp.val)
    }

    async fn get_all_impl(&self, keys: &[K]) -> IgniteResult<Vec<(Option<K>, Option<V>)>> {
        self.ensure_tx_cache_ops_allowed().await?;
        let resp: CachePairsResp<K, V> = self
            .map_tx_err(
                self.exec
                    .send_and_read_with_route(
                        OpCode::CacheGetAll,
                        CacheReq::GetAll::<K, V>(self.cache_info(false)?, keys),
                        self.tx_route().await?.unwrap_or_default(),
                    )
                    .await,
            )
            .await?;
        Ok(resp.val)
    }

    async fn put_impl(&self, key: &K, value: &V) -> IgniteResult<()> {
        self.ensure_tx_cache_ops_allowed().await?;
        let (prepared_key, route) = self.prepare_key_route(key, true).await?;
        self.map_tx_err(
            self.exec
                .send_with_route(
                    OpCode::CachePut,
                    CacheReq::Put::<PreparedKey, V>(self.cache_info(false)?, &prepared_key, value),
                    route,
                )
                .await,
        )
        .await
    }

    async fn put_all_impl(&self, pairs: &[(K, V)]) -> IgniteResult<()> {
        self.ensure_tx_cache_ops_allowed().await?;
        self.map_tx_err(
            self.exec
                .send_with_route(
                    OpCode::CachePutAll,
                    CacheReq::PutAll::<K, V>(self.cache_info(false)?, pairs),
                    self.tx_route().await?.unwrap_or_default(),
                )
                .await,
        )
        .await
    }

    async fn contains_key_impl(&self, key: &K) -> IgniteResult<bool> {
        self.ensure_tx_cache_ops_allowed().await?;
        let (prepared_key, route) = self.prepare_key_route(key, false).await?;
        let resp: CacheBoolResp = self
            .map_tx_err(
                self.exec
                    .send_and_read_with_route(
                        OpCode::CacheContainsKey,
                        CacheReq::ContainsKey::<PreparedKey, V>(
                            self.cache_info(false)?,
                            &prepared_key,
                        ),
                        route,
                    )
                    .await,
            )
            .await?;
        Ok(resp.flag)
    }

    async fn contains_keys_impl(&self, keys: &[K]) -> IgniteResult<bool> {
        self.ensure_tx_cache_ops_allowed().await?;
        let resp: CacheBoolResp = self
            .map_tx_err(
                self.exec
                    .send_and_read_with_route(
                        OpCode::CacheContainsKeys,
                        CacheReq::ContainsKeys::<K, V>(self.cache_info(false)?, keys),
                        self.tx_route().await?.unwrap_or_default(),
                    )
                    .await,
            )
            .await?;
        Ok(resp.flag)
    }

    async fn get_and_put_impl(&self, key: &K, value: &V) -> IgniteResult<Option<V>> {
        self.ensure_tx_cache_ops_allowed().await?;
        let (prepared_key, route) = self.prepare_key_route(key, true).await?;
        let resp: CacheDataObjectResp<V> = self
            .map_tx_err(
                self.exec
                    .send_and_read_with_route(
                        OpCode::CacheGetAndPut,
                        CacheReq::GetAndPut::<PreparedKey, V>(
                            self.cache_info(false)?,
                            &prepared_key,
                            value,
                        ),
                        route,
                    )
                    .await,
            )
            .await?;
        Ok(resp.val)
    }

    async fn get_and_replace_impl(&self, key: &K, value: &V) -> IgniteResult<Option<V>> {
        self.ensure_tx_cache_ops_allowed().await?;
        let (prepared_key, route) = self.prepare_key_route(key, true).await?;
        let resp: CacheDataObjectResp<V> = self
            .map_tx_err(
                self.exec
                    .send_and_read_with_route(
                        OpCode::CacheGetAndReplace,
                        CacheReq::GetAndReplace::<PreparedKey, V>(
                            self.cache_info(false)?,
                            &prepared_key,
                            value,
                        ),
                        route,
                    )
                    .await,
            )
            .await?;
        Ok(resp.val)
    }

    async fn get_and_remove_impl(&self, key: &K) -> IgniteResult<Option<V>> {
        self.ensure_tx_cache_ops_allowed().await?;
        let (prepared_key, route) = self.prepare_key_route(key, true).await?;
        let resp: CacheDataObjectResp<V> = self
            .map_tx_err(
                self.exec
                    .send_and_read_with_route(
                        OpCode::CacheGetAndRemove,
                        CacheReq::GetAndRemove::<PreparedKey, V>(
                            self.cache_info(false)?,
                            &prepared_key,
                        ),
                        route,
                    )
                    .await,
            )
            .await?;
        Ok(resp.val)
    }

    async fn put_if_absent_impl(&self, key: &K, value: &V) -> IgniteResult<bool> {
        self.ensure_tx_cache_ops_allowed().await?;
        let (prepared_key, route) = self.prepare_key_route(key, true).await?;
        let resp: CacheBoolResp = self
            .map_tx_err(
                self.exec
                    .send_and_read_with_route(
                        OpCode::CachePutIfAbsent,
                        CacheReq::PutIfAbsent::<PreparedKey, V>(
                            self.cache_info(false)?,
                            &prepared_key,
                            value,
                        ),
                        route,
                    )
                    .await,
            )
            .await?;
        Ok(resp.flag)
    }

    async fn get_and_put_if_absent_impl(&self, key: &K, value: &V) -> IgniteResult<Option<V>> {
        self.ensure_tx_cache_ops_allowed().await?;
        let (prepared_key, route) = self.prepare_key_route(key, true).await?;
        let resp: CacheDataObjectResp<V> = self
            .map_tx_err(
                self.exec
                    .send_and_read_with_route(
                        OpCode::CacheGetAndPutIfAbsent,
                        CacheReq::GetAndPutIfAbsent::<PreparedKey, V>(
                            self.cache_info(false)?,
                            &prepared_key,
                            value,
                        ),
                        route,
                    )
                    .await,
            )
            .await?;
        Ok(resp.val)
    }

    async fn replace_impl(&self, key: &K, value: &V) -> IgniteResult<bool> {
        self.ensure_tx_cache_ops_allowed().await?;
        let (prepared_key, route) = self.prepare_key_route(key, true).await?;
        let resp: CacheBoolResp = self
            .map_tx_err(
                self.exec
                    .send_and_read_with_route(
                        OpCode::CacheReplace,
                        CacheReq::Replace::<PreparedKey, V>(
                            self.cache_info(false)?,
                            &prepared_key,
                            value,
                        ),
                        route,
                    )
                    .await,
            )
            .await?;
        Ok(resp.flag)
    }

    async fn replace_if_equals_impl(&self, key: &K, old: &V, new: &V) -> IgniteResult<bool> {
        self.ensure_tx_cache_ops_allowed().await?;
        let (prepared_key, route) = self.prepare_key_route(key, true).await?;
        let resp: CacheBoolResp = self
            .map_tx_err(
                self.exec
                    .send_and_read_with_route(
                        OpCode::CacheReplaceIfEquals,
                        CacheReq::ReplaceIfEquals::<PreparedKey, V>(
                            self.cache_info(false)?,
                            &prepared_key,
                            old,
                            new,
                        ),
                        route,
                    )
                    .await,
            )
            .await?;
        Ok(resp.flag)
    }

    async fn clear_impl(&self) -> IgniteResult<()> {
        self.ensure_clear_allowed("clear").await?;
        self.exec
            .send_with_route(
                OpCode::CacheClear,
                CacheReq::Clear::<K, V>(self.cache_info(false)?),
                RequestRoute::default(),
            )
            .await
    }

    async fn clear_key_impl(&self, key: &K) -> IgniteResult<()> {
        self.ensure_clear_allowed("clear").await?;
        let (prepared_key, route) = self.prepare_key_route(key, true).await?;
        self.exec
            .send_with_route(
                OpCode::CacheClearKey,
                CacheReq::ClearKey::<PreparedKey, V>(self.cache_info(false)?, &prepared_key),
                route,
            )
            .await
    }

    async fn clear_keys_impl(&self, keys: &[K]) -> IgniteResult<()> {
        self.ensure_clear_allowed("clear").await?;
        self.exec
            .send_with_route(
                OpCode::CacheClearKeys,
                CacheReq::ClearKeys::<K, V>(self.cache_info(false)?, keys),
                RequestRoute::default(),
            )
            .await
    }

    async fn remove_key_impl(&self, key: &K) -> IgniteResult<bool> {
        self.ensure_tx_cache_ops_allowed().await?;
        let (prepared_key, route) = self.prepare_key_route(key, true).await?;
        let resp: CacheBoolResp = self
            .map_tx_err(
                self.exec
                    .send_and_read_with_route(
                        OpCode::CacheRemoveKey,
                        CacheReq::RemoveKey::<PreparedKey, V>(
                            self.cache_info(false)?,
                            &prepared_key,
                        ),
                        route,
                    )
                    .await,
            )
            .await?;
        Ok(resp.flag)
    }

    async fn remove_if_equals_impl(&self, key: &K, value: &V) -> IgniteResult<bool> {
        self.ensure_tx_cache_ops_allowed().await?;
        let (prepared_key, route) = self.prepare_key_route(key, true).await?;
        let resp: CacheBoolResp = self
            .map_tx_err(
                self.exec
                    .send_and_read_with_route(
                        OpCode::CacheRemoveIfEquals,
                        CacheReq::RemoveIfEquals::<PreparedKey, V>(
                            self.cache_info(false)?,
                            &prepared_key,
                            value,
                        ),
                        route,
                    )
                    .await,
            )
            .await?;
        Ok(resp.flag)
    }

    async fn get_size_impl(&self) -> IgniteResult<i64> {
        self.ensure_tx_cache_ops_allowed().await?;
        let modes = Vec::new();
        let resp: CacheSizeResp = self
            .map_tx_err(
                self.exec
                    .send_and_read_with_route(
                        OpCode::CacheGetSize,
                        CacheReq::GetSize::<K, V>(self.cache_info(false)?, modes),
                        self.tx_route().await?.unwrap_or_default(),
                    )
                    .await,
            )
            .await?;
        Ok(resp.size)
    }

    async fn get_size_peek_mode_impl(&self, mode: CachePeekMode) -> IgniteResult<i64> {
        self.ensure_tx_cache_ops_allowed().await?;
        let modes = vec![mode];
        let resp: CacheSizeResp = self
            .map_tx_err(
                self.exec
                    .send_and_read_with_route(
                        OpCode::CacheGetSize,
                        CacheReq::GetSize::<K, V>(self.cache_info(false)?, modes),
                        self.tx_route().await?.unwrap_or_default(),
                    )
                    .await,
            )
            .await?;
        Ok(resp.size)
    }

    async fn get_size_peek_modes_impl(&self, modes: Vec<CachePeekMode>) -> IgniteResult<i64> {
        self.ensure_tx_cache_ops_allowed().await?;
        let resp: CacheSizeResp = self
            .map_tx_err(
                self.exec
                    .send_and_read_with_route(
                        OpCode::CacheGetSize,
                        CacheReq::GetSize::<K, V>(self.cache_info(false)?, modes),
                        self.tx_route().await?.unwrap_or_default(),
                    )
                    .await,
            )
            .await?;
        Ok(resp.size)
    }

    async fn remove_keys_impl(&self, keys: &[K]) -> IgniteResult<()> {
        self.ensure_tx_cache_ops_allowed().await?;
        self.map_tx_err(
            self.exec
                .send_with_route(
                    OpCode::CacheRemoveKeys,
                    CacheReq::RemoveKeys::<K, V>(self.cache_info(false)?, keys),
                    self.tx_route().await?.unwrap_or_default(),
                )
                .await,
        )
        .await
    }

    async fn remove_all_impl(&self) -> IgniteResult<()> {
        self.ensure_tx_cache_ops_allowed().await?;
        self.map_tx_err(
            self.exec
                .send_with_route(
                    OpCode::CacheRemoveAll,
                    CacheReq::RemoveAll::<K, V>(self.cache_info(false)?),
                    self.tx_route().await?.unwrap_or_default(),
                )
                .await,
        )
        .await
    }
}

// Async specialization API (Tokio)
impl<K: WritableType + ReadableType, V: WritableType + ReadableType> CacheCore<K, V> {
    pub async fn scan_query(&self, query: ScanQuery) -> IgniteResult<EntryCursor<K, V>> {
        self.scan_query_impl(query).await
    }

    pub async fn continuous_query(
        &self,
        query: ContinuousQuery,
    ) -> IgniteResult<ContinuousQueryCursor<K, V>> {
        self.continuous_query_impl(query).await
    }

    pub async fn continuous_query_with_initial_scan(
        &self,
        query: ContinuousQuery,
        initial_query: ScanQuery,
    ) -> IgniteResult<(EntryCursor<K, V>, ContinuousQueryCursor<K, V>)> {
        let listener = self.continuous_query_impl(query).await?;
        let initial_cursor = self.scan_query_impl(initial_query).await?;
        Ok((initial_cursor, listener))
    }

    pub async fn register_cache_entry_listener(
        &self,
        name: &str,
        query: ContinuousQuery,
    ) -> IgniteResult<RegisteredCacheEntryListener<K, V>>
    where
        K: Send + 'static,
        V: Send + 'static,
    {
        let (close_tx, mut close_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
        self.exec
            .register_cache_listener_name(self.id, name, close_tx)?;
        let mut cursor = match self.continuous_query_impl(query).await {
            Ok(cursor) => cursor,
            Err(err) => {
                let _ = self.exec.deregister_cache_listener_name(self.id, name);
                return Err(err);
            }
        };

        let registry = self.exec.cache_listener_registry.clone();
        let cache_id = self.id;
        let listener_name = name.to_string();
        let (event_tx, event_rx) = tokio::sync::mpsc::unbounded_channel();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    close = close_rx.recv() => {
                        if close.is_some() {
                            let _ = cursor.close().await;
                        }
                        break;
                    }
                    result = cursor.next_batch() => {
                        match result {
                            Ok(Some(events)) => {
                                for event in events {
                                    if event_tx.send(Ok(event)).is_err() {
                                        let _ = cursor.close().await;
                                        break;
                                    }
                                }
                            }
                            Ok(None) => break,
                            Err(err) => {
                                let _ = event_tx.send(Err(err));
                                let _ = cursor.close().await;
                                break;
                            }
                        }
                    }
                }
            }

            let _ = registry.deregister(cache_id, &listener_name);
        });

        Ok(RegisteredCacheEntryListener::new(
            self.id,
            name.to_string(),
            self.exec.cache_listener_registry.clone(),
            event_rx,
        ))
    }

    pub async fn deregister_cache_entry_listener(&self, name: &str) -> IgniteResult<()> {
        if let Some(close_tx) = self.exec.deregister_cache_listener_name(self.id, name) {
            let _ = close_tx.send(());
        }
        Ok(())
    }

    pub async fn sql_query(&self, query: SqlQuery<K, V>) -> IgniteResult<EntryCursor<K, V>> {
        self.sql_query_impl(query).await
    }

    /// Execute an index query against cache indexes, returning a cursor of key-value entries.
    pub async fn index_query(&self, query: IndexQuery) -> IgniteResult<EntryCursor<K, V>> {
        self.index_query_impl(query).await
    }

    pub async fn sql_fields<Row: SqlRow>(
        &self,
        query: SqlFieldsQuery<Row>,
    ) -> IgniteResult<SqlFieldsCursor<Row>> {
        self.sql_fields_impl(query).await
    }

    pub async fn query_scan(&self, page_size: i32) -> IgniteResult<Vec<(Option<K>, Option<V>)>> {
        self.query_scan_impl(page_size).await
    }

    pub async fn query_close(&self, cursor_id: i64) -> IgniteResult<()> {
        self.query_close_impl(cursor_id).await
    }

    pub async fn put_all_conflict(&self, entries: &[(K, ConflictEntry<V>)]) -> IgniteResult<()> {
        self.ensure_tx_cache_ops_allowed().await?;
        self.map_tx_err(
            self.exec
                .send_with_route(
                    OpCode::CachePutAllConflict,
                    PutAllConflictRequest {
                        cache_info: self.cache_info(false)?,
                        entries,
                    },
                    self.tx_route().await?.unwrap_or_default(),
                )
                .await,
        )
        .await
    }

    pub async fn remove_all_conflict(&self, entries: &[(K, CacheVersion)]) -> IgniteResult<()> {
        self.ensure_tx_cache_ops_allowed().await?;
        self.map_tx_err(
            self.exec
                .send_with_route(
                    OpCode::CacheRemoveAllConflict,
                    RemoveAllConflictRequest {
                        cache_info: self.cache_info(false)?,
                        entries,
                    },
                    self.tx_route().await?.unwrap_or_default(),
                )
                .await,
        )
        .await
    }

    pub async fn invoke_binary<R: ReadableType>(
        &self,
        key: &K,
        processor: &crate::binary::BinaryObject,
        args: &[IgniteValue],
    ) -> IgniteResult<Option<R>> {
        self.ensure_tx_cache_ops_allowed().await?;
        // FND gate: Java `TcpClientCache.writeEntryProcessor` throws
        // `ClientFeatureNotSupportedByServerException` when `CACHE_INVOKE`
        // (bit 17) is not negotiated (`TcpClientCache.java:964-965@2.17.0`).
        if !self.exec.supports_cache_invoke().await {
            return Err(IgniteError::from(
                "CACHE_INVOKE is not supported by the server",
            ));
        }
        let (prepared_key, route) = self.prepare_key_route(key, true).await?;
        let resp: CacheDataObjectResp<R> = self
            .map_tx_err(
                self.exec
                    .send_and_read_with_route(
                        OpCode::CacheInvoke,
                        InvokeRequest {
                            cache_info: self.cache_info(false)?,
                            key: &prepared_key,
                            processor,
                            args,
                        },
                        route,
                    )
                    .await,
            )
            .await?;
        Ok(resp.val)
    }

    pub async fn invoke_all_binary<R: ReadableType>(
        &self,
        keys: &[K],
        processor: &crate::binary::BinaryObject,
        args: &[IgniteValue],
    ) -> IgniteResult<Vec<(Option<K>, InvokeAllResult<R>)>> {
        self.ensure_tx_cache_ops_allowed().await?;
        // FND gate: Java `TcpClientCache.writeEntryProcessor` throws
        // `ClientFeatureNotSupportedByServerException` when `CACHE_INVOKE`
        // (bit 17) is not negotiated (`TcpClientCache.java:964-965@2.17.0`).
        if !self.exec.supports_cache_invoke().await {
            return Err(IgniteError::from(
                "CACHE_INVOKE is not supported by the server",
            ));
        }
        let (first_key, remaining_keys) = match keys.split_first() {
            Some((first, remaining)) => (Some(PreparedKey::new(first)?), remaining),
            None => (None, &[][..]),
        };
        let route = match (self.tx_route().await?, first_key.as_ref()) {
            (Some(route), _) => route,
            (None, Some(first_key)) => self
                .exec
                .affinity_node_for_key(self.id, first_key.as_marshaled(), true)
                .await
                .map(RequestRoute::preferred_node)
                .unwrap_or_default(),
            (None, None) => RequestRoute::default(),
        };

        let resp: InvokeAllResponse<K, R> = self
            .map_tx_err(
                self.exec
                    .send_and_read_with_route(
                        OpCode::CacheInvokeAll,
                        InvokeAllPreparedFirstRequest {
                            cache_info: self.cache_info(false)?,
                            first_key: first_key.as_ref(),
                            remaining_keys,
                            processor,
                            args,
                        },
                        route,
                    )
                    .await,
            )
            .await?;
        Ok(resp.entries)
    }

    pub async fn get(&self, key: &K) -> IgniteResult<Option<V>> {
        self.get_impl(key).await
    }
    pub fn name(&self) -> &str {
        &self._name
    }
    pub async fn get_all(&self, keys: &[K]) -> IgniteResult<Vec<(Option<K>, Option<V>)>> {
        self.get_all_impl(keys).await
    }
    pub async fn put(&self, key: &K, value: &V) -> IgniteResult<()> {
        self.put_impl(key, value).await
    }
    pub async fn put_all(&self, pairs: &[(K, V)]) -> IgniteResult<()> {
        self.put_all_impl(pairs).await
    }
    pub async fn contains_key(&self, key: &K) -> IgniteResult<bool> {
        self.contains_key_impl(key).await
    }
    pub async fn contains_keys(&self, keys: &[K]) -> IgniteResult<bool> {
        self.contains_keys_impl(keys).await
    }
    pub async fn get_and_put(&self, key: &K, value: &V) -> IgniteResult<Option<V>> {
        self.get_and_put_impl(key, value).await
    }
    pub async fn get_and_replace(&self, key: &K, value: &V) -> IgniteResult<Option<V>> {
        self.get_and_replace_impl(key, value).await
    }
    pub async fn get_and_remove(&self, key: &K) -> IgniteResult<Option<V>> {
        self.get_and_remove_impl(key).await
    }
    pub async fn put_if_absent(&self, key: &K, value: &V) -> IgniteResult<bool> {
        self.put_if_absent_impl(key, value).await
    }
    pub async fn get_and_put_if_absent(&self, key: &K, value: &V) -> IgniteResult<Option<V>> {
        self.get_and_put_if_absent_impl(key, value).await
    }
    pub async fn replace(&self, key: &K, value: &V) -> IgniteResult<bool> {
        self.replace_impl(key, value).await
    }
    pub async fn replace_if_equals(&self, key: &K, old: &V, new: &V) -> IgniteResult<bool> {
        self.replace_if_equals_impl(key, old, new).await
    }
    pub async fn clear(&self) -> IgniteResult<()> {
        self.clear_impl().await
    }
    pub async fn clear_key(&self, key: &K) -> IgniteResult<()> {
        self.clear_key_impl(key).await
    }
    pub async fn clear_keys(&self, keys: &[K]) -> IgniteResult<()> {
        self.clear_keys_impl(keys).await
    }
    pub async fn remove_key(&self, key: &K) -> IgniteResult<bool> {
        self.remove_key_impl(key).await
    }
    pub async fn remove_if_equals(&self, key: &K, value: &V) -> IgniteResult<bool> {
        self.remove_if_equals_impl(key, value).await
    }
    pub async fn get_size(&self) -> IgniteResult<i64> {
        self.get_size_impl().await
    }
    pub async fn get_size_peek_mode(&self, mode: CachePeekMode) -> IgniteResult<i64> {
        self.get_size_peek_mode_impl(mode).await
    }
    pub async fn get_size_peek_modes(&self, modes: Vec<CachePeekMode>) -> IgniteResult<i64> {
        self.get_size_peek_modes_impl(modes).await
    }
    pub async fn remove_keys(&self, keys: &[K]) -> IgniteResult<()> {
        self.remove_keys_impl(keys).await
    }
    pub async fn remove_all(&self) -> IgniteResult<()> {
        self.remove_all_impl().await
    }
}

// Async cache aliases.
pub type Cache<K, V> = CacheCore<K, V>;
pub type AsyncCache<K, V> = CacheCore<K, V>;
