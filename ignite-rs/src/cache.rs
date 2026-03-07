use std::convert::TryFrom;

use crate::api::key_value::{
    CacheBoolResp, CacheDataObjectResp, CachePairsResp, CacheReq, CacheSizeResp, QueryScanResp,
};
use crate::cache::AtomicityMode::{Atomic, Transactional};
use crate::cache::CacheMode::{Local, Partitioned, Replicated};
use crate::cache::IndexType::{Fulltext, GeoSpatial, Sorted};
use crate::cache::PartitionLossPolicy::{
    Ignore, ReadOnlyAll, ReadOnlySafe, ReadWriteAll, ReadWriteSafe,
};
use crate::cache::RebalanceMode::Async;
use crate::cache::WriteSynchronizationMode::{FullAsync, FullSync, PrimarySync};
use crate::error::{IgniteError, IgniteResult};

use crate::api::OpCode;
use crate::exec::{IgniteFuture, TokioExec};
use crate::protocol::complex_obj::IgniteValue;
use crate::{ReadableType, WritableType};
use std::marker::PhantomData;

#[derive(Clone, Debug)]
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

#[derive(Clone, Debug)]
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

#[derive(Clone, Debug)]
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

#[derive(Clone, Debug)]
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

#[derive(Clone, Debug)]
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

#[derive(Clone, Debug)]
pub enum CachePeekMode {
    All = 0,
    Near = 1,
    Primary = 2,
    Backup = 3,
}

impl Into<u8> for CachePeekMode {
    fn into(self) -> u8 {
        self as u8
    }
}

#[derive(Clone, Debug)]
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

#[derive(Clone, Debug)]
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
        }
    }
}

#[derive(Clone, Debug)]
pub struct CacheKeyConfiguration {
    pub type_name: String,
    pub affinity_key_field_name: String,
}

#[derive(Clone, Debug)]
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

#[derive(Clone, Debug)]
pub struct QueryField {
    pub(crate) name: String,
    pub(crate) type_name: String,
    pub(crate) key_field: bool,
    pub(crate) not_null_constraint: bool,
    pub(crate) precision: i32,
    pub(crate) scale: i32,
}

#[derive(Clone, Debug)]
pub struct QueryIndex {
    pub(crate) index_name: String,
    pub(crate) index_type: IndexType,
    pub(crate) inline_size: i32,
    pub(crate) fields: Vec<(String, bool)>,
}

/// Ignite key-value cache. This cache is strongly typed and reading/writing some other
/// types leads to errors.
/// All caches created from the single IgniteClient shares the common TCP connection
pub struct CacheCore<K: WritableType + ReadableType, V: WritableType + ReadableType> {
    id: i32,
    pub _name: String,
    exec: TokioExec,
    k_phantom: PhantomData<K>,
    v_phantom: PhantomData<V>,
}

impl<K: WritableType + ReadableType, V: WritableType + ReadableType> CacheCore<K, V> {
    pub(crate) fn new(id: i32, name: String, exec: TokioExec) -> CacheCore<K, V> {
        CacheCore {
            id,
            _name: name,
            exec,
            k_phantom: PhantomData,
            v_phantom: PhantomData,
        }
    }

    /// https://ignite.apache.org/docs/latest/binary-client-protocol/sql-and-scan-queries#op_query_scan
    fn query_scan_impl<'a>(
        &'a self,
        page_size: i32,
    ) -> IgniteResultOrFuture<'a, Vec<(Option<K>, Option<V>)>> {
        let fut = self.exec.send_and_read(
            OpCode::QueryScan,
            CacheReq::QueryScan::<K, V>(self.id, page_size),
        );
        self.exec.map(fut, |resp: QueryScanResp<K, V>| resp.val)
    }

    // SQL fields helpers

    fn query_sql_fields_open_impl<'a>(
        &'a self,
        schema: Option<&'a str>,
        sql: &'a str,
        page_size: i32,
        args: &'a [IgniteValue],
    ) -> IgniteResultOrFuture<'a, crate::api::key_value::SqlFieldsOpenRespLong> {
        self.exec.send_and_read(
            OpCode::QuerySqlFields,
            CacheReq::QuerySqlFieldsArgs::<K, V>(self.id, schema, page_size, sql, args),
        )
    }

    fn query_sql_fields_page_impl<'a>(
        &'a self,
        cursor_id: i64,
        page_size: i32,
    ) -> IgniteResultOrFuture<'a, (Vec<i64>, bool)> {
        let fut = self.exec.send_and_read(
            OpCode::QuerySqlFieldsCursorGetPage,
            CacheReq::CursorGetPage::<K, V>(cursor_id, page_size),
        );
        self.exec
            .map(fut, |resp: crate::api::key_value::SqlFieldsPageRespLong| {
                (resp.rows, resp.has_more)
            })
    }

    fn query_close_impl<'a>(&'a self, cursor_id: i64) -> IgniteResultOrFuture<'a, ()> {
        self.exec
            .send(OpCode::QueryClose, CacheReq::CursorClose::<K, V>(cursor_id))
    }

    fn get_impl<'a>(&'a self, key: &'a K) -> IgniteResultOrFuture<'a, Option<V>> {
        let fut = self
            .exec
            .send_and_read(OpCode::CacheGet, CacheReq::Get::<K, V>(self.id, key));
        self.exec.map(fut, |resp: CacheDataObjectResp<V>| resp.val)
    }

    fn get_all_impl<'a>(
        &'a self,
        keys: &'a [K],
    ) -> IgniteResultOrFuture<'a, Vec<(Option<K>, Option<V>)>> {
        let fut = self
            .exec
            .send_and_read(OpCode::CacheGetAll, CacheReq::GetAll::<K, V>(self.id, keys));
        self.exec.map(fut, |resp: CachePairsResp<K, V>| resp.val)
    }

    fn put_impl<'a>(&'a self, key: &'a K, value: &'a V) -> IgniteResultOrFuture<'a, ()> {
        self.exec
            .send(OpCode::CachePut, CacheReq::Put::<K, V>(self.id, key, value))
    }

    fn put_all_impl<'a>(&'a self, pairs: &'a [(K, V)]) -> IgniteResultOrFuture<'a, ()> {
        self.exec.send(
            OpCode::CachePutAll,
            CacheReq::PutAll::<K, V>(self.id, pairs),
        )
    }

    fn contains_key_impl<'a>(&'a self, key: &'a K) -> IgniteResultOrFuture<'a, bool> {
        let fut = self.exec.send_and_read(
            OpCode::CacheContainsKey,
            CacheReq::ContainsKey::<K, V>(self.id, key),
        );
        self.exec.map(fut, |resp: CacheBoolResp| resp.flag)
    }

    fn contains_keys_impl<'a>(&'a self, keys: &'a [K]) -> IgniteResultOrFuture<'a, bool> {
        let fut = self.exec.send_and_read(
            OpCode::CacheContainsKeys,
            CacheReq::ContainsKeys::<K, V>(self.id, keys),
        );
        self.exec.map(fut, |resp: CacheBoolResp| resp.flag)
    }

    fn get_and_put_impl<'a>(
        &'a self,
        key: &'a K,
        value: &'a V,
    ) -> IgniteResultOrFuture<'a, Option<V>> {
        let fut = self.exec.send_and_read(
            OpCode::CacheGetAndPut,
            CacheReq::GetAndPut::<K, V>(self.id, key, value),
        );
        self.exec.map(fut, |resp: CacheDataObjectResp<V>| resp.val)
    }

    fn get_and_replace_impl<'a>(
        &'a self,
        key: &'a K,
        value: &'a V,
    ) -> IgniteResultOrFuture<'a, Option<V>> {
        let fut = self.exec.send_and_read(
            OpCode::CacheGetAndReplace,
            CacheReq::GetAndReplace::<K, V>(self.id, key, value),
        );
        self.exec.map(fut, |resp: CacheDataObjectResp<V>| resp.val)
    }

    fn get_and_remove_impl<'a>(&'a self, key: &'a K) -> IgniteResultOrFuture<'a, Option<V>> {
        let fut = self.exec.send_and_read(
            OpCode::CacheGetAndRemove,
            CacheReq::GetAndRemove::<K, V>(self.id, key),
        );
        self.exec.map(fut, |resp: CacheDataObjectResp<V>| resp.val)
    }

    fn put_if_absent_impl<'a>(
        &'a self,
        key: &'a K,
        value: &'a V,
    ) -> IgniteResultOrFuture<'a, bool> {
        let fut = self.exec.send_and_read(
            OpCode::CachePutIfAbsent,
            CacheReq::PutIfAbsent::<K, V>(self.id, key, value),
        );
        self.exec.map(fut, |resp: CacheBoolResp| resp.flag)
    }

    fn get_and_put_if_absent_impl<'a>(
        &'a self,
        key: &'a K,
        value: &'a V,
    ) -> IgniteResultOrFuture<'a, Option<V>> {
        let fut = self.exec.send_and_read(
            OpCode::CacheGetAndPutIfAbsent,
            CacheReq::GetAndPutIfAbsent::<K, V>(self.id, key, value),
        );
        self.exec.map(fut, |resp: CacheDataObjectResp<V>| resp.val)
    }

    fn replace_impl<'a>(&'a self, key: &'a K, value: &'a V) -> IgniteResultOrFuture<'a, bool> {
        let fut = self.exec.send_and_read(
            OpCode::CacheReplace,
            CacheReq::Replace::<K, V>(self.id, key, value),
        );
        self.exec.map(fut, |resp: CacheBoolResp| resp.flag)
    }

    fn replace_if_equals_impl<'a>(
        &'a self,
        key: &'a K,
        old: &'a V,
        new: &'a V,
    ) -> IgniteResultOrFuture<'a, bool> {
        let fut = self.exec.send_and_read(
            OpCode::CacheReplaceIfEquals,
            CacheReq::ReplaceIfEquals::<K, V>(self.id, key, old, new),
        );
        self.exec.map(fut, |resp: CacheBoolResp| resp.flag)
    }

    fn clear_impl<'a>(&'a self) -> IgniteResultOrFuture<'a, ()> {
        self.exec
            .send(OpCode::CacheClear, CacheReq::Clear::<K, V>(self.id))
    }

    fn clear_key_impl<'a>(&'a self, key: &'a K) -> IgniteResultOrFuture<'a, ()> {
        self.exec.send(
            OpCode::CacheClearKey,
            CacheReq::ClearKey::<K, V>(self.id, key),
        )
    }

    fn clear_keys_impl<'a>(&'a self, keys: &'a [K]) -> IgniteResultOrFuture<'a, ()> {
        self.exec.send(
            OpCode::CacheClearKeys,
            CacheReq::ClearKeys::<K, V>(self.id, keys),
        )
    }

    fn remove_key_impl<'a>(&'a self, key: &'a K) -> IgniteResultOrFuture<'a, bool> {
        let fut = self.exec.send_and_read(
            OpCode::CacheRemoveKey,
            CacheReq::RemoveKey::<K, V>(self.id, key),
        );
        self.exec.map(fut, |resp: CacheBoolResp| resp.flag)
    }

    fn remove_if_equals_impl<'a>(
        &'a self,
        key: &'a K,
        value: &'a V,
    ) -> IgniteResultOrFuture<'a, bool> {
        let fut = self.exec.send_and_read(
            OpCode::CacheRemoveIfEquals,
            CacheReq::RemoveIfEquals::<K, V>(self.id, key, value),
        );
        self.exec.map(fut, |resp: CacheBoolResp| resp.flag)
    }

    fn get_size_impl<'a>(&'a self) -> IgniteResultOrFuture<'a, i64> {
        let modes = Vec::new();
        let fut = self.exec.send_and_read(
            OpCode::CacheGetSize,
            CacheReq::GetSize::<K, V>(self.id, modes),
        );
        self.exec.map(fut, |resp: CacheSizeResp| resp.size)
    }

    fn get_size_peek_mode_impl<'a>(&'a self, mode: CachePeekMode) -> IgniteResultOrFuture<'a, i64> {
        let modes = vec![mode];
        let fut = self.exec.send_and_read(
            OpCode::CacheGetSize,
            CacheReq::GetSize::<K, V>(self.id, modes),
        );
        self.exec.map(fut, |resp: CacheSizeResp| resp.size)
    }

    fn get_size_peek_modes_impl<'a>(
        &'a self,
        modes: Vec<CachePeekMode>,
    ) -> IgniteResultOrFuture<'a, i64> {
        let fut = self.exec.send_and_read(
            OpCode::CacheGetSize,
            CacheReq::GetSize::<K, V>(self.id, modes),
        );
        self.exec.map(fut, |resp: CacheSizeResp| resp.size)
    }

    fn remove_keys_impl<'a>(&'a self, keys: &'a [K]) -> IgniteResultOrFuture<'a, ()> {
        self.exec.send(
            OpCode::CacheRemoveKeys,
            CacheReq::RemoveKeys::<K, V>(self.id, keys),
        )
    }

    fn remove_all_impl<'a>(&'a self) -> IgniteResultOrFuture<'a, ()> {
        self.exec
            .send(OpCode::CacheRemoveAll, CacheReq::RemoveAll::<K, V>(self.id))
    }
}

// Helper alias to shorten generic return type usage
type IgniteResultOrFuture<'a, T> = IgniteFuture<'a, T>;

// Async specialization API (Tokio)
impl<K: WritableType + ReadableType, V: WritableType + ReadableType> CacheCore<K, V> {
    pub async fn query_scan(&self, page_size: i32) -> IgniteResult<Vec<(Option<K>, Option<V>)>> {
        self.query_scan_impl(page_size).await
    }
    pub async fn query_sql_fields_long_with_args(
        &self,
        sql: &str,
        page_size: i32,
        args: &[IgniteValue],
    ) -> IgniteResult<Vec<i64>> {
        let resp = self
            .query_sql_fields_open_impl(None, sql, page_size, args)
            .await?;
        // Best-effort close; ignore error to preserve original return type
        let _ = self.query_close_impl(resp.cursor_id).await;
        Ok(resp.rows)
    }
    pub async fn query_sql_fields_long_with_args_schema(
        &self,
        schema: &str,
        sql: &str,
        page_size: i32,
        args: &[IgniteValue],
    ) -> IgniteResult<Vec<i64>> {
        let resp = self
            .query_sql_fields_open_impl(Some(schema), sql, page_size, args)
            .await?;
        let _ = self.query_close_impl(resp.cursor_id).await;
        Ok(resp.rows)
    }
    pub async fn query_close(&self, cursor_id: i64) -> IgniteResult<()> {
        self.query_close_impl(cursor_id).await
    }
    pub async fn query_sql_fields_long_open_with_args(
        &self,
        sql: &str,
        page_size: i32,
        args: &[IgniteValue],
    ) -> IgniteResult<(i64, Vec<i64>, bool)> {
        let resp = self
            .query_sql_fields_open_impl(None, sql, page_size, args)
            .await?;
        Ok((resp.cursor_id, resp.rows, resp.has_more))
    }
    pub async fn query_sql_fields_long_open_with_args_schema(
        &self,
        schema: &str,
        sql: &str,
        page_size: i32,
        args: &[IgniteValue],
    ) -> IgniteResult<(i64, Vec<i64>, bool)> {
        let resp = self
            .query_sql_fields_open_impl(Some(schema), sql, page_size, args)
            .await?;
        Ok((resp.cursor_id, resp.rows, resp.has_more))
    }
    pub async fn query_sql_fields_long_get_page(
        &self,
        cursor_id: i64,
        page_size: i32,
    ) -> IgniteResult<(Vec<i64>, bool)> {
        self.query_sql_fields_page_impl(cursor_id, page_size).await
    }
    pub async fn query_sql_fields_long_fetch_up_to_with_args(
        &self,
        sql: &str,
        page_size: i32,
        args: &[IgniteValue],
        max: usize,
    ) -> IgniteResult<(Vec<i64>, bool)> {
        let mut out = Vec::new();
        let open = self
            .query_sql_fields_open_impl(None, sql, page_size, args)
            .await?;
        let cursor_id = open.cursor_id;
        for pk in open.rows.into_iter() {
            out.push(pk);
            if out.len() >= max {
                let _ = self.query_close_impl(cursor_id).await;
                return Ok((out, true));
            }
        }
        let mut has_more = open.has_more;
        while has_more {
            let (rows, more) = self
                .query_sql_fields_page_impl(cursor_id, page_size)
                .await?;
            for pk in rows.into_iter() {
                out.push(pk);
                if out.len() >= max {
                    let _ = self.query_close_impl(cursor_id).await;
                    return Ok((out, true));
                }
            }
            has_more = more;
        }
        let _ = self.query_close_impl(cursor_id).await;
        Ok((out, false))
    }
    pub async fn query_sql_fields_long_fetch_up_to_with_args_schema(
        &self,
        schema: &str,
        sql: &str,
        page_size: i32,
        args: &[IgniteValue],
        max: usize,
    ) -> IgniteResult<(Vec<i64>, bool)> {
        let mut out = Vec::new();
        let open = self
            .query_sql_fields_open_impl(Some(schema), sql, page_size, args)
            .await?;
        let cursor_id = open.cursor_id;
        for pk in open.rows.into_iter() {
            out.push(pk);
            if out.len() >= max {
                let _ = self.query_close_impl(cursor_id).await;
                return Ok((out, true));
            }
        }
        let mut has_more = open.has_more;
        while has_more {
            let (rows, more) = self
                .query_sql_fields_page_impl(cursor_id, page_size)
                .await?;
            for pk in rows.into_iter() {
                out.push(pk);
                if out.len() >= max {
                    let _ = self.query_close_impl(cursor_id).await;
                    return Ok((out, true));
                }
            }
            has_more = more;
        }
        let _ = self.query_close_impl(cursor_id).await;
        Ok((out, false))
    }
    pub async fn query_sql_fields_long_fetch_all_with_args(
        &self,
        sql: &str,
        page_size: i32,
        args: &[IgniteValue],
    ) -> IgniteResult<Vec<i64>> {
        let mut all = Vec::new();
        let open = self
            .query_sql_fields_open_impl(None, sql, page_size, args)
            .await?;
        let cursor_id = open.cursor_id;
        all.extend(open.rows);
        let mut has_more = open.has_more;
        while has_more {
            let (rows, more) = self
                .query_sql_fields_page_impl(cursor_id, page_size)
                .await?;
            all.extend(rows);
            has_more = more;
        }
        // ensure cursor close
        let _ = self.query_close_impl(cursor_id).await;
        Ok(all)
    }
    pub async fn query_sql_fields_long_fetch_all_with_args_schema(
        &self,
        schema: &str,
        sql: &str,
        page_size: i32,
        args: &[IgniteValue],
    ) -> IgniteResult<Vec<i64>> {
        let mut all = Vec::new();
        let open = self
            .query_sql_fields_open_impl(Some(schema), sql, page_size, args)
            .await?;
        let cursor_id = open.cursor_id;
        all.extend(open.rows);
        let mut has_more = open.has_more;
        while has_more {
            let (rows, more) = self
                .query_sql_fields_page_impl(cursor_id, page_size)
                .await?;
            all.extend(rows);
            has_more = more;
        }
        let _ = self.query_close_impl(cursor_id).await;
        Ok(all)
    }
    pub async fn get(&self, key: &K) -> IgniteResult<Option<V>> {
        self.get_impl(key).await
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
