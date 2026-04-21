use crate::affinity::marshal_key;
use crate::api::key_value::CacheReq;
use crate::api::OpCode;
use crate::cache::{AtomicityMode, CacheMode};
use crate::error::{IgniteError, IgniteResult};
use crate::exec::TokioExec;
use crate::transport::RequestRoute;
use crate::{ReadableReq, ReadableType, WritableType, WriteableReq};
use std::io::{self, Read, Write};
use std::marker::PhantomData;

const DEFAULT_DS_GROUP_NAME: &str = "default-ds-group";
const ATOMICS_CACHE_PREFIX: &str = "ignite-sys-atomic-cache@";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AtomicConfiguration {
    pub reserve_size: i32,
    pub cache_mode: CacheMode,
    pub backups: i32,
    pub group_name: Option<String>,
}

impl Default for AtomicConfiguration {
    fn default() -> Self {
        Self {
            reserve_size: 1,
            cache_mode: CacheMode::Partitioned,
            backups: 1,
            group_name: None,
        }
    }
}

impl AtomicConfiguration {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_group_name(mut self, group_name: &str) -> Self {
        self.group_name = Some(group_name.to_string());
        self
    }

    pub fn with_cache_mode(mut self, cache_mode: CacheMode) -> Self {
        self.cache_mode = cache_mode;
        self
    }

    pub fn with_backups(mut self, backups: i32) -> Self {
        self.backups = backups;
        self
    }

    pub fn with_reserve_size(mut self, reserve_size: i32) -> Self {
        self.reserve_size = reserve_size;
        self
    }
}

#[derive(Clone)]
pub struct AtomicLong {
    exec: TokioExec,
    name: String,
    group_name: Option<String>,
    cache_id: i32,
}

impl AtomicLong {
    pub(crate) async fn get_or_create(
        exec: TokioExec,
        name: &str,
        config: Option<AtomicConfiguration>,
        initial_value: i64,
        create: bool,
    ) -> IgniteResult<Option<Self>> {
        if create {
            exec.send(
                OpCode::AtomicLongCreate,
                AtomicLongCreateRequest {
                    name: name.to_string(),
                    initial_value,
                    config: config.clone(),
                },
            )
            .await?;
        }

        let handle = Self::new(
            exec,
            name,
            config.as_ref().and_then(|cfg| cfg.group_name.clone()),
        );
        if !create && handle.removed().await? {
            return Ok(None);
        }

        Ok(Some(handle))
    }

    fn new(exec: TokioExec, name: &str, group_name: Option<String>) -> Self {
        let group = group_name
            .clone()
            .unwrap_or_else(|| DEFAULT_DS_GROUP_NAME.to_string());
        let cache_id = crate::utils::string_to_java_hashcode(
            format!("{ATOMICS_CACHE_PREFIX}{group}").as_str(),
        );

        Self {
            exec,
            name: name.to_string(),
            group_name,
            cache_id,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub async fn get(&self) -> IgniteResult<i64> {
        let response: LongResponse = self
            .exec
            .send_and_read_with_route(
                OpCode::AtomicLongValueGet,
                AtomicLongIdentityRequest::new(&self.name, self.group_name.clone()),
                self.route().await?,
            )
            .await?;
        Ok(response.value)
    }

    pub async fn increment_and_get(&self) -> IgniteResult<i64> {
        self.add_and_get(1).await
    }

    pub async fn get_and_increment(&self) -> IgniteResult<i64> {
        Ok(self.increment_and_get().await? - 1)
    }

    pub async fn decrement_and_get(&self) -> IgniteResult<i64> {
        self.add_and_get(-1).await
    }

    pub async fn get_and_decrement(&self) -> IgniteResult<i64> {
        Ok(self.decrement_and_get().await? + 1)
    }

    pub async fn add_and_get(&self, delta: i64) -> IgniteResult<i64> {
        let response: LongResponse = self
            .exec
            .send_and_read_with_route(
                OpCode::AtomicLongValueAddAndGet,
                AtomicLongLongRequest::new(&self.name, self.group_name.clone(), delta),
                self.route().await?,
            )
            .await?;
        Ok(response.value)
    }

    pub async fn get_and_add(&self, delta: i64) -> IgniteResult<i64> {
        Ok(self.add_and_get(delta).await? - delta)
    }

    pub async fn get_and_set(&self, value: i64) -> IgniteResult<i64> {
        let response: LongResponse = self
            .exec
            .send_and_read_with_route(
                OpCode::AtomicLongValueGetAndSet,
                AtomicLongLongRequest::new(&self.name, self.group_name.clone(), value),
                self.route().await?,
            )
            .await?;
        Ok(response.value)
    }

    pub async fn compare_and_set(&self, expected: i64, value: i64) -> IgniteResult<bool> {
        let response: BoolResponse = self
            .exec
            .send_and_read_with_route(
                OpCode::AtomicLongValueCompareAndSet,
                AtomicLongCompareAndSetRequest::new(
                    &self.name,
                    self.group_name.clone(),
                    expected,
                    value,
                ),
                self.route().await?,
            )
            .await?;
        Ok(response.value)
    }

    pub async fn compare_and_set_and_get(&self, expected: i64, value: i64) -> IgniteResult<i64> {
        let response: LongResponse = self
            .exec
            .send_and_read_with_route(
                OpCode::AtomicLongValueCompareAndSetAndGet,
                AtomicLongCompareAndSetRequest::new(
                    &self.name,
                    self.group_name.clone(),
                    expected,
                    value,
                ),
                self.route().await?,
            )
            .await?;
        // The server returns the witness (pre-CAS) value. Convert to the
        // resulting value: on success (witness == expected) the new value was
        // written; on failure the atomic is unchanged so witness IS the result.
        let witness = response.value;
        if witness == expected {
            Ok(value)
        } else {
            Ok(witness)
        }
    }

    pub async fn removed(&self) -> IgniteResult<bool> {
        let exists: BoolResponse = self
            .exec
            .send_and_read_with_route(
                OpCode::AtomicLongExists,
                AtomicLongIdentityRequest::new(&self.name, self.group_name.clone()),
                self.route().await?,
            )
            .await?;
        Ok(!exists.value)
    }

    pub async fn close(&self) -> IgniteResult<()> {
        self.exec
            .send_with_route(
                OpCode::AtomicLongRemove,
                AtomicLongIdentityRequest::new(&self.name, self.group_name.clone()),
                self.route().await?,
            )
            .await
    }

    async fn route(&self) -> IgniteResult<RequestRoute> {
        let key = marshal_key(&self.name)?;
        Ok(self
            .exec
            .affinity_node_for_key(self.cache_id, &key, true)
            .await
            .map(RequestRoute::preferred_node)
            .unwrap_or_default())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CollectionConfiguration {
    pub atomicity_mode: AtomicityMode,
    pub cache_mode: CacheMode,
    pub backups: i32,
    pub group_name: Option<String>,
    pub colocated: bool,
}

impl Default for CollectionConfiguration {
    fn default() -> Self {
        Self {
            atomicity_mode: AtomicityMode::Atomic,
            cache_mode: CacheMode::Partitioned,
            backups: 0,
            group_name: None,
            colocated: false,
        }
    }
}

impl CollectionConfiguration {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_atomicity_mode(mut self, atomicity_mode: AtomicityMode) -> Self {
        self.atomicity_mode = atomicity_mode;
        self
    }

    pub fn with_cache_mode(mut self, cache_mode: CacheMode) -> Self {
        self.cache_mode = cache_mode;
        self
    }

    pub fn with_backups(mut self, backups: i32) -> Self {
        self.backups = backups;
        self
    }

    pub fn with_group_name(mut self, group_name: &str) -> Self {
        self.group_name = Some(group_name.to_string());
        self
    }

    pub fn with_colocated(mut self, colocated: bool) -> Self {
        self.colocated = colocated;
        self
    }
}

#[derive(Clone)]
pub struct IgniteSet<T> {
    exec: TokioExec,
    name: String,
    colocated: bool,
    cache_id: i32,
    page_size: i32,
    server_keep_binary: bool,
    _value: PhantomData<T>,
}

impl<T: WritableType + ReadableType> IgniteSet<T> {
    pub(crate) async fn get_or_create(
        exec: TokioExec,
        name: &str,
        config: Option<CollectionConfiguration>,
    ) -> IgniteResult<Option<Self>> {
        let resp: SetGetOrCreateResponse = exec
            .send_and_read(
                OpCode::SetGetOrCreate,
                SetGetOrCreateRequest {
                    name: name.to_string(),
                    config,
                },
            )
            .await?;

        if !resp.exists {
            return Ok(None);
        }

        Ok(Some(Self {
            exec,
            name: name.to_string(),
            colocated: resp.colocated,
            cache_id: resp.cache_id,
            page_size: 1024,
            server_keep_binary: true,
            _value: PhantomData,
        }))
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn colocated(&self) -> bool {
        self.colocated
    }

    pub fn page_size(&self) -> i32 {
        self.page_size
    }

    pub fn with_page_size(mut self, page_size: i32) -> IgniteResult<Self> {
        if page_size <= 0 {
            return Err(IgniteError::from("Page size must be greater than 0."));
        }
        self.page_size = page_size;
        Ok(self)
    }

    pub fn server_keep_binary(&self) -> bool {
        self.server_keep_binary
    }

    pub fn with_server_keep_binary(mut self, keep_binary: bool) -> Self {
        self.server_keep_binary = keep_binary;
        self
    }

    pub async fn add(&self, value: &T) -> IgniteResult<bool> {
        let response: BoolResponse = self
            .exec
            .send_and_read_with_route(
                OpCode::SetValueAdd,
                SetSingleKeyRequest::new(self, value),
                self.route_for_key(value).await?,
            )
            .await?;
        Ok(response.value)
    }

    pub async fn add_all(&self, values: &[T]) -> IgniteResult<bool> {
        if values.is_empty() {
            return Ok(false);
        }

        let response: BoolResponse = self
            .exec
            .send_and_read_with_route(
                OpCode::SetValueAddAll,
                SetMultiKeyRequest::new(self, values),
                self.route_for_first(values.first()).await?,
            )
            .await?;
        Ok(response.value)
    }

    pub async fn contains(&self, value: &T) -> IgniteResult<bool> {
        let response: BoolResponse = self
            .exec
            .send_and_read_with_route(
                OpCode::SetValueContains,
                SetSingleKeyRequest::new(self, value),
                self.route_for_key(value).await?,
            )
            .await?;
        Ok(response.value)
    }

    pub async fn contains_all(&self, values: &[T]) -> IgniteResult<bool> {
        if values.is_empty() {
            return Ok(false);
        }

        let response: BoolResponse = self
            .exec
            .send_and_read_with_route(
                OpCode::SetValueContainsAll,
                SetMultiKeyRequest::new(self, values),
                self.route_for_first(values.first()).await?,
            )
            .await?;
        Ok(response.value)
    }

    pub async fn remove(&self, value: &T) -> IgniteResult<bool> {
        let response: BoolResponse = self
            .exec
            .send_and_read_with_route(
                OpCode::SetValueRemove,
                SetSingleKeyRequest::new(self, value),
                self.route_for_key(value).await?,
            )
            .await?;
        Ok(response.value)
    }

    pub async fn remove_all(&self, values: &[T]) -> IgniteResult<bool> {
        if values.is_empty() {
            return Ok(false);
        }

        let response: BoolResponse = self
            .exec
            .send_and_read_with_route(
                OpCode::SetValueRemoveAll,
                SetMultiKeyRequest::new(self, values),
                self.route_for_first(values.first()).await?,
            )
            .await?;
        Ok(response.value)
    }

    pub async fn retain_all(&self, values: &[T]) -> IgniteResult<bool> {
        let response: BoolResponse = self
            .exec
            .send_and_read_with_route(
                OpCode::SetValueRetainAll,
                SetMultiKeyRequest::new(self, values),
                self.route_for_first(values.first()).await?,
            )
            .await?;
        Ok(response.value)
    }

    pub async fn clear(&self) -> IgniteResult<()> {
        self.exec
            .send(OpCode::SetClear, SetIdentityRequest::new(self))
            .await
    }

    pub async fn size(&self) -> IgniteResult<i32> {
        let response: IntResponse = self
            .exec
            .send_and_read(OpCode::SetSize, SetIdentityRequest::new(self))
            .await?;
        Ok(response.value)
    }

    pub async fn is_empty(&self) -> IgniteResult<bool> {
        Ok(self.size().await? == 0)
    }

    pub async fn removed(&self) -> IgniteResult<bool> {
        let exists: BoolResponse = self
            .exec
            .send_and_read(OpCode::SetExists, SetIdentityRequest::new(self))
            .await?;
        Ok(!exists.value)
    }

    pub async fn close(&self) -> IgniteResult<()> {
        self.exec
            .send(OpCode::SetClose, SetIdentityRequest::new(self))
            .await
    }

    pub async fn iter(&self) -> IgniteResult<SetCursor<T>> {
        let response: SetIteratorOpenResponse<T> = self
            .exec
            .send_and_read_with_route(
                OpCode::SetIteratorStart,
                SetIteratorStartRequest::new(self),
                self.iterator_route().await?,
            )
            .await?;
        Ok(SetCursor {
            exec: self.exec.clone(),
            resource_id: response.resource_id,
            page_size: self.page_size,
            pending_items: response.items,
            has_more: response.has_more,
            closed: false,
        })
    }

    async fn route_for_key(&self, value: &T) -> IgniteResult<RequestRoute> {
        let affinity_value = if self.colocated {
            marshal_key(&crate::utils::string_to_java_hashcode(self.name.as_str()))?
        } else {
            marshal_key(value)?
        };

        Ok(self
            .exec
            .affinity_node_for_key(self.cache_id, &affinity_value, true)
            .await
            .map(RequestRoute::preferred_node)
            .unwrap_or_default())
    }

    async fn route_for_first(&self, first: Option<&T>) -> IgniteResult<RequestRoute> {
        match first {
            Some(first) => self.route_for_key(first).await,
            None => Ok(RequestRoute::default()),
        }
    }

    async fn iterator_route(&self) -> IgniteResult<RequestRoute> {
        if !self.colocated {
            return Ok(RequestRoute::default());
        }

        let affinity_value =
            marshal_key(&crate::utils::string_to_java_hashcode(self.name.as_str()))?;
        Ok(self
            .exec
            .affinity_node_for_key(self.cache_id, &affinity_value, true)
            .await
            .map(RequestRoute::preferred_node)
            .unwrap_or_default())
    }
}

pub struct SetCursor<T> {
    exec: TokioExec,
    resource_id: Option<i64>,
    page_size: i32,
    pending_items: Vec<T>,
    has_more: bool,
    closed: bool,
}

impl<T: ReadableType> SetCursor<T> {
    pub async fn next_page(&mut self) -> IgniteResult<Vec<T>> {
        if !self.pending_items.is_empty() {
            let page = std::mem::take(&mut self.pending_items);
            if !self.has_more {
                let _ = self.close().await;
            }
            return Ok(page);
        }

        if !self.has_more {
            let _ = self.close().await;
            return Ok(Vec::new());
        }

        let resource_id = self
            .resource_id
            .ok_or_else(|| IgniteError::from("set cursor resource id missing"))?;
        let response: SetIteratorPageResponse<T> = self
            .exec
            .send_and_read(
                OpCode::SetIteratorGetPage,
                SetIteratorGetPageRequest {
                    resource_id,
                    page_size: self.page_size,
                    _value: PhantomData::<T>,
                },
            )
            .await?;
        self.has_more = response.has_more;
        if !self.has_more {
            self.resource_id = None;
        }
        if !self.has_more {
            let _ = self.close().await;
        }
        Ok(response.items)
    }

    pub async fn fetch_all(mut self) -> IgniteResult<Vec<T>> {
        let mut items = Vec::new();
        loop {
            let page = self.next_page().await?;
            if page.is_empty() {
                break;
            }
            items.extend(page);
        }
        Ok(items)
    }

    pub async fn close(&mut self) -> IgniteResult<()> {
        if self.closed {
            return Ok(());
        }
        if let Some(resource_id) = self.resource_id.take() {
            self.exec
                .send(
                    OpCode::ResourceClose,
                    CacheReq::CursorClose::<i32, i32>(resource_id),
                )
                .await?;
        }
        self.closed = true;
        Ok(())
    }
}

impl<T> Drop for SetCursor<T> {
    fn drop(&mut self) {
        if self.closed {
            return;
        }
        if let Some(resource_id) = self.resource_id.take() {
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                let exec = self.exec.clone();
                handle.spawn(async move {
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
}

struct AtomicLongCreateRequest {
    name: String,
    initial_value: i64,
    config: Option<AtomicConfiguration>,
}

impl WriteableReq for AtomicLongCreateRequest {
    fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        self.name.write(writer)?;
        crate::protocol::write_i64(writer, self.initial_value)?;
        crate::protocol::write_bool(writer, self.config.is_some())?;
        if let Some(config) = &self.config {
            crate::protocol::write_i32(writer, config.reserve_size)?;
            crate::protocol::write_u8(writer, config.cache_mode.clone() as u8)?;
            crate::protocol::write_i32(writer, config.backups)?;
            config.group_name.write(writer)?;
        }
        Ok(())
    }

    fn size(&self) -> usize {
        self.name.size()
            + 8
            + 1
            + self
                .config
                .as_ref()
                .map(|config| 4 + 1 + 4 + config.group_name.size())
                .unwrap_or(0)
    }
}

struct AtomicLongIdentityRequest {
    name: String,
    group_name: Option<String>,
}

impl AtomicLongIdentityRequest {
    fn new(name: &str, group_name: Option<String>) -> Self {
        Self {
            name: name.to_string(),
            group_name,
        }
    }
}

impl WriteableReq for AtomicLongIdentityRequest {
    fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        self.name.write(writer)?;
        self.group_name.write(writer)
    }

    fn size(&self) -> usize {
        self.name.size() + self.group_name.size()
    }
}

struct AtomicLongLongRequest {
    identity: AtomicLongIdentityRequest,
    value: i64,
}

impl AtomicLongLongRequest {
    fn new(name: &str, group_name: Option<String>, value: i64) -> Self {
        Self {
            identity: AtomicLongIdentityRequest::new(name, group_name),
            value,
        }
    }
}

impl WriteableReq for AtomicLongLongRequest {
    fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        self.identity.write(writer)?;
        crate::protocol::write_i64(writer, self.value)
    }

    fn size(&self) -> usize {
        self.identity.size() + 8
    }
}

struct AtomicLongCompareAndSetRequest {
    identity: AtomicLongIdentityRequest,
    expected: i64,
    value: i64,
}

impl AtomicLongCompareAndSetRequest {
    fn new(name: &str, group_name: Option<String>, expected: i64, value: i64) -> Self {
        Self {
            identity: AtomicLongIdentityRequest::new(name, group_name),
            expected,
            value,
        }
    }
}

impl WriteableReq for AtomicLongCompareAndSetRequest {
    fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        self.identity.write(writer)?;
        crate::protocol::write_i64(writer, self.expected)?;
        crate::protocol::write_i64(writer, self.value)
    }

    fn size(&self) -> usize {
        self.identity.size() + 16
    }
}

struct BoolResponse {
    value: bool,
}

impl ReadableReq for BoolResponse {
    fn read(reader: &mut impl Read) -> IgniteResult<Self> {
        Ok(Self {
            value: crate::protocol::read_bool(reader).map_err(IgniteError::from)?,
        })
    }
}

struct LongResponse {
    value: i64,
}

impl ReadableReq for LongResponse {
    fn read(reader: &mut impl Read) -> IgniteResult<Self> {
        Ok(Self {
            value: crate::protocol::read_i64(reader).map_err(IgniteError::from)?,
        })
    }
}

struct IntResponse {
    value: i32,
}

impl ReadableReq for IntResponse {
    fn read(reader: &mut impl Read) -> IgniteResult<Self> {
        Ok(Self {
            value: crate::protocol::read_i32(reader).map_err(IgniteError::from)?,
        })
    }
}

struct SetGetOrCreateRequest {
    name: String,
    config: Option<CollectionConfiguration>,
}

impl WriteableReq for SetGetOrCreateRequest {
    fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        self.name.write(writer)?;
        crate::protocol::write_bool(writer, self.config.is_some())?;
        if let Some(config) = &self.config {
            crate::protocol::write_u8(writer, config.atomicity_mode.clone() as u8)?;
            crate::protocol::write_u8(writer, config.cache_mode.clone() as u8)?;
            crate::protocol::write_i32(writer, config.backups)?;
            config.group_name.write(writer)?;
            crate::protocol::write_bool(writer, config.colocated)?;
        }
        Ok(())
    }

    fn size(&self) -> usize {
        self.name.size()
            + 1
            + self
                .config
                .as_ref()
                .map(|config| 1 + 1 + 4 + config.group_name.size() + 1)
                .unwrap_or(0)
    }
}

struct SetGetOrCreateResponse {
    exists: bool,
    colocated: bool,
    cache_id: i32,
}

impl ReadableReq for SetGetOrCreateResponse {
    fn read(reader: &mut impl Read) -> IgniteResult<Self> {
        let exists = crate::protocol::read_bool(reader).map_err(IgniteError::from)?;
        if !exists {
            return Ok(Self {
                exists,
                colocated: false,
                cache_id: 0,
            });
        }
        Ok(Self {
            exists,
            colocated: crate::protocol::read_bool(reader).map_err(IgniteError::from)?,
            cache_id: crate::protocol::read_i32(reader).map_err(IgniteError::from)?,
        })
    }
}

struct SetIdentityRequest {
    name: String,
    cache_id: i32,
    colocated: bool,
}

impl SetIdentityRequest {
    fn new<T>(set: &IgniteSet<T>) -> Self {
        Self {
            name: set.name.clone(),
            cache_id: set.cache_id,
            colocated: set.colocated,
        }
    }
}

impl WriteableReq for SetIdentityRequest {
    fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        self.name.write(writer)?;
        crate::protocol::write_i32(writer, self.cache_id)?;
        crate::protocol::write_bool(writer, self.colocated)
    }

    fn size(&self) -> usize {
        self.name.size() + 4 + 1
    }
}

struct SetSingleKeyRequest<'a, T> {
    identity: SetIdentityRequest,
    server_keep_binary: bool,
    value: &'a T,
}

impl<'a, T: WritableType + ReadableType> SetSingleKeyRequest<'a, T> {
    fn new(set: &'a IgniteSet<T>, value: &'a T) -> Self {
        Self {
            identity: SetIdentityRequest::new(set),
            server_keep_binary: set.server_keep_binary,
            value,
        }
    }
}

impl<T: WritableType + ReadableType> WriteableReq for SetSingleKeyRequest<'_, T> {
    fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        self.identity.write(writer)?;
        crate::protocol::write_bool(writer, self.server_keep_binary)?;
        self.value.write(writer)
    }

    fn size(&self) -> usize {
        self.identity.size() + 1 + self.value.size()
    }
}

struct SetMultiKeyRequest<'a, T> {
    identity: SetIdentityRequest,
    server_keep_binary: bool,
    values: &'a [T],
}

impl<'a, T: WritableType + ReadableType> SetMultiKeyRequest<'a, T> {
    fn new(set: &'a IgniteSet<T>, values: &'a [T]) -> Self {
        Self {
            identity: SetIdentityRequest::new(set),
            server_keep_binary: set.server_keep_binary,
            values,
        }
    }
}

impl<T: WritableType + ReadableType> WriteableReq for SetMultiKeyRequest<'_, T> {
    fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        self.identity.write(writer)?;
        crate::protocol::write_bool(writer, self.server_keep_binary)?;
        crate::protocol::write_i32(writer, self.values.len() as i32)?;
        for value in self.values {
            value.write(writer)?;
        }
        Ok(())
    }

    fn size(&self) -> usize {
        self.identity.size() + 1 + 4 + self.values.iter().map(WritableType::size).sum::<usize>()
    }
}

struct SetIteratorStartRequest {
    identity: SetIdentityRequest,
    page_size: i32,
}

impl SetIteratorStartRequest {
    fn new<T>(set: &IgniteSet<T>) -> Self {
        Self {
            identity: SetIdentityRequest::new(set),
            page_size: set.page_size,
        }
    }
}

impl WriteableReq for SetIteratorStartRequest {
    fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        self.identity.write(writer)?;
        crate::protocol::write_i32(writer, self.page_size)
    }

    fn size(&self) -> usize {
        self.identity.size() + 4
    }
}

struct SetIteratorGetPageRequest<T> {
    resource_id: i64,
    page_size: i32,
    _value: PhantomData<T>,
}

impl<T> WriteableReq for SetIteratorGetPageRequest<T> {
    fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        crate::protocol::write_i64(writer, self.resource_id)?;
        crate::protocol::write_i32(writer, self.page_size)
    }

    fn size(&self) -> usize {
        8 + 4
    }
}

struct SetIteratorOpenResponse<T> {
    items: Vec<T>,
    has_more: bool,
    resource_id: Option<i64>,
}

impl<T: ReadableType> ReadableReq for SetIteratorOpenResponse<T> {
    fn read(reader: &mut impl Read) -> IgniteResult<Self> {
        let items = read_set_items(reader)?;
        let has_more = crate::protocol::read_bool(reader).map_err(IgniteError::from)?;
        let resource_id = if has_more {
            Some(crate::protocol::read_i64(reader).map_err(IgniteError::from)?)
        } else {
            None
        };

        Ok(Self {
            items,
            has_more,
            resource_id,
        })
    }
}

struct SetIteratorPageResponse<T> {
    items: Vec<T>,
    has_more: bool,
}

impl<T: ReadableType> ReadableReq for SetIteratorPageResponse<T> {
    fn read(reader: &mut impl Read) -> IgniteResult<Self> {
        Ok(Self {
            items: read_set_items(reader)?,
            has_more: crate::protocol::read_bool(reader).map_err(IgniteError::from)?,
        })
    }
}

fn read_set_items<T: ReadableType>(reader: &mut impl Read) -> IgniteResult<Vec<T>> {
    let count = crate::protocol::read_i32(reader).map_err(IgniteError::from)?;
    let mut items = Vec::with_capacity(count.max(0) as usize);
    for _ in 0..count {
        items.push(
            T::read(reader)?
                .ok_or_else(|| IgniteError::from("Ignite set iterator returned null item"))?,
        );
    }
    Ok(items)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode(req: &dyn WriteableReq) -> Vec<u8> {
        let mut buf = Vec::new();
        req.write(&mut buf).unwrap();
        buf
    }

    /// FND-050 — `ATOMIC_LONG_CREATE` wire layout matches
    /// `TcpIgniteClient#atomicLong@2.17.0:428-441`:
    ///   writeString(name)                             // typed String
    ///   writeLong(initialValue)                        // i64
    ///   writeBoolean(cfg != null)                      // bool
    ///   if cfg != null {
    ///       writeInt(reserveSize)                      // i32
    ///       writeByte(CacheMode.toCode(cacheMode))     // i8
    ///       writeInt(backups)                          // i32
    ///       writeString(groupName)                     // typed String | NULL
    ///   }
    /// The audit's alternative "writeName then initialValue;create" applies
    /// to the other AtomicLong ops (§8.1 `writeName` helper), not CREATE —
    /// CREATE has the config payload inlined per Java.
    #[test]
    fn atomic_long_create_request_matches_java_without_config() {
        let req = AtomicLongCreateRequest {
            name: "counter".to_string(),
            initial_value: 42,
            config: None,
        };
        let mut expected = Vec::new();
        // name: typed String (TypeCode::String=9, i32 len=7, bytes)
        expected.push(9);
        expected.extend_from_slice(&7i32.to_le_bytes());
        expected.extend_from_slice(b"counter");
        // initialValue
        expected.extend_from_slice(&42i64.to_le_bytes());
        // cfg == null marker
        expected.push(0);
        assert_eq!(encode(&req), expected);
        assert_eq!(req.size(), expected.len());
    }

    /// FND-050 — CREATE with a non-null `AtomicConfiguration` writes the
    /// config fields (reserveSize, cacheMode, backups, groupName) inline,
    /// matching the Java client exactly. `groupName` is a typed String that
    /// may be NULL (TypeCode 101) when `ClientAtomicConfiguration.groupName`
    /// is null.
    #[test]
    fn atomic_long_create_request_matches_java_with_config_and_group() {
        let req = AtomicLongCreateRequest {
            name: "c".to_string(),
            initial_value: 7,
            config: Some(AtomicConfiguration {
                reserve_size: 64,
                cache_mode: CacheMode::Partitioned,
                backups: 2,
                group_name: Some("grp".to_string()),
            }),
        };
        let mut expected = Vec::new();
        // name
        expected.push(9);
        expected.extend_from_slice(&1i32.to_le_bytes());
        expected.push(b'c');
        // initialValue
        expected.extend_from_slice(&7i64.to_le_bytes());
        // cfg != null marker
        expected.push(1);
        // reserveSize / cacheMode / backups
        expected.extend_from_slice(&64i32.to_le_bytes());
        expected.push(2); // CacheMode::Partitioned == 2 (matches Java CacheMode.toCode)
        expected.extend_from_slice(&2i32.to_le_bytes());
        // groupName as typed String
        expected.push(9);
        expected.extend_from_slice(&3i32.to_le_bytes());
        expected.extend_from_slice(b"grp");
        assert_eq!(encode(&req), expected);
        assert_eq!(req.size(), expected.len());
    }

    /// FND-050 — CREATE with a config that has a null `group_name` must
    /// write `groupName` as typed-string NULL (TypeCode 101), matching
    /// `BinaryWriterEx.writeString(null)` in Java.
    #[test]
    fn atomic_long_create_request_encodes_null_group_name_as_typed_null() {
        let req = AtomicLongCreateRequest {
            name: "n".to_string(),
            initial_value: 0,
            config: Some(AtomicConfiguration {
                reserve_size: 1,
                cache_mode: CacheMode::Replicated,
                backups: 0,
                group_name: None,
            }),
        };
        let bytes = encode(&req);
        // Last byte is the typed-string NULL marker for groupName.
        assert_eq!(*bytes.last().unwrap(), 101);
        assert_eq!(req.size(), bytes.len());
    }

    /// FND-050 — The non-CREATE AtomicLong ops use Java's `writeName`
    /// helper (`ClientAtomicLongImpl#writeName@2.17.0:136-141`):
    ///   writeString(name); writeString(groupName)
    /// `AtomicLongIdentityRequest` pins that layout.
    #[test]
    fn atomic_long_identity_request_matches_java_write_name() {
        let req = AtomicLongIdentityRequest::new("counter", Some("grp".to_string()));
        let mut expected = Vec::new();
        // name
        expected.push(9);
        expected.extend_from_slice(&7i32.to_le_bytes());
        expected.extend_from_slice(b"counter");
        // groupName as typed String
        expected.push(9);
        expected.extend_from_slice(&3i32.to_le_bytes());
        expected.extend_from_slice(b"grp");
        assert_eq!(encode(&req), expected);
        assert_eq!(req.size(), expected.len());
    }

    /// FND-050 — `writeName` with a null groupName must emit typed-string
    /// NULL (TypeCode 101), matching `w.writeString(groupName)` in Java
    /// when `groupName == null`.
    #[test]
    fn atomic_long_identity_request_encodes_null_group_name_as_typed_null() {
        let req = AtomicLongIdentityRequest::new("counter", None);
        let bytes = encode(&req);
        let mut expected = Vec::new();
        expected.push(9);
        expected.extend_from_slice(&7i32.to_le_bytes());
        expected.extend_from_slice(b"counter");
        expected.push(101); // typed-string NULL
        assert_eq!(bytes, expected);
        assert_eq!(req.size(), expected.len());
    }
}
