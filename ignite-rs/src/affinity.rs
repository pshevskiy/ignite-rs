use crate::connection_async::read_uuid_string;
use crate::error::{IgniteError, IgniteResult};
use crate::protocol::{
    read_bool, read_i16, read_i32, read_i64, read_string, write_bool, write_i32, write_string,
    TypeCode,
};
use crate::topology::TopologyVersion;
use crate::utils::string_to_java_hashcode;
use crate::{ReadableReq, WriteableReq};
use arc_swap::ArcSwap;
use std::collections::{HashMap, HashSet};
use std::convert::{TryFrom, TryInto};
use std::io::{self, Cursor, Read, Write};
use std::sync::Arc;

const MAX_AFFINITY_NODE_COUNT: i32 = 4096;
const MAX_AFFINITY_PARTITIONS_PER_NODE: i32 = 65_536;
const MAX_AFFINITY_PARTITION_ID: i32 = 65_535;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CachePartitionsRequest {
    /// ALL_AFFINITY_MAPPINGS bit (13) negotiated on the channel this request is
    /// sent on. When true, the Java wire shape prepends a `bool
    /// customMappingsRequired` before the cache-id array (Java §9.1,
    /// `ClientCacheAffinityMapping.writeRequest@2.17.0`).
    pub(crate) all_affinity_mappings: bool,
    /// Matches Java's `customMappingsRequired` flag — set when the client wants
    /// the server to include custom (non-Rendezvous) affinity mappings in the
    /// response. Ignored when `all_affinity_mappings == false`.
    pub(crate) custom_mappings_required: bool,
    /// Gridgain-downstream DC_AWARE extension (bit 22, not present in Java
    /// 2.17.0). Only true when both the client user-attribute
    /// `IGNITE_DATA_CENTER_ID` is set AND the server advertises bit 22 (FND-005
    /// gate in `connection_async.rs`). When the DC_AWARE bit is negotiated the
    /// server expects a typed-string (or `-1 i32` nullable) after the
    /// `customMappingsRequired` bool.
    pub(crate) include_dc_id: bool,
    pub(crate) dc_id: Option<String>,
    pub(crate) cache_ids: Vec<i32>,
}

impl WriteableReq for CachePartitionsRequest {
    fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        if self.all_affinity_mappings {
            write_bool(writer, self.custom_mappings_required)?;
        }
        if self.include_dc_id {
            match self.dc_id.as_deref() {
                Some(dc_id) => write_string(writer, dc_id)?,
                None => write_i32(writer, -1)?,
            }
        }
        write_i32(writer, self.cache_ids.len() as i32)?;
        for cache_id in &self.cache_ids {
            write_i32(writer, *cache_id)?;
        }
        Ok(())
    }

    fn size(&self) -> usize {
        (if self.all_affinity_mappings { 1 } else { 0 })
            + (if self.include_dc_id {
                4 + self.dc_id.as_ref().map(|dc_id| dc_id.len()).unwrap_or(0)
            } else {
                0
            })
            + 4
            + (self.cache_ids.len() * 4)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CacheAffinityMap {
    primary_partition_to_node: Arc<[Option<Arc<str>>]>,
    dc_partition_to_node: Arc<[Option<Arc<str>>]>,
    key_field_mapping_present: bool,
}

#[derive(Clone)]
struct AffinityState {
    topology_version: Option<TopologyVersion>,
    caches: HashMap<i32, CacheAffinityMap>,
    single_node: bool,
}

impl Default for AffinityState {
    fn default() -> Self {
        Self {
            topology_version: None,
            caches: HashMap::new(),
            single_node: false,
        }
    }
}

pub(crate) struct AffinityCache {
    /// Lock-free affinity state — atomic pointer swap on write, load on read.
    /// Equivalent to Java's volatile + ConcurrentHashMap for read performance.
    state: ArcSwap<AffinityState>,
    /// Lock-free cache of single_node flag — avoids even the ArcSwap load per request.
    single_node_cached: std::sync::atomic::AtomicBool,
}

impl Default for AffinityCache {
    fn default() -> Self {
        Self {
            state: ArcSwap::from_pointee(AffinityState::default()),
            single_node_cached: std::sync::atomic::AtomicBool::new(false),
        }
    }
}

impl AffinityCache {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) async fn invalidate(&self) {
        let mut new_state: AffinityState = (**self.state.load()).clone();
        new_state.topology_version = None;
        new_state.caches.clear();
        new_state.single_node = false;
        self.single_node_cached
            .store(false, std::sync::atomic::Ordering::Relaxed);
        self.state.store(Arc::new(new_state));
    }

    pub(crate) async fn invalidate_cache(&self, cache_id: i32) {
        let mut new_state: AffinityState = (**self.state.load()).clone();
        new_state.caches.remove(&cache_id);
        let all_nodes: HashSet<&str> = new_state
            .caches
            .values()
            .flat_map(|c| {
                c.primary_partition_to_node
                    .iter()
                    .chain(c.dc_partition_to_node.iter())
            })
            .filter_map(|n| n.as_deref())
            .collect();
        new_state.single_node = all_nodes.len() <= 1 && !new_state.caches.is_empty();
        self.single_node_cached
            .store(new_state.single_node, std::sync::atomic::Ordering::Relaxed);
        self.state.store(Arc::new(new_state));
    }

    pub(crate) async fn needs_refresh(&self, cache_id: i32) -> bool {
        let state = self.state.load();
        state.topology_version.is_none() || !state.caches.contains_key(&cache_id)
    }

    pub(crate) fn is_single_node(&self) -> bool {
        self.single_node_cached
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub(crate) async fn apply(
        &self,
        topology_version: TopologyVersion,
        caches: HashMap<i32, CacheAffinityMap>,
    ) {
        let mut new_state: AffinityState = (**self.state.load()).clone();
        new_state.topology_version = Some(topology_version);
        new_state.caches.extend(caches);
        let all_nodes: HashSet<&str> = new_state
            .caches
            .values()
            .flat_map(|c| {
                c.primary_partition_to_node
                    .iter()
                    .chain(c.dc_partition_to_node.iter())
            })
            .filter_map(|n| n.as_deref())
            .collect();
        new_state.single_node = all_nodes.len() <= 1;
        self.single_node_cached
            .store(new_state.single_node, std::sync::atomic::Ordering::Relaxed);
        self.state.store(Arc::new(new_state));
    }

    pub(crate) async fn node_for_partition(
        &self,
        cache_id: i32,
        partition: i32,
        primary: bool,
    ) -> Option<Arc<str>> {
        if partition < 0 {
            return None;
        }

        let state = self.state.load();
        state
            .caches
            .get(&cache_id)
            .and_then(|cache| {
                if primary {
                    cache.primary_partition_to_node.get(partition as usize)
                } else {
                    cache.dc_partition_to_node.get(partition as usize)
                }
            })
            .and_then(|opt| opt.clone())
    }

    /// Combined lookup: check freshness + compute partition + resolve node — lock-free via ArcSwap.
    pub(crate) fn resolve_node_for_key(
        &self,
        cache_id: i32,
        marshaled_key: &[u8],
        primary: bool,
    ) -> Option<Arc<str>> {
        let state = self.state.load();
        // Check freshness
        if state.topology_version.is_none() || !state.caches.contains_key(&cache_id) {
            return None; // Caller will ensure_affinity_mapping and retry
        }
        let cache = state.caches.get(&cache_id)?;
        if cache.key_field_mapping_present {
            return None;
        }
        let partition_count = cache.primary_partition_to_node.len() as i32;
        if partition_count <= 0 {
            return None;
        }
        // Compute partition from key hash
        let key_hash = affinity_hash_marshaled(marshaled_key)?;
        let partition = rendezvous_partition(key_hash, partition_count);
        // Resolve node — all within the same atomic snapshot
        let mapping = if primary {
            &cache.primary_partition_to_node
        } else {
            &cache.dc_partition_to_node
        };
        mapping.get(partition as usize).and_then(|opt| opt.clone())
    }

    pub(crate) async fn node_for_marshaled_key(
        &self,
        cache_id: i32,
        marshaled_key: &[u8],
        primary: bool,
    ) -> Option<Arc<str>> {
        let state = self.state.load();
        let cache = state.caches.get(&cache_id)?;
        let partition_count = cache.primary_partition_to_node.len() as i32;
        if cache.key_field_mapping_present || partition_count <= 0 {
            return None;
        }

        let key_hash = affinity_hash_marshaled(marshaled_key)?;
        let partition = rendezvous_partition(key_hash, partition_count);
        let mapping = if primary {
            &cache.primary_partition_to_node
        } else {
            &cache.dc_partition_to_node
        };
        mapping.get(partition as usize).and_then(|opt| opt.clone())
    }
}

pub(crate) struct CachePartitionsResponse {
    pub(crate) topology_version: TopologyVersion,
    pub(crate) caches: HashMap<i32, CacheAffinityMap>,
}

impl CachePartitionsResponse {
    pub(crate) fn read_with_dc_aware(reader: &mut impl Read, dc_aware: bool) -> IgniteResult<Self> {
        Self::read_with_flags(reader, dc_aware, false)
    }

    /// Decode a `CACHE_PARTITIONS` response, mirroring Java 2.17.0's
    /// `ClientCacheAffinityMapping.readResponse`:
    ///
    /// ```text
    /// i64 topVerMajor, i32 topVerMinor
    /// i32 mappingCount
    /// mappingCount × (
    ///   bool applicable, i32 cachesInGroup,
    ///   [if applicable]:
    ///     cachesInGroup × (i32 cacheId, i32 keyCfgCount, keyCfgCount × (i32, i32))
    ///     nodeCount + partition map                                // primary
    ///     [if DC_AWARE (gridgain bit 22)]: nodeCount + partition map // dc
    ///     [if ALL_AFFINITY_MAPPINGS (bit 13)]: bool defaultAffinity
    ///   [else]: cachesInGroup × i32 cacheId
    /// )
    /// ```
    ///
    /// The `bool defaultAffinity` after the partition maps tells the client
    /// whether to use Rendezvous or a user-supplied
    /// `ClientPartitionAwarenessMapperFactory`. Rust only supports Rendezvous,
    /// so the value is read and dropped.
    pub(crate) fn read_with_flags(
        reader: &mut impl Read,
        dc_aware: bool,
        all_affinity_mappings: bool,
    ) -> IgniteResult<Self> {
        let topology_version = TopologyVersion::new(read_i64(reader)?, read_i32(reader)?);
        let mappings_count = read_i32(reader)?;
        if mappings_count < 0 {
            return Err(IgniteError::from("negative affinity mappings count"));
        }

        let mut caches = HashMap::new();

        for _ in 0..mappings_count {
            let applicable = read_bool(reader)?;
            let cache_count = read_i32(reader)?;
            if cache_count < 0 {
                return Err(IgniteError::from("negative affinity cache count"));
            }

            let mut cache_ids = Vec::with_capacity(cache_count as usize);
            let mut key_field_mapping_present = false;

            if applicable {
                for _ in 0..cache_count {
                    let cache_id = read_i32(reader)?;
                    let key_cfg_count = read_i32(reader)?;
                    if key_cfg_count < 0 {
                        return Err(IgniteError::from("negative affinity key config count"));
                    }

                    if key_cfg_count > 0 {
                        key_field_mapping_present = true;
                    }

                    for _ in 0..key_cfg_count {
                        let _key_type_id = read_i32(reader)?;
                        let _affinity_field_id = read_i32(reader)?;
                    }

                    cache_ids.push(cache_id);
                }

                let primary_partition_to_node = read_partition_map(reader)?;
                let primary_partition_to_node: Arc<[Option<Arc<str>>]> =
                    primary_partition_to_node.into();
                let dc_partition_to_node = if dc_aware {
                    let dc_map = read_partition_map(reader)?;
                    if dc_map.is_empty() {
                        primary_partition_to_node.clone()
                    } else {
                        dc_map.into()
                    }
                } else {
                    primary_partition_to_node.clone()
                };

                if all_affinity_mappings {
                    // `defaultAffinity` — Rust only supports Rendezvous, so the
                    // flag is read and discarded (Java §9.1, FND-054).
                    let _default_affinity = read_bool(reader)?;
                }

                for cache_id in cache_ids {
                    caches.insert(
                        cache_id,
                        CacheAffinityMap {
                            primary_partition_to_node: primary_partition_to_node.clone(),
                            dc_partition_to_node: dc_partition_to_node.clone(),
                            key_field_mapping_present,
                        },
                    );
                }
            } else {
                for _ in 0..cache_count {
                    let cache_id = read_i32(reader)?;
                    cache_ids.push(cache_id);
                }

                for cache_id in cache_ids {
                    caches.insert(
                        cache_id,
                        CacheAffinityMap {
                            primary_partition_to_node: Arc::from([]),
                            dc_partition_to_node: Arc::from([]),
                            key_field_mapping_present: true,
                        },
                    );
                }
            }
        }

        Ok(Self {
            topology_version,
            caches,
        })
    }
}

impl ReadableReq for CachePartitionsResponse {
    fn read(reader: &mut impl Read) -> IgniteResult<Self> {
        Self::read_with_dc_aware(reader, false)
    }
}

pub(crate) fn marshal_key(key: &impl crate::WritableType) -> IgniteResult<Vec<u8>> {
    let mut bytes = Vec::with_capacity(key.size());
    key.write(&mut bytes).map_err(IgniteError::from)?;
    Ok(bytes)
}

fn read_partition_map(reader: &mut impl Read) -> IgniteResult<Vec<Option<Arc<str>>>> {
    let node_count = read_i32(reader)?;
    if node_count < 0 {
        return Err(IgniteError::from("negative affinity node count"));
    }
    if node_count > MAX_AFFINITY_NODE_COUNT {
        return Err(IgniteError::from(
            "affinity node count exceeds sanity limit",
        ));
    }

    let mut partition_to_node = Vec::new();

    for _ in 0..node_count {
        let node_id: Arc<str> = Arc::from(read_uuid_string(reader)?.as_str());
        let part_count = read_i32(reader)?;
        if part_count < 0 {
            return Err(IgniteError::from("negative affinity partition count"));
        }
        if part_count > MAX_AFFINITY_PARTITIONS_PER_NODE {
            return Err(IgniteError::from(
                "affinity partition count exceeds sanity limit",
            ));
        }

        for _ in 0..part_count {
            let partition = read_i32(reader)?;
            if partition < 0 {
                return Err(IgniteError::from("negative affinity partition id"));
            }
            if partition > MAX_AFFINITY_PARTITION_ID {
                return Err(IgniteError::from(
                    "affinity partition id exceeds sanity limit",
                ));
            }

            let partition = partition as usize;
            if partition_to_node.len() <= partition {
                partition_to_node.resize(partition + 1, None);
            }
            partition_to_node[partition] = Some(node_id.clone());
        }
    }

    Ok(partition_to_node)
}

fn rendezvous_partition(key_hash: i32, partition_count: i32) -> i32 {
    let mask = if (partition_count & (partition_count - 1)) == 0 {
        partition_count - 1
    } else {
        -1
    };

    if mask >= 0 {
        (key_hash ^ ((key_hash as u32 >> 16) as i32)) & mask
    } else {
        let part = (key_hash % partition_count).abs();
        if part > 0 {
            part
        } else {
            0
        }
    }
}

fn long_hash(value: i64) -> i32 {
    (value ^ ((value as u64 >> 32) as i64)) as i32
}

fn affinity_hash_marshaled(bytes: &[u8]) -> Option<i32> {
    let type_code = TypeCode::try_from(*bytes.first()?).ok()?;
    let mut reader = Cursor::new(&bytes[1..]);

    match type_code {
        TypeCode::Byte => Some(crate::protocol::read_i8(&mut reader).ok()? as i32),
        TypeCode::Short => Some(read_i16(&mut reader).ok()? as i32),
        TypeCode::Int => Some(read_i32(&mut reader).ok()?),
        TypeCode::Long => Some(long_hash(read_i64(&mut reader).ok()?)),
        TypeCode::Float => Some(read_i32(&mut reader).ok()?),
        TypeCode::Double => Some(long_hash(read_i64(&mut reader).ok()?)),
        TypeCode::Char => Some(crate::protocol::read_u16(&mut reader).ok()? as i32),
        TypeCode::Bool => Some(if read_bool(&mut reader).ok()? {
            1231
        } else {
            1237
        }),
        TypeCode::String => Some(string_to_java_hashcode(
            read_string(&mut reader).ok()?.as_str(),
        )),
        TypeCode::ArrByte => array_hash_i8(&mut reader),
        TypeCode::ArrShort => array_hash_i16(&mut reader),
        TypeCode::ArrInt => array_hash_i32(&mut reader),
        TypeCode::ArrLong => array_hash_i64(&mut reader),
        TypeCode::ArrChar => array_hash_u16(&mut reader),
        TypeCode::ArrBool => array_hash_bool(&mut reader),
        TypeCode::ComplexObj => {
            if bytes.len() < 12 {
                None
            } else {
                Some(i32::from_le_bytes(bytes[8..12].try_into().ok()?))
            }
        }
        _ => None,
    }
}

fn array_hash_i8(reader: &mut impl Read) -> Option<i32> {
    let len = read_i32(reader).ok()?;
    let mut result = 1i32;
    for _ in 0..len {
        result = result
            .wrapping_mul(31)
            .wrapping_add(crate::protocol::read_i8(reader).ok()? as i32);
    }
    Some(result)
}

fn array_hash_i16(reader: &mut impl Read) -> Option<i32> {
    let len = read_i32(reader).ok()?;
    let mut result = 1i32;
    for _ in 0..len {
        result = result
            .wrapping_mul(31)
            .wrapping_add(read_i16(reader).ok()? as i32);
    }
    Some(result)
}

fn array_hash_i32(reader: &mut impl Read) -> Option<i32> {
    let len = read_i32(reader).ok()?;
    let mut result = 1i32;
    for _ in 0..len {
        result = result.wrapping_mul(31).wrapping_add(read_i32(reader).ok()?);
    }
    Some(result)
}

fn array_hash_i64(reader: &mut impl Read) -> Option<i32> {
    let len = read_i32(reader).ok()?;
    let mut result = 1i32;
    for _ in 0..len {
        result = result
            .wrapping_mul(31)
            .wrapping_add(long_hash(read_i64(reader).ok()?));
    }
    Some(result)
}

fn array_hash_u16(reader: &mut impl Read) -> Option<i32> {
    let len = read_i32(reader).ok()?;
    let mut result = 1i32;
    for _ in 0..len {
        result = result
            .wrapping_mul(31)
            .wrapping_add(crate::protocol::read_u16(reader).ok()? as i32);
    }
    Some(result)
}

fn array_hash_bool(reader: &mut impl Read) -> Option<i32> {
    let len = read_i32(reader).ok()?;
    let mut result = 1i32;
    for _ in 0..len {
        let hash = if read_bool(reader).ok()? { 1231 } else { 1237 };
        result = result.wrapping_mul(31).wrapping_add(hash);
    }
    Some(result)
}

#[cfg(test)]
mod tests {
    use super::{
        affinity_hash_marshaled, marshal_key, rendezvous_partition, CachePartitionsRequest,
        CachePartitionsResponse,
    };
    use crate::connection_async::read_uuid_string;
    use crate::protocol::{write_bool, write_i32, write_i64};
    use crate::topology::TopologyVersion;
    use crate::WriteableReq;
    use std::io::Cursor;
    use std::sync::Arc;

    /// FND-053 — Java `ClientCacheAffinityMapping.writeRequest@2.17.0` emits
    /// `bool customMappingsRequired` before the cache-id array when the
    /// `ALL_AFFINITY_MAPPINGS` feature bit (13) is negotiated. Against any
    /// 2.17.0 server the bit is always advertised — so the bool must always
    /// precede `cacheIdCount` on the wire.
    #[test]
    fn cache_partitions_request_matches_java_layout_when_all_affinity_mappings_supported() {
        let req = CachePartitionsRequest {
            all_affinity_mappings: true,
            custom_mappings_required: false,
            include_dc_id: false,
            dc_id: None,
            cache_ids: vec![99, 100],
        };

        let mut buf = Vec::new();
        req.write(&mut buf).unwrap();

        let mut expected = Vec::new();
        write_bool(&mut expected, false).unwrap(); // customMappingsRequired
        write_i32(&mut expected, 2).unwrap(); // cacheIdCount
        write_i32(&mut expected, 99).unwrap();
        write_i32(&mut expected, 100).unwrap();

        assert_eq!(buf, expected);
        assert_eq!(req.size(), buf.len());
    }

    /// When the `ALL_AFFINITY_MAPPINGS` bit is NOT negotiated (old server), the
    /// Java client omits the bool — `writeRequest` writes only
    /// `i32 cacheIdCount` + the cache-id array.
    #[test]
    fn cache_partitions_request_omits_custom_mappings_when_bit_not_negotiated() {
        let req = CachePartitionsRequest {
            all_affinity_mappings: false,
            custom_mappings_required: false,
            include_dc_id: false,
            dc_id: None,
            cache_ids: vec![42],
        };

        let mut buf = Vec::new();
        req.write(&mut buf).unwrap();

        let mut expected = Vec::new();
        write_i32(&mut expected, 1).unwrap();
        write_i32(&mut expected, 42).unwrap();

        assert_eq!(buf, expected);
        assert_eq!(req.size(), buf.len());
    }

    /// FND-054 — When ALL_AFFINITY_MAPPINGS is negotiated, Java's
    /// `ClientCacheAffinityMapping.readResponse@2.17.0:231-232` reads a
    /// trailing `bool defaultAffinity` after the partition map. Rust was
    /// never consuming this byte, so subsequent mapping groups over-read
    /// into the next applicable-bool. This test pins the trailing-bool
    /// semantics.
    #[test]
    fn should_decode_cache_partitions_response_with_default_affinity_bool() {
        let node_a = (1i64, 2i64);
        let mut bytes = Vec::new();
        // topology version
        write_i64(&mut bytes, 5).unwrap();
        write_i32(&mut bytes, 0).unwrap();
        // mapping count = 1
        write_i32(&mut bytes, 1).unwrap();
        // applicable = true
        write_bool(&mut bytes, true).unwrap();
        // cachesInGroup = 1
        write_i32(&mut bytes, 1).unwrap();
        // cacheId=42, keyCfgCount=0
        write_i32(&mut bytes, 42).unwrap();
        write_i32(&mut bytes, 0).unwrap();
        // primary partition map — 1 node, 1 partition (part 0 → node_a)
        write_i32(&mut bytes, 1).unwrap();
        write_i64(&mut bytes, node_a.0).unwrap();
        write_i64(&mut bytes, node_a.1).unwrap();
        write_i32(&mut bytes, 1).unwrap();
        write_i32(&mut bytes, 0).unwrap();
        // ALL_AFFINITY_MAPPINGS trailing: defaultAffinity = true
        write_bool(&mut bytes, true).unwrap();

        let mut cursor = Cursor::new(bytes);
        let response =
            CachePartitionsResponse::read_with_flags(&mut cursor, false, true).unwrap();

        let cache = response.caches.get(&42).unwrap();
        assert_eq!(cache.primary_partition_to_node.len(), 1);
        // Cursor must be fully consumed — no leftover bytes.
        assert_eq!(cursor.position() as usize, cursor.get_ref().len());
    }

    /// Without ALL_AFFINITY_MAPPINGS, no trailing bool is expected.
    #[test]
    fn should_decode_cache_partitions_response_without_default_affinity_bool() {
        let node_a = (1i64, 2i64);
        let mut bytes = Vec::new();
        write_i64(&mut bytes, 5).unwrap();
        write_i32(&mut bytes, 0).unwrap();
        write_i32(&mut bytes, 1).unwrap();
        write_bool(&mut bytes, true).unwrap();
        write_i32(&mut bytes, 1).unwrap();
        write_i32(&mut bytes, 42).unwrap();
        write_i32(&mut bytes, 0).unwrap();
        write_i32(&mut bytes, 1).unwrap();
        write_i64(&mut bytes, node_a.0).unwrap();
        write_i64(&mut bytes, node_a.1).unwrap();
        write_i32(&mut bytes, 1).unwrap();
        write_i32(&mut bytes, 0).unwrap();

        let mut cursor = Cursor::new(bytes);
        let response =
            CachePartitionsResponse::read_with_flags(&mut cursor, false, false).unwrap();

        let cache = response.caches.get(&42).unwrap();
        assert_eq!(cache.primary_partition_to_node.len(), 1);
        assert_eq!(cursor.position() as usize, cursor.get_ref().len());
    }

    /// Gridgain-downstream DC_AWARE extension (bit 22, not in Java 2.17.0):
    /// when negotiated, the dc-id typed-string follows the
    /// `customMappingsRequired` bool. Preserved here for downstream
    /// compatibility. See FND-005 / FND-053 (Rust keeps this branch as the
    /// gridgain wire extension).
    #[test]
    fn cache_partitions_request_prepends_custom_mappings_bool_even_with_dc_aware() {
        let req = CachePartitionsRequest {
            all_affinity_mappings: true,
            custom_mappings_required: true,
            include_dc_id: true,
            dc_id: Some("dc1".to_string()),
            cache_ids: vec![7],
        };

        let mut buf = Vec::new();
        req.write(&mut buf).unwrap();

        let mut expected = Vec::new();
        write_bool(&mut expected, true).unwrap();
        crate::protocol::write_string(&mut expected, "dc1").unwrap();
        write_i32(&mut expected, 1).unwrap();
        write_i32(&mut expected, 7).unwrap();

        assert_eq!(buf, expected);
        assert_eq!(req.size(), buf.len());
    }

    #[test]
    fn should_compute_affinity_hash_for_primitives_and_strings() {
        assert_eq!(
            affinity_hash_marshaled(&marshal_key(&42i32).unwrap()),
            Some(42)
        );
        assert_eq!(
            affinity_hash_marshaled(&marshal_key(&"abc".to_string()).unwrap()),
            Some(crate::utils::string_to_java_hashcode("abc"))
        );
        assert_eq!(
            affinity_hash_marshaled(&marshal_key(&true).unwrap()),
            Some(1231)
        );
    }

    #[test]
    fn should_compute_rendezvous_partition() {
        assert_eq!(rendezvous_partition(42, 16), (42 ^ (42 >> 16)) & 15);
        assert_eq!(rendezvous_partition(-5, 10), 5);
    }

    #[test]
    fn should_decode_cache_partitions_response() {
        let node_a = (1i64, 2i64);
        let node_b = (3i64, 4i64);
        let mut bytes = Vec::new();
        write_i64(&mut bytes, 7).unwrap();
        write_i32(&mut bytes, 1).unwrap();
        write_i32(&mut bytes, 1).unwrap();
        write_bool(&mut bytes, true).unwrap();
        write_i32(&mut bytes, 1).unwrap();
        write_i32(&mut bytes, 99).unwrap();
        write_i32(&mut bytes, 0).unwrap();
        write_i32(&mut bytes, 2).unwrap();
        write_i64(&mut bytes, node_a.0).unwrap();
        write_i64(&mut bytes, node_a.1).unwrap();
        write_i32(&mut bytes, 2).unwrap();
        write_i32(&mut bytes, 0).unwrap();
        write_i32(&mut bytes, 2).unwrap();
        write_i64(&mut bytes, node_b.0).unwrap();
        write_i64(&mut bytes, node_b.1).unwrap();
        write_i32(&mut bytes, 1).unwrap();
        write_i32(&mut bytes, 1).unwrap();

        write_i32(&mut bytes, 2).unwrap();
        write_i64(&mut bytes, node_a.0).unwrap();
        write_i64(&mut bytes, node_a.1).unwrap();
        write_i32(&mut bytes, 1).unwrap();
        write_i32(&mut bytes, 0).unwrap();
        write_i64(&mut bytes, node_b.0).unwrap();
        write_i64(&mut bytes, node_b.1).unwrap();
        write_i32(&mut bytes, 2).unwrap();
        write_i32(&mut bytes, 1).unwrap();
        write_i32(&mut bytes, 2).unwrap();

        let mut cursor = Cursor::new(bytes);
        let response = CachePartitionsResponse::read_with_dc_aware(&mut cursor, true).unwrap();

        assert_eq!(response.topology_version, TopologyVersion::new(7, 1));
        let cache = response.caches.get(&99).unwrap();
        assert_eq!(cache.primary_partition_to_node.len(), 3);
        assert_eq!(
            cache.primary_partition_to_node[0],
            Some(MockUuid::new(node_a.0, node_a.1).as_arc_str())
        );
        assert_eq!(
            cache.primary_partition_to_node[1],
            Some(MockUuid::new(node_b.0, node_b.1).as_arc_str())
        );
        assert_eq!(
            cache.primary_partition_to_node[2],
            Some(MockUuid::new(node_a.0, node_a.1).as_arc_str())
        );
        assert_eq!(
            cache.dc_partition_to_node[0],
            Some(MockUuid::new(node_a.0, node_a.1).as_arc_str())
        );
        assert_eq!(
            cache.dc_partition_to_node[1],
            Some(MockUuid::new(node_b.0, node_b.1).as_arc_str())
        );
        assert_eq!(
            cache.dc_partition_to_node[2],
            Some(MockUuid::new(node_b.0, node_b.1).as_arc_str())
        );
    }

    #[test]
    fn should_share_partition_maps_across_caches_in_same_mapping_group() {
        let node_a = (1i64, 2i64);
        let mut bytes = Vec::new();
        write_i64(&mut bytes, 7).unwrap();
        write_i32(&mut bytes, 1).unwrap();
        write_i32(&mut bytes, 1).unwrap();
        write_bool(&mut bytes, true).unwrap();
        write_i32(&mut bytes, 2).unwrap();
        write_i32(&mut bytes, 99).unwrap();
        write_i32(&mut bytes, 0).unwrap();
        write_i32(&mut bytes, 100).unwrap();
        write_i32(&mut bytes, 0).unwrap();
        write_i32(&mut bytes, 1).unwrap();
        write_i64(&mut bytes, node_a.0).unwrap();
        write_i64(&mut bytes, node_a.1).unwrap();
        write_i32(&mut bytes, 2).unwrap();
        write_i32(&mut bytes, 0).unwrap();
        write_i32(&mut bytes, 1).unwrap();
        write_i32(&mut bytes, 1).unwrap();
        write_i64(&mut bytes, node_a.0).unwrap();
        write_i64(&mut bytes, node_a.1).unwrap();
        write_i32(&mut bytes, 2).unwrap();
        write_i32(&mut bytes, 0).unwrap();
        write_i32(&mut bytes, 1).unwrap();

        let mut cursor = Cursor::new(bytes);
        let response = CachePartitionsResponse::read_with_dc_aware(&mut cursor, true).unwrap();

        let cache_a = response.caches.get(&99).unwrap();
        let cache_b = response.caches.get(&100).unwrap();

        assert!(Arc::ptr_eq(
            &cache_a.primary_partition_to_node,
            &cache_b.primary_partition_to_node
        ));
        assert!(Arc::ptr_eq(
            &cache_a.dc_partition_to_node,
            &cache_b.dc_partition_to_node
        ));
    }

    #[test]
    fn should_reject_insane_affinity_partition_id() {
        let node_a = (1i64, 2i64);
        let mut bytes = Vec::new();
        write_i64(&mut bytes, 7).unwrap();
        write_i32(&mut bytes, 1).unwrap();
        write_i32(&mut bytes, 1).unwrap();
        write_bool(&mut bytes, true).unwrap();
        write_i32(&mut bytes, 1).unwrap();
        write_i32(&mut bytes, 99).unwrap();
        write_i32(&mut bytes, 0).unwrap();
        write_i32(&mut bytes, 1).unwrap();
        write_i64(&mut bytes, node_a.0).unwrap();
        write_i64(&mut bytes, node_a.1).unwrap();
        write_i32(&mut bytes, 1).unwrap();
        write_i32(&mut bytes, 100_000).unwrap();

        let mut cursor = Cursor::new(bytes);
        let err = match CachePartitionsResponse::read_with_dc_aware(&mut cursor, false) {
            Ok(_) => panic!("expected insane affinity partition id to be rejected"),
            Err(err) => err,
        };

        assert!(
            err.to_string().contains("sanity limit"),
            "unexpected affinity sanity-limit error: {}",
            err
        );
    }

    #[derive(Clone, Copy)]
    struct MockUuid {
        most: i64,
        least: i64,
    }

    impl MockUuid {
        const fn new(most: i64, least: i64) -> Self {
            Self { most, least }
        }

        fn as_arc_str(self) -> Arc<str> {
            let mut bytes = Vec::new();
            bytes.extend_from_slice(&self.most.to_le_bytes());
            bytes.extend_from_slice(&self.least.to_le_bytes());
            let mut cur = Cursor::new(bytes);
            Arc::from(read_uuid_string(&mut cur).unwrap().as_str())
        }
    }
}
