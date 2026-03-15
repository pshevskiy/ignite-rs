use crate::api::key_value::{cache_info_size, write_cache_info, CacheInfo};
use crate::binary::BinaryObjectBuilder;
use crate::protocol::write_i32;
use crate::{WritableType, WriteableReq};
use std::io::{self, Write};

const GRID_CACHE_VERSION_TYPE_NAME: &str =
    "org.apache.ignite.internal.processors.cache.version.GridCacheVersion";
const NODE_ORDER_MASK: i32 = 0x07_FF_FF_FF;
const DR_ID_SHIFT: i32 = 27;
pub const EXPIRE_TIME_ETERNAL: i64 = 0;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CacheVersion {
    topology_version: i32,
    order: i64,
    node_order: i32,
    data_center_id: u8,
}

impl CacheVersion {
    pub fn new(topology_version: i32, order: i64, node_order: i32, data_center_id: u8) -> Self {
        assert!(node_order >= 0, "node_order must be non-negative");
        assert!(
            node_order <= NODE_ORDER_MASK,
            "node_order exceeds Ignite wire-format mask"
        );
        assert!(
            data_center_id < 32,
            "data_center_id must fit into Ignite's 5-bit DR id field"
        );

        Self {
            topology_version,
            order,
            node_order,
            data_center_id,
        }
    }

    pub fn topology_version(&self) -> i32 {
        self.topology_version
    }

    pub fn order(&self) -> i64 {
        self.order
    }

    pub fn node_order(&self) -> i32 {
        self.node_order
    }

    pub fn data_center_id(&self) -> u8 {
        self.data_center_id
    }

    pub fn node_order_dr_id(&self) -> i32 {
        self.node_order | ((self.data_center_id as i32) << DR_ID_SHIFT)
    }

    fn as_binary_object(&self) -> crate::binary::BinaryObject {
        BinaryObjectBuilder::new(GRID_CACHE_VERSION_TYPE_NAME)
            .set_field("topVer", self.topology_version)
            .set_field("nodeOrderDrId", self.node_order_dr_id())
            .set_field("order", self.order)
            .build()
    }
}

impl WritableType for CacheVersion {
    fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        self.as_binary_object().write(writer)
    }

    fn size(&self) -> usize {
        self.as_binary_object().size()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConflictEntry<V> {
    pub value: V,
    pub version: CacheVersion,
    pub expire_time: i64,
}

impl<V> ConflictEntry<V> {
    pub fn new(value: V, version: CacheVersion) -> Self {
        Self {
            value,
            version,
            expire_time: EXPIRE_TIME_ETERNAL,
        }
    }

    pub fn with_expire_time(mut self, expire_time: i64) -> Self {
        self.expire_time = expire_time;
        self
    }
}

pub(crate) struct PutAllConflictRequest<'a, K: WritableType, V: WritableType> {
    pub(crate) cache_info: CacheInfo,
    pub(crate) entries: &'a [(K, ConflictEntry<V>)],
}

impl<'a, K: WritableType, V: WritableType> WriteableReq for PutAllConflictRequest<'a, K, V> {
    fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        write_cache_info(writer, self.cache_info)?;
        write_i32(writer, self.entries.len() as i32)?;

        for (key, entry) in self.entries {
            key.write(writer)?;
            entry.value.write(writer)?;
            entry.version.write(writer)?;
            crate::protocol::write_i64(writer, entry.expire_time)?;
        }

        Ok(())
    }

    fn size(&self) -> usize {
        cache_info_size(self.cache_info)
            + 4
            + self
                .entries
                .iter()
                .map(|(key, entry)| key.size() + entry.value.size() + entry.version.size() + 8)
                .sum::<usize>()
    }
}

pub(crate) struct RemoveAllConflictRequest<'a, K: WritableType> {
    pub(crate) cache_info: CacheInfo,
    pub(crate) entries: &'a [(K, CacheVersion)],
}

impl<'a, K: WritableType> WriteableReq for RemoveAllConflictRequest<'a, K> {
    fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        write_cache_info(writer, self.cache_info)?;
        write_i32(writer, self.entries.len() as i32)?;

        for (key, version) in self.entries {
            key.write(writer)?;
            version.write(writer)?;
        }

        Ok(())
    }

    fn size(&self) -> usize {
        cache_info_size(self.cache_info)
            + 4
            + self
                .entries
                .iter()
                .map(|(key, version)| key.size() + version.size())
                .sum::<usize>()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CacheVersion, ConflictEntry, PutAllConflictRequest, RemoveAllConflictRequest,
        EXPIRE_TIME_ETERNAL,
    };
    use crate::api::key_value::CacheInfo;
    use crate::binary::BinaryObject;
    use crate::protocol::complex_obj::IgniteValue;
    use crate::protocol::{read_i32, read_i64, read_u8};
    use crate::{ReadableType, WriteableReq};
    use std::io::Cursor;

    fn encode_request(req: &impl WriteableReq) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(req.size());
        req.write(&mut bytes).unwrap();
        bytes
    }

    fn read_cache_info(reader: &mut Cursor<&[u8]>) -> (i32, u8) {
        let cache_id = read_i32(reader).unwrap();
        let flags = read_u8(reader).unwrap();
        (cache_id, flags)
    }

    #[test]
    fn should_encode_put_all_conflict_entries_with_versions_and_expiry() {
        let entries = vec![
            (
                1,
                ConflictEntry::new(10, CacheVersion::new(1, 2, 3, 0)).with_expire_time(123_456),
            ),
            (2, ConflictEntry::new(20, CacheVersion::new(4, 5, 6, 1))),
        ];

        let bytes = encode_request(&PutAllConflictRequest {
            cache_info: CacheInfo::new(77),
            entries: &entries,
        });

        let mut reader = Cursor::new(bytes.as_slice());
        let (cache_id, flags) = read_cache_info(&mut reader);
        assert_eq!(cache_id, 77);
        assert_eq!(flags, 0);
        assert_eq!(read_i32(&mut reader).unwrap(), 2);

        let key = i32::read(&mut reader).unwrap().unwrap();
        let value = i32::read(&mut reader).unwrap().unwrap();
        let version = BinaryObject::read(&mut reader).unwrap().unwrap();
        let expire_time = read_i64(&mut reader).unwrap();
        assert_eq!(key, 1);
        assert_eq!(value, 10);
        assert_eq!(version.field("topVer"), Some(&IgniteValue::Int(1)));
        assert_eq!(version.field("nodeOrderDrId"), Some(&IgniteValue::Int(3)));
        assert_eq!(version.field("order"), Some(&IgniteValue::Long(2)));
        assert_eq!(expire_time, 123_456);

        let key = i32::read(&mut reader).unwrap().unwrap();
        let value = i32::read(&mut reader).unwrap().unwrap();
        let version = BinaryObject::read(&mut reader).unwrap().unwrap();
        let expire_time = read_i64(&mut reader).unwrap();
        assert_eq!(key, 2);
        assert_eq!(value, 20);
        assert_eq!(version.field("topVer"), Some(&IgniteValue::Int(4)));
        assert_eq!(
            version.field("nodeOrderDrId"),
            Some(&IgniteValue::Int(134_217_734))
        );
        assert_eq!(expire_time, EXPIRE_TIME_ETERNAL);
    }

    #[test]
    fn should_encode_remove_all_conflict_entries_with_versions() {
        let entries = vec![
            (1, CacheVersion::new(7, 8, 9, 0)),
            (2, CacheVersion::new(10, 11, 12, 0)),
        ];

        let bytes = encode_request(&RemoveAllConflictRequest {
            cache_info: CacheInfo::new(88),
            entries: &entries,
        });

        let mut reader = Cursor::new(bytes.as_slice());
        let (cache_id, flags) = read_cache_info(&mut reader);
        assert_eq!(cache_id, 88);
        assert_eq!(flags, 0);
        assert_eq!(read_i32(&mut reader).unwrap(), 2);

        let key = i32::read(&mut reader).unwrap().unwrap();
        let version = BinaryObject::read(&mut reader).unwrap().unwrap();
        assert_eq!(key, 1);
        assert_eq!(version.field("topVer"), Some(&IgniteValue::Int(7)));

        let key = i32::read(&mut reader).unwrap().unwrap();
        let version = BinaryObject::read(&mut reader).unwrap().unwrap();
        assert_eq!(key, 2);
        assert_eq!(version.field("order"), Some(&IgniteValue::Long(11)));
    }
}
