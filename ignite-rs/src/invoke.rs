use crate::api::key_value::{cache_info_size, write_cache_info, CacheInfo};
use crate::binary::BinaryObject;
use crate::error::{IgniteError, IgniteResult};
use crate::protocol::complex_obj::IgniteValue;
use crate::protocol::{read_bool, read_i32, read_string};
use crate::{ReadableReq, ReadableType, WritableType, WriteableReq};
use std::io::{self, Read, Write};

const JAVA_CLIENT_PLATFORM: u8 = 1;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InvokeAllResult<V> {
    Value(Option<V>),
    Error(String),
}

pub(crate) struct InvokeRequest<'a, K> {
    pub(crate) cache_info: CacheInfo,
    pub(crate) key: &'a K,
    pub(crate) processor: &'a BinaryObject,
    pub(crate) args: &'a [IgniteValue],
}

impl<K: WritableType> WriteableReq for InvokeRequest<'_, K> {
    fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        // FND-025: emit the caller's CacheInfo verbatim; do not force keep_binary.
        // Java's CacheInvokeRequest uses whatever flags the cache is configured with.
        write_cache_info(writer, self.cache_info)?;
        self.key.write(writer)?;
        self.processor.write(writer)?;
        crate::protocol::write_u8(writer, JAVA_CLIENT_PLATFORM)?;
        crate::protocol::write_i32(writer, self.args.len() as i32)?;
        for arg in self.args {
            arg.write(writer)?;
        }
        Ok(())
    }

    fn size(&self) -> usize {
        cache_info_size(self.cache_info)
            + self.key.size()
            + self.processor.size()
            + 1
            + 4
            + self.args.iter().map(IgniteValue::size).sum::<usize>()
    }
}

pub(crate) struct InvokeAllPreparedFirstRequest<'a, F, K> {
    pub(crate) cache_info: CacheInfo,
    pub(crate) first_key: Option<&'a F>,
    pub(crate) remaining_keys: &'a [K],
    pub(crate) processor: &'a BinaryObject,
    pub(crate) args: &'a [IgniteValue],
}

impl<F: WritableType, K: WritableType> WriteableReq for InvokeAllPreparedFirstRequest<'_, F, K> {
    fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        // FND-025: caller's cache_info is authoritative; do not force keep_binary.
        write_cache_info(writer, self.cache_info)?;
        crate::protocol::write_i32(
            writer,
            (usize::from(self.first_key.is_some()) + self.remaining_keys.len()) as i32,
        )?;
        if let Some(first_key) = self.first_key {
            first_key.write(writer)?;
        }
        for key in self.remaining_keys {
            key.write(writer)?;
        }
        self.processor.write(writer)?;
        crate::protocol::write_u8(writer, JAVA_CLIENT_PLATFORM)?;
        crate::protocol::write_i32(writer, self.args.len() as i32)?;
        for arg in self.args {
            arg.write(writer)?;
        }
        Ok(())
    }

    fn size(&self) -> usize {
        cache_info_size(self.cache_info)
            + 4
            + self.first_key.map(WritableType::size).unwrap_or(0)
            + self
                .remaining_keys
                .iter()
                .map(WritableType::size)
                .sum::<usize>()
            + self.processor.size()
            + 1
            + 4
            + self.args.iter().map(IgniteValue::size).sum::<usize>()
    }
}

pub(crate) struct InvokeAllResponse<K, V> {
    pub(crate) entries: Vec<(Option<K>, InvokeAllResult<V>)>,
}

impl<K: ReadableType, V: ReadableType> ReadableReq for InvokeAllResponse<K, V> {
    fn read(reader: &mut impl Read) -> IgniteResult<Self> {
        let count = read_i32(reader).map_err(IgniteError::from)?;
        let mut entries = Vec::with_capacity(count.max(0) as usize);

        for _ in 0..count {
            let key = K::read(reader)?;
            let success = read_bool(reader).map_err(IgniteError::from)?;
            let value = if success {
                InvokeAllResult::Value(V::read(reader)?)
            } else {
                InvokeAllResult::Error(read_string(reader).map_err(IgniteError::from)?)
            };
            entries.push((key, value));
        }

        Ok(Self { entries })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::key_value::KEEP_BINARY_FLAG_MASK;
    use crate::binary::BinaryObjectBuilder;

    /// FND-025: invoke must not force keep_binary=true; the caller's cache_info flag is authoritative
    /// (Java CacheInvokeRequest propagates the cache flags as-is).
    #[test]
    fn should_honour_caller_keep_binary_flag_on_invoke() {
        let processor = BinaryObjectBuilder::new("Proc").build();
        let key: i32 = 1;
        let args: Vec<IgniteValue> = Vec::new();

        let req = InvokeRequest {
            cache_info: CacheInfo::new(42), // keep_binary=false
            key: &key,
            processor: &processor,
            args: &args,
        };

        let mut buf = Vec::new();
        req.write(&mut buf).unwrap();

        // cache_id (4) then flags (1) — flags must be 0 when caller did not request keep_binary
        assert_eq!(&buf[0..4], 42i32.to_le_bytes().as_slice());
        assert_eq!(
            buf[4] & KEEP_BINARY_FLAG_MASK,
            0,
            "invoke must not force KEEP_BINARY flag; caller's cache_info drives it"
        );
        assert_eq!(req.size(), buf.len());
    }

    #[test]
    fn should_honour_caller_keep_binary_flag_on_invoke_all() {
        let processor = BinaryObjectBuilder::new("Proc").build();
        let remaining: [i32; 0] = [];
        let args: Vec<IgniteValue> = Vec::new();
        let first: i32 = 1;

        let req = InvokeAllPreparedFirstRequest {
            cache_info: CacheInfo::new(7),
            first_key: Some(&first),
            remaining_keys: &remaining[..],
            processor: &processor,
            args: &args,
        };

        let mut buf = Vec::new();
        req.write(&mut buf).unwrap();

        assert_eq!(&buf[0..4], 7i32.to_le_bytes().as_slice());
        assert_eq!(
            buf[4] & KEEP_BINARY_FLAG_MASK,
            0,
            "invoke_all must not force KEEP_BINARY flag; caller's cache_info drives it"
        );
        assert_eq!(req.size(), buf.len());
    }
}
