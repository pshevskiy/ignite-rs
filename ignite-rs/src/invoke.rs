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
        write_cache_info(writer, self.cache_info.with_keep_binary(true))?;
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
        cache_info_size(self.cache_info.with_keep_binary(true))
            + self.key.size()
            + self.processor.size()
            + 1
            + 4
            + self.args.iter().map(IgniteValue::size).sum::<usize>()
    }
}

pub(crate) struct InvokeAllRequest<'a, K> {
    pub(crate) cache_info: CacheInfo,
    pub(crate) keys: &'a [K],
    pub(crate) processor: &'a BinaryObject,
    pub(crate) args: &'a [IgniteValue],
}

impl<K: WritableType> WriteableReq for InvokeAllRequest<'_, K> {
    fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        write_cache_info(writer, self.cache_info.with_keep_binary(true))?;
        crate::protocol::write_i32(writer, self.keys.len() as i32)?;
        for key in self.keys {
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
        cache_info_size(self.cache_info.with_keep_binary(true))
            + 4
            + self.keys.iter().map(WritableType::size).sum::<usize>()
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
