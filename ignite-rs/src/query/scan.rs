use crate::api::key_value::{cache_info_size, write_cache_info, CacheInfo};
use crate::protocol::{write_bool, write_i32, write_null};
use crate::WriteableReq;
use std::io::{self, Write};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScanQuery {
    page_size: i32,
    partition: Option<i32>,
    local: bool,
}

impl Default for ScanQuery {
    fn default() -> Self {
        Self {
            page_size: 1024,
            partition: None,
            local: false,
        }
    }
}

impl ScanQuery {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_page_size(mut self, page_size: i32) -> Self {
        self.page_size = page_size;
        self
    }

    pub fn with_partition(mut self, partition: i32) -> Self {
        self.partition = Some(partition);
        self
    }

    pub fn with_local(mut self, local: bool) -> Self {
        self.local = local;
        self
    }

    pub fn page_size(&self) -> i32 {
        self.page_size
    }

    pub fn partition(&self) -> Option<i32> {
        self.partition
    }

    pub fn local(&self) -> bool {
        self.local
    }
}

pub(crate) struct ScanQueryRequest {
    pub(crate) cache_info: CacheInfo,
    pub(crate) query: ScanQuery,
}

impl WriteableReq for ScanQueryRequest {
    fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        write_cache_info(writer, self.cache_info.with_keep_binary(true))?;
        write_null(writer)?;
        write_i32(writer, self.query.page_size)?;
        write_i32(writer, self.query.partition.unwrap_or(-1))?;
        write_bool(writer, self.query.local)?;
        Ok(())
    }

    fn size(&self) -> usize {
        cache_info_size(self.cache_info.with_keep_binary(true)) + 1 + 4 + 4 + 1
    }
}

#[cfg(test)]
mod tests {
    use super::{ScanQuery, ScanQueryRequest};
    use crate::api::key_value::CacheInfo;
    use crate::WriteableReq;

    #[test]
    fn should_encode_scan_query_with_partition_and_local_flags() {
        let req = ScanQueryRequest {
            cache_info: CacheInfo::new(42),
            query: ScanQuery::new()
                .with_page_size(64)
                .with_partition(7)
                .with_local(true),
        };

        let mut actual = Vec::new();
        req.write(&mut actual).unwrap();

        assert_eq!(req.size(), actual.len());
        assert_eq!(&actual[0..4], 42i32.to_le_bytes().as_slice());
        assert_eq!(actual[4], 1);
        assert_eq!(actual[5], crate::protocol::TypeCode::Null as u8);
        assert_eq!(&actual[6..10], 64i32.to_le_bytes().as_slice());
        assert_eq!(&actual[10..14], 7i32.to_le_bytes().as_slice());
        assert_eq!(actual[14], 1);
    }
}
