use crate::cache::{CachePeekMode, ExpiryPolicy};
use crate::error::IgniteResult;
use crate::protocol::complex_obj::{ComplexObject, IgniteValue};
use crate::protocol::{
    read_bool, read_i32, read_i64, write_bool, write_i32, write_i64, write_null, write_u8,
};
use crate::{ReadableReq, ReadableType, WritableType, WriteableReq};

use std::io;
use std::io::{Read, Write};
use std::mem::size_of;

// https://apacheignite.readme.io/docs/binary-client-protocol-key-value-operations#op_cache_get
pub(crate) const KEEP_BINARY_FLAG_MASK: u8 = 0x01;
pub(crate) const TRANSACTIONAL_FLAG_MASK: u8 = 0x02;
pub(crate) const EXPIRY_POLICY_FLAG_MASK: u8 = 0x04;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct CacheInfo {
    pub(crate) cache_id: i32,
    pub(crate) flags: u8,
    pub(crate) expiry_policy: Option<ExpiryPolicy>,
    pub(crate) tx_id: Option<i32>,
}

impl CacheInfo {
    pub(crate) const fn new(cache_id: i32) -> Self {
        Self {
            cache_id,
            flags: 0,
            expiry_policy: None,
            tx_id: None,
        }
    }

    pub(crate) const fn with_keep_binary(mut self, keep_binary: bool) -> Self {
        if keep_binary {
            self.flags |= KEEP_BINARY_FLAG_MASK;
        }
        self
    }

    pub(crate) const fn with_tx_id(mut self, tx_id: Option<i32>) -> Self {
        self.tx_id = tx_id;
        if tx_id.is_some() {
            self.flags |= TRANSACTIONAL_FLAG_MASK;
        }
        self
    }

    pub(crate) const fn with_expiry_policy(mut self, expiry_policy: Option<ExpiryPolicy>) -> Self {
        self.expiry_policy = expiry_policy;
        if expiry_policy.is_some() {
            self.flags |= EXPIRY_POLICY_FLAG_MASK;
        }
        self
    }
}

pub(crate) fn write_cache_info(writer: &mut dyn Write, info: CacheInfo) -> io::Result<()> {
    write_i32(writer, info.cache_id)?;
    write_u8(writer, info.flags)?;
    if let Some(expiry_policy) = info.expiry_policy {
        write_i64(writer, expiry_policy.create.to_wire())?;
        write_i64(writer, expiry_policy.update.to_wire())?;
        write_i64(writer, expiry_policy.access.to_wire())?;
    }
    if let Some(tx_id) = info.tx_id {
        write_i32(writer, tx_id)?;
    }
    Ok(())
}

pub(crate) const fn cache_info_size(info: CacheInfo) -> usize {
    4 + 1
        + if info.expiry_policy.is_some() {
            8 * 3
        } else {
            0
        }
        + if info.tx_id.is_some() { 4 } else { 0 }
}

#[allow(dead_code)]
pub(crate) enum CacheReq<'a, K: WritableType, V: WritableType> {
    Get(CacheInfo, &'a K),
    GetAll(CacheInfo, &'a [K]),
    Put(CacheInfo, &'a K, &'a V),
    PutAll(CacheInfo, &'a [(K, V)]),
    ContainsKey(CacheInfo, &'a K),
    ContainsKeys(CacheInfo, &'a [K]),
    GetAndPut(CacheInfo, &'a K, &'a V),
    GetAndReplace(CacheInfo, &'a K, &'a V),
    GetAndRemove(CacheInfo, &'a K),
    PutIfAbsent(CacheInfo, &'a K, &'a V),
    GetAndPutIfAbsent(CacheInfo, &'a K, &'a V),
    Replace(CacheInfo, &'a K, &'a V),
    ReplaceIfEquals(CacheInfo, &'a K, &'a V, &'a V),
    Clear(CacheInfo),
    ClearKey(CacheInfo, &'a K),
    ClearKeys(CacheInfo, &'a [K]),
    RemoveKey(CacheInfo, &'a K),
    RemoveIfEquals(CacheInfo, &'a K, &'a V),
    GetSize(CacheInfo, Vec<CachePeekMode>),
    RemoveKeys(CacheInfo, &'a [K]),
    RemoveAll(CacheInfo),
    QueryScan(CacheInfo, i32), // cache ID, page size,
    // OP_QUERY_SQL_FIELDS
    // (cache_id, schema, page_size, sql)
    // QuerySqlFields(i32, Option<&'a str>, i32, &'a str),
    // (cache_id, schema, page_size, sql, args)
    QuerySqlFieldsArgs(CacheInfo, Option<&'a str>, i32, &'a str, &'a [IgniteValue]),
    // Cursor control
    CursorGetPage(i64),
    CursorClose(i64),
}

impl<'a, K: WritableType, V: WritableType> WriteableReq for CacheReq<'a, K, V> {
    fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        match self {
            CacheReq::Get(info, key)
            | CacheReq::ContainsKey(info, key)
            | CacheReq::GetAndRemove(info, key)
            | CacheReq::ClearKey(info, key)
            | CacheReq::RemoveKey(info, key) => {
                write_cache_info(writer, *info)?;
                key.write(writer)?;
                Ok(())
            }
            CacheReq::GetAll(info, keys)
            | CacheReq::ContainsKeys(info, keys)
            | CacheReq::ClearKeys(info, keys)
            | CacheReq::RemoveKeys(info, keys) => {
                write_cache_info(writer, *info)?;
                write_i32(writer, keys.len() as i32)?;
                for k in *keys {
                    k.write(writer)?;
                }
                Ok(())
            }
            CacheReq::Put(info, key, value)
            | CacheReq::GetAndPut(info, key, value)
            | CacheReq::GetAndReplace(info, key, value)
            | CacheReq::PutIfAbsent(info, key, value)
            | CacheReq::GetAndPutIfAbsent(info, key, value)
            | CacheReq::Replace(info, key, value)
            | CacheReq::RemoveIfEquals(info, key, value) => {
                write_cache_info(writer, *info)?;
                key.write(writer)?;
                value.write(writer)?;
                Ok(())
            }
            CacheReq::PutAll(info, pairs) => {
                write_cache_info(writer, *info)?;
                write_i32(writer, pairs.len() as i32)?;
                for pair in *pairs {
                    pair.0.write(writer)?;
                    pair.1.write(writer)?;
                }
                Ok(())
            }
            CacheReq::ReplaceIfEquals(info, key, old, new) => {
                write_cache_info(writer, *info)?;
                key.write(writer)?;
                old.write(writer)?;
                new.write(writer)?;
                Ok(())
            }
            CacheReq::Clear(info) | CacheReq::RemoveAll(info) => {
                write_cache_info(writer, *info)?;
                Ok(())
            }
            CacheReq::GetSize(info, modes) => {
                write_cache_info(writer, *info)?;
                write_i32(writer, modes.len() as i32)?;
                for mode in modes {
                    write_u8(writer, mode.clone() as u8)?;
                }
                Ok(())
            }
            // https://ignite.apache.org/docs/latest/binary-client-protocol/sql-and-scan-queries#op_query_scan
            CacheReq::QueryScan(info, pg_sz) => {
                write_cache_info(writer, info.with_keep_binary(true))?;
                write_null(writer)?; // Not possible to pass filter object unless Java or .NET
                write_i32(writer, *pg_sz)?;
                write_i32(writer, -1)?; // negative to query entire cache
                write_bool(writer, false)?; // can be executed anywhere?
                Ok(())
            }
            // CacheReq::QuerySqlFields(id, schema, page_size, sql) => {
            //     // OP_QUERY_SQL_FIELDS per spec
            //     write_i32(writer, *id)?;
            //     write_u8(writer, 1u8)?;                           // flags: KEEP_BINARY
            //     match schema {
            //         Some(s) => crate::protocol::write_string_type_code(writer, s)?,
            //         None => write_null(writer)?,
            //     }
            //     write_i32(writer, *page_size)?;                   // page size
            //     write_i32(writer, -1)?;                           // max rows (-1 => do not limit)
            //     crate::protocol::write_string_type_code(writer, sql)?; // SQL (typed string)
            //     write_i32(writer, 0)?;                            // arg count
            //     write_u8(writer, 0u8)?;                           // statement type: ANY
            //     write_bool(writer, false)?; // distributed joins
            //     write_bool(writer, false)?; // local query
            //     write_bool(writer, false)?; // replicated only
            //     write_bool(writer, false)?; // enforce join order
            //     write_bool(writer, false)?; // collocated
            //     write_bool(writer, false)?; // lazy
            //     write_i64(writer, 0i64)?;    // timeout (ms)
            //     write_bool(writer, true)?;   // include field names
            //     Ok(())
            // }
            CacheReq::QuerySqlFieldsArgs(info, schema, page_size, sql, args) => {
                // OP_QUERY_SQL_FIELDS per spec
                write_cache_info(writer, info.with_keep_binary(true))?;
                match schema {
                    Some(s) => crate::protocol::write_string_type_code(writer, s)?,
                    None => write_null(writer)?,
                }
                write_i32(writer, *page_size)?; // page size
                write_i32(writer, -1)?; // max rows (-1 => unlimited)

                crate::protocol::write_string_type_code(writer, sql)?; // SQL (typed string)
                write_i32(writer, args.len() as i32)?; // arg count
                for a in (*args).iter() {
                    // args (data objects)
                    a.write(writer)?;
                }
                write_u8(writer, 0u8)?; // statement type: ANY
                write_bool(writer, false)?; // distributed joins
                write_bool(writer, false)?; // local query
                write_bool(writer, false)?; // replicated only
                write_bool(writer, false)?; // enforce join order
                write_bool(writer, false)?; // collocated
                write_bool(writer, false)?; // lazy
                write_i64(writer, 500i64)?; // timeout (ms)
                write_bool(writer, true)?; // include field names
                Ok(())
            }
            CacheReq::CursorGetPage(cursor_id) => {
                write_i64(writer, *cursor_id)?;
                Ok(())
            }
            CacheReq::CursorClose(cursor_id) => {
                write_i64(writer, *cursor_id)?;
                Ok(())
            }
        }
    }

    fn size(&self) -> usize {
        match self {
            CacheReq::Get(info, key)
            | CacheReq::ContainsKey(info, key)
            | CacheReq::GetAndRemove(info, key)
            | CacheReq::ClearKey(info, key)
            | CacheReq::RemoveKey(info, key) => cache_info_size(*info) + key.size(),
            CacheReq::GetAll(info, keys)
            | CacheReq::ContainsKeys(info, keys)
            | CacheReq::ClearKeys(info, keys)
            | CacheReq::RemoveKeys(info, keys) => {
                let mut size = cache_info_size(*info);
                size += 4; // len
                for k in *keys {
                    size += k.size();
                }
                size
            }
            CacheReq::Put(info, key, value)
            | CacheReq::GetAndPut(info, key, value)
            | CacheReq::GetAndReplace(info, key, value)
            | CacheReq::PutIfAbsent(info, key, value)
            | CacheReq::GetAndPutIfAbsent(info, key, value)
            | CacheReq::Replace(info, key, value)
            | CacheReq::RemoveIfEquals(info, key, value) => {
                cache_info_size(*info) + key.size() + value.size()
            }
            CacheReq::PutAll(info, pairs) => {
                let mut size = cache_info_size(*info);
                size += 4; //len
                for pair in *pairs {
                    size += pair.0.size();
                    size += pair.1.size();
                }
                size
            }
            CacheReq::ReplaceIfEquals(info, key, old, new) => {
                cache_info_size(*info) + key.size() + old.size() + new.size()
            }
            CacheReq::Clear(info) | CacheReq::RemoveAll(info) => cache_info_size(*info),
            CacheReq::GetSize(info, modes) => {
                let mut size = cache_info_size(*info);
                size += 4; //len
                for _ in modes {
                    size += 1;
                }
                size
            }
            CacheReq::QueryScan(info, _) => {
                cache_info_size(info.with_keep_binary(true))
                    + size_of::<u8>() // Filter object: Null
                    + size_of::<i32>() // Cursor page size
                    + size_of::<i32>() // Partition count
                    + size_of::<u8>() // local only flag
            }
            // CacheReq::QuerySqlFields(_, schema, page_size, sql) => {
            //     4 // cache id
            //         + 1 // deprecated byte
            //         + match schema {
            //             Some(s) => 1 + size_of::<i32>() + s.len(),
            //             None => 1, // Null type code
            //         }
            //         + size_of::<i32>() // page size
            //         + size_of::<i32>() // max rows
            //         + (1 + size_of::<i32>() + (*sql).len()) // typed SQL string
            //         + size_of::<i32>() // arg count (0)
            //         + 1 // statement type
            //         + (size_of::<u8>() * 6) // flags
            //         + size_of::<i64>() // timeout
            //         + size_of::<u8>() // include field names
            // }
            CacheReq::QuerySqlFieldsArgs(info, schema, _page_size, sql, args) => {
                let mut sz = cache_info_size(info.with_keep_binary(true))
                    + match schema {
                        Some(s) => 1 + size_of::<i32>() + s.len(),
                        None => 1,
                    }
                    + size_of::<i32>() // page size
                    + size_of::<i32>() // max rows
                    + (1 + size_of::<i32>() + (*sql).len()) // typed SQL string
                    + size_of::<i32>(); // arg count
                for a in (*args).iter() {
                    sz += a.size();
                }
                sz + 1 // statement type
                    + (size_of::<u8>() * 6) // flags
                    + size_of::<i64>() // timeout
                    + size_of::<u8>() // include field names
            }
            CacheReq::CursorGetPage(_) => size_of::<i64>(),
            CacheReq::CursorClose(_) => size_of::<i64>(),
        }
    }
}

pub(crate) struct CacheDataObjectResp<V: ReadableType> {
    pub(crate) val: Option<V>,
}

impl<V: ReadableType> ReadableReq for CacheDataObjectResp<V> {
    fn read(reader: &mut impl Read) -> IgniteResult<Self> {
        let val = V::read(reader)?;
        Ok(CacheDataObjectResp { val })
    }
}

pub(crate) struct CachePairsResp<K: ReadableType, V: ReadableType> {
    pub(crate) val: Vec<(Option<K>, Option<V>)>,
}

impl<K: ReadableType, V: ReadableType> ReadableReq for CachePairsResp<K, V> {
    fn read(reader: &mut impl Read) -> IgniteResult<Self> {
        let count = read_i32(reader)?;
        let mut pairs: Vec<(Option<K>, Option<V>)> = Vec::new();
        for _ in 0..count {
            let key = K::read(reader)?;
            let val = V::read(reader)?;
            pairs.push((key, val));
        }
        Ok(CachePairsResp { val: pairs })
    }
}

pub(crate) struct CursorOpenResp<K: ReadableType, V: ReadableType> {
    pub(crate) cursor_id: i64,
    pub(crate) rows: Vec<(Option<K>, Option<V>)>,
    pub(crate) has_more: bool,
}

impl<K: ReadableType, V: ReadableType> ReadableReq for CursorOpenResp<K, V> {
    fn read(reader: &mut impl Read) -> IgniteResult<Self> {
        let cursor_id = read_i64(reader)?;
        let count = read_i32(reader)?;
        let mut rows: Vec<(Option<K>, Option<V>)> = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let key = K::read(reader)?;
            let val = V::read(reader)?;
            rows.push((key, val));
        }
        let has_more = read_bool(reader)?;
        Ok(CursorOpenResp {
            cursor_id,
            rows,
            has_more,
        })
    }
}

pub(crate) struct CursorPageResp<K: ReadableType, V: ReadableType> {
    pub(crate) rows: Vec<(Option<K>, Option<V>)>,
    pub(crate) has_more: bool,
}

// SqlFields open response; we return the first column as Long per row.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) struct SqlFieldsOpenRespLong {
    pub(crate) cursor_id: i64,
    pub(crate) rows: Vec<i64>,
    pub(crate) has_more: bool,
}

impl ReadableReq for SqlFieldsOpenRespLong {
    fn read(reader: &mut impl Read) -> IgniteResult<Self> {
        let cursor_id = read_i64(reader)?;
        let col_cnt = read_i32(reader)? as usize;
        // We send includeFieldNames=true, so read and discard field names
        for _ in 0..col_cnt {
            let _ = String::read(reader)?;
        }
        let row_cnt = read_i32(reader)? as usize;
        let mut rows: Vec<i64> = Vec::with_capacity(row_cnt);
        for _ in 0..row_cnt {
            // Read first column as i64, skip the rest generically
            if let Some(v) = i64::read(reader)? {
                rows.push(v);
            } else {
                rows.push(0);
            }
            for _ in 1..col_cnt {
                let _ = ComplexObject::read(reader)?; // consume any value type
            }
        }
        let has_more = read_bool(reader)?;
        Ok(SqlFieldsOpenRespLong {
            cursor_id,
            rows,
            has_more,
        })
    }
}

// SqlFields page response returning a single Long column per row
#[allow(dead_code)]
pub(crate) struct SqlFieldsPageRespLong {
    pub(crate) rows: Vec<i64>,
    pub(crate) has_more: bool,
}

impl ReadableReq for SqlFieldsPageRespLong {
    fn read(reader: &mut impl Read) -> IgniteResult<Self> {
        let row_cnt = read_i32(reader)? as usize;
        let mut rows: Vec<i64> = Vec::with_capacity(row_cnt);
        for _ in 0..row_cnt {
            if let Some(v) = i64::read(reader)? {
                rows.push(v);
            } else {
                rows.push(0);
            }
        }
        let has_more = read_bool(reader)?;
        Ok(SqlFieldsPageRespLong { rows, has_more })
    }
}

impl<K: ReadableType, V: ReadableType> ReadableReq for CursorPageResp<K, V> {
    fn read(reader: &mut impl Read) -> IgniteResult<Self> {
        let count = read_i32(reader)?;
        let mut rows: Vec<(Option<K>, Option<V>)> = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let key = K::read(reader)?;
            let val = V::read(reader)?;
            rows.push((key, val));
        }
        let has_more = read_bool(reader)?;
        Ok(CursorPageResp { rows, has_more })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::{ExpiryDuration, ExpiryPolicy};
    use crate::protocol::TypeCode;
    use crate::protocol::{write_bool, write_i32, write_i64, write_string, write_u8};
    use std::io::Cursor;

    #[test]
    fn test_sql_fields_args_encoding_size_matches() {
        let sql = "SELECT _key.payload FROM \"my_cache\" WHERE _key.value = ?";
        let args = [IgniteValue::String("abc".to_string())];
        let req: CacheReq<'_, i32, i32> =
            CacheReq::QuerySqlFieldsArgs(CacheInfo::new(42), None, 1000, sql, &args);
        let mut buf = Vec::new();
        req.write(&mut buf).unwrap();
        assert_eq!(req.size(), buf.len());

        // Quick sanity: buffer starts with cacheId, then flags 0x01, then schema NULL (0x65)
        assert_eq!(&buf[0..4], 42i32.to_le_bytes().as_slice());
        assert_eq!(buf[4], 1);
        assert_eq!(buf[5], crate::protocol::TypeCode::Null as u8);
    }

    #[test]
    fn test_sql_fields_noargs_encoding_size_matches() {
        let sql = "SELECT 1";
        let args: [IgniteValue; 0] = [];
        let req: CacheReq<'_, i32, i32> =
            CacheReq::QuerySqlFieldsArgs(CacheInfo::new(7), None, 1, sql, &args);
        let mut buf = Vec::new();
        req.write(&mut buf).unwrap();
        assert_eq!(req.size(), buf.len());
        assert_eq!(&buf[0..4], 7i32.to_le_bytes().as_slice());
        assert_eq!(buf[4], 1);
        assert_eq!(buf[5], crate::protocol::TypeCode::Null as u8);
    }

    #[test]
    fn test_cursor_get_page_encoding() {
        let req: CacheReq<'_, i32, i32> = CacheReq::CursorGetPage(42);
        let mut actual = Vec::new();
        req.write(&mut actual).unwrap();

        let mut expected = Vec::new();
        write_i64(&mut expected, 42).unwrap();

        assert_eq!(actual, expected);
        assert_eq!(req.size(), actual.len());
    }

    #[test]
    fn test_cursor_close_encoding() {
        let req: CacheReq<'_, i32, i32> = CacheReq::CursorClose(0x1122334455667788);
        let mut actual = Vec::new();
        req.write(&mut actual).unwrap();

        let mut expected = Vec::new();
        write_i64(&mut expected, 0x1122334455667788).unwrap();
        assert_eq!(actual, expected);
        assert_eq!(req.size(), actual.len());
    }

    #[test]
    fn test_cache_info_encodes_expiry_before_tx_id() {
        let info = CacheInfo::new(7)
            .with_keep_binary(true)
            .with_expiry_policy(Some(ExpiryPolicy::new(
                ExpiryDuration::Millis(std::time::Duration::from_millis(10)),
                ExpiryDuration::Unchanged,
                ExpiryDuration::Zero,
            )))
            .with_tx_id(Some(42));

        let mut actual = Vec::new();
        write_cache_info(&mut actual, info).unwrap();

        assert_eq!(&actual[0..4], 7i32.to_le_bytes().as_slice());
        assert_eq!(
            actual[4],
            KEEP_BINARY_FLAG_MASK | EXPIRY_POLICY_FLAG_MASK | TRANSACTIONAL_FLAG_MASK
        );
        assert_eq!(&actual[5..13], 10i64.to_le_bytes().as_slice());
        assert_eq!(&actual[13..21], (-2i64).to_le_bytes().as_slice());
        assert_eq!(&actual[21..29], 0i64.to_le_bytes().as_slice());
        assert_eq!(&actual[29..33], 42i32.to_le_bytes().as_slice());
        assert_eq!(cache_info_size(info), actual.len());
    }

    #[test]
    fn test_cursor_open_resp_decode() {
        // Build a synthetic response: cursor_id, count=2, [ (k=1, v="a"), (k=2, v="bb") ], more=true
        let mut bytes = Vec::new();
        write_i64(&mut bytes, 99).unwrap();
        write_i32(&mut bytes, 2).unwrap();
        // row 1
        write_u8(&mut bytes, TypeCode::Int as u8).unwrap();
        write_i32(&mut bytes, 1).unwrap();
        write_u8(&mut bytes, TypeCode::String as u8).unwrap();
        write_string(&mut bytes, "a").unwrap();
        // row 2
        write_u8(&mut bytes, TypeCode::Int as u8).unwrap();
        write_i32(&mut bytes, 2).unwrap();
        write_u8(&mut bytes, TypeCode::String as u8).unwrap();
        write_string(&mut bytes, "bb").unwrap();
        write_bool(&mut bytes, true).unwrap();

        let mut cursor = Cursor::new(bytes);
        let resp = CursorOpenResp::<i32, String>::read(&mut cursor).unwrap();
        assert_eq!(resp.cursor_id, 99);
        assert_eq!(resp.rows.len(), 2);
        assert_eq!(resp.rows[0].0, Some(1));
        assert_eq!(resp.rows[0].1, Some("a".to_string()));
        assert_eq!(resp.rows[1].0, Some(2));
        assert_eq!(resp.rows[1].1, Some("bb".to_string()));
        assert!(resp.has_more);
    }

    #[test]
    fn test_cursor_page_resp_decode() {
        // Build a synthetic page: count=1, [ (k=3, v="ccc") ], more=false
        let mut bytes = Vec::new();
        write_i32(&mut bytes, 1).unwrap();
        write_u8(&mut bytes, TypeCode::Int as u8).unwrap();
        write_i32(&mut bytes, 3).unwrap();
        write_u8(&mut bytes, TypeCode::String as u8).unwrap();
        write_string(&mut bytes, "ccc").unwrap();
        write_bool(&mut bytes, false).unwrap();

        let mut cursor = Cursor::new(bytes);
        let resp = CursorPageResp::<i32, String>::read(&mut cursor).unwrap();
        assert_eq!(resp.rows.len(), 1);
        assert_eq!(resp.rows[0].0, Some(3));
        assert_eq!(resp.rows[0].1, Some("ccc".to_string()));
        assert!(!resp.has_more);
    }

    #[test]
    fn test_sql_fields_open_resp_long_decode_with_field_names_and_extra_cols() {
        // Build a synthetic SqlFields open response with field names and 2 columns
        // Layout: cursor_id, col_cnt=2, [names...], row_cnt=2, rows, has_more=false
        let mut bytes = Vec::new();
        // cursor id
        write_i64(&mut bytes, 7).unwrap();
        // columns
        write_i32(&mut bytes, 2).unwrap();
        // field names (as DataObjects)
        write_u8(&mut bytes, TypeCode::String as u8).unwrap();
        write_string(&mut bytes, "ID").unwrap();
        write_u8(&mut bytes, TypeCode::String as u8).unwrap();
        write_string(&mut bytes, "NAME").unwrap();
        // rows
        write_i32(&mut bytes, 2).unwrap();
        // row 1: first col long=10, second col string="a"
        write_u8(&mut bytes, TypeCode::Long as u8).unwrap();
        write_i64(&mut bytes, 10).unwrap();
        write_u8(&mut bytes, TypeCode::String as u8).unwrap();
        write_string(&mut bytes, "a").unwrap();
        // row 2: first col long=20, second col string="bb"
        write_u8(&mut bytes, TypeCode::Long as u8).unwrap();
        write_i64(&mut bytes, 20).unwrap();
        write_u8(&mut bytes, TypeCode::String as u8).unwrap();
        write_string(&mut bytes, "bb").unwrap();
        // has more
        write_bool(&mut bytes, false).unwrap();

        let mut cursor = Cursor::new(bytes);
        let resp = SqlFieldsOpenRespLong::read(&mut cursor).unwrap();
        assert_eq!(resp.cursor_id, 7);
        assert_eq!(resp.rows, vec![10, 20]);
        assert!(!resp.has_more);
    }
}

pub(crate) struct CacheSizeResp {
    pub(crate) size: i64,
}

impl ReadableReq for CacheSizeResp {
    fn read(reader: &mut impl Read) -> IgniteResult<Self> {
        let size = read_i64(reader)?;
        Ok(CacheSizeResp { size })
    }
}

pub(crate) struct CacheBoolResp {
    pub(crate) flag: bool,
}

impl ReadableReq for CacheBoolResp {
    fn read(reader: &mut impl Read) -> IgniteResult<Self> {
        let flag = read_bool(reader)?;
        Ok(CacheBoolResp { flag })
    }
}
