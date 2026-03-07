use crate::cache::CachePeekMode;
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
const MAGIC_BYTE: u8 = 0;
const CACHE_ID_MAGIC_BYTE_SIZE: usize = 5;

pub(crate) enum CacheReq<'a, K: WritableType, V: WritableType> {
    Get(i32, &'a K),
    GetAll(i32, &'a [K]),
    Put(i32, &'a K, &'a V),
    PutAll(i32, &'a [(K, V)]),
    ContainsKey(i32, &'a K),
    ContainsKeys(i32, &'a [K]),
    GetAndPut(i32, &'a K, &'a V),
    GetAndReplace(i32, &'a K, &'a V),
    GetAndRemove(i32, &'a K),
    PutIfAbsent(i32, &'a K, &'a V),
    GetAndPutIfAbsent(i32, &'a K, &'a V),
    Replace(i32, &'a K, &'a V),
    ReplaceIfEquals(i32, &'a K, &'a V, &'a V),
    Clear(i32),
    ClearKey(i32, &'a K),
    ClearKeys(i32, &'a [K]),
    RemoveKey(i32, &'a K),
    RemoveIfEquals(i32, &'a K, &'a V),
    GetSize(i32, Vec<CachePeekMode>),
    RemoveKeys(i32, &'a [K]),
    RemoveAll(i32),
    QueryScan(i32, i32), // cache ID, page size,
    // OP_QUERY_SQL_FIELDS
    // (cache_id, schema, page_size, sql)
    // QuerySqlFields(i32, Option<&'a str>, i32, &'a str),
    // (cache_id, schema, page_size, sql, args)
    QuerySqlFieldsArgs(i32, Option<&'a str>, i32, &'a str, &'a [IgniteValue]),
    // Cursor control
    CursorGetPage(i64, i32),
    CursorClose(i64),
}

impl<'a, K: WritableType, V: WritableType> WriteableReq for CacheReq<'a, K, V> {
    fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        match self {
            CacheReq::Get(id, key)
            | CacheReq::ContainsKey(id, key)
            | CacheReq::GetAndRemove(id, key)
            | CacheReq::ClearKey(id, key)
            | CacheReq::RemoveKey(id, key) => {
                write_i32(writer, *id)?;
                write_u8(writer, MAGIC_BYTE)?;
                key.write(writer)?;
                Ok(())
            }
            CacheReq::GetAll(id, keys)
            | CacheReq::ContainsKeys(id, keys)
            | CacheReq::ClearKeys(id, keys)
            | CacheReq::RemoveKeys(id, keys) => {
                write_i32(writer, *id)?;
                write_u8(writer, MAGIC_BYTE)?;
                write_i32(writer, keys.len() as i32)?;
                for k in *keys {
                    k.write(writer)?;
                }
                Ok(())
            }
            CacheReq::Put(id, key, value)
            | CacheReq::GetAndPut(id, key, value)
            | CacheReq::GetAndReplace(id, key, value)
            | CacheReq::PutIfAbsent(id, key, value)
            | CacheReq::GetAndPutIfAbsent(id, key, value)
            | CacheReq::Replace(id, key, value)
            | CacheReq::RemoveIfEquals(id, key, value) => {
                write_i32(writer, *id)?;
                write_u8(writer, MAGIC_BYTE)?;
                key.write(writer)?;
                value.write(writer)?;
                Ok(())
            }
            CacheReq::PutAll(id, pairs) => {
                write_i32(writer, *id)?;
                write_u8(writer, MAGIC_BYTE)?;
                write_i32(writer, pairs.len() as i32)?;
                for pair in *pairs {
                    pair.0.write(writer)?;
                    pair.1.write(writer)?;
                }
                Ok(())
            }
            CacheReq::ReplaceIfEquals(id, key, old, new) => {
                write_i32(writer, *id)?;
                write_u8(writer, MAGIC_BYTE)?;
                key.write(writer)?;
                old.write(writer)?;
                new.write(writer)?;
                Ok(())
            }
            CacheReq::Clear(id) | CacheReq::RemoveAll(id) => {
                write_i32(writer, *id)?;
                write_u8(writer, MAGIC_BYTE)?;
                Ok(())
            }
            CacheReq::GetSize(id, modes) => {
                write_i32(writer, *id)?;
                write_u8(writer, MAGIC_BYTE)?;
                write_i32(writer, modes.len() as i32)?;
                for mode in modes {
                    write_u8(writer, mode.clone() as u8)?;
                }
                Ok(())
            }
            // https://ignite.apache.org/docs/latest/binary-client-protocol/sql-and-scan-queries#op_query_scan
            CacheReq::QueryScan(id, pg_sz) => {
                write_i32(writer, *id)?;
                write_u8(writer, 1u8)?; // 1 to keep the value in binary form
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
            CacheReq::QuerySqlFieldsArgs(id, schema, page_size, sql, args) => {
                // OP_QUERY_SQL_FIELDS per spec
                //Write cache info
                write_i32(writer, *id)?;
                write_u8(writer, 1u8)?; // flags: KEEP_BINARY
                                        // Write schema
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
            CacheReq::CursorGetPage(cursor_id, page_size) => {
                write_i64(writer, *cursor_id)?;
                write_i32(writer, *page_size)?;
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
            CacheReq::Get(_, key)
            | CacheReq::ContainsKey(_, key)
            | CacheReq::GetAndRemove(_, key)
            | CacheReq::ClearKey(_, key)
            | CacheReq::RemoveKey(_, key) => CACHE_ID_MAGIC_BYTE_SIZE + key.size(),
            CacheReq::GetAll(_, keys)
            | CacheReq::ContainsKeys(_, keys)
            | CacheReq::ClearKeys(_, keys)
            | CacheReq::RemoveKeys(_, keys) => {
                let mut size = CACHE_ID_MAGIC_BYTE_SIZE;
                size += 4; // len
                for k in *keys {
                    size += k.size();
                }
                size
            }
            CacheReq::Put(_, key, value)
            | CacheReq::GetAndPut(_, key, value)
            | CacheReq::GetAndReplace(_, key, value)
            | CacheReq::PutIfAbsent(_, key, value)
            | CacheReq::GetAndPutIfAbsent(_, key, value)
            | CacheReq::Replace(_, key, value)
            | CacheReq::RemoveIfEquals(_, key, value) => {
                CACHE_ID_MAGIC_BYTE_SIZE + key.size() + value.size()
            }
            CacheReq::PutAll(_, pairs) => {
                let mut size = CACHE_ID_MAGIC_BYTE_SIZE;
                size += 4; //len
                for pair in *pairs {
                    size += pair.0.size();
                    size += pair.1.size();
                }
                size
            }
            CacheReq::ReplaceIfEquals(_, key, old, new) => {
                CACHE_ID_MAGIC_BYTE_SIZE + key.size() + old.size() + new.size()
            }
            CacheReq::Clear(_) | CacheReq::RemoveAll(_) => CACHE_ID_MAGIC_BYTE_SIZE,
            CacheReq::GetSize(_, modes) => {
                let mut size = CACHE_ID_MAGIC_BYTE_SIZE;
                size += 4; //len
                for _ in modes {
                    size += 1;
                }
                size
            }
            CacheReq::QueryScan(_, _) => {
                CACHE_ID_MAGIC_BYTE_SIZE
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
            CacheReq::QuerySqlFieldsArgs(_, schema, _page_size, sql, args) => {
                let mut sz = 4 // cache id
                    + 1 // deprecated byte
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
            CacheReq::CursorGetPage(_, _) => size_of::<i64>() + size_of::<i32>(),
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

pub(crate) struct QueryScanResp<K: ReadableType, V: ReadableType> {
    pub(crate) val: Vec<(Option<K>, Option<V>)>,
}

impl<K: ReadableType, V: ReadableType> ReadableReq for QueryScanResp<K, V> {
    fn read(reader: &mut impl Read) -> IgniteResult<Self> {
        let _cursor_id = read_i64(reader)?;
        let count = read_i32(reader)?;
        let mut pairs: Vec<(Option<K>, Option<V>)> = Vec::new();
        for _ in 0..count {
            let key = K::read(reader)?;
            let val = V::read(reader)?;
            pairs.push((key, val));
        }
        let _more = read_bool(reader)?; // TODO: get more results
        Ok(QueryScanResp { val: pairs })
    }
}

#[cfg(test)]
pub(crate) struct CursorOpenResp<K: ReadableType, V: ReadableType> {
    pub(crate) cursor_id: i64,
    pub(crate) rows: Vec<(Option<K>, Option<V>)>,
    pub(crate) has_more: bool,
}

#[cfg(test)]
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

#[cfg(test)]
pub(crate) struct CursorPageResp<K: ReadableType, V: ReadableType> {
    pub(crate) rows: Vec<(Option<K>, Option<V>)>,
    pub(crate) has_more: bool,
}

// SqlFields open response; we return the first column as Long per row.
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

#[cfg(test)]
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
    use crate::protocol::TypeCode;
    use crate::protocol::{write_bool, write_i32, write_i64, write_string, write_u8};
    use std::io::Cursor;

    #[test]
    fn test_sql_fields_args_encoding_size_matches() {
        let sql = "SELECT _key.payload FROM \"my_cache\" WHERE _key.value = ?";
        let args = [IgniteValue::String("abc".to_string())];
        let req: CacheReq<'_, i32, i32> = CacheReq::QuerySqlFieldsArgs(42, None, 1000, sql, &args);
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
        let req: CacheReq<'_, i32, i32> = CacheReq::QuerySqlFieldsArgs(7, None, 1, sql, &args);
        let mut buf = Vec::new();
        req.write(&mut buf).unwrap();
        assert_eq!(req.size(), buf.len());
        assert_eq!(&buf[0..4], 7i32.to_le_bytes().as_slice());
        assert_eq!(buf[4], 1);
        assert_eq!(buf[5], crate::protocol::TypeCode::Null as u8);
    }

    #[test]
    fn test_cursor_get_page_encoding() {
        let req: CacheReq<'_, i32, i32> = CacheReq::CursorGetPage(42, 1000);
        let mut actual = Vec::new();
        req.write(&mut actual).unwrap();

        let mut expected = Vec::new();
        write_i64(&mut expected, 42).unwrap();
        write_i32(&mut expected, 1000).unwrap();

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
