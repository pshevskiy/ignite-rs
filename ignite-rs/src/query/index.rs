use crate::api::key_value::{cache_info_size, write_cache_info, CacheInfo};
use crate::protocol::complex_obj::IgniteValue;
use crate::protocol::{write_bool, write_i32, write_null, write_string_type_code};
use crate::{WritableType, WriteableReq};
use std::io::{self, Write};

/// Criterion type constants matching the Java protocol.
const CRITERION_TYPE_RANGE: u8 = 0;
const CRITERION_TYPE_IN: u8 = 1;

/// An index query criterion — either a range bound or an IN-list.
#[derive(Clone, Debug)]
pub enum IndexQueryCriterion {
    /// Range criterion: field_name, lower bound (inclusive?), upper bound (inclusive?).
    Range {
        field_name: String,
        lower: Option<IgniteValue>,
        lower_inclusive: bool,
        upper: Option<IgniteValue>,
        upper_inclusive: bool,
    },
    /// IN-list criterion: field_name IN (values...).
    In {
        field_name: String,
        values: Vec<IgniteValue>,
    },
}

impl IndexQueryCriterion {
    /// Create a range criterion with inclusive bounds.
    pub fn range(
        field_name: impl Into<String>,
        lower: Option<IgniteValue>,
        upper: Option<IgniteValue>,
    ) -> Self {
        Self::Range {
            field_name: field_name.into(),
            lower,
            lower_inclusive: true,
            upper,
            upper_inclusive: true,
        }
    }

    /// Create a range criterion with explicit inclusivity.
    pub fn range_with_bounds(
        field_name: impl Into<String>,
        lower: Option<IgniteValue>,
        lower_inclusive: bool,
        upper: Option<IgniteValue>,
        upper_inclusive: bool,
    ) -> Self {
        Self::Range {
            field_name: field_name.into(),
            lower,
            lower_inclusive,
            upper,
            upper_inclusive,
        }
    }

    /// Create an IN-list criterion.
    pub fn in_list(field_name: impl Into<String>, values: Vec<IgniteValue>) -> Self {
        Self::In {
            field_name: field_name.into(),
            values,
        }
    }

    /// Create a "greater than or equal" criterion.
    pub fn gte(field_name: impl Into<String>, value: IgniteValue) -> Self {
        Self::Range {
            field_name: field_name.into(),
            lower: Some(value),
            lower_inclusive: true,
            upper: None,
            upper_inclusive: true,
        }
    }

    /// Create a "less than or equal" criterion.
    pub fn lte(field_name: impl Into<String>, value: IgniteValue) -> Self {
        Self::Range {
            field_name: field_name.into(),
            lower: None,
            lower_inclusive: true,
            upper: Some(value),
            upper_inclusive: true,
        }
    }

    /// Create an "equals" criterion (range where lower == upper, both inclusive).
    pub fn eq(field_name: impl Into<String>, value: IgniteValue) -> Self {
        Self::Range {
            field_name: field_name.into(),
            lower: Some(value.clone()),
            lower_inclusive: true,
            upper: Some(value),
            upper_inclusive: true,
        }
    }

    fn write_criterion(&self, writer: &mut dyn Write) -> io::Result<()> {
        match self {
            IndexQueryCriterion::Range {
                field_name,
                lower,
                lower_inclusive,
                upper,
                upper_inclusive,
            } => {
                crate::protocol::write_u8(writer, CRITERION_TYPE_RANGE)?;
                // FND-036: Java writes `range.field()` as a typed string via
                // `w.writeString(range.field())` in `TcpClientCache.indexQuery`.
                write_string_type_code(writer, field_name)?;
                write_bool(writer, *lower_inclusive)?;
                write_bool(writer, *upper_inclusive)?;
                let lower_null = lower.is_none();
                let upper_null = upper.is_none();
                write_bool(writer, lower_null)?;
                write_bool(writer, upper_null)?;
                match lower {
                    Some(val) => val.write(writer)?,
                    None => write_null(writer)?,
                }
                match upper {
                    Some(val) => val.write(writer)?,
                    None => write_null(writer)?,
                }
            }
            IndexQueryCriterion::In { field_name, values } => {
                crate::protocol::write_u8(writer, CRITERION_TYPE_IN)?;
                // FND-036: typed string for field name, same as Range branch.
                write_string_type_code(writer, field_name)?;
                write_i32(writer, values.len() as i32)?;
                for val in values {
                    val.write(writer)?;
                }
            }
        }
        Ok(())
    }

    fn size_criterion(&self) -> usize {
        match self {
            IndexQueryCriterion::Range {
                field_name,
                lower,
                upper,
                ..
            } => {
                1 // criterion type
                + 1 + 4 + field_name.len() // typed string: code + len + bytes
                + 1 + 1 // lower_inclusive, upper_inclusive
                + 1 + 1 // lower_null, upper_null
                + match lower { Some(val) => val.size(), None => 1 } // lower bound or null
                + match upper { Some(val) => val.size(), None => 1 } // upper bound or null
            }
            IndexQueryCriterion::In { field_name, values } => {
                1 // criterion type
                + 1 + 4 + field_name.len() // typed string
                + 4 // value count
                + values.iter().map(|v| v.size()).sum::<usize>()
            }
        }
    }
}

/// Index query that queries directly against cache indexes.
#[derive(Clone, Debug)]
pub struct IndexQuery {
    value_type: String,
    index_name: Option<String>,
    criteria: Vec<IndexQueryCriterion>,
    page_size: i32,
    partition: Option<i32>,
    local: bool,
    limit: Option<i32>,
}

impl IndexQuery {
    /// Create a new index query for the given value type name.
    pub fn new(value_type: impl Into<String>) -> Self {
        Self {
            value_type: value_type.into(),
            index_name: None,
            criteria: Vec::new(),
            page_size: 1024,
            partition: None,
            local: false,
            limit: None,
        }
    }

    pub fn with_index_name(mut self, index_name: impl Into<String>) -> Self {
        self.index_name = Some(index_name.into());
        self
    }

    pub fn with_criterion(mut self, criterion: IndexQueryCriterion) -> Self {
        self.criteria.push(criterion);
        self
    }

    pub fn with_criteria(mut self, criteria: Vec<IndexQueryCriterion>) -> Self {
        self.criteria = criteria;
        self
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

    pub fn with_limit(mut self, limit: i32) -> Self {
        self.limit = Some(limit);
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

pub(crate) struct IndexQueryRequest<'a> {
    pub(crate) cache_info: CacheInfo,
    pub(crate) query: &'a IndexQuery,
}

impl<'a> WriteableReq for IndexQueryRequest<'a> {
    fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        write_cache_info(writer, self.cache_info.with_keep_binary(true))?;
        write_i32(writer, self.query.page_size)?;
        write_bool(writer, self.query.local)?;
        write_i32(writer, self.query.partition.unwrap_or(-1))?;

        // limit (optional, written unconditionally for simplicity — server ignores if unsupported)
        write_i32(writer, self.query.limit.unwrap_or(0))?;

        // FND-033: Java writes `valueType` via `BinaryWriterExImpl.writeString`,
        // which emits `[STRING_CODE=9, i32 len, bytes]`. The server decodes the
        // leading byte as a TypeCode, so the raw-length form misaligns the frame.
        write_string_type_code(writer, &self.query.value_type)?;

        // FND-033: `indexName` is nullable in Java; null → single NULL type-code
        // byte, non-null → typed string.
        match &self.query.index_name {
            Some(name) => write_string_type_code(writer, name)?,
            None => write_null(writer)?,
        }

        // criteria
        if self.query.criteria.is_empty() {
            write_null(writer)?;
        } else {
            // Write as collection type marker + array
            crate::protocol::write_u8(writer, crate::protocol::TypeCode::Collection as u8)?;
            write_i32(writer, self.query.criteria.len() as i32)?;
            for criterion in &self.query.criteria {
                criterion.write_criterion(writer)?;
            }
        }

        // filter object (null — no client-side filter support from thin client)
        write_null(writer)?;

        Ok(())
    }

    fn size(&self) -> usize {
        let cache_info_sz = cache_info_size(self.cache_info.with_keep_binary(true));
        let page_size_sz = 4; // page_size
        let local_sz = 1; // local
        let partition_sz = 4; // partition
        let limit_sz = 4; // limit

        // Typed string: 1-byte type code + i32 length + bytes.
        let value_type_sz = 1 + 4 + self.query.value_type.len();
        let index_name_sz = match &self.query.index_name {
            Some(name) => 1 + 4 + name.len(),
            None => 1, // NULL type code
        };

        let criteria_sz = if self.query.criteria.is_empty() {
            1 // null
        } else {
            1 // collection type code
            + 4 // count
            + self.query.criteria.iter().map(|c| c.size_criterion()).sum::<usize>()
        };

        let filter_sz = 1; // null

        cache_info_sz
            + page_size_sz
            + local_sz
            + partition_sz
            + limit_sz
            + value_type_sz
            + index_name_sz
            + criteria_sz
            + filter_sz
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::key_value::CacheInfo;
    use crate::protocol::complex_obj::IgniteValue;
    use crate::protocol::TypeCode;

    const TYPE_CODE_STRING: u8 = TypeCode::String as u8;
    const TYPE_CODE_NULL: u8 = TypeCode::Null as u8;

    /// FND-033: Java `w.writeString(valueType)` emits `[STRING_CODE, i32 len, bytes]`.
    #[test]
    fn value_type_is_typed_string() {
        let query = IndexQuery::new("Person");
        let req = IndexQueryRequest {
            cache_info: CacheInfo::new(1),
            query: &query,
        };
        let mut buf = Vec::new();
        req.write(&mut buf).unwrap();

        // cache_info (4+1) + page_size (4) + local (1) + partition (4) + limit (4) = 18
        let offset = 18;
        assert_eq!(buf[offset], TYPE_CODE_STRING, "valueType missing STRING type code");
        let len = i32::from_le_bytes(buf[offset + 1..offset + 5].try_into().unwrap());
        assert_eq!(len as usize, "Person".len());
        assert_eq!(&buf[offset + 5..offset + 5 + "Person".len()], b"Person");
    }

    /// FND-033: `indexName` when Some is a typed string.
    #[test]
    fn index_name_some_is_typed_string() {
        let query = IndexQuery::new("Person").with_index_name("idx_name");
        let req = IndexQueryRequest {
            cache_info: CacheInfo::new(1),
            query: &query,
        };
        let mut buf = Vec::new();
        req.write(&mut buf).unwrap();

        // 18 + typed-string valueType (1+4+6) = 29
        let offset = 18 + 1 + 4 + "Person".len();
        assert_eq!(buf[offset], TYPE_CODE_STRING);
        let len = i32::from_le_bytes(buf[offset + 1..offset + 5].try_into().unwrap());
        assert_eq!(len as usize, "idx_name".len());
    }

    /// FND-036: Range criterion `field_name` is a typed string.
    #[test]
    fn range_criterion_field_name_is_typed_string() {
        let query = IndexQuery::new("Person").with_criterion(IndexQueryCriterion::eq(
            "age",
            IgniteValue::Int(42),
        ));
        let req = IndexQueryRequest {
            cache_info: CacheInfo::new(1),
            query: &query,
        };
        let mut buf = Vec::new();
        req.write(&mut buf).unwrap();

        // Layout so far: cache_info(5) + page_size(4) + local(1) + partition(4) + limit(4) = 18
        // + typed valueType (1+4+6) = 29, + indexName NULL (1) = 30
        // + criteria collection marker (1) + count (4) = 35
        // → criterion byte at 35 (type=0 for Range), field_name typed string at 36.
        assert_eq!(buf[35], 0u8, "Range criterion type");
        assert_eq!(buf[36], TYPE_CODE_STRING, "field_name must be typed string");
        let len = i32::from_le_bytes(buf[37..41].try_into().unwrap());
        assert_eq!(len as usize, "age".len());
        assert_eq!(&buf[41..41 + "age".len()], b"age");
    }

    /// FND-036: In-list criterion `field_name` is a typed string.
    #[test]
    fn in_list_criterion_field_name_is_typed_string() {
        let query = IndexQuery::new("Person").with_criterion(IndexQueryCriterion::in_list(
            "city",
            vec![IgniteValue::String("NYC".to_string())],
        ));
        let req = IndexQueryRequest {
            cache_info: CacheInfo::new(1),
            query: &query,
        };
        let mut buf = Vec::new();
        req.write(&mut buf).unwrap();

        // Criterion byte at offset 35 (see above), field_name starts at 36.
        assert_eq!(buf[35], 1u8, "In-list criterion type");
        assert_eq!(buf[36], TYPE_CODE_STRING, "field_name must be typed string");
    }

    /// FND-033: `indexName` when None is a single NULL type code byte.
    #[test]
    fn index_name_none_is_null_type_code() {
        let query = IndexQuery::new("Person");
        let req = IndexQueryRequest {
            cache_info: CacheInfo::new(1),
            query: &query,
        };
        let mut buf = Vec::new();
        req.write(&mut buf).unwrap();

        let offset = 18 + 1 + 4 + "Person".len();
        assert_eq!(buf[offset], TYPE_CODE_NULL);
    }

    #[test]
    fn should_encode_basic_index_query() {
        let query = IndexQuery::new("com.example.Person")
            .with_index_name("idx_name")
            .with_page_size(100)
            .with_local(false);

        let req = IndexQueryRequest {
            cache_info: CacheInfo::new(42),
            query: &query,
        };

        let mut buf = Vec::new();
        req.write(&mut buf).unwrap();
        assert_eq!(req.size(), buf.len());
    }

    #[test]
    fn should_encode_index_query_with_range_criterion() {
        let query = IndexQuery::new("Person")
            .with_criterion(IndexQueryCriterion::range(
                "age",
                Some(IgniteValue::Int(18)),
                Some(IgniteValue::Int(65)),
            ))
            .with_page_size(50);

        let req = IndexQueryRequest {
            cache_info: CacheInfo::new(7),
            query: &query,
        };

        let mut buf = Vec::new();
        req.write(&mut buf).unwrap();
        assert_eq!(req.size(), buf.len());
    }

    #[test]
    fn should_encode_index_query_with_in_criterion() {
        let query = IndexQuery::new("Person").with_criterion(IndexQueryCriterion::in_list(
            "city",
            vec![
                IgniteValue::String("NYC".to_string()),
                IgniteValue::String("LA".to_string()),
            ],
        ));

        let req = IndexQueryRequest {
            cache_info: CacheInfo::new(7),
            query: &query,
        };

        let mut buf = Vec::new();
        req.write(&mut buf).unwrap();
        assert_eq!(req.size(), buf.len());
    }

    #[test]
    fn should_encode_index_query_without_index_name() {
        let query = IndexQuery::new("Person");

        let req = IndexQueryRequest {
            cache_info: CacheInfo::new(1),
            query: &query,
        };

        let mut buf = Vec::new();
        req.write(&mut buf).unwrap();
        assert_eq!(req.size(), buf.len());
    }
}
