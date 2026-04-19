use crate::api::key_value::{cache_info_size, write_cache_info, CacheInfo};
use crate::error::{IgniteError, IgniteResult};
use crate::protocol::complex_obj::{ComplexObject, IgniteValue};
use crate::protocol::{
    read_bool, read_enum, read_f32, read_f64, read_i16, read_i32, read_i64, read_i8,
    read_primitive_arr, read_string, read_u16, read_u8, write_bool, write_i32, write_i64,
    write_string_type_code, write_u8, TypeCode,
};
use crate::transport::SqlFieldsCapabilities;
use crate::{Enum, ReadableReq, ReadableType, WritableType, WriteableReq};
use std::convert::TryFrom;
use std::io::{self, Cursor, Read, Write};
use std::marker::PhantomData;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlDecimal {
    pub scale: i32,
    pub magnitude: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlUuid {
    pub most: i64,
    pub least: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlDate {
    pub millis: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlTimestamp {
    pub millis: i64,
    pub nanos: i32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SqlTime {
    pub millis: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub enum SqlValue {
    Null,
    Byte(u8),
    Short(i16),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    Char(u16),
    Bool(bool),
    String(String),
    Decimal(SqlDecimal),
    Uuid(SqlUuid),
    Date(SqlDate),
    Timestamp(SqlTimestamp),
    Time(SqlTime),
    Binary(Vec<u8>),
    Array(Vec<SqlValue>),
    Collection(Vec<SqlValue>),
    Map(Vec<(SqlValue, SqlValue)>),
    Enum(Enum),
    ComplexObject(ComplexObject),
}

pub trait SqlField: Sized {
    fn from_sql_value(value: SqlValue) -> IgniteResult<Self>;
}

pub trait SqlRow: Sized {
    fn from_sql_values(values: Vec<SqlValue>) -> IgniteResult<Self>;
}

impl SqlField for SqlValue {
    fn from_sql_value(value: SqlValue) -> IgniteResult<Self> {
        Ok(value)
    }
}

impl SqlField for u8 {
    fn from_sql_value(value: SqlValue) -> IgniteResult<Self> {
        match value {
            SqlValue::Byte(value) => Ok(value),
            other => Err(sql_type_error("u8", &other)),
        }
    }
}

impl SqlField for i16 {
    fn from_sql_value(value: SqlValue) -> IgniteResult<Self> {
        match value {
            SqlValue::Byte(value) => Ok(value as i16),
            SqlValue::Short(value) => Ok(value),
            other => Err(sql_type_error("i16", &other)),
        }
    }
}

impl SqlField for i32 {
    fn from_sql_value(value: SqlValue) -> IgniteResult<Self> {
        match value {
            SqlValue::Byte(value) => Ok(value as i32),
            SqlValue::Short(value) => Ok(value as i32),
            SqlValue::Int(value) => Ok(value),
            other => Err(sql_type_error("i32", &other)),
        }
    }
}

impl SqlField for i64 {
    fn from_sql_value(value: SqlValue) -> IgniteResult<Self> {
        match value {
            SqlValue::Byte(value) => Ok(value as i64),
            SqlValue::Short(value) => Ok(value as i64),
            SqlValue::Int(value) => Ok(value as i64),
            SqlValue::Long(value) => Ok(value),
            other => Err(sql_type_error("i64", &other)),
        }
    }
}

impl SqlField for f32 {
    fn from_sql_value(value: SqlValue) -> IgniteResult<Self> {
        match value {
            SqlValue::Float(value) => Ok(value),
            other => Err(sql_type_error("f32", &other)),
        }
    }
}

impl SqlField for f64 {
    fn from_sql_value(value: SqlValue) -> IgniteResult<Self> {
        match value {
            SqlValue::Float(value) => Ok(value as f64),
            SqlValue::Double(value) => Ok(value),
            other => Err(sql_type_error("f64", &other)),
        }
    }
}

impl SqlField for u16 {
    fn from_sql_value(value: SqlValue) -> IgniteResult<Self> {
        match value {
            SqlValue::Char(value) => Ok(value),
            other => Err(sql_type_error("u16", &other)),
        }
    }
}

impl SqlField for bool {
    fn from_sql_value(value: SqlValue) -> IgniteResult<Self> {
        match value {
            SqlValue::Bool(value) => Ok(value),
            other => Err(sql_type_error("bool", &other)),
        }
    }
}

impl SqlField for String {
    fn from_sql_value(value: SqlValue) -> IgniteResult<Self> {
        match value {
            SqlValue::String(value) => Ok(value),
            SqlValue::Char(value) => char::decode_utf16(std::iter::once(value))
                .next()
                .expect("decode_utf16 yielded no item")
                .map(|value| value.to_string())
                .map_err(|err| IgniteError::new(format!("Cannot decode UTF-16 char: {err}"))),
            other => Err(sql_type_error("String", &other)),
        }
    }
}

impl SqlField for Vec<u8> {
    fn from_sql_value(value: SqlValue) -> IgniteResult<Self> {
        match value {
            SqlValue::Binary(value) => Ok(value),
            other => Err(sql_type_error("Vec<u8>", &other)),
        }
    }
}

impl SqlField for SqlDecimal {
    fn from_sql_value(value: SqlValue) -> IgniteResult<Self> {
        match value {
            SqlValue::Decimal(value) => Ok(value),
            other => Err(sql_type_error("SqlDecimal", &other)),
        }
    }
}

impl SqlField for SqlUuid {
    fn from_sql_value(value: SqlValue) -> IgniteResult<Self> {
        match value {
            SqlValue::Uuid(value) => Ok(value),
            other => Err(sql_type_error("SqlUuid", &other)),
        }
    }
}

impl SqlField for SqlDate {
    fn from_sql_value(value: SqlValue) -> IgniteResult<Self> {
        match value {
            SqlValue::Date(value) => Ok(value),
            other => Err(sql_type_error("SqlDate", &other)),
        }
    }
}

impl SqlField for SqlTimestamp {
    fn from_sql_value(value: SqlValue) -> IgniteResult<Self> {
        match value {
            SqlValue::Timestamp(value) => Ok(value),
            other => Err(sql_type_error("SqlTimestamp", &other)),
        }
    }
}

impl SqlField for SqlTime {
    fn from_sql_value(value: SqlValue) -> IgniteResult<Self> {
        match value {
            SqlValue::Time(value) => Ok(value),
            other => Err(sql_type_error("SqlTime", &other)),
        }
    }
}

impl SqlField for Enum {
    fn from_sql_value(value: SqlValue) -> IgniteResult<Self> {
        match value {
            SqlValue::Enum(value) => Ok(value),
            other => Err(sql_type_error("Enum", &other)),
        }
    }
}

impl SqlField for ComplexObject {
    fn from_sql_value(value: SqlValue) -> IgniteResult<Self> {
        match value {
            SqlValue::ComplexObject(value) => Ok(value),
            other => Err(sql_type_error("ComplexObject", &other)),
        }
    }
}

impl<T: SqlField> SqlField for Option<T> {
    fn from_sql_value(value: SqlValue) -> IgniteResult<Self> {
        match value {
            SqlValue::Null => Ok(None),
            other => T::from_sql_value(other).map(Some),
        }
    }
}

impl<T: SqlField> SqlRow for T {
    fn from_sql_values(mut values: Vec<SqlValue>) -> IgniteResult<Self> {
        if values.len() != 1 {
            return Err(IgniteError::new(format!(
                "Expected exactly one SQL column, got {}",
                values.len()
            )));
        }

        T::from_sql_value(values.remove(0))
    }
}

impl SqlRow for Vec<SqlValue> {
    fn from_sql_values(values: Vec<SqlValue>) -> IgniteResult<Self> {
        Ok(values)
    }
}

macro_rules! impl_sql_row_tuple {
    ($($name:ident),+ $(,)?) => {
        #[allow(non_snake_case)]
        impl<$($name: SqlField),+> SqlRow for ($($name,)+) {
            fn from_sql_values(values: Vec<SqlValue>) -> IgniteResult<Self> {
                let mut iter = values.into_iter();
                $(
                    let $name = <$name as SqlField>::from_sql_value(
                        iter.next().ok_or_else(|| {
                            IgniteError::new("SQL row has fewer columns than expected")
                        })?
                    )?;
                )+

                if iter.next().is_some() {
                    return Err(IgniteError::from("SQL row has more columns than expected"));
                }

                Ok(($($name,)+))
            }
        }
    };
}

impl_sql_row_tuple!(A, B);
impl_sql_row_tuple!(A, B, C);
impl_sql_row_tuple!(A, B, C, D);
impl_sql_row_tuple!(A, B, C, D, E);
impl_sql_row_tuple!(A, B, C, D, E, F);

#[derive(Clone, Debug)]
pub struct SqlFieldsQuery<Row = Vec<SqlValue>> {
    schema: Option<String>,
    sql: String,
    page_size: i32,
    args: Vec<IgniteValue>,
    distributed_joins: bool,
    local: bool,
    replicated_only: bool,
    enforce_join_order: bool,
    collocated: bool,
    lazy: bool,
    timeout_ms: i64,
    include_field_names: bool,
    update_batch_size: i32,
    partitions: Option<Vec<i32>>,
    query_initiator_id: Option<String>,
    _row: PhantomData<Row>,
}

impl<Row> SqlFieldsQuery<Row> {
    pub fn new(sql: &str) -> Self {
        Self {
            schema: None,
            sql: sql.to_string(),
            page_size: 1024,
            args: Vec::new(),
            distributed_joins: false,
            local: false,
            replicated_only: false,
            enforce_join_order: false,
            collocated: false,
            lazy: false,
            timeout_ms: 500,
            include_field_names: true,
            update_batch_size: 1,
            partitions: None,
            query_initiator_id: None,
            _row: PhantomData,
        }
    }

    pub fn with_schema(mut self, schema: &str) -> Self {
        self.schema = Some(schema.to_string());
        self
    }

    pub fn with_page_size(mut self, page_size: i32) -> Self {
        self.page_size = page_size;
        self
    }

    pub fn with_args(mut self, args: Vec<IgniteValue>) -> Self {
        self.args = args;
        self
    }

    pub fn with_timeout_ms(mut self, timeout_ms: i64) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }

    pub fn with_update_batch_size(mut self, update_batch_size: i32) -> Self {
        self.update_batch_size = update_batch_size;
        self
    }

    pub fn with_partitions<I>(mut self, partitions: I) -> Self
    where
        I: IntoIterator<Item = i32>,
    {
        self.partitions = Some(partitions.into_iter().collect());
        self
    }

    pub fn with_query_initiator_id(mut self, query_initiator_id: impl Into<String>) -> Self {
        self.query_initiator_id = Some(query_initiator_id.into());
        self
    }

    pub fn with_local(mut self, local: bool) -> Self {
        self.local = local;
        self
    }

    pub fn with_distributed_joins(mut self, distributed_joins: bool) -> Self {
        self.distributed_joins = distributed_joins;
        self
    }

    pub fn with_replicated_only(mut self, replicated_only: bool) -> Self {
        self.replicated_only = replicated_only;
        self
    }

    pub fn with_enforce_join_order(mut self, enforce_join_order: bool) -> Self {
        self.enforce_join_order = enforce_join_order;
        self
    }

    pub fn with_collocated(mut self, collocated: bool) -> Self {
        self.collocated = collocated;
        self
    }

    pub fn with_lazy(mut self, lazy: bool) -> Self {
        self.lazy = lazy;
        self
    }

    pub fn into_row<T>(self) -> SqlFieldsQuery<T> {
        SqlFieldsQuery {
            schema: self.schema,
            sql: self.sql,
            page_size: self.page_size,
            args: self.args,
            distributed_joins: self.distributed_joins,
            local: self.local,
            replicated_only: self.replicated_only,
            enforce_join_order: self.enforce_join_order,
            collocated: self.collocated,
            lazy: self.lazy,
            timeout_ms: self.timeout_ms,
            include_field_names: self.include_field_names,
            update_batch_size: self.update_batch_size,
            partitions: self.partitions,
            query_initiator_id: self.query_initiator_id,
            _row: PhantomData,
        }
    }

    pub fn page_size(&self) -> i32 {
        self.page_size
    }

    pub(crate) fn schema(&self) -> Option<&str> {
        self.schema.as_deref()
    }

    pub(crate) fn sql(&self) -> &str {
        &self.sql
    }

    pub(crate) fn args(&self) -> &[IgniteValue] {
        &self.args
    }

    pub(crate) fn distributed_joins(&self) -> bool {
        self.distributed_joins
    }

    pub(crate) fn local(&self) -> bool {
        self.local
    }

    pub(crate) fn replicated_only(&self) -> bool {
        self.replicated_only
    }

    pub(crate) fn enforce_join_order(&self) -> bool {
        self.enforce_join_order
    }

    pub(crate) fn collocated(&self) -> bool {
        self.collocated
    }

    pub(crate) fn lazy(&self) -> bool {
        self.lazy
    }

    pub(crate) fn timeout_ms(&self) -> i64 {
        self.timeout_ms
    }

    pub(crate) fn include_field_names(&self) -> bool {
        self.include_field_names
    }

    pub(crate) fn update_batch_size(&self) -> i32 {
        self.update_batch_size
    }

    pub(crate) fn partitions(&self) -> Option<&[i32]> {
        self.partitions.as_deref()
    }

    pub(crate) fn query_initiator_id(&self) -> Option<&str> {
        self.query_initiator_id.as_deref()
    }

    pub(crate) fn validate(&self) -> IgniteResult<()> {
        if self.sql.trim().is_empty() {
            return Err(IgniteError::from("Failed to parse SQL"));
        }

        if self.update_batch_size < 1 {
            return Err(IgniteError::from("updateBatchSize cannot be lower than 1"));
        }

        if let Some(partitions) = &self.partitions {
            if partitions.iter().any(|partition| *partition < 0) {
                return Err(IgniteError::from("Illegal partition"));
            }
        }

        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct SqlQuery<K, V> {
    type_name: String,
    sql: String,
    args: Vec<IgniteValue>,
    distributed_joins: bool,
    local: bool,
    replicated_only: bool,
    page_size: i32,
    timeout_ms: i64,
    _key: PhantomData<K>,
    _value: PhantomData<V>,
}

impl<K, V> SqlQuery<K, V> {
    pub fn new(type_name: &str, sql: &str) -> Self {
        Self {
            type_name: type_name.to_string(),
            sql: sql.to_string(),
            args: Vec::new(),
            distributed_joins: false,
            local: false,
            replicated_only: false,
            page_size: 1024,
            timeout_ms: 0,
            _key: PhantomData,
            _value: PhantomData,
        }
    }

    pub fn with_args(mut self, args: Vec<IgniteValue>) -> Self {
        self.args = args;
        self
    }

    pub fn with_page_size(mut self, page_size: i32) -> Self {
        self.page_size = page_size;
        self
    }

    pub fn with_timeout_ms(mut self, timeout_ms: i64) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }

    pub fn with_local(mut self, local: bool) -> Self {
        self.local = local;
        self
    }

    pub fn with_distributed_joins(mut self, distributed_joins: bool) -> Self {
        self.distributed_joins = distributed_joins;
        self
    }

    pub fn with_replicated_only(mut self, replicated_only: bool) -> Self {
        self.replicated_only = replicated_only;
        self
    }

    pub fn into_types<K2, V2>(self) -> SqlQuery<K2, V2> {
        SqlQuery {
            type_name: self.type_name,
            sql: self.sql,
            args: self.args,
            distributed_joins: self.distributed_joins,
            local: self.local,
            replicated_only: self.replicated_only,
            page_size: self.page_size,
            timeout_ms: self.timeout_ms,
            _key: PhantomData,
            _value: PhantomData,
        }
    }

    pub fn page_size(&self) -> i32 {
        self.page_size
    }

    pub(crate) fn type_name(&self) -> &str {
        &self.type_name
    }

    pub(crate) fn sql(&self) -> &str {
        &self.sql
    }

    pub(crate) fn args(&self) -> &[IgniteValue] {
        &self.args
    }

    pub(crate) fn distributed_joins(&self) -> bool {
        self.distributed_joins
    }

    pub(crate) fn local(&self) -> bool {
        self.local
    }

    pub(crate) fn replicated_only(&self) -> bool {
        self.replicated_only
    }

    pub(crate) fn timeout_ms(&self) -> i64 {
        self.timeout_ms
    }
}

pub(crate) struct SqlFieldsQueryRequest<'a, Row> {
    pub(crate) cache_info: CacheInfo,
    pub(crate) query: &'a SqlFieldsQuery<Row>,
    pub(crate) capabilities: SqlFieldsCapabilities,
}

impl<Row> WriteableReq for SqlFieldsQueryRequest<'_, Row> {
    fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        write_cache_info(writer, self.cache_info.with_keep_binary(true))?;
        match self.query.schema() {
            Some(schema) => write_string_type_code(writer, schema)?,
            None => write_u8(writer, TypeCode::Null as u8)?,
        }
        write_i32(writer, self.query.page_size())?;
        write_i32(writer, -1)?;
        write_string_type_code(writer, self.query.sql())?;
        write_i32(writer, self.query.args().len() as i32)?;
        for arg in self.query.args() {
            arg.write(writer)?;
        }
        write_u8(writer, 0u8)?;
        write_bool(writer, self.query.distributed_joins())?;
        write_bool(writer, self.query.local())?;
        write_bool(writer, self.query.replicated_only())?;
        write_bool(writer, self.query.enforce_join_order())?;
        write_bool(writer, self.query.collocated())?;
        write_bool(writer, self.query.lazy())?;
        write_i64(writer, self.query.timeout_ms())?;
        write_bool(writer, self.query.include_field_names())?;
        if self.capabilities.partitions_batch_size {
            match self.query.partitions() {
                Some(partitions) => {
                    write_i32(writer, partitions.len() as i32)?;
                    for partition in partitions {
                        write_i32(writer, *partition)?;
                    }
                }
                None => write_i32(writer, -1)?,
            }
            write_i32(writer, self.query.update_batch_size())?;
        }
        if self.capabilities.query_initiator_id {
            match self.query.query_initiator_id() {
                Some(initiator_id) => write_string_type_code(writer, initiator_id)?,
                None => write_u8(writer, TypeCode::Null as u8)?,
            }
        }
        Ok(())
    }

    fn size(&self) -> usize {
        let mut size = cache_info_size(self.cache_info.with_keep_binary(true));
        size += match self.query.schema() {
            Some(schema) => 1 + 4 + schema.len(),
            None => 1,
        };
        size += 4 + 4;
        size += 1 + 4 + self.query.sql().len();
        size += 4;
        for arg in self.query.args() {
            size += arg.size();
        }
        size += 1 + 6 + 8 + 1;
        if self.capabilities.partitions_batch_size {
            size += 4;
            if let Some(partitions) = self.query.partitions() {
                size += partitions.len() * 4;
            }
            size += 4;
        }
        if self.capabilities.query_initiator_id {
            size += match self.query.query_initiator_id() {
                Some(initiator_id) => 1 + 4 + initiator_id.len(),
                None => 1,
            };
        }
        size
    }
}

pub(crate) struct SqlQueryRequest<'a, K, V> {
    pub(crate) cache_info: CacheInfo,
    pub(crate) query: &'a SqlQuery<K, V>,
}

impl<K, V> WriteableReq for SqlQueryRequest<'_, K, V> {
    fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        write_cache_info(writer, self.cache_info.with_keep_binary(true))?;
        write_string_type_code(writer, self.query.type_name())?;
        write_string_type_code(writer, self.query.sql())?;
        write_i32(writer, self.query.args().len() as i32)?;
        for arg in self.query.args() {
            arg.write(writer)?;
        }
        write_bool(writer, self.query.distributed_joins())?;
        write_bool(writer, self.query.local())?;
        write_bool(writer, self.query.replicated_only())?;
        write_i32(writer, self.query.page_size())?;
        write_i64(writer, self.query.timeout_ms())?;
        Ok(())
    }

    fn size(&self) -> usize {
        let mut size = cache_info_size(self.cache_info.with_keep_binary(true));
        size += 1 + 4 + self.query.type_name().len();
        size += 1 + 4 + self.query.sql().len();
        size += 4;
        for arg in self.query.args() {
            size += arg.size();
        }
        size + 1 + 1 + 1 + 4 + 8
    }
}

pub(crate) struct SqlFieldsOpenResponse {
    pub(crate) cursor_id: i64,
    pub(crate) field_names: Vec<String>,
    pub(crate) rows: Vec<Vec<SqlValue>>,
    pub(crate) has_more: bool,
}

impl ReadableReq for SqlFieldsOpenResponse {
    fn read(reader: &mut impl Read) -> IgniteResult<Self> {
        let cursor_id = read_i64(reader).map_err(IgniteError::from)?;
        let column_count = read_i32(reader).map_err(IgniteError::from)? as usize;
        let mut field_names = Vec::with_capacity(column_count);

        for _ in 0..column_count {
            let field_name = String::read(reader)?.ok_or_else(|| {
                IgniteError::from("SQL fields response contained null field name")
            })?;
            field_names.push(field_name);
        }

        let row_count = read_i32(reader).map_err(IgniteError::from)? as usize;
        let mut rows = Vec::with_capacity(row_count);
        for _ in 0..row_count {
            rows.push(read_sql_row(reader, column_count)?);
        }

        let has_more = read_bool(reader).map_err(IgniteError::from)?;
        Ok(Self {
            cursor_id,
            field_names,
            rows,
            has_more,
        })
    }
}

pub(crate) struct RawPageBody {
    pub(crate) body: Vec<u8>,
}

impl ReadableReq for RawPageBody {
    fn read(reader: &mut impl Read) -> IgniteResult<Self> {
        let mut body = Vec::new();
        reader.read_to_end(&mut body).map_err(IgniteError::from)?;
        Ok(Self { body })
    }
}

pub(crate) fn read_sql_fields_page(
    body: &[u8],
    column_count: usize,
) -> IgniteResult<(Vec<Vec<SqlValue>>, bool)> {
    let mut reader = Cursor::new(body);
    let row_count = read_i32(&mut reader).map_err(IgniteError::from)? as usize;
    let mut rows = Vec::with_capacity(row_count);
    for _ in 0..row_count {
        rows.push(read_sql_row(&mut reader, column_count)?);
    }
    let has_more = read_bool(&mut reader).map_err(IgniteError::from)?;
    Ok((rows, has_more))
}

pub(crate) fn read_sql_row(
    reader: &mut impl Read,
    column_count: usize,
) -> IgniteResult<Vec<SqlValue>> {
    let mut row = Vec::with_capacity(column_count);
    for _ in 0..column_count {
        row.push(read_sql_value(reader)?);
    }
    Ok(row)
}

pub(crate) fn read_sql_value(reader: &mut impl Read) -> IgniteResult<SqlValue> {
    let type_code = TypeCode::try_from(read_u8(reader).map_err(IgniteError::from)?)
        .map_err(IgniteError::from)?;
    read_sql_value_unwrapped(type_code, reader)
}

pub(crate) fn read_sql_value_unwrapped(
    type_code: TypeCode,
    reader: &mut impl Read,
) -> IgniteResult<SqlValue> {
    match type_code {
        TypeCode::Null => Ok(SqlValue::Null),
        TypeCode::Byte => read_u8(reader)
            .map(SqlValue::Byte)
            .map_err(IgniteError::from),
        TypeCode::Short => read_i16(reader)
            .map(SqlValue::Short)
            .map_err(IgniteError::from),
        TypeCode::Int => read_i32(reader)
            .map(SqlValue::Int)
            .map_err(IgniteError::from),
        TypeCode::Long => read_i64(reader)
            .map(SqlValue::Long)
            .map_err(IgniteError::from),
        TypeCode::Float => read_f32(reader)
            .map(SqlValue::Float)
            .map_err(IgniteError::from),
        TypeCode::Double => read_f64(reader)
            .map(SqlValue::Double)
            .map_err(IgniteError::from),
        TypeCode::Char => read_u16(reader)
            .map(SqlValue::Char)
            .map_err(IgniteError::from),
        TypeCode::Bool => read_bool(reader)
            .map(SqlValue::Bool)
            .map_err(IgniteError::from),
        TypeCode::String => read_string(reader)
            .map(SqlValue::String)
            .map_err(IgniteError::from),
        TypeCode::Decimal => read_decimal(reader),
        TypeCode::Uuid => read_uuid(reader),
        TypeCode::Date => read_i64(reader)
            .map(|millis| SqlValue::Date(SqlDate { millis }))
            .map_err(IgniteError::from),
        TypeCode::Timestamp => read_timestamp(reader),
        TypeCode::Time => read_i64(reader)
            .map(|millis| SqlValue::Time(SqlTime { millis }))
            .map_err(IgniteError::from),
        TypeCode::ArrByte => read_primitive_arr(reader, read_u8)
            .map(SqlValue::Binary)
            .map_err(IgniteError::from),
        TypeCode::ArrShort => read_primitive_arr(reader, read_i16)
            .map(|values| SqlValue::Array(values.into_iter().map(SqlValue::Short).collect()))
            .map_err(IgniteError::from),
        TypeCode::ArrInt => read_primitive_arr(reader, read_i32)
            .map(|values| SqlValue::Array(values.into_iter().map(SqlValue::Int).collect()))
            .map_err(IgniteError::from),
        TypeCode::ArrLong => read_primitive_arr(reader, read_i64)
            .map(|values| SqlValue::Array(values.into_iter().map(SqlValue::Long).collect()))
            .map_err(IgniteError::from),
        TypeCode::ArrFloat => read_primitive_arr(reader, read_f32)
            .map(|values| SqlValue::Array(values.into_iter().map(SqlValue::Float).collect()))
            .map_err(IgniteError::from),
        TypeCode::ArrDouble => read_primitive_arr(reader, read_f64)
            .map(|values| SqlValue::Array(values.into_iter().map(SqlValue::Double).collect()))
            .map_err(IgniteError::from),
        TypeCode::ArrChar => read_primitive_arr(reader, read_u16)
            .map(|values| SqlValue::Array(values.into_iter().map(SqlValue::Char).collect()))
            .map_err(IgniteError::from),
        TypeCode::ArrBool => read_primitive_arr(reader, read_bool)
            .map(|values| SqlValue::Array(values.into_iter().map(SqlValue::Bool).collect()))
            .map_err(IgniteError::from),
        TypeCode::ArrString => read_typed_nullable_array(reader, TypeCode::String, |reader| {
            read_string(reader)
                .map(SqlValue::String)
                .map_err(IgniteError::from)
        })
        .map(SqlValue::Array),
        TypeCode::ArrUuid => {
            read_typed_nullable_array(reader, TypeCode::Uuid, |reader| read_uuid(reader))
                .map(SqlValue::Array)
        }
        TypeCode::ArrDate => read_typed_nullable_array(reader, TypeCode::Date, |reader| {
            read_i64(reader)
                .map(|millis| SqlValue::Date(SqlDate { millis }))
                .map_err(IgniteError::from)
        })
        .map(SqlValue::Array),
        TypeCode::ArrDecimal => {
            read_typed_nullable_array(reader, TypeCode::Decimal, |reader| read_decimal(reader))
                .map(SqlValue::Array)
        }
        TypeCode::ArrTimestamp => {
            read_typed_nullable_array(reader, TypeCode::Timestamp, |reader| read_timestamp(reader))
                .map(SqlValue::Array)
        }
        TypeCode::ArrTime => read_typed_nullable_array(reader, TypeCode::Time, |reader| {
            read_i64(reader)
                .map(|millis| SqlValue::Time(SqlTime { millis }))
                .map_err(IgniteError::from)
        })
        .map(SqlValue::Array),
        TypeCode::ArrObj => {
            read_i32(reader).map_err(IgniteError::from)?;
            let len = read_i32(reader).map_err(IgniteError::from)? as usize;
            let mut values = Vec::with_capacity(len);
            for _ in 0..len {
                values.push(read_sql_value(reader)?);
            }
            Ok(SqlValue::Array(values))
        }
        TypeCode::Collection => {
            let len = read_i32(reader).map_err(IgniteError::from)? as usize;
            let _collection_type = read_i8(reader).map_err(IgniteError::from)?;
            let mut values = Vec::with_capacity(len);
            for _ in 0..len {
                values.push(read_sql_value(reader)?);
            }
            Ok(SqlValue::Collection(values))
        }
        TypeCode::Map => {
            // Some cluster-node attributes are Maps (observed in Ignite 2.17.0
            // topologies, e.g. user-defined attributes of type java.util.Map).
            // Wire layout matches Ignite's generic Map: count(i32) + subtype(u8)
            // + (key, value)* pairs. We don't care about the subtype at the
            // SqlValue level — just surface the entries so compute-task dispatch
            // can read node attributes without bailing out.
            let len = read_i32(reader).map_err(IgniteError::from)? as usize;
            let _map_type = read_u8(reader).map_err(IgniteError::from)?;
            let mut entries = Vec::with_capacity(len);
            for _ in 0..len {
                let k = read_sql_value(reader)?;
                let v = read_sql_value(reader)?;
                entries.push((k, v));
            }
            Ok(SqlValue::Map(entries))
        }
        TypeCode::WrappedData => {
            read_i32(reader).map_err(IgniteError::from)?;
            let value = read_sql_value(reader)?;
            read_i32(reader).map_err(IgniteError::from)?;
            Ok(value)
        }
        TypeCode::Enum | TypeCode::BinaryEnum => read_enum(reader)
            .map(SqlValue::Enum)
            .map_err(IgniteError::from),
        TypeCode::ComplexObj => Ok(SqlValue::ComplexObject(
            ComplexObject::read_unwrapped(TypeCode::ComplexObj, reader)?
                .ok_or_else(|| IgniteError::from("Expected complex object value"))?,
        )),
        TypeCode::OptimizedMarshaller => {
            // JDK-serialized Java object — opaque to the thin client.
            // Format: length(4) + data(length).
            let len = read_i32(reader).map_err(IgniteError::from)?;
            if len > 0 {
                let mut buf = vec![0u8; len as usize];
                reader.read_exact(&mut buf).map_err(IgniteError::from)?;
            }
            Ok(SqlValue::Null)
        }
        unsupported => Err(IgniteError::new(format!(
            "Unsupported SQL field type code: {:?}",
            unsupported
        ))),
    }
}

fn read_decimal(reader: &mut impl Read) -> IgniteResult<SqlValue> {
    let scale = read_i32(reader).map_err(IgniteError::from)?;
    let len = read_i32(reader).map_err(IgniteError::from)? as usize;
    let mut magnitude = vec![0u8; len];
    reader
        .read_exact(&mut magnitude)
        .map_err(IgniteError::from)?;
    Ok(SqlValue::Decimal(SqlDecimal { scale, magnitude }))
}

fn read_uuid(reader: &mut impl Read) -> IgniteResult<SqlValue> {
    let most = read_i64(reader).map_err(IgniteError::from)?;
    let least = read_i64(reader).map_err(IgniteError::from)?;
    Ok(SqlValue::Uuid(SqlUuid { most, least }))
}

fn read_timestamp(reader: &mut impl Read) -> IgniteResult<SqlValue> {
    let millis = read_i64(reader).map_err(IgniteError::from)?;
    let nanos = read_i32(reader).map_err(IgniteError::from)?;
    Ok(SqlValue::Timestamp(SqlTimestamp { millis, nanos }))
}

fn read_typed_nullable_array<R: Read, F>(
    reader: &mut R,
    expected_type: TypeCode,
    mut read_inner: F,
) -> IgniteResult<Vec<SqlValue>>
where
    F: FnMut(&mut R) -> IgniteResult<SqlValue>,
{
    let len = read_i32(reader).map_err(IgniteError::from)? as usize;
    let mut values = Vec::with_capacity(len);

    for _ in 0..len {
        let flag = TypeCode::try_from(read_u8(reader).map_err(IgniteError::from)?)
            .map_err(IgniteError::from)?;
        if flag == TypeCode::Null {
            values.push(SqlValue::Null);
            continue;
        }

        if flag != expected_type {
            return Err(IgniteError::new(format!(
                "Unexpected SQL array element type {:?}, expected {:?}",
                flag, expected_type
            )));
        }

        values.push(read_inner(reader)?);
    }

    Ok(values)
}

fn sql_type_error(target: &str, value: &SqlValue) -> IgniteError {
    IgniteError::new(format!(
        "Cannot decode SQL value {:?} into {}",
        value, target
    ))
}

#[cfg(test)]
mod tests {
    use super::{
        read_sql_fields_page, read_sql_value, SqlField, SqlFieldsOpenResponse, SqlFieldsQuery,
        SqlFieldsQueryRequest, SqlQuery, SqlQueryRequest, SqlValue,
    };
    use crate::api::key_value::CacheInfo;
    use crate::protocol::complex_obj::IgniteValue;
    use crate::protocol::{write_bool, write_i32, write_i64, write_string, write_u8, TypeCode};
    use crate::transport::SqlFieldsCapabilities;
    use crate::{ReadableReq, WriteableReq};
    use std::io::Cursor;

    #[test]
    fn should_decode_sql_value_arrays_and_nulls() {
        let mut bytes = Vec::new();
        write_u8(&mut bytes, TypeCode::ArrString as u8).unwrap();
        write_i32(&mut bytes, 3).unwrap();
        write_u8(&mut bytes, TypeCode::String as u8).unwrap();
        write_string(&mut bytes, "a").unwrap();
        write_u8(&mut bytes, TypeCode::Null as u8).unwrap();
        write_u8(&mut bytes, TypeCode::String as u8).unwrap();
        write_string(&mut bytes, "c").unwrap();

        let value = read_sql_value(&mut Cursor::new(bytes)).unwrap();
        assert_eq!(
            value,
            SqlValue::Array(vec![
                SqlValue::String("a".to_string()),
                SqlValue::Null,
                SqlValue::String("c".to_string()),
            ])
        );
    }

    #[test]
    fn should_decode_sql_fields_open_response_into_rows() {
        let mut bytes = Vec::new();
        write_i64(&mut bytes, 9).unwrap();
        write_i32(&mut bytes, 2).unwrap();
        write_u8(&mut bytes, TypeCode::String as u8).unwrap();
        write_string(&mut bytes, "ID").unwrap();
        write_u8(&mut bytes, TypeCode::String as u8).unwrap();
        write_string(&mut bytes, "NAME").unwrap();
        write_i32(&mut bytes, 1).unwrap();
        write_u8(&mut bytes, TypeCode::Long as u8).unwrap();
        write_i64(&mut bytes, 42).unwrap();
        write_u8(&mut bytes, TypeCode::String as u8).unwrap();
        write_string(&mut bytes, "alpha").unwrap();
        write_bool(&mut bytes, false).unwrap();

        let resp = SqlFieldsOpenResponse::read(&mut Cursor::new(bytes)).unwrap();
        assert_eq!(resp.cursor_id, 9);
        assert_eq!(resp.field_names, vec!["ID".to_string(), "NAME".to_string()]);
        assert_eq!(
            resp.rows,
            vec![vec![
                SqlValue::Long(42),
                SqlValue::String("alpha".to_string())
            ]]
        );
        assert!(!resp.has_more);
    }

    #[test]
    fn should_decode_sql_fields_page_using_known_column_count() {
        let mut bytes = Vec::new();
        write_i32(&mut bytes, 2).unwrap();
        write_u8(&mut bytes, TypeCode::Long as u8).unwrap();
        write_i64(&mut bytes, 1).unwrap();
        write_u8(&mut bytes, TypeCode::String as u8).unwrap();
        write_string(&mut bytes, "a").unwrap();
        write_u8(&mut bytes, TypeCode::Long as u8).unwrap();
        write_i64(&mut bytes, 2).unwrap();
        write_u8(&mut bytes, TypeCode::String as u8).unwrap();
        write_string(&mut bytes, "b").unwrap();
        write_bool(&mut bytes, false).unwrap();

        let (rows, has_more) = read_sql_fields_page(&bytes, 2).unwrap();
        assert_eq!(
            rows,
            vec![
                vec![SqlValue::Long(1), SqlValue::String("a".to_string())],
                vec![SqlValue::Long(2), SqlValue::String("b".to_string())],
            ]
        );
        assert!(!has_more);
    }

    #[test]
    fn should_decode_single_column_and_tuple_rows() {
        let single = i64::from_sql_value(SqlValue::Int(7)).unwrap();
        assert_eq!(single, 7);

        let tuple = <(i64, Option<String>) as super::SqlRow>::from_sql_values(vec![
            SqlValue::Long(1),
            SqlValue::Null,
        ])
        .unwrap();
        assert_eq!(tuple, (1, None));
    }

    #[test]
    fn should_encode_sql_query_request() {
        let query = SqlQuery::<(), ()>::new("Person", "_val >= ?")
            .with_page_size(64)
            .with_timeout_ms(123)
            .with_args(vec![IgniteValue::Int(7)]);
        let req = SqlQueryRequest {
            cache_info: CacheInfo::new(3),
            query: &query,
        };

        let mut actual = Vec::new();
        req.write(&mut actual).unwrap();

        assert_eq!(req.size(), actual.len());
        assert_eq!(&actual[0..4], 3i32.to_le_bytes().as_slice());
    }

    #[test]
    fn should_preserve_sql_fields_query_builder_values() {
        let query = SqlFieldsQuery::<Vec<SqlValue>>::new("SELECT 1")
            .with_schema("PUBLIC")
            .with_page_size(2)
            .with_timeout_ms(321)
            .with_update_batch_size(7)
            .with_partitions([1, 3])
            .with_query_initiator_id("test-initiator")
            .with_lazy(true);

        assert_eq!(query.schema(), Some("PUBLIC"));
        assert_eq!(query.page_size(), 2);
        assert_eq!(query.timeout_ms(), 321);
        assert_eq!(query.update_batch_size(), 7);
        assert_eq!(query.partitions(), Some(&[1, 3][..]));
        assert_eq!(query.query_initiator_id(), Some("test-initiator"));
        assert!(query.lazy());
    }

    #[test]
    fn should_skip_optional_sql_fields_segments_when_server_capabilities_are_absent() {
        let query = SqlFieldsQuery::<Vec<SqlValue>>::new("SELECT 1")
            .with_update_batch_size(7)
            .with_partitions([1, 3])
            .with_query_initiator_id("test-initiator");
        let req = SqlFieldsQueryRequest {
            cache_info: CacheInfo::new(0),
            query: &query,
            capabilities: SqlFieldsCapabilities::default(),
        };

        let mut actual = Vec::new();
        req.write(&mut actual).unwrap();

        assert_eq!(req.size(), actual.len());
        assert!(
            !actual
                .windows("test-initiator".len())
                .any(|window| window == b"test-initiator"),
            "initiator id should not be encoded when the server did not negotiate support"
        );
        assert!(
            !actual.windows(4).any(|window| window == 7i32.to_le_bytes()),
            "updateBatchSize should not be encoded when the server did not negotiate support"
        );
    }

    #[test]
    fn should_encode_optional_sql_fields_segments_when_server_capabilities_are_present() {
        let query = SqlFieldsQuery::<Vec<SqlValue>>::new("SELECT 1")
            .with_update_batch_size(7)
            .with_partitions([1, 3])
            .with_query_initiator_id("test-initiator");
        let req = SqlFieldsQueryRequest {
            cache_info: CacheInfo::new(0),
            query: &query,
            capabilities: SqlFieldsCapabilities {
                partitions_batch_size: true,
                query_initiator_id: true,
            },
        };

        let mut actual = Vec::new();
        req.write(&mut actual).unwrap();

        assert_eq!(req.size(), actual.len());
        assert!(
            actual
                .windows("test-initiator".len())
                .any(|window| window == b"test-initiator"),
            "initiator id should be encoded when the server negotiated support"
        );
        assert!(
            actual.windows(4).any(|window| window == 7i32.to_le_bytes()),
            "updateBatchSize should be encoded when the server negotiated support"
        );
    }

    #[test]
    fn should_validate_sql_fields_query() {
        let empty = SqlFieldsQuery::<Vec<SqlValue>>::new("   ")
            .validate()
            .unwrap_err();
        assert!(empty.to_string().contains("Failed to parse SQL"));

        let batch = SqlFieldsQuery::<Vec<SqlValue>>::new("SELECT 1")
            .with_update_batch_size(0)
            .validate()
            .unwrap_err();
        assert!(batch
            .to_string()
            .contains("updateBatchSize cannot be lower than 1"));

        let partitions = SqlFieldsQuery::<Vec<SqlValue>>::new("SELECT 1")
            .with_partitions([0, -1])
            .validate()
            .unwrap_err();
        assert!(partitions.to_string().contains("Illegal partition"));
    }
}
