use crate::cache::{QueryEntity, QueryField};
use crate::error::{IgniteError, IgniteResult};
use crate::protocol::{
    read_bool, read_enum, read_f32, read_f64, read_i16, read_i32, read_i64, read_string, read_u16,
    read_u8, write_enum, write_f32, write_f64, write_i16, write_i32, write_i64, write_null,
    write_string, write_u16, write_u8, TypeCode, COMPLEX_OBJ_HEADER_LEN, FLAG_COMPACT_FOOTER,
    FLAG_HAS_SCHEMA, FLAG_OFFSET_ONE_BYTE, FLAG_OFFSET_TWO_BYTES, FLAG_USER_TYPE, HAS_RAW_DATA,
};
use crate::utils::{bytes_to_java_hashcode, get_schema_id, string_to_java_hashcode};
use crate::{binary_registry, Enum};
use crate::{ReadableType, WritableType};
use std::convert::TryFrom;
use std::io::{Cursor, ErrorKind, Read, Write};
use std::mem::size_of;
use std::sync::Arc;

#[derive(Debug, PartialEq, Clone)]
pub enum IgniteValue {
    Byte(u8),
    String(String),
    Long(i64),
    Int(i32),
    Short(i16),
    Float(f32),
    Double(f64),
    Char(u16),
    Bool(bool),
    Uuid(i64, i64),
    Date(i64),
    Time(i64),
    Binary(Vec<u8>),
    Object(Box<ComplexObject>),
    Array(Vec<IgniteValue>),
    Enum(Enum),
    Timestamp(i64, i32), // milliseconds since 1 Jan 1970 UTC, Nanosecond fraction of a millisecond.
    Decimal(i32, Vec<u8>), // scale, big int value in bytes
    Null,
    /// Map: (map_subtype, entries). Subtype: 1=HashMap, 2=LinkedHashMap.
    Map(u8, Vec<(IgniteValue, IgniteValue)>),
    /// Collection: (col_subtype, elements). Subtype: 1=ArrayList, 2=LinkedList, 3=HashSet, 4=LinkedHashSet.
    Collection(u8, Vec<IgniteValue>),
    /// OptimizedMarshaller opaque blob (TypeCode 0xFE + length + data).
    /// Used for JDK-serialized objects that can't be represented as native Ignite types.
    OpaqueMarshal(Vec<u8>),
    /// Pre-encoded wire bytes (written verbatim). The caller has already
    /// produced a full Ignite wire payload — type code prefix included —
    /// for a value that doesn't fit any of the above variants (e.g.
    /// `AffinityKey` with its non-USER_TYPE flag layout). Bypasses the
    /// schema-registry path.
    PreEncoded(Vec<u8>),
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum IgniteType {
    Byte,
    String,
    Long,
    Int,
    Short,
    Float,
    Double,
    Char,
    Bool,
    Uuid,
    Date,
    Time,
    Binary,
    Object,
    Array,
    Timestamp,
    Decimal(i32, i32), // precision, scale
    Enum,
    Null,
    Map,
    Collection,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct IgniteField {
    pub name: String,
    pub r#type: IgniteType,
}

// https://apacheignite.readme.io/docs/binary-client-protocol-data-format#schema
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct ComplexObjectSchema {
    pub type_name: String,
    pub fields: Vec<IgniteField>,
}

// https://apacheignite.readme.io/docs/binary-client-protocol-data-format#complex-object
#[derive(Debug, PartialEq, Clone)]
pub struct ComplexObject {
    pub schema: Arc<ComplexObjectSchema>,
    pub values: Vec<IgniteValue>,
}

impl ComplexObject {
    fn get_data(&self) -> std::io::Result<(Vec<u8>, Vec<u8>)> {
        let mut values: Vec<u8> = Vec::new();
        let mut schema: Vec<u8> = Vec::new();
        for (val, field) in self.values.iter().zip(self.schema.fields.iter()) {
            write_i32(
                &mut schema,
                string_to_java_hashcode(field.name.to_lowercase().as_str()),
            )?;
            write_i32(&mut schema, COMPLEX_OBJ_HEADER_LEN + values.len() as i32)?;
            match val {
                IgniteValue::Byte(val) => {
                    write_u8(&mut values, TypeCode::Byte as u8)?;
                    write_u8(&mut values, *val)?;
                }
                IgniteValue::String(val) => {
                    write_u8(&mut values, TypeCode::String as u8)?;
                    write_string(&mut values, val)?
                }
                IgniteValue::Long(val) => {
                    write_u8(&mut values, TypeCode::Long as u8)?;
                    write_i64(&mut values, *val)?;
                }
                IgniteValue::Int(val) => {
                    write_u8(&mut values, TypeCode::Int as u8)?;
                    write_i32(&mut values, *val)?;
                }
                IgniteValue::Short(val) => {
                    write_u8(&mut values, TypeCode::Short as u8)?;
                    write_i16(&mut values, *val)?;
                }
                IgniteValue::Float(val) => {
                    write_u8(&mut values, TypeCode::Float as u8)?;
                    write_f32(&mut values, *val)?;
                }
                IgniteValue::Double(val) => {
                    write_u8(&mut values, TypeCode::Double as u8)?;
                    write_f64(&mut values, *val)?;
                }
                IgniteValue::Char(val) => {
                    write_u8(&mut values, TypeCode::Char as u8)?;
                    write_u16(&mut values, *val)?;
                }
                IgniteValue::Bool(val) => {
                    write_u8(&mut values, TypeCode::Bool as u8)?;
                    write_u8(&mut values, *val as u8)?;
                }
                IgniteValue::Uuid(most, least) => {
                    write_u8(&mut values, TypeCode::Uuid as u8)?;
                    write_i64(&mut values, *most)?;
                    write_i64(&mut values, *least)?;
                }
                IgniteValue::Date(val) => {
                    write_u8(&mut values, TypeCode::Date as u8)?;
                    write_i64(&mut values, *val)?;
                }
                IgniteValue::Time(val) => {
                    write_u8(&mut values, TypeCode::Time as u8)?;
                    write_i64(&mut values, *val)?;
                }
                IgniteValue::Binary(val) => {
                    write_u8(&mut values, TypeCode::ArrByte as u8)?;
                    write_i32(&mut values, val.len() as i32)?;
                    values.write_all(val)?;
                }
                IgniteValue::Object(val) => {
                    val.write(&mut values)?;
                }
                IgniteValue::Array(items) => {
                    write_u8(&mut values, TypeCode::ArrObj as u8)?;
                    write_i32(&mut values, -1)?;
                    write_i32(&mut values, items.len() as i32)?;
                    for item in items {
                        item.write(&mut values)?;
                    }
                }
                IgniteValue::Enum(val) => {
                    write_u8(&mut values, TypeCode::Enum as u8)?;
                    write_enum(&mut values, *val)?;
                }
                IgniteValue::Timestamp(big, little) => {
                    write_u8(&mut values, TypeCode::Timestamp as u8)?;
                    write_i64(&mut values, *big)?;
                    write_i32(&mut values, *little)?;
                }
                IgniteValue::Decimal(scale, data) => {
                    write_u8(&mut values, TypeCode::Decimal as u8)?;
                    write_i32(&mut values, *scale)?;
                    write_i32(&mut values, data.len() as i32)?;
                    values.write_all(data)?;
                }
                IgniteValue::Null => {
                    write_null(&mut values)?;
                }
                IgniteValue::Map(map_type, entries) => {
                    write_u8(&mut values, TypeCode::Map as u8)?;
                    write_i32(&mut values, entries.len() as i32)?;
                    write_u8(&mut values, *map_type)?;
                    for (k, v) in entries {
                        k.write(&mut values)?;
                        v.write(&mut values)?;
                    }
                }
                IgniteValue::Collection(col_type, items) => {
                    write_u8(&mut values, TypeCode::Collection as u8)?;
                    write_i32(&mut values, items.len() as i32)?;
                    write_u8(&mut values, *col_type)?;
                    for item in items {
                        item.write(&mut values)?;
                    }
                }
                IgniteValue::OpaqueMarshal(data) => {
                    write_u8(&mut values, TypeCode::OptimizedMarshaller as u8)?;
                    write_i32(&mut values, data.len() as i32)?;
                    values.write_all(data)?;
                }
                IgniteValue::PreEncoded(data) => {
                    values.write_all(data)?;
                }
            }
        }
        Ok((values, schema))
    }

    pub fn type_name(&self) -> &str {
        self.schema.type_name()
    }

    pub fn field(&self, name: &str) -> Option<&IgniteValue> {
        self.schema
            .fields
            .iter()
            .position(|field| field.name == name)
            .and_then(|idx| self.values.get(idx))
    }

    pub fn get_offset_flags(offsets: &[i32]) -> u16 {
        match offsets.last() {
            None => FLAG_OFFSET_ONE_BYTE,
            Some(n) => {
                let zeroes = n.leading_zeros();
                let bits = 32 - zeroes;
                match bits {
                    msb if msb < 8 => FLAG_OFFSET_ONE_BYTE,
                    msb if msb < 16 => FLAG_OFFSET_TWO_BYTES,
                    _ => 0,
                }
            }
        }
    }
}

impl WritableType for IgniteValue {
    fn write(&self, writer: &mut dyn Write) -> std::io::Result<()> {
        match self {
            IgniteValue::Byte(val) => {
                write_u8(writer, TypeCode::Byte as u8)?;
                write_u8(writer, *val)
            }
            IgniteValue::String(val) => {
                write_u8(writer, TypeCode::String as u8)?;
                write_string(writer, val)
            }
            IgniteValue::Long(val) => {
                write_u8(writer, TypeCode::Long as u8)?;
                write_i64(writer, *val)
            }
            IgniteValue::Int(val) => {
                write_u8(writer, TypeCode::Int as u8)?;
                write_i32(writer, *val)
            }
            IgniteValue::Short(val) => {
                write_u8(writer, TypeCode::Short as u8)?;
                write_i16(writer, *val)
            }
            IgniteValue::Float(val) => {
                write_u8(writer, TypeCode::Float as u8)?;
                write_f32(writer, *val)
            }
            IgniteValue::Double(val) => {
                write_u8(writer, TypeCode::Double as u8)?;
                write_f64(writer, *val)
            }
            IgniteValue::Char(val) => {
                write_u8(writer, TypeCode::Char as u8)?;
                write_u16(writer, *val)
            }
            IgniteValue::Bool(val) => {
                write_u8(writer, TypeCode::Bool as u8)?;
                write_u8(writer, if *val { 1 } else { 0 })
            }
            IgniteValue::Uuid(most, least) => {
                write_u8(writer, TypeCode::Uuid as u8)?;
                write_i64(writer, *most)?;
                write_i64(writer, *least)
            }
            IgniteValue::Date(val) => {
                write_u8(writer, TypeCode::Date as u8)?;
                write_i64(writer, *val)
            }
            IgniteValue::Time(val) => {
                write_u8(writer, TypeCode::Time as u8)?;
                write_i64(writer, *val)
            }
            IgniteValue::Binary(data) => {
                write_u8(writer, TypeCode::ArrByte as u8)?;
                write_i32(writer, data.len() as i32)?;
                writer.write_all(data)
            }
            IgniteValue::Object(value) => value.write(writer),
            IgniteValue::Array(items) => {
                write_u8(writer, TypeCode::ArrObj as u8)?;
                write_i32(writer, -1)?;
                write_i32(writer, items.len() as i32)?;
                for item in items {
                    item.write(writer)?;
                }
                Ok(())
            }
            IgniteValue::Enum(val) => {
                write_u8(writer, TypeCode::Enum as u8)?;
                write_enum(writer, *val)
            }
            IgniteValue::Timestamp(big, little) => {
                write_u8(writer, TypeCode::Timestamp as u8)?;
                write_i64(writer, *big)?;
                write_i32(writer, *little)
            }
            IgniteValue::Decimal(scale, data) => {
                write_u8(writer, TypeCode::Decimal as u8)?;
                write_i32(writer, *scale)?;
                write_i32(writer, data.len() as i32)?;
                writer.write_all(data)
            }
            IgniteValue::Null => write_null(writer),
            IgniteValue::Map(map_type, entries) => {
                write_u8(writer, TypeCode::Map as u8)?;
                write_i32(writer, entries.len() as i32)?;
                write_u8(writer, *map_type)?;
                for (k, v) in entries {
                    k.write(writer)?;
                    v.write(writer)?;
                }
                Ok(())
            }
            IgniteValue::Collection(col_type, items) => {
                write_u8(writer, TypeCode::Collection as u8)?;
                write_i32(writer, items.len() as i32)?;
                write_u8(writer, *col_type)?;
                for item in items {
                    item.write(writer)?;
                }
                Ok(())
            }
            IgniteValue::OpaqueMarshal(data) => {
                write_u8(writer, TypeCode::OptimizedMarshaller as u8)?;
                write_i32(writer, data.len() as i32)?;
                writer.write_all(data)
            }
            IgniteValue::PreEncoded(data) => writer.write_all(data),
        }
    }

    fn size(&self) -> usize {
        use std::mem::size_of;
        match self {
            IgniteValue::Byte(_) => 1 + size_of::<u8>(),
            IgniteValue::String(s) => 1 + size_of::<i32>() + s.len(),
            IgniteValue::Long(_) => 1 + size_of::<i64>(),
            IgniteValue::Int(_) => 1 + size_of::<i32>(),
            IgniteValue::Short(_) => 1 + size_of::<i16>(),
            IgniteValue::Float(_) => 1 + size_of::<f32>(),
            IgniteValue::Double(_) => 1 + size_of::<f64>(),
            IgniteValue::Char(_) => 1 + size_of::<u16>(),
            IgniteValue::Bool(_) => 1 + size_of::<u8>(),
            IgniteValue::Uuid(_, _) => 1 + size_of::<i64>() + size_of::<i64>(),
            IgniteValue::Date(_) | IgniteValue::Time(_) => 1 + size_of::<i64>(),
            IgniteValue::Binary(data) => 1 + size_of::<i32>() + data.len(),
            IgniteValue::Object(value) => value.size(),
            IgniteValue::Array(items) => {
                1 + size_of::<i32>()
                    + size_of::<i32>()
                    + items.iter().map(IgniteValue::size).sum::<usize>()
            }
            IgniteValue::Enum(_) => 1 + size_of::<i32>() + size_of::<i32>(),
            IgniteValue::Timestamp(_, _) => 1 + size_of::<i64>() + size_of::<i32>(),
            IgniteValue::Decimal(_, data) => 1 + size_of::<i32>() + size_of::<i32>() + data.len(),
            IgniteValue::Null => 1,
            IgniteValue::Map(_, entries) => {
                1 + size_of::<i32>()
                    + 1
                    + entries
                        .iter()
                        .map(|(k, v)| k.size() + v.size())
                        .sum::<usize>()
            }
            IgniteValue::Collection(_, items) => {
                1 + size_of::<i32>() + 1 + items.iter().map(IgniteValue::size).sum::<usize>()
            }
            IgniteValue::OpaqueMarshal(data) => 1 + size_of::<i32>() + data.len(),
            IgniteValue::PreEncoded(data) => data.len(),
        }
    }
}

impl IgniteValue {
    pub fn ignite_type(&self) -> IgniteType {
        match self {
            IgniteValue::Byte(_) => IgniteType::Byte,
            IgniteValue::String(_) => IgniteType::String,
            IgniteValue::Long(_) => IgniteType::Long,
            IgniteValue::Int(_) => IgniteType::Int,
            IgniteValue::Short(_) => IgniteType::Short,
            IgniteValue::Float(_) => IgniteType::Float,
            IgniteValue::Double(_) => IgniteType::Double,
            IgniteValue::Char(_) => IgniteType::Char,
            IgniteValue::Bool(_) => IgniteType::Bool,
            IgniteValue::Uuid(_, _) => IgniteType::Uuid,
            IgniteValue::Date(_) => IgniteType::Date,
            IgniteValue::Time(_) => IgniteType::Time,
            IgniteValue::Binary(_) => IgniteType::Binary,
            IgniteValue::Object(_) => IgniteType::Object,
            IgniteValue::Array(_) => IgniteType::Array,
            IgniteValue::Enum(_) => IgniteType::Enum,
            IgniteValue::Timestamp(_, _) => IgniteType::Timestamp,
            IgniteValue::Decimal(_, _) => IgniteType::Decimal(0, 0),
            IgniteValue::Null => IgniteType::Null,
            IgniteValue::Map(_, _) => IgniteType::Map,
            IgniteValue::Collection(_, _) => IgniteType::Collection,
            IgniteValue::OpaqueMarshal(_) => IgniteType::Binary, // opaque blob
            IgniteValue::PreEncoded(_) => IgniteType::Object, // pre-encoded object
        }
    }
}

impl From<u8> for IgniteValue {
    fn from(value: u8) -> Self {
        IgniteValue::Byte(value)
    }
}

impl From<i16> for IgniteValue {
    fn from(value: i16) -> Self {
        IgniteValue::Short(value)
    }
}

impl From<i32> for IgniteValue {
    fn from(value: i32) -> Self {
        IgniteValue::Int(value)
    }
}

impl From<i64> for IgniteValue {
    fn from(value: i64) -> Self {
        IgniteValue::Long(value)
    }
}

impl From<f32> for IgniteValue {
    fn from(value: f32) -> Self {
        IgniteValue::Float(value)
    }
}

impl From<f64> for IgniteValue {
    fn from(value: f64) -> Self {
        IgniteValue::Double(value)
    }
}

impl From<bool> for IgniteValue {
    fn from(value: bool) -> Self {
        IgniteValue::Bool(value)
    }
}

impl From<String> for IgniteValue {
    fn from(value: String) -> Self {
        IgniteValue::String(value)
    }
}

impl From<&str> for IgniteValue {
    fn from(value: &str) -> Self {
        IgniteValue::String(value.to_string())
    }
}

impl From<Vec<u8>> for IgniteValue {
    fn from(value: Vec<u8>) -> Self {
        IgniteValue::Binary(value)
    }
}

impl From<ComplexObject> for IgniteValue {
    fn from(value: ComplexObject) -> Self {
        IgniteValue::Object(Box::new(value))
    }
}

impl From<Enum> for IgniteValue {
    fn from(value: Enum) -> Self {
        IgniteValue::Enum(value)
    }
}

impl ReadableType for ComplexObject {
    fn read_unwrapped(type_code: TypeCode, reader: &mut impl Read) -> IgniteResult<Option<Self>> {
        let mut me = ComplexObject {
            schema: Arc::new(ComplexObjectSchema {
                type_name: "".to_string(),
                fields: vec![],
            }),
            values: vec![],
        };
        match type_code {
            TypeCode::Byte => {
                me.schema = Arc::new(ComplexObjectSchema {
                    type_name: "java.lang.Byte".to_string(),
                    fields: vec![],
                });
                me.values.push(IgniteValue::Byte(read_u8(reader)?));
            }
            TypeCode::String => {
                me.schema = Arc::new(ComplexObjectSchema {
                    type_name: "java.lang.String".to_string(),
                    fields: vec![],
                });
                me.values.push(IgniteValue::String(read_string(reader)?));
            }
            TypeCode::Long => {
                me.schema = Arc::new(ComplexObjectSchema {
                    type_name: "java.lang.Long".to_string(),
                    fields: vec![],
                });
                me.values.push(IgniteValue::Long(read_i64(reader)?));
            }
            TypeCode::Int => {
                me.schema = Arc::new(ComplexObjectSchema {
                    type_name: "java.lang.Integer".to_string(),
                    fields: vec![],
                });
                me.values.push(IgniteValue::Int(read_i32(reader)?));
            }
            TypeCode::Short => {
                me.schema = Arc::new(ComplexObjectSchema {
                    type_name: "java.lang.Short".to_string(),
                    fields: vec![],
                });
                me.values.push(IgniteValue::Short(read_i16(reader)?));
            }
            TypeCode::Float => {
                me.schema = Arc::new(ComplexObjectSchema {
                    type_name: "java.lang.Float".to_string(),
                    fields: vec![],
                });
                me.values.push(IgniteValue::Float(read_f32(reader)?));
            }
            TypeCode::Double => {
                me.schema = Arc::new(ComplexObjectSchema {
                    type_name: "java.lang.Double".to_string(),
                    fields: vec![],
                });
                me.values.push(IgniteValue::Double(read_f64(reader)?));
            }
            TypeCode::Char => {
                me.schema = Arc::new(ComplexObjectSchema {
                    type_name: "java.lang.Character".to_string(),
                    fields: vec![],
                });
                me.values.push(IgniteValue::Char(read_u16(reader)?));
            }
            TypeCode::Bool => {
                me.schema = Arc::new(ComplexObjectSchema {
                    type_name: "java.lang.Boolean".to_string(),
                    fields: vec![],
                });
                me.values.push(IgniteValue::Bool(read_bool(reader)?));
            }
            TypeCode::Uuid => {
                me.schema = Arc::new(ComplexObjectSchema {
                    type_name: "java.util.UUID".to_string(),
                    fields: vec![],
                });
                me.values
                    .push(IgniteValue::Uuid(read_i64(reader)?, read_i64(reader)?));
            }
            TypeCode::Date => {
                me.schema = Arc::new(ComplexObjectSchema {
                    type_name: "java.sql.Date".to_string(),
                    fields: vec![],
                });
                me.values.push(IgniteValue::Date(read_i64(reader)?));
            }
            TypeCode::Time => {
                me.schema = Arc::new(ComplexObjectSchema {
                    type_name: "java.sql.Time".to_string(),
                    fields: vec![],
                });
                me.values.push(IgniteValue::Time(read_i64(reader)?));
            }
            TypeCode::ArrByte => {
                let len = read_i32(reader)?;
                let mut data = vec![0; len as usize];
                reader.read_exact(&mut data)?;
                me.schema = Arc::new(ComplexObjectSchema {
                    type_name: "byte[]".to_string(),
                    fields: vec![],
                });
                me.values.push(IgniteValue::Binary(data));
            }
            TypeCode::ArrObj => {
                read_i32(reader)?;
                let len = read_i32(reader)?;
                let mut values = Vec::with_capacity(len as usize);
                for _ in 0..len {
                    let item = ComplexObject::read(reader)?
                        .map(flatten_complex_value)
                        .unwrap_or(IgniteValue::Null);
                    values.push(item);
                }
                me.schema = Arc::new(ComplexObjectSchema {
                    type_name: "java.lang.Object[]".to_string(),
                    fields: vec![],
                });
                me.values.push(IgniteValue::Array(values));
            }
            TypeCode::Enum | TypeCode::BinaryEnum => {
                me.schema = Arc::new(ComplexObjectSchema {
                    type_name: "java.lang.Enum".to_string(),
                    fields: vec![],
                });
                me.values.push(IgniteValue::Enum(read_enum(reader)?));
            }
            TypeCode::ComplexObj => {
                // Read header fields directly from reader (no intermediate allocation).
                let _version = read_u8(reader)?;
                let flags = read_u16(reader)?;
                let type_id = read_i32(reader)?;
                let _hash_code = read_i32(reader)?;
                let object_len = read_i32(reader)? as usize;
                let schema_id = read_i32(reader)?;
                let field_indexes_offset = read_i32(reader)? as usize;

                let (one, two) = (
                    (flags & FLAG_OFFSET_ONE_BYTE) != 0,
                    (flags & FLAG_OFFSET_TWO_BYTES) != 0,
                );
                let has_raw = (flags & HAS_RAW_DATA) != 0;
                let _compact = flags & FLAG_COMPACT_FOOTER != 0;
                let has_schema = (flags & FLAG_HAS_SCHEMA) != 0;
                if !has_schema && !has_raw {
                    return Err(IgniteError::from(
                        "Schema is required for non-raw-data objects",
                    ));
                }
                let _offset_sz = match (one, two) {
                    (true, false) => 1,
                    (false, true) => 2,
                    (false, false) => 4,
                    (true, true) => Err(IgniteError::from("Invalid offset flags"))?,
                };

                // Read body (field data + schema footer) in one allocation.
                let body_len = object_len - COMPLEX_OBJ_HEADER_LEN as usize;
                let mut data = vec![0u8; body_len];
                reader.read_exact(&mut data)?;

                // for acquiring test fixture data
                // println!("data={:02X?}", data);

                // read field data
                // For HAS_RAW_DATA without FLAG_HAS_SCHEMA (Externalizable):
                // field_indexes_offset = raw data offset (typically 24 = header length).
                // Raw data extends to object_len. Use object_len as end boundary.
                // For FLAG_HAS_SCHEMA: field_indexes_offset = start of schema footer.
                // data_end is relative to the body buffer (header already consumed).
                let data_end = if has_raw && !has_schema {
                    body_len
                } else if field_indexes_offset > COMPLEX_OBJ_HEADER_LEN as usize {
                    field_indexes_offset - COMPLEX_OBJ_HEADER_LEN as usize
                } else {
                    body_len
                };
                let mut remainder = Cursor::new(&data);
                while (remainder.position() as usize) < data_end {
                    let field_type = TypeCode::try_from(read_u8(&mut remainder)?)?;
                    let val = match field_type {
                        TypeCode::Byte => IgniteValue::Byte(read_u8(&mut remainder)?),
                        TypeCode::String => IgniteValue::String(read_string(&mut remainder)?),
                        TypeCode::Long => IgniteValue::Long(read_i64(&mut remainder)?),
                        TypeCode::Int => IgniteValue::Int(read_i32(&mut remainder)?),
                        TypeCode::Short => IgniteValue::Short(read_i16(&mut remainder)?),
                        TypeCode::Float => IgniteValue::Float(read_f32(&mut remainder)?),
                        TypeCode::Double => IgniteValue::Double(read_f64(&mut remainder)?),
                        TypeCode::Char => IgniteValue::Char(read_u16(&mut remainder)?),
                        TypeCode::Bool => IgniteValue::Bool(read_bool(&mut remainder)?),
                        TypeCode::Uuid => {
                            IgniteValue::Uuid(read_i64(&mut remainder)?, read_i64(&mut remainder)?)
                        }
                        TypeCode::Date => IgniteValue::Date(read_i64(&mut remainder)?),
                        TypeCode::Time => IgniteValue::Time(read_i64(&mut remainder)?),
                        TypeCode::ArrByte => {
                            let len = read_i32(&mut remainder)?;
                            let mut buf = vec![0; len as usize];
                            remainder.read_exact(&mut buf)?;
                            IgniteValue::Binary(buf)
                        }
                        TypeCode::ArrObj => {
                            read_i32(&mut remainder)?;
                            let len = read_i32(&mut remainder)?;
                            let mut values = Vec::with_capacity(len as usize);
                            for _ in 0..len {
                                let item = ComplexObject::read(&mut remainder)?
                                    .map(flatten_complex_value)
                                    .unwrap_or(IgniteValue::Null);
                                values.push(item);
                            }
                            IgniteValue::Array(values)
                        }
                        TypeCode::Enum | TypeCode::BinaryEnum => {
                            IgniteValue::Enum(read_enum(&mut remainder)?)
                        }
                        TypeCode::ComplexObj => IgniteValue::Object(Box::new(
                            ComplexObject::read_unwrapped(TypeCode::ComplexObj, &mut remainder)?
                                .ok_or_else(|| {
                                    IgniteError::from("missing nested complex object")
                                })?,
                        )),
                        TypeCode::Timestamp => {
                            let big = read_i64(&mut remainder)?;
                            let little = read_i32(&mut remainder)?;
                            IgniteValue::Timestamp(big, little)
                        }
                        TypeCode::Decimal => {
                            let scale = read_i32(&mut remainder)?;
                            let len = read_i32(&mut remainder)?;
                            let mut buf = vec![0; len as usize];
                            remainder.read_exact(&mut buf)?;
                            IgniteValue::Decimal(scale, buf)
                        }
                        TypeCode::Null => IgniteValue::Null,
                        TypeCode::Map => {
                            let count = read_i32(&mut remainder)?;
                            let map_type = read_u8(&mut remainder)?;
                            let mut entries = Vec::with_capacity(count.max(0) as usize);
                            for _ in 0..count {
                                let k = ComplexObject::read(&mut remainder)?
                                    .map(flatten_complex_value)
                                    .unwrap_or(IgniteValue::Null);
                                let v = ComplexObject::read(&mut remainder)?
                                    .map(flatten_complex_value)
                                    .unwrap_or(IgniteValue::Null);
                                entries.push((k, v));
                            }
                            IgniteValue::Map(map_type, entries)
                        }
                        TypeCode::Collection => {
                            let count = read_i32(&mut remainder)?;
                            let col_type = read_u8(&mut remainder)?;
                            let mut items = Vec::with_capacity(count.max(0) as usize);
                            for _ in 0..count {
                                let item = ComplexObject::read(&mut remainder)?
                                    .map(flatten_complex_value)
                                    .unwrap_or(IgniteValue::Null);
                                items.push(item);
                            }
                            IgniteValue::Collection(col_type, items)
                        }
                        TypeCode::ArrString => {
                            let len = read_i32(&mut remainder)?;
                            let mut items = Vec::with_capacity(len.max(0) as usize);
                            for _ in 0..len {
                                let s = ComplexObject::read(&mut remainder)?
                                    .map(flatten_complex_value)
                                    .unwrap_or(IgniteValue::Null);
                                items.push(s);
                            }
                            IgniteValue::Array(items)
                        }
                        TypeCode::ArrEnum => {
                            // Java §2.1 ENUM_ARR: i32 componentTypeId; i32 length;
                            // length × (code + body). Any element may be NULL.
                            let _component_type_id = read_i32(&mut remainder)?;
                            let len = read_i32(&mut remainder)?;
                            let mut items = Vec::with_capacity(len.max(0) as usize);
                            for _ in 0..len {
                                let item = ComplexObject::read(&mut remainder)?
                                    .map(flatten_complex_value)
                                    .unwrap_or(IgniteValue::Null);
                                items.push(item);
                            }
                            IgniteValue::Array(items)
                        }
                        TypeCode::OptimizedMarshaller => {
                            // JDK-serialized opaque object — preserve as OpaqueMarshal so a
                            // subsequent write round-trips with the same TypeCode byte. Losing
                            // the type distinction (e.g. storing as Binary then rewriting as
                            // ArrByte/9 instead of OptimizedMarshaller/254) breaks byte-level
                            // `replace_if_equals` on the server side (REG-1).
                            let len = read_i32(&mut remainder)?;
                            let mut buf = vec![0; len as usize];
                            remainder.read_exact(&mut buf)?;
                            IgniteValue::OpaqueMarshal(buf)
                        }
                        TypeCode::WrappedData => {
                            // `BINARY_OBJ` field-level wrapper — Java `BinaryWriterExImpl
                            // .writeBinaryObject`: `[type_code(1) | length(4) | bytes(length) |
                            // start_offset(4)]`. Decode into an inner ComplexObject so field
                            // access (`obj.field("...")`) works on the wrapped value.
                            let len = read_i32(&mut remainder)? as usize;
                            let mut buf = vec![0u8; len];
                            remainder.read_exact(&mut buf)?;
                            let start_offset = read_i32(&mut remainder)? as usize;
                            let mut inner = Cursor::new(&buf[start_offset..]);
                            let inner_type = TypeCode::try_from(read_u8(&mut inner)?)?;
                            let inner_obj = ComplexObject::read_unwrapped(inner_type, &mut inner)?;
                            match inner_obj {
                                Some(obj) => IgniteValue::Object(Box::new(obj)),
                                None => IgniteValue::Null,
                            }
                        }
                        _ => {
                            let msg = format!("Unknown type: {:?}", field_type);
                            Err(IgniteError::from(msg.as_str()))?
                        }
                    };
                    me.values.push(val);
                }
                if has_schema {
                    // Read footer field_ids to determine field order (matches values order).
                    // Offsets are body-relative (header already stripped).
                    let footer_start = if field_indexes_offset > COMPLEX_OBJ_HEADER_LEN as usize {
                        field_indexes_offset - COMPLEX_OBJ_HEADER_LEN as usize
                    } else {
                        0
                    };
                    let footer_end = body_len;
                    let entry_size = _offset_sz + 4; // field_id(4) + offset(offset_sz)
                    let mut footer_field_ids = Vec::new();
                    if footer_end > footer_start && entry_size > 0 {
                        let num_fields = (footer_end - footer_start) / entry_size;
                        remainder.set_position(footer_start as u64);
                        for _ in 0..num_fields {
                            let fid = read_i32(&mut remainder)?;
                            // skip offset bytes
                            for _ in 0.._offset_sz {
                                read_u8(&mut remainder)?;
                            }
                            footer_field_ids.push(fid);
                        }
                    }

                    if let Some(schema) =
                        binary_registry::schema_for_ordered(type_id, schema_id, &footer_field_ids)
                    {
                        me.schema = schema;
                    } else if let Some(schema) = binary_registry::schema_for(type_id, schema_id) {
                        me.schema = schema;
                    }
                }
                // the remainder of bytes are offsets to fields which we have already read
            }
            TypeCode::Map => {
                let count = read_i32(reader)?;
                let map_type = read_u8(reader)?;
                let mut entries = Vec::with_capacity(count.max(0) as usize);
                for _ in 0..count {
                    let k = ComplexObject::read(reader)?
                        .map(flatten_complex_value)
                        .unwrap_or(IgniteValue::Null);
                    let v = ComplexObject::read(reader)?
                        .map(flatten_complex_value)
                        .unwrap_or(IgniteValue::Null);
                    entries.push((k, v));
                }
                me.schema = Arc::new(ComplexObjectSchema {
                    type_name: "java.util.HashMap".to_string(),
                    fields: vec![],
                });
                me.values.push(IgniteValue::Map(map_type, entries));
            }
            TypeCode::Collection => {
                let count = read_i32(reader)?;
                let col_type = read_u8(reader)?;
                let mut items = Vec::with_capacity(count.max(0) as usize);
                for _ in 0..count {
                    let item = ComplexObject::read(reader)?
                        .map(flatten_complex_value)
                        .unwrap_or(IgniteValue::Null);
                    items.push(item);
                }
                me.schema = Arc::new(ComplexObjectSchema {
                    type_name: "java.util.HashSet".to_string(),
                    fields: vec![],
                });
                me.values.push(IgniteValue::Collection(col_type, items));
            }
            TypeCode::Null => {
                me.values.push(IgniteValue::Null);
            }
            TypeCode::OptimizedMarshaller => {
                // JDK-serialized opaque object — preserve as OpaqueMarshal so a write
                // round-trip retains the original TypeCode byte (REG-1).
                let len = read_i32(reader)?;
                let mut buf = vec![0; len as usize];
                reader.read_exact(&mut buf)?;
                me.schema = Arc::new(ComplexObjectSchema {
                    type_name: "java.lang.Object".to_string(),
                    fields: vec![],
                });
                me.values.push(IgniteValue::OpaqueMarshal(buf));
            }
            _ => {
                return Err(IgniteError::from(
                    format!("Unsupported type code: {:?}", type_code).as_str(),
                ))
            }
        }
        Ok(Some(me))
    }
}

fn flatten_complex_value(value: ComplexObject) -> IgniteValue {
    if value.schema.fields.is_empty() && value.values.len() == 1 {
        return value.values.into_iter().next().unwrap_or(IgniteValue::Null);
    }

    IgniteValue::Object(Box::new(value))
}

impl WritableType for ComplexObject {
    fn write(&self, writer: &mut dyn Write) -> std::io::Result<()> {
        // Primitive wrappers are serialized as the underlying Ignite value.
        if self.schema.fields.is_empty() && self.values.len() == 1 {
            return self.values[0].write(writer);
        }
        if self.schema.type_name == "java.lang.Long" {
            let val = self
                .values
                .last()
                .ok_or_else(|| std::io::Error::new(ErrorKind::Other, "No values"))?;
            let val = match val {
                IgniteValue::Long(val) => val,
                _ => Err(std::io::Error::new(ErrorKind::Other, "Mismatched types!"))?,
            };
            write_u8(writer, TypeCode::Long as u8)?;
            write_i64(writer, *val)?;
            return Ok(());
        }
        if self.schema.type_name == "java.lang.String" {
            let val = self
                .values
                .last()
                .ok_or_else(|| std::io::Error::new(ErrorKind::Other, "No values"))?;
            let val = match val {
                IgniteValue::String(val) => val,
                _ => Err(std::io::Error::new(ErrorKind::Other, "Mismatched types!"))?,
            };
            write_u8(writer, TypeCode::String as u8)?;
            write_string(writer, val)?;
            return Ok(());
        }

        // write fields to vec so we can hash
        binary_registry::register_complex_schema(self.schema.as_ref());
        let (values, schema) = self.get_data()?;

        // https://apacheignite.readme.io/docs/binary-client-protocol-data-format#complex-object
        let flags = FLAG_HAS_SCHEMA | FLAG_USER_TYPE;
        let type_name = self.schema.type_name.to_lowercase();
        let type_id = string_to_java_hashcode(type_name.as_str());
        let schema_id = get_schema_id(&self.schema.fields);
        write_u8(writer, TypeCode::ComplexObj as u8)?; // complex type - offset 0
        write_u8(writer, 1)?; // version - offset 1
        write_u16(writer, flags)?; // flags - 2 - TODO: > 1 byte offsets
        write_i32(writer, type_id)?; // type_id - offset 4
        write_i32(writer, bytes_to_java_hashcode(values.as_slice()))?; // hash - offset 8
        write_i32(writer, self.size() as i32)?; // size - offset 12
        write_i32(writer, schema_id)?; // schema_id - offset 16
        write_i32(writer, COMPLEX_OBJ_HEADER_LEN + values.len() as i32)?; // offset to schema
        writer.write_all(&values)?; // field data - offset 24
        writer.write_all(&schema)?;

        Ok(())
    }

    fn size(&self) -> usize {
        if self.schema.fields.is_empty() && self.values.len() == 1 {
            return self.values[0].size();
        }
        if self.schema.type_name == "java.lang.Long" {
            return size_of::<i64>() + 1;
        }
        if self.schema.type_name == "java.lang.String" {
            let val = self.values.last().expect("No values!");
            let val = match val {
                IgniteValue::String(val) => val,
                _ => panic!("Mismatched types!"),
            };
            return size_of::<i32>() + 1 + val.len();
        }
        let (values, schema) = self.get_data().expect("Can't get size!");
        values.len() + schema.len() + COMPLEX_OBJ_HEADER_LEN as usize
    }
}

// ---------------------------------------------------------------------------
// LazyBinaryObject: zero-copy field access for read-hot paths
// ---------------------------------------------------------------------------

/// A BinaryObject that stores raw bytes and parses fields on demand.
/// Avoids allocating IgniteValue for every field upfront — only the
/// requested field is parsed. Ideal for read-hot paths where only a
/// subset of fields is needed.
#[derive(Debug, Clone)]
pub struct LazyBinaryObject {
    /// Raw bytes of the entire body (after the 24-byte header).
    body: Vec<u8>,
    /// Field entries: (field_id, offset_in_body) from the schema footer.
    fields: Vec<(i32, usize)>,
    /// End of field data region (start of schema footer) relative to body.
    data_end: usize,
}

impl LazyBinaryObject {
    /// Read a ComplexObj from the wire into a lazy representation.
    /// The type code byte (0x67) must already be consumed.
    pub fn read_from(reader: &mut impl Read) -> Result<Self, IgniteError> {
        let _version = read_u8(reader)?;
        let flags = read_u16(reader)?;
        let _type_id = read_i32(reader)?;
        let _hash = read_i32(reader)?;
        let object_len = read_i32(reader)? as usize;
        let _schema_id = read_i32(reader)?;
        let field_indexes_offset = read_i32(reader)? as usize;

        let has_schema = (flags & FLAG_HAS_SCHEMA) != 0;
        let one_byte = (flags & FLAG_OFFSET_ONE_BYTE) != 0;
        let two_byte = (flags & FLAG_OFFSET_TWO_BYTES) != 0;
        let offset_sz: usize = match (one_byte, two_byte) {
            (true, false) => 1,
            (false, true) => 2,
            _ => 4,
        };

        let hdr = COMPLEX_OBJ_HEADER_LEN as usize;
        let body_len = object_len.saturating_sub(hdr);
        let mut body = vec![0u8; body_len];
        reader.read_exact(&mut body)?;

        let data_end = if field_indexes_offset > hdr {
            field_indexes_offset - hdr
        } else {
            body_len
        };

        // Parse schema footer: [(field_id:i32, offset)] entries.
        let mut fields = Vec::new();
        if has_schema && data_end < body_len {
            let entry_size = 4 + offset_sz;
            let footer = &body[data_end..];
            let num_fields = footer.len() / entry_size;
            for i in 0..num_fields {
                let base = i * entry_size;
                if base + entry_size > footer.len() {
                    break;
                }
                let fid = i32::from_le_bytes([
                    footer[base],
                    footer[base + 1],
                    footer[base + 2],
                    footer[base + 3],
                ]);
                let off = match offset_sz {
                    1 => footer[base + 4] as usize,
                    2 => u16::from_le_bytes([footer[base + 4], footer[base + 5]]) as usize,
                    _ => u32::from_le_bytes([
                        footer[base + 4],
                        footer[base + 5],
                        footer[base + 6],
                        footer[base + 7],
                    ]) as usize,
                };
                // offset is relative to object start, convert to body-relative.
                let body_off = off.saturating_sub(hdr);
                fields.push((fid, body_off));
            }
        }

        Ok(Self {
            body,
            fields,
            data_end,
        })
    }

    /// Look up a field by name. Returns a cursor positioned at the field's
    /// type code byte. The caller must read the type code + value from it.
    fn field_offset(&self, name: &str) -> Option<usize> {
        let field_id = string_to_java_hashcode(&name.to_lowercase());
        self.fields
            .iter()
            .find(|(fid, _)| *fid == field_id)
            .map(|(_, off)| *off)
    }

    /// Read an i32 field.
    pub fn get_i32(&self, name: &str) -> Option<i32> {
        let off = self.field_offset(name)?;
        let b = &self.body[off..];
        if b.first()? == &(TypeCode::Int as u8) {
            Some(i32::from_le_bytes([b[1], b[2], b[3], b[4]]))
        } else {
            None
        }
    }

    /// Read an i64 field.
    pub fn get_i64(&self, name: &str) -> Option<i64> {
        let off = self.field_offset(name)?;
        let b = &self.body[off..];
        if b.first()? == &(TypeCode::Long as u8) {
            Some(i64::from_le_bytes([
                b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8],
            ]))
        } else {
            None
        }
    }

    /// Read a string field (returns a reference into the body buffer — zero copy).
    pub fn get_str(&self, name: &str) -> Option<&str> {
        let off = self.field_offset(name)?;
        let b = &self.body[off..];
        if b.first()? != &(TypeCode::String as u8) {
            return None;
        }
        let len = i32::from_le_bytes([b[1], b[2], b[3], b[4]]) as usize;
        let start = 5;
        let end = start + len;
        if end > b.len() {
            return None;
        }
        std::str::from_utf8(&b[start..end]).ok()
    }

    /// Read a byte array field (returns a reference — zero copy).
    pub fn get_bytes(&self, name: &str) -> Option<&[u8]> {
        let off = self.field_offset(name)?;
        let b = &self.body[off..];
        if b.first()? != &(TypeCode::ArrByte as u8) {
            return None;
        }
        let len = i32::from_le_bytes([b[1], b[2], b[3], b[4]]) as usize;
        let start = 5;
        let end = start + len;
        if end > b.len() {
            return None;
        }
        Some(&b[start..end])
    }

    /// Check if a field is Null.
    pub fn is_null(&self, name: &str) -> bool {
        match self.field_offset(name) {
            Some(off) => self.body.get(off) == Some(&(TypeCode::Null as u8)),
            None => true,
        }
    }

    /// Get the full raw body for fields that need advanced parsing
    /// (Map, Collection, ComplexObj, OptimizedMarshaller).
    pub fn raw_field_cursor(&self, name: &str) -> Option<Cursor<&[u8]>> {
        let off = self.field_offset(name)?;
        Some(Cursor::new(&self.body[off..self.data_end.max(off)]))
    }
}

impl crate::ReadableType for LazyBinaryObject {
    fn read_unwrapped(
        type_code: TypeCode,
        reader: &mut impl Read,
    ) -> crate::error::IgniteResult<Option<Self>> {
        match type_code {
            TypeCode::ComplexObj => Ok(Some(LazyBinaryObject::read_from(reader)?)),
            TypeCode::Null => Ok(None),
            _ => Err(crate::error::IgniteError::from(
                format!("LazyBinaryObject: unexpected type code {:?}", type_code).as_str(),
            )),
        }
    }
}

impl crate::WritableType for LazyBinaryObject {
    fn write(&self, _writer: &mut dyn Write) -> std::io::Result<()> {
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "LazyBinaryObject is read-only",
        ))
    }
    fn size(&self) -> usize {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::complex_obj::ComplexObject;
    use std::convert::TryInto;

    /// FND-013: Java §2.1 `ENUM_ARR = 29 (0x1D)` — `i32 componentTypeId; i32 length;
    /// length × (code + body)`. Previously fell through to `Unknown type: ArrEnum`.
    #[test]
    fn arr_enum_field_decodes_as_array_of_enums() {
        // ComplexObject with one field `e: Enum[]` containing two enums:
        //   (typeId=0xAABBCCDD, ordinal=1), (typeId=0xAABBCCDD, ordinal=2)
        // Wire layout for the field:
        //   0x1D (ArrEnum) + componentTypeId(i32) + length(i32) + elements
        //   element: 0x1C (Enum) + typeId(i32) + ordinal(i32)
        let mut field_bytes: Vec<u8> = Vec::new();
        field_bytes.push(TypeCode::ArrEnum as u8);
        field_bytes.extend_from_slice(&0xAABBCCDDu32.to_le_bytes()); // componentTypeId
        field_bytes.extend_from_slice(&2i32.to_le_bytes()); // length
        // element 0
        field_bytes.push(TypeCode::Enum as u8);
        field_bytes.extend_from_slice(&0xAABBCCDDu32.to_le_bytes());
        field_bytes.extend_from_slice(&1i32.to_le_bytes());
        // element 1
        field_bytes.push(TypeCode::Enum as u8);
        field_bytes.extend_from_slice(&0xAABBCCDDu32.to_le_bytes());
        field_bytes.extend_from_slice(&2i32.to_le_bytes());

        // Wrap in a ComplexObject with 1 field. Build header + footer.
        // Field data bytes, then schema: field_id(i32)=0xDEAD + offset(i32)=24.
        let data_len = field_bytes.len() as i32;
        let footer_start_abs = COMPLEX_OBJ_HEADER_LEN + data_len; // abs offset from type code
        let schema_entry = {
            let mut s = Vec::new();
            s.extend_from_slice(&0x0000DEADi32.to_le_bytes()); // field_id
            s.extend_from_slice(&(COMPLEX_OBJ_HEADER_LEN).to_le_bytes()); // offset within obj
            s
        };
        let total_len = footer_start_abs + schema_entry.len() as i32;
        let mut obj_bytes: Vec<u8> = Vec::new();
        obj_bytes.push(TypeCode::ComplexObj as u8);
        obj_bytes.push(1u8); // version
        let flags: u16 = FLAG_HAS_SCHEMA | FLAG_USER_TYPE;
        obj_bytes.extend_from_slice(&flags.to_le_bytes());
        obj_bytes.extend_from_slice(&0x12345678i32.to_le_bytes()); // type_id
        obj_bytes.extend_from_slice(&0i32.to_le_bytes()); // hash (arbitrary)
        obj_bytes.extend_from_slice(&total_len.to_le_bytes());
        obj_bytes.extend_from_slice(&0i32.to_le_bytes()); // schema_id (arbitrary)
        obj_bytes.extend_from_slice(&footer_start_abs.to_le_bytes());
        obj_bytes.extend_from_slice(&field_bytes);
        obj_bytes.extend_from_slice(&schema_entry);

        let mut cur = Cursor::new(obj_bytes);
        let code = read_u8(&mut cur).unwrap();
        let obj = ComplexObject::read_unwrapped(code.try_into().unwrap(), &mut cur)
            .expect("ArrEnum must decode")
            .unwrap();

        assert_eq!(obj.values.len(), 1);
        match &obj.values[0] {
            IgniteValue::Array(items) => {
                assert_eq!(items.len(), 2);
                assert_eq!(
                    items[0],
                    IgniteValue::Enum(Enum {
                        type_id: 0xAABBCCDDu32 as i32,
                        ordinal: 1
                    })
                );
                assert_eq!(
                    items[1],
                    IgniteValue::Enum(Enum {
                        type_id: 0xAABBCCDDu32 as i32,
                        ordinal: 2
                    })
                );
            }
            other => panic!("expected Array, got {:?}", other),
        }
    }

    #[test]
    fn test_round_trip() {
        let expected_bytes = hex_literal::hex!(
            "67" // type
            "01" // version
            "03 00" // flags for has schema, user type
            "34 59 48 16" // Hash of type name (type_id)
            "BA 27 D2 B2" // Hash of fields slice (hash_code)
            "7B 01 00 00" // total size including header
            "C0 40 3B B5" // hash of field names (schema_id)
            "2B 01 00 00" // offset to field indexes
            "09 42 00 00 00 30 78 35 62 35 38 36 37 35 37 63 33 36 65 62 34 63 39 34 66 36 39 30 31 35 66 33 63 62 36 64 33 64 35 62 35 31 63 36 64 62 61 63 65 36 64 33 37 63 62 66 33 34 64 33 36 37 62 30 31 37 31 63 39 34 61 09 13 00 00 00 32 30 32 32 2D 30 31 2D 30 31 20 30 30 3A 30 30 3A 32 30 09 2A 00 00 00 30 78 45 41 36 37 34 66 64 44 65 37 31 34 66 64 39 37 39 64 65 33 45 64 46 30 46 35 36 41 41 39 37 31 36 42 38 39 38 65 63 38 09 42 00 00 00 30 78 33 32 61 65 64 30 63 66 33 31 36 64 31 37 66 30 64 37 63 39 61 62 65 63 63 62 39 38 31 31 37 32 34 61 61 35 38 63 30 39 63 65 35 33 31 66 36 31 66 33 38 36 35 37 38 31 63 38 33 65 32 33 63 32 09 15 00 00 00 32 2E 33 32 30 35 31 33 31 31 30 36 31 37 39 39 31 65 2B 31 38 03 74 0E 02 00 03 47 F7 C9 01 03 E5 05 CA 01 09 0B 00 00 00 36 31 35 38 34 33 34 33 37 32 39 03 DF 01 00 00"
            "80 85 A9 4C 18 00 00 00 D1 6B B5 43 5F 00 00 00 7F 66 31 06 77 00 00 00 83 4B 7D 3C A6 00 00 00 2F 4F 4F C8 ED 00 00 00 7E 20 86 06 07 01 00 00 63 75 84 A0 0C 01 00 00 D5 F6 86 6F 11 01 00 00 90 98 68 68 16 01 00 00 6E 68 A6 AB 26 01 00 00" // schema
        );
        let schema = ComplexObjectSchema {
            type_name: "VT.PUBLIC.BLOCKS-3178274329684762144".to_string(),
            fields: vec![
                IgniteField {
                    name: "BLOCK_HASH".to_string(),
                    r#type: IgniteType::String,
                },
                IgniteField {
                    name: "TIME_STAMP".to_string(),
                    r#type: IgniteType::Timestamp,
                },
                IgniteField {
                    name: "MINER".to_string(),
                    r#type: IgniteType::String,
                },
                IgniteField {
                    name: "PARENT_HASH".to_string(),
                    r#type: IgniteType::String,
                },
                IgniteField {
                    name: "REWARD".to_string(),
                    r#type: IgniteType::String,
                },
                IgniteField {
                    name: "SIZE_".to_string(),
                    r#type: IgniteType::Int,
                },
                IgniteField {
                    name: "GAS_USED".to_string(),
                    r#type: IgniteType::Int,
                },
                IgniteField {
                    name: "GAS_LIMIT".to_string(),
                    r#type: IgniteType::Int,
                },
                IgniteField {
                    name: "BASE_FEE_PER_GAS".to_string(),
                    r#type: IgniteType::Decimal(78, 0),
                },
                IgniteField {
                    name: "TRANSACTION_COUNT".to_string(),
                    r#type: IgniteType::Int,
                },
            ],
        };

        // deserialize
        let mut reader = Cursor::new(expected_bytes);
        let type_code = read_u8(&mut reader).unwrap();
        let val = ComplexObject::read_unwrapped(type_code.try_into().unwrap(), &mut reader)
            .unwrap()
            .unwrap();
        let expected_values = vec![
            IgniteValue::String(
                "0x5b586757c36eb4c94f69015f3cb6d3d5b51c6dbace6d37cbf34d367b0171c94a".to_string(),
            ),
            IgniteValue::String("2022-01-01 00:00:20".to_string()),
            IgniteValue::String("0xEA674fdDe714fd979de3EdF0F56AA9716B898ec8".to_string()),
            IgniteValue::String(
                "0x32aed0cf316d17f0d7c9abeccb9811724aa58c09ce531f61f3865781c83e23c2".to_string(),
            ),
            IgniteValue::String("2.320513110617991e+18".to_string()),
            IgniteValue::Int(134772),
            IgniteValue::Int(30013255),
            IgniteValue::Int(30016997),
            IgniteValue::String("61584343729".to_string()),
            IgniteValue::Int(479),
        ];
        assert_eq!(val.values, expected_values);

        // set schema stuff so it has info info to save itself
        let val = ComplexObject {
            schema: Arc::new(schema),
            values: val.values.clone(),
        };

        // serialize
        let mut actual_bytes = vec![];
        val.write(&mut actual_bytes).unwrap();

        let expected_hex = format!("{:02X?}", expected_bytes);
        let actual_hex = format!("{:02X?}", actual_bytes);
        assert_eq!(actual_hex, expected_hex);
    }
}

impl ComplexObjectSchema {
    /// Find the key and value DynamicIgniteTypes for a table.
    pub fn infer_schemas(
        entity: &QueryEntity,
    ) -> IgniteResult<(Arc<ComplexObjectSchema>, Arc<ComplexObjectSchema>)> {
        let key_fields: Vec<_> = entity
            .query_fields
            .iter()
            .filter(|f| f.key_field || entity.key_field == f.name)
            .collect();
        let val_fields: Vec<_> = entity
            .query_fields
            .iter()
            .filter(|f| !f.key_field && entity.key_field != f.name)
            .collect();
        let key_fields = Self::convert_fields(&key_fields)?;
        let val_fields = Self::convert_fields(&val_fields)?;
        let k = ComplexObjectSchema {
            type_name: entity.key_type.clone(),
            fields: key_fields,
        };
        let v = ComplexObjectSchema {
            type_name: entity.value_type.clone(),
            fields: val_fields,
        };
        Ok((Arc::new(k), Arc::new(v)))
    }

    fn convert_fields(qry_fields: &[&QueryField]) -> IgniteResult<Vec<IgniteField>> {
        let mut fields = vec![];
        for f in qry_fields.iter() {
            let t: IgniteType = match f.type_name.as_str() {
                "java.lang.Byte" => IgniteType::Byte,
                "java.lang.Long" => IgniteType::Long,
                "java.lang.Short" => IgniteType::Short,
                "java.lang.String" => IgniteType::String,
                "java.lang.Float" => IgniteType::Float,
                "java.lang.Double" => IgniteType::Double,
                "java.lang.Character" => IgniteType::Char,
                "java.sql.Timestamp" => IgniteType::Timestamp,
                "java.sql.Date" => IgniteType::Date,
                "java.sql.Time" => IgniteType::Time,
                "java.util.UUID" => IgniteType::Uuid,
                "byte[]" => IgniteType::Binary,
                "java.lang.Integer" => IgniteType::Int,
                "java.lang.Boolean" => IgniteType::Bool,
                "java.math.BigDecimal" => IgniteType::Decimal(f.precision, f.scale),
                _ => Err(IgniteError::from(
                    format!("Unknown field type: {}", f.type_name).as_str(),
                ))?,
            };
            let field = IgniteField {
                name: f.name.to_string(),
                r#type: t,
            };
            fields.push(field);
        }
        Ok(fields)
    }

    pub fn type_name(&self) -> &str {
        self.type_name.as_str()
    }
}
