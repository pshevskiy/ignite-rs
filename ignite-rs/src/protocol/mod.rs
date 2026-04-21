use std::io;
use std::io::{ErrorKind, Read, Write};

use crate::error::{IgniteError, IgniteResult};

use crate::{Enum, ReadableType};
use std::convert::TryFrom;

pub(crate) mod cache_config;
pub mod complex_obj;
pub(crate) mod data_types;

pub const FLAG_USER_TYPE: u16 = 0x0001;
pub const FLAG_HAS_SCHEMA: u16 = 0x0002;
pub const HAS_RAW_DATA: u16 = 0x0004;
pub const FLAG_COMPACT_FOOTER: u16 = 0x0020;
pub const FLAG_OFFSET_ONE_BYTE: u16 = 0x0008;
pub const FLAG_OFFSET_TWO_BYTES: u16 = 0x0010;

pub const COMPLEX_OBJ_HEADER_LEN: i32 = 24;

/// All Data types described in Binary Protocol
/// https://apacheignite.readme.io/docs/binary-client-protocol-data-format
#[derive(PartialOrd, PartialEq, Debug)]
pub enum TypeCode {
    Byte = 1,
    Short = 2,
    Int = 3,
    Long = 4,
    Float = 5,
    Double = 6,
    Char = 7,
    Bool = 8,
    String = 9,
    Uuid = 10,
    Date = 11,
    ArrByte = 12,
    ArrShort = 13,
    ArrInt = 14,
    ArrLong = 15,
    ArrFloat = 16,
    ArrDouble = 17,
    ArrChar = 18,
    ArrBool = 19,
    ArrString = 20,
    ArrUuid = 21,
    ArrDate = 22,
    ArrObj = 23,
    Collection = 24,
    Map = 25,
    Decimal = 30,
    ArrDecimal = 31,
    Timestamp = 33,
    ArrTimestamp = 34,
    Time = 36,
    ArrTime = 37,
    WrappedData = 27,
    Enum = 28,
    ArrEnum = 29,
    BinaryEnum = 38,
    Null = 101,
    /// Self-reference to a previously-written object by absolute offset.
    /// Body: `i32 offset` into the enclosing stream. See Java §2.1.
    Handle = 102,
    ComplexObj = 103,
    /// `Class<?>` descriptor — body: `i32 typeId`.
    Class = 32,
    /// Java `Proxy` marker — platform-only, opaque body.
    Proxy = 35,
    /// Transformed wrapper — opaque (same round-trip discipline as
    /// `OptimizedMarshaller`). Signed Java byte `-3` / `0xFD`.
    Transformed = 253,
    /// Java optimized marshaller object (JDK-serialized, opaque to the thin client).
    OptimizedMarshaller = 254,
}

impl TryFrom<u8> for TypeCode {
    type Error = IgniteError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(TypeCode::Byte),
            2 => Ok(TypeCode::Short),
            3 => Ok(TypeCode::Int),
            4 => Ok(TypeCode::Long),
            5 => Ok(TypeCode::Float),
            6 => Ok(TypeCode::Double),
            7 => Ok(TypeCode::Char),
            8 => Ok(TypeCode::Bool),
            9 => Ok(TypeCode::String),
            10 => Ok(TypeCode::Uuid),
            11 => Ok(TypeCode::Date),
            28 => Ok(TypeCode::Enum),
            29 => Ok(TypeCode::ArrEnum),
            12 => Ok(TypeCode::ArrByte),
            13 => Ok(TypeCode::ArrShort),
            14 => Ok(TypeCode::ArrInt),
            15 => Ok(TypeCode::ArrLong),
            16 => Ok(TypeCode::ArrFloat),
            17 => Ok(TypeCode::ArrDouble),
            18 => Ok(TypeCode::ArrChar),
            19 => Ok(TypeCode::ArrBool),
            20 => Ok(TypeCode::ArrString),
            21 => Ok(TypeCode::ArrUuid),
            22 => Ok(TypeCode::ArrDate),
            23 => Ok(TypeCode::ArrObj),
            30 => Ok(TypeCode::Decimal),
            31 => Ok(TypeCode::ArrDecimal),
            33 => Ok(TypeCode::Timestamp),
            34 => Ok(TypeCode::ArrTimestamp),
            36 => Ok(TypeCode::Time),
            37 => Ok(TypeCode::ArrTime),
            38 => Ok(TypeCode::BinaryEnum),
            24 => Ok(TypeCode::Collection),
            25 => Ok(TypeCode::Map),
            27 => Ok(TypeCode::WrappedData),
            32 => Ok(TypeCode::Class),
            35 => Ok(TypeCode::Proxy),
            102 => Ok(TypeCode::Handle),
            103 => Ok(TypeCode::ComplexObj),
            101 => Ok(TypeCode::Null),
            253 => Ok(TypeCode::Transformed),
            254 => Ok(TypeCode::OptimizedMarshaller),
            _ => Err(IgniteError::from(
                format!("Cannot read TypeCode {}", value).as_str(),
            )),
        }
    }
}

/// Flag of general Response header
#[derive(Clone, Debug)]
pub(crate) enum Flag {
    Success,
    /// A non-zero server status code was set on the response. `status`
    /// is the i32 code from `ClientStatus.java@2.17.0` (e.g. `1012` for
    /// `SECURITY_VIOLATION`, `1040` for `ENTRY_PROCESSOR_EXCEPTION`).
    /// `err_msg` is the attached message string. Callers classify the
    /// error via `IgniteError::from_server_status(status, err_msg)`.
    Failure { status: i32, err_msg: String },
}

fn read_object(reader: &mut impl Read) -> IgniteResult<Option<()>> {
    let flag = read_u8(reader)?;
    let code = TypeCode::try_from(flag);
    let code = code?;
    match code {
        TypeCode::Null => Ok(Some(())),
        _ => Err(IgniteError::from(
            format!("Cannot read TypeCode {}", flag).as_str(),
        )),
    }
}

/// Reads data objects that are wrapped in the WrappedData(type code = 27)
pub fn read_wrapped_data<T: ReadableType>(reader: &mut impl Read) -> IgniteResult<Option<T>> {
    let type_code = TypeCode::try_from(read_u8(reader)?)?;
    match type_code {
        TypeCode::Null => Ok(None),
        TypeCode::WrappedData => {
            read_i32(reader)?; // skip len
            let value = T::read(reader);
            read_i32(reader)?; // skip offset
            value
        }
        _ => T::read_unwrapped(type_code, reader),
    }
}

/// This function is basically a String's PackType implementation but for &str.
/// It should be used only for strings in request bodies (like cache creation, configuration etc.)
/// not for KV (a.k.a DataObject)
pub(crate) fn write_string_type_code(writer: &mut dyn Write, value: &str) -> io::Result<()> {
    let value_bytes = value.as_bytes();
    write_u8(writer, TypeCode::String as u8)?;
    write_i32(writer, value_bytes.len() as i32)?;
    writer.write_all(value_bytes)?;
    Ok(())
}

//// Read functions. No TypeCode, no NULL checking

pub fn write_string(writer: &mut dyn Write, value: &str) -> io::Result<()> {
    let value_bytes = value.as_bytes();
    write_i32(writer, value_bytes.len() as i32)?;
    writer.write_all(value_bytes)?;
    Ok(())
}

pub fn read_string(reader: &mut impl Read) -> io::Result<String> {
    let str_len = read_i32(reader)?;

    let mut new_alloc = vec![0u8; str_len as usize];
    match reader.read_exact(new_alloc.as_mut_slice()) {
        Ok(_) => match String::from_utf8(new_alloc) {
            Ok(s) => Ok(s),
            Err(err) => Err(io::Error::new(ErrorKind::InvalidData, err)),
        },
        Err(err) => Err(err),
    }
}

pub fn read_bool(reader: &mut impl Read) -> io::Result<bool> {
    let mut new_alloc = [0u8; 1];
    match reader.read_exact(&mut new_alloc[..]) {
        Ok(_) => Ok(0u8.ne(&new_alloc[0])),
        Err(err) => Err(err),
    }
}

pub fn write_bool(writer: &mut dyn Write, v: bool) -> io::Result<()> {
    if v {
        write_u8(writer, 1u8)
    } else {
        write_u8(writer, 0u8)
    }
}

pub fn read_u8(reader: &mut impl Read) -> io::Result<u8> {
    let mut new_alloc = [0u8; 1];
    match reader.read_exact(&mut new_alloc[..]) {
        Ok(_) => Ok(u8::from_le_bytes(new_alloc)),
        Err(err) => Err(err),
    }
}

pub fn write_u8(writer: &mut dyn Write, v: u8) -> io::Result<()> {
    writer.write_all(&u8::to_le_bytes(v))?;
    Ok(())
}

pub fn read_i8(reader: &mut impl Read) -> io::Result<i8> {
    let mut new_alloc = [0u8; 1];
    match reader.read_exact(&mut new_alloc[..]) {
        Ok(_) => Ok(i8::from_le_bytes(new_alloc)),
        Err(err) => Err(err),
    }
}

pub fn write_i8(writer: &mut dyn Write, v: i8) -> io::Result<()> {
    writer.write_all(&i8::to_le_bytes(v))?;
    Ok(())
}

pub fn read_u16(reader: &mut impl Read) -> io::Result<u16> {
    let mut new_alloc = [0u8; 2];
    match reader.read_exact(&mut new_alloc[..]) {
        Ok(_) => Ok(u16::from_le_bytes(new_alloc)),
        Err(err) => Err(err),
    }
}

pub fn write_u16(writer: &mut dyn Write, v: u16) -> io::Result<()> {
    writer.write_all(&u16::to_le_bytes(v))?;
    Ok(())
}

pub fn read_i16(reader: &mut impl Read) -> io::Result<i16> {
    let mut new_alloc = [0u8; 2];
    match reader.read_exact(&mut new_alloc[..]) {
        Ok(_) => Ok(i16::from_le_bytes(new_alloc)),
        Err(err) => Err(err),
    }
}

pub fn write_i16(writer: &mut dyn Write, v: i16) -> io::Result<()> {
    writer.write_all(&i16::to_le_bytes(v))?;
    Ok(())
}

pub fn read_i32(reader: &mut impl Read) -> io::Result<i32> {
    let mut new_alloc = [0u8; 4];
    match reader.read_exact(&mut new_alloc[..]) {
        Ok(_) => Ok(i32::from_le_bytes(new_alloc)),
        Err(err) => Err(err),
    }
}

pub fn write_i32(writer: &mut dyn Write, v: i32) -> io::Result<()> {
    writer.write_all(&i32::to_le_bytes(v))?;
    Ok(())
}

pub fn read_u32(reader: &mut impl Read) -> io::Result<u32> {
    let mut new_alloc = [0u8; 4];
    match reader.read_exact(&mut new_alloc[..]) {
        Ok(_) => Ok(u32::from_le_bytes(new_alloc)),
        Err(err) => Err(err),
    }
}

pub fn write_u32(writer: &mut dyn Write, v: u32) -> io::Result<()> {
    writer.write_all(&u32::to_le_bytes(v))?;
    Ok(())
}

pub fn read_i64(reader: &mut impl Read) -> io::Result<i64> {
    let mut new_alloc = [0u8; 8];
    match reader.read_exact(&mut new_alloc[..]) {
        Ok(_) => Ok(i64::from_le_bytes(new_alloc)),
        Err(err) => Err(err),
    }
}

pub fn write_u64(writer: &mut dyn Write, v: u64) -> io::Result<()> {
    writer.write_all(&u64::to_le_bytes(v))?;
    Ok(())
}

pub fn read_u64(reader: &mut impl Read) -> io::Result<u64> {
    let mut new_alloc = [0u8; 8];
    match reader.read_exact(&mut new_alloc[..]) {
        Ok(_) => Ok(u64::from_le_bytes(new_alloc)),
        Err(err) => Err(err),
    }
}

pub fn write_i64(writer: &mut dyn Write, v: i64) -> io::Result<()> {
    writer.write_all(&i64::to_le_bytes(v))?;
    Ok(())
}

pub fn read_f32(reader: &mut impl Read) -> io::Result<f32> {
    let mut new_alloc = [0u8; 4];
    match reader.read_exact(&mut new_alloc[..]) {
        Ok(_) => Ok(f32::from_le_bytes(new_alloc)),
        Err(err) => Err(err),
    }
}

pub fn write_f32(writer: &mut dyn Write, v: f32) -> io::Result<()> {
    writer.write_all(&f32::to_le_bytes(v))?;
    Ok(())
}

pub fn read_f64(reader: &mut impl Read) -> io::Result<f64> {
    let mut new_alloc = [0u8; 8];
    match reader.read_exact(&mut new_alloc[..]) {
        Ok(_) => Ok(f64::from_le_bytes(new_alloc)),
        Err(err) => Err(err),
    }
}

pub fn write_f64(writer: &mut dyn Write, v: f64) -> io::Result<()> {
    writer.write_all(&f64::to_le_bytes(v))?;
    Ok(())
}

pub fn read_primitive_arr<T, R, F>(reader: &mut R, read_fn: F) -> io::Result<Vec<T>>
where
    R: Read,
    F: Fn(&mut R) -> io::Result<T>,
{
    let len = read_i32(reader)?;
    let mut payload: Vec<T> = Vec::with_capacity(len as usize);
    for _ in 0..len {
        payload.push(read_fn(reader)?);
    }
    Ok(payload)
}

pub fn read_enum(reader: &mut impl Read) -> io::Result<Enum> {
    let type_id = read_i32(reader)?;
    let ordinal = read_i32(reader)?;
    Ok(Enum { type_id, ordinal })
}

pub fn write_enum(writer: &mut dyn Write, val: Enum) -> io::Result<()> {
    write_i32(writer, val.type_id)?;
    write_i32(writer, val.ordinal)?;
    Ok(())
}

pub fn write_null(writer: &mut dyn Write) -> io::Result<()> {
    write_u8(writer, TypeCode::Null as u8)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Java §2.1: HANDLE = 102 (0x66). Previously rejected with
    /// "Cannot read TypeCode 102", causing any response with a self-reference
    /// to fail to decode.
    #[test]
    fn type_code_handle_0x66_decodes() {
        assert_eq!(TypeCode::try_from(0x66u8).unwrap(), TypeCode::Handle);
        assert_eq!(TypeCode::Handle as u8, 0x66);
    }

    /// Java §2.1: CLASS = 32 (0x20). Previously rejected.
    #[test]
    fn type_code_class_0x20_decodes() {
        assert_eq!(TypeCode::try_from(0x20u8).unwrap(), TypeCode::Class);
        assert_eq!(TypeCode::Class as u8, 0x20);
    }

    /// Java §2.1: PROXY = 35 (0x23). Previously rejected.
    #[test]
    fn type_code_proxy_0x23_decodes() {
        assert_eq!(TypeCode::try_from(0x23u8).unwrap(), TypeCode::Proxy);
        assert_eq!(TypeCode::Proxy as u8, 0x23);
    }

    /// Java §2.1: TRANSFORMED = −3 (0xFD). Previously rejected.
    #[test]
    fn type_code_transformed_0xfd_decodes() {
        assert_eq!(TypeCode::try_from(0xFDu8).unwrap(), TypeCode::Transformed);
        assert_eq!(TypeCode::Transformed as u8, 0xFD);
    }
}
