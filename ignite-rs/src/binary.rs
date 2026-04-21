use crate::api::OpCode;
use crate::binary_registry::{
    register_complex_schema, register_type, registered_type_from_schema, type_by_id, type_name,
    RegisteredBinaryField, RegisteredBinarySchema, RegisteredBinaryType,
};
use crate::error::{IgniteError, IgniteResult};
use crate::exec::TokioExec;
use crate::protocol::complex_obj::{
    ComplexObject, ComplexObjectSchema, IgniteField, IgniteType, IgniteValue,
};
use crate::protocol::{read_bool, read_i32, read_u8, write_bool, write_i32, write_u8, TypeCode};
use crate::utils::{get_schema_id, string_to_java_hashcode};
use crate::{ReadableReq, ReadableType, WritableType, WriteableReq};
use std::convert::TryFrom;
use std::io::{self, Cursor, Read, Write};
use std::marker::PhantomData;
use std::sync::Arc;

/// Java platform id, matches `MarshallerPlatformIds.JAVA_ID` in the Apache
/// Ignite source. The DotNet ID is 1, which is what this constant used to
/// hold — register calls were going to the .NET mapping table, so the Java
/// `MarshallerContext.getClassName(platformId=0, typeId=...)` lookup then
/// reported "Failed to resolve .NET class '...' in Java [platformId=0, ...]".
const JAVA_PLATFORM_ID: u8 = 0;

pub type BinaryObject = ComplexObject;
pub type BinaryField = IgniteField;
pub type BinaryFieldType = IgniteType;
pub type BinaryValue = IgniteValue;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BinaryNameMapperMode {
    BasicFull,
    BasicSimple,
    Custom,
    Unknown(u8),
}

impl BinaryNameMapperMode {
    fn from_wire(value: u8) -> Self {
        match value {
            0 => Self::BasicFull,
            1 => Self::BasicSimple,
            2 => Self::Custom,
            other => Self::Unknown(other),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BinaryConfigurationInfo {
    pub compact_footer: bool,
    pub name_mapper_mode: BinaryNameMapperMode,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BinaryFieldMetadata {
    pub name: String,
    pub type_id: i32,
    pub field_id: i32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BinarySchema {
    pub id: i32,
    pub field_ids: Vec<i32>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BinaryEnumVariant {
    pub name: String,
    pub ordinal: i32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BinaryTypeMetadata {
    pub type_id: i32,
    pub type_name: String,
    pub affinity_key_field_name: Option<String>,
    pub fields: Vec<BinaryFieldMetadata>,
    pub is_enum: bool,
    pub enum_values: Vec<BinaryEnumVariant>,
    pub schemas: Vec<BinarySchema>,
}

impl BinaryTypeMetadata {
    pub fn from_schema(schema: &ComplexObjectSchema) -> Self {
        from_registered(registered_type_from_schema(schema))
    }

    fn from_object(object: &BinaryObject) -> Self {
        if object.is_enum() {
            return Self {
                type_id: string_to_java_hashcode(object.type_name().to_lowercase().as_str()),
                type_name: object.type_name().to_string(),
                affinity_key_field_name: None,
                fields: Vec::new(),
                is_enum: true,
                enum_values: Vec::new(),
                schemas: Vec::new(),
            };
        }
        Self::from_schema(object.schema.as_ref())
    }

    fn into_registered(self) -> RegisteredBinaryType {
        RegisteredBinaryType {
            type_id: self.type_id,
            type_name: self.type_name,
            affinity_key_field_name: self.affinity_key_field_name,
            fields: self
                .fields
                .into_iter()
                .map(|field| RegisteredBinaryField {
                    name: field.name,
                    type_id: field.type_id,
                    field_id: field.field_id,
                })
                .collect(),
            is_enum: self.is_enum,
            enum_values: self
                .enum_values
                .into_iter()
                .map(|variant| (variant.name, variant.ordinal))
                .collect(),
            schemas: self
                .schemas
                .into_iter()
                .map(|schema| RegisteredBinarySchema {
                    id: schema.id,
                    field_ids: schema.field_ids,
                })
                .collect(),
        }
    }
}

fn from_registered(registered: RegisteredBinaryType) -> BinaryTypeMetadata {
    BinaryTypeMetadata {
        type_id: registered.type_id,
        type_name: registered.type_name,
        affinity_key_field_name: registered.affinity_key_field_name,
        fields: registered
            .fields
            .into_iter()
            .map(|field| BinaryFieldMetadata {
                name: field.name,
                type_id: field.type_id,
                field_id: field.field_id,
            })
            .collect(),
        is_enum: registered.is_enum,
        enum_values: registered
            .enum_values
            .into_iter()
            .map(|(name, ordinal)| BinaryEnumVariant { name, ordinal })
            .collect(),
        schemas: registered
            .schemas
            .into_iter()
            .map(|schema| BinarySchema {
                id: schema.id,
                field_ids: schema.field_ids,
            })
            .collect(),
    }
}

#[derive(Clone)]
pub struct Binary {
    exec: TokioExec,
}

impl Binary {
    pub(crate) fn new(exec: TokioExec) -> Self {
        Self { exec }
    }

    pub fn builder(&self, type_name: &str) -> BinaryObjectBuilder {
        BinaryObjectBuilder::new(type_name)
    }

    pub fn type_id(&self, type_name: &str) -> i32 {
        string_to_java_hashcode(type_name.to_lowercase().as_str())
    }

    pub fn build_enum(&self, type_name: &str, ordinal: i32) -> BinaryObject {
        register_type(RegisteredBinaryType {
            type_id: self.type_id(type_name),
            type_name: type_name.to_string(),
            affinity_key_field_name: None,
            fields: Vec::new(),
            is_enum: true,
            enum_values: Vec::new(),
            schemas: Vec::new(),
        });

        BinaryObject {
            schema: Arc::new(ComplexObjectSchema {
                type_name: type_name.to_string(),
                fields: Vec::new(),
            }),
            values: vec![IgniteValue::Enum(crate::Enum {
                type_id: self.type_id(type_name),
                ordinal,
            })],
        }
    }

    pub fn build_enum_name(
        &self,
        type_name: &str,
        variant_name: &str,
    ) -> IgniteResult<BinaryObject> {
        let type_id = self.type_id(type_name);
        let ordinal = type_by_id(type_id)
            .and_then(|meta| {
                meta.enum_values
                    .into_iter()
                    .find_map(|(name, ordinal)| (name == variant_name).then_some(ordinal))
            })
            .ok_or_else(|| {
                IgniteError::from(
                    format!(
                        "Enum variant '{}' for type '{}' is not registered",
                        variant_name, type_name
                    )
                    .as_str(),
                )
            })?;

        Ok(self.build_enum(type_name, ordinal))
    }

    pub fn to_binary<T: WritableType>(&self, value: T) -> IgniteResult<BinaryObject> {
        let mut payload = Vec::with_capacity(value.size());
        value.write(&mut payload).map_err(IgniteError::from)?;

        let mut reader = Cursor::new(payload);
        let type_code = TypeCode::try_from(read_u8(&mut reader).map_err(IgniteError::from)?)?;
        ComplexObject::read_unwrapped(type_code, &mut reader)?
            .ok_or_else(|| IgniteError::from("binary conversion returned null"))
    }

    pub async fn register_enum(
        &self,
        type_name: &str,
        variants: &[BinaryEnumVariant],
    ) -> IgniteResult<BinaryTypeMetadata> {
        let field_ids = variants
            .iter()
            .map(|variant| string_to_java_hashcode(variant.name.to_lowercase().as_str()))
            .collect::<Vec<_>>();
        let meta = BinaryTypeMetadata {
            type_id: self.type_id(type_name),
            type_name: type_name.to_string(),
            affinity_key_field_name: None,
            fields: Vec::new(),
            is_enum: true,
            enum_values: variants.to_vec(),
            schemas: vec![BinarySchema {
                id: get_schema_id(
                    &variants
                        .iter()
                        .map(|variant| IgniteField {
                            name: variant.name.clone(),
                            r#type: IgniteType::Enum,
                        })
                        .collect::<Vec<_>>(),
                ),
                field_ids,
            }],
        };

        self.put_type(&meta).await?;
        Ok(meta)
    }

    pub async fn get_configuration(&self) -> IgniteResult<BinaryConfigurationInfo> {
        // FND gate: Java `TcpIgniteClient` short-circuits this call to null
        // when `BINARY_CONFIGURATION` (bit 8) is not negotiated
        // (`TcpIgniteClient.java:555-558@2.17.0`). Rust surfaces an explicit
        // client-side error since the return type is `IgniteResult<_>` rather
        // than `Option<_>`.
        if !self.exec.supports_binary_configuration().await {
            return Err(IgniteError::from(
                "BINARY_CONFIGURATION is not supported by the server",
            ));
        }
        self.exec
            .send_and_read(OpCode::GetBinaryConfiguration, EmptyReq)
            .await
    }

    pub async fn get_type_name(&self, type_id: i32) -> IgniteResult<String> {
        if let Some(type_name) = type_name(type_id) {
            return Ok(type_name);
        }

        let response: StringResp = self
            .exec
            .send_and_read(
                OpCode::GetBinaryTypeName,
                BinaryTypeNameGetRequest {
                    platform_id: JAVA_PLATFORM_ID,
                    type_id,
                },
            )
            .await?;

        register_type(RegisteredBinaryType {
            type_id,
            type_name: response.value.clone(),
            affinity_key_field_name: None,
            fields: Vec::new(),
            is_enum: false,
            enum_values: Vec::new(),
            schemas: Vec::new(),
        });

        Ok(response.value)
    }

    pub async fn register_type_name(&self, type_id: i32, type_name: &str) -> IgniteResult<bool> {
        let response: BoolResp = self
            .exec
            .send_and_read(
                OpCode::RegisterBinaryTypeName,
                BinaryTypeNamePutRequest {
                    platform_id: JAVA_PLATFORM_ID,
                    type_id,
                    type_name: type_name.to_string(),
                },
            )
            .await?;

        if response.value {
            register_type(RegisteredBinaryType {
                type_id,
                type_name: type_name.to_string(),
                affinity_key_field_name: None,
                fields: Vec::new(),
                is_enum: false,
                enum_values: Vec::new(),
                schemas: Vec::new(),
            });
        }

        Ok(response.value)
    }

    pub async fn get_type(&self, type_id: i32) -> IgniteResult<Option<BinaryTypeMetadata>> {
        if let Some(registered) = type_by_id(type_id) {
            return Ok(Some(from_registered(registered)));
        }

        let response: BinaryTypeGetResponse = self
            .exec
            .send_and_read(OpCode::GetBinaryType, BinaryTypeGetRequest { type_id })
            .await?;
        if let Some(meta) = response.meta.clone() {
            register_type(meta.clone().into_registered());
            Ok(Some(meta))
        } else {
            Ok(None)
        }
    }

    pub async fn put_type(&self, meta: &BinaryTypeMetadata) -> IgniteResult<()> {
        self.exec
            .send(
                OpCode::PutBinaryType,
                BinaryTypePutRequest::new(meta.clone()),
            )
            .await?;
        register_type(meta.clone().into_registered());
        Ok(())
    }

    pub async fn register_object_type(&self, object: &BinaryObject) -> IgniteResult<()> {
        let meta = BinaryTypeMetadata::from_object(object);
        self.put_type(&meta).await
    }
}

#[derive(Clone, Debug, Default)]
pub struct BinaryObjectBuilder {
    type_name: String,
    fields: Vec<(String, IgniteValue)>,
}

impl BinaryObjectBuilder {
    pub fn new(type_name: &str) -> Self {
        Self {
            type_name: type_name.to_string(),
            fields: Vec::new(),
        }
    }

    pub fn set_field<T: Into<IgniteValue>>(mut self, name: &str, value: T) -> Self {
        self.fields.push((name.to_string(), value.into()));
        self
    }

    pub fn set_field_value(mut self, name: &str, value: IgniteValue) -> Self {
        self.fields.push((name.to_string(), value));
        self
    }

    pub fn build(self) -> BinaryObject {
        let schema = Arc::new(ComplexObjectSchema {
            type_name: self.type_name,
            fields: self
                .fields
                .iter()
                .map(|(name, value)| IgniteField {
                    name: name.clone(),
                    r#type: value.ignite_type(),
                })
                .collect(),
        });
        register_complex_schema(schema.as_ref());

        BinaryObject {
            schema,
            values: self.fields.into_iter().map(|(_, value)| value).collect(),
        }
    }
}

impl BinaryObject {
    pub fn enum_ordinal(&self) -> Option<i32> {
        match self.values.as_slice() {
            [IgniteValue::Enum(value)] => Some(value.ordinal),
            _ => None,
        }
    }

    pub fn is_enum(&self) -> bool {
        self.enum_ordinal().is_some()
    }
}

struct EmptyReq;

impl WriteableReq for EmptyReq {
    fn write(&self, _writer: &mut dyn Write) -> io::Result<()> {
        Ok(())
    }

    fn size(&self) -> usize {
        0
    }
}

struct BoolResp {
    value: bool,
}

impl ReadableReq for BoolResp {
    fn read(reader: &mut impl Read) -> IgniteResult<Self> {
        Ok(Self {
            value: read_bool(reader).map_err(IgniteError::from)?,
        })
    }
}

struct StringResp {
    value: String,
}

impl ReadableReq for StringResp {
    fn read(reader: &mut impl Read) -> IgniteResult<Self> {
        Ok(Self {
            value: String::read(reader)?
                .ok_or_else(|| IgniteError::from("binary type name response was null"))?,
        })
    }
}

impl ReadableReq for BinaryConfigurationInfo {
    fn read(reader: &mut impl Read) -> IgniteResult<Self> {
        Ok(Self {
            compact_footer: read_bool(reader).map_err(IgniteError::from)?,
            name_mapper_mode: BinaryNameMapperMode::from_wire(
                read_u8(reader).map_err(IgniteError::from)?,
            ),
        })
    }
}

struct BinaryTypeNameGetRequest {
    platform_id: u8,
    type_id: i32,
}

impl WriteableReq for BinaryTypeNameGetRequest {
    fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        write_u8(writer, self.platform_id)?;
        write_i32(writer, self.type_id)
    }

    fn size(&self) -> usize {
        1 + 4
    }
}

struct BinaryTypeNamePutRequest {
    platform_id: u8,
    type_id: i32,
    type_name: String,
}

impl WriteableReq for BinaryTypeNamePutRequest {
    fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        write_u8(writer, self.platform_id)?;
        write_i32(writer, self.type_id)?;
        self.type_name.write(writer)
    }

    fn size(&self) -> usize {
        1 + 4 + self.type_name.size()
    }
}

struct BinaryTypeGetRequest {
    type_id: i32,
}

impl WriteableReq for BinaryTypeGetRequest {
    fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        write_i32(writer, self.type_id)
    }

    fn size(&self) -> usize {
        4
    }
}

struct BinaryTypePutRequest {
    meta: BinaryTypeMetadata,
}

impl BinaryTypePutRequest {
    fn new(meta: BinaryTypeMetadata) -> Self {
        Self { meta }
    }
}

impl WriteableReq for BinaryTypePutRequest {
    fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        write_binary_metadata(writer, &self.meta)
    }

    fn size(&self) -> usize {
        binary_metadata_size(&self.meta)
    }
}

struct BinaryTypeGetResponse {
    meta: Option<BinaryTypeMetadata>,
}

impl ReadableReq for BinaryTypeGetResponse {
    fn read(reader: &mut impl Read) -> IgniteResult<Self> {
        let has_meta = read_bool(reader).map_err(IgniteError::from)?;
        if !has_meta {
            return Ok(Self { meta: None });
        }

        Ok(Self {
            meta: Some(read_binary_metadata(reader)?),
        })
    }
}

fn write_binary_metadata(writer: &mut dyn Write, meta: &BinaryTypeMetadata) -> io::Result<()> {
    write_i32(writer, meta.type_id)?;
    meta.type_name.write(writer)?;
    meta.affinity_key_field_name.write(writer)?;
    write_i32(writer, meta.fields.len() as i32)?;
    for field in &meta.fields {
        field.name.write(writer)?;
        write_i32(writer, field.type_id)?;
        write_i32(writer, field.field_id)?;
    }
    write_bool(writer, meta.is_enum)?;
    if meta.is_enum {
        write_i32(writer, meta.enum_values.len() as i32)?;
        for variant in &meta.enum_values {
            variant.name.write(writer)?;
            write_i32(writer, variant.ordinal)?;
        }
    }
    write_i32(writer, meta.schemas.len() as i32)?;
    for schema in &meta.schemas {
        write_i32(writer, schema.id)?;
        write_i32(writer, schema.field_ids.len() as i32)?;
        for field_id in &schema.field_ids {
            write_i32(writer, *field_id)?;
        }
    }
    Ok(())
}

fn binary_metadata_size(meta: &BinaryTypeMetadata) -> usize {
    let mut size = 4 + meta.type_name.size() + meta.affinity_key_field_name.size() + 4;
    for field in &meta.fields {
        size += field.name.size() + 4 + 4;
    }
    size += 1;
    if meta.is_enum {
        size += 4;
        for variant in &meta.enum_values {
            size += variant.name.size() + 4;
        }
    }
    size += 4;
    for schema in &meta.schemas {
        size += 4 + 4 + (schema.field_ids.len() * 4);
    }
    size
}

fn read_binary_metadata(reader: &mut impl Read) -> IgniteResult<BinaryTypeMetadata> {
    let type_id = read_i32(reader).map_err(IgniteError::from)?;
    let type_name = String::read(reader)?
        .ok_or_else(|| IgniteError::from("binary metadata type_name was null"))?;
    let affinity_key_field_name = Option::<String>::read(reader)?.flatten();
    let field_count = read_i32(reader).map_err(IgniteError::from)?;
    let mut fields = Vec::with_capacity(field_count.max(0) as usize);
    for _ in 0..field_count {
        fields.push(BinaryFieldMetadata {
            name: String::read(reader)?
                .ok_or_else(|| IgniteError::from("binary metadata field name was null"))?,
            type_id: read_i32(reader).map_err(IgniteError::from)?,
            field_id: read_i32(reader).map_err(IgniteError::from)?,
        });
    }

    let is_enum = read_bool(reader).map_err(IgniteError::from)?;
    let mut enum_values = Vec::new();
    if is_enum {
        let enum_count = read_i32(reader).map_err(IgniteError::from)?;
        enum_values.reserve(enum_count.max(0) as usize);
        for _ in 0..enum_count {
            enum_values.push(BinaryEnumVariant {
                name: String::read(reader)?
                    .ok_or_else(|| IgniteError::from("binary enum value name was null"))?,
                ordinal: read_i32(reader).map_err(IgniteError::from)?,
            });
        }
    }

    let schema_count = read_i32(reader).map_err(IgniteError::from)?;
    let mut schemas = Vec::with_capacity(schema_count.max(0) as usize);
    for _ in 0..schema_count {
        let id = read_i32(reader).map_err(IgniteError::from)?;
        let field_count = read_i32(reader).map_err(IgniteError::from)?;
        let mut field_ids = Vec::with_capacity(field_count.max(0) as usize);
        for _ in 0..field_count {
            field_ids.push(read_i32(reader).map_err(IgniteError::from)?);
        }
        schemas.push(BinarySchema { id, field_ids });
    }

    Ok(BinaryTypeMetadata {
        type_id,
        type_name,
        affinity_key_field_name,
        fields,
        is_enum,
        enum_values,
        schemas,
    })
}

impl<T: Into<IgniteValue>> From<Option<T>> for IgniteValue {
    fn from(value: Option<T>) -> Self {
        value.map(Into::into).unwrap_or(IgniteValue::Null)
    }
}

#[allow(dead_code)]
struct TypedResponse<T> {
    _marker: PhantomData<T>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::TypeCode;

    #[test]
    fn should_build_binary_object_from_builder() {
        let object = BinaryObjectBuilder::new("Person")
            .set_field("id", 1i32)
            .set_field("name", "Jane")
            .build();

        assert_eq!(object.schema.type_name, "Person");
        assert_eq!(object.field("id"), Some(&IgniteValue::Int(1)));
        assert_eq!(
            object.field("name"),
            Some(&IgniteValue::String("Jane".to_string()))
        );
    }

    #[test]
    fn should_convert_schema_into_metadata() {
        let object = BinaryObjectBuilder::new("Person")
            .set_field("id", 1i32)
            .set_field("name", "Jane")
            .build();
        let meta = BinaryTypeMetadata::from_object(&object);

        assert_eq!(meta.type_name, "Person");
        assert_eq!(meta.fields.len(), 2);
        assert_eq!(meta.schemas.len(), 1);
    }

    #[test]
    fn should_round_trip_binary_metadata_payload() {
        let meta = BinaryTypeMetadata {
            type_id: 42,
            type_name: "Person".to_string(),
            affinity_key_field_name: Some("id".to_string()),
            fields: vec![BinaryFieldMetadata {
                name: "id".to_string(),
                type_id: TypeCode::Int as i32,
                field_id: 7,
            }],
            is_enum: false,
            enum_values: Vec::new(),
            schemas: vec![BinarySchema {
                id: 13,
                field_ids: vec![7],
            }],
        };
        let mut bytes = Vec::new();
        write_binary_metadata(&mut bytes, &meta).unwrap();
        let decoded = read_binary_metadata(&mut std::io::Cursor::new(bytes)).unwrap();

        assert_eq!(decoded, meta);
    }
}
