//! Rust encoders/decoders for `PutAllComputeTask` POJOs.
//!
//! The server-side compute task
//! `ru.sbrf.ucpcloud.ignite.server.tasks.PutAllComputeTask` ships in the
//! `global-compute-tasks-*.jar` on the Ignite cluster. It is invoked from a
//! thin client via `Compute::execute(task_name, arg)` where `arg` is a Java
//! `BulkPutParams` POJO serialised with the Ignite binary marshaller.
//!
//! Java `writeObject(arg)` on the thin-client side walks the POJO's fields
//! by reflection and writes them as a `ComplexObj` (type code 103) with:
//! - `type_id`  = `stringHashCode(lowerCase(FQN))`
//! - `field_id` = `stringHashCode(lowerCase(fieldName))` per field
//! - HashMap fields  → type code 25 + count(i32) + subtype(1=HashMap) + kv*
//! - HashSet fields  → type code 24 + count(i32) + subtype(3=HashSet) + v*
//! - ArrayList fields→ type code 24 + count(i32) + subtype(1=ArrayList) + v*
//! - Java enums      → type code 28 + type_id(i32) + ordinal(i32)
//!
//! That is exactly what `BinaryObjectBuilder` + `IgniteValue::{Map, Collection, Enum}`
//! already emit, so we can compose the four POJOs from the existing primitive
//! building blocks without a new wire codec.
//!
//! The Java class names (FQN used by the server-side) are frozen constants —
//! mismatching them means the server will fail to deserialize the argument.

use std::collections::HashMap;
use std::io::{self, Read, Write};

use crate::binary::{
    Binary, BinaryEnumVariant, BinaryFieldMetadata, BinaryObject, BinaryObjectBuilder,
    BinarySchema, BinaryTypeMetadata,
};
use crate::error::{IgniteError, IgniteResult};
use crate::protocol::complex_obj::{ComplexObject, IgniteField, IgniteType, IgniteValue};
use crate::protocol::{read_u8, TypeCode};
use crate::utils::{get_schema_id, string_to_java_hashcode};
use crate::{ReadableType, WritableType};

/// Fully-qualified class names as shipped in the Java compute-task JAR.
/// `type_id` is derived from the lower-cased FQN via the same Java string
/// hash used server-side.
pub mod class_names {
    pub const BULK_PUT_PARAMS: &str =
        "ru.sbrf.ucpcloud.ignite.server.tasks.model.BulkPutParams";
    pub const PUT_PARAMS: &str = "ru.sbrf.ucpcloud.ignite.server.tasks.model.PutParams";
    pub const INDEX_CONTEXT: &str =
        "ru.sbrf.ucpcloud.ignite.server.tasks.model.IndexContext";
    pub const SAVE_STRATEGY: &str =
        "ru.sbrf.ucpcloud.ignite.server.tasks.model.SaveStrategy";
    pub const PUT_RESULT: &str = "ru.sbrf.ucpcloud.ignite.server.tasks.model.PutResult";
    pub const PUT_STATUS: &str = "ru.sbrf.ucpcloud.ignite.server.tasks.model.PutStatus";
    pub const BULK_PUT_RESPONSE_PARAMS: &str =
        "ru.sbrf.ucpcloud.ignite.server.tasks.model.BulkPutResponseParams";
}

/// Fully-qualified Java task class for `PutAllComputeTask`.
pub const PUT_ALL_COMPUTE_TASK: &str =
    "ru.sbrf.ucpcloud.ignite.server.tasks.PutAllComputeTask";

/// Java `SaveStrategy` enum values in declaration order — the ordinal is
/// what the server matches on (`SaveStrategy.values()[ordinal]`).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum SaveStrategy {
    Transaction = 0,
    Atomic = 1,
}

impl SaveStrategy {
    pub fn ordinal(self) -> i32 {
        self as i32
    }

    pub fn type_id() -> i32 {
        string_to_java_hashcode(&class_names::SAVE_STRATEGY.to_lowercase())
    }

    /// Build the enum as a wire value (type code 28 + type_id + ordinal).
    pub fn as_ignite_value(self) -> IgniteValue {
        IgniteValue::Enum(crate::Enum {
            type_id: Self::type_id(),
            ordinal: self.ordinal(),
        })
    }
}

/// Java `IndexContext` POJO.
/// Fields (server-side order, matching Java's declaration):
/// - `indexName2UniqueFactor: Map<String, Boolean>`
/// - `indexName2AtomicFactor: Map<String, Boolean>`
/// - `indexName2CacheName:    Map<String, String>`
#[derive(Clone, Debug)]
pub struct IndexContext {
    pub index_name_to_unique: HashMap<String, bool>,
    pub index_name_to_atomic: HashMap<String, bool>,
    pub index_name_to_cache_name: HashMap<String, String>,
}

impl IndexContext {
    pub fn empty() -> Self {
        Self {
            index_name_to_unique: HashMap::new(),
            index_name_to_atomic: HashMap::new(),
            index_name_to_cache_name: HashMap::new(),
        }
    }

    /// Build the Java-compatible `BinaryObject` wire representation.
    pub fn to_binary(&self) -> BinaryObject {
        let uniq_map = map_bool_to_ignite(&self.index_name_to_unique);
        let atom_map = map_bool_to_ignite(&self.index_name_to_atomic);
        let cache_map = map_string_to_ignite(&self.index_name_to_cache_name);

        BinaryObjectBuilder::new(class_names::INDEX_CONTEXT)
            .set_field_value("indexName2UniqueFactor", uniq_map)
            .set_field_value("indexName2AtomicFactor", atom_map)
            .set_field_value("indexName2CacheName", cache_map)
            .build()
    }
}

impl WritableType for IndexContext {
    fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        self.to_binary().write(writer)
    }

    fn size(&self) -> usize {
        self.to_binary().size()
    }
}

/// Java `PutParams` POJO.
///
/// - `cacheName: String`
/// - `key:       Object` (typed — the thin client writes whatever type the
///   caller passed; we mirror this with a `IgniteValue`).
/// - `object:    BinaryObject`  — the value to write.
/// - `forceUpdate: boolean`
/// - `indexContext: IndexContext`
/// - `isSecondaryIndexesExists: boolean`
/// - `saveStrategy: SaveStrategy`
/// - `index: int` — populated server-side in `map()`, but the Java client
///   sends 0; the compute task reassigns it.
#[derive(Clone, Debug)]
pub struct PutParams {
    pub cache_name: String,
    pub key: IgniteValue,
    pub object: BinaryObject,
    pub force_update: bool,
    pub index_context: IndexContext,
    pub is_secondary_indexes_exists: bool,
    pub save_strategy: SaveStrategy,
    pub index: i32,
}

impl PutParams {
    pub fn new(
        cache_name: impl Into<String>,
        key: IgniteValue,
        object: BinaryObject,
        force_update: bool,
        index_context: IndexContext,
        is_secondary_indexes_exists: bool,
        save_strategy: SaveStrategy,
    ) -> Self {
        Self {
            cache_name: cache_name.into(),
            key,
            object,
            force_update,
            index_context,
            is_secondary_indexes_exists,
            save_strategy,
            index: 0,
        }
    }

    pub fn to_binary(&self) -> BinaryObject {
        // Keep the field order matching Java's declaration order. The
        // binary marshaller doesn't require this (fields are looked up by
        // hashed id at read time) but stable ordering produces stable
        // schema_ids, which makes test golden-bytes reproducible.
        //
        // The `object` field is declared as `BinaryObject` on the Java side;
        // Java's reflective writer emits it via the `BINARY_OBJ` (WrappedData,
        // type code 27) path — `writeBinaryObject()` in `BinaryWriterExImpl`.
        // The server reader then keeps the value as a raw `BinaryObject`
        // without trying to deserialize it as a POJO (crucial for types
        // like `UcpRecord` whose class is not on the server's classpath).
        // If we instead emit a plain `ComplexObj` (type code 103), the server
        // treats the field as a generic Object and tries to resolve the
        // nested typeId to a class, failing with "Failed to resolve class
        // name [typeId=859486279]" for `UcpRecord`.
        let object_wrapped = IgniteValue::PreEncoded(encode_wrapped_binary_object(&self.object));
        BinaryObjectBuilder::new(class_names::PUT_PARAMS)
            .set_field("cacheName", self.cache_name.as_str())
            .set_field_value("key", self.key.clone())
            .set_field_value("object", object_wrapped)
            .set_field("forceUpdate", self.force_update)
            .set_field_value(
                "indexContext",
                IgniteValue::Object(Box::new(self.index_context.to_binary())),
            )
            .set_field("isSecondaryIndexesExists", self.is_secondary_indexes_exists)
            .set_field_value("saveStrategy", self.save_strategy.as_ignite_value())
            .set_field("index", self.index)
            .build()
    }
}

/// Encode a `BinaryObject` as the `BINARY_OBJ` (WrappedData) field payload:
/// `[type_code(27, 1 byte), length(i32), bytes, start(i32)]`. Matches Java's
/// `BinaryWriterExImpl.writeBinaryObject(BinaryObjectEx)`.
fn encode_wrapped_binary_object(object: &BinaryObject) -> Vec<u8> {
    let mut object_bytes = Vec::with_capacity(object.size());
    // BinaryObject::write always produces a self-contained `ComplexObj` blob
    // with its own 24-byte header; we embed those bytes verbatim after the
    // WrappedData envelope.
    object.write(&mut object_bytes).expect("BinaryObject write to Vec cannot fail");
    let body_len = object_bytes.len() as i32;

    let mut out = Vec::with_capacity(1 + 4 + object_bytes.len() + 4);
    out.push(crate::protocol::TypeCode::WrappedData as u8);
    out.extend_from_slice(&body_len.to_le_bytes());
    out.extend_from_slice(&object_bytes);
    // `start` offset: Java uses the offset inside the wrapped byte array.
    // Since our wrapped bytes start with the ComplexObj at offset 0, start=0.
    out.extend_from_slice(&0i32.to_le_bytes());
    out
}

impl WritableType for PutParams {
    fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        self.to_binary().write(writer)
    }

    fn size(&self) -> usize {
        self.to_binary().size()
    }
}

/// Java `BulkPutParams` POJO — single field `putParamsList: List<PutParams>`.
#[derive(Clone, Debug)]
pub struct BulkPutParams {
    pub put_params_list: Vec<PutParams>,
}

impl BulkPutParams {
    pub fn new(put_params_list: Vec<PutParams>) -> Self {
        Self { put_params_list }
    }

    /// Build the Java-compatible wire representation.
    pub fn to_binary(&self) -> BinaryObject {
        let list = IgniteValue::Collection(
            COLLECTION_SUBTYPE_ARRAYLIST,
            self.put_params_list
                .iter()
                .map(|pp| IgniteValue::Object(Box::new(pp.to_binary())))
                .collect(),
        );

        BinaryObjectBuilder::new(class_names::BULK_PUT_PARAMS)
            .set_field_value("putParamsList", list)
            .build()
    }

    /// Send `PutBinaryType` AND `RegisterBinaryTypeName` for every POJO the
    /// server needs to deserialize the request. Without this, the server
    /// reports "Failed to resolve class name [typeId=...]" — `PutBinaryType`
    /// only populates `BinaryContext` (field layout), whereas
    /// `MarshallerContext.getClassName` (used during `arg.deserialize()`)
    /// reads from a separate typeId→className table that is filled by
    /// `RegisterBinaryTypeName`.
    ///
    /// Safe to call repeatedly — both ops are idempotent on the server side.
    pub async fn register_types(&self, binary: &Binary) -> IgniteResult<()> {
        for (type_name, build) in all_metadata_builders() {
            let meta = build();
            binary.put_type(&meta).await?;
            binary.register_type_name(meta.type_id, type_name).await?;
        }
        // Pre-register the response-side schemas in the local registry so the
        // decoder can look them up by `(typeId, schemaId, footer field ids)`
        // when reading the compute-task result. Without this local hint the
        // decoder can't associate field offsets with field names and
        // `BinaryObject::field("putResultMap")` returns None.
        //
        // Building a throwaway `BinaryObject` via `BinaryObjectBuilder` is
        // enough — `build()` calls `register_complex_schema` on the schema,
        // which is all the decoder needs (it never sends the empty values
        // anywhere).
        prewarm_response_schemas();
        Ok(())
    }
}

impl WritableType for BulkPutParams {
    fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        self.to_binary().write(writer)
    }

    fn size(&self) -> usize {
        self.to_binary().size()
    }
}

// ---------------------------------------------------------------------------
// Response decoding
// ---------------------------------------------------------------------------

/// Java `PutStatus` enum ordinal values (declaration order).
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum PutStatus {
    /// ordinal 0 — entity did not exist, now added
    Added = 0,
    /// ordinal 1 — overwrote same-version data (`forceUpdate=true`)
    RewriteSameVersion = 1,
    /// ordinal 2 — overwrote newer-version data
    RewriteNewerVersion = 2,
    /// ordinal 3 — grid version ≥ incoming; no write
    AlreadyExist = 3,
    /// ordinal 4 — exception during write
    Error = 4,
}

impl PutStatus {
    pub fn from_ordinal(ordinal: i32) -> Option<Self> {
        match ordinal {
            0 => Some(Self::Added),
            1 => Some(Self::RewriteSameVersion),
            2 => Some(Self::RewriteNewerVersion),
            3 => Some(Self::AlreadyExist),
            4 => Some(Self::Error),
            _ => None,
        }
    }

    pub fn is_data_in_grid_changed(self) -> bool {
        matches!(
            self,
            Self::Added | Self::RewriteSameVersion | Self::RewriteNewerVersion
        )
    }
}

/// Java `PutResult` POJO.
#[derive(Clone, Debug)]
pub struct PutResult {
    pub status: PutStatus,
    pub message: Option<String>,
    pub key: IgniteValue,
    pub index: i32,
}

impl PutResult {
    fn from_object(obj: &BinaryObject) -> IgniteResult<Self> {
        let status = match obj.field("status") {
            Some(IgniteValue::Enum(e)) => PutStatus::from_ordinal(e.ordinal).ok_or_else(|| {
                IgniteError::from(
                    format!("PutResult: unknown PutStatus ordinal {}", e.ordinal).as_str(),
                )
            })?,
            other => {
                return Err(IgniteError::from(
                    format!("PutResult.status: expected Enum, got {:?}", other).as_str(),
                ));
            }
        };

        let message = match obj.field("message") {
            Some(IgniteValue::String(s)) => Some(s.clone()),
            Some(IgniteValue::Null) | None => None,
            other => {
                return Err(IgniteError::from(
                    format!("PutResult.message: expected String or Null, got {:?}", other).as_str(),
                ));
            }
        };

        let key = obj.field("key").cloned().unwrap_or(IgniteValue::Null);

        let index = match obj.field("index") {
            Some(IgniteValue::Int(v)) => *v,
            other => {
                return Err(IgniteError::from(
                    format!("PutResult.index: expected Int, got {:?}", other).as_str(),
                ));
            }
        };

        Ok(Self {
            status,
            message,
            key,
            index,
        })
    }
}

/// Java `BulkPutResponseParams` — single field `putResultMap: Map<Integer, PutResult>`.
#[derive(Clone, Debug, Default)]
pub struct BulkPutResponseParams {
    pub put_result_map: HashMap<i32, PutResult>,
}

impl BulkPutResponseParams {
    /// Decode from a Java-serialized `BulkPutResponseParams` complex object.
    pub fn from_object(obj: &BinaryObject) -> IgniteResult<Self> {
        let map_value = obj.field("putResultMap").ok_or_else(|| {
            IgniteError::from("BulkPutResponseParams: missing `putResultMap` field")
        })?;
        let entries: &Vec<(IgniteValue, IgniteValue)> = match map_value {
            IgniteValue::Map(_, entries) => entries,
            IgniteValue::Null => return Ok(Self::default()),
            other => {
                return Err(IgniteError::from(
                    format!(
                        "BulkPutResponseParams.putResultMap: expected Map, got {:?}",
                        other
                    )
                    .as_str(),
                ));
            }
        };

        let mut put_result_map = HashMap::with_capacity(entries.len());
        for (k, v) in entries {
            let index = match k {
                IgniteValue::Int(v) => *v,
                other => {
                    return Err(IgniteError::from(
                        format!(
                            "BulkPutResponseParams: map key must be Int, got {:?}",
                            other
                        )
                        .as_str(),
                    ));
                }
            };
            let result_obj = match v {
                IgniteValue::Object(inner) => inner.as_ref(),
                other => {
                    return Err(IgniteError::from(
                        format!(
                            "BulkPutResponseParams: map value must be PutResult, got {:?}",
                            other
                        )
                        .as_str(),
                    ));
                }
            };
            put_result_map.insert(index, PutResult::from_object(result_obj)?);
        }

        Ok(Self { put_result_map })
    }
}

impl ReadableType for BulkPutResponseParams {
    fn read_unwrapped(type_code: TypeCode, reader: &mut impl Read) -> IgniteResult<Option<Self>> {
        match type_code {
            TypeCode::Null => Ok(None),
            TypeCode::ComplexObj => {
                let obj = ComplexObject::read_unwrapped(TypeCode::ComplexObj, reader)?
                    .ok_or_else(|| IgniteError::from("BulkPutResponseParams: null ComplexObj"))?;
                Ok(Some(Self::from_object(&obj)?))
            }
            _ => Err(IgniteError::from(
                format!(
                    "BulkPutResponseParams: expected ComplexObj or Null, got {:?}",
                    type_code
                )
                .as_str(),
            )),
        }
    }

    fn read(reader: &mut impl Read) -> IgniteResult<Option<Self>> {
        // The Ignite thin-client protocol wraps compute-task results in
        // a plain type-code prefix (no WrappedData envelope). We need to
        // dispatch on the type code directly.
        let type_code = TypeCode::try_from(read_u8(reader).map_err(IgniteError::from)?)?;
        Self::read_unwrapped(type_code, reader)
    }
}

// ---------------------------------------------------------------------------
// Binary type metadata builders (PutBinaryType payloads)
// ---------------------------------------------------------------------------
//
// The server-side `ClientExecuteTaskRequest` deserializes the compute-task
// argument before dispatch, which forces it to resolve each referenced
// `typeId` via binary metadata. Types defined only inside the
// `global-compute-tasks-*.jar` are not published to the thin-client metadata
// store on cluster start; we must send `PutBinaryType` for them ourselves,
// matching what the Java client does implicitly when serializing these POJOs.

fn field_metadata(name: &str, type_code: TypeCode) -> BinaryFieldMetadata {
    BinaryFieldMetadata {
        name: name.to_string(),
        type_id: type_code as i32,
        field_id: string_to_java_hashcode(&name.to_lowercase()),
    }
}

fn metadata_for_schema(type_name: &str, fields: Vec<(&str, IgniteType)>) -> BinaryTypeMetadata {
    let ignite_fields: Vec<IgniteField> = fields
        .iter()
        .map(|(name, ty)| IgniteField {
            name: name.to_string(),
            r#type: ty.clone(),
        })
        .collect();
    let field_metas: Vec<BinaryFieldMetadata> = fields
        .iter()
        .map(|(name, ty)| field_metadata(name, ignite_type_to_code(ty)))
        .collect();

    BinaryTypeMetadata {
        type_id: string_to_java_hashcode(&type_name.to_lowercase()),
        type_name: type_name.to_string(),
        affinity_key_field_name: None,
        fields: field_metas.clone(),
        is_enum: false,
        enum_values: Vec::new(),
        schemas: vec![BinarySchema {
            id: get_schema_id(&ignite_fields),
            field_ids: field_metas.iter().map(|f| f.field_id).collect(),
        }],
    }
}

fn ignite_type_to_code(ty: &IgniteType) -> TypeCode {
    match ty {
        IgniteType::Byte => TypeCode::Byte,
        IgniteType::String => TypeCode::String,
        IgniteType::Long => TypeCode::Long,
        IgniteType::Int => TypeCode::Int,
        IgniteType::Short => TypeCode::Short,
        IgniteType::Float => TypeCode::Float,
        IgniteType::Double => TypeCode::Double,
        IgniteType::Char => TypeCode::Char,
        IgniteType::Bool => TypeCode::Bool,
        IgniteType::Uuid => TypeCode::Uuid,
        IgniteType::Date => TypeCode::Date,
        IgniteType::Time => TypeCode::Time,
        IgniteType::Binary => TypeCode::ArrByte,
        IgniteType::Object => TypeCode::ComplexObj,
        IgniteType::Array => TypeCode::ArrObj,
        IgniteType::Enum => TypeCode::Enum,
        IgniteType::Timestamp => TypeCode::Timestamp,
        IgniteType::Decimal(_, _) => TypeCode::Decimal,
        IgniteType::Null => TypeCode::Null,
        IgniteType::Map => TypeCode::Map,
        IgniteType::Collection => TypeCode::Collection,
    }
}

fn bulk_put_params_metadata() -> BinaryTypeMetadata {
    metadata_for_schema(
        class_names::BULK_PUT_PARAMS,
        vec![("putParamsList", IgniteType::Collection)],
    )
}

fn put_params_metadata() -> BinaryTypeMetadata {
    // Field list mirrors `PutParams::to_binary` — order matters for schemaId.
    // NB: the Java `object` field is declared as `BinaryObject` which maps to
    // Ignite's `BINARY_OBJ` field-type id (27 = `WrappedData`), not the
    // generic `OBJ` id (103 = `ComplexObj`). The chosen id drives server-side
    // behaviour: `BINARY_OBJ` skips POJO deserialization and keeps the field
    // as a raw `BinaryObject` — required when the server does not have the
    // inner class (e.g. `UcpRecord`) on its classpath.
    let fields: Vec<IgniteField> = vec![
        ("cacheName", IgniteType::String),
        ("key", IgniteType::Object),
        ("object", IgniteType::Object),
        ("forceUpdate", IgniteType::Bool),
        ("indexContext", IgniteType::Object),
        ("isSecondaryIndexesExists", IgniteType::Bool),
        ("saveStrategy", IgniteType::Enum),
        ("index", IgniteType::Int),
    ]
    .into_iter()
    .map(|(name, ty)| IgniteField {
        name: name.to_string(),
        r#type: ty,
    })
    .collect();

    let type_code_for_field = |name: &str, ty: &IgniteType| -> TypeCode {
        if name == "object" {
            TypeCode::WrappedData
        } else {
            ignite_type_to_code(ty)
        }
    };
    let field_metas: Vec<BinaryFieldMetadata> = fields
        .iter()
        .map(|f| BinaryFieldMetadata {
            name: f.name.clone(),
            type_id: type_code_for_field(&f.name, &f.r#type) as i32,
            field_id: string_to_java_hashcode(&f.name.to_lowercase()),
        })
        .collect();

    BinaryTypeMetadata {
        type_id: string_to_java_hashcode(&class_names::PUT_PARAMS.to_lowercase()),
        type_name: class_names::PUT_PARAMS.to_string(),
        affinity_key_field_name: None,
        fields: field_metas.clone(),
        is_enum: false,
        enum_values: Vec::new(),
        schemas: vec![BinarySchema {
            id: get_schema_id(&fields),
            field_ids: field_metas.iter().map(|f| f.field_id).collect(),
        }],
    }
}

fn index_context_metadata() -> BinaryTypeMetadata {
    metadata_for_schema(
        class_names::INDEX_CONTEXT,
        vec![
            ("indexName2UniqueFactor", IgniteType::Map),
            ("indexName2AtomicFactor", IgniteType::Map),
            ("indexName2CacheName", IgniteType::Map),
        ],
    )
}

fn save_strategy_metadata() -> BinaryTypeMetadata {
    BinaryTypeMetadata {
        type_id: string_to_java_hashcode(&class_names::SAVE_STRATEGY.to_lowercase()),
        type_name: class_names::SAVE_STRATEGY.to_string(),
        affinity_key_field_name: None,
        fields: Vec::new(),
        is_enum: true,
        enum_values: vec![
            BinaryEnumVariant {
                name: "TRANSACTION".to_string(),
                ordinal: SaveStrategy::Transaction.ordinal(),
            },
            BinaryEnumVariant {
                name: "ATOMIC".to_string(),
                ordinal: SaveStrategy::Atomic.ordinal(),
            },
        ],
        schemas: Vec::new(),
    }
}

/// Register the response-side schemas (`BulkPutResponseParams`, `PutResult`)
/// in the Rust-side binary registry so the compute-task response decoder can
/// resolve field offsets → field names. We don't ship these to the server —
/// the server already has the classes (from the compute-tasks JAR) and
/// derives the metadata via reflection when it serializes the response.
fn prewarm_response_schemas() {
    // BulkPutResponseParams { putResultMap: Map }
    let _ = BinaryObjectBuilder::new(class_names::BULK_PUT_RESPONSE_PARAMS)
        .set_field_value("putResultMap", IgniteValue::Map(1, Vec::new()))
        .build();
    // PutResult { status: Enum, message: String, key: Object, index: Int }
    let _ = BinaryObjectBuilder::new(class_names::PUT_RESULT)
        .set_field_value(
            "status",
            IgniteValue::Enum(crate::Enum {
                type_id: 0,
                ordinal: 0,
            }),
        )
        .set_field_value("message", IgniteValue::Null)
        .set_field_value("key", IgniteValue::Long(0))
        .set_field_value("index", IgniteValue::Int(0))
        .build();
}

/// `(FQN, builder)` pairs for every POJO the server must deserialize on
/// input. Response-side POJOs (`BulkPutResponseParams`, `PutResult`,
/// `PutStatus`) are **not** registered here — the server has the classes on
/// its classpath (from the compute-tasks JAR) and discovers the metadata via
/// reflection when the job's reduce step serializes the output. Pre-emptively
/// registering them risks a "type for field 'X' differs" conflict with the
/// reflection-derived descriptor if our field-type ids don't match Ignite's
/// Java reflection modes exactly (e.g. `Map<K,V>` declared as the interface
/// maps to `OBJECT`, not `MAP`).
///
/// Iteration order is significant only for determinism — the server accepts
/// any order.
fn all_metadata_builders() -> Vec<(&'static str, fn() -> BinaryTypeMetadata)> {
    vec![
        (class_names::BULK_PUT_PARAMS, bulk_put_params_metadata),
        (class_names::PUT_PARAMS, put_params_metadata),
        (class_names::INDEX_CONTEXT, index_context_metadata),
        (class_names::SAVE_STRATEGY, save_strategy_metadata),
    ]
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Ignite collection-subtype byte for `java.util.ArrayList`.
const COLLECTION_SUBTYPE_ARRAYLIST: u8 = 1;

/// Ignite map-subtype byte for `java.util.HashMap`.
const MAP_SUBTYPE_HASHMAP: u8 = 1;

fn map_bool_to_ignite(m: &HashMap<String, bool>) -> IgniteValue {
    let entries: Vec<(IgniteValue, IgniteValue)> = m
        .iter()
        .map(|(k, v)| (IgniteValue::String(k.clone()), IgniteValue::Bool(*v)))
        .collect();
    IgniteValue::Map(MAP_SUBTYPE_HASHMAP, entries)
}

fn map_string_to_ignite(m: &HashMap<String, String>) -> IgniteValue {
    let entries: Vec<(IgniteValue, IgniteValue)> = m
        .iter()
        .map(|(k, v)| (IgniteValue::String(k.clone()), IgniteValue::String(v.clone())))
        .collect();
    IgniteValue::Map(MAP_SUBTYPE_HASHMAP, entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{read_i32, TypeCode};
    use std::io::Cursor;

    #[test]
    fn save_strategy_atomic_ordinal_matches_java_declaration_order() {
        assert_eq!(SaveStrategy::Transaction.ordinal(), 0);
        assert_eq!(SaveStrategy::Atomic.ordinal(), 1);
    }

    #[test]
    fn save_strategy_type_id_matches_java_fqn_hash() {
        // Java server-side: string.hashCode(lowerCase(FQN))
        let expected =
            string_to_java_hashcode(&class_names::SAVE_STRATEGY.to_lowercase());
        assert_eq!(SaveStrategy::type_id(), expected);
    }

    #[test]
    fn save_strategy_writes_as_enum_wire_value() {
        // Enum wire layout: typeCode(28) + typeId(i32) + ordinal(i32)
        let mut bytes = Vec::new();
        SaveStrategy::Atomic.as_ignite_value().write(&mut bytes).unwrap();

        let mut cur = Cursor::new(bytes);
        let code = read_u8(&mut cur).unwrap();
        assert_eq!(code, TypeCode::Enum as u8);
        let type_id = read_i32(&mut cur).unwrap();
        assert_eq!(type_id, SaveStrategy::type_id());
        let ordinal = read_i32(&mut cur).unwrap();
        assert_eq!(ordinal, 1);
    }

    #[test]
    fn index_context_empty_writes_as_complex_obj_with_three_map_fields() {
        let ctx = IndexContext::empty();
        let mut bytes = Vec::new();
        ctx.write(&mut bytes).unwrap();

        // First byte must be ComplexObj type code (103).
        assert_eq!(bytes[0], TypeCode::ComplexObj as u8);

        // Type id (offset 4, i32 LE) must match lower-cased FQN hashcode.
        let type_id = i32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
        assert_eq!(
            type_id,
            string_to_java_hashcode(&class_names::INDEX_CONTEXT.to_lowercase())
        );

        // Round-trip: parse back and confirm the three map fields are present
        // and empty.
        let mut cur = Cursor::new(bytes);
        let code = read_u8(&mut cur).unwrap();
        let obj = ComplexObject::read_unwrapped(code.try_into().unwrap(), &mut cur)
            .unwrap()
            .unwrap();

        for field_name in [
            "indexName2UniqueFactor",
            "indexName2AtomicFactor",
            "indexName2CacheName",
        ] {
            match obj.field(field_name) {
                Some(IgniteValue::Map(_, entries)) => assert!(entries.is_empty()),
                other => panic!("field {} expected empty Map, got {:?}", field_name, other),
            }
        }
    }

    #[test]
    fn put_params_round_trips_all_fields() {
        let mut uniq = HashMap::new();
        uniq.insert("idx1".to_string(), true);
        let mut cache_names = HashMap::new();
        cache_names.insert("idx1".to_string(), "IDX_CACHE_A".to_string());
        let ctx = IndexContext {
            index_name_to_unique: uniq,
            index_name_to_atomic: HashMap::new(),
            index_name_to_cache_name: cache_names,
        };

        // Use a tiny payload BinaryObject for `object` — two primitive fields.
        let payload = BinaryObjectBuilder::new("com.example.Payload")
            .set_field("id", 42i32)
            .set_field("version", 7i32)
            .build();

        let pp = PutParams::new(
            "DATA_RSC_INDIVIDUAL",
            IgniteValue::Long(12345),
            payload,
            true,
            ctx,
            false,
            SaveStrategy::Atomic,
        );

        let mut bytes = Vec::new();
        pp.write(&mut bytes).unwrap();
        // Sanity: first byte ComplexObj, 5-byte skew = version byte + 2-byte flags.
        assert_eq!(bytes[0], TypeCode::ComplexObj as u8);

        // Parse back.
        let mut cur = Cursor::new(bytes);
        let code = read_u8(&mut cur).unwrap();
        let obj = ComplexObject::read_unwrapped(code.try_into().unwrap(), &mut cur)
            .unwrap()
            .unwrap();
        match obj.field("cacheName") {
            Some(IgniteValue::String(s)) => assert_eq!(s, "DATA_RSC_INDIVIDUAL"),
            other => panic!("cacheName: {:?}", other),
        }
        match obj.field("key") {
            Some(IgniteValue::Long(v)) => assert_eq!(*v, 12345),
            other => panic!("key: {:?}", other),
        }
        match obj.field("forceUpdate") {
            Some(IgniteValue::Bool(v)) => assert!(*v),
            other => panic!("forceUpdate: {:?}", other),
        }
        match obj.field("isSecondaryIndexesExists") {
            Some(IgniteValue::Bool(v)) => assert!(!(*v)),
            other => panic!("isSecondaryIndexesExists: {:?}", other),
        }
        match obj.field("saveStrategy") {
            Some(IgniteValue::Enum(e)) => {
                assert_eq!(e.type_id, SaveStrategy::type_id());
                assert_eq!(e.ordinal, SaveStrategy::Atomic.ordinal());
            }
            other => panic!("saveStrategy: {:?}", other),
        }
        match obj.field("index") {
            Some(IgniteValue::Int(v)) => assert_eq!(*v, 0),
            other => panic!("index: {:?}", other),
        }
        // Nested BinaryObject lookups work — the `object` field should be
        // Object-typed (either wrapped or flattened into its single primitive).
        match obj.field("object") {
            Some(IgniteValue::Object(_)) => {}
            other => panic!("object: expected Object, got {:?}", other),
        }
        match obj.field("indexContext") {
            Some(IgniteValue::Object(_)) => {}
            other => panic!("indexContext: expected Object, got {:?}", other),
        }
    }

    #[test]
    fn bulk_put_params_collects_put_params_as_arraylist() {
        let payload = BinaryObjectBuilder::new("com.example.Payload")
            .set_field("v", 1i32)
            .build();
        let pp = PutParams::new(
            "C",
            IgniteValue::Int(1),
            payload,
            false,
            IndexContext::empty(),
            false,
            SaveStrategy::Atomic,
        );
        let bp = BulkPutParams::new(vec![pp.clone(), pp.clone(), pp]);

        let mut bytes = Vec::new();
        bp.write(&mut bytes).unwrap();
        assert_eq!(bytes[0], TypeCode::ComplexObj as u8);

        // Parse back and verify the collection subtype + size.
        let mut cur = Cursor::new(bytes);
        let code = read_u8(&mut cur).unwrap();
        let obj = ComplexObject::read_unwrapped(code.try_into().unwrap(), &mut cur)
            .unwrap()
            .unwrap();
        match obj.field("putParamsList") {
            Some(IgniteValue::Collection(subtype, items)) => {
                assert_eq!(*subtype, COLLECTION_SUBTYPE_ARRAYLIST);
                assert_eq!(items.len(), 3);
            }
            other => panic!("putParamsList: expected Collection, got {:?}", other),
        }
    }

    #[test]
    fn bulk_put_response_decodes_put_result_map() {
        // Synthesize a BulkPutResponseParams ComplexObject by building it
        // with the same machinery used server-side — this round-trips the
        // decoder against its symmetric encoder for coverage. A genuine
        // Java-side golden payload would also be valid here if captured.
        use crate::protocol::complex_obj::ComplexObjectSchema;
        use std::sync::Arc;

        let put_result_type_id =
            string_to_java_hashcode(&class_names::PUT_RESULT.to_lowercase());
        let put_status_type_id =
            string_to_java_hashcode(&class_names::PUT_STATUS.to_lowercase());

        let pr_bytes = |status_ord: i32, message: Option<&str>, key_int: i32, index: i32| {
            let msg_val = match message {
                Some(m) => IgniteValue::String(m.to_string()),
                None => IgniteValue::Null,
            };
            ComplexObject {
                schema: Arc::new(ComplexObjectSchema {
                    type_name: class_names::PUT_RESULT.to_string(),
                    fields: vec![
                        crate::protocol::complex_obj::IgniteField {
                            name: "status".to_string(),
                            r#type: crate::protocol::complex_obj::IgniteType::Enum,
                        },
                        crate::protocol::complex_obj::IgniteField {
                            name: "message".to_string(),
                            r#type: crate::protocol::complex_obj::IgniteType::String,
                        },
                        crate::protocol::complex_obj::IgniteField {
                            name: "key".to_string(),
                            r#type: crate::protocol::complex_obj::IgniteType::Int,
                        },
                        crate::protocol::complex_obj::IgniteField {
                            name: "index".to_string(),
                            r#type: crate::protocol::complex_obj::IgniteType::Int,
                        },
                    ],
                }),
                values: vec![
                    IgniteValue::Enum(crate::Enum {
                        type_id: put_status_type_id,
                        ordinal: status_ord,
                    }),
                    msg_val,
                    IgniteValue::Int(key_int),
                    IgniteValue::Int(index),
                ],
            }
        };

        // Two entries: index=0 (Added, key=1), index=1 (AlreadyExist, key=2)
        let entries = vec![
            (IgniteValue::Int(0), IgniteValue::Object(Box::new(pr_bytes(0, None, 1, 0)))),
            (
                IgniteValue::Int(1),
                IgniteValue::Object(Box::new(pr_bytes(3, None, 2, 1))),
            ),
        ];
        let map_val = IgniteValue::Map(MAP_SUBTYPE_HASHMAP, entries);

        let resp_obj = ComplexObject {
            schema: Arc::new(ComplexObjectSchema {
                type_name: class_names::BULK_PUT_RESPONSE_PARAMS.to_string(),
                fields: vec![crate::protocol::complex_obj::IgniteField {
                    name: "putResultMap".to_string(),
                    r#type: crate::protocol::complex_obj::IgniteType::Map,
                }],
            }),
            values: vec![map_val],
        };

        let mut bytes = Vec::new();
        resp_obj.write(&mut bytes).unwrap();

        // We need the registered put_result type to decode the inner Object
        // fields. Register it by writing a PutResult ComplexObject once — it
        // self-registers into the binary registry.
        let mut sink = Vec::new();
        pr_bytes(0, None, 1, 0).write(&mut sink).unwrap();
        // Also register BulkPutResponseParams type.
        let mut sink2 = Vec::new();
        resp_obj.write(&mut sink2).unwrap();

        let _ = put_result_type_id; // keep var for debugging

        // Decode via BulkPutResponseParams::from_object (skips the wire envelope
        // for this particular roundtrip test).
        let parsed = BulkPutResponseParams::from_object(&resp_obj).unwrap();
        assert_eq!(parsed.put_result_map.len(), 2);
        assert_eq!(parsed.put_result_map[&0].status, PutStatus::Added);
        assert_eq!(parsed.put_result_map[&1].status, PutStatus::AlreadyExist);
        assert_eq!(parsed.put_result_map[&0].index, 0);
        assert_eq!(parsed.put_result_map[&1].index, 1);
    }
}
