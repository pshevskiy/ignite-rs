use crate::protocol::complex_obj::{ComplexObjectSchema, IgniteField, IgniteType};
use crate::protocol::TypeCode;
use crate::utils::{get_schema_id, string_to_java_hashcode};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, OnceLock, RwLock};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RegisteredBinaryField {
    pub(crate) name: String,
    pub(crate) type_id: i32,
    pub(crate) field_id: i32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RegisteredBinarySchema {
    pub(crate) id: i32,
    pub(crate) field_ids: Vec<i32>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RegisteredBinaryType {
    pub(crate) type_id: i32,
    pub(crate) type_name: String,
    pub(crate) affinity_key_field_name: Option<String>,
    pub(crate) fields: Vec<RegisteredBinaryField>,
    pub(crate) is_enum: bool,
    pub(crate) enum_values: Vec<(String, i32)>,
    pub(crate) schemas: Vec<RegisteredBinarySchema>,
}

#[derive(Default)]
struct BinaryRegistry {
    by_type_id: HashMap<i32, RegisteredBinaryType>,
}

fn registry() -> &'static RwLock<BinaryRegistry> {
    static REGISTRY: OnceLock<RwLock<BinaryRegistry>> = OnceLock::new();
    REGISTRY.get_or_init(|| RwLock::new(BinaryRegistry::default()))
}

pub(crate) fn register_complex_schema(schema: &ComplexObjectSchema) {
    register_type(registered_type_from_schema(schema));
}

pub(crate) fn register_type(meta: RegisteredBinaryType) {
    let mut registry = registry()
        .write()
        .expect("binary registry write lock poisoned");

    match registry.by_type_id.get_mut(&meta.type_id) {
        Some(existing) => merge_registered_type(existing, meta),
        None => {
            registry.by_type_id.insert(meta.type_id, meta);
        }
    }
}

pub(crate) fn type_by_id(type_id: i32) -> Option<RegisteredBinaryType> {
    registry()
        .read()
        .expect("binary registry read lock poisoned")
        .by_type_id
        .get(&type_id)
        .cloned()
}

pub(crate) fn type_name(type_id: i32) -> Option<String> {
    type_by_id(type_id).map(|meta| meta.type_name)
}

pub(crate) fn schema_for(type_id: i32, schema_id: i32) -> Option<Arc<ComplexObjectSchema>> {
    let meta = type_by_id(type_id)?;
    let mut fields_by_id = BTreeMap::new();
    for field in &meta.fields {
        fields_by_id.insert(
            field.field_id,
            IgniteField {
                name: field.name.clone(),
                r#type: ignite_type_from_type_id(field.type_id),
            },
        );
    }

    if let Some(schema) = meta.schemas.iter().find(|schema| schema.id == schema_id) {
        let fields = schema
            .field_ids
            .iter()
            .filter_map(|field_id| fields_by_id.get(field_id).cloned())
            .collect::<Vec<_>>();

        return Some(Arc::new(ComplexObjectSchema {
            type_name: meta.type_name,
            fields,
        }));
    }

    Some(Arc::new(ComplexObjectSchema {
        type_name: meta.type_name,
        fields: fields_by_id.into_values().collect(),
    }))
}

pub(crate) fn registered_type_from_schema(schema: &ComplexObjectSchema) -> RegisteredBinaryType {
    let fields = schema
        .fields
        .iter()
        .map(|field| RegisteredBinaryField {
            name: field.name.clone(),
            type_id: ignite_type_id(&field.r#type),
            field_id: string_to_java_hashcode(field.name.to_lowercase().as_str()),
        })
        .collect::<Vec<_>>();

    let field_ids = fields
        .iter()
        .map(|field| field.field_id)
        .collect::<Vec<_>>();

    RegisteredBinaryType {
        type_id: string_to_java_hashcode(schema.type_name.to_lowercase().as_str()),
        type_name: schema.type_name.clone(),
        affinity_key_field_name: None,
        fields,
        is_enum: false,
        enum_values: Vec::new(),
        schemas: vec![RegisteredBinarySchema {
            id: get_schema_id(&schema.fields),
            field_ids,
        }],
    }
}

pub(crate) fn ignite_type_id(ty: &IgniteType) -> i32 {
    match ty {
        IgniteType::Byte => TypeCode::Byte as i32,
        IgniteType::Short => TypeCode::Short as i32,
        IgniteType::Int => TypeCode::Int as i32,
        IgniteType::Long => TypeCode::Long as i32,
        IgniteType::Float => TypeCode::Float as i32,
        IgniteType::Double => TypeCode::Double as i32,
        IgniteType::Char => TypeCode::Char as i32,
        IgniteType::Bool => TypeCode::Bool as i32,
        IgniteType::String => TypeCode::String as i32,
        IgniteType::Uuid => TypeCode::Uuid as i32,
        IgniteType::Date => TypeCode::Date as i32,
        IgniteType::Binary => TypeCode::ArrByte as i32,
        IgniteType::Object => TypeCode::ComplexObj as i32,
        IgniteType::Array => TypeCode::ArrObj as i32,
        IgniteType::Timestamp => TypeCode::Timestamp as i32,
        IgniteType::Time => TypeCode::Time as i32,
        IgniteType::Decimal(_, _) => TypeCode::Decimal as i32,
        IgniteType::Enum => TypeCode::Enum as i32,
        IgniteType::Null => TypeCode::Null as i32,
    }
}

fn ignite_type_from_type_id(type_id: i32) -> IgniteType {
    match type_id {
        x if x == TypeCode::Byte as i32 => IgniteType::Byte,
        x if x == TypeCode::Short as i32 => IgniteType::Short,
        x if x == TypeCode::Int as i32 => IgniteType::Int,
        x if x == TypeCode::Long as i32 => IgniteType::Long,
        x if x == TypeCode::Float as i32 => IgniteType::Float,
        x if x == TypeCode::Double as i32 => IgniteType::Double,
        x if x == TypeCode::Char as i32 => IgniteType::Char,
        x if x == TypeCode::Bool as i32 => IgniteType::Bool,
        x if x == TypeCode::String as i32 => IgniteType::String,
        x if x == TypeCode::Uuid as i32 => IgniteType::Uuid,
        x if x == TypeCode::Date as i32 => IgniteType::Date,
        x if x == TypeCode::ArrByte as i32 => IgniteType::Binary,
        x if x == TypeCode::ComplexObj as i32 => IgniteType::Object,
        x if x == TypeCode::ArrObj as i32 => IgniteType::Array,
        x if x == TypeCode::Timestamp as i32 => IgniteType::Timestamp,
        x if x == TypeCode::Time as i32 => IgniteType::Time,
        x if x == TypeCode::Decimal as i32 => IgniteType::Decimal(0, 0),
        x if x == TypeCode::Enum as i32 || x == TypeCode::BinaryEnum as i32 => IgniteType::Enum,
        _ => IgniteType::Null,
    }
}

fn merge_registered_type(existing: &mut RegisteredBinaryType, incoming: RegisteredBinaryType) {
    if existing.type_name.is_empty() {
        existing.type_name = incoming.type_name;
    }
    if existing.affinity_key_field_name.is_none() {
        existing.affinity_key_field_name = incoming.affinity_key_field_name;
    }
    existing.is_enum |= incoming.is_enum;

    let mut field_names = existing
        .fields
        .iter()
        .map(|field| field.name.clone())
        .collect::<Vec<_>>();
    for field in incoming.fields {
        if !field_names.iter().any(|name| *name == field.name) {
            field_names.push(field.name.clone());
            existing.fields.push(field);
        }
    }

    for enum_value in incoming.enum_values {
        if !existing
            .enum_values
            .iter()
            .any(|value| value == &enum_value)
        {
            existing.enum_values.push(enum_value);
        }
    }

    for schema in incoming.schemas {
        if !existing
            .schemas
            .iter()
            .any(|existing_schema| existing_schema.id == schema.id)
        {
            existing.schemas.push(schema);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        register_type, type_by_id, type_name, RegisteredBinaryField, RegisteredBinarySchema,
        RegisteredBinaryType,
    };

    fn clear_registry() {
        super::registry()
            .write()
            .expect("binary registry write lock poisoned")
            .by_type_id
            .clear();
    }

    #[test]
    fn should_return_cached_type_name_after_registration() {
        clear_registry();

        register_type(RegisteredBinaryType {
            type_id: 555,
            type_name: "java.time.LocalDateTime".to_string(),
            affinity_key_field_name: None,
            fields: Vec::new(),
            is_enum: false,
            enum_values: Vec::new(),
            schemas: Vec::new(),
        });

        assert_eq!(type_name(555).as_deref(), Some("java.time.LocalDateTime"));
    }

    #[test]
    fn should_merge_registered_metadata_without_losing_cached_type_name() {
        clear_registry();

        register_type(RegisteredBinaryType {
            type_id: 777,
            type_name: "example.Type".to_string(),
            affinity_key_field_name: None,
            fields: Vec::new(),
            is_enum: false,
            enum_values: Vec::new(),
            schemas: Vec::new(),
        });

        register_type(RegisteredBinaryType {
            type_id: 777,
            type_name: String::new(),
            affinity_key_field_name: None,
            fields: vec![RegisteredBinaryField {
                name: "name".to_string(),
                type_id: 9,
                field_id: 12,
            }],
            is_enum: false,
            enum_values: Vec::new(),
            schemas: vec![RegisteredBinarySchema {
                id: 12,
                field_ids: vec![12],
            }],
        });

        let registered = type_by_id(777).expect("registered type");
        assert_eq!(registered.type_name, "example.Type");
        assert_eq!(registered.fields.len(), 1);
        assert_eq!(registered.schemas.len(), 1);
    }
}
