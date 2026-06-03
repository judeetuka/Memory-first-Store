//! Schema definitions for the app-facing memory-first store path.

use std::collections::BTreeSet;
use std::fmt;

const MAX_ARRAY_NESTING: usize = 8;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    pub name: String,
    pub fields: Vec<SchemaField>,
    pub enable_nested_fields: bool,
    pub default_sort_field: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaField {
    pub name: String,
    pub field_type: SchemaFieldType,
    pub primary: bool,
    pub optional: bool,
    pub indexed: bool,
    pub stored: bool,
    pub unique: bool,
    pub reference: Option<Reference>,
    pub sort: bool,
    pub range_index: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaFieldType {
    String,
    Int32,
    Int64,
    Float,
    Bool,
    Object(Vec<SchemaField>),
    Array(Box<SchemaFieldType>),
    Json,
    Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reference {
    pub collection: String,
    pub field: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaError {
    EmptySchemaName,
    InvalidSchemaName { name: String },
    EmptyFields,
    EmptyFieldName,
    InvalidFieldPath { name: String },
    NestedFieldDisabled { name: String },
    DuplicateField { name: String },
    MissingPrimaryField,
    MultiplePrimaryFields,
    PrimaryFieldOptional { name: String },
    PrimaryFieldNotStored { name: String },
    InvalidPrimaryType { name: String },
    UnsupportedIndexType { name: String },
    UniqueFieldNotIndexed { name: String },
    UniqueFieldOptional { name: String },
    SortFieldNotIndexed { name: String },
    UnsupportedSortType { name: String },
    RangeFieldNotIndexed { name: String },
    UnsupportedRangeIndexType { name: String },
    InvalidReference { value: String },
    UnsupportedReferenceType { name: String },
    ObjectFieldUsesOperationalFlag { name: String, flag: &'static str },
    DefaultSortFieldMissing { name: String },
    DefaultSortFieldNotSortable { name: String },
    UnknownFieldType { field_type: String },
    ArrayNestingTooDeep { field_type: String },
    JsonParse,
    JsonShape(&'static str),
}

impl Schema {
    pub fn new(name: impl Into<String>, fields: Vec<SchemaField>) -> Self {
        Self {
            name: name.into(),
            fields,
            enable_nested_fields: false,
            default_sort_field: None,
        }
    }

    pub fn validate(&self) -> Result<(), SchemaError> {
        validate_collection_name(&self.name)?;

        if self.fields.is_empty() {
            return Err(SchemaError::EmptyFields);
        }

        let mut seen = BTreeSet::new();
        let mut primary_count = 0usize;

        for field in &self.fields {
            self.validate_field(field)?;

            if !seen.insert(field.name.clone()) {
                return Err(SchemaError::DuplicateField {
                    name: field.name.clone(),
                });
            }

            if field.primary {
                primary_count += 1;
                validate_primary_field(field)?;
            }
        }

        match primary_count {
            0 => return Err(SchemaError::MissingPrimaryField),
            1 => {}
            _ => return Err(SchemaError::MultiplePrimaryFields),
        }

        if let Some(default_sort_field) = &self.default_sort_field {
            let field = self.field(default_sort_field).ok_or_else(|| {
                SchemaError::DefaultSortFieldMissing {
                    name: default_sort_field.clone(),
                }
            })?;

            if !field.sort {
                return Err(SchemaError::DefaultSortFieldNotSortable {
                    name: default_sort_field.clone(),
                });
            }
        }

        Ok(())
    }

    pub fn field(&self, name: &str) -> Option<&SchemaField> {
        self.fields.iter().find(|field| field.name == name)
    }

    pub fn primary_field(&self) -> Option<&SchemaField> {
        self.fields.iter().find(|field| field.primary)
    }

    #[cfg(feature = "json")]
    pub fn from_json_str(input: &str) -> Result<Self, SchemaError> {
        let value: serde_json::Value =
            serde_json::from_str(input).map_err(|_| SchemaError::JsonParse)?;
        let schema = parse_schema_value(&value)?;
        schema.validate()?;
        Ok(schema)
    }

    fn validate_field(&self, field: &SchemaField) -> Result<(), SchemaError> {
        validate_field_name(&field.name)?;

        if !self.enable_nested_fields && field.name.contains('.') {
            return Err(SchemaError::NestedFieldDisabled {
                name: field.name.clone(),
            });
        }

        validate_field_type(&field.field_type)?;

        if field.indexed && !field.field_type.is_indexable() {
            return Err(SchemaError::UnsupportedIndexType {
                name: field.name.clone(),
            });
        }

        if field.unique && !field.indexed {
            return Err(SchemaError::UniqueFieldNotIndexed {
                name: field.name.clone(),
            });
        }

        if field.unique && field.optional {
            return Err(SchemaError::UniqueFieldOptional {
                name: field.name.clone(),
            });
        }

        if field.sort && !field.indexed {
            return Err(SchemaError::SortFieldNotIndexed {
                name: field.name.clone(),
            });
        }

        if field.sort && !field.field_type.is_sortable() {
            return Err(SchemaError::UnsupportedSortType {
                name: field.name.clone(),
            });
        }

        if field.range_index && !field.indexed {
            return Err(SchemaError::RangeFieldNotIndexed {
                name: field.name.clone(),
            });
        }

        if field.range_index && !field.field_type.is_numeric() {
            return Err(SchemaError::UnsupportedRangeIndexType {
                name: field.name.clone(),
            });
        }

        if let Some(reference) = &field.reference {
            reference.validate()?;

            if !field.field_type.is_reference_compatible() {
                return Err(SchemaError::UnsupportedReferenceType {
                    name: field.name.clone(),
                });
            }
        }

        Ok(())
    }
}

impl SchemaField {
    pub fn new(name: impl Into<String>, field_type: SchemaFieldType) -> Self {
        Self {
            name: name.into(),
            field_type,
            primary: false,
            optional: false,
            indexed: false,
            stored: true,
            unique: false,
            reference: None,
            sort: false,
            range_index: false,
        }
    }
}

impl SchemaFieldType {
    pub fn parse(input: &str) -> Result<Self, SchemaError> {
        let mut base = input;
        let mut array_depth = 0usize;

        while let Some(inner) = base.strip_suffix("[]") {
            if inner.is_empty() {
                return Err(SchemaError::UnknownFieldType {
                    field_type: input.to_string(),
                });
            }

            array_depth += 1;
            if array_depth > MAX_ARRAY_NESTING {
                return Err(SchemaError::ArrayNestingTooDeep {
                    field_type: input.to_string(),
                });
            }

            base = inner;
        }

        let mut field_type = match base {
            "string" => Ok(Self::String),
            "int32" => Ok(Self::Int32),
            "int64" => Ok(Self::Int64),
            "float" => Ok(Self::Float),
            "bool" => Ok(Self::Bool),
            "object" => Ok(Self::Object(Vec::new())),
            "json" => Ok(Self::Json),
            "bytes" => Ok(Self::Bytes),
            _ => Err(SchemaError::UnknownFieldType {
                field_type: input.to_string(),
            }),
        }?;

        for _ in 0..array_depth {
            field_type = Self::Array(Box::new(field_type));
        }

        Ok(field_type)
    }

    pub fn is_indexable(&self) -> bool {
        match self {
            Self::String | Self::Int32 | Self::Int64 | Self::Float | Self::Bool | Self::Bytes => {
                true
            }
            Self::Array(inner) => match inner.as_ref() {
                Self::Array(_) => false,
                _ => inner.is_indexable(),
            },
            Self::Object(_) | Self::Json => false,
        }
    }

    pub fn is_sortable(&self) -> bool {
        match self {
            Self::String | Self::Int32 | Self::Int64 | Self::Float | Self::Bool | Self::Bytes => {
                true
            }
            Self::Object(_) | Self::Array(_) | Self::Json => false,
        }
    }

    pub fn is_numeric(&self) -> bool {
        match self {
            Self::Int32 | Self::Int64 | Self::Float => true,
            Self::String
            | Self::Bool
            | Self::Object(_)
            | Self::Array(_)
            | Self::Json
            | Self::Bytes => false,
        }
    }

    pub fn is_reference_compatible(&self) -> bool {
        match self {
            Self::String | Self::Int32 | Self::Int64 | Self::Bytes => true,
            Self::Float | Self::Bool | Self::Object(_) | Self::Array(_) | Self::Json => false,
        }
    }
}

impl Reference {
    pub fn new(collection: impl Into<String>, field: impl Into<String>) -> Self {
        Self {
            collection: collection.into(),
            field: field.into(),
        }
    }

    pub fn parse(input: &str) -> Result<Self, SchemaError> {
        let (collection, field) =
            input
                .split_once('.')
                .ok_or_else(|| SchemaError::InvalidReference {
                    value: input.to_string(),
                })?;

        let reference = Self::new(collection, field);
        reference
            .validate()
            .map_err(|_| SchemaError::InvalidReference {
                value: input.to_string(),
            })?;
        Ok(reference)
    }

    pub fn validate(&self) -> Result<(), SchemaError> {
        validate_collection_name(&self.collection).map_err(|_| SchemaError::InvalidReference {
            value: format!("{}.{}", self.collection, self.field),
        })?;
        validate_field_name(&self.field).map_err(|_| SchemaError::InvalidReference {
            value: format!("{}.{}", self.collection, self.field),
        })?;
        Ok(())
    }
}

impl fmt::Display for SchemaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptySchemaName => write!(f, "schema name is empty"),
            Self::InvalidSchemaName { name } => write!(f, "invalid schema name `{name}`"),
            Self::EmptyFields => write!(f, "schema must define at least one field"),
            Self::EmptyFieldName => write!(f, "field name is empty"),
            Self::InvalidFieldPath { name } => write!(f, "invalid field path `{name}`"),
            Self::NestedFieldDisabled { name } => {
                write!(
                    f,
                    "field `{name}` uses dot notation while nested fields are disabled"
                )
            }
            Self::DuplicateField { name } => write!(f, "duplicate field `{name}`"),
            Self::MissingPrimaryField => write!(f, "schema must define one primary field"),
            Self::MultiplePrimaryFields => write!(f, "schema defines more than one primary field"),
            Self::PrimaryFieldOptional { name } => {
                write!(f, "primary field `{name}` cannot be optional")
            }
            Self::PrimaryFieldNotStored { name } => {
                write!(f, "primary field `{name}` must be stored")
            }
            Self::InvalidPrimaryType { name } => {
                write!(f, "primary field `{name}` uses an unsupported type")
            }
            Self::UnsupportedIndexType { name } => {
                write!(f, "field `{name}` uses an unsupported indexed type")
            }
            Self::UniqueFieldNotIndexed { name } => {
                write!(f, "unique field `{name}` must be indexed")
            }
            Self::UniqueFieldOptional { name } => {
                write!(f, "unique field `{name}` cannot be optional")
            }
            Self::SortFieldNotIndexed { name } => {
                write!(f, "sort field `{name}` must be indexed")
            }
            Self::UnsupportedSortType { name } => {
                write!(f, "field `{name}` uses an unsupported sort type")
            }
            Self::RangeFieldNotIndexed { name } => {
                write!(f, "range-index field `{name}` must be indexed")
            }
            Self::UnsupportedRangeIndexType { name } => {
                write!(f, "field `{name}` uses an unsupported range-index type")
            }
            Self::InvalidReference { value } => write!(f, "invalid reference `{value}`"),
            Self::UnsupportedReferenceType { name } => {
                write!(f, "field `{name}` uses an unsupported reference type")
            }
            Self::ObjectFieldUsesOperationalFlag { name, flag } => {
                write!(f, "object field `{name}` cannot use `{flag}`")
            }
            Self::DefaultSortFieldMissing { name } => {
                write!(f, "default sort field `{name}` is missing")
            }
            Self::DefaultSortFieldNotSortable { name } => {
                write!(f, "default sort field `{name}` is not marked sortable")
            }
            Self::UnknownFieldType { field_type } => {
                write!(f, "unknown schema field type `{field_type}`")
            }
            Self::ArrayNestingTooDeep { field_type } => {
                write!(
                    f,
                    "schema field type `{field_type}` exceeds max array nesting"
                )
            }
            Self::JsonParse => write!(f, "schema JSON could not be parsed"),
            Self::JsonShape(message) => write!(f, "invalid schema JSON shape: {message}"),
        }
    }
}

impl std::error::Error for SchemaError {}

fn validate_collection_name(name: &str) -> Result<(), SchemaError> {
    if name.is_empty() {
        return Err(SchemaError::EmptySchemaName);
    }

    if !is_identifier_segment(name) {
        return Err(SchemaError::InvalidSchemaName {
            name: name.to_string(),
        });
    }

    Ok(())
}

fn validate_field_name(name: &str) -> Result<(), SchemaError> {
    if name.is_empty() {
        return Err(SchemaError::EmptyFieldName);
    }

    if name
        .split('.')
        .any(|segment| !is_identifier_segment(segment))
    {
        return Err(SchemaError::InvalidFieldPath {
            name: name.to_string(),
        });
    }

    Ok(())
}

fn validate_field_type(field_type: &SchemaFieldType) -> Result<(), SchemaError> {
    match field_type {
        SchemaFieldType::Object(fields) => validate_object_fields(fields),
        SchemaFieldType::Array(inner) => validate_field_type(inner),
        _ => Ok(()),
    }
}

fn validate_object_fields(fields: &[SchemaField]) -> Result<(), SchemaError> {
    let mut seen = BTreeSet::new();

    for field in fields {
        validate_object_field(field)?;

        if !seen.insert(field.name.clone()) {
            return Err(SchemaError::DuplicateField {
                name: field.name.clone(),
            });
        }
    }

    Ok(())
}

fn validate_object_field(field: &SchemaField) -> Result<(), SchemaError> {
    validate_field_name(&field.name)?;
    validate_field_type(&field.field_type)?;

    if field.primary {
        return Err(SchemaError::ObjectFieldUsesOperationalFlag {
            name: field.name.clone(),
            flag: "primary",
        });
    }

    if field.indexed {
        return Err(SchemaError::ObjectFieldUsesOperationalFlag {
            name: field.name.clone(),
            flag: "indexed",
        });
    }

    if field.unique {
        return Err(SchemaError::ObjectFieldUsesOperationalFlag {
            name: field.name.clone(),
            flag: "unique",
        });
    }

    if field.sort {
        return Err(SchemaError::ObjectFieldUsesOperationalFlag {
            name: field.name.clone(),
            flag: "sort",
        });
    }

    if field.range_index {
        return Err(SchemaError::ObjectFieldUsesOperationalFlag {
            name: field.name.clone(),
            flag: "range_index",
        });
    }

    if field.reference.is_some() {
        return Err(SchemaError::ObjectFieldUsesOperationalFlag {
            name: field.name.clone(),
            flag: "reference",
        });
    }

    Ok(())
}

fn validate_primary_field(field: &SchemaField) -> Result<(), SchemaError> {
    if field.optional {
        return Err(SchemaError::PrimaryFieldOptional {
            name: field.name.clone(),
        });
    }

    if !field.stored {
        return Err(SchemaError::PrimaryFieldNotStored {
            name: field.name.clone(),
        });
    }

    if !field.field_type.is_reference_compatible() {
        return Err(SchemaError::InvalidPrimaryType {
            name: field.name.clone(),
        });
    }

    Ok(())
}

fn is_identifier_segment(segment: &str) -> bool {
    let mut chars = segment.chars();
    let Some(first) = chars.next() else {
        return false;
    };

    if !(first.is_ascii_alphabetic() || first == '_') {
        return false;
    }

    chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-')
}

#[cfg(feature = "json")]
fn parse_schema_value(value: &serde_json::Value) -> Result<Schema, SchemaError> {
    let object = value
        .as_object()
        .ok_or(SchemaError::JsonShape("schema root must be an object"))?;
    let name = required_string(object, "name", "schema.name must be a string")?;
    let fields_value = object
        .get("fields")
        .ok_or(SchemaError::JsonShape("schema.fields is required"))?;

    Ok(Schema {
        name: name.to_string(),
        fields: parse_fields_value(fields_value)?,
        enable_nested_fields: optional_bool(object, "enable_nested_fields", false)?,
        default_sort_field: optional_string(object, "default_sort_field")?.map(str::to_string),
    })
}

#[cfg(feature = "json")]
fn parse_fields_value(value: &serde_json::Value) -> Result<Vec<SchemaField>, SchemaError> {
    let fields = value
        .as_array()
        .ok_or(SchemaError::JsonShape("fields must be an array"))?;
    fields.iter().map(parse_field_value).collect()
}

#[cfg(feature = "json")]
fn parse_field_value(value: &serde_json::Value) -> Result<SchemaField, SchemaError> {
    let object = value
        .as_object()
        .ok_or(SchemaError::JsonShape("field must be an object"))?;
    let name = required_string(object, "name", "field.name must be a string")?;
    let field_type_value = object
        .get("type")
        .ok_or(SchemaError::JsonShape("field.type is required"))?;
    let field_type = parse_field_type_value(field_type_value, object)?;
    let reference = match object.get("reference") {
        Some(value) if value.is_null() => None,
        Some(value) => {
            Some(Reference::parse(value.as_str().ok_or(
                SchemaError::JsonShape("field.reference must be a string"),
            )?)?)
        }
        None => None,
    };

    Ok(SchemaField {
        name: name.to_string(),
        field_type,
        primary: optional_bool(object, "primary", false)?,
        optional: optional_bool(object, "optional", false)?,
        indexed: optional_bool(object, "indexed", false)?,
        stored: optional_bool(object, "stored", true)?,
        unique: optional_bool(object, "unique", false)?,
        reference,
        sort: optional_bool(object, "sort", false)?,
        range_index: optional_bool(object, "range_index", false)?,
    })
}

#[cfg(feature = "json")]
fn parse_field_type_value(
    value: &serde_json::Value,
    field_object: &serde_json::Map<String, serde_json::Value>,
) -> Result<SchemaFieldType, SchemaError> {
    match value {
        serde_json::Value::String(field_type) => {
            let parsed = SchemaFieldType::parse(field_type)?;
            attach_inline_object_fields(parsed, field_object)
        }
        serde_json::Value::Object(object) => {
            if let Some(array_type) = object.get("array") {
                let inner = parse_field_type_value(array_type, object)?;
                return Ok(SchemaFieldType::Array(Box::new(inner)));
            }

            if let Some(object_fields) = object.get("object") {
                return Ok(SchemaFieldType::Object(parse_fields_value(object_fields)?));
            }

            Err(SchemaError::JsonShape(
                "field.type object must contain array or object",
            ))
        }
        _ => Err(SchemaError::JsonShape(
            "field.type must be a string or object",
        )),
    }
}

#[cfg(feature = "json")]
fn attach_inline_object_fields(
    field_type: SchemaFieldType,
    field_object: &serde_json::Map<String, serde_json::Value>,
) -> Result<SchemaFieldType, SchemaError> {
    let Some(fields) = field_object.get("fields") else {
        return Ok(field_type);
    };

    match field_type {
        SchemaFieldType::Object(_) => Ok(SchemaFieldType::Object(parse_fields_value(fields)?)),
        SchemaFieldType::Array(inner) if matches!(inner.as_ref(), SchemaFieldType::Object(_)) => {
            Ok(SchemaFieldType::Array(Box::new(SchemaFieldType::Object(
                parse_fields_value(fields)?,
            ))))
        }
        _ => Err(SchemaError::JsonShape(
            "field.fields is only valid for object field types",
        )),
    }
}

#[cfg(feature = "json")]
fn required_string<'a>(
    object: &'a serde_json::Map<String, serde_json::Value>,
    key: &str,
    message: &'static str,
) -> Result<&'a str, SchemaError> {
    object
        .get(key)
        .and_then(serde_json::Value::as_str)
        .ok_or(SchemaError::JsonShape(message))
}

#[cfg(feature = "json")]
fn optional_string<'a>(
    object: &'a serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> Result<Option<&'a str>, SchemaError> {
    match object.get(key) {
        Some(value) if value.is_null() => Ok(None),
        Some(value) => value.as_str().map(Some).ok_or(SchemaError::JsonShape(
            "optional string field has wrong type",
        )),
        None => Ok(None),
    }
}

#[cfg(feature = "json")]
fn optional_bool(
    object: &serde_json::Map<String, serde_json::Value>,
    key: &str,
    default: bool,
) -> Result<bool, SchemaError> {
    match object.get(key) {
        Some(value) => value
            .as_bool()
            .ok_or(SchemaError::JsonShape("optional bool field has wrong type")),
        None => Ok(default),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn primary_id() -> SchemaField {
        let mut field = SchemaField::new("id", SchemaFieldType::String);
        field.primary = true;
        field.indexed = true;
        field.unique = true;
        field
    }

    fn valid_schema() -> Schema {
        let mut email = SchemaField::new("email", SchemaFieldType::String);
        email.indexed = true;
        email.unique = true;

        let profile_name = SchemaField {
            indexed: true,
            ..SchemaField::new("profile.name", SchemaFieldType::String)
        };

        let company_id = SchemaField {
            indexed: true,
            reference: Some(Reference::parse("companies.id").unwrap()),
            ..SchemaField::new("company_id", SchemaFieldType::String)
        };

        let created_at = SchemaField {
            indexed: true,
            sort: true,
            range_index: true,
            ..SchemaField::new("created_at", SchemaFieldType::Int64)
        };

        Schema {
            name: "users".to_string(),
            fields: vec![primary_id(), email, profile_name, company_id, created_at],
            enable_nested_fields: true,
            default_sort_field: Some("created_at".to_string()),
        }
    }

    #[test]
    fn valid_schema_accepts_typesense_style_fields() {
        let schema = valid_schema();

        assert_eq!(schema.validate(), Ok(()));
        assert_eq!(schema.primary_field().unwrap().name, "id");
        assert_eq!(
            schema.field("email").unwrap().field_type,
            SchemaFieldType::String
        );
    }

    #[test]
    fn field_type_parser_accepts_arrays() {
        assert_eq!(
            SchemaFieldType::parse("string[]"),
            Ok(SchemaFieldType::Array(Box::new(SchemaFieldType::String)))
        );
        assert_eq!(
            SchemaFieldType::parse("int64[][]"),
            Ok(SchemaFieldType::Array(Box::new(SchemaFieldType::Array(
                Box::new(SchemaFieldType::Int64)
            ))))
        );
    }

    #[test]
    fn field_type_parser_rejects_excessive_array_nesting() {
        let field_type = format!("int64{}", "[]".repeat(MAX_ARRAY_NESTING + 1));

        assert_eq!(
            SchemaFieldType::parse(&field_type),
            Err(SchemaError::ArrayNestingTooDeep { field_type })
        );
    }

    #[test]
    fn reference_parser_requires_collection_and_field() {
        assert_eq!(
            Reference::parse("companies.id"),
            Ok(Reference::new("companies", "id"))
        );
        assert_eq!(
            Reference::parse("companies"),
            Err(SchemaError::InvalidReference {
                value: "companies".to_string()
            })
        );
    }

    #[test]
    fn validation_rejects_missing_primary() {
        let schema = Schema::new(
            "users",
            vec![SchemaField::new("email", SchemaFieldType::String)],
        );

        assert_eq!(schema.validate(), Err(SchemaError::MissingPrimaryField));
    }

    #[test]
    fn validation_rejects_duplicate_fields() {
        let schema = Schema::new(
            "users",
            vec![
                primary_id(),
                SchemaField::new("email", SchemaFieldType::String),
                SchemaField::new("email", SchemaFieldType::String),
            ],
        );

        assert_eq!(
            schema.validate(),
            Err(SchemaError::DuplicateField {
                name: "email".to_string()
            })
        );
    }

    #[test]
    fn validation_rejects_nested_field_when_disabled() {
        let schema = Schema::new(
            "users",
            vec![
                primary_id(),
                SchemaField::new("profile.name", SchemaFieldType::String),
            ],
        );

        assert_eq!(
            schema.validate(),
            Err(SchemaError::NestedFieldDisabled {
                name: "profile.name".to_string()
            })
        );
    }

    #[test]
    fn validation_rejects_unique_field_without_index() {
        let mut email = SchemaField::new("email", SchemaFieldType::String);
        email.unique = true;
        let schema = Schema::new("users", vec![primary_id(), email]);

        assert_eq!(
            schema.validate(),
            Err(SchemaError::UniqueFieldNotIndexed {
                name: "email".to_string()
            })
        );
    }

    #[test]
    fn validation_rejects_default_sort_field_without_sort_flag() {
        let mut created_at = SchemaField::new("created_at", SchemaFieldType::Int64);
        created_at.indexed = true;
        let mut schema = Schema::new("users", vec![primary_id(), created_at]);
        schema.default_sort_field = Some("created_at".to_string());

        assert_eq!(
            schema.validate(),
            Err(SchemaError::DefaultSortFieldNotSortable {
                name: "created_at".to_string()
            })
        );
    }

    #[test]
    fn validation_rejects_json_index() {
        let indexed_json = SchemaField {
            indexed: true,
            ..SchemaField::new("payload", SchemaFieldType::Json)
        };
        let schema = Schema::new("events", vec![primary_id(), indexed_json]);

        assert_eq!(
            schema.validate(),
            Err(SchemaError::UnsupportedIndexType {
                name: "payload".to_string()
            })
        );
    }

    #[test]
    fn validation_rejects_operational_flags_inside_object_fields() {
        let object_child = SchemaField {
            indexed: true,
            ..SchemaField::new("email", SchemaFieldType::String)
        };
        let profile = SchemaField::new("profile", SchemaFieldType::Object(vec![object_child]));
        let schema = Schema::new("users", vec![primary_id(), profile]);

        assert_eq!(
            schema.validate(),
            Err(SchemaError::ObjectFieldUsesOperationalFlag {
                name: "email".to_string(),
                flag: "indexed"
            })
        );
    }

    #[cfg(feature = "json")]
    #[test]
    fn json_loader_accepts_schema_document() {
        let schema = Schema::from_json_str(
            r#"
            {
              "name": "users",
              "enable_nested_fields": true,
              "default_sort_field": "created_at",
              "fields": [
                { "name": "id", "type": "string", "primary": true },
                { "name": "email", "type": "string", "unique": true, "indexed": true },
                { "name": "profile.name", "type": "string", "indexed": true },
                { "name": "company_id", "type": "string", "reference": "companies.id", "indexed": true },
                { "name": "created_at", "type": "int64", "indexed": true, "sort": true, "range_index": true },
                { "name": "tags", "type": "string[]", "indexed": true }
              ]
            }
            "#,
        )
        .unwrap();

        assert_eq!(schema.validate(), Ok(()));
        assert_eq!(
            schema.field("tags").unwrap().field_type,
            SchemaFieldType::Array(Box::new(SchemaFieldType::String))
        );
    }

    #[cfg(feature = "json")]
    #[test]
    fn json_loader_rejects_operational_object_child_flags() {
        let err = Schema::from_json_str(
            r#"
            {
              "name": "users",
              "fields": [
                { "name": "id", "type": "string", "primary": true },
                {
                  "name": "profile",
                  "type": "object",
                  "fields": [
                    { "name": "email", "type": "string", "indexed": true }
                  ]
                }
              ]
            }
            "#,
        )
        .unwrap_err();

        assert_eq!(
            err,
            SchemaError::ObjectFieldUsesOperationalFlag {
                name: "email".to_string(),
                flag: "indexed"
            }
        );
    }

    #[cfg(feature = "json")]
    #[test]
    fn json_loader_rejects_fields_on_scalar_types() {
        let err = Schema::from_json_str(
            r#"
            {
              "name": "users",
              "fields": [
                { "name": "id", "type": "string", "primary": true },
                {
                  "name": "profile",
                  "type": "string",
                  "fields": [
                    { "name": "email", "type": "string" }
                  ]
                }
              ]
            }
            "#,
        )
        .unwrap_err();

        assert_eq!(
            err,
            SchemaError::JsonShape("field.fields is only valid for object field types")
        );
    }
}
