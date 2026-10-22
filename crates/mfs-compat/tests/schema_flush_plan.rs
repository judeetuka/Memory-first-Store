//! Planner tests: SQL generation, column mapping, and error vocabulary.
//!
//! The planner itself lives in `mfs_compat::schema_flush`; these are its
//! contract tests.

use mfs_compat::schema_flush::{
    SchemaFlushError, SchemaFlushRecord, SqlValue, create_index_sql, create_table_sql, delete_sql,
    delete_values, sql_columns, upsert_sql, upsert_values,
};
use mfs_core::Operation;
use mfs_store::schema::{Schema, SchemaField, SchemaFieldType};
use mfs_store::schema_value::SchemaValue;

fn user_schema() -> Schema {
    let mut id = SchemaField::new("id", SchemaFieldType::String);
    id.primary = true;
    id.indexed = true;
    id.unique = true;
    let mut email = SchemaField::new("email", SchemaFieldType::String);
    email.indexed = true;
    email.unique = true;
    let mut company_id = SchemaField::new("company_id", SchemaFieldType::String);
    company_id.indexed = true;
    company_id.reference = Some(mfs_store::schema::Reference::new("companies", "id"));
    let mut created_at = SchemaField::new("created_at", SchemaFieldType::Int64);
    created_at.indexed = true;
    created_at.sort = true;
    Schema::new("users", vec![id, email, company_id, created_at])
}

fn document() -> SchemaValue {
    SchemaValue::object([
        ("id".to_string(), SchemaValue::String("u1".to_string())),
        (
            "email".to_string(),
            SchemaValue::String("ada@example.com".to_string()),
        ),
        (
            "company_id".to_string(),
            SchemaValue::String("c1".to_string()),
        ),
        ("created_at".to_string(), SchemaValue::Int64(10)),
    ])
}

#[test]
fn ddl_contains_columns_lsn_and_references() {
    let ddl = create_table_sql(&user_schema()).unwrap();

    assert!(ddl.contains("\"id\" TEXT PRIMARY KEY"));
    assert!(ddl.contains("\"email\" TEXT UNIQUE"));
    assert!(ddl.contains("\"company_id\" TEXT REFERENCES \"companies\"(\"id\")"));
    assert!(ddl.contains("\"doc\" BLOB"));
    assert!(ddl.contains("\"mfs_key\" BLOB NOT NULL UNIQUE"));
    assert!(ddl.contains("\"mfs_lsn\" INTEGER NOT NULL"));
}

#[test]
fn indexes_include_indexed_non_unique_fields() {
    let indexes = create_index_sql(&user_schema()).unwrap();

    assert!(
        indexes
            .iter()
            .any(|sql| sql.contains("users_company_id_idx"))
    );
    assert!(
        indexes
            .iter()
            .any(|sql| sql.contains("users_created_at_idx"))
    );
    assert!(!indexes.iter().any(|sql| sql.contains("users_email_idx")));
}

#[test]
fn upsert_sql_has_lsn_guard() {
    let sql = upsert_sql(&user_schema()).unwrap();

    assert!(sql.contains("ON CONFLICT (\"id\") DO UPDATE"));
    assert!(sql.contains("WHERE \"users\".\"mfs_lsn\" < excluded.\"mfs_lsn\""));
}

#[test]
fn delete_sql_has_lsn_guard() {
    let sql = delete_sql(&user_schema()).unwrap();

    assert_eq!(
        sql,
        "DELETE FROM \"users\" WHERE \"mfs_key\" = ? AND \"mfs_lsn\" <= ?;"
    );
}

#[test]
fn upsert_values_follow_schema_column_order() {
    let record = SchemaFlushRecord {
        collection: "users".to_string(),
        key: vec![0, b'u', b'1'],
        op: Operation::Put,
        document: Some(document()),
        lsn: 42,
    };

    let mut encoded = Vec::new();
    mfs_store::schema_value::encode_schema_value(&document(), &mut encoded);

    assert_eq!(
        upsert_values(&user_schema(), &record).unwrap(),
        vec![
            SqlValue::Text("u1".to_string()),
            SqlValue::Text("ada@example.com".to_string()),
            SqlValue::Text("c1".to_string()),
            SqlValue::Integer(10),
            SqlValue::Blob(encoded),
            SqlValue::Blob(vec![0, b'u', b'1']),
            SqlValue::Integer(42),
        ]
    );
}

#[test]
fn reserved_doc_column_is_rejected() {
    let mut id = SchemaField::new("id", SchemaFieldType::String);
    id.primary = true;
    let schema = Schema::new(
        "bad",
        vec![id, SchemaField::new("doc", SchemaFieldType::String)],
    );

    assert!(matches!(
        sql_columns(&schema),
        Err(SchemaFlushError::ReservedColumnName { collection, field, column })
            if collection == "bad" && field == "doc" && column == "doc"
    ));
}

#[test]
fn delete_values_are_key_and_lsn() {
    let record = SchemaFlushRecord {
        collection: "users".to_string(),
        key: vec![0, b'u', b'1'],
        op: Operation::Delete,
        document: None,
        lsn: 43,
    };

    assert_eq!(
        delete_values(&record).unwrap(),
        vec![SqlValue::Blob(vec![0, b'u', b'1']), SqlValue::Integer(43)]
    );
}

#[test]
fn reserved_mfs_columns_are_rejected() {
    let mut id = SchemaField::new("id", SchemaFieldType::String);
    id.primary = true;
    let schema = Schema::new(
        "bad",
        vec![id, SchemaField::new("mfs_key", SchemaFieldType::String)],
    );

    assert!(matches!(
        sql_columns(&schema),
        Err(SchemaFlushError::ReservedColumnName { collection, field, column })
            if collection == "bad" && field == "mfs_key" && column == "mfs_key"
    ));
}

#[test]
fn column_name_collisions_are_rejected() {
    let mut id = SchemaField::new("id", SchemaFieldType::String);
    id.primary = true;
    let mut schema = Schema::new(
        "bad",
        vec![
            id,
            SchemaField::new("profile.name", SchemaFieldType::String),
            SchemaField::new("profile_name", SchemaFieldType::String),
        ],
    );
    schema.enable_nested_fields = true;

    assert!(matches!(
        sql_columns(&schema),
        Err(SchemaFlushError::DuplicateColumnName { collection, column, .. })
            if collection == "bad" && column == "profile_name"
    ));
}

#[test]
fn object_and_array_fields_reject_wrong_value_shape() {
    let mut id = SchemaField::new("id", SchemaFieldType::String);
    id.primary = true;
    let schema = Schema::new(
        "docs",
        vec![
            id,
            SchemaField::new("profile", SchemaFieldType::Object(Vec::new())),
            SchemaField::new(
                "tags",
                SchemaFieldType::Array(Box::new(SchemaFieldType::String)),
            ),
        ],
    );
    let record = SchemaFlushRecord {
        collection: "docs".to_string(),
        key: vec![0, b'd', b'1'],
        op: Operation::Put,
        document: Some(SchemaValue::object([
            ("id".to_string(), SchemaValue::String("d1".to_string())),
            (
                "profile".to_string(),
                SchemaValue::String("not object".to_string()),
            ),
            (
                "tags".to_string(),
                SchemaValue::String("not array".to_string()),
            ),
        ])),
        lsn: 1,
    };

    assert!(matches!(
        upsert_values(&schema, &record),
        Err(SchemaFlushError::TypeMismatch { field, expected })
            if field == "profile" && expected == "BLOB"
    ));
}

#[test]
fn codec_unsafe_nested_values_return_error() {
    let mut id = SchemaField::new("id", SchemaFieldType::String);
    id.primary = true;
    let schema = Schema::new(
        "docs",
        vec![id, SchemaField::new("payload", SchemaFieldType::Json)],
    );
    let record = SchemaFlushRecord {
        collection: "docs".to_string(),
        key: vec![0, b'd', b'1'],
        op: Operation::Put,
        document: Some(SchemaValue::object([
            ("id".to_string(), SchemaValue::String("d1".to_string())),
            ("payload".to_string(), SchemaValue::Float(f64::NAN)),
        ])),
        lsn: 1,
    };

    assert!(matches!(
        upsert_values(&schema, &record),
        Err(SchemaFlushError::EncodeValue { field, .. }) if field == "payload"
    ));
}
