//! Schema-mode documents on the NoSQL engine.
//!
//! Run with `cargo run --release --example nosql_schema_mode`.

use mfs_db::engine::{DocumentVersion, EngineConfig, NoSqlEngine, ReadOptions, WriteOptions};
use mfs_db::schema::{Schema, SchemaField, SchemaFieldType};
use mfs_db::schema_value::SchemaValue;

fn users_schema() -> Schema {
    let mut id = SchemaField::new("id", SchemaFieldType::String);
    id.primary = true;
    id.indexed = true;
    id.unique = true;

    let mut email = SchemaField::new("email", SchemaFieldType::String);
    email.indexed = true;
    email.unique = true;

    let mut age = SchemaField::new("age", SchemaFieldType::Int64);
    age.indexed = true;

    Schema::new("users", vec![id, email, age])
}

fn user_document(id: &str, email: &str, age: i64) -> SchemaValue {
    SchemaValue::object([
        ("id".to_string(), SchemaValue::String(id.to_string())),
        ("email".to_string(), SchemaValue::String(email.to_string())),
        ("age".to_string(), SchemaValue::Int64(age)),
    ])
}

fn string_field(document: &SchemaValue, field: &str) -> String {
    match document.field(field) {
        Some(SchemaValue::String(value)) => value.clone(),
        _ => "<missing>".to_string(),
    }
}

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let schema = users_schema();
    let engine = NoSqlEngine::open_memory(EngineConfig {
        raw_initial_capacity: 16,
        ..EngineConfig::default()
    })?;
    let collection_id = engine.create_schema_collection(&schema)?;
    println!(
        "created schema collection `{}` as id {}",
        schema.name,
        collection_id.get()
    );

    let document = user_document("u1", "ada@example.com", 36);
    document.validate_against(&schema)?;
    println!("validated document u1 against `{}`", schema.name);

    let put = engine.put_schema(&schema, document.clone(), WriteOptions::default())?;
    assert_eq!(put.version, DocumentVersion::new(1));

    let read = engine
        .get_schema(
            &schema,
            &SchemaValue::String("u1".to_string()),
            ReadOptions::default(),
        )?
        .expect("schema document exists after put");
    assert_eq!(read.version, put.version);
    assert_eq!(read.document, document);
    println!(
        "put/get round trip: v{} id={} email={}",
        read.version.get(),
        string_field(&read.document, "id"),
        string_field(&read.document, "email")
    );

    let invalid = SchemaValue::object([("id".to_string(), SchemaValue::String("u2".to_string()))]);
    match invalid.validate_against(&schema) {
        Err(error) => println!("validation rejected invalid document: {error}"),
        Ok(()) => panic!("invalid document unexpectedly passed schema validation"),
    }

    Ok(())
}
