//! Multi-get, count, and partial-update APIs for schema-mode documents.
//!
//! Run with `cargo run -p mfs-store --release --example nosql_query`.

use mfs_store::store::{
    FieldUpdate, FieldUpdateOp, MfsStore, ReadOptions, WriteOptions,
};
use mfs_store::schema::{Schema, SchemaField, SchemaFieldType};
use mfs_store::schema_value::SchemaValue;

fn users_schema() -> Schema {
    let mut id = SchemaField::new("id", SchemaFieldType::String);
    id.primary = true;
    id.indexed = true;
    id.unique = true;

    let mut name = SchemaField::new("name", SchemaFieldType::String);
    name.indexed = true;

    let mut age = SchemaField::new("age", SchemaFieldType::Int64);
    age.indexed = true;

    Schema::new("users", vec![id, name, age])
}

fn user_document(id: &str, name: &str, age: i64) -> SchemaValue {
    SchemaValue::object([
        ("id".to_string(), SchemaValue::String(id.to_string())),
        ("name".to_string(), SchemaValue::String(name.to_string())),
        ("age".to_string(), SchemaValue::Int64(age)),
    ])
}

fn string_field(doc: &SchemaValue, field: &str) -> String {
    match doc.field(field) {
        Some(SchemaValue::String(v)) => v.clone(),
        _ => "<missing>".to_string(),
    }
}

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let schema = users_schema();
    let engine = MfsStore::open_memory(Default::default())?;
    engine.create_schema_collection(&schema)?;
    println!("created schema collection `{}`\n", schema.name);

    // Seed documents.
    let users = [
        ("u1", "Ada", 36),
        ("u2", "Bob", 28),
        ("u3", "Cleo", 42),
        ("u4", "Dan", 31),
        ("u5", "Eve", 25),
        ("u6", "Fay", 38),
        ("u7", "Gus", 29),
    ];
    for (id, name, age) in &users {
        let _result = engine.put_schema(&schema, user_document(id, name, *age), WriteOptions::default())?;
        println!("put {id}");
    }
    println!();

    // 1. count_schema: total documents.
    let total = engine.count_schema(&schema)?;
    println!("total documents: {total}");
    println!();

    // 2. multi_get_schema: batch read by primary keys.
    println!("--- multi_get_schema: [u1, u3, u5, u999] ---");
    let keys = vec![
        SchemaValue::String("u1".to_string()),
        SchemaValue::String("u3".to_string()),
        SchemaValue::String("u5".to_string()),
        SchemaValue::String("u999".to_string()),
    ];
    let batch = engine.multi_get_schema(&schema, &keys, ReadOptions::default())?;
    for doc in &batch {
        println!("  found id={} name={} v{}",
            string_field(&doc.document, "id"),
            string_field(&doc.document, "name"),
            doc.version.get());
    }
    println!("  requested 4 keys, got {} documents\n", batch.len());

    // 3. update_schema: $set and $inc.
    println!("--- update_schema: set name, increment age ---");
    let before = engine.get_schema(&schema, &SchemaValue::String("u2".to_string()), ReadOptions::default())?
        .expect("u2 exists");
    println!("  before: id={} name={} age={:?} v{}",
        string_field(&before.document, "id"),
        string_field(&before.document, "name"),
        before.document.field("age"),
        before.version.get());

    let updates = FieldUpdateOp {
        updates: vec![
            FieldUpdate::Set { field: "name".into(), value: SchemaValue::String("Bobby".to_string()) },
            FieldUpdate::Increment { field: "age".into(), delta: 1 },
        ],
    };
    engine.update_schema(&schema, &SchemaValue::String("u2".to_string()), updates, WriteOptions::default())?;

    let after = engine.get_schema(&schema, &SchemaValue::String("u2".to_string()), ReadOptions::default())?
        .expect("u2 exists after update");
    println!("  after:  id={} name={} age={:?} v{}",
        string_field(&after.document, "id"),
        string_field(&after.document, "name"),
        after.document.field("age"),
        after.version.get());

    Ok(())
}
