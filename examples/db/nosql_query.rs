//! Query, count, batch-read, and partial-update APIs for schema-mode documents.
//!
//! Run with `cargo run -p mfs-db --release --example nosql_query`.

use mfs_db::engine::{
    DocumentVersion, EngineConfig, FieldUpdate, FieldUpdateOp, FilterClause, FilterOp, NoSqlEngine,
    QueryOptions, ReadOptions, SortDirection, WriteOptions,
};
use mfs_db::schema::{Schema, SchemaField, SchemaFieldType};
use mfs_db::schema_value::SchemaValue;

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

fn string_field(document: &SchemaValue, field: &str) -> String {
    match document.field(field) {
        Some(SchemaValue::String(value)) => value.clone(),
        _ => "<missing>".to_string(),
    }
}

fn int_field(document: &SchemaValue, field: &str) -> String {
    match document.field(field) {
        Some(SchemaValue::Int64(value)) => value.to_string(),
        Some(SchemaValue::Int32(value)) => value.to_string(),
        _ => "<missing>".to_string(),
    }
}

fn print_document(label: &str, doc: &SchemaValue, version: DocumentVersion) {
    println!(
        "  {label}: id={} name={} age={} v{}",
        string_field(doc, "id"),
        string_field(doc, "name"),
        int_field(doc, "age"),
        version.get(),
    );
}

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let schema = users_schema();
    let engine = NoSqlEngine::open_memory(EngineConfig {
        raw_initial_capacity: 16,
        ..EngineConfig::default()
    })?;
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
        let doc = user_document(id, name, *age);
        let result = engine.put_schema(&schema, doc, WriteOptions::default())?;
        println!("put {id} -> v{}", result.version.get());
    }
    println!();

    // 1. query_schema: range filter + sort + pagination.
    println!("--- query_schema: age >= 30, sort by age asc, limit 3 ---");
    let query = QueryOptions {
        filter: Some(FilterClause {
            field: "age".to_string(),
            op: FilterOp::Gte,
            value: SchemaValue::Int64(30),
        }),
        sort_field: Some("age".to_string()),
        sort_direction: SortDirection::Asc,
        limit: Some(3),
        offset: None,
    };
    let result = engine.query_schema(&schema, query)?;
    for doc in &result.documents {
        print_document("result", &doc.document, doc.version);
    }
    println!("returned {} documents\n", result.documents.len());

    // 2. count_schema: without filter.
    println!("--- count_schema: no filter ---");
    let total = engine.count_schema(&schema, None)?;
    println!("total documents: {total}\n");

    // 3. count_schema: with filter.
    println!("--- count_schema: age < 30 ---");
    let young = engine.count_schema(
        &schema,
        Some(FilterClause {
            field: "age".to_string(),
            op: FilterOp::Lt,
            value: SchemaValue::Int64(30),
        }),
    )?;
    println!("documents with age < 30: {young}\n");

    // 4. multi_get_schema: batch read by primary keys.
    println!("--- multi_get_schema: [u1, u3, u5, u999] ---");
    let keys = vec![
        SchemaValue::String("u1".to_string()),
        SchemaValue::String("u3".to_string()),
        SchemaValue::String("u5".to_string()),
        SchemaValue::String("u999".to_string()),
    ];
    let batch = engine.multi_get_schema(&schema, &keys, ReadOptions::default())?;
    for doc in &batch {
        print_document("found", &doc.document, doc.version);
    }
    println!(
        "requested 4 keys (one missing), got {} documents\n",
        batch.len()
    );

    // 5. update_schema: $set and $inc.
    println!("--- update_schema: set name, increment age ---");
    let before = engine
        .get_schema(
            &schema,
            &SchemaValue::String("u2".to_string()),
            ReadOptions::default(),
        )?
        .expect("u2 exists");
    print_document("before", &before.document, before.version);

    let updates = FieldUpdateOp {
        updates: vec![
            FieldUpdate::Set {
                field: "name".to_string(),
                value: SchemaValue::String("Bobby".to_string()),
            },
            FieldUpdate::Increment {
                field: "age".to_string(),
                delta: 1,
            },
        ],
    };
    let updated = engine.update_schema(
        &schema,
        &SchemaValue::String("u2".to_string()),
        updates,
        WriteOptions::default(),
    )?;

    let after = engine
        .get_schema(
            &schema,
            &SchemaValue::String("u2".to_string()),
            ReadOptions::default(),
        )?
        .expect("u2 exists after update");
    print_document("after ", &after.document, after.version);
    println!("update wrote v{}", updated.version.get());

    Ok(())
}
