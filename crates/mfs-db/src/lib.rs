//! Database engine primitives for `memory-first-store`.

pub mod engine;
pub mod schema;
pub mod schema_value;
pub mod value;

pub use engine::*;
pub use schema::*;
pub use schema_value::*;
pub use value::*;

#[cfg(test)]
mod tests {
    use super::*;

    fn small_config() -> EngineConfig {
        EngineConfig {
            raw_initial_capacity: 16,
            ..EngineConfig::default()
        }
    }

    fn primary_id() -> SchemaField {
        let mut field = SchemaField::new("id", SchemaFieldType::String);
        field.primary = true;
        field.indexed = true;
        field.unique = true;
        field
    }

    fn user_schema() -> Schema {
        let mut email = SchemaField::new("email", SchemaFieldType::String);
        email.indexed = true;
        email.unique = true;

        let age = SchemaField {
            indexed: true,
            ..SchemaField::new("age", SchemaFieldType::Int64)
        };

        Schema::new("users", vec![primary_id(), email, age])
    }

    fn user_document(id: &str, email: &str, age: i64) -> SchemaValue {
        SchemaValue::object([
            ("id".to_string(), SchemaValue::String(id.to_string())),
            ("email".to_string(), SchemaValue::String(email.to_string())),
            ("age".to_string(), SchemaValue::Int64(age)),
        ])
    }

    #[test]
    fn nosql_db_imports_compile_and_raw_round_trip() {
        let engine = NoSqlEngine::open_memory(small_config()).expect("open memory engine");
        let collection_id = engine
            .create_raw_collection("raw")
            .expect("create raw collection");
        assert_eq!(collection_id.get(), 1);

        let key = RawKey::from(&b"user:1"[..]);
        let put = engine
            .put_raw(
                "raw",
                key.clone(),
                RawValue::from(&b"ada"[..]),
                WriteOptions::default(),
            )
            .expect("put raw value");
        assert_eq!(put.version, DocumentVersion::new(1));

        let read = engine
            .get_raw("raw", &key, ReadOptions::default())
            .expect("read raw value")
            .expect("live raw value");
        assert_eq!(read.value.as_bytes(), b"ada");
        assert_eq!(read.version, DocumentVersion::new(1));

        let updated = engine
            .compare_put_raw(
                "raw",
                key.clone(),
                RawValue::from(&b"lovelace"[..]),
                put.version,
            )
            .expect("compare put against current version");
        assert_eq!(updated.version, DocumentVersion::new(2));

        let stale = engine
            .compare_put_raw("raw", key, RawValue::from(&b"stale"[..]), put.version)
            .expect_err("stale expected version conflicts");
        assert!(matches!(
            stale,
            EngineError::Conflict { expected, actual, .. }
                if expected == put.version && actual == updated.version
        ));
    }

    #[test]
    fn nosql_schema_put_lookup_and_value_codecs_compile() {
        let engine = NoSqlEngine::open_memory(small_config()).expect("open memory engine");
        let schema = user_schema();
        engine
            .create_schema_collection(&schema)
            .expect("create schema collection");

        let document = user_document("u1", "ada@example.com", 37);
        let put = engine
            .put_schema(&schema, document.clone(), WriteOptions::default())
            .expect("put schema document");
        assert_eq!(put.version, DocumentVersion::new(1));

        let read = engine
            .get_schema(
                &schema,
                &SchemaValue::String("u1".to_string()),
                ReadOptions::default(),
            )
            .expect("get schema document")
            .expect("schema document exists");
        assert_eq!(read.document, document);

        let hits = engine
            .lookup_schema(
                &schema,
                "age",
                &SchemaValue::Int64(37),
                ReadOptions::default(),
            )
            .expect("lookup by indexed field");
        assert_eq!(hits.len(), 1);
        assert_eq!(
            hits[0].document.field("id"),
            Some(&SchemaValue::String("u1".into()))
        );

        let mut encoded = Vec::new();
        encode_value(&MfsValue::String("cached".to_string()), &mut encoded);
        assert_eq!(
            decode_value(&encoded).expect("decode object-store value"),
            MfsValue::String("cached".to_string())
        );
    }
}
