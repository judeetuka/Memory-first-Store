//! Durable hot storage layer for `memory-first-store`.

pub mod store;
pub mod schema;
pub mod schema_value;
pub mod value;

pub use store::*;
pub use schema::*;
pub use schema_value::*;
pub use value::*;

#[cfg(test)]
mod tests {
    use super::*;

    fn small_config() -> MfsStoreConfig {
        MfsStoreConfig {
            raw_initial_capacity: 16,
            ..MfsStoreConfig::default()
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
        let engine = MfsStore::open_memory(small_config()).expect("open memory engine");
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
            StoreError::Conflict { expected, actual, .. }
                if expected == put.version && actual == updated.version
        ));
    }

    #[test]
    fn nosql_schema_put_lookup_and_value_codecs_compile() {
        let engine = MfsStore::open_memory(small_config()).expect("open memory engine");
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

    fn user_schema_with_optional_bio() -> Schema {
        let mut email = SchemaField::new("email", SchemaFieldType::String);
        email.indexed = true;
        email.unique = true;

        let age = SchemaField {
            indexed: true,
            ..SchemaField::new("age", SchemaFieldType::Int64)
        };

        let mut bio = SchemaField::new("bio", SchemaFieldType::String);
        bio.optional = true;

        Schema::new("users_with_bio", vec![primary_id(), email, age, bio])
    }

    fn user_document_with_bio(id: &str, email: &str, age: i64, bio: &str) -> SchemaValue {
        SchemaValue::object([
            ("id".to_string(), SchemaValue::String(id.to_string())),
            ("email".to_string(), SchemaValue::String(email.to_string())),
            ("age".to_string(), SchemaValue::Int64(age)),
            ("bio".to_string(), SchemaValue::String(bio.to_string())),
        ])
    }

    fn seed_users(engine: &MfsStore, schema: &Schema, n: usize, base: i64) {
        for i in 0..n {
            let id = format!("u{i}");
            let email = format!("u{i}@example.com");
            let age = base + i as i64;
            let document = user_document(&id, &email, age);
            engine
                .put_schema(schema, document, WriteOptions::default())
                .expect("seed put_schema");
        }
    }

    // ===== update_* tests =====

    #[test]
    fn update_set_field() {
        let engine = MfsStore::open_memory(small_config()).expect("open memory engine");
        let schema = user_schema();
        engine
            .create_schema_collection(&schema)
            .expect("create schema collection");

        let document = user_document("u1", "ada@example.com", 37);
        let put = engine
            .put_schema(&schema, document, WriteOptions::default())
            .expect("put u1");
        assert_eq!(put.version, DocumentVersion::new(1));

        let new_email = SchemaValue::String("ada@lovelace.org".to_string());
        let update = FieldUpdateOp {
            updates: vec![FieldUpdate::Set {
                field: "email".to_string(),
                value: new_email.clone(),
            }],
        };
        let updated = engine
            .update_schema(
                &schema,
                &SchemaValue::String("u1".to_string()),
                update,
                WriteOptions::default(),
            )
            .expect("update email");
        assert_eq!(updated.version, DocumentVersion::new(2));

        let read = engine
            .get_schema(
                &schema,
                &SchemaValue::String("u1".to_string()),
                ReadOptions::default(),
            )
            .expect("get updated doc")
            .expect("updated doc exists");
        assert_eq!(read.document.field("email"), Some(&new_email));
    }

    #[test]
    fn update_unset_optional() {
        let engine = MfsStore::open_memory(small_config()).expect("open memory engine");
        let schema = user_schema_with_optional_bio();
        engine
            .create_schema_collection(&schema)
            .expect("create schema collection");

        let document = user_document_with_bio("u1", "ada@example.com", 37, "first programmer");
        engine
            .put_schema(&schema, document, WriteOptions::default())
            .expect("put u1 with bio");

        let update = FieldUpdateOp {
            updates: vec![FieldUpdate::Unset {
                field: "bio".to_string(),
            }],
        };
        let result = engine
            .update_schema(
                &schema,
                &SchemaValue::String("u1".to_string()),
                update,
                WriteOptions::default(),
            )
            .expect("unset bio");
        assert_eq!(result.version, DocumentVersion::new(2));

        let read = engine
            .get_schema(
                &schema,
                &SchemaValue::String("u1".to_string()),
                ReadOptions::default(),
            )
            .expect("get after unset")
            .expect("doc exists after unset");
        assert!(
            read.document.field("bio").is_none(),
            "bio should be removed after unset"
        );
    }

    #[test]
    fn update_unset_required_errors() {
        let engine = MfsStore::open_memory(small_config()).expect("open memory engine");
        let schema = user_schema();
        engine
            .create_schema_collection(&schema)
            .expect("create schema collection");
        engine
            .put_schema(
                &schema,
                user_document("u1", "ada@example.com", 37),
                WriteOptions::default(),
            )
            .expect("put u1");

        let update = FieldUpdateOp {
            updates: vec![FieldUpdate::Unset {
                field: "id".to_string(),
            }],
        };
        let err = engine
            .update_schema(
                &schema,
                &SchemaValue::String("u1".to_string()),
                update,
                WriteOptions::default(),
            )
            .expect_err("unset on primary field must fail");
        assert!(
            matches!(err, StoreError::PrimaryKeyUpdateForbidden),
            "expected PrimaryKeyUpdateForbidden, got {err:?}"
        );
    }

    #[test]
    fn update_increment_int64() {
        let engine = MfsStore::open_memory(small_config()).expect("open memory engine");
        let schema = user_schema();
        engine
            .create_schema_collection(&schema)
            .expect("create schema collection");
        engine
            .put_schema(
                &schema,
                user_document("u1", "ada@example.com", 37),
                WriteOptions::default(),
            )
            .expect("put u1");

        let update = FieldUpdateOp {
            updates: vec![FieldUpdate::Increment {
                field: "age".to_string(),
                delta: 5,
            }],
        };
        let result = engine
            .update_schema(
                &schema,
                &SchemaValue::String("u1".to_string()),
                update,
                WriteOptions::default(),
            )
            .expect("increment age");
        assert_eq!(result.version, DocumentVersion::new(2));

        let read = engine
            .get_schema(
                &schema,
                &SchemaValue::String("u1".to_string()),
                ReadOptions::default(),
            )
            .expect("get after increment")
            .expect("doc exists after increment");
        assert_eq!(read.document.field("age"), Some(&SchemaValue::Int64(42)));
    }

    #[test]
    fn update_increment_overflow() {
        let engine = MfsStore::open_memory(small_config()).expect("open memory engine");
        let schema = user_schema();
        engine
            .create_schema_collection(&schema)
            .expect("create schema collection");
        engine
            .put_schema(
                &schema,
                user_document("u1", "ada@example.com", i64::MAX),
                WriteOptions::default(),
            )
            .expect("put u1 with max age");

        let update = FieldUpdateOp {
            updates: vec![FieldUpdate::Increment {
                field: "age".to_string(),
                delta: 1,
            }],
        };
        let err = engine
            .update_schema(
                &schema,
                &SchemaValue::String("u1".to_string()),
                update,
                WriteOptions::default(),
            )
            .expect_err("increment on i64::MAX must fail");
        match err {
            StoreError::NumericOverflow { field } => {
                assert_eq!(field, "age");
            }
            other => panic!("expected NumericOverflow, got {other:?}"),
        }
    }

    #[test]
    fn update_increment_type_mismatch() {
        let engine = MfsStore::open_memory(small_config()).expect("open memory engine");
        let schema = user_schema();
        engine
            .create_schema_collection(&schema)
            .expect("create schema collection");
        engine
            .put_schema(
                &schema,
                user_document("u1", "ada@example.com", 37),
                WriteOptions::default(),
            )
            .expect("put u1");

        let update = FieldUpdateOp {
            updates: vec![FieldUpdate::Increment {
                field: "email".to_string(),
                delta: 1,
            }],
        };
        let err = engine
            .update_schema(
                &schema,
                &SchemaValue::String("u1".to_string()),
                update,
                WriteOptions::default(),
            )
            .expect_err("increment on String field must fail");
        match err {
            StoreError::UpdateTypeMismatch {
                field,
                expected,
                actual,
            } => {
                assert_eq!(field, "email");
                assert_eq!(expected, "numeric");
                assert_eq!(actual, SchemaValueKind::String);
            }
            other => panic!("expected UpdateTypeMismatch, got {other:?}"),
        }
    }

    #[test]
    fn update_primary_key_forbidden() {
        let engine = MfsStore::open_memory(small_config()).expect("open memory engine");
        let schema = user_schema();
        engine
            .create_schema_collection(&schema)
            .expect("create schema collection");
        engine
            .put_schema(
                &schema,
                user_document("u1", "ada@example.com", 37),
                WriteOptions::default(),
            )
            .expect("put u1");

        let update = FieldUpdateOp {
            updates: vec![FieldUpdate::Set {
                field: "id".to_string(),
                value: SchemaValue::String("u2".to_string()),
            }],
        };
        let err = engine
            .update_schema(
                &schema,
                &SchemaValue::String("u1".to_string()),
                update,
                WriteOptions::default(),
            )
            .expect_err("set on primary field must fail");
        assert!(
            matches!(err, StoreError::PrimaryKeyUpdateForbidden),
            "expected PrimaryKeyUpdateForbidden, got {err:?}"
        );
    }

    #[test]
    fn update_document_not_found() {
        let engine = MfsStore::open_memory(small_config()).expect("open memory engine");
        let schema = user_schema();
        engine
            .create_schema_collection(&schema)
            .expect("create schema collection");

        let update = FieldUpdateOp {
            updates: vec![FieldUpdate::Set {
                field: "email".to_string(),
                value: SchemaValue::String("x@y.com".to_string()),
            }],
        };
        let err = engine
            .update_schema(
                &schema,
                &SchemaValue::String("ghost".to_string()),
                update,
                WriteOptions::default(),
            )
            .expect_err("update on missing doc must fail");
        match err {
            StoreError::DocumentNotFound { collection } => {
                assert_eq!(collection, "users");
            }
            other => panic!("expected DocumentNotFound, got {other:?}"),
        }
    }


    #[test]
    fn multiget_basic() {
        let engine = MfsStore::open_memory(small_config()).expect("open memory engine");
        let schema = user_schema();
        engine
            .create_schema_collection(&schema)
            .expect("create schema collection");
        seed_users(&engine, &schema, 3, 30);

        let keys: Vec<SchemaValue> = ["u0", "u1", "u2"]
            .iter()
            .map(|s| SchemaValue::String(s.to_string()))
            .collect();
        let results = engine
            .multi_get_schema(&schema, &keys, ReadOptions::default())
            .expect("multi_get by 3 keys");
        assert_eq!(results.len(), 3);
        let ids: Vec<String> = results
            .iter()
            .map(|r| match r.document.field("id") {
                Some(SchemaValue::String(v)) => v.clone(),
                _ => panic!("expected String id"),
            })
            .collect();
        assert_eq!(ids, vec!["u0", "u1", "u2"]);
    }

    #[test]
    fn multiget_missing() {
        let engine = MfsStore::open_memory(small_config()).expect("open memory engine");
        let schema = user_schema();
        engine
            .create_schema_collection(&schema)
            .expect("create schema collection");
        seed_users(&engine, &schema, 2, 40);

        let keys: Vec<SchemaValue> = ["u0", "ghost", "u1"]
            .iter()
            .map(|s| SchemaValue::String(s.to_string()))
            .collect();
        let results = engine
            .multi_get_schema(&schema, &keys, ReadOptions::default())
            .expect("multi_get with missing key");
        assert_eq!(results.len(), 2, "missing keys should be skipped");
        let ids: Vec<String> = results
            .iter()
            .map(|r| match r.document.field("id") {
                Some(SchemaValue::String(v)) => v.clone(),
                _ => panic!("expected String id"),
            })
            .collect();
        assert_eq!(ids, vec!["u0", "u1"]);
    }

    #[test]
    fn multiget_dedup() {
        let engine = MfsStore::open_memory(small_config()).expect("open memory engine");
        let schema = user_schema();
        engine
            .create_schema_collection(&schema)
            .expect("create schema collection");
        seed_users(&engine, &schema, 2, 50);

        let keys: Vec<SchemaValue> = ["u0", "u0", "u1", "u0", "u1"]
            .iter()
            .map(|s| SchemaValue::String(s.to_string()))
            .collect();
        let results = engine
            .multi_get_schema(&schema, &keys, ReadOptions::default())
            .expect("multi_get with duplicate keys");
        assert_eq!(
            results.len(),
            2,
            "duplicate keys should be deduplicated to one per key"
        );
    }

    #[test]
    fn multiget_empty() {
        let engine = MfsStore::open_memory(small_config()).expect("open memory engine");
        let schema = user_schema();
        engine
            .create_schema_collection(&schema)
            .expect("create schema collection");
        seed_users(&engine, &schema, 2, 60);

        let keys: Vec<SchemaValue> = Vec::new();
        let results = engine
            .multi_get_schema(&schema, &keys, ReadOptions::default())
            .expect("multi_get with empty keys");
        assert!(results.is_empty(), "empty keys should return empty results");
    }


    #[test]
    fn count_unfiltered() {
        let engine = MfsStore::open_memory(small_config()).expect("open memory engine");
        let schema = user_schema();
        engine
            .create_schema_collection(&schema)
            .expect("create schema collection");
        seed_users(&engine, &schema, 7, 70);

        let count = engine
            .count_schema(&schema)
            .expect("count unfiltered");
        assert_eq!(count, 7);
    }

    #[test]
    fn count_after_delete() {
        let engine = MfsStore::open_memory(small_config()).expect("open memory engine");
        let schema = user_schema();
        engine
            .create_schema_collection(&schema)
            .expect("create schema collection");
        seed_users(&engine, &schema, 5, 100);

        let before = engine
            .count_schema(&schema)
            .expect("count before delete");
        assert_eq!(before, 5);

        engine
            .delete_schema(
                &schema,
                &SchemaValue::String("u2".to_string()),
                WriteOptions::default(),
            )
            .expect("delete u2");

        let after = engine
            .count_schema(&schema)
            .expect("count after delete");
        assert_eq!(after, 4, "count should decrease by 1 after deleting u2");
    }
}
