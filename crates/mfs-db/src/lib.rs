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

    fn name_schema() -> Schema {
        let mut name = SchemaField::new("name", SchemaFieldType::String);
        name.indexed = true;
        Schema::new("names", vec![primary_id(), name])
    }

    fn name_document(id: &str, name: &str) -> SchemaValue {
        SchemaValue::object([
            ("id".to_string(), SchemaValue::String(id.to_string())),
            ("name".to_string(), SchemaValue::String(name.to_string())),
        ])
    }

    fn user_schema_with_optional_nickname() -> Schema {
        let mut email = SchemaField::new("email", SchemaFieldType::String);
        email.indexed = true;
        email.unique = true;

        let age = SchemaField {
            indexed: true,
            ..SchemaField::new("age", SchemaFieldType::Int64)
        };

        let mut nickname = SchemaField::new("nickname", SchemaFieldType::String);
        nickname.optional = true;

        Schema::new(
            "users_with_nickname",
            vec![primary_id(), email, age, nickname],
        )
    }

    fn user_document_with_nickname(
        id: &str,
        email: &str,
        age: i64,
        nickname: Option<&str>,
    ) -> SchemaValue {
        let mut fields: Vec<(String, SchemaValue)> = vec![
            ("id".to_string(), SchemaValue::String(id.to_string())),
            ("email".to_string(), SchemaValue::String(email.to_string())),
            ("age".to_string(), SchemaValue::Int64(age)),
        ];
        if let Some(nick) = nickname {
            fields.push(("nickname".to_string(), SchemaValue::String(nick.to_string())));
        }
        SchemaValue::object(fields)
    }

    fn query_opts(filter: FilterClause) -> QueryOptions {
        QueryOptions {
            filter: Some(filter),
            sort_field: None,
            sort_direction: SortDirection::Asc,
            limit: None,
            offset: None,
        }
    }

    fn filter_eq(field: &str, value: SchemaValue) -> FilterClause {
        FilterClause {
            field: field.to_string(),
            op: FilterOp::Eq,
            value,
        }
    }

    fn filter_neq(field: &str, value: SchemaValue) -> FilterClause {
        FilterClause {
            field: field.to_string(),
            op: FilterOp::Neq,
            value,
        }
    }

    fn filter_gt(field: &str, value: SchemaValue) -> FilterClause {
        FilterClause {
            field: field.to_string(),
            op: FilterOp::Gt,
            value,
        }
    }

    fn filter_gte(field: &str, value: SchemaValue) -> FilterClause {
        FilterClause {
            field: field.to_string(),
            op: FilterOp::Gte,
            value,
        }
    }

    fn filter_lt(field: &str, value: SchemaValue) -> FilterClause {
        FilterClause {
            field: field.to_string(),
            op: FilterOp::Lt,
            value,
        }
    }

    fn filter_lte(field: &str, value: SchemaValue) -> FilterClause {
        FilterClause {
            field: field.to_string(),
            op: FilterOp::Lte,
            value,
        }
    }

    fn seed_users(engine: &NoSqlEngine, schema: &Schema, n: usize, base: i64) {
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


    #[test]
    fn query_range_gt_int64() {
        let engine = NoSqlEngine::open_memory(small_config()).expect("open memory engine");
        let schema = user_schema();
        engine
            .create_schema_collection(&schema)
            .expect("create schema collection");
        seed_users(&engine, &schema, 10, 20);

        let options = query_opts(filter_gt("age", SchemaValue::Int64(25)));
        let result = engine
            .query_schema(&schema, options)
            .expect("query with Gt filter");
        assert_eq!(
            result.documents.len(),
            4,
            "Gt(25) on ages 20..29 should return 4 docs (26,27,28,29)"
        );

        let ages: Vec<i64> = result
            .documents
            .iter()
            .map(|r| match r.document.field("age") {
                Some(SchemaValue::Int64(v)) => *v,
                _ => panic!("expected Int64 age field"),
            })
            .collect();
        assert_eq!(ages, vec![26, 27, 28, 29]);
    }

    #[test]
    fn query_range_gte_int64() {
        let engine = NoSqlEngine::open_memory(small_config()).expect("open memory engine");
        let schema = user_schema();
        engine
            .create_schema_collection(&schema)
            .expect("create schema collection");
        seed_users(&engine, &schema, 10, 20);

        let options = query_opts(filter_gte("age", SchemaValue::Int64(25)));
        let result = engine
            .query_schema(&schema, options)
            .expect("query with Gte filter");
        assert_eq!(
            result.documents.len(),
            5,
            "Gte(25) on ages 20..29 should return 5 docs (25..29)"
        );

        let ages: Vec<i64> = result
            .documents
            .iter()
            .map(|r| match r.document.field("age") {
                Some(SchemaValue::Int64(v)) => *v,
                _ => panic!("expected Int64 age field"),
            })
            .collect();
        assert_eq!(ages, vec![25, 26, 27, 28, 29]);
    }

    #[test]
    fn query_range_lt_int64() {
        let engine = NoSqlEngine::open_memory(small_config()).expect("open memory engine");
        let schema = user_schema();
        engine
            .create_schema_collection(&schema)
            .expect("create schema collection");
        seed_users(&engine, &schema, 10, 20);

        let options = query_opts(filter_lt("age", SchemaValue::Int64(25)));
        let result = engine
            .query_schema(&schema, options)
            .expect("query with Lt filter");
        assert_eq!(
            result.documents.len(),
            5,
            "Lt(25) on ages 20..29 should return 5 docs (20..24)"
        );

        let ages: Vec<i64> = result
            .documents
            .iter()
            .map(|r| match r.document.field("age") {
                Some(SchemaValue::Int64(v)) => *v,
                _ => panic!("expected Int64 age field"),
            })
            .collect();
        assert_eq!(ages, vec![20, 21, 22, 23, 24]);
    }

    #[test]
    fn query_range_lte_int64() {
        let engine = NoSqlEngine::open_memory(small_config()).expect("open memory engine");
        let schema = user_schema();
        engine
            .create_schema_collection(&schema)
            .expect("create schema collection");
        seed_users(&engine, &schema, 10, 20);

        let options = query_opts(filter_lte("age", SchemaValue::Int64(25)));
        let result = engine
            .query_schema(&schema, options)
            .expect("query with Lte filter");
        assert_eq!(
            result.documents.len(),
            6,
            "Lte(25) on ages 20..29 should return 6 docs (20..25)"
        );

        let ages: Vec<i64> = result
            .documents
            .iter()
            .map(|r| match r.document.field("age") {
                Some(SchemaValue::Int64(v)) => *v,
                _ => panic!("expected Int64 age field"),
            })
            .collect();
        assert_eq!(ages, vec![20, 21, 22, 23, 24, 25]);
    }

    #[test]
    fn query_range_eq_uses_index() {
        let engine = NoSqlEngine::open_memory(small_config()).expect("open memory engine");
        let schema = user_schema();
        engine
            .create_schema_collection(&schema)
            .expect("create schema collection");
        seed_users(&engine, &schema, 10, 20);

        let options = query_opts(filter_eq("age", SchemaValue::Int64(25)));
        let result = engine
            .query_schema(&schema, options)
            .expect("query with Eq filter (indexed lookup)");
        assert_eq!(
            result.documents.len(),
            1,
            "Eq(25) should return exactly 1 doc via indexed lookup"
        );
        assert_eq!(
            result.documents[0].document.field("id"),
            Some(&SchemaValue::String("u5".to_string()))
        );
    }

    #[test]
    fn query_range_neq() {
        let engine = NoSqlEngine::open_memory(small_config()).expect("open memory engine");
        let schema = user_schema();
        engine
            .create_schema_collection(&schema)
            .expect("create schema collection");
        seed_users(&engine, &schema, 10, 20);

        let options = query_opts(filter_neq("age", SchemaValue::Int64(25)));
        let result = engine
            .query_schema(&schema, options)
            .expect("query with Neq filter");
        assert_eq!(
            result.documents.len(),
            9,
            "Neq(25) on ages 20..29 should return 9 docs"
        );
    }

    #[test]
    fn query_range_string_gt() {
        let engine = NoSqlEngine::open_memory(small_config()).expect("open memory engine");
        let schema = name_schema();
        engine
            .create_schema_collection(&schema)
            .expect("create schema collection");

        for (id, name) in [("n1", "a"), ("n2", "b"), ("n3", "c")] {
            engine
                .put_schema(&schema, name_document(id, name), WriteOptions::default())
                .expect("seed name document");
        }

        let options = query_opts(filter_gt("name", SchemaValue::String("b".to_string())));
        let result = engine
            .query_schema(&schema, options)
            .expect("query with Gt filter on string");
        assert_eq!(
            result.documents.len(),
            1,
            "Gt(\"b\") on [\"a\",\"b\",\"c\"] should return 1 doc (\"c\")"
        );
        assert_eq!(
            result.documents[0].document.field("name"),
            Some(&SchemaValue::String("c".to_string()))
        );
    }

    #[test]
    fn query_range_empty_result() {
        let engine = NoSqlEngine::open_memory(small_config()).expect("open memory engine");
        let schema = user_schema();
        engine
            .create_schema_collection(&schema)
            .expect("create schema collection");
        seed_users(&engine, &schema, 10, 20);

        let options = query_opts(filter_gt("age", SchemaValue::Int64(999)));
        let result = engine
            .query_schema(&schema, options)
            .expect("query with Gt filter that matches nothing");
        assert!(
            result.documents.is_empty(),
            "Gt(999) on ages 20..29 should return empty results"
        );
    }


    #[test]
    fn query_sort_asc_int64() {
        let engine = NoSqlEngine::open_memory(small_config()).expect("open memory engine");
        let schema = user_schema();
        engine
            .create_schema_collection(&schema)
            .expect("create schema collection");
        for &age in &[25, 20, 29, 22, 27, 21, 28, 24, 26, 23] {
            let idx = age - 20;
            let id = format!("u{idx}");
            let email = format!("u{idx}@example.com");
            engine
                .put_schema(
                    &schema,
                    user_document(&id, &email, age),
                    WriteOptions::default(),
                )
                .expect("seed put_schema");
        }

        let options = QueryOptions {
            filter: None,
            sort_field: Some("age".to_string()),
            sort_direction: SortDirection::Asc,
            limit: None,
            offset: None,
        };
        let result = engine
            .query_schema(&schema, options)
            .expect("query with sort by age ASC");
        let ages: Vec<i64> = result
            .documents
            .iter()
            .map(|r| match r.document.field("age") {
                Some(SchemaValue::Int64(v)) => *v,
                _ => panic!("expected Int64 age field"),
            })
            .collect();
        assert_eq!(ages, vec![20, 21, 22, 23, 24, 25, 26, 27, 28, 29]);
    }

    #[test]
    fn query_sort_desc_string() {
        let engine = NoSqlEngine::open_memory(small_config()).expect("open memory engine");
        let schema = name_schema();
        engine
            .create_schema_collection(&schema)
            .expect("create schema collection");

        for (id, name) in [("n1", "ada"), ("n2", "babbage"), ("n3", "curry")] {
            engine
                .put_schema(&schema, name_document(id, name), WriteOptions::default())
                .expect("seed name document");
        }

        let options = QueryOptions {
            filter: None,
            sort_field: Some("name".to_string()),
            sort_direction: SortDirection::Desc,
            limit: None,
            offset: None,
        };
        let result = engine
            .query_schema(&schema, options)
            .expect("query with sort by name DESC");
        let names: Vec<String> = result
            .documents
            .iter()
            .map(|r| match r.document.field("name") {
                Some(SchemaValue::String(v)) => v.clone(),
                _ => panic!("expected String name field"),
            })
            .collect();
        assert_eq!(names, vec!["curry", "babbage", "ada"]);
    }

    #[test]
    fn query_sort_tiebreaker() {
        let engine = NoSqlEngine::open_memory(small_config()).expect("open memory engine");
        let schema = user_schema();
        engine
            .create_schema_collection(&schema)
            .expect("create schema collection");

        for (id, age) in [("zz", 30), ("aa", 30), ("mm", 30)] {
            let email = format!("{id}@example.com");
            engine
                .put_schema(
                    &schema,
                    user_document(id, &email, age),
                    WriteOptions::default(),
                )
                .expect("seed tiebreaker doc");
        }

        let options = QueryOptions {
            filter: None,
            sort_field: Some("age".to_string()),
            sort_direction: SortDirection::Asc,
            limit: None,
            offset: None,
        };
        let result = engine
            .query_schema(&schema, options)
            .expect("query with tiebreaker sort");
        let ids: Vec<String> = result
            .documents
            .iter()
            .map(|r| match r.document.field("id") {
                Some(SchemaValue::String(v)) => v.clone(),
                _ => panic!("expected String id field"),
            })
            .collect();
        assert_eq!(
            ids,
            vec!["aa", "mm", "zz"],
            "Tiebreaker should sort by primary key bytes ascending"
        );
    }

    #[test]
    fn query_sort_null_missing() {
        let engine = NoSqlEngine::open_memory(small_config()).expect("open memory engine");
        let schema = user_schema_with_optional_nickname();
        engine
            .create_schema_collection(&schema)
            .expect("create schema collection");

        for (id, age, nick) in [
            ("a", 30, Some("charlie")),
            ("b", 20, None),
            ("c", 25, Some("alpha")),
            ("d", 20, None),
        ] {
            let email = format!("{id}@example.com");
            let doc = user_document_with_nickname(id, &email, age, nick);
            engine
                .put_schema(&schema, doc, WriteOptions::default())
                .expect("seed nickname doc");
        }

        let options = QueryOptions {
            filter: None,
            sort_field: Some("age".to_string()),
            sort_direction: SortDirection::Asc,
            limit: None,
            offset: None,
        };
        let result = engine
            .query_schema(&schema, options)
            .expect("query with optional age sort");
        let ages: Vec<i64> = result
            .documents
            .iter()
            .map(|r| match r.document.field("age") {
                Some(SchemaValue::Int64(v)) => *v,
                _ => panic!("expected Int64 age field"),
            })
            .collect();
        assert_eq!(ages, vec![20, 20, 25, 30]);
    }

    #[test]
    fn query_sort_optional_field_nulls_last() {
        let engine = NoSqlEngine::open_memory(small_config()).expect("open memory engine");
        let schema = user_schema_with_optional_nickname();
        engine
            .create_schema_collection(&schema)
            .expect("create schema collection");

        for (id, age, nick) in [
            ("a", 30, Some("charlie")),
            ("b", 20, Some("alpha")),
            ("c", 25, None),
            ("d", 40, Some("bravo")),
        ] {
            let email = format!("{id}@example.com");
            engine
                .put_schema(
                    &schema,
                    user_document_with_nickname(id, &email, age, nick),
                    WriteOptions::default(),
                )
                .expect("seed sort-null-last doc");
        }

        let options = QueryOptions {
            filter: None,
            sort_field: Some("nickname".to_string()),
            sort_direction: SortDirection::Asc,
            limit: None,
            offset: None,
        };
        let result = engine
            .query_schema(&schema, options)
            .expect("query sorting by optional nickname field");

        let nicks: Vec<Option<String>> = result
            .documents
            .iter()
            .map(|r| match r.document.field("nickname") {
                Some(SchemaValue::String(s)) => Some(s.clone()),
                _ => None,
            })
            .collect();
        let id_for_missing = result
            .documents
            .last()
            .expect("at least one result")
            .document
            .field("id")
            .cloned();
        assert_eq!(
            nicks,
            vec![
                Some("alpha".to_string()),
                Some("bravo".to_string()),
                Some("charlie".to_string()),
                None,
            ],
            "Null/missing nickname should sort last in ASC order"
        );
        assert_eq!(
            id_for_missing,
            Some(SchemaValue::String("c".to_string())),
            "Doc missing the nickname field should be the last item"
        );
    }


    #[test]
    fn query_paginate_limit() {
        let engine = NoSqlEngine::open_memory(small_config()).expect("open memory engine");
        let schema = user_schema();
        engine
            .create_schema_collection(&schema)
            .expect("create schema collection");
        seed_users(&engine, &schema, 10, 20);

        let options = QueryOptions {
            filter: None,
            sort_field: None,
            sort_direction: SortDirection::Asc,
            limit: Some(3),
            offset: None,
        };
        let result = engine
            .query_schema(&schema, options)
            .expect("query with limit=3");
        assert_eq!(result.documents.len(), 3);
    }

    #[test]
    fn query_paginate_offset() {
        let engine = NoSqlEngine::open_memory(small_config()).expect("open memory engine");
        let schema = user_schema();
        engine
            .create_schema_collection(&schema)
            .expect("create schema collection");
        seed_users(&engine, &schema, 10, 20);

        let options = QueryOptions {
            filter: None,
            sort_field: None,
            sort_direction: SortDirection::Asc,
            limit: None,
            offset: Some(5),
        };
        let result = engine
            .query_schema(&schema, options)
            .expect("query with offset=5");
        assert_eq!(result.documents.len(), 5, "offset=5 on 10 docs returns 5");
    }

    #[test]
    fn query_paginate_limit_offset() {
        let engine = NoSqlEngine::open_memory(small_config()).expect("open memory engine");
        let schema = user_schema();
        engine
            .create_schema_collection(&schema)
            .expect("create schema collection");
        seed_users(&engine, &schema, 10, 20);

        let options = QueryOptions {
            filter: None,
            sort_field: None,
            sort_direction: SortDirection::Asc,
            limit: Some(3),
            offset: Some(3),
        };
        let result = engine
            .query_schema(&schema, options)
            .expect("query with limit=3 offset=3");
        assert_eq!(result.documents.len(), 3);
        let ids: Vec<String> = result
            .documents
            .iter()
            .map(|r| match r.document.field("id") {
                Some(SchemaValue::String(v)) => v.clone(),
                _ => panic!("expected String id field"),
            })
            .collect();
        assert_eq!(ids, vec!["u3", "u4", "u5"]);
    }

    #[test]
    fn query_paginate_overflow() {
        let engine = NoSqlEngine::open_memory(small_config()).expect("open memory engine");
        let schema = user_schema();
        engine
            .create_schema_collection(&schema)
            .expect("create schema collection");
        seed_users(&engine, &schema, 10, 20);

        let options = QueryOptions {
            filter: None,
            sort_field: None,
            sort_direction: SortDirection::Asc,
            limit: Some(100),
            offset: None,
        };
        let result = engine
            .query_schema(&schema, options)
            .expect("query with limit=100 on 10 docs");
        assert_eq!(
            result.documents.len(),
            10,
            "limit larger than result set should return all 10 docs"
        );
    }


    #[test]
    fn update_set_field() {
        let engine = NoSqlEngine::open_memory(small_config()).expect("open memory engine");
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
        let engine = NoSqlEngine::open_memory(small_config()).expect("open memory engine");
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
        let engine = NoSqlEngine::open_memory(small_config()).expect("open memory engine");
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
            matches!(err, EngineError::PrimaryKeyUpdateForbidden),
            "expected PrimaryKeyUpdateForbidden, got {err:?}"
        );
    }

    #[test]
    fn update_increment_int64() {
        let engine = NoSqlEngine::open_memory(small_config()).expect("open memory engine");
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
        let engine = NoSqlEngine::open_memory(small_config()).expect("open memory engine");
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
            EngineError::NumericOverflow { field } => {
                assert_eq!(field, "age");
            }
            other => panic!("expected NumericOverflow, got {other:?}"),
        }
    }

    #[test]
    fn update_increment_type_mismatch() {
        let engine = NoSqlEngine::open_memory(small_config()).expect("open memory engine");
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
            EngineError::UpdateTypeMismatch {
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
        let engine = NoSqlEngine::open_memory(small_config()).expect("open memory engine");
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
            matches!(err, EngineError::PrimaryKeyUpdateForbidden),
            "expected PrimaryKeyUpdateForbidden, got {err:?}"
        );
    }

    #[test]
    fn update_document_not_found() {
        let engine = NoSqlEngine::open_memory(small_config()).expect("open memory engine");
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
            EngineError::DocumentNotFound { collection } => {
                assert_eq!(collection, "users");
            }
            other => panic!("expected DocumentNotFound, got {other:?}"),
        }
    }


    #[test]
    fn multiget_basic() {
        let engine = NoSqlEngine::open_memory(small_config()).expect("open memory engine");
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
        let engine = NoSqlEngine::open_memory(small_config()).expect("open memory engine");
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
        let engine = NoSqlEngine::open_memory(small_config()).expect("open memory engine");
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
        let engine = NoSqlEngine::open_memory(small_config()).expect("open memory engine");
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
        let engine = NoSqlEngine::open_memory(small_config()).expect("open memory engine");
        let schema = user_schema();
        engine
            .create_schema_collection(&schema)
            .expect("create schema collection");
        seed_users(&engine, &schema, 7, 70);

        let count = engine
            .count_schema(&schema, None)
            .expect("count unfiltered");
        assert_eq!(count, 7);
    }

    #[test]
    fn count_filtered_eq() {
        let engine = NoSqlEngine::open_memory(small_config()).expect("open memory engine");
        let schema = user_schema();
        engine
            .create_schema_collection(&schema)
            .expect("create schema collection");
        seed_users(&engine, &schema, 10, 80);

        let count = engine
            .count_schema(
                &schema,
                Some(filter_eq("age", SchemaValue::Int64(85))),
            )
            .expect("count with Eq filter");
        assert_eq!(count, 1, "Eq(85) should match exactly 1 doc");
    }

    #[test]
    fn count_filtered_range() {
        let engine = NoSqlEngine::open_memory(small_config()).expect("open memory engine");
        let schema = user_schema();
        engine
            .create_schema_collection(&schema)
            .expect("create schema collection");
        seed_users(&engine, &schema, 10, 90);

        let count = engine
            .count_schema(
                &schema,
                Some(filter_gt("age", SchemaValue::Int64(95))),
            )
            .expect("count with Gt filter");
        assert_eq!(count, 4, "Gt(95) on ages 90..99 should count 4 docs");
    }

    #[test]
    fn count_after_delete() {
        let engine = NoSqlEngine::open_memory(small_config()).expect("open memory engine");
        let schema = user_schema();
        engine
            .create_schema_collection(&schema)
            .expect("create schema collection");
        seed_users(&engine, &schema, 5, 100);

        let before = engine
            .count_schema(&schema, None)
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
            .count_schema(&schema, None)
            .expect("count after delete");
        assert_eq!(after, 4, "count should decrease by 1 after deleting u2");
    }


    #[test]
    fn combined_count_matches_query() {
        let engine = NoSqlEngine::open_memory(small_config()).expect("open memory engine");
        let schema = user_schema();
        engine
            .create_schema_collection(&schema)
            .expect("create schema collection");
        seed_users(&engine, &schema, 10, 110);

        let filter = filter_gte("age", SchemaValue::Int64(116));
        let counted = engine
            .count_schema(&schema, Some(filter.clone()))
            .expect("count with Gte filter");
        let queried = engine
            .query_schema(&schema, query_opts(filter))
            .expect("query with Gte filter");
        assert_eq!(
            counted as usize,
            queried.documents.len(),
            "count(filter) should equal query(filter).len()"
        );
        assert_eq!(counted, 4);
    }

    #[test]
    fn combined_update_then_query() {
        let engine = NoSqlEngine::open_memory(small_config()).expect("open memory engine");
        let schema = user_schema();
        engine
            .create_schema_collection(&schema)
            .expect("create schema collection");
        seed_users(&engine, &schema, 3, 120);

        let before = engine
            .query_schema(
                &schema,
                query_opts(filter_eq("age", SchemaValue::Int64(200))),
            )
            .expect("query before update");
        assert_eq!(before.documents.len(), 0);

        let update = FieldUpdateOp {
            updates: vec![FieldUpdate::Increment {
                field: "age".to_string(),
                delta: 200 - 120,
            }],
        };
        engine
            .update_schema(
                &schema,
                &SchemaValue::String("u0".to_string()),
                update,
                WriteOptions::default(),
            )
            .expect("update u0 age");

        let after = engine
            .query_schema(
                &schema,
                query_opts(filter_eq("age", SchemaValue::Int64(200))),
            )
            .expect("query after update");
        assert_eq!(after.documents.len(), 1);
        assert_eq!(
            after.documents[0].document.field("id"),
            Some(&SchemaValue::String("u0".to_string()))
        );
    }
}
