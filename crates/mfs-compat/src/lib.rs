//! Legacy SQL, object-store, and SQLite-shaped compatibility adapters.

pub mod object_store;
pub mod object_store_durability;
pub mod page_store;
pub mod page_vfs;
pub mod schema_flush;
pub mod schema_store;

#[cfg(test)]
mod tests {
    #[test]
    fn compat_module_imports_compile() {
        use crate::object_store::MfsObjectStore;
        use crate::page_store::{InMemoryPageStore, MfsPageStore};
        use crate::page_vfs::MfsPageVfs;
        use crate::schema_flush::quote_ident;
        use crate::schema_store::{SchemaKey, SchemaStore};

        let object_store = MfsObjectStore::with_capacity(8);
        object_store.set_integer(b"n".to_vec(), 1);
        assert_eq!(object_store.get_integer(b"n"), Ok(Some(1)));

        let schema_store = SchemaStore::new();
        assert!(schema_store.collection_names().is_empty());
        let _schema_key = SchemaKey::String("id".to_string());

        let page_store = InMemoryPageStore::new();
        let page_vfs = MfsPageVfs::new(page_store.clone());
        let file = page_vfs.x_open("compile.db").unwrap();
        assert_eq!(page_store.file_size(file.file_id()), Ok(0));

        assert_eq!(quote_ident("mfs"), "\"mfs\"");
    }
}
