use heed::EnvOpenOptions;
use scoped_heed::{GlobalScopeRegistry, Scope, ScopedDatabase};
use std::sync::Arc;
use tempfile::tempdir;

#[test]
fn test_cloned_db_shares_global_registry() {
    let dir = tempdir().unwrap();
    let env = unsafe {
        EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024) // 10MB
            .max_dbs(10) // Increased for registry dbs: scope_data, scope_hashes, scope_metadata
            .open(dir.path())
            .unwrap()
    };

    // 1. Create the GlobalScopeRegistry
    let global_registry = {
        let mut wtxn_init = env.write_txn().unwrap();
        let reg = Arc::new(GlobalScopeRegistry::new(&env, &mut wtxn_init).unwrap());
        wtxn_init.commit().unwrap();
        reg
    };

    // 2. Create the original database instance with the global registry
    let db_original = ScopedDatabase::<String, String>::new(
        &env,
        "test_shared_scope",
        global_registry.clone()
    )
    .unwrap();

    // 3. Use a scope with the original database
    let scope1_name = "my_shared_scope";
    let scope1 = Scope::named(scope1_name).unwrap();
    let key1 = "key1_original".to_string();
    let value1 = "value1_original".to_string();

    // Put will attempt to register the scope with the registry
    let mut wtxn = env.write_txn().unwrap();
    db_original.put(&mut wtxn, &scope1, &key1, &value1).unwrap();
    wtxn.commit().unwrap();

    // 4. Clone the database instance. It will share the same Arc<GlobalScopeRegistry>
    let db_cloned = db_original.clone();

    // 5. Access the same scope with the cloned database
    let rtxn_cloned = env.read_txn().unwrap();
    let retrieved_value1_cloned = db_cloned.get(&rtxn_cloned, &scope1, &key1).unwrap();
    assert_eq!(retrieved_value1_cloned, Some(value1.clone()));
    rtxn_cloned.commit().unwrap();

    // 6. Write to the same scope using the cloned DB
    let key2_cloned = "key2_cloned".to_string();
    let value2_cloned = "value2_cloned".to_string();
    let mut wtxn_cloned = env.write_txn().unwrap();
    db_cloned
        .put(&mut wtxn_cloned, &scope1, &key2_cloned, &value2_cloned)
        .unwrap();
    wtxn_cloned.commit().unwrap();

    // 7. Verify the write from the cloned DB can be read by the original DB
    let rtxn_original = env.read_txn().unwrap();
    let retrieved_value2_original = db_original
        .get(&rtxn_original, &scope1, &key2_cloned)
        .unwrap();
    assert_eq!(retrieved_value2_original, Some(value2_cloned.clone()));

    let retrieved_value1_original = db_original.get(&rtxn_original, &scope1, &key1).unwrap();
    assert_eq!(retrieved_value1_original, Some(value1.clone()));
    rtxn_original.commit().unwrap();

    // 8. Use a new scope with the CLONED database first
    let scope2_name = "new_scope_via_cloned";
    let scope2 = Scope::named(scope2_name).unwrap();
    let key3_cloned = "key3_new_scope".to_string();
    let value3_cloned = "value3_new_scope".to_string();

    let mut wtxn_cloned_2 = env.write_txn().unwrap();
    db_cloned // This will also attempt to register scope2 with the shared registry
        .put(&mut wtxn_cloned_2, &scope2, &key3_cloned, &value3_cloned)
        .unwrap();
    wtxn_cloned_2.commit().unwrap();

    // 9. Verify the original DB can see the new scope and its data (because registry is shared)
    let rtxn_original_2 = env.read_txn().unwrap();
    let retrieved_value3_original = db_original
        .get(&rtxn_original_2, &scope2, &key3_cloned)
        .unwrap();
    assert_eq!(retrieved_value3_original, Some(value3_cloned.clone()));
    rtxn_original_2.commit().unwrap();

    // 10. Test default scope consistency
    let default_scope = Scope::Default;
    let default_key_original = "default_key_orig".to_string();
    let default_val_original = "default_val_orig".to_string();
    let mut wtxn_orig_default = env.write_txn().unwrap();
    db_original
        .put(
            &mut wtxn_orig_default,
            &default_scope,
            &default_key_original,
            &default_val_original,
        )
        .unwrap();
    wtxn_orig_default.commit().unwrap();

    let rtxn_cloned_default = env.read_txn().unwrap();
    let retrieved_default_cloned = db_cloned
        .get(&rtxn_cloned_default, &default_scope, &default_key_original)
        .unwrap();
    assert_eq!(retrieved_default_cloned, Some(default_val_original.clone()));
    rtxn_cloned_default.commit().unwrap();

    let default_key_cloned = "default_key_cloned".to_string();
    let default_val_cloned = "default_val_cloned".to_string();
    let mut wtxn_cloned_default = env.write_txn().unwrap();
    db_cloned
        .put(
            &mut wtxn_cloned_default,
            &default_scope,
            &default_key_cloned,
            &default_val_cloned,
        )
        .unwrap();
    wtxn_cloned_default.commit().unwrap();

    let rtxn_orig_default = env.read_txn().unwrap();
    let retrieved_default_orig = db_original
        .get(&rtxn_orig_default, &default_scope, &default_key_cloned)
        .unwrap();
    assert_eq!(retrieved_default_orig, Some(default_val_cloned.clone()));
    rtxn_orig_default.commit().unwrap();
}
