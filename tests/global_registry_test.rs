use heed::EnvOpenOptions;
use scoped_heed::{GlobalScopeRegistry, Scope, ScopedDbError, scoped_database_options};
use std::sync::Arc;

// Helper function to create a test environment
fn setup_test_env() -> (tempfile::TempDir, heed::Env) {
    let temp_dir = tempfile::tempdir().unwrap();
    let env = unsafe {
        EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024)
            .max_dbs(10)
            .open(temp_dir.path())
            .unwrap()
    };

    (temp_dir, env)
}

#[test]
fn test_global_registry_basic() -> Result<(), ScopedDbError> {
    let (_temp_dir, env) = setup_test_env();

    // Create a global registry
    let mut wtxn = env.write_txn()?;
    let registry = Arc::new(GlobalScopeRegistry::new(&env, &mut wtxn)?);
    wtxn.commit()?;

    // Create some scopes
    let scope1 = Scope::named("tenant1")?;
    let scope2 = Scope::named("tenant2")?;

    // Create databases with the shared registry
    let mut wtxn = env.write_txn()?;
    let db_users = scoped_database_options(&env, registry.clone())
        .types::<String, String>()
        .name("users")
        .create(&mut wtxn)?;
    let db_posts = scoped_database_options(&env, registry.clone())
        .bytes_keys::<String>()
        .name("posts")
        .create(&mut wtxn)?;
    wtxn.commit()?;

    // Add data to scopes
    let mut wtxn = env.write_txn()?;
    db_users.put(
        &mut wtxn,
        &scope1,
        &"alice".to_string(),
        &"user data".to_string(),
    )?;
    db_posts.put(&mut wtxn, &scope1, b"post1", &"post content".to_string())?;
    db_posts.put(&mut wtxn, &scope2, b"post1", &"another post".to_string())?;
    wtxn.commit()?;

    // Create a block so rtxn is dropped at the end of the block
    {
        let rtxn = env.read_txn()?;
        let scopes = registry.list_all_scopes(&rtxn)?;

        // Should have Default + tenant1 + tenant2
        assert_eq!(scopes.len(), 3);

        // Check we can find both tenant scopes
        let has_tenant1 = scopes.iter().any(|s| {
            if let Scope::Named { name, .. } = s {
                name == "tenant1"
            } else {
                false
            }
        });

        let has_tenant2 = scopes.iter().any(|s| {
            if let Scope::Named { name, .. } = s {
                name == "tenant2"
            } else {
                false
            }
        });

        assert!(has_tenant1, "Registry should have tenant1");
        assert!(has_tenant2, "Registry should have tenant2");
    }

    // Test registry doesn't allow hash collisions in a separate transaction
    if let Scope::Named { hash, .. } = &scope1 {
        let rtxn = env.read_txn()?;
        let name = registry.get_scope_name(&rtxn, hash)?;
        assert_eq!(name, Some("tenant1".to_string()));
        // Explicitly drop the transaction
        drop(rtxn);
    }

    // TempDir will be automatically cleaned up when dropped
    Ok(())
}

#[test]
fn test_multiple_databases_sharing_registry() -> Result<(), ScopedDbError> {
    let (_temp_dir, env) = setup_test_env();

    // Create a global registry
    let mut wtxn = env.write_txn()?;
    let registry = Arc::new(GlobalScopeRegistry::new(&env, &mut wtxn)?);
    wtxn.commit()?;

    // Create three different database types with the shared registry
    let mut wtxn = env.write_txn()?;
    let db1 = scoped_database_options(&env, registry.clone())
        .types::<String, String>()
        .name("db1")
        .create(&mut wtxn)?;
    let db2 = scoped_database_options(&env, registry.clone())
        .bytes_keys::<String>()
        .name("db2")
        .create(&mut wtxn)?;
    wtxn.commit()?;
    let mut wtxn = env.write_txn()?;
    let db3 = scoped_database_options(&env, registry.clone())
        .raw_bytes()
        .name("db3")
        .create(&mut wtxn)?;
    wtxn.commit()?;

    // Create some test data across databases for the same scope
    let tenant = Scope::named("shared_tenant")?;

    let mut wtxn = env.write_txn()?;
    db1.put(
        &mut wtxn,
        &tenant,
        &"key1".to_string(),
        &"value1".to_string(),
    )?;
    db2.put(&mut wtxn, &tenant, b"key2", &"value2".to_string())?;
    db3.put(&mut wtxn, &tenant, b"key3", b"value3")?;
    wtxn.commit()?;

    // Check that data is stored correctly in a block to ensure the transaction is dropped
    {
        let rtxn = env.read_txn()?;
        let val1 = db1.get(&rtxn, &tenant, &"key1".to_string())?;
        let val2 = db2.get(&rtxn, &tenant, b"key2")?;
        let val3 = db3.get(&rtxn, &tenant, b"key3")?;

        assert_eq!(val1, Some("value1".to_string()));
        assert_eq!(val2, Some("value2".to_string()));
        assert_eq!(val3.map(|v| v == b"value3"), Some(true));
        // Transaction dropped at end of block
    }

    // Create a block for the read transaction to ensure it's dropped
    {
        let rtxn = env.read_txn()?;
        let scopes = registry.list_all_scopes(&rtxn)?;

        // Should have Default + shared_tenant
        assert_eq!(scopes.len(), 2);

        // Verify scope content
        let shared_tenant_present = scopes.iter().any(|s| {
            if let Scope::Named { name, .. } = s {
                name == "shared_tenant"
            } else {
                false
            }
        });

        assert!(shared_tenant_present, "Registry should have shared_tenant");

        // The transaction will be dropped at the end of this block
    }

    // Find empty scopes
    let mut wtxn = env.write_txn()?;

    // Clear scope in one database
    db1.clear(&mut wtxn, &tenant)?;

    // The scope is still not empty overall because it's used in db2 and db3
    let empty_count_db1 = db1.find_empty_scopes(&mut wtxn)?;
    assert_eq!(empty_count_db1, 1, "DB1 should find one empty scope");

    // After clearing all databases, the scope should be empty
    db2.clear(&mut wtxn, &tenant)?;
    db3.clear(&mut wtxn, &tenant)?;

    let empty_count_db1_after = db1.find_empty_scopes(&mut wtxn)?;
    assert_eq!(
        empty_count_db1_after, 1,
        "DB1 should still find one empty scope"
    );

    wtxn.commit()?;

    // Test pruning empty scopes
    let mut wtxn = env.write_txn()?;

    // Verify we have the shared_tenant in the registry using a scope to drop the transaction
    {
        let rtxn = env.read_txn()?;
        let initial_scopes = registry.list_all_scopes(&rtxn)?;
        assert_eq!(
            initial_scopes.len(),
            2,
            "Registry should have two scopes (Default + shared_tenant)"
        );
        // rtxn is dropped at the end of this scope
    }

    // Prune globally empty scopes using the global registry
    let databases: [&dyn scoped_heed::ScopeEmptinessChecker; 3] = [&db1, &db2, &db3];
    let pruned_count = registry.prune_globally_unused_scopes(&mut wtxn, &databases)?;
    assert_eq!(pruned_count, 1, "Should have pruned 1 empty scope");

    // Check that the scope was removed from the registry
    let scopes_after_prune = registry.list_all_scopes(&wtxn)?;
    assert_eq!(
        scopes_after_prune.len(),
        1,
        "Registry should only have Default scope after pruning"
    );
    assert!(
        scopes_after_prune
            .iter()
            .all(|s| matches!(s, Scope::Default)),
        "Only the Default scope should remain"
    );

    wtxn.commit()?;

    // TempDir will be automatically cleaned up when dropped
    Ok(())
}
