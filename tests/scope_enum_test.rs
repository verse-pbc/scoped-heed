//! Test suite specifically for the new Scope enum API and metadata features
use heed::{Env, EnvOpenOptions};
use scoped_heed::{GlobalScopeRegistry, Scope, ScopedDbError, scoped_database_options};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

struct TestEnv {
    env: Env,
    db_path: PathBuf,
}

impl TestEnv {
    fn new(test_name: &str) -> Result<Self, ScopedDbError> {
        let db_path = PathBuf::from(format!("/tmp/test_scope_enum_{}", test_name));

        if db_path.exists() {
            fs::remove_dir_all(&db_path).unwrap();
        }
        fs::create_dir_all(&db_path).unwrap();

        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(10 * 1024 * 1024)
                .max_dbs(5)
                .open(&db_path)?
        };
        Ok(TestEnv { env, db_path })
    }
}

impl Drop for TestEnv {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.db_path);
    }
}

#[test]
fn test_scope_enum_basic_operations() -> Result<(), ScopedDbError> {
    let test_env = TestEnv::new("basic_ops")?;
    let env = &test_env.env;

    // Create a global registry for tracking scopes
    let mut wtxn = env.write_txn()?;
    let registry = Arc::new(GlobalScopeRegistry::new(env, &mut wtxn)?);
    wtxn.commit()?;

    let mut wtxn = env.write_txn()?;
    let db = scoped_database_options(env, registry.clone())
        .types::<String, String>()
        .name("test_scope_enum")
        .create(&mut wtxn)?;
    wtxn.commit()?;

    // Create scope objects
    let tenant1 = Scope::named("tenant1")?;
    let tenant2 = Scope::named("tenant2")?;

    {
        let mut wtxn = env.write_txn()?;

        // Test scoped operations with Scope enum
        db.put(
            &mut wtxn,
            &tenant1,
            &"key1".to_string(),
            &"value1".to_string(),
        )?;
        db.put(
            &mut wtxn,
            &tenant2,
            &"key1".to_string(),
            &"value2".to_string(),
        )?;

        // Test default scope
        db.put(
            &mut wtxn,
            &Scope::Default,
            &"global_key".to_string(),
            &"global_value".to_string(),
        )?;

        wtxn.commit()?;
    }

    {
        let rtxn = env.read_txn()?;

        // Verify scoped data isolation
        let val1 = db.get(&rtxn, &tenant1, &"key1".to_string())?;
        assert_eq!(val1, Some("value1".to_string()));

        let val2 = db.get(&rtxn, &tenant2, &"key1".to_string())?;
        assert_eq!(val2, Some("value2".to_string()));

        // Verify default scope
        let global_val = db.get(&rtxn, &Scope::Default, &"global_key".to_string())?;
        assert_eq!(global_val, Some("global_value".to_string()));

        // Verify non-existent keys
        let missing = db.get(&rtxn, &tenant1, &"missing".to_string())?;
        assert_eq!(missing, None);
    }

    Ok(())
}

#[test]
fn test_scope_listing_and_pruning() -> Result<(), ScopedDbError> {
    let test_env = TestEnv::new("listing_pruning")?;
    let env = &test_env.env;

    // Create a global registry for tracking scopes
    let mut wtxn = env.write_txn()?;
    let registry = Arc::new(GlobalScopeRegistry::new(env, &mut wtxn)?);
    wtxn.commit()?;

    let mut wtxn = env.write_txn()?;
    let db = scoped_database_options(env, registry.clone())
        .types::<String, String>()
        .name("test")
        .create(&mut wtxn)?;
    wtxn.commit()?;

    // Create scope objects
    let tenant1 = Scope::named("tenant1")?;
    let tenant2 = Scope::named("tenant2")?;
    let tenant3 = Scope::named("tenant3")?;

    // Add data to scopes 1 and 2 but not 3
    {
        let mut wtxn = env.write_txn()?;

        // Add data to default scope
        db.put(
            &mut wtxn,
            &Scope::Default,
            &"key1".to_string(),
            &"default_value".to_string(),
        )?;

        // Add data to tenant1
        db.put(
            &mut wtxn,
            &tenant1,
            &"key1".to_string(),
            &"tenant1_value".to_string(),
        )?;

        // Add data to tenant2
        db.put(
            &mut wtxn,
            &tenant2,
            &"key1".to_string(),
            &"tenant2_value".to_string(),
        )?;

        // Register tenant3 but don't add data
        db.register_scope(&mut wtxn, &tenant3)?;

        wtxn.commit()?;
    }

    // List scopes
    {
        let rtxn = env.read_txn()?;
        let scopes = db.list_scopes(&rtxn)?;

        // Should have 4 scopes: Default + 3 named scopes
        assert_eq!(scopes.len(), 4);

        // Verify all scopes are present
        let has_default = scopes.iter().any(|s| matches!(s, Scope::Default));
        let has_tenant1 = scopes
            .iter()
            .any(|s| matches!(s, Scope::Named { name, .. } if name == "tenant1"));
        let has_tenant2 = scopes
            .iter()
            .any(|s| matches!(s, Scope::Named { name, .. } if name == "tenant2"));
        let has_tenant3 = scopes
            .iter()
            .any(|s| matches!(s, Scope::Named { name, .. } if name == "tenant3"));

        assert!(has_default, "Default scope missing");
        assert!(has_tenant1, "tenant1 scope missing");
        assert!(has_tenant2, "tenant2 scope missing");
        assert!(has_tenant3, "tenant3 scope missing");
    }

    // Find empty scopes
    {
        let mut wtxn = env.write_txn()?;
        let empty_scopes = db.find_empty_scopes(&mut wtxn)?;

        // Should have found only tenant3 as empty
        assert_eq!(empty_scopes, 1, "Wrong number of empty scopes found");

        wtxn.commit()?;
    }

    // Now let's test the prune_empty_scopes functionality
    {
        let rtxn = env.read_txn()?;
        let scopes_before = db.list_scopes(&rtxn)?;

        // Should have 4 scopes: Default + tenant1 + tenant2 + tenant3
        assert_eq!(scopes_before.len(), 4);

        let mut wtxn = env.write_txn()?;

        // Use the global registry to prune empty scopes instead of the database method
        let databases: [&dyn scoped_heed::ScopeEmptinessChecker; 1] = [&db];
        let pruned_count = registry.prune_globally_unused_scopes(&mut wtxn, &databases)?;

        // Should have pruned only tenant3
        assert_eq!(pruned_count, 1, "Wrong number of scopes pruned");

        wtxn.commit()?;
    }

    // Verify tenant3 was pruned
    {
        let rtxn = env.read_txn()?;
        let scopes = db.list_scopes(&rtxn)?;

        // Should have 3 scopes now: Default + tenant1 + tenant2
        assert_eq!(scopes.len(), 3);

        let has_tenant3 = scopes
            .iter()
            .any(|s| matches!(s, Scope::Named { name, .. } if name == "tenant3"));
        assert!(!has_tenant3, "tenant3 scope should have been pruned");
    }

    Ok(())
}

#[test]
fn test_delete_operations_with_scope_enum() -> Result<(), ScopedDbError> {
    let test_env = TestEnv::new("delete_ops_scope")?;
    let env = &test_env.env;

    // Create a global registry for tracking scopes
    let mut wtxn = env.write_txn()?;
    let registry = Arc::new(GlobalScopeRegistry::new(env, &mut wtxn)?);
    wtxn.commit()?;

    let mut wtxn = env.write_txn()?;
    let db = scoped_database_options(env, registry.clone())
        .types::<String, String>()
        .name("test")
        .create(&mut wtxn)?;
    wtxn.commit()?;

    // Create scope
    let scope1 = Scope::named("scope1")?;

    // Insert some data
    {
        let mut wtxn = env.write_txn()?;
        db.put(
            &mut wtxn,
            &scope1,
            &"key1".to_string(),
            &"value1".to_string(),
        )?;
        db.put(
            &mut wtxn,
            &scope1,
            &"key2".to_string(),
            &"value2".to_string(),
        )?;
        db.put(
            &mut wtxn,
            &Scope::Default,
            &"key1".to_string(),
            &"default_value".to_string(),
        )?;
        wtxn.commit()?;
    }

    // Delete a key
    {
        let mut wtxn = env.write_txn()?;
        let deleted = db.delete(&mut wtxn, &scope1, &"key1".to_string())?;
        assert!(deleted);

        // Try to delete non-existent key
        let not_deleted = db.delete(&mut wtxn, &scope1, &"key3".to_string())?;
        assert!(!not_deleted);

        wtxn.commit()?;
    }

    // Verify deletion
    {
        let rtxn = env.read_txn()?;
        let val1 = db.get(&rtxn, &scope1, &"key1".to_string())?;
        assert_eq!(val1, None);

        let val2 = db.get(&rtxn, &scope1, &"key2".to_string())?;
        assert_eq!(val2, Some("value2".to_string()));

        let default_val = db.get(&rtxn, &Scope::Default, &"key1".to_string())?;
        assert_eq!(default_val, Some("default_value".to_string()));
    }

    Ok(())
}

#[test]
fn test_clear_operations_with_scope_enum() -> Result<(), ScopedDbError> {
    let test_env = TestEnv::new("clear_ops_scope")?;
    let env = &test_env.env;

    // Create a global registry for tracking scopes
    let mut wtxn = env.write_txn()?;
    let registry = Arc::new(GlobalScopeRegistry::new(env, &mut wtxn)?);
    wtxn.commit()?;

    let mut wtxn = env.write_txn()?;
    let db = scoped_database_options(env, registry.clone())
        .types::<String, String>()
        .name("test")
        .create(&mut wtxn)?;
    wtxn.commit()?;

    // Create scopes
    let scope1 = Scope::named("scope1")?;
    let scope2 = Scope::named("scope2")?;

    // Insert data in multiple scopes
    {
        let mut wtxn = env.write_txn()?;
        db.put(
            &mut wtxn,
            &scope1,
            &"key1".to_string(),
            &"value1".to_string(),
        )?;
        db.put(
            &mut wtxn,
            &scope1,
            &"key2".to_string(),
            &"value2".to_string(),
        )?;
        db.put(
            &mut wtxn,
            &scope2,
            &"key1".to_string(),
            &"value3".to_string(),
        )?;
        db.put(
            &mut wtxn,
            &Scope::Default,
            &"key1".to_string(),
            &"default_value".to_string(),
        )?;
        wtxn.commit()?;
    }

    // Clear a specific scope
    {
        let mut wtxn = env.write_txn()?;
        db.clear(&mut wtxn, &scope1)?;
        wtxn.commit()?;
    }

    // Verify only scope1 was cleared
    {
        let rtxn = env.read_txn()?;
        let val1 = db.get(&rtxn, &scope1, &"key1".to_string())?;
        assert_eq!(val1, None);

        let val2 = db.get(&rtxn, &scope1, &"key2".to_string())?;
        assert_eq!(val2, None);

        let val3 = db.get(&rtxn, &scope2, &"key1".to_string())?;
        assert_eq!(val3, Some("value3".to_string()));

        let default_val = db.get(&rtxn, &Scope::Default, &"key1".to_string())?;
        assert_eq!(default_val, Some("default_value".to_string()));
    }

    // Clear default scope
    {
        let mut wtxn = env.write_txn()?;
        db.clear(&mut wtxn, &Scope::Default)?;
        wtxn.commit()?;
    }

    // Verify default scope was cleared
    {
        let rtxn = env.read_txn()?;
        let default_val = db.get(&rtxn, &Scope::Default, &"key1".to_string())?;
        assert_eq!(default_val, None);

        // scope2 should still have data
        let val3 = db.get(&rtxn, &scope2, &"key1".to_string())?;
        assert_eq!(val3, Some("value3".to_string()));
    }

    Ok(())
}

#[test]
fn test_iter_operations_with_scope_enum() -> Result<(), ScopedDbError> {
    let test_env = TestEnv::new("iter_ops_scope")?;
    let env = &test_env.env;

    // Create a global registry for tracking scopes
    let mut wtxn = env.write_txn()?;
    let registry = Arc::new(GlobalScopeRegistry::new(env, &mut wtxn)?);
    wtxn.commit()?;

    let mut wtxn = env.write_txn()?;
    let db = scoped_database_options(env, registry.clone())
        .types::<String, String>()
        .name("test")
        .create(&mut wtxn)?;
    wtxn.commit()?;

    // Create scopes
    let scope1 = Scope::named("scope1")?;
    let scope2 = Scope::named("scope2")?;

    // Insert data in multiple scopes
    {
        let mut wtxn = env.write_txn()?;
        db.put(&mut wtxn, &scope1, &"a".to_string(), &"value1".to_string())?;
        db.put(&mut wtxn, &scope1, &"b".to_string(), &"value2".to_string())?;
        db.put(&mut wtxn, &scope1, &"c".to_string(), &"value3".to_string())?;
        db.put(&mut wtxn, &scope2, &"x".to_string(), &"value4".to_string())?;
        db.put(&mut wtxn, &scope2, &"y".to_string(), &"value5".to_string())?;
        db.put(
            &mut wtxn,
            &Scope::Default,
            &"d".to_string(),
            &"default1".to_string(),
        )?;
        db.put(
            &mut wtxn,
            &Scope::Default,
            &"e".to_string(),
            &"default2".to_string(),
        )?;
        wtxn.commit()?;
    }

    // Iterate over scope1
    {
        let rtxn = env.read_txn()?;
        let mut items: Vec<(String, String)> = vec![];

        for result in db.iter(&rtxn, &scope1)? {
            let (key, value) = result?;
            items.push((key, value));
        }

        assert_eq!(items.len(), 3);
        // Sort for consistent comparison
        items.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(items[0], ("a".to_string(), "value1".to_string()));
        assert_eq!(items[1], ("b".to_string(), "value2".to_string()));
        assert_eq!(items[2], ("c".to_string(), "value3".to_string()));
    }

    // Iterate over default scope
    {
        let rtxn = env.read_txn()?;
        let mut items: Vec<(String, String)> = vec![];

        for result in db.iter(&rtxn, &Scope::Default)? {
            let (key, value) = result?;
            items.push((key, value));
        }

        assert_eq!(items.len(), 2);
        items.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(items[0], ("d".to_string(), "default1".to_string()));
        assert_eq!(items[1], ("e".to_string(), "default2".to_string()));
    }

    Ok(())
}

#[test]
fn test_range_operations_with_scope_enum() -> Result<(), ScopedDbError> {
    let test_env = TestEnv::new("range_ops_scope")?;
    let env = &test_env.env;

    // Create a global registry for tracking scopes
    let mut wtxn = env.write_txn()?;
    let registry = Arc::new(GlobalScopeRegistry::new(env, &mut wtxn)?);
    wtxn.commit()?;

    let mut wtxn = env.write_txn()?;
    let db = scoped_database_options(env, registry.clone())
        .types::<String, String>()
        .name("test")
        .create(&mut wtxn)?;
    wtxn.commit()?;

    // Create scopes
    let scope1 = Scope::named("scope1")?;

    // Insert ordered data
    {
        let mut wtxn = env.write_txn()?;

        // Default scope
        db.put(
            &mut wtxn,
            &Scope::Default,
            &"a".to_string(),
            &"default_a".to_string(),
        )?;
        db.put(
            &mut wtxn,
            &Scope::Default,
            &"b".to_string(),
            &"default_b".to_string(),
        )?;
        db.put(
            &mut wtxn,
            &Scope::Default,
            &"c".to_string(),
            &"default_c".to_string(),
        )?;
        db.put(
            &mut wtxn,
            &Scope::Default,
            &"d".to_string(),
            &"default_d".to_string(),
        )?;

        // Scope1
        db.put(
            &mut wtxn,
            &scope1,
            &"a".to_string(),
            &"scope1_a".to_string(),
        )?;
        db.put(
            &mut wtxn,
            &scope1,
            &"b".to_string(),
            &"scope1_b".to_string(),
        )?;
        db.put(
            &mut wtxn,
            &scope1,
            &"c".to_string(),
            &"scope1_c".to_string(),
        )?;
        db.put(
            &mut wtxn,
            &scope1,
            &"d".to_string(),
            &"scope1_d".to_string(),
        )?;

        wtxn.commit()?;
    }

    // Test inclusive range [b, c]
    {
        let rtxn = env.read_txn()?;
        let mut items: Vec<(String, String)> = vec![];

        let range = "b".to_string()..="c".to_string();
        for result in db.range(&rtxn, &scope1, &range)? {
            let (key, value) = result?;
            items.push((key, value));
        }

        assert_eq!(items.len(), 2);
        assert_eq!(items[0], ("b".to_string(), "scope1_b".to_string()));
        assert_eq!(items[1], ("c".to_string(), "scope1_c".to_string()));
    }

    // Test exclusive range (a, d)
    {
        let rtxn = env.read_txn()?;
        let mut items: Vec<(String, String)> = vec![];

        use std::ops::Bound;
        let range = (
            Bound::Excluded("a".to_string()),
            Bound::Excluded("d".to_string()),
        );
        for result in db.range(&rtxn, &Scope::Default, &range)? {
            let (key, value) = result?;
            items.push((key, value));
        }

        assert_eq!(items.len(), 2);
        assert_eq!(items[0], ("b".to_string(), "default_b".to_string()));
        assert_eq!(items[1], ("c".to_string(), "default_c".to_string()));
    }

    Ok(())
}

#[test]
fn test_bytes_database_with_scope_enum() -> Result<(), ScopedDbError> {
    let test_env = TestEnv::new("bytes_scope")?;
    let env = &test_env.env;

    // Create a global registry for tracking scopes
    let mut wtxn = env.write_txn()?;
    let registry = Arc::new(GlobalScopeRegistry::new(env, &mut wtxn)?);
    wtxn.commit()?;

    // Create a ScopedBytesDatabase
    let mut wtxn = env.write_txn()?;
    let db = scoped_database_options(env, registry.clone())
        .raw_bytes()
        .name("bytes_test")
        .create(&mut wtxn)?;
    wtxn.commit()?;

    // Create scope
    let scope1 = Scope::named("scope1")?;

    // Insert data
    {
        let mut wtxn = env.write_txn()?;
        db.put(&mut wtxn, &scope1, b"key1", b"value1")?;
        db.put(&mut wtxn, &scope1, b"key2", b"value2")?;
        db.put(&mut wtxn, &Scope::Default, b"key1", b"default1")?;
        wtxn.commit()?;
    }

    // Verify data
    {
        let rtxn = env.read_txn()?;
        let val1 = db.get(&rtxn, &scope1, b"key1")?;
        assert_eq!(val1, Some(&b"value1"[..]));

        let val2 = db.get(&rtxn, &scope1, b"key2")?;
        assert_eq!(val2, Some(&b"value2"[..]));

        let default_val = db.get(&rtxn, &Scope::Default, b"key1")?;
        assert_eq!(default_val, Some(&b"default1"[..]));
    }

    // List scopes
    {
        let rtxn = env.read_txn()?;
        let scopes = db.list_scopes(&rtxn)?;

        // Should have 2 scopes: Default + scope1
        assert_eq!(scopes.len(), 2);

        let has_scope1 = scopes
            .iter()
            .any(|s| matches!(s, Scope::Named { name, .. } if name == "scope1"));
        assert!(has_scope1, "scope1 missing");
    }

    Ok(())
}
