use heed::{Env, EnvOpenOptions};
use scoped_heed::{GlobalScopeRegistry, Scope, ScopedDbError, scoped_database_options};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

struct TestEnv {
    env: Env,
    db_path: PathBuf,
}

// Unused helper function has been removed

impl TestEnv {
    fn new(test_name: &str) -> Result<Self, ScopedDbError> {
        let db_path = PathBuf::from(format!("/tmp/test_db_{}", test_name));

        if db_path.exists() {
            fs::remove_dir_all(&db_path).unwrap();
        }
        fs::create_dir_all(&db_path).unwrap();

        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(10 * 1024 * 1024) // 10MB
                .max_dbs(20) // Increased from 5 to 20 to support metadata databases
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
fn test_basic_operations() -> Result<(), ScopedDbError> {
    let test_env = TestEnv::new("basic_ops")?;
    let env = &test_env.env;

    // Create a global registry for tracking scopes
    let mut wtxn = env.write_txn()?;
    let registry = Arc::new(GlobalScopeRegistry::new(env, &mut wtxn)?);
    wtxn.commit()?;

    // Create the database with the registry
    let mut wtxn = env.write_txn()?;
    let db = scoped_database_options(env, registry.clone())
        .types::<String, String>()
        .name("test")
        .create(&mut wtxn)?;
    wtxn.commit()?;

    {
        let mut wtxn = env.write_txn()?;

        // Test scoped operations
        let tenant1_scope = Scope::named("tenant1")?;
        let tenant2_scope = Scope::named("tenant2")?;

        db.put(
            &mut wtxn,
            &tenant1_scope,
            &"key1".to_string(),
            &"value1".to_string(),
        )?;
        db.put(
            &mut wtxn,
            &tenant2_scope,
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

        // Print contents for debugging
        println!("Debugging scoped data...");
        for scope_name in ["tenant1", "tenant2"].iter() {
            println!("Checking scope: {}", scope_name);
            // Create Scope enum directly for debugging
            let scope = Scope::named(scope_name)?;

            // Get value with Scope API
            let val = db.get(&rtxn, &scope, &"key1".to_string())?;
            println!("  Value: {:?}", val);
        }

        // Verify scoped data isolation
        let tenant1_scope = Scope::named("tenant1")?;
        let tenant2_scope = Scope::named("tenant2")?;

        let val1 = db.get(&rtxn, &tenant1_scope, &"key1".to_string())?;
        assert_eq!(val1, Some("value1".to_string()));

        let val2 = db.get(&rtxn, &tenant2_scope, &"key1".to_string())?;
        assert_eq!(val2, Some("value2".to_string()));

        // Verify default scope
        let global_val = db.get(&rtxn, &Scope::Default, &"global_key".to_string())?;
        assert_eq!(global_val, Some("global_value".to_string()));

        // Verify non-existent keys
        let missing = db.get(&rtxn, &tenant1_scope, &"missing".to_string())?;
        assert_eq!(missing, None);
    }

    Ok(())
}

#[test]
fn test_empty_scope_error() -> Result<(), ScopedDbError> {
    let test_env = TestEnv::new("empty_scope")?;
    let env = &test_env.env;

    // Create a global registry for tracking scopes
    let mut wtxn = env.write_txn()?;
    let registry = Arc::new(GlobalScopeRegistry::new(env, &mut wtxn)?);
    wtxn.commit()?;

    // Create the database with the registry
    let mut wtxn = env.write_txn()?;
    let _db = scoped_database_options(env, registry.clone())
        .types::<String, String>()
        .name("test")
        .create(&mut wtxn)?;
    wtxn.commit()?;

    let mut _wtxn = env.write_txn()?;

    // Test that empty scope string is rejected
    let result = Scope::named("");

    match result {
        Err(ScopedDbError::EmptyScopeDisallowed) => Ok(()),
        _ => panic!("Expected EmptyScopeDisallowed error"),
    }
}

#[test]
fn test_multiple_databases() -> Result<(), ScopedDbError> {
    let test_env = TestEnv::new("multiple_dbs")?;
    let env = &test_env.env;

    // Create a global registry for tracking scopes
    let mut wtxn = env.write_txn()?;
    let registry = Arc::new(GlobalScopeRegistry::new(env, &mut wtxn)?);
    wtxn.commit()?;

    // Create two separate databases with different types
    let mut wtxn = env.write_txn()?;
    let string_db = scoped_database_options(env, registry.clone())
        .types::<String, String>()
        .name("strings")
        .create(&mut wtxn)?;
    let int_db = scoped_database_options(env, registry.clone())
        .types::<String, u32>()
        .name("integers")
        .create(&mut wtxn)?;
    wtxn.commit()?;

    {
        let mut wtxn = env.write_txn()?;

        // Store different types in different databases
        let tenant1_scope = Scope::named("tenant1")?;

        string_db.put(
            &mut wtxn,
            &tenant1_scope,
            &"name".to_string(),
            &"Alice".to_string(),
        )?;
        int_db.put(&mut wtxn, &tenant1_scope, &"age".to_string(), &30u32)?;

        wtxn.commit()?;
    }

    {
        let rtxn = env.read_txn()?;

        let tenant1_scope = Scope::named("tenant1")?;

        let name = string_db.get(&rtxn, &tenant1_scope, &"name".to_string())?;
        assert_eq!(name, Some("Alice".to_string()));

        let age = int_db.get(&rtxn, &tenant1_scope, &"age".to_string())?;
        assert_eq!(age, Some(30));
    }

    Ok(())
}

#[test]
fn test_delete_operations() -> Result<(), ScopedDbError> {
    let test_env = TestEnv::new("delete_ops")?;
    let env = &test_env.env;

    // Create a global registry for tracking scopes
    let mut wtxn = env.write_txn()?;
    let registry = Arc::new(GlobalScopeRegistry::new(env, &mut wtxn)?);
    wtxn.commit()?;

    // Create the database with the registry
    let mut wtxn = env.write_txn()?;
    let db = scoped_database_options(env, registry.clone())
        .types::<String, String>()
        .name("test")
        .create(&mut wtxn)?;
    wtxn.commit()?;

    // Insert some data
    {
        let mut wtxn = env.write_txn()?;
        let scope1 = Scope::named("scope1")?;

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
        let scope1 = Scope::named("scope1")?;

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
        let scope1 = Scope::named("scope1")?;

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
fn test_clear_operations() -> Result<(), ScopedDbError> {
    let test_env = TestEnv::new("clear_ops")?;
    let env = &test_env.env;

    // Create a global registry for tracking scopes
    let mut wtxn = env.write_txn()?;
    let registry = Arc::new(GlobalScopeRegistry::new(env, &mut wtxn)?);
    wtxn.commit()?;

    // Create the database with the registry
    let mut wtxn = env.write_txn()?;
    let db = scoped_database_options(env, registry.clone())
        .types::<String, String>()
        .name("test")
        .create(&mut wtxn)?;
    wtxn.commit()?;

    // Insert data in multiple scopes
    {
        let mut wtxn = env.write_txn()?;
        let scope1 = Scope::named("scope1")?;
        let scope2 = Scope::named("scope2")?;

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
        let scope1 = Scope::named("scope1")?;
        db.clear(&mut wtxn, &scope1)?;
        wtxn.commit()?;
    }

    // Verify only scope1 was cleared
    {
        let rtxn = env.read_txn()?;
        let scope1 = Scope::named("scope1")?;
        let scope2 = Scope::named("scope2")?;

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
        let scope2 = Scope::named("scope2")?;

        let default_val = db.get(&rtxn, &Scope::Default, &"key1".to_string())?;
        assert_eq!(default_val, None);

        // scope2 should still have data
        let val3 = db.get(&rtxn, &scope2, &"key1".to_string())?;
        assert_eq!(val3, Some("value3".to_string()));
    }

    Ok(())
}

#[test]
fn test_iter_operations() -> Result<(), ScopedDbError> {
    let test_env = TestEnv::new("iter_ops")?;
    let env = &test_env.env;

    // Create a global registry for tracking scopes
    let mut wtxn = env.write_txn()?;
    let registry = Arc::new(GlobalScopeRegistry::new(env, &mut wtxn)?);
    wtxn.commit()?;

    // Create the database with the registry
    let mut wtxn = env.write_txn()?;
    let db = scoped_database_options(env, registry.clone())
        .types::<String, String>()
        .name("test")
        .create(&mut wtxn)?;
    wtxn.commit()?;

    // Insert data in multiple scopes
    {
        let mut wtxn = env.write_txn()?;
        let scope1 = Scope::named("scope1")?;
        db.put(&mut wtxn, &scope1, &"a".to_string(), &"value1".to_string())?;
        db.put(&mut wtxn, &scope1, &"b".to_string(), &"value2".to_string())?;
        db.put(&mut wtxn, &scope1, &"c".to_string(), &"value3".to_string())?;
        let scope2 = Scope::named("scope2")?;
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

        let scope1 = Scope::named("scope1")?;
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
fn test_range_operations() -> Result<(), ScopedDbError> {
    let test_env = TestEnv::new("range_ops")?;
    let env = &test_env.env;

    // Create a global registry for tracking scopes
    let mut wtxn = env.write_txn()?;
    let registry = Arc::new(GlobalScopeRegistry::new(env, &mut wtxn)?);
    wtxn.commit()?;

    // Create the database with the registry
    let mut wtxn = env.write_txn()?;
    let db = scoped_database_options(env, registry.clone())
        .types::<String, String>()
        .name("test")
        .create(&mut wtxn)?;
    wtxn.commit()?;

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
        let scope1 = Scope::named("scope1")?;
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
        let scope1 = Scope::named("scope1")?;
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

    // Test range with bytes database
    let mut wtxn = env.write_txn()?;
    let bytes_db = scoped_database_options(env, registry.clone())
        .bytes_keys::<String>()
        .name("bytes_test")
        .create(&mut wtxn)?;
    wtxn.commit()?;

    {
        let mut wtxn = env.write_txn()?;

        // Insert byte keys
        let scope1 = Scope::named("scope1")?;
        bytes_db.put(&mut wtxn, &scope1, b"key1", &"value1".to_string())?;
        bytes_db.put(&mut wtxn, &scope1, b"key2", &"value2".to_string())?;
        bytes_db.put(&mut wtxn, &scope1, b"key3", &"value3".to_string())?;

        wtxn.commit()?;
    }

    // Test byte range [key1, key2]
    {
        let rtxn = env.read_txn()?;
        let mut items: Vec<(&[u8], String)> = vec![];

        let key1: &[u8] = b"key1";
        let key2: &[u8] = b"key2";
        let range = key1..=key2;
        let scope1 = Scope::named("scope1")?;
        for result in bytes_db.range(&rtxn, &scope1, &range)? {
            let (key, value) = result?;
            items.push((key, value));
        }

        assert_eq!(items.len(), 2);
        assert_eq!(items[0].0, b"key1");
        assert_eq!(items[0].1, "value1");
        assert_eq!(items[1].0, b"key2");
        assert_eq!(items[1].1, "value2");
    }

    Ok(())
}
