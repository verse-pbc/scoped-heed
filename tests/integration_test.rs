use heed::{Env, EnvOpenOptions};
use scoped_heed::{ScopedDbError, scoped_database_options};
use std::fs;
use std::path::PathBuf;

struct TestEnv {
    env: Env,
    db_path: PathBuf,
}

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
fn test_basic_operations() -> Result<(), ScopedDbError> {
    let test_env = TestEnv::new("basic_ops")?;
    let env = &test_env.env;

    let mut wtxn = env.write_txn()?;
    let db = scoped_database_options(env)
        .types::<String, String>()
        .name("test")
        .create(&mut wtxn)?;
    wtxn.commit()?;

    {
        let mut wtxn = env.write_txn()?;

        // Test scoped operations
        db.put(
            &mut wtxn,
            Some("tenant1"),
            &"key1".to_string(),
            &"value1".to_string(),
        )?;
        db.put(
            &mut wtxn,
            Some("tenant2"),
            &"key1".to_string(),
            &"value2".to_string(),
        )?;

        // Test default scope
        db.put(
            &mut wtxn,
            None,
            &"global_key".to_string(),
            &"global_value".to_string(),
        )?;

        wtxn.commit()?;
    }

    {
        let rtxn = env.read_txn()?;

        // Verify scoped data isolation
        let val1 = db.get(&rtxn, Some("tenant1"), &"key1".to_string())?;
        assert_eq!(val1, Some("value1".to_string()));

        let val2 = db.get(&rtxn, Some("tenant2"), &"key1".to_string())?;
        assert_eq!(val2, Some("value2".to_string()));

        // Verify default scope
        let global_val = db.get(&rtxn, None, &"global_key".to_string())?;
        assert_eq!(global_val, Some("global_value".to_string()));

        // Verify non-existent keys
        let missing = db.get(&rtxn, Some("tenant1"), &"missing".to_string())?;
        assert_eq!(missing, None);
    }

    Ok(())
}

#[test]
fn test_empty_scope_error() -> Result<(), ScopedDbError> {
    let test_env = TestEnv::new("empty_scope")?;
    let env = &test_env.env;

    let mut wtxn = env.write_txn()?;
    let db = scoped_database_options(env)
        .types::<String, String>()
        .name("test")
        .create(&mut wtxn)?;
    wtxn.commit()?;

    let mut wtxn = env.write_txn()?;

    // Test that empty scope string is rejected
    let result = db.put(
        &mut wtxn,
        Some(""),
        &"key".to_string(),
        &"value".to_string(),
    );

    match result {
        Err(ScopedDbError::EmptyScopeDisallowed) => Ok(()),
        _ => panic!("Expected EmptyScopeDisallowed error"),
    }
}

#[test]
fn test_multiple_databases() -> Result<(), ScopedDbError> {
    let test_env = TestEnv::new("multiple_dbs")?;
    let env = &test_env.env;

    // Create two separate databases with different types
    let mut wtxn = env.write_txn()?;
    let string_db = scoped_database_options(env)
        .types::<String, String>()
        .name("strings")
        .create(&mut wtxn)?;
    let int_db = scoped_database_options(env)
        .types::<String, u32>()
        .name("integers")
        .create(&mut wtxn)?;
    wtxn.commit()?;

    {
        let mut wtxn = env.write_txn()?;

        // Store different types in different databases
        string_db.put(
            &mut wtxn,
            Some("tenant1"),
            &"name".to_string(),
            &"Alice".to_string(),
        )?;
        int_db.put(&mut wtxn, Some("tenant1"), &"age".to_string(), &30u32)?;

        wtxn.commit()?;
    }

    {
        let rtxn = env.read_txn()?;

        let name = string_db.get(&rtxn, Some("tenant1"), &"name".to_string())?;
        assert_eq!(name, Some("Alice".to_string()));

        let age = int_db.get(&rtxn, Some("tenant1"), &"age".to_string())?;
        assert_eq!(age, Some(30));
    }

    Ok(())
}

#[test]
fn test_delete_operations() -> Result<(), ScopedDbError> {
    let test_env = TestEnv::new("delete_ops")?;
    let env = &test_env.env;

    let mut wtxn = env.write_txn()?;
    let db = scoped_database_options(env)
        .types::<String, String>()
        .name("test")
        .create(&mut wtxn)?;
    wtxn.commit()?;

    // Insert some data
    {
        let mut wtxn = env.write_txn()?;
        db.put(
            &mut wtxn,
            Some("scope1"),
            &"key1".to_string(),
            &"value1".to_string(),
        )?;
        db.put(
            &mut wtxn,
            Some("scope1"),
            &"key2".to_string(),
            &"value2".to_string(),
        )?;
        db.put(
            &mut wtxn,
            None,
            &"key1".to_string(),
            &"default_value".to_string(),
        )?;
        wtxn.commit()?;
    }

    // Delete a key
    {
        let mut wtxn = env.write_txn()?;
        let deleted = db.delete(&mut wtxn, Some("scope1"), &"key1".to_string())?;
        assert!(deleted);

        // Try to delete non-existent key
        let not_deleted = db.delete(&mut wtxn, Some("scope1"), &"key3".to_string())?;
        assert!(!not_deleted);

        wtxn.commit()?;
    }

    // Verify deletion
    {
        let rtxn = env.read_txn()?;
        let val1 = db.get(&rtxn, Some("scope1"), &"key1".to_string())?;
        assert_eq!(val1, None);

        let val2 = db.get(&rtxn, Some("scope1"), &"key2".to_string())?;
        assert_eq!(val2, Some("value2".to_string()));

        let default_val = db.get(&rtxn, None, &"key1".to_string())?;
        assert_eq!(default_val, Some("default_value".to_string()));
    }

    Ok(())
}

#[test]
fn test_clear_operations() -> Result<(), ScopedDbError> {
    let test_env = TestEnv::new("clear_ops")?;
    let env = &test_env.env;

    let mut wtxn = env.write_txn()?;
    let db = scoped_database_options(env)
        .types::<String, String>()
        .name("test")
        .create(&mut wtxn)?;
    wtxn.commit()?;

    // Insert data in multiple scopes
    {
        let mut wtxn = env.write_txn()?;
        db.put(
            &mut wtxn,
            Some("scope1"),
            &"key1".to_string(),
            &"value1".to_string(),
        )?;
        db.put(
            &mut wtxn,
            Some("scope1"),
            &"key2".to_string(),
            &"value2".to_string(),
        )?;
        db.put(
            &mut wtxn,
            Some("scope2"),
            &"key1".to_string(),
            &"value3".to_string(),
        )?;
        db.put(
            &mut wtxn,
            None,
            &"key1".to_string(),
            &"default_value".to_string(),
        )?;
        wtxn.commit()?;
    }

    // Clear a specific scope
    {
        let mut wtxn = env.write_txn()?;
        db.clear(&mut wtxn, Some("scope1"))?;
        wtxn.commit()?;
    }

    // Verify only scope1 was cleared
    {
        let rtxn = env.read_txn()?;
        let val1 = db.get(&rtxn, Some("scope1"), &"key1".to_string())?;
        assert_eq!(val1, None);

        let val2 = db.get(&rtxn, Some("scope1"), &"key2".to_string())?;
        assert_eq!(val2, None);

        let val3 = db.get(&rtxn, Some("scope2"), &"key1".to_string())?;
        assert_eq!(val3, Some("value3".to_string()));

        let default_val = db.get(&rtxn, None, &"key1".to_string())?;
        assert_eq!(default_val, Some("default_value".to_string()));
    }

    // Clear default scope
    {
        let mut wtxn = env.write_txn()?;
        db.clear(&mut wtxn, None)?;
        wtxn.commit()?;
    }

    // Verify default scope was cleared
    {
        let rtxn = env.read_txn()?;
        let default_val = db.get(&rtxn, None, &"key1".to_string())?;
        assert_eq!(default_val, None);

        // scope2 should still have data
        let val3 = db.get(&rtxn, Some("scope2"), &"key1".to_string())?;
        assert_eq!(val3, Some("value3".to_string()));
    }

    Ok(())
}

#[test]
fn test_iter_operations() -> Result<(), ScopedDbError> {
    let test_env = TestEnv::new("iter_ops")?;
    let env = &test_env.env;

    let mut wtxn = env.write_txn()?;
    let db = scoped_database_options(env)
        .types::<String, String>()
        .name("test")
        .create(&mut wtxn)?;
    wtxn.commit()?;

    // Insert data in multiple scopes
    {
        let mut wtxn = env.write_txn()?;
        db.put(
            &mut wtxn,
            Some("scope1"),
            &"a".to_string(),
            &"value1".to_string(),
        )?;
        db.put(
            &mut wtxn,
            Some("scope1"),
            &"b".to_string(),
            &"value2".to_string(),
        )?;
        db.put(
            &mut wtxn,
            Some("scope1"),
            &"c".to_string(),
            &"value3".to_string(),
        )?;
        db.put(
            &mut wtxn,
            Some("scope2"),
            &"x".to_string(),
            &"value4".to_string(),
        )?;
        db.put(
            &mut wtxn,
            Some("scope2"),
            &"y".to_string(),
            &"value5".to_string(),
        )?;
        db.put(&mut wtxn, None, &"d".to_string(), &"default1".to_string())?;
        db.put(&mut wtxn, None, &"e".to_string(), &"default2".to_string())?;
        wtxn.commit()?;
    }

    // Iterate over scope1
    {
        let rtxn = env.read_txn()?;
        let mut items: Vec<(String, String)> = vec![];

        for result in db.iter(&rtxn, Some("scope1"))? {
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

        for result in db.iter(&rtxn, None)? {
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

    let mut wtxn = env.write_txn()?;
    let db = scoped_database_options(env)
        .types::<String, String>()
        .name("test")
        .create(&mut wtxn)?;
    wtxn.commit()?;

    // Insert ordered data
    {
        let mut wtxn = env.write_txn()?;

        // Default scope
        db.put(&mut wtxn, None, &"a".to_string(), &"default_a".to_string())?;
        db.put(&mut wtxn, None, &"b".to_string(), &"default_b".to_string())?;
        db.put(&mut wtxn, None, &"c".to_string(), &"default_c".to_string())?;
        db.put(&mut wtxn, None, &"d".to_string(), &"default_d".to_string())?;

        // Scope1
        db.put(
            &mut wtxn,
            Some("scope1"),
            &"a".to_string(),
            &"scope1_a".to_string(),
        )?;
        db.put(
            &mut wtxn,
            Some("scope1"),
            &"b".to_string(),
            &"scope1_b".to_string(),
        )?;
        db.put(
            &mut wtxn,
            Some("scope1"),
            &"c".to_string(),
            &"scope1_c".to_string(),
        )?;
        db.put(
            &mut wtxn,
            Some("scope1"),
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
        for result in db.range(&rtxn, Some("scope1"), &range)? {
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
        for result in db.range(&rtxn, None, &range)? {
            let (key, value) = result?;
            items.push((key, value));
        }

        assert_eq!(items.len(), 2);
        assert_eq!(items[0], ("b".to_string(), "default_b".to_string()));
        assert_eq!(items[1], ("c".to_string(), "default_c".to_string()));
    }

    // Test range with bytes database
    let mut wtxn = env.write_txn()?;
    let bytes_db = scoped_database_options(env)
        .bytes_keys::<String>()
        .name("bytes_test")
        .create(&mut wtxn)?;
    wtxn.commit()?;

    {
        let mut wtxn = env.write_txn()?;

        // Insert byte keys
        bytes_db.put(&mut wtxn, Some("scope1"), b"key1", &"value1".to_string())?;
        bytes_db.put(&mut wtxn, Some("scope1"), b"key2", &"value2".to_string())?;
        bytes_db.put(&mut wtxn, Some("scope1"), b"key3", &"value3".to_string())?;

        wtxn.commit()?;
    }

    // Test byte range [key1, key2]
    {
        let rtxn = env.read_txn()?;
        let mut items: Vec<(&[u8], String)> = vec![];

        let key1: &[u8] = b"key1";
        let key2: &[u8] = b"key2";
        let range = key1..=key2;
        for result in bytes_db.range(&rtxn, Some("scope1"), &range)? {
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
