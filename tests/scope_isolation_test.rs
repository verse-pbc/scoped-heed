//! Test suite specifically for verifying Redis-like scope isolation
use scoped_heed::{scoped_database_options, ScopedDbError};
use heed::{Env, EnvOpenOptions};
use std::fs;
use std::path::PathBuf;

struct TestEnv {
    env: Env,
    db_path: PathBuf,
}

impl TestEnv {
    fn new(test_name: &str) -> Result<Self, ScopedDbError> {
        let db_path = PathBuf::from(format!("/tmp/test_scope_isolation_{}", test_name));
        
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
fn test_scope_isolation_like_redis() -> Result<(), ScopedDbError> {
    let test_env = TestEnv::new("redis_like")?;
    let env = &test_env.env;
    
    // Create a database
    let mut wtxn = env.write_txn()?;
    let db = scoped_database_options(env)
        .types::<String, String>()
        .name("isolated_db")
        .create(&mut wtxn)?;
    wtxn.commit()?;
    
    // Insert same key in different scopes - like Redis DB 0, DB 1, etc.
    let mut wtxn = env.write_txn()?;
    db.put(&mut wtxn, Some("db0"), &"mykey".to_string(), &"value_in_db0".to_string())?;
    db.put(&mut wtxn, Some("db1"), &"mykey".to_string(), &"value_in_db1".to_string())?;
    db.put(&mut wtxn, Some("db2"), &"mykey".to_string(), &"value_in_db2".to_string())?;
    wtxn.commit()?;
    
    // Verify complete isolation
    let rtxn = env.read_txn()?;
    assert_eq!(
        db.get(&rtxn, Some("db0"), &"mykey".to_string())?,
        Some("value_in_db0".to_string())
    );
    assert_eq!(
        db.get(&rtxn, Some("db1"), &"mykey".to_string())?,
        Some("value_in_db1".to_string())
    );
    assert_eq!(
        db.get(&rtxn, Some("db2"), &"mykey".to_string())?,
        Some("value_in_db2".to_string())
    );
    
    Ok(())
}

#[test]
fn test_no_cross_scope_access() -> Result<(), ScopedDbError> {
    let test_env = TestEnv::new("no_cross_access")?;
    let env = &test_env.env;
    
    // Create a database
    let mut wtxn = env.write_txn()?;
    let db = scoped_database_options(env)
        .types::<String, i32>()
        .name("secure_db")
        .create(&mut wtxn)?;
    wtxn.commit()?;
    
    // Insert data in scope A
    let mut wtxn = env.write_txn()?;
    db.put(&mut wtxn, Some("scope_a"), &"secret".to_string(), &42)?;
    db.put(&mut wtxn, Some("scope_a"), &"public".to_string(), &100)?;
    wtxn.commit()?;
    
    // Verify scope B cannot see scope A's data (no cross-scope access)
    let rtxn = env.read_txn()?;
    assert_eq!(db.get(&rtxn, Some("scope_b"), &"secret".to_string())?, None);
    assert_eq!(db.get(&rtxn, Some("scope_b"), &"public".to_string())?, None);
    
    // Verify iteration is also scoped
    let count_a = db.iter(&rtxn, Some("scope_a"))?.count();
    let count_b = db.iter(&rtxn, Some("scope_b"))?.count();
    assert_eq!(count_a, 2);
    assert_eq!(count_b, 0);
    
    Ok(())
}

#[test]
fn test_scope_operations_are_independent() -> Result<(), ScopedDbError> {
    let test_env = TestEnv::new("independent_ops")?;
    let env = &test_env.env;
    
    // Create a database
    let mut wtxn = env.write_txn()?;
    let db = scoped_database_options(env)
        .types::<String, String>()
        .name("independent_db")
        .create(&mut wtxn)?;
    wtxn.commit()?;
    
    // Populate multiple scopes
    let mut wtxn = env.write_txn()?;
    for i in 0..5 {
        let scope = format!("tenant_{}", i);
        for j in 0..10 {
            let key = format!("key_{}", j);
            let value = format!("value_{}_{}", i, j);
            db.put(&mut wtxn, Some(&scope), &key, &value)?;
        }
    }
    wtxn.commit()?;
    
    // Clear one scope - others remain unaffected
    let mut wtxn = env.write_txn()?;
    db.clear(&mut wtxn, Some("tenant_2"))?;
    wtxn.commit()?;
    
    // Verify only tenant_2 was cleared
    let rtxn = env.read_txn()?;
    assert_eq!(db.iter(&rtxn, Some("tenant_0"))?.count(), 10);
    assert_eq!(db.iter(&rtxn, Some("tenant_1"))?.count(), 10);
    assert_eq!(db.iter(&rtxn, Some("tenant_2"))?.count(), 0); // Cleared
    assert_eq!(db.iter(&rtxn, Some("tenant_3"))?.count(), 10);
    assert_eq!(db.iter(&rtxn, Some("tenant_4"))?.count(), 10);
    
    Ok(())
}

#[test]
fn test_range_queries_respect_scope_boundaries() -> Result<(), ScopedDbError> {
    let test_env = TestEnv::new("range_boundaries")?;
    let env = &test_env.env;
    
    // Create a bytes database for range testing
    let mut wtxn = env.write_txn()?;
    let db = scoped_database_options(env)
        .bytes_keys::<String>()
        .name("range_test")
        .create(&mut wtxn)?;
    wtxn.commit()?;
    
    // Insert data in different scopes
    let mut wtxn = env.write_txn()?;
    
    // Scope A: keys 00-09
    for i in 0..10 {
        let key = format!("key{:02}", i);
        let value = format!("scope_a_{}", i);
        db.put(&mut wtxn, Some("scope_a"), key.as_bytes(), &value)?;
    }
    
    // Scope B: keys 05-14 (overlapping key range)
    for i in 5..15 {
        let key = format!("key{:02}", i);
        let value = format!("scope_b_{}", i);
        db.put(&mut wtxn, Some("scope_b"), key.as_bytes(), &value)?;
    }
    
    wtxn.commit()?;
    
    // Range query on scope A
    let rtxn = env.read_txn()?;
    let range = b"key05".as_ref()..=b"key08".as_ref();
    
    let scope_a_results: Vec<_> = db.range(&rtxn, Some("scope_a"), &range)?
        .collect::<Result<Vec<_>, _>>()?;
    
    let scope_b_results: Vec<_> = db.range(&rtxn, Some("scope_b"), &range)?
        .collect::<Result<Vec<_>, _>>()?;
    
    // Both scopes have the same key range, but completely different values
    assert_eq!(scope_a_results.len(), 4); // keys 05-08
    assert_eq!(scope_b_results.len(), 4); // keys 05-08
    
    // Verify the values are from the correct scopes
    for (_, value) in &scope_a_results {
        assert!(value.starts_with("scope_a_"));
    }
    
    for (_, value) in &scope_b_results {
        assert!(value.starts_with("scope_b_"));
    }
    
    Ok(())
}

#[test]
fn test_scope_names_are_arbitrary_strings() -> Result<(), ScopedDbError> {
    let test_env = TestEnv::new("arbitrary_names")?;
    let env = &test_env.env;
    
    // Create a database
    let mut wtxn = env.write_txn()?;
    let db = scoped_database_options(env)
        .types::<String, String>()
        .name("names_test")
        .create(&mut wtxn)?;
    wtxn.commit()?;
    
    // Test various scope names that might be used in real applications
    let scope_names = vec![
        "production",
        "staging",
        "user:12345",
        "tenant:acme-corp",
        "region:us-west-2",
        "test_2024_01_15",
        "namespace.subnamespace",
    ];
    
    let mut wtxn = env.write_txn()?;
    for scope in &scope_names {
        db.put(&mut wtxn, Some(scope), &"test_key".to_string(), &scope.to_string())?;
    }
    wtxn.commit()?;
    
    // Verify each scope maintains its own data
    let rtxn = env.read_txn()?;
    for scope in &scope_names {
        let value = db.get(&rtxn, Some(scope), &"test_key".to_string())?;
        assert_eq!(value, Some(scope.to_string()));
    }
    
    Ok(())
}