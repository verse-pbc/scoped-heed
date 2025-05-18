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
        db.put(&mut wtxn, Some("tenant1"), &"key1".to_string(), &"value1".to_string())?;
        db.put(&mut wtxn, Some("tenant2"), &"key1".to_string(), &"value2".to_string())?;
        
        // Test default scope
        db.put(&mut wtxn, None, &"global_key".to_string(), &"global_value".to_string())?;
        
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
    let result = db.put(&mut wtxn, Some(""), &"key".to_string(), &"value".to_string());
    
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
        string_db.put(&mut wtxn, Some("tenant1"), &"name".to_string(), &"Alice".to_string())?;
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