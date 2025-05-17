use std::fs;
use std::path::Path;

use heed::EnvOpenOptions;
use scoped_heed::{ScopedDatabase, ScopedDbError};

fn main() -> Result<(), ScopedDbError> {
    // Create a temporary database directory
    let db_path = Path::new("./example_basic_db");
    fs::create_dir_all(db_path)
        .map_err(|e| ScopedDbError::InvalidInput(format!("Failed to create DB dir: {}", e)))?;

    // Open the environment
    let env = unsafe {
        EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024) // 10MB
            .max_dbs(3) // Minimum required for ScopedDatabase
            .open(db_path)?
    };

    // Initialize the scoped database
    let scoped_db = ScopedDatabase::new(&env)?;
    println!("ScopedDatabase initialized");

    // Write data to different scopes
    {
        let mut wtxn = env.write_txn()?;
        
        // Default scope (None)
        scoped_db.put(&mut wtxn, None, "key1", "value1")?;
        scoped_db.put(&mut wtxn, None, "key2", "value2")?;
        
        // Named scope
        scoped_db.put(&mut wtxn, Some("tenant1"), "key1", "tenant1_value1")?;
        scoped_db.put(&mut wtxn, Some("tenant1"), "key2", "tenant1_value2")?;
        
        wtxn.commit()?;
    }

    // Read data from different scopes
    {
        let rtxn = env.read_txn()?;
        
        // Read from default scope
        let value = scoped_db.get(&rtxn, None, "key1")?;
        println!("Default scope - key1: {:?}", value);
        
        // Read from named scope
        let value = scoped_db.get(&rtxn, Some("tenant1"), "key1")?;
        println!("Tenant1 scope - key1: {:?}", value);
        
        // Non-existent key returns None
        let value = scoped_db.get(&rtxn, Some("tenant1"), "key3")?;
        println!("Tenant1 scope - key3 (non-existent): {:?}", value);
    }

    // Update existing value
    {
        let mut wtxn = env.write_txn()?;
        scoped_db.put(&mut wtxn, None, "key1", "updated_value1")?;
        wtxn.commit()?;
    }

    // Verify update
    {
        let rtxn = env.read_txn()?;
        let value = scoped_db.get(&rtxn, None, "key1")?;
        println!("Default scope - key1 (after update): {:?}", value);
    }

    // Delete key
    {
        let mut wtxn = env.write_txn()?;
        let deleted = scoped_db.delete(&mut wtxn, None, "key2")?;
        println!("Deleted key2 from default scope: {}", deleted);
        
        // Trying to delete non-existent key returns false
        let deleted = scoped_db.delete(&mut wtxn, None, "key_not_exist")?;
        println!("Deleted non-existent key: {}", deleted);
        
        wtxn.commit()?;
    }

    println!("Basic usage example completed successfully!");
    
    // Clean up
    let _ = fs::remove_dir_all(db_path);
    
    Ok(())
}