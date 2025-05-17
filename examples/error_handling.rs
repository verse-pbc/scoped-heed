use std::fs;
use std::path::Path;

use heed::EnvOpenOptions;
use scoped_heed::{ScopedDatabase, ScopedDbError};

fn main() -> Result<(), ScopedDbError> {
    let db_path = Path::new("./example_error_handling_db");
    fs::create_dir_all(db_path)
        .map_err(|e| ScopedDbError::InvalidInput(format!("Failed to create DB dir: {}", e)))?;

    let env = unsafe {
        EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024)
            .max_dbs(3)
            .open(db_path)?
    };

    let scoped_db = ScopedDatabase::new(&env)?;

    // Demonstrate various error scenarios and proper handling
    
    // 1. Invalid scope name (empty string)
    println!("Testing invalid scope name (empty string):");
    {
        let mut wtxn = env.write_txn()?;
        match scoped_db.put(&mut wtxn, Some(""), "key", "value") {
            Ok(_) => println!("  Unexpected success"),
            Err(ScopedDbError::InvalidInput(msg)) => println!("  Expected error: {}", msg),
            Err(e) => println!("  Unexpected error type: {:?}", e),
        }
        // Transaction not committed due to error
    }

    // 2. Transaction error propagation
    println!("\nTesting transaction error propagation:");
    {
        // Create a read transaction first
        let rtxn = env.read_txn()?;
        
        // Try to create a write transaction while read is active (might fail in some scenarios)
        match env.write_txn() {
            Ok(mut wtxn) => {
                // This should work in most cases
                scoped_db.put(&mut wtxn, None, "test_key", "test_value")?;
                wtxn.commit()?;
                println!("  Write transaction succeeded even with active read");
            }
            Err(e) => {
                println!("  Failed to create write transaction: {:?}", e);
            }
        }
        
        // Read transaction still valid
        let _ = scoped_db.get(&rtxn, None, "test_key")?;
    }

    // 3. Key not found vs actual errors
    println!("\nTesting key not found scenarios:");
    {
        let rtxn = env.read_txn()?;
        
        // Non-existent key returns None, not an error
        match scoped_db.get(&rtxn, None, "non_existent_key")? {
            Some(value) => println!("  Unexpected value: {}", value),
            None => println!("  Key not found (as expected)"),
        }
        
        // Non-existent scope also returns None for get
        match scoped_db.get(&rtxn, Some("non_existent_scope"), "any_key")? {
            Some(value) => println!("  Unexpected value: {}", value),
            None => println!("  Key in non-existent scope not found (as expected)"),
        }
    }

    // 4. Delete operations on non-existent items
    println!("\nTesting delete operations:");
    {
        let mut wtxn = env.write_txn()?;
        
        // Add a key to delete
        scoped_db.put(&mut wtxn, None, "delete_me", "value")?;
        
        // Delete existing key
        let deleted = scoped_db.delete(&mut wtxn, None, "delete_me")?;
        println!("  Deleted existing key: {}", deleted);
        
        // Try to delete non-existent key
        let deleted = scoped_db.delete(&mut wtxn, None, "never_existed")?;
        println!("  Deleted non-existent key: {}", deleted);
        
        // Try to delete from non-existent scope
        let deleted = scoped_db.delete(&mut wtxn, Some("ghost_scope"), "any_key")?;
        println!("  Deleted from non-existent scope: {}", deleted);
        
        wtxn.commit()?;
    }

    // 5. Clear scope operations
    println!("\nTesting clear scope operations:");
    {
        let mut wtxn = env.write_txn()?;
        
        // Add data to a scope
        scoped_db.put(&mut wtxn, Some("temp_scope"), "key1", "value1")?;
        scoped_db.put(&mut wtxn, Some("temp_scope"), "key2", "value2")?;
        
        // Clear the scope
        let cleared = scoped_db.clear_scope(&mut wtxn, Some("temp_scope"))?;
        println!("  Cleared {} items from temp_scope", cleared);
        
        // Clear empty scope
        let cleared = scoped_db.clear_scope(&mut wtxn, Some("empty_scope"))?;
        println!("  Cleared {} items from empty_scope", cleared);
        
        // Clear default scope
        scoped_db.put(&mut wtxn, None, "default_key", "default_value")?;
        let cleared = scoped_db.clear_scope(&mut wtxn, None)?;
        println!("  Cleared {} items from default scope", cleared);
        
        wtxn.commit()?;
    }

    // 6. Iterator error handling
    println!("\nTesting iterator error handling:");
    {
        let mut wtxn = env.write_txn()?;
        scoped_db.put(&mut wtxn, Some("iter_scope"), "key1", "value1")?;
        scoped_db.put(&mut wtxn, Some("iter_scope"), "key2", "value2")?;
        wtxn.commit()?;
    }
    
    {
        let rtxn = env.read_txn()?;
        
        // Normal iteration
        let mut count = 0;
        for result in scoped_db.iter(&rtxn, Some("iter_scope"))? {
            match result {
                Ok((key, value)) => {
                    println!("  Found: {} -> {}", key, value);
                    count += 1;
                }
                Err(e) => {
                    println!("  Error during iteration: {:?}", e);
                    // Decide whether to continue or break
                    break;
                }
            }
        }
        println!("  Successfully iterated {} items", count);
        
        // Iteration over non-existent scope (returns empty iterator, not error)
        let ghost_count = scoped_db.iter(&rtxn, Some("ghost_scope"))?.count();
        println!("  Items in ghost_scope: {}", ghost_count);
    }

    // 7. Custom error conversion
    println!("\nTesting custom error handling:");
    {
        fn process_user_data(db: &ScopedDatabase, env: &heed::Env) -> Result<String, String> {
            let rtxn = env.read_txn().map_err(|e| format!("Failed to create transaction: {}", e))?;
            
            match db.get(&rtxn, Some("users"), "admin") {
                Ok(Some(value)) => Ok(value.to_string()),
                Ok(None) => Err("Admin user not found".to_string()),
                Err(e) => Err(format!("Database error: {}", e)),
            }
        }
        
        match process_user_data(&scoped_db, &env) {
            Ok(admin) => println!("  Admin found: {}", admin),
            Err(e) => println!("  Error: {}", e),
        }
    }

    // 8. Recovery from errors
    println!("\nTesting error recovery:");
    {
        let mut wtxn = env.write_txn()?;
        
        // Simulate a series of operations where one might fail
        let operations = vec![
            (Some("data"), "key1", "value1"),
            (Some(""), "key2", "value2"), // This will fail
            (Some("data"), "key3", "value3"),
        ];
        
        let mut successful = 0;
        let mut failed = 0;
        
        for (scope, key, value) in operations {
            match scoped_db.put(&mut wtxn, scope, key, value) {
                Ok(_) => successful += 1,
                Err(e) => {
                    println!("  Operation failed: {:?}", e);
                    failed += 1;
                    // Continue with other operations
                }
            }
        }
        
        println!("  Operations: {} successful, {} failed", successful, failed);
        
        // Commit only if all critical operations succeeded
        if failed == 0 {
            wtxn.commit()?;
            println!("  All operations successful, committed");
        } else {
            // Transaction will be aborted when dropped
            println!("  Some operations failed, transaction aborted");
        }
    }

    println!("\nError handling example completed successfully!");
    
    // Clean up
    let _ = fs::remove_dir_all(db_path);
    
    Ok(())
}