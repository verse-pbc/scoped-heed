use std::fs;
use std::path::Path;

use heed::types::Str;
use heed::{Database as HeedDatabase, EnvOpenOptions};
use scoped_heed::{ScopedDatabase, ScopedDbError};

/// This example demonstrates how ScopedDatabase can work with existing heed databases
/// that were created before introducing scoping functionality.
fn main() -> Result<(), ScopedDbError> {
    // Create paths for legacy and migration
    let legacy_path = Path::new("./example_legacy_db");
    let migration_path = Path::new("./example_migrated_db");
    
    fs::create_dir_all(legacy_path)
        .map_err(|e| ScopedDbError::InvalidInput(format!("Failed to create legacy DB dir: {}", e)))?;
    fs::create_dir_all(migration_path)
        .map_err(|e| ScopedDbError::InvalidInput(format!("Failed to create migration DB dir: {}", e)))?;

    // Step 1: Create a legacy database using raw heed
    println!("Step 1: Creating legacy database with raw heed...");
    {
        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(10 * 1024 * 1024)
                .max_dbs(2)
                .open(legacy_path)?
        };

        // Create a database with the same name ScopedDatabase uses for default scope
        const DEFAULT_DB_NAME: &str = "my_default_db";
        
        let legacy_db: HeedDatabase<Str, Str> = {
            let mut wtxn = env.write_txn()?;
            let db = env.create_database::<Str, Str>(&mut wtxn, Some(DEFAULT_DB_NAME))?;
            
            // Populate with legacy data
            db.put(&mut wtxn, "user:001", "john.doe@example.com")?;
            db.put(&mut wtxn, "user:002", "jane.smith@example.com")?;
            db.put(&mut wtxn, "config:version", "1.0.0")?;
            db.put(&mut wtxn, "config:last_update", "2024-01-15")?;
            
            wtxn.commit()?;
            db
        };
        
        // Verify legacy data
        println!("Legacy data created:");
        {
            let rtxn = env.read_txn()?;
            for result in legacy_db.iter(&rtxn)? {
                let (key, value) = result?;
                println!("  {} -> {}", key, value);
            }
        }
    }

    // Step 2: Open the legacy database with ScopedDatabase
    println!("\nStep 2: Opening legacy database with ScopedDatabase...");
    {
        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(10 * 1024 * 1024)
                .max_dbs(3) // Need at least 3 for ScopedDatabase
                .open(legacy_path)?
        };

        let scoped_db = ScopedDatabase::new(&env)?;
        
        // Read legacy data through ScopedDatabase (using None scope)
        println!("Reading legacy data through ScopedDatabase:");
        {
            let rtxn = env.read_txn()?;
            for result in scoped_db.iter(&rtxn, None)? {
                let (key, value) = result?;
                println!("  {} -> {}", key, value);
            }
        }
        
        // Add new data while maintaining legacy compatibility
        println!("\nAdding new data through ScopedDatabase:");
        {
            let mut wtxn = env.write_txn()?;
            
            // Add to default scope (compatible with legacy)
            scoped_db.put(&mut wtxn, None, "user:003", "new.user@example.com")?;
            
            // Add to a new scope (won't interfere with legacy data)
            scoped_db.put(&mut wtxn, Some("tenant_1"), "user:001", "tenant1.user@example.com")?;
            scoped_db.put(&mut wtxn, Some("tenant_1"), "config:theme", "dark")?;
            
            wtxn.commit()?;
        }
        
        // Verify that legacy and new data coexist
        println!("\nLegacy data (default scope):");
        {
            let rtxn = env.read_txn()?;
            for result in scoped_db.iter(&rtxn, None)? {
                let (key, value) = result?;
                println!("  {} -> {}", key, value);
            }
        }
        
        println!("\nNew scoped data (tenant_1):");
        {
            let rtxn = env.read_txn()?;
            for result in scoped_db.iter(&rtxn, Some("tenant_1"))? {
                let (key, value) = result?;
                println!("  {} -> {}", key, value);
            }
        }
    }

    // Step 3: Demonstrate migration strategy
    println!("\nStep 3: Demonstrating migration strategy...");
    {
        // Open source database
        let source_env = unsafe {
            EnvOpenOptions::new()
                .map_size(10 * 1024 * 1024)
                .max_dbs(3)
                .open(legacy_path)?
        };
        let source_db = ScopedDatabase::new(&source_env)?;
        
        // Create destination database
        let dest_env = unsafe {
            EnvOpenOptions::new()
                .map_size(10 * 1024 * 1024)
                .max_dbs(5)
                .open(migration_path)?
        };
        let dest_db = ScopedDatabase::new(&dest_env)?;
        
        // Migrate data with scope organization
        {
            let source_rtxn = source_env.read_txn()?;
            let mut dest_wtxn = dest_env.write_txn()?;
            
            // Copy all legacy default scope data
            println!("Migrating legacy data to organized scopes:");
            for result in source_db.iter(&source_rtxn, None)? {
                let (key, value) = result?;
                
                // Organize by key prefix during migration
                if key.starts_with("user:") {
                    dest_db.put(&mut dest_wtxn, Some("users"), key, value)?;
                    println!("  Migrated {} to 'users' scope", key);
                } else if key.starts_with("config:") {
                    dest_db.put(&mut dest_wtxn, Some("config"), key, value)?;
                    println!("  Migrated {} to 'config' scope", key);
                } else {
                    // Keep unknown keys in default scope
                    dest_db.put(&mut dest_wtxn, None, key, value)?;
                    println!("  Kept {} in default scope", key);
                }
            }
            
            // Copy tenant data as-is
            for result in source_db.iter(&source_rtxn, Some("tenant_1"))? {
                let (key, value) = result?;
                dest_db.put(&mut dest_wtxn, Some("tenant_1"), key, value)?;
            }
            
            dest_wtxn.commit()?;
        }
        
        // Verify migrated data
        println!("\nMigrated data organization:");
        {
            let rtxn = dest_env.read_txn()?;
            
            println!("Users scope:");
            for result in dest_db.iter(&rtxn, Some("users"))? {
                let (key, value) = result?;
                println!("  {} -> {}", key, value);
            }
            
            println!("\nConfig scope:");
            for result in dest_db.iter(&rtxn, Some("config"))? {
                let (key, value) = result?;
                println!("  {} -> {}", key, value);
            }
            
            println!("\nTenant 1 scope:");
            for result in dest_db.iter(&rtxn, Some("tenant_1"))? {
                let (key, value) = result?;
                println!("  {} -> {}", key, value);
            }
        }
    }

    println!("\nLegacy compatibility example completed successfully!");
    
    // Clean up
    let _ = fs::remove_dir_all(legacy_path);
    let _ = fs::remove_dir_all(migration_path);
    
    Ok(())
}