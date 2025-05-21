use heed::{EnvOpenOptions, RoTxn};
use scoped_heed::{ScopedDatabase, ScopedDbError, Scope, scoped_database_options, GlobalScopeRegistry};
use std::fs;
use std::sync::Arc;

// Type aliases for our database
type UserDb = ScopedDatabase<String, String>;

fn main() -> Result<(), ScopedDbError> {
    // Create a temporary database directory
    let db_path = "./scope_management_example";
    if fs::metadata(db_path).is_ok() {
        fs::remove_dir_all(db_path).unwrap();
    }
    fs::create_dir_all(db_path).unwrap();

    // Open the environment
    let env = unsafe {
        EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024)
            .max_dbs(3)
            .open(db_path)?
    };

    // Create a global registry for storing scope metadata
    let mut wtxn = env.write_txn()?;
    let registry = Arc::new(GlobalScopeRegistry::new(&env, &mut wtxn)?);
    wtxn.commit()?;
    
    // Create a database for storing user data
    let mut wtxn = env.write_txn()?;
    let db: UserDb = scoped_database_options(&env, registry.clone())
        .types::<String, String>()
        .name("users")
        .create(&mut wtxn)?;
    wtxn.commit()?;

    // Helper function to list all scopes
    fn list_scopes(txn: &RoTxn, db: &UserDb) -> Result<(), ScopedDbError> {
        println!("Current scopes:");
        for scope in db.list_scopes(txn)? {
            match scope {
                Scope::Default => println!("  - Default scope"),
                Scope::Named { name, hash } => println!("  - Name: {}, Hash: {:#x}", name, hash),
            }
        }
        Ok(())
    }

    // Phase 1: Add data to multiple scopes
    {
        println!("\n=== Phase 1: Adding data to multiple scopes ===");
        
        // Create scope objects for different tenants
        let tenant_a = Scope::named("tenant_a")?;
        let tenant_b = Scope::named("tenant_b")?;
        let tenant_c = Scope::named("tenant_c")?; // We'll keep this one empty
        
        let mut wtxn = env.write_txn()?;
        
        // Add data for tenant A
        db.put(&mut wtxn, &tenant_a, &"name".to_string(), &"Alice".to_string())?;
        db.put(&mut wtxn, &tenant_a, &"email".to_string(), &"alice@example.com".to_string())?;
        
        // Add data for tenant B
        db.put(&mut wtxn, &tenant_b, &"name".to_string(), &"Bob".to_string())?;
        db.put(&mut wtxn, &tenant_b, &"email".to_string(), &"bob@example.com".to_string())?;
        
        // Add a global user to the default scope
        db.put(&mut wtxn, &Scope::Default, &"admin".to_string(), &"admin@example.com".to_string())?;
        
        // Register the empty tenant C
        db.register_scope(&mut wtxn, &tenant_c)?;
        
        wtxn.commit()?;
        
        // List all registered scopes
        {
            let rtxn = env.read_txn()?;
            list_scopes(&rtxn, &db)?;
            // rtxn is dropped at the end of this block
        }
    }
    
    // Phase 2: Demonstrate scope isolation
    {
        println!("\n=== Phase 2: Demonstrating scope isolation ===");
        
        // Enclose all read operations in a block to ensure transaction is dropped
        {
            let rtxn = env.read_txn()?;
            
            // Retrieve data from tenant_a scope
            let tenant_a = Scope::named("tenant_a")?;
            let name = db.get(&rtxn, &tenant_a, &"name".to_string())?;
            println!("tenant_a name: {:?}", name);
            
            // Same key in tenant_b scope has different value
            let tenant_b = Scope::named("tenant_b")?;
            let name = db.get(&rtxn, &tenant_b, &"name".to_string())?;
            println!("tenant_b name: {:?}", name);
            
            // Iterate over all keys in tenant_a
            println!("All data in tenant_a:");
            for result in db.iter(&rtxn, &tenant_a)? {
                let (key, value) = result?;
                println!("  {} = {}", key, value);
            }
            // rtxn is dropped at the end of this block
        }
    }
    
    // Phase 3: Pruning empty scopes
    {
        println!("\n=== Phase 3: Pruning empty scopes ===");
        
        // List scopes before pruning
        {
            let rtxn = env.read_txn()?;
            println!("Before pruning:");
            list_scopes(&rtxn, &db)?;
            // rtxn is dropped at the end of this block
        }
        
        // Find empty scopes (should find tenant_c)
        let mut wtxn = env.write_txn()?;
        let empty_count = db.find_empty_scopes(&mut wtxn)?;
        println!("Found {} empty scopes", empty_count);
        
        // Use the global registry to prune empty scopes
        let databases: [&dyn scoped_heed::ScopeEmptinessChecker; 1] = [&db];
        let pruned_count = registry.prune_globally_unused_scopes(&mut wtxn, &databases)?;
        println!("Pruned {} empty scopes", pruned_count);
        
        wtxn.commit()?;
        
        // List scopes after pruning
        {
            let rtxn = env.read_txn()?;
            println!("After pruning:");
            list_scopes(&rtxn, &db)?;
            // rtxn is dropped at the end of this block
        }
    }
    
    // Phase 4: Clearing a scope and then pruning it
    {
        println!("\n=== Phase 4: Clearing and pruning a tenant ===");
        
        // Clear tenant_b
        let tenant_b = Scope::named("tenant_b")?;
        let mut wtxn = env.write_txn()?;
        db.clear(&mut wtxn, &tenant_b)?;
        println!("Cleared tenant_b");
        wtxn.commit()?;
        
        // Verify tenant_b is empty
        {
            let rtxn = env.read_txn()?;
            let count = db.iter(&rtxn, &tenant_b)?.count();
            println!("tenant_b has {} entries", count);
            // rtxn is dropped at the end of this block
        }
        
        // Find empty scopes again (should find tenant_b)
        let mut wtxn = env.write_txn()?;
        let empty_count = db.find_empty_scopes(&mut wtxn)?;
        println!("Found {} more empty scopes", empty_count);
        
        // Prune the empty scope using the global registry
        let databases: [&dyn scoped_heed::ScopeEmptinessChecker; 1] = [&db];
        let pruned_count = registry.prune_globally_unused_scopes(&mut wtxn, &databases)?;
        println!("Pruned {} more empty scopes", pruned_count);
        wtxn.commit()?;
        
        // List final scopes
        {
            let rtxn = env.read_txn()?;
            println!("Final scope list:");
            list_scopes(&rtxn, &db)?;
            // rtxn is dropped at the end of this block
        }
    }
    
    // Clean up
    drop(env);
    fs::remove_dir_all(db_path).unwrap();
    
    println!("\nExample completed successfully");
    Ok(())
}