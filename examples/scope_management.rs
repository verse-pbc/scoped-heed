use heed::{EnvOpenOptions, RoTxn};
use scoped_heed::{GlobalScopeRegistry, Scope, ScopedDatabase, scoped_database_options};
use std::fs;
use std::sync::Arc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db_path = "/tmp/scope_lifecycle_example";
    if fs::metadata(db_path).is_ok() {
        fs::remove_dir_all(db_path)?;
    }
    fs::create_dir_all(db_path)?;

    let env = unsafe {
        EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024)
            .max_dbs(5)
            .open(db_path)?
    };

    let mut wtxn = env.write_txn()?;
    let registry = Arc::new(GlobalScopeRegistry::new(&env, &mut wtxn)?);
    wtxn.commit()?;

    let mut wtxn = env.write_txn()?;
    let users_db = scoped_database_options(&env, registry.clone())
        .types::<String, String>()
        .name("users")
        .create(&mut wtxn)?;

    let products_db = scoped_database_options(&env, registry.clone())
        .types::<String, String>()
        .name("products")
        .create(&mut wtxn)?;

    wtxn.commit()?;

    // Helper function to list all registered scopes
    fn list_scopes<K, V>(
        txn: &RoTxn,
        db: &ScopedDatabase<K, V>,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        K: serde::Serialize + for<'de> serde::Deserialize<'de> + Clone + Default + 'static,
        V: serde::Serialize + for<'de> serde::Deserialize<'de> + 'static,
    {
        println!("Registered scopes:");
        for scope in db.list_scopes(txn)? {
            match scope {
                Scope::Default => println!("  - Default scope"),
                Scope::Named { name, hash } => println!("  - Name: {}, Hash: {:#x}", name, hash),
            }
        }
        Ok(())
    }

    // Phase 1: Register and populate scopes
    {
        println!("\n=== Phase 1: Registering and populating scopes ===");

        let tenant_a = Scope::named("tenant_a")?;
        let tenant_b = Scope::named("tenant_b")?;
        let tenant_c = Scope::named("tenant_c")?; // Will remain empty

        let mut wtxn = env.write_txn()?;

        users_db.put(
            &mut wtxn,
            &tenant_a,
            &"user1".to_string(),
            &"Alice".to_string(),
        )?;
        products_db.put(
            &mut wtxn,
            &tenant_a,
            &"prod1".to_string(),
            &"Laptop".to_string(),
        )?;

        users_db.put(
            &mut wtxn,
            &tenant_b,
            &"user1".to_string(),
            &"Bob".to_string(),
        )?;
        products_db.put(
            &mut wtxn,
            &tenant_b,
            &"prod1".to_string(),
            &"Phone".to_string(),
        )?;

        users_db.register_scope(&mut wtxn, &tenant_c)?;

        users_db.put(
            &mut wtxn,
            &Scope::Default,
            &"admin".to_string(),
            &"SuperAdmin".to_string(),
        )?;

        wtxn.commit()?;

        let rtxn = env.read_txn()?;
        list_scopes(&rtxn, &users_db)?;
    }

    // Phase 2: Find and prune empty scopes
    {
        println!("\n=== Phase 2: Finding and pruning empty scopes ===");

        let mut wtxn = env.write_txn()?;
        let empty_users = users_db.find_empty_scopes(&mut wtxn)?;
        let empty_products = products_db.find_empty_scopes(&mut wtxn)?;

        println!("Found {} empty user scopes", empty_users);
        println!("Found {} empty product scopes", empty_products);

        let databases: [&dyn scoped_heed::ScopeEmptinessChecker; 2] = [&users_db, &products_db];
        registry.prune_globally_unused_scopes(&mut wtxn, &databases)?;
        println!("Pruned empty scopes");

        wtxn.commit()?;

        let rtxn = env.read_txn()?;
        println!("\nAfter pruning:");
        list_scopes(&rtxn, &users_db)?;
    }

    // Phase 3: Clear a tenant and then prune it
    {
        println!("\n=== Phase 3: Clearing and pruning a tenant ===");

        let tenant_b = Scope::named("tenant_b")?;
        let mut wtxn = env.write_txn()?;

        users_db.clear(&mut wtxn, &tenant_b)?;
        products_db.clear(&mut wtxn, &tenant_b)?;

        println!("Cleared all user records and product records from tenant B");

        wtxn.commit()?;

        let mut wtxn = env.write_txn()?;
        let databases: [&dyn scoped_heed::ScopeEmptinessChecker; 2] = [&users_db, &products_db];
        registry.prune_globally_unused_scopes(&mut wtxn, &databases)?;

        println!("Pruned more empty scopes");
        wtxn.commit()?;

        let rtxn = env.read_txn()?;
        println!("\nFinal scope list:");
        list_scopes(&rtxn, &users_db)?;
    }

    drop(env);
    fs::remove_dir_all(db_path)?;

    println!("\n✅ Scope lifecycle management completed successfully");
    Ok(())
}
