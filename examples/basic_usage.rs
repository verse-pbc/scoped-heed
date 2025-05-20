use heed::EnvOpenOptions;
use scoped_heed::{Scope, scoped_database_options, GlobalScopeRegistry};
use std::sync::Arc;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct User {
    id: u64,
    name: String,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a temporary directory for the database
    let db_path = "/tmp/scoped_heed_example";
    if std::path::Path::new(db_path).exists() {
        std::fs::remove_dir_all(db_path)?;
    }
    std::fs::create_dir_all(db_path)?;

    // Initialize the environment
    let env = unsafe {
        EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024) // 10MB
            .max_dbs(3)
            .open(db_path)?
    };

    // Create a global registry for scope metadata
    let mut wtxn = env.write_txn()?;
    let registry = Arc::new(GlobalScopeRegistry::new(&env, &mut wtxn)?);
    wtxn.commit()?;
    
    // Create a database with String keys and User values using builder pattern
    let mut wtxn = env.write_txn()?;
    let db = scoped_database_options(&env, registry.clone())
        .types::<String, User>()
        .name("users")
        .create(&mut wtxn)?;
    wtxn.commit()?;

    // Write some data
    {
        let mut wtxn = env.write_txn()?;

        // Default scope
        let admin = User {
            id: 1,
            name: "Admin".to_string(),
        };
        db.put(&mut wtxn, &Scope::Default, &"admin".to_string(), &admin)?;

        // Scoped data for tenant_a
        let alice = User {
            id: 100,
            name: "Alice".to_string(),
        };
        let tenant_a_scope = Scope::named("tenant_a")?;
        db.put(&mut wtxn, &tenant_a_scope, &"user1".to_string(), &alice)?;

        // Scoped data for tenant_b
        let bob = User {
            id: 200,
            name: "Bob".to_string(),
        };
        let tenant_b_scope = Scope::named("tenant_b")?;
        db.put(&mut wtxn, &tenant_b_scope, &"user1".to_string(), &bob)?;

        wtxn.commit()?;
    }

    // Read data back
    {
        let rtxn = env.read_txn()?;
        let tenant_a_scope = Scope::named("tenant_a")?;
        let tenant_b_scope = Scope::named("tenant_b")?;

        // Read from default scope
        let admin = db.get(&rtxn, &Scope::Default, &"admin".to_string())?;
        println!("Default scope - admin: {:?}", admin);

        // Read from tenant scopes
        let tenant_a_user = db.get(&rtxn, &tenant_a_scope, &"user1".to_string())?;
        let tenant_b_user = db.get(&rtxn, &tenant_b_scope, &"user1".to_string())?;

        println!("Tenant A - user1: {:?}", tenant_a_user);
        println!("Tenant B - user1: {:?}", tenant_b_user);
    }

    // Clean up
    drop(env);
    std::fs::remove_dir_all(db_path)?;

    Ok(())
}
