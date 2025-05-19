use heed::EnvOpenOptions;
use scoped_heed::scoped_database_options;
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

    // Create a database with String keys and User values using builder pattern
    let mut wtxn = env.write_txn()?;
    let db = scoped_database_options(&env)
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
        db.put(&mut wtxn, None, &"admin".to_string(), &admin)?;

        // Scoped data for tenant_a
        let alice = User {
            id: 100,
            name: "Alice".to_string(),
        };
        db.put(&mut wtxn, Some("tenant_a"), &"user1".to_string(), &alice)?;

        // Scoped data for tenant_b
        let bob = User {
            id: 200,
            name: "Bob".to_string(),
        };
        db.put(&mut wtxn, Some("tenant_b"), &"user1".to_string(), &bob)?;

        wtxn.commit()?;
    }

    // Read data back
    {
        let rtxn = env.read_txn()?;

        // Read from default scope
        let admin = db.get(&rtxn, None, &"admin".to_string())?;
        println!("Default scope - admin: {:?}", admin);

        // Read from tenant scopes
        let tenant_a_user = db.get(&rtxn, Some("tenant_a"), &"user1".to_string())?;
        let tenant_b_user = db.get(&rtxn, Some("tenant_b"), &"user1".to_string())?;

        println!("Tenant A - user1: {:?}", tenant_a_user);
        println!("Tenant B - user1: {:?}", tenant_b_user);
    }

    // Clean up
    drop(env);
    std::fs::remove_dir_all(db_path)?;

    Ok(())
}
