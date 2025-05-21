use heed::EnvOpenOptions;
use scoped_heed::{GlobalScopeRegistry, Scope, scoped_database_options};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct User {
    id: u64,
    name: String,
    email: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Product {
    id: u64,
    name: String,
    price: f64,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db_path = "/tmp/multi_tenant_example";
    if std::path::Path::new(db_path).exists() {
        std::fs::remove_dir_all(db_path)?;
    }
    std::fs::create_dir_all(db_path)?;

    let env = unsafe {
        EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024)
            .max_dbs(6)
            .open(db_path)?
    };

    let mut wtxn = env.write_txn()?;
    let registry = Arc::new(GlobalScopeRegistry::new(&env, &mut wtxn)?);
    wtxn.commit()?;

    let mut wtxn = env.write_txn()?;
    let users_db = scoped_database_options(&env, registry.clone())
        .types::<u64, User>()
        .name("users")
        .create(&mut wtxn)?;

    let products_db = scoped_database_options(&env, registry.clone())
        .types::<u64, Product>()
        .name("products")
        .create(&mut wtxn)?;

    wtxn.commit()?;

    let tenant_a = Scope::named("tenant_a")?;
    let tenant_b = Scope::named("tenant_b")?;

    // Populate data for both tenants
    {
        let mut wtxn = env.write_txn()?;

        users_db.put(
            &mut wtxn,
            &tenant_a,
            &1,
            &User {
                id: 1,
                name: "Alice".to_string(),
                email: "alice@tenanta.com".to_string(),
            },
        )?;

        products_db.put(
            &mut wtxn,
            &tenant_a,
            &101,
            &Product {
                id: 101,
                name: "Product A".to_string(),
                price: 99.99,
            },
        )?;

        // Same key IDs for tenant B
        users_db.put(
            &mut wtxn,
            &tenant_b,
            &1,
            &User {
                id: 1,
                name: "Bob".to_string(),
                email: "bob@tenantb.com".to_string(),
            },
        )?;

        products_db.put(
            &mut wtxn,
            &tenant_b,
            &101,
            &Product {
                id: 101,
                name: "Product B".to_string(),
                price: 149.99,
            },
        )?;

        users_db.put(
            &mut wtxn,
            &Scope::Default,
            &999,
            &User {
                id: 999,
                name: "Admin".to_string(),
                email: "admin@example.com".to_string(),
            },
        )?;

        wtxn.commit()?;
    }

    // Demonstrate data isolation
    {
        let rtxn = env.read_txn()?;

        println!("=== Tenant Isolation Demo ===\n");

        let user_a = users_db.get(&rtxn, &tenant_a, &1)?.unwrap();
        let user_b = users_db.get(&rtxn, &tenant_b, &1)?.unwrap();

        println!("User 1 in tenant A: {}", user_a.name);
        println!("User 1 in tenant B: {}", user_b.name);

        let product_a = products_db.get(&rtxn, &tenant_a, &101)?.unwrap();
        let product_b = products_db.get(&rtxn, &tenant_b, &101)?.unwrap();

        println!(
            "\nProduct 101 in tenant A: {} (${:.2})",
            product_a.name, product_a.price
        );
        println!(
            "Product 101 in tenant B: {} (${:.2})",
            product_b.name, product_b.price
        );

        println!("\nAll users in tenant A:");
        for result in users_db.iter(&rtxn, &tenant_a)? {
            let (id, user) = result?;
            println!("  ID {}: {}", id, user.name);
        }
    }

    // Demonstrate scope operations
    {
        let mut wtxn = env.write_txn()?;

        println!("\nClearing tenant A data...");
        users_db.clear(&mut wtxn, &tenant_a)?;
        products_db.clear(&mut wtxn, &tenant_a)?;

        println!("Cleared all data for tenant A");
        wtxn.commit()?;

        let rtxn = env.read_txn()?;
        let tenant_a_user = users_db.get(&rtxn, &tenant_a, &1)?;
        let tenant_b_user = users_db.get(&rtxn, &tenant_b, &1)?;

        println!("\nAfter clearing tenant A:");
        println!("  User 1 in tenant A: {:?}", tenant_a_user);
        println!("  User 1 in tenant B: {:?}", tenant_b_user.map(|u| u.name));

        println!("\n✅ Complete isolation between tenants achieved!");
    }

    drop(env);
    std::fs::remove_dir_all(db_path)?;

    Ok(())
}
