//! Multi-tenant example demonstrating Redis-like database isolation
//!
//! This example shows how scopes provide complete isolation between tenants,
//! similar to how Redis databases work. Each tenant has its own isolated
//! namespace with no possibility of cross-tenant data access.

use heed::EnvOpenOptions;
use scoped_heed::{ScopedDbError, scoped_database_options};
use serde::{Deserialize, Serialize};

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

fn main() -> Result<(), ScopedDbError> {
    // Create the database environment
    let db_path = "./test_multi_tenant";
    if std::path::Path::new(db_path).exists() {
        std::fs::remove_dir_all(db_path).unwrap();
    }
    std::fs::create_dir_all(db_path).unwrap();

    let env = unsafe {
        EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024)
            .max_dbs(6) // Need multiple databases for different types
            .open(db_path)?
    };

    // Create databases for different data types
    let mut wtxn = env.write_txn()?;

    let users_db = scoped_database_options(&env)
        .types::<u64, User>()
        .name("users")
        .create(&mut wtxn)?;

    let products_db = scoped_database_options(&env)
        .types::<u64, Product>()
        .name("products")
        .create(&mut wtxn)?;

    wtxn.commit()?;

    // Simulate two different tenants using the same database
    let tenant1 = "acme_corp";
    let tenant2 = "tech_startup";

    // Populate data for tenant 1
    {
        let mut wtxn = env.write_txn()?;

        // Add users for tenant 1
        users_db.put(
            &mut wtxn,
            Some(tenant1),
            &1,
            &User {
                id: 1,
                name: "Alice Johnson".to_string(),
                email: "alice@acme.com".to_string(),
            },
        )?;

        users_db.put(
            &mut wtxn,
            Some(tenant1),
            &2,
            &User {
                id: 2,
                name: "Bob Smith".to_string(),
                email: "bob@acme.com".to_string(),
            },
        )?;

        // Add products for tenant 1
        products_db.put(
            &mut wtxn,
            Some(tenant1),
            &101,
            &Product {
                id: 101,
                name: "Enterprise Widget".to_string(),
                price: 999.99,
            },
        )?;

        wtxn.commit()?;
    }

    // Populate data for tenant 2 - using THE SAME keys but different scope
    {
        let mut wtxn = env.write_txn()?;

        // Add users for tenant 2 - same user IDs, completely different data
        users_db.put(
            &mut wtxn,
            Some(tenant2),
            &1,
            &User {
                id: 1,
                name: "Charlie Davis".to_string(),
                email: "charlie@techstartup.io".to_string(),
            },
        )?;

        users_db.put(
            &mut wtxn,
            Some(tenant2),
            &2,
            &User {
                id: 2,
                name: "Diana Martinez".to_string(),
                email: "diana@techstartup.io".to_string(),
            },
        )?;

        // Add products for tenant 2 - same product ID, different data
        products_db.put(
            &mut wtxn,
            Some(tenant2),
            &101,
            &Product {
                id: 101,
                name: "Startup Gadget".to_string(),
                price: 49.99,
            },
        )?;

        wtxn.commit()?;
    }

    // Demonstrate complete isolation between tenants
    {
        let rtxn = env.read_txn()?;

        println!("=== Redis-like Scope Isolation Demo ===\n");

        // Query tenant 1 data
        println!("Tenant 1 ({}) Data:", tenant1);
        let user = users_db.get(&rtxn, Some(tenant1), &1)?.unwrap();
        println!("  User 1: {:?}", user);
        let product = products_db.get(&rtxn, Some(tenant1), &101)?.unwrap();
        println!("  Product 101: {:?}", product);

        println!();

        // Query tenant 2 data - same keys, completely different data
        println!("Tenant 2 ({}) Data:", tenant2);
        let user = users_db.get(&rtxn, Some(tenant2), &1)?.unwrap();
        println!("  User 1: {:?}", user);
        let product = products_db.get(&rtxn, Some(tenant2), &101)?.unwrap();
        println!("  Product 101: {:?}", product);

        println!();

        // Demonstrate iteration is scope-isolated
        println!("All users in tenant 1:");
        for result in users_db.iter(&rtxn, Some(tenant1))? {
            let (id, user) = result?;
            println!("  ID {}: {}", id, user.name);
        }

        println!("\nAll users in tenant 2:");
        for result in users_db.iter(&rtxn, Some(tenant2))? {
            let (id, user) = result?;
            println!("  ID {}: {}", id, user.name);
        }

        println!();

        // Demonstrate that clearing one scope doesn't affect others
        println!("Clearing all data for tenant 1...");
    }

    {
        let mut wtxn = env.write_txn()?;
        users_db.clear(&mut wtxn, Some(tenant1))?;
        products_db.clear(&mut wtxn, Some(tenant1))?;
        wtxn.commit()?;
    }

    {
        let rtxn = env.read_txn()?;

        println!("After clearing tenant 1:");
        println!(
            "  Tenant 1 users: {:?}",
            users_db.get(&rtxn, Some(tenant1), &1)?
        );
        println!(
            "  Tenant 2 users: {:?}",
            users_db.get(&rtxn, Some(tenant2), &1)?
        );

        println!("\n✅ Tenant 2 data remains intact - scopes are completely isolated!");
    }

    // Clean up
    drop(env);
    std::fs::remove_dir_all(db_path).unwrap();

    Ok(())
}
