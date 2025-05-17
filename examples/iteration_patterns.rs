use std::fs;
use std::path::Path;

use heed::EnvOpenOptions;
use scoped_heed::{ScopedDatabase, ScopedDbError};

fn main() -> Result<(), ScopedDbError> {
    // Create database directory
    let db_path = Path::new("./example_iteration_db");
    fs::create_dir_all(db_path)
        .map_err(|e| ScopedDbError::InvalidInput(format!("Failed to create DB dir: {}", e)))?;

    let env = unsafe {
        EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024) // 10MB
            .max_dbs(5)
            .open(db_path)?
    };

    let scoped_db = ScopedDatabase::new(&env)?;

    // Populate database with sample data
    {
        let mut wtxn = env.write_txn()?;
        
        // Products in default scope
        scoped_db.put(&mut wtxn, None, "product:001", "Laptop")?;
        scoped_db.put(&mut wtxn, None, "product:002", "Mouse")?;
        scoped_db.put(&mut wtxn, None, "product:003", "Keyboard")?;
        scoped_db.put(&mut wtxn, None, "user:admin", "admin@example.com")?;
        
        // Inventory in "warehouse_a" scope
        scoped_db.put(&mut wtxn, Some("warehouse_a"), "item:001", "50 units")?;
        scoped_db.put(&mut wtxn, Some("warehouse_a"), "item:002", "120 units")?;
        scoped_db.put(&mut wtxn, Some("warehouse_a"), "item:003", "75 units")?;
        scoped_db.put(&mut wtxn, Some("warehouse_a"), "status", "operational")?;
        
        // Inventory in "warehouse_b" scope
        scoped_db.put(&mut wtxn, Some("warehouse_b"), "item:001", "30 units")?;
        scoped_db.put(&mut wtxn, Some("warehouse_b"), "item:002", "0 units")?;
        scoped_db.put(&mut wtxn, Some("warehouse_b"), "status", "restocking")?;
        
        wtxn.commit()?;
    }

    // Iterate through all entries in a scope
    println!("All entries in default scope:");
    {
        let rtxn = env.read_txn()?;
        for result in scoped_db.iter(&rtxn, None)? {
            let (key, value) = result?;
            println!("  {} -> {}", key, value);
        }
    }

    // Filter entries while iterating
    println!("\nProducts only (filtered by key prefix):");
    {
        let rtxn = env.read_txn()?;
        for result in scoped_db.iter(&rtxn, None)? {
            let (key, value) = result?;
            if key.starts_with("product:") {
                println!("  {} -> {}", key, value);
            }
        }
    }

    // Collect into vector for sorting or further processing
    println!("\nWarehouse A inventory (sorted):");
    {
        let rtxn = env.read_txn()?;
        let mut inventory: Vec<(String, String)> = scoped_db
            .iter(&rtxn, Some("warehouse_a"))?
            .filter_map(|result| {
                result.ok().and_then(|(k, v)| {
                    if k.starts_with("item:") {
                        Some((k.to_string(), v.to_string()))
                    } else {
                        None
                    }
                })
            })
            .collect();
        
        inventory.sort_by(|a, b| a.0.cmp(&b.0));
        for (key, value) in inventory {
            println!("  {} -> {}", key, value);
        }
    }

    // Count entries in each scope
    println!("\nEntry counts by scope:");
    {
        let rtxn = env.read_txn()?;
        
        let default_count = scoped_db.iter(&rtxn, None)?.count();
        let warehouse_a_count = scoped_db.iter(&rtxn, Some("warehouse_a"))?.count();
        let warehouse_b_count = scoped_db.iter(&rtxn, Some("warehouse_b"))?.count();
        
        println!("  Default scope: {} entries", default_count);
        println!("  Warehouse A: {} entries", warehouse_a_count);
        println!("  Warehouse B: {} entries", warehouse_b_count);
    }

    // Find low inventory items across warehouses
    println!("\nLow/zero inventory items:");
    {
        let rtxn = env.read_txn()?;
        let warehouses = ["warehouse_a", "warehouse_b"];
        
        for warehouse in &warehouses {
            for result in scoped_db.iter(&rtxn, Some(warehouse))? {
                let (key, value) = result?;
                if key.starts_with("item:") && (value.contains("0 units") || value.contains("5 units")) {
                    println!("  {} in {}: {}", key, warehouse, value);
                }
            }
        }
    }

    // Complex iteration: aggregate data from multiple scopes
    println!("\nTotal inventory by product:");
    {
        let rtxn = env.read_txn()?;
        let mut totals = std::collections::HashMap::new();
        
        // Get product names from default scope
        for result in scoped_db.iter(&rtxn, None)? {
            let (key, name) = result?;
            if key.starts_with("product:") {
                let product_id = key.strip_prefix("product:").unwrap();
                totals.insert(product_id.to_string(), (name.to_string(), 0u32));
            }
        }
        
        // Sum inventory from all warehouses
        for warehouse in &["warehouse_a", "warehouse_b"] {
            for result in scoped_db.iter(&rtxn, Some(warehouse))? {
                let (key, value) = result?;
                if key.starts_with("item:") {
                    let product_id = key.strip_prefix("item:").unwrap();
                    if let Some((_, total)) = totals.get_mut(&product_id.to_string()) {
                        // Extract number from "X units" format
                        if let Some(count_str) = value.split_whitespace().next() {
                            if let Ok(count) = count_str.parse::<u32>() {
                                *total += count;
                            }
                        }
                    }
                }
            }
        }
        
        // Display totals
        for (id, (name, total)) in &totals {
            println!("  Product {} ({}): {} units total", id, name, total);
        }
    }

    println!("\nIteration patterns example completed successfully!");
    
    // Clean up
    let _ = fs::remove_dir_all(db_path);
    
    Ok(())
}