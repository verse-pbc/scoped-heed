//! Test isolation example demonstrating how each test can use its own scope
//!
//! This shows how the Redis-like scope isolation makes it easy to run tests
//! in parallel without data conflicts, as each test operates in its own
//! isolated database scope.

use heed::EnvOpenOptions;
use scoped_heed::{Scope, ScopedDbError, scoped_database_options, GlobalScopeRegistry};
use std::sync::Arc;
use std::thread;

fn main() -> Result<(), ScopedDbError> {
    // Create a shared database environment
    let db_path = "./test_isolation";
    if std::path::Path::new(db_path).exists() {
        std::fs::remove_dir_all(db_path).unwrap();
    }
    std::fs::create_dir_all(db_path).unwrap();

    let env = Arc::new(unsafe {
        EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024)
            .max_dbs(3)
            .open(db_path)?
    });

    // Create a global registry
    let mut wtxn = env.write_txn()?;
    let registry = Arc::new(GlobalScopeRegistry::new(&env, &mut wtxn)?);
    wtxn.commit()?;

    // Create a shared database instance
    let mut wtxn = env.write_txn()?;
    let db = Arc::new(
        scoped_database_options(&env, registry.clone())
            .types::<String, i32>()
            .name("test_data")
            .create(&mut wtxn)?,
    );
    wtxn.commit()?;

    // Simulate multiple tests running in parallel
    let mut handles = vec![];

    for test_id in 1..=5 {
        let env = Arc::clone(&env);
        let db = Arc::clone(&db);

        let handle = thread::spawn(move || {
            // Each test uses its own scope - completely isolated
            let test_scope = Scope::named(&format!("test_{}", test_id)).unwrap();

            // Run test operations
            let mut wtxn = env.write_txn().unwrap();

            // Each test can use the same keys without conflicts
            db.put(&mut wtxn, &test_scope, &"counter".to_string(), &0)
                .unwrap();
            db.put(&mut wtxn, &test_scope, &"status".to_string(), &1)
                .unwrap();

            // Simulate test operations
            for i in 1..=10 {
                let counter_key = "counter".to_string();
                let current = db
                    .get(&wtxn, &test_scope, &counter_key)
                    .unwrap()
                    .unwrap_or(0);
                db.put(&mut wtxn, &test_scope, &counter_key, &(current + i))
                    .unwrap();
            }

            wtxn.commit().unwrap();

            // Verify test results
            let rtxn = env.read_txn().unwrap();
            let final_counter = db
                .get(&rtxn, &test_scope, &"counter".to_string())
                .unwrap()
                .unwrap();

            println!(
                "Test {} completed. Final counter: {}",
                test_id, final_counter
            );

            // Each test can clean up its own scope without affecting others
            let mut wtxn = env.write_txn().unwrap();
            db.clear(&mut wtxn, &test_scope).unwrap();
            wtxn.commit().unwrap();

            println!("Test {} cleaned up its scope", test_id);
        });

        handles.push(handle);
    }

    // Wait for all tests to complete
    for handle in handles {
        handle.join().unwrap();
    }

    println!("\n✅ All tests ran in complete isolation!");
    println!("Each test had its own scope - no data conflicts!");

    // Verify all test scopes are cleaned up
    {
        let rtxn = env.read_txn()?;
        for test_id in 1..=5 {
            let test_scope = Scope::named(&format!("test_{}", test_id))?;
            let count: i32 = db
                .iter(&rtxn, &test_scope)?
                .count()
                .try_into()
                .unwrap();
            println!("Scope {} has {} entries", test_id, count);
        }
    }

    // Clean up
    drop(db);
    drop(env);
    std::fs::remove_dir_all(db_path).unwrap();

    Ok(())
}