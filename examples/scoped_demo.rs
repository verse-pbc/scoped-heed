use std::fs;
use std::path::Path;

use heed::types::{Bytes, Str};
use heed::{Database as HeedDatabase, EnvOpenOptions};
use scoped_heed::{ScopedDatabase, ScopedDbError}; // Adjusted to use the library crate name

fn main() -> Result<(), ScopedDbError> {
    #![allow(clippy::needless_option_as_deref)]
    // For an example, it might be better to put this in target/ or a specific example_dbs/ directory.
    let db_path = Path::new("./example_scoped_db");
    fs::create_dir_all(db_path)
        .map_err(|e| ScopedDbError::InvalidInput(format!("Failed to create DB dir: {}", e)))?;

    // Open the environment
    let env = unsafe {
        EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024) // 10MB
            .max_dbs(15) // Max DBs used by the example
            .open(db_path)?
    };

    let user_db = ScopedDatabase::new(&env)?;
    println!("ScopedDatabase initialized for example.");

    // Add some users to the "main" scope
    println!("Putting data into \'main\' scope...");
    {
        let mut wtxn = env.write_txn()?;
        user_db.put(&mut wtxn, Some("main"), "user1", "Alice (main)")?;
        user_db.put(&mut wtxn, Some("main"), "user2", "Bob (main)")?;
        wtxn.commit()?;
    }

    // Add some users to the default (None) scope
    println!("Putting data into default (None) scope...");
    {
        let mut wtxn = env.write_txn()?;
        user_db.put(&mut wtxn, None, "user1", "Alice (default)")?;
        user_db.put(&mut wtxn, None, "user4", "Zane (default)")?;
        wtxn.commit()?;
    }

    // Switch to a different scope - like a different subdomain
    println!("Putting data into \'customer\' scope...");
    {
        let mut wtxn = env.write_txn()?;
        user_db.put(&mut wtxn, Some("customer"), "user1", "David (customer)")?;
        user_db.put(&mut wtxn, Some("customer"), "user2", "Eve (customer)")?;
        user_db.put(&mut wtxn, Some("customer"), "user3", "Carol (customer)")?;
        wtxn.commit()?;
    }

    // Switch to yet another scope
    println!("Putting data into \'admin\' scope...");
    {
        let mut wtxn = env.write_txn()?;
        user_db.put(&mut wtxn, Some("admin"), "user1", "Frank (admin)")?;
        user_db.put(&mut wtxn, Some("admin"), "admin1", "Supervisor (admin)")?;
        wtxn.commit()?;
    }
    println!("All initial data write transactions committed.");

    // Start a read transaction to verify data
    println!("Starting read transactions to verify data...");

    {
        let rtxn = env.read_txn()?;
        println!("Verifying \'main\' scope:");
        assert_eq!(
            user_db.get(&rtxn, Some("main"), "user1")?.as_deref(),
            Some("Alice (main)")
        );
        assert_eq!(
            user_db.get(&rtxn, Some("main"), "user2")?.as_deref(),
            Some("Bob (main)")
        );
        assert_eq!(user_db.get(&rtxn, Some("main"), "user3")?, None);

        let mut main_users: Vec<(String, String)> = user_db
            .iter(&rtxn, Some("main"))?
            .map(|res| res.map(|(k, v)| (k.to_string(), v.to_string())))
            .collect::<Result<_, _>>()?;
        main_users.sort();
        println!("  Users in \'main\': {:?}", main_users);
        assert_eq!(
            main_users,
            vec![
                ("user1".to_string(), "Alice (main)".to_string()),
                ("user2".to_string(), "Bob (main)".to_string()),
            ]
        );

        println!("Verifying default (None) scope:");
        assert_eq!(
            user_db.get(&rtxn, None, "user1")?.as_deref(),
            Some("Alice (default)")
        );
        assert_eq!(
            user_db.get(&rtxn, None, "user4")?.as_deref(),
            Some("Zane (default)")
        );
        let mut default_users: Vec<(String, String)> = user_db
            .iter(&rtxn, None)?
            .map(|res| res.map(|(k, v)| (k.to_string(), v.to_string())))
            .collect::<Result<_, _>>()?;
        default_users.sort();
        println!("  Users in default (None): {:?}", default_users);
        assert_eq!(
            default_users,
            vec![
                ("user1".to_string(), "Alice (default)".to_string()),
                ("user4".to_string(), "Zane (default)".to_string()),
            ]
        );

        println!("Verifying \'customer\' scope:");
        assert_eq!(
            user_db.get(&rtxn, Some("customer"), "user1")?.as_deref(),
            Some("David (customer)")
        );
        let mut customer_users: Vec<(String, String)> = user_db
            .iter(&rtxn, Some("customer"))?
            .map(|res| res.map(|(k, v)| (k.to_string(), v.to_string())))
            .collect::<Result<_, _>>()?;
        customer_users.sort();
        println!("  Users in \'customer\': {:?}", customer_users);
        assert_eq!(
            customer_users,
            vec![
                ("user1".to_string(), "David (customer)".to_string()),
                ("user2".to_string(), "Eve (customer)".to_string()),
                ("user3".to_string(), "Carol (customer)".to_string()),
            ]
        );

        println!("Verifying \'admin\' scope:");
        let mut admin_users: Vec<(String, String)> = user_db
            .iter(&rtxn, Some("admin"))?
            .map(|res| res.map(|(k, v)| (k.to_string(), v.to_string())))
            .collect::<Result<_, _>>()?;
        admin_users.sort();
        println!("  Users in \'admin\': {:?}", admin_users);
        assert_eq!(
            admin_users,
            vec![
                ("admin1".to_string(), "Supervisor (admin)".to_string()),
                ("user1".to_string(), "Frank (admin)".to_string()),
            ]
        );
    }
    println!("All verification read transactions finished.");

    // Demonstrate delete and clear operations
    println!("Demonstrating delete and clear operations...");
    {
        let mut wtxn = env.write_txn()?;
        println!("Deleting \'user1\' from \'customer\' scope...");
        assert!(user_db.delete(&mut wtxn, Some("customer"), "user1")?);
        println!("Deleting \'non_existent_user\' from \'customer\' scope (should be false)...");
        assert!(!user_db.delete(&mut wtxn, Some("customer"), "non_existent_user")?);

        println!("Deleting \'user4\' from default (None) scope...");
        assert!(user_db.delete(&mut wtxn, None, "user4")?);

        println!("Clearing \'admin\' scope...");
        assert_eq!(user_db.clear_scope(&mut wtxn, Some("admin"))?, 2);

        println!("Clearing \'non_existent_scope\' (should remove 0 items)...");
        assert_eq!(
            user_db.clear_scope(&mut wtxn, Some("non_existent_scope"))?,
            0
        );
        wtxn.commit()?;
    }
    println!("All delete/clear transactions committed.");

    // Verify deletions and clears
    println!("Verifying deletions and clears...");
    {
        let rtxn = env.read_txn()?;
        println!("Verifying \'main\' scope (should be unaffected):");
        assert_eq!(
            user_db.get(&rtxn, Some("main"), "user1")?.as_deref(),
            Some("Alice (main)")
        );

        println!("Verifying \'customer\' scope (after deletions):");
        assert_eq!(user_db.get(&rtxn, Some("customer"), "user1")?, None);
        assert_eq!(
            user_db.get(&rtxn, Some("customer"), "user2")?.as_deref(),
            Some("Eve (customer)")
        );

        println!("Verifying default (None) scope (after deletions):");
        assert_eq!(
            user_db.get(&rtxn, None, "user1")?.as_deref(),
            Some("Alice (default)")
        );
        assert_eq!(user_db.get(&rtxn, None, "user4")?, None);

        println!("Verifying \'admin\' scope (should be empty):");
        let admin_iter_final: Vec<(String, String)> = user_db
            .iter(&rtxn, Some("admin"))?
            .map(|res| res.map(|(k, v)| (k.to_string(), v.to_string())))
            .collect::<Result<_, _>>()?;
        assert_eq!(admin_iter_final.len(), 0);
        assert_eq!(user_db.get(&rtxn, Some("admin"), "user1")?, None);
        assert_eq!(user_db.get(&rtxn, Some("admin"), "admin1")?, None);
    }
    println!("Verification of deletions/clears finished.");

    // --- Additional Test Cases ---
    println!("\n--- Running Additional Test Cases ---");
    {
        let mut wtxn = env.write_txn()?;
        user_db.put(&mut wtxn, Some("main"), "user1", "Alice (main)")?;
        wtxn.commit()?;
    }

    {
        let mut wtxn = env.write_txn()?;
        user_db.put(&mut wtxn, Some("main"), "user1", "Alice Updated (main)")?;
        let deleted_non_existent =
            user_db.delete(&mut wtxn, Some("main"), "user_does_not_exist_in_main")?;
        println!(
            "Attempted to delete \'user_does_not_exist_in_main\' from \'main\' scope. Deleted: {}",
            deleted_non_existent
        );
        assert!(
            !deleted_non_existent,
            "Deleting a non-existent key should return false"
        );
        wtxn.commit()?;
    }

    {
        let mut wtxn = env.write_txn()?;
        println!("Attempting to clear a new, empty scope \'new_empty_scope\'");
        let cleared_count_empty = user_db.clear_scope(&mut wtxn, Some("new_empty_scope"))?;
        println!(
            "Cleared {} items from \'new_empty_scope\'",
            cleared_count_empty
        );
        assert_eq!(
            cleared_count_empty, 0,
            "Clearing an empty scope should remove 0 items"
        );
        wtxn.commit()?;
    }

    {
        let rtxn = env.read_txn()?;
        assert_eq!(
            user_db.get(&rtxn, Some("main"), "user1")?.as_deref(),
            Some("Alice Updated (main)"),
            "Value of user1 should be updated"
        );
        let get_non_existent = user_db.get(&rtxn, Some("main"), "really_does_not_exist")?;
        println!(
            "Attempting to get \'really_does_not_exist\' from \'main\' scope: {:?}",
            get_non_existent
        );
        assert!(
            get_non_existent.is_none(),
            "Getting a non-existent key should return None"
        );

        let mut items_in_new_empty_scope = 0;
        for result in user_db.iter(&rtxn, Some("new_empty_scope"))? {
            result?;
            items_in_new_empty_scope += 1;
        }
        println!(
            "Number of items in \'new_empty_scope\' after clear: {}",
            items_in_new_empty_scope
        );
        assert_eq!(
            items_in_new_empty_scope, 0,
            "new_empty_scope should be empty after clearing"
        );
    }
    println!("Additional test cases finished.");

    // --- Raw Heed API Demonstrations (Optional for library example, but good to keep parts) ---
    println!("\n--- Running Raw Heed API Demonstrations (Simplified for Example) ---");

    const RAW_BYTES_DB_NAME: &str = "example_raw_bytes_db";

    let raw_bytes_db: HeedDatabase<Bytes, Bytes> = {
        let mut wtxn_setup = env.write_txn()?;
        let db = env.create_database::<Bytes, Bytes>(&mut wtxn_setup, Some(RAW_BYTES_DB_NAME))?;
        wtxn_setup.commit()?;
        db
    };

    println!(
        "\nTesting raw Database<Bytes, Bytes> ('{}')",
        RAW_BYTES_DB_NAME
    );
    let mut wtxn_bytes = env.write_txn()?;
    raw_bytes_db.put(&mut wtxn_bytes, b"key1_bytes", b"value1_bytes")?;
    wtxn_bytes.commit()?;

    {
        let rtxn_bytes = env.read_txn()?;
        let val1 = raw_bytes_db.get(&rtxn_bytes, b"key1_bytes")?;
        println!(
            "  Get 'key1_bytes': {:?}",
            val1.map(|v| std::str::from_utf8(v).unwrap_or("invalid utf8"))
        );
        assert_eq!(val1, Some(&b"value1_bytes"[..]));
        rtxn_bytes.commit()?;
    }
    println!("Raw Heed API demonstration finished.");

    // --- Legacy DB Compatibility Test (Simplified for Example) ---
    println!("\n--- Running Legacy DB Compatibility Test (Simplified for Example) ---");
    const LEGACY_DEFAULT_DB_NAME: &str = "my_default_db"; // Name ScopedDatabase uses for None scope

    {
        let legacy_db_path = Path::new("./example_legacy_compat_db");
        if legacy_db_path.exists() {
            fs::remove_dir_all(legacy_db_path).map_err(|e| {
                ScopedDbError::InvalidInput(format!("Failed to clean up old legacy DB dir: {}", e))
            })?;
        }
        fs::create_dir_all(legacy_db_path).map_err(|e| {
            ScopedDbError::InvalidInput(format!("Failed to create legacy DB dir: {}", e))
        })?;
        let legacy_env = unsafe {
            EnvOpenOptions::new()
                .map_size(10 * 1024 * 1024) // 10MB
                .max_dbs(5)
                .open(legacy_db_path)?
        };

        println!("Simulating legacy database in '{}'", LEGACY_DEFAULT_DB_NAME);
        let _legacy_raw_default_db: HeedDatabase<Str, Str> = {
            let mut wtxn_legacy_setup = legacy_env.write_txn()?;
            let db = legacy_env.create_database::<Str, Str>(
                &mut wtxn_legacy_setup,
                Some(LEGACY_DEFAULT_DB_NAME),
            )?;
            db.put(
                &mut wtxn_legacy_setup,
                "legacy_key1",
                "legacy_value1_original",
            )?;
            wtxn_legacy_setup.commit()?;
            db
        };
        println!("Legacy data committed to '{}'.", LEGACY_DEFAULT_DB_NAME);

        let scoped_db_on_legacy = ScopedDatabase::new(&legacy_env)?;
        println!("Initialized ScopedDatabase for legacy test.");

        {
            let rtxn = legacy_env.read_txn()?;
            println!("Reading legacy data via ScopedDatabase (default/None scope):");
            assert_eq!(
                scoped_db_on_legacy
                    .get(&rtxn, None, "legacy_key1")?
                    .as_deref(),
                Some("legacy_value1_original")
            );
        }

        {
            println!("Writing new data via ScopedDatabase (default/None scope):");
            let mut wtxn = legacy_env.write_txn()?;
            scoped_db_on_legacy.put(
                &mut wtxn,
                None,
                "legacy_key1",
                "legacy_value1_overwritten_by_scoped",
            )?;
            wtxn.commit()?;
        }

        {
            let rtxn = legacy_env.read_txn()?;
            assert_eq!(
                scoped_db_on_legacy
                    .get(&rtxn, None, "legacy_key1")?
                    .as_deref(),
                Some("legacy_value1_overwritten_by_scoped")
            );
        }
        println!("Legacy DB compatibility test finished.");
    }
    println!("\nExample finished successfully!");

    // Clean up example database directories
    let _ = fs::remove_dir_all(db_path);
    let _ = fs::remove_dir_all(Path::new("./example_legacy_compat_db"));

    Ok(())
}
