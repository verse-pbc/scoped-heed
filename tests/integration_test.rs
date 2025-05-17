#![allow(clippy::needless_option_as_deref)]
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

use heed::types::{Bytes, Str};
use heed::{Env, EnvOpenOptions};
// Adjusted to use the library crate name, assuming `scoped_heed` is the crate name.
// If tests are in the same crate and `src/lib.rs` is the crate root, `crate::` or `super::` might be used.
// For an integration test, it typically uses the crate as an external dependency.
use scoped_heed::{ScopedDatabase, ScopedDbError};

static TEST_DB_COUNTER: AtomicUsize = AtomicUsize::new(0);

struct TestEnv {
    env: Env,
    db_path: PathBuf,
}

impl TestEnv {
    fn new(test_name: &str) -> Result<Self, ScopedDbError> {
        let count = TEST_DB_COUNTER.fetch_add(1, Ordering::SeqCst);
        // Store test databases in target directory to be cleaned by `cargo clean`
        let base_path = PathBuf::from(format!("{}/test_dbs", env!("CARGO_TARGET_TMPDIR")));
        fs::create_dir_all(&base_path).map_err(|e| {
            ScopedDbError::InvalidInput(format!("Failed to create base test DB dir: {}", e))
        })?;
        let db_path = base_path.join(format!("test_db_{}_{}", test_name, count));

        if db_path.exists() {
            fs::remove_dir_all(&db_path).map_err(|e| {
                ScopedDbError::InvalidInput(format!("Failed to clean up old test DB dir: {}", e))
            })?;
        }
        fs::create_dir_all(&db_path).map_err(|e| {
            ScopedDbError::InvalidInput(format!("Failed to create test DB dir: {}", e))
        })?;

        println!("TestEnv: Initializing new Env for path: {:?}", db_path);
        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(10 * 1024 * 1024) // 10MB
                .max_dbs(15) // Max DBs used by tests
                .open(&db_path)?
        };
        println!("TestEnv: Env initialized.");
        Ok(TestEnv { env, db_path })
    }
}

impl Drop for TestEnv {
    fn drop(&mut self) {
        // Attempt to clean up, but don\'t panic on failure as it\'s just a test utility.
        let _ = fs::remove_dir_all(&self.db_path);
    }
}

#[test]
fn test_basic_scoped_operations() -> Result<(), ScopedDbError> {
    let test_env = TestEnv::new("basic_ops_simplified")?;
    let env = &test_env.env;

    let user_db = ScopedDatabase::new(env)?;

    {
        let mut wtxn = env.write_txn()?;
        user_db.put(&mut wtxn, Some("main"), "user1", "Alice (main)")?;
        wtxn.commit()?;
    }

    {
        let rtxn = env.read_txn()?;
        let value = user_db.get(&rtxn, Some("main"), "user1")?;
        let cloned_value_str = value.map(|s| s.to_string());
        assert_eq!(cloned_value_str.as_deref(), Some("Alice (main)"));
    }

    {
        let mut wtxn = env.write_txn()?;
        user_db.put(&mut wtxn, None, "user_default", "Bob (default)")?;
        wtxn.commit()?;
    }

    {
        let rtxn = env.read_txn()?;
        let value_default = user_db.get(&rtxn, None, "user_default")?;
        let cloned_value_default_str = value_default.map(|s| s.to_string());
        assert_eq!(cloned_value_default_str.as_deref(), Some("Bob (default)"));
    }

    // Restore more complete basic operations testing
    {
        let mut wtxn = env.write_txn()?;
        user_db.put(&mut wtxn, Some("main"), "user2", "Bob (main)")?;
        user_db.put(&mut wtxn, None, "user1_default", "Alice (default)")?;
        user_db.put(&mut wtxn, None, "user4_default", "Zane (default)")?;
        user_db.put(&mut wtxn, Some("customer"), "cust1", "David (customer)")?;
        user_db.put(&mut wtxn, Some("customer"), "cust2", "Eve (customer)")?;
        user_db.put(&mut wtxn, Some("admin"), "adm1", "Frank (admin)")?;
        user_db.put(&mut wtxn, Some("admin"), "adm2", "Supervisor (admin)")?;
        wtxn.commit()?;
    }

    {
        let rtxn = env.read_txn()?;
        let mut default_users: Vec<(String, String)> = user_db
            .iter(&rtxn, None)?
            .map(|res| res.map(|(k, v)| (k.to_string(), v.to_string())))
            .collect::<Result<_, _>>()?;
        default_users.sort();
        assert_eq!(
            default_users,
            vec![
                ("user1_default".to_string(), "Alice (default)".to_string()),
                ("user4_default".to_string(), "Zane (default)".to_string()),
                ("user_default".to_string(), "Bob (default)".to_string()),
            ]
        );

        let mut main_users: Vec<(String, String)> = user_db
            .iter(&rtxn, Some("main"))?
            .map(|res| res.map(|(k, v)| (k.to_string(), v.to_string())))
            .collect::<Result<_, _>>()?;
        main_users.sort();
        assert_eq!(
            main_users,
            vec![
                ("user1".to_string(), "Alice (main)".to_string()),
                ("user2".to_string(), "Bob (main)".to_string()),
            ]
        );
    }

    {
        let mut wtxn = env.write_txn()?;
        assert!(user_db.delete(&mut wtxn, Some("customer"), "cust1")?);
        assert!(!user_db.delete(&mut wtxn, Some("customer"), "non_existent_user")?);
        assert!(user_db.delete(&mut wtxn, None, "user4_default")?);
        assert_eq!(user_db.clear_scope(&mut wtxn, Some("admin"))?, 2);
        wtxn.commit()?;
    }

    {
        let rtxn = env.read_txn()?;
        assert_eq!(
            user_db
                .get(&rtxn, Some("customer"), "cust1")?
                .map(|s| s.to_string()),
            None
        );
        assert_eq!(
            user_db
                .get(&rtxn, Some("customer"), "cust2")?
                .map(|s| s.to_string()),
            Some("Eve (customer)".to_string())
        );
        assert_eq!(
            user_db
                .get(&rtxn, None, "user4_default")?
                .map(|s| s.to_string()),
            None
        );
        let admin_users_after_clear: Vec<(String, String)> = user_db
            .iter(&rtxn, Some("admin"))?
            .map(|res| res.map(|(k, v)| (k.to_string(), v.to_string())))
            .collect::<Result<_, _>>()?;
        assert!(admin_users_after_clear.is_empty());
    }

    Ok(())
}

#[test]
fn test_additional_cases() -> Result<(), ScopedDbError> {
    let test_env = TestEnv::new("additional_cases")?;
    let env = &test_env.env;
    let user_db = ScopedDatabase::new(env)?;

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
        assert!(
            !deleted_non_existent,
            "Deleting a non-existent key should return false"
        );
        wtxn.commit()?;
    }

    {
        let mut wtxn = env.write_txn()?;
        let cleared_count_empty = user_db.clear_scope(&mut wtxn, Some("new_empty_scope"))?;
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
            Some("Alice Updated (main)")
        );
        let get_non_existent = user_db.get(&rtxn, Some("main"), "really_does_not_exist")?;
        assert!(
            get_non_existent.is_none(),
            "Getting a non-existent key should return None"
        );

        let new_empty_items: Vec<(String, String)> = user_db
            .iter(&rtxn, Some("new_empty_scope"))?
            .map(|res| res.map(|(k, v)| (k.to_string(), v.to_string())))
            .collect::<Result<_, _>>()?;
        assert_eq!(
            new_empty_items.len(),
            0,
            "new_empty_scope should be empty after clearing"
        );
    }
    Ok(())
}

#[test]
fn test_legacy_db_compatibility() -> Result<(), ScopedDbError> {
    const LEGACY_DEFAULT_DB_NAME: &str = "my_default_db";
    let test_env = TestEnv::new("legacy_compat")?;
    let legacy_env = &test_env.env;
    {
        let mut wtxn_legacy_setup = legacy_env.write_txn()?;
        // Use Bytes for key to match DefaultKeyStrCodec which is effectively Bytes for key
        let db = legacy_env
            .create_database::<Bytes, Str>(&mut wtxn_legacy_setup, Some(LEGACY_DEFAULT_DB_NAME))?;
        db.put(
            &mut wtxn_legacy_setup,
            b"legacy_key1",
            "legacy_value1_original",
        )?;
        db.put(
            &mut wtxn_legacy_setup,
            b"legacy_key2",
            "legacy_value2_original",
        )?;
        wtxn_legacy_setup.commit()?;
    }
    {
        // Diagnostic read to ensure legacy DB was set up as expected
        let rtxn_diag = legacy_env.read_txn()?;
        let temp_db_handle = legacy_env
            .open_database::<Bytes, Str>(&rtxn_diag, Some(LEGACY_DEFAULT_DB_NAME))?
            .ok_or_else(|| {
                ScopedDbError::InvalidInput(format!(
                    "DIAGNOSTIC: DB {} not found",
                    LEGACY_DEFAULT_DB_NAME
                ))
            })?;
        assert_eq!(
            temp_db_handle.get(&rtxn_diag, b"legacy_key1")?,
            Some("legacy_value1_original")
        );
    }
    let scoped_db_on_legacy = ScopedDatabase::new(legacy_env)?;
    {
        // Use a read transaction for initial gets, as per good practice, then a write txn for puts.
        let rtxn = legacy_env.read_txn()?;
        assert_eq!(
            scoped_db_on_legacy
                .get(&rtxn, None, "legacy_key1")?
                .as_deref(),
            Some("legacy_value1_original")
        );
        assert_eq!(
            scoped_db_on_legacy
                .get(&rtxn, None, "legacy_key2")?
                .as_deref(),
            Some("legacy_value2_original")
        );
    }
    {
        let mut wtxn = legacy_env.write_txn()?;
        scoped_db_on_legacy.put(&mut wtxn, None, "new_default_key1", "new_default_value1")?;
        scoped_db_on_legacy.put(
            &mut wtxn,
            None,
            "legacy_key1",
            "legacy_value1_overwritten_by_scoped",
        )?;
        wtxn.commit()?;
    }
    {
        let mut wtxn = legacy_env.write_txn()?;
        scoped_db_on_legacy.put(
            &mut wtxn,
            Some("brand_new_scope"),
            "new_scoped_key1",
            "new_scoped_value1_in_brand_new",
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
        assert_eq!(
            scoped_db_on_legacy
                .get(&rtxn, None, "legacy_key2")?
                .as_deref(),
            Some("legacy_value2_original")
        );
        assert_eq!(
            scoped_db_on_legacy
                .get(&rtxn, None, "new_default_key1")?
                .as_deref(),
            Some("new_default_value1")
        );
        let mut default_items: Vec<(String, String)> = scoped_db_on_legacy
            .iter(&rtxn, None)?
            .map(|res| res.map(|(k, v)| (k.to_string(), v.to_string())))
            .collect::<Result<_, _>>()?;
        default_items.sort(); // Sort for consistent assertion
        assert_eq!(default_items.len(), 3);
        assert_eq!(
            default_items,
            vec![
                (
                    "legacy_key1".to_string(),
                    "legacy_value1_overwritten_by_scoped".to_string()
                ),
                (
                    "legacy_key2".to_string(),
                    "legacy_value2_original".to_string()
                ),
                (
                    "new_default_key1".to_string(),
                    "new_default_value1".to_string()
                ),
            ]
        );

        assert_eq!(
            scoped_db_on_legacy
                .get(&rtxn, Some("brand_new_scope"), "new_scoped_key1")?
                .as_deref(),
            Some("new_scoped_value1_in_brand_new")
        );
        let mut brand_new_items: Vec<(String, String)> = scoped_db_on_legacy
            .iter(&rtxn, Some("brand_new_scope"))?
            .map(|res| res.map(|(k, v)| (k.to_string(), v.to_string())))
            .collect::<Result<_, _>>()?;
        brand_new_items.sort(); // Sort for consistent assertion
        assert_eq!(brand_new_items.len(), 1);
        assert_eq!(
            brand_new_items,
            vec![(
                "new_scoped_key1".to_string(),
                "new_scoped_value1_in_brand_new".to_string()
            )]
        );

        let initial_items: Vec<(String, String)> = scoped_db_on_legacy
            .iter(&rtxn, Some("initial_scope_for_legacy_test"))?
            .map(|res| res.map(|(k, v)| (k.to_string(), v.to_string())))
            .collect::<Result<_, _>>()?;
        assert_eq!(initial_items.len(), 0);
    }
    Ok(())
}
