use heed::types::Str;
use heed::Database as HeedDatabase;
use heed::{BytesDecode, BytesEncode};
use heed::{Env, RoTxn as HeedRoTxn, RwTxn as HeedRwTxn};
use std::borrow::Cow;
use std::convert::TryInto;
use std::error::Error as StdError;
use std::fmt;

const SCOPED_DB_NAME: &str = "my_scoped_db";
const DEFAULT_DB_NAME: &str = "my_default_db";

/// Custom error for ScopedDatabase operations
#[derive(Debug)]
pub enum ScopedDbError {
    Heed(heed::Error),
    EmptyScopeDisallowed,
    InvalidInput(String),
}

impl fmt::Display for ScopedDbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ScopedDbError::Heed(e) => write!(f, "Heed error: {}", e),
            ScopedDbError::EmptyScopeDisallowed => {
                write!(
                    f,
                    "Empty scope string (\"\") is not allowed for named scopes. If you intend to use the default (unscoped) database, use `None` instead."
                )
            }
            ScopedDbError::InvalidInput(s) => write!(f, "Invalid input: {}", s),
        }
    }
}

impl StdError for ScopedDbError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            ScopedDbError::Heed(e) => Some(e),
            _ => None,
        }
    }
}

impl From<heed::Error> for ScopedDbError {
    fn from(err: heed::Error) -> ScopedDbError {
        ScopedDbError::Heed(err)
    }
}

/// A helper struct to implement scoped databases using the Str type
pub struct ScopedDatabase {
    db_scoped: HeedDatabase<ScopedKeyCodec, Str>,
    db_default: HeedDatabase<DefaultKeyStrCodec, Str>,
}

impl ScopedDatabase {
    /// Create a new scoped database wrapper
    pub fn new(env: &Env) -> std::result::Result<Self, ScopedDbError> {
        let db_scoped = {
            let rtxn = env.read_txn()?;
            match env.open_database::<ScopedKeyCodec, Str>(&rtxn, Some(SCOPED_DB_NAME))? {
                Some(db) => {
                    rtxn.commit()?;
                    db
                }
                None => {
                    drop(rtxn);
                    let mut wtxn = env.write_txn()?;
                    let db = env
                        .create_database::<ScopedKeyCodec, Str>(&mut wtxn, Some(SCOPED_DB_NAME))?;
                    wtxn.commit()?;
                    db
                }
            }
        };

        let db_default = {
            let rtxn = env.read_txn()?;
            match env.open_database::<DefaultKeyStrCodec, Str>(&rtxn, Some(DEFAULT_DB_NAME))? {
                Some(db) => {
                    rtxn.commit()?;
                    db
                }
                None => {
                    drop(rtxn);
                    let mut wtxn = env.write_txn()?;
                    let db = env.create_database::<DefaultKeyStrCodec, Str>(
                        &mut wtxn,
                        Some(DEFAULT_DB_NAME),
                    )?;
                    wtxn.commit()?;
                    db
                }
            }
        };

        Ok(ScopedDatabase { db_scoped, db_default })
    }

    // Add new direct operation methods

    pub fn put(
        &self,
        txn: &mut HeedRwTxn<'_>,
        scope_name: Option<&str>,
        key: &str,
        value: &str,
    ) -> Result<(), ScopedDbError> {
        match scope_name {
            Some("") => Err(ScopedDbError::EmptyScopeDisallowed),
            Some(actual_scope) => {
                self.db_scoped.put(txn, &(actual_scope, key), value).map_err(ScopedDbError::from)
            }
            None => self.db_default.put(txn, key, value).map_err(ScopedDbError::from),
        }
    }

    // 'txn_borrow is the lifetime of the borrowed transaction `txn`
    pub fn get<'txn_borrow>(
        &self,
        txn: &'txn_borrow HeedRoTxn,
        scope_name: Option<&str>,
        key: &str,
    ) -> Result<Option<&'txn_borrow str>, ScopedDbError> {
        match scope_name {
            Some("") => Err(ScopedDbError::EmptyScopeDisallowed),
            Some(actual_scope) => {
                self.db_scoped.get(txn, &(actual_scope, key)).map_err(ScopedDbError::from)
            }
            None => self.db_default.get(txn, key).map_err(ScopedDbError::from),
        }
    }

    pub fn delete(
        &self,
        txn: &mut HeedRwTxn<'_>,
        scope_name: Option<&str>,
        key: &str,
    ) -> Result<bool, ScopedDbError> {
        match scope_name {
            Some("") => Err(ScopedDbError::EmptyScopeDisallowed),
            Some(actual_scope) => {
                self.db_scoped.delete(txn, &(actual_scope, key)).map_err(ScopedDbError::from)
            }
            None => self.db_default.delete(txn, key).map_err(ScopedDbError::from),
        }
    }

    pub fn clear_scope(
        &self,
        txn: &mut HeedRwTxn<'_>,
        scope_name: Option<&str>,
    ) -> Result<usize, ScopedDbError> {
        let mut count = 0;
        match scope_name {
            Some("") => Err(ScopedDbError::EmptyScopeDisallowed),
            Some(actual_scope) => {
                let mut iter_mut = self.db_scoped.prefix_iter_mut(txn, &(actual_scope, ""))?;
                while let Some(result) = iter_mut.next() {
                    result.map_err(ScopedDbError::from)?;
                    unsafe {
                        iter_mut.del_current().map_err(ScopedDbError::from)?;
                    }
                    count += 1;
                }
                Ok(count)
            }
            None => {
                // Clear entire default database
                let mut iter_mut = self.db_default.iter_mut(txn)?;
                while let Some(result) = iter_mut.next() {
                    result.map_err(ScopedDbError::from)?;
                    unsafe {
                        iter_mut.del_current().map_err(ScopedDbError::from)?;
                    }
                    count += 1;
                }
                // For potentially better performance / completeness on full clear:
                // self.db_default.clear(txn)?;
                // However, to return the count, iteration is necessary.
                Ok(count)
            }
        }
    }

    // 'txn_borrow is the lifetime of the borrowed transaction `txn`
    pub fn iter<'txn_borrow>(
        &self,
        txn: &'txn_borrow HeedRoTxn,
        scope_name: Option<&str>,
    ) -> Result<ScopedIter<'txn_borrow>, ScopedDbError> {
        match scope_name {
            Some("") => Err(ScopedDbError::EmptyScopeDisallowed),
            Some(actual_scope) => {
                let iter_native = self.db_scoped.prefix_iter(txn, &(actual_scope, ""))?;
                Ok(ScopedIter::Scoped(iter_native))
            }
            None => {
                let iter_native = self.db_default.iter(txn)?;
                Ok(ScopedIter::Default(iter_native))
            }
        }
    }
}

pub enum ScopedIter<'iter_life> {
    Default(heed::RoIter<'iter_life, DefaultKeyStrCodec, Str>),
    Scoped(heed::RoPrefix<'iter_life, ScopedKeyCodec, Str>),
}

impl<'iter_life> Iterator for ScopedIter<'iter_life> {
    type Item = std::result::Result<(&'iter_life str, &'iter_life str), ScopedDbError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            ScopedIter::Default(iter) => iter.next().map(|res| res.map_err(ScopedDbError::from)),
            ScopedIter::Scoped(iter) => iter.next().map(|res| match res {
                Ok(((_scope_str, original_key_str), value_str)) => {
                    Ok((original_key_str, value_str))
                }
                Err(e) => Err(ScopedDbError::from(e)),
            }),
        }
    }
}

// Custom codec for (scope, key) tuples
pub struct ScopedKeyCodec;

impl<'encode> BytesEncode<'encode> for ScopedKeyCodec {
    type EItem = (&'encode str, &'encode str);

    fn bytes_encode(
        item: &Self::EItem,
    ) -> std::result::Result<Cow<'encode, [u8]>, Box<dyn StdError + Send + Sync + 'static>> {
        let (scope, key) = *item;
        assert!(!scope.is_empty(), "ScopedKeyCodec should not be used with an empty scope string.");
        let scope_bytes = scope.as_bytes();
        let key_bytes = key.as_bytes();

        let scope_len = scope_bytes.len();
        if scope_len > u32::MAX as usize {
            return Err(Box::new(heed::Error::Encoding(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Scope string too long to encode its length in u32",
            )))));
        }
        let scope_len_u32 = scope_len as u32;

        let mut owned_bytes = Vec::with_capacity(4 + scope_bytes.len() + key_bytes.len());
        owned_bytes.extend_from_slice(&scope_len_u32.to_be_bytes());
        owned_bytes.extend_from_slice(scope_bytes);
        owned_bytes.extend_from_slice(key_bytes);
        Ok(Cow::Owned(owned_bytes))
    }
}

impl<'decode> BytesDecode<'decode> for ScopedKeyCodec {
    type DItem = (&'decode str, &'decode str);

    fn bytes_decode(
        bytes: &'decode [u8],
    ) -> std::result::Result<Self::DItem, Box<dyn StdError + Send + Sync + 'static>> {
        if bytes.len() < 4 {
            return Err(Box::new(heed::Error::Decoding(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Byte slice too short for scope length",
            )))));
        }

        let scope_len_bytes: [u8; 4] =
            bytes[0..4].try_into().map_err(|e| -> Box<dyn StdError + Send + Sync + 'static> {
                Box::new(heed::Error::Decoding(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Failed to read scope length bytes: {}", e),
                ))))
            })?;
        let scope_len = u32::from_be_bytes(scope_len_bytes) as usize;

        let scope_end_index = 4 + scope_len;
        if bytes.len() < scope_end_index {
            return Err(Box::new(heed::Error::Decoding(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Byte slice too short for declared scope length",
            )))));
        }

        let scope_bytes = &bytes[4..scope_end_index];
        let key_bytes = &bytes[scope_end_index..];

        let scope_str = std::str::from_utf8(scope_bytes).map_err(|e| {
            Box::new(heed::Error::Decoding(Box::new(e)))
                as Box<dyn StdError + Send + Sync + 'static>
        })?; // Cast to trait object
        let key_str = std::str::from_utf8(key_bytes).map_err(|e| {
            Box::new(heed::Error::Decoding(Box::new(e)))
                as Box<dyn StdError + Send + Sync + 'static>
        })?; // Cast to trait object

        Ok((scope_str, key_str))
    }
}

// New codec for default database keys, functionally identical to Str
// but a distinct type to potentially alter compiler lifetime inference.
pub struct DefaultKeyStrCodec;

impl<'encode> BytesEncode<'encode> for DefaultKeyStrCodec {
    type EItem = str; // Key type is str (unsized, like in heed::types::Str)

    fn bytes_encode(
        item: &'encode Self::EItem, // This becomes &'encode str
    ) -> std::result::Result<Cow<'encode, [u8]>, Box<dyn StdError + Send + Sync + 'static>> {
        // Delegate to Str's encoding for &str
        heed::types::Str::bytes_encode(item)
    }
}

impl<'decode> BytesDecode<'decode> for DefaultKeyStrCodec {
    type DItem = &'decode str; // Decoded type is &'decode str

    fn bytes_decode(
        bytes: &'decode [u8],
    ) -> std::result::Result<Self::DItem, Box<dyn StdError + Send + Sync + 'static>> {
        // Delegate to Str's decoding
        heed::types::Str::bytes_decode(bytes)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::needless_option_as_deref)]
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::{ScopedDatabase, ScopedDbError};
    use heed::types::Str;
    use heed::{Env, EnvOpenOptions};

    static TEST_DB_COUNTER: AtomicUsize = AtomicUsize::new(0);

    struct TestEnv {
        env: Env,
        db_path: PathBuf,
    }

    impl TestEnv {
        fn new(test_name: &str) -> Result<Self, ScopedDbError> {
            let count = TEST_DB_COUNTER.fetch_add(1, Ordering::SeqCst);
            let db_path = PathBuf::from(format!("./test_db_{}_{}", test_name, count));

            if db_path.exists() {
                fs::remove_dir_all(&db_path).map_err(|e| {
                    ScopedDbError::InvalidInput(format!(
                        "Failed to clean up old test DB dir: {}",
                        e
                    ))
                })?;
            }
            fs::create_dir_all(&db_path).map_err(|e| {
                ScopedDbError::InvalidInput(format!("Failed to create test DB dir: {}", e))
            })?;

            // Intentionally not printing to stdout during library tests
            // println!("TestEnv: Initializing new Env for path: {:?}", db_path);
            let env = unsafe {
                EnvOpenOptions::new()
                    .map_size(10 * 1024 * 1024) // 10MB
                    .max_dbs(15)
                    .open(&db_path)?
            };
            // println!("TestEnv: Env initialized.");
            Ok(TestEnv { env, db_path })
        }
    }

    impl Drop for TestEnv {
        fn drop(&mut self) {
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
            assert_eq!(user_db.get(&rtxn, Some("customer"), "cust1")?.map(|s| s.to_string()), None);
            assert_eq!(
                user_db.get(&rtxn, Some("customer"), "cust2")?.map(|s| s.to_string()),
                Some("Eve (customer)".to_string())
            );
            assert_eq!(user_db.get(&rtxn, None, "user4_default")?.map(|s| s.to_string()), None);
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
            assert!(!deleted_non_existent, "Deleting a non-existent key should return false");
            wtxn.commit()?;
        }

        {
            let mut wtxn = env.write_txn()?;
            let cleared_count_empty = user_db.clear_scope(&mut wtxn, Some("new_empty_scope"))?;
            assert_eq!(cleared_count_empty, 0, "Clearing an empty scope should remove 0 items");
            wtxn.commit()?;
        }

        {
            let rtxn = env.read_txn()?;
            assert_eq!(
                user_db.get(&rtxn, Some("main"), "user1")?.as_deref(),
                Some("Alice Updated (main)")
            );
            let get_non_existent = user_db.get(&rtxn, Some("main"), "really_does_not_exist")?;
            assert!(get_non_existent.is_none(), "Getting a non-existent key should return None");

            let new_empty_items: Vec<(String, String)> = user_db
                .iter(&rtxn, Some("new_empty_scope"))?
                .map(|res| res.map(|(k, v)| (k.to_string(), v.to_string())))
                .collect::<Result<_, _>>()?;
            assert_eq!(new_empty_items.len(), 0, "new_empty_scope should be empty after clearing");
        }
        Ok(())
    }

    #[test]
    fn test_legacy_db_compatibility() -> Result<(), ScopedDbError> {
        const LEGACY_DEFAULT_DB_NAME: &str = "my_default_db"; // This matches the constant in the parent module
        let test_env = TestEnv::new("legacy_compat")?;
        let legacy_env = &test_env.env;
        {
            let mut wtxn_legacy_setup = legacy_env.write_txn()?;
            // When testing legacy, we directly use heed's Database with Str, Str (or Bytes, Str as in original)
            // to simulate an existing database created without DefaultKeyStrCodec.
            let db = legacy_env.create_database::<Str, Str>(
                &mut wtxn_legacy_setup,
                Some(LEGACY_DEFAULT_DB_NAME),
            )?;
            db.put(&mut wtxn_legacy_setup, "legacy_key1", "legacy_value1_original")?;
            db.put(&mut wtxn_legacy_setup, "legacy_key2", "legacy_value2_original")?;
            wtxn_legacy_setup.commit()?;
        }
        {
            // Diagnostic check: ensure the raw database was populated as expected
            let rtxn_diag = legacy_env.read_txn()?;
            let temp_db_handle = legacy_env
                .open_database::<Str, Str>(&rtxn_diag, Some(LEGACY_DEFAULT_DB_NAME))?
                .ok_or_else(|| {
                    ScopedDbError::InvalidInput(format!(
                        "DIAGNOSTIC: DB {} not found for Str, Str",
                        LEGACY_DEFAULT_DB_NAME
                    ))
                })?;
            assert_eq!(
                temp_db_handle.get(&rtxn_diag, "legacy_key1")?,
                Some("legacy_value1_original")
            );
        }

        // Now, initialize ScopedDatabase on this environment
        let scoped_db_on_legacy = ScopedDatabase::new(legacy_env)?;

        // Test reading existing legacy data via ScopedDatabase's default (None) scope
        {
            let rtxn = legacy_env.read_txn()?; // Changed wtxn to rtxn for read operations
            assert_eq!(
                scoped_db_on_legacy.get(&rtxn, None, "legacy_key1")?.as_deref(),
                Some("legacy_value1_original")
            );
            assert_eq!(
                scoped_db_on_legacy.get(&rtxn, None, "legacy_key2")?.as_deref(),
                Some("legacy_value2_original")
            );
        }

        // Test writing new/overwriting legacy data via ScopedDatabase's default (None) scope
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

        // Test writing to a new named scope in the same environment
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

        // Final verification
        {
            let rtxn = legacy_env.read_txn()?;
            assert_eq!(
                scoped_db_on_legacy.get(&rtxn, None, "legacy_key1")?.as_deref(),
                Some("legacy_value1_overwritten_by_scoped")
            );
            assert_eq!(
                scoped_db_on_legacy.get(&rtxn, None, "legacy_key2")?.as_deref(),
                Some("legacy_value2_original")
            );
            assert_eq!(
                scoped_db_on_legacy.get(&rtxn, None, "new_default_key1")?.as_deref(),
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
                    ("legacy_key1".to_string(), "legacy_value1_overwritten_by_scoped".to_string()),
                    ("legacy_key2".to_string(), "legacy_value2_original".to_string()),
                    ("new_default_key1".to_string(), "new_default_value1".to_string()),
                ]
            );

            assert_eq!(
                scoped_db_on_legacy
                    .get(&rtxn, Some("brand_new_scope"), "new_scoped_key1")?
                    .as_deref(),
                Some("new_scoped_value1_in_brand_new")
            );
            let brand_new_items: Vec<(String, String)> = scoped_db_on_legacy
                .iter(&rtxn, Some("brand_new_scope"))?
                .map(|res| res.map(|(k, v)| (k.to_string(), v.to_string())))
                .collect::<Result<_, _>>()?;
            assert_eq!(brand_new_items.len(), 1);

            let initial_items: Vec<(String, String)> = scoped_db_on_legacy
                .iter(&rtxn, Some("initial_scope_for_legacy_test"))?
                .map(|res| res.map(|(k, v)| (k.to_string(), v.to_string())))
                .collect::<Result<_, _>>()?;
            assert_eq!(initial_items.len(), 0);
        }
        Ok(())
    }
}
