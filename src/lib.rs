//! # scoped-heed
//!
//! A library providing Redis-like database isolation for LMDB via the heed wrapper.
//!
//! ## Scope Isolation Model
//!
//! This library implements complete scope isolation similar to Redis databases:
//! - Each scope acts as an independent database within the same LMDB environment
//! - Operations are strictly confined to a single scope
//! - No cross-scope queries or operations are possible
//! - Keys can be identical across different scopes without collision
//! - Clearing a scope only affects that specific scope's data
//!
//! This design is perfect for:
//! - Multi-tenant applications requiring data isolation
//! - Test scenarios where each test needs its own database
//! - Modular systems with independent components
//!
//! ## Example
//!
//! ```rust,no_run
//! use scoped_heed::{scoped_database_options, ScopedDbError};
//! use heed::EnvOpenOptions;
//!
//! # fn main() -> Result<(), ScopedDbError> {
//! // Open environment
//! let env = unsafe {
//!     EnvOpenOptions::new()
//!         .map_size(10 * 1024 * 1024)
//!         .max_dbs(3)
//!         .open("./my_db")?
//! };
//!
//! // Create a scoped database
//! let mut wtxn = env.write_txn()?;
//! let db = scoped_database_options(&env)
//!     .types::<String, String>()
//!     .name("my_data")
//!     .create(&mut wtxn)?;
//! wtxn.commit()?;
//!
//! // Use different scopes for different tenants
//! let mut wtxn = env.write_txn()?;
//! db.put(&mut wtxn, Some("tenant1"), &"key1".to_string(), &"value1".to_string())?;
//! db.put(&mut wtxn, Some("tenant2"), &"key1".to_string(), &"value2".to_string())?;
//! wtxn.commit()?;
//!
//! // Each scope is completely isolated
//! let rtxn = env.read_txn()?;
//! let val1 = db.get(&rtxn, Some("tenant1"), &"key1".to_string())?; // Some("value1")
//! let val2 = db.get(&rtxn, Some("tenant2"), &"key1".to_string())?; // Some("value2")
//! # Ok(())
//! # }
//! ```

use heed::Database as HeedDatabase;
use heed::types::{Bytes, SerdeBincode};
use heed::{BytesDecode, BytesEncode};
use heed::{Env, RoTxn, RwTxn};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error as StdError;
use std::fmt;
use std::hash::Hasher;
use std::marker::PhantomData;
use std::ops::{Bound, RangeBounds};
use std::sync::RwLock;

/// Adapter to convert `RangeBounds<&[u8]>` to `RangeBounds<[u8]>` for heed's Bytes codec.
struct HeedRangeAdapter<'a, R: RangeBounds<&'a [u8]>>(&'a R, PhantomData<&'a ()>);

impl<'a, R: RangeBounds<&'a [u8]>> HeedRangeAdapter<'a, R> {
    fn new(range: &'a R) -> Self {
        HeedRangeAdapter(range, PhantomData)
    }
}

impl<'a, R: RangeBounds<&'a [u8]>> RangeBounds<[u8]> for HeedRangeAdapter<'a, R> {
    fn start_bound(&self) -> Bound<&[u8]> {
        match self.0.start_bound() {
            Bound::Included(&k) => Bound::Included(k),
            Bound::Excluded(&k) => Bound::Excluded(k),
            Bound::Unbounded => Bound::Unbounded,
        }
    }

    fn end_bound(&self) -> Bound<&[u8]> {
        match self.0.end_bound() {
            Bound::Included(&k) => Bound::Included(k),
            Bound::Excluded(&k) => Bound::Excluded(k),
            Bound::Unbounded => Bound::Unbounded,
        }
    }
}

pub mod builder;
pub use builder::scoped_database_options;

// Removed hardcoded database names - now constructed from base name

/// Error type for scoped database operations.
#[derive(Debug)]
pub enum ScopedDbError {
    Heed(heed::Error),
    /// Attempted to use an empty string as a scope name, which is disallowed.
    EmptyScopeDisallowed,
    /// Other input validation errors.
    InvalidInput(String),
    /// Encoding error
    Encoding(String),
}

impl fmt::Display for ScopedDbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ScopedDbError::Heed(e) => write!(f, "Heed error: {}", e),
            ScopedDbError::EmptyScopeDisallowed => {
                write!(
                    f,
                    "Empty strings are not allowed as scope names. Use `None` for the default scope."
                )
            }
            ScopedDbError::InvalidInput(msg) => write!(f, "Invalid input: {}", msg),
            ScopedDbError::Encoding(msg) => write!(f, "Encoding error: {}", msg),
        }
    }
}

impl StdError for ScopedDbError {}

impl From<heed::Error> for ScopedDbError {
    fn from(error: heed::Error) -> Self {
        ScopedDbError::Heed(error)
    }
}

impl From<Box<dyn std::error::Error + Send + Sync>> for ScopedDbError {
    fn from(error: Box<dyn std::error::Error + Send + Sync>) -> Self {
        ScopedDbError::Encoding(error.to_string())
    }
}

/// Manages scope hashes to avoid hash collisions.
#[derive(Debug)]
struct ScopeHasher {
    scope_to_hash: HashMap<String, u32>,
    hash_to_scope: HashMap<u32, String>,
}

impl ScopeHasher {
    fn new() -> Self {
        Self {
            scope_to_hash: HashMap::new(),
            hash_to_scope: HashMap::new(),
        }
    }

    fn hash(&mut self, scope: &str) -> Result<u32, ScopedDbError> {
        if let Some(&hash) = self.scope_to_hash.get(scope) {
            return Ok(hash);
        }

        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        hasher.write(scope.as_bytes());
        let full_hash = hasher.finish();
        let hash = (full_hash & 0xFFFF_FFFF) as u32;

        if let Some(existing_scope) = self.hash_to_scope.get(&hash) {
            if existing_scope != scope {
                return Err(ScopedDbError::InvalidInput(format!(
                    "Hash collision detected between '{}' and '{}'",
                    scope, existing_scope
                )));
            }
        }

        self.scope_to_hash.insert(scope.to_string(), hash);
        self.hash_to_scope.insert(hash, scope.to_string());
        Ok(hash)
    }
}

/// Tuple type for scoped keys: (scope_hash, original_key)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScopedKey<K> {
    pub scope_hash: u32,
    pub key: K,
}

/// A scoped database providing Redis-like isolation between scopes.
///
/// Each scope acts as a completely isolated database:
/// - Operations are confined to a single scope
/// - No cross-scope queries or access is possible
/// - Keys can overlap between scopes without collision
///
/// This is the most flexible database type, supporting any Serialize/Deserialize types
/// for both keys and values. For better performance with byte keys, see
/// `ScopedBytesKeyDatabase` or `ScopedBytesDatabase`.
#[derive(Debug)]
pub struct ScopedDatabase<K, V>
where
    K: Serialize + for<'de> Deserialize<'de> + Clone + 'static,
    V: Serialize + for<'de> Deserialize<'de> + 'static,
{
    db_scoped: HeedDatabase<SerdeBincode<ScopedKey<K>>, SerdeBincode<V>>,
    db_default: HeedDatabase<SerdeBincode<K>, SerdeBincode<V>>,
    scope_hasher: RwLock<ScopeHasher>,
    _phantom: PhantomData<(K, V)>,
}

impl<K, V> ScopedDatabase<K, V>
where
    K: Serialize + for<'de> Deserialize<'de> + Clone + 'static,
    V: Serialize + for<'de> Deserialize<'de> + 'static,
{
    pub fn new(env: &Env, name: &str) -> Result<Self, ScopedDbError> {
        let mut wtxn = env.write_txn()?;
        let db = Self::create(env, name, &mut wtxn)?;
        wtxn.commit()?;
        Ok(db)
    }

    /// Create a new ScopedDatabase with a provided transaction
    pub fn create(env: &Env, name: &str, txn: &mut RwTxn) -> Result<Self, ScopedDbError> {
        // Create database names from base name
        let default_name = format!("{}_default", name);
        let scoped_name = format!("{}_scoped", name);

        // Open databases
        let db_default = env
            .database_options()
            .types::<SerdeBincode<K>, SerdeBincode<V>>()
            .name(&default_name)
            .create(txn)?;

        let db_scoped = env
            .database_options()
            .types::<SerdeBincode<ScopedKey<K>>, SerdeBincode<V>>()
            .name(&scoped_name)
            .create(txn)?;

        Ok(Self {
            db_scoped,
            db_default,
            scope_hasher: RwLock::new(ScopeHasher::new()),
            _phantom: PhantomData,
        })
    }

    /// Insert a key-value pair into the database.
    pub fn put(
        &self,
        txn: &mut RwTxn<'_>,
        scope_name: Option<&str>,
        key: &K,
        value: &V,
    ) -> Result<(), ScopedDbError> {
        match scope_name {
            Some("") => Err(ScopedDbError::EmptyScopeDisallowed),
            Some(actual_scope) => {
                let mut hasher = self.scope_hasher.write().unwrap();
                let scope_hash = hasher.hash(actual_scope)?;

                // Use ScopedKey tuple
                let scoped_key = ScopedKey {
                    scope_hash,
                    key: key.clone(),
                };
                self.db_scoped
                    .put(txn, &scoped_key, value)
                    .map_err(ScopedDbError::from)
            }
            None => self
                .db_default
                .put(txn, key, value)
                .map_err(ScopedDbError::from),
        }
    }

    /// Get a value from the database.
    pub fn get<'txn>(
        &self,
        txn: &'txn RoTxn<'txn>,
        scope_name: Option<&str>,
        key: &K,
    ) -> Result<Option<V>, ScopedDbError> {
        match scope_name {
            Some("") => Err(ScopedDbError::EmptyScopeDisallowed),
            Some(actual_scope) => {
                let mut hasher = self.scope_hasher.write().unwrap();
                let scope_hash = hasher.hash(actual_scope)?;

                // Use ScopedKey tuple
                let scoped_key = ScopedKey {
                    scope_hash,
                    key: key.clone(),
                };
                self.db_scoped
                    .get(txn, &scoped_key)
                    .map_err(ScopedDbError::from)
            }
            None => self.db_default.get(txn, key).map_err(ScopedDbError::from),
        }
    }

    /// Delete a key-value pair from the database.
    pub fn delete(
        &self,
        txn: &mut RwTxn<'_>,
        scope_name: Option<&str>,
        key: &K,
    ) -> Result<bool, ScopedDbError> {
        match scope_name {
            Some("") => Err(ScopedDbError::EmptyScopeDisallowed),
            Some(actual_scope) => {
                let mut hasher = self.scope_hasher.write().unwrap();
                let scope_hash = hasher.hash(actual_scope)?;

                let scoped_key = ScopedKey {
                    scope_hash,
                    key: key.clone(),
                };
                self.db_scoped
                    .delete(txn, &scoped_key)
                    .map_err(ScopedDbError::from)
            }
            None => self
                .db_default
                .delete(txn, key)
                .map_err(ScopedDbError::from),
        }
    }

    /// Clear all entries within a specific scope or the default database.
    pub fn clear(
        &self,
        txn: &mut RwTxn<'_>,
        scope_name: Option<&str>,
    ) -> Result<(), ScopedDbError> {
        match scope_name {
            Some("") => Err(ScopedDbError::EmptyScopeDisallowed),
            Some(actual_scope) => {
                let mut hasher = self.scope_hasher.write().unwrap();
                let scope_hash = hasher.hash(actual_scope)?;

                // We need to collect entries to delete before modifying the database
                let mut entries_to_delete = Vec::new();

                // Use a temporary reference to the transaction for iteration
                let txn_ref = &*txn;
                for result in self.db_scoped.iter(txn_ref)? {
                    let (scoped_key, _value) = result?;
                    if scoped_key.scope_hash == scope_hash {
                        entries_to_delete.push(scoped_key.clone());
                    }
                }

                // Now delete the collected entries
                for scoped_key in entries_to_delete {
                    self.db_scoped.delete(txn, &scoped_key)?;
                }

                Ok(())
            }
            None => self.db_default.clear(txn).map_err(ScopedDbError::from),
        }
    }

    /// Iterate over entries in a specific scope or the default database.
    pub fn iter<'txn>(
        &self,
        txn: &'txn RoTxn<'txn>,
        scope_name: Option<&str>,
    ) -> Result<Box<dyn Iterator<Item = Result<(K, V), ScopedDbError>> + 'txn>, ScopedDbError> {
        match scope_name {
            Some("") => Err(ScopedDbError::EmptyScopeDisallowed),
            Some(actual_scope) => {
                let mut hasher = self.scope_hasher.write().unwrap();
                let scope_hash = hasher.hash(actual_scope)?;

                let iter = self
                    .db_scoped
                    .iter(txn)?
                    .filter_map(move |result| match result {
                        Ok((scoped_key, value)) => {
                            if scoped_key.scope_hash == scope_hash {
                                Some(Ok((scoped_key.key, value)))
                            } else {
                                None
                            }
                        }
                        Err(e) => Some(Err(ScopedDbError::from(e))),
                    });
                Ok(Box::new(iter))
            }
            None => {
                let iter = self
                    .db_default
                    .iter(txn)?
                    .map(|result| result.map_err(ScopedDbError::from));
                Ok(Box::new(iter))
            }
        }
    }

    /// Iterate over a range of entries in a specific scope or the default database.
    pub fn range<'sbd_ref, 'txn_ref, 'bounds_ref, R>(
        &'sbd_ref self,
        txn: &'txn_ref RoTxn<'txn_ref>,
        scope_name: Option<&str>,
        range: &'bounds_ref R,
    ) -> Result<Box<dyn Iterator<Item = Result<(K, V), ScopedDbError>> + 'txn_ref>, ScopedDbError>
    where
        K: Clone + PartialOrd,
        R: RangeBounds<K> + 'bounds_ref,
    {
        match scope_name {
            Some("") => Err(ScopedDbError::EmptyScopeDisallowed),
            Some(actual_scope) => {
                let mut hasher = self.scope_hasher.write().unwrap();
                let scope_hash = hasher.hash(actual_scope)?;

                // Transform the range bounds to work with our ScopedKey<K> structure
                use std::ops::Bound;

                let transformed_start = match range.start_bound() {
                    Bound::Included(key) => Bound::Included(ScopedKey {
                        scope_hash,
                        key: key.clone(),
                    }),
                    Bound::Excluded(key) => Bound::Excluded(ScopedKey {
                        scope_hash,
                        key: key.clone(),
                    }),
                    Bound::Unbounded => {
                        // Start from the beginning of this scope
                        // Note: This correctly handles the unbounded case since
                        // keys are ordered first by scope_hash, then by key
                        Bound::Unbounded
                    }
                };

                let transformed_end = match range.end_bound() {
                    Bound::Included(key) => Bound::Included(ScopedKey {
                        scope_hash,
                        key: key.clone(),
                    }),
                    Bound::Excluded(key) => Bound::Excluded(ScopedKey {
                        scope_hash,
                        key: key.clone(),
                    }),
                    Bound::Unbounded => {
                        // We can't use Unbounded here as it would include keys from other scopes
                        // No good solution for this with the current design - fall back to filtering
                        return self
                            .db_scoped
                            .iter(txn)?
                            .filter_map(move |result| match result {
                                Ok((scoped_key, value)) => {
                                    if scoped_key.scope_hash == scope_hash
                                        && range.contains(&scoped_key.key)
                                    {
                                        Some(Ok((scoped_key.key, value)))
                                    } else {
                                        None
                                    }
                                }
                                Err(e) => Some(Err(ScopedDbError::from(e))),
                            })
                            .collect::<Result<Vec<_>, _>>()
                            .map(|v| {
                                Box::new(v.into_iter().map(Ok))
                                    as Box<
                                        dyn Iterator<Item = Result<(K, V), ScopedDbError>>
                                            + 'txn_ref,
                                    >
                            });
                    }
                };

                let transformed_range = (transformed_start, transformed_end);

                // Use the native range method directly
                let iter =
                    self.db_scoped
                        .range(txn, &transformed_range)?
                        .map(|result| match result {
                            Ok((scoped_key, value)) => Ok((scoped_key.key, value)),
                            Err(e) => Err(ScopedDbError::from(e)),
                        });
                Ok(Box::new(iter))
            }
            None => {
                let iter = self
                    .db_default
                    .range(txn, range)?
                    .map(|result| result.map_err(ScopedDbError::from));
                Ok(Box::new(iter))
            }
        }
    }
}

impl<K, V> Clone for ScopedDatabase<K, V>
where
    K: Serialize + for<'de> Deserialize<'de> + Clone + 'static,
    V: Serialize + for<'de> Deserialize<'de> + 'static,
{
    fn clone(&self) -> Self {
        Self {
            db_scoped: self.db_scoped.clone(),
            db_default: self.db_default.clone(),
            scope_hasher: RwLock::new(ScopeHasher::new()), // Create fresh hasher
            _phantom: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use heed::EnvOpenOptions;

    #[test]
    fn test_basic_string_operations() -> Result<(), ScopedDbError> {
        let db_path = "./test_scoped_db";
        if std::path::Path::new(db_path).exists() {
            std::fs::remove_dir_all(db_path).unwrap();
        }
        std::fs::create_dir_all(db_path).unwrap();

        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(10 * 1024 * 1024)
                .max_dbs(4)
                .open(db_path)?
        };

        type ScopedStrDatabase = ScopedDatabase<String, String>;
        let db = ScopedStrDatabase::new(&env, "test")?;

        {
            let mut wtxn = env.write_txn()?;
            db.put(
                &mut wtxn,
                Some("main"),
                &"user1".to_string(),
                &"Alice (main)".to_string(),
            )?;
            wtxn.commit()?;
        }

        {
            let rtxn = env.read_txn()?;
            let value = db.get(&rtxn, Some("main"), &"user1".to_string())?;
            assert_eq!(value, Some("Alice (main)".to_string()));
        }

        // Clean up
        drop(env);
        std::fs::remove_dir_all(db_path).unwrap();

        Ok(())
    }

    #[test]
    fn test_encoding_compatibility() -> Result<(), ScopedDbError> {
        // Test that our manual encoding matches bincode encoding
        use bincode;

        let scope_hash = 0x12345678u32;
        let key_bytes = b"test_key";

        // Create our ScopedKey struct
        let scoped_key = ScopedKey {
            scope_hash,
            key: key_bytes.to_vec(),
        };

        // Encode with bincode
        let bincode_encoded = bincode::serialize(&scoped_key).unwrap();

        // Encode with our manual encoder
        let manual_encoded = ScopedBytesCodec::encode(scope_hash, key_bytes);

        // They should be identical
        assert_eq!(bincode_encoded, manual_encoded);

        // Test decoding
        let (decoded_hash, decoded_key) = ScopedBytesCodec::decode(&manual_encoded)?;
        assert_eq!(decoded_hash, scope_hash);
        assert_eq!(decoded_key, key_bytes);

        Ok(())
    }

    #[test]
    fn test_bytes_database() -> Result<(), ScopedDbError> {
        let db_path = "./test_bytes_db";
        if std::path::Path::new(db_path).exists() {
            std::fs::remove_dir_all(db_path).unwrap();
        }
        std::fs::create_dir_all(db_path).unwrap();

        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(10 * 1024 * 1024)
                .max_dbs(4)
                .open(db_path)?
        };

        let db = ScopedBytesKeyDatabase::<String>::new(&env, "test_bytes")?;

        {
            let mut wtxn = env.write_txn()?;
            db.put(&mut wtxn, Some("main"), b"key1", &"value1".to_string())?;
            db.put(&mut wtxn, None, b"key2", &"value2".to_string())?;
            wtxn.commit()?;
        }

        {
            let rtxn = env.read_txn()?;
            let value1 = db.get(&rtxn, Some("main"), b"key1")?;
            let value2 = db.get(&rtxn, None, b"key2")?;
            assert_eq!(value1, Some("value1".to_string()));
            assert_eq!(value2, Some("value2".to_string()));
        }

        // Clean up
        drop(env);
        std::fs::remove_dir_all(db_path).unwrap();

        Ok(())
    }

    #[test]
    fn test_fully_optimized_bytes_database() -> Result<(), ScopedDbError> {
        let db_path = "./test_pure_bytes_db";
        if std::path::Path::new(db_path).exists() {
            std::fs::remove_dir_all(db_path).unwrap();
        }
        std::fs::create_dir_all(db_path).unwrap();

        let env = unsafe {
            EnvOpenOptions::new()
                .map_size(10 * 1024 * 1024)
                .max_dbs(4)
                .open(db_path)?
        };

        let db = ScopedBytesDatabase::new(&env, "test_pure")?;

        {
            let mut wtxn = env.write_txn()?;
            // Test with binary data
            db.put(
                &mut wtxn,
                Some("binary"),
                b"\x00\x01\x02\x03",
                b"\xff\xfe\xfd\xfc",
            )?;
            db.put(&mut wtxn, None, b"default_key", b"default_value")?;
            wtxn.commit()?;
        }

        {
            let rtxn = env.read_txn()?;
            let binary_value = db.get(&rtxn, Some("binary"), b"\x00\x01\x02\x03")?;
            assert_eq!(binary_value, Some(&b"\xff\xfe\xfd\xfc"[..]));

            let default_value = db.get(&rtxn, None, b"default_key")?;
            assert_eq!(default_value, Some(&b"default_value"[..]));
        }

        // Clean up
        drop(env);
        std::fs::remove_dir_all(db_path).unwrap();

        Ok(())
    }
}

/// Specialized codec for byte-based scoped keys to match bincode encoding
#[doc(hidden)]
pub enum ScopedBytesCodec {}

impl ScopedBytesCodec {
    #[inline]
    pub fn encode(scope_hash: u32, key: &[u8]) -> Vec<u8> {
        // Total size: 4 (u32) + 8 (u64 length) + key.len()
        let mut output = Vec::with_capacity(12 + key.len());

        // 1. Encode scope_hash as u32 little-endian (4 bytes)
        output.extend_from_slice(&scope_hash.to_le_bytes());

        // 2. Encode key length as u64 little-endian (8 bytes) - to match bincode
        let key_len = key.len() as u64;
        output.extend_from_slice(&key_len.to_le_bytes());

        // 3. Encode key bytes
        output.extend_from_slice(key);

        output
    }

    #[inline]
    pub fn decode(bytes: &[u8]) -> Result<(u32, &[u8]), ScopedDbError> {
        if bytes.len() < 12 {
            return Err(ScopedDbError::Encoding(
                "Not enough bytes to decode scoped key".into(),
            ));
        }

        // 1. Decode scope_hash
        let scope_hash = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);

        // 2. Decode key length
        let key_len_bytes = &bytes[4..12];
        let key_len = u64::from_le_bytes(key_len_bytes.try_into().unwrap());

        // 3. Extract key
        let key_start = 12;
        let key_end = key_start + key_len as usize;
        if bytes.len() < key_end {
            return Err(ScopedDbError::Encoding("Not enough bytes for key".into()));
        }
        let key = &bytes[key_start..key_end];

        Ok((scope_hash, key))
    }
}

impl<'a> BytesEncode<'a> for ScopedBytesCodec {
    type EItem = (u32, &'a [u8]);

    fn bytes_encode(
        (scope_hash, key): &Self::EItem,
    ) -> Result<std::borrow::Cow<'a, [u8]>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(std::borrow::Cow::Owned(Self::encode(*scope_hash, key)))
    }
}

impl<'a> BytesDecode<'a> for ScopedBytesCodec {
    type DItem = (u32, &'a [u8]);

    fn bytes_decode(
        bytes: &'a [u8],
    ) -> Result<Self::DItem, Box<dyn std::error::Error + Send + Sync>> {
        Self::decode(bytes).map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
    }
}

/// Performance-optimized scoped database for byte slice keys with Redis-like isolation.
///
/// Provides the same complete scope isolation as `ScopedDatabase` but optimized for
/// applications using byte slice keys. This avoids serialization overhead for keys
/// while maintaining type safety for values.
#[derive(Debug)]
pub struct ScopedBytesKeyDatabase<V>
where
    V: Serialize + for<'de> Deserialize<'de> + 'static,
{
    db_scoped: HeedDatabase<ScopedBytesCodec, SerdeBincode<V>>,
    db_default: HeedDatabase<Bytes, SerdeBincode<V>>,
    scope_hasher: RwLock<ScopeHasher>,
    _phantom: PhantomData<V>,
}

impl<V> ScopedBytesKeyDatabase<V>
where
    V: Serialize + for<'de> Deserialize<'de> + 'static,
{
    pub fn new(env: &Env, name: &str) -> Result<Self, ScopedDbError> {
        let mut wtxn = env.write_txn()?;
        let db = Self::create(env, name, &mut wtxn)?;
        wtxn.commit()?;
        Ok(db)
    }

    /// Create a new ScopedBytesKeyDatabase with a provided transaction
    pub fn create(env: &Env, name: &str, txn: &mut RwTxn) -> Result<Self, ScopedDbError> {
        // Create database names from base name
        let default_name = format!("{}_default", name);
        let scoped_name = format!("{}_scoped", name);

        // Open databases
        let db_default = env
            .database_options()
            .types::<Bytes, SerdeBincode<V>>()
            .name(&default_name)
            .create(txn)?;

        let db_scoped = env
            .database_options()
            .types::<ScopedBytesCodec, SerdeBincode<V>>()
            .name(&scoped_name)
            .create(txn)?;

        Ok(Self {
            db_scoped,
            db_default,
            scope_hasher: RwLock::new(ScopeHasher::new()),
            _phantom: PhantomData,
        })
    }

    /// Insert a key-value pair into the database.
    pub fn put(
        &self,
        txn: &mut RwTxn<'_>,
        scope_name: Option<&str>,
        key: &[u8],
        value: &V,
    ) -> Result<(), ScopedDbError> {
        match scope_name {
            Some("") => Err(ScopedDbError::EmptyScopeDisallowed),
            Some(actual_scope) => {
                let mut hasher = self.scope_hasher.write().unwrap();
                let scope_hash = hasher.hash(actual_scope)?;

                self.db_scoped
                    .put(txn, &(scope_hash, key), value)
                    .map_err(ScopedDbError::from)
            }
            None => self
                .db_default
                .put(txn, key, value)
                .map_err(ScopedDbError::from),
        }
    }

    /// Get a value from the database.
    pub fn get<'txn>(
        &self,
        txn: &'txn RoTxn<'txn>,
        scope_name: Option<&str>,
        key: &[u8],
    ) -> Result<Option<V>, ScopedDbError> {
        match scope_name {
            Some("") => Err(ScopedDbError::EmptyScopeDisallowed),
            Some(actual_scope) => {
                let mut hasher = self.scope_hasher.write().unwrap();
                let scope_hash = hasher.hash(actual_scope)?;

                self.db_scoped
                    .get(txn, &(scope_hash, key))
                    .map_err(ScopedDbError::from)
            }
            None => self.db_default.get(txn, key).map_err(ScopedDbError::from),
        }
    }

    /// Delete a key-value pair from the database.
    pub fn delete(
        &self,
        txn: &mut RwTxn<'_>,
        scope_name: Option<&str>,
        key: &[u8],
    ) -> Result<bool, ScopedDbError> {
        match scope_name {
            Some("") => Err(ScopedDbError::EmptyScopeDisallowed),
            Some(actual_scope) => {
                let mut hasher = self.scope_hasher.write().unwrap();
                let scope_hash = hasher.hash(actual_scope)?;

                self.db_scoped
                    .delete(txn, &(scope_hash, key))
                    .map_err(ScopedDbError::from)
            }
            None => self
                .db_default
                .delete(txn, key)
                .map_err(ScopedDbError::from),
        }
    }

    /// Clear all entries within a specific scope or the default database.
    pub fn clear(
        &self,
        txn: &mut RwTxn<'_>,
        scope_name: Option<&str>,
    ) -> Result<(), ScopedDbError> {
        match scope_name {
            Some("") => Err(ScopedDbError::EmptyScopeDisallowed),
            Some(actual_scope) => {
                let mut hasher = self.scope_hasher.write().unwrap();
                let scope_hash = hasher.hash(actual_scope)?;

                // We need to collect entries to delete before modifying the database
                let mut entries_to_delete = Vec::new();

                // Use a temporary reference to the transaction for iteration
                let txn_ref = &*txn;
                for result in self.db_scoped.iter(txn_ref)? {
                    let ((entry_scope_hash, key), _value) = result?;
                    if entry_scope_hash == scope_hash {
                        // Clone the key to avoid lifetime issues
                        entries_to_delete.push((entry_scope_hash, key.to_vec()));
                    }
                }

                // Now delete the collected entries
                for (scope_hash, key) in entries_to_delete {
                    self.db_scoped.delete(txn, &(scope_hash, &key[..]))?;
                }

                Ok(())
            }
            None => self.db_default.clear(txn).map_err(ScopedDbError::from),
        }
    }

    /// Iterate over entries in a specific scope or the default database.
    pub fn iter<'txn>(
        &self,
        txn: &'txn RoTxn<'txn>,
        scope_name: Option<&str>,
    ) -> Result<
        Box<dyn Iterator<Item = Result<(&'txn [u8], V), ScopedDbError>> + 'txn>,
        ScopedDbError,
    > {
        match scope_name {
            Some("") => Err(ScopedDbError::EmptyScopeDisallowed),
            Some(actual_scope) => {
                let mut hasher = self.scope_hasher.write().unwrap();
                let scope_hash = hasher.hash(actual_scope)?;

                let iter = self
                    .db_scoped
                    .iter(txn)?
                    .filter_map(move |result| match result {
                        Ok(((entry_scope_hash, key), value)) => {
                            if entry_scope_hash == scope_hash {
                                Some(Ok((key, value)))
                            } else {
                                None
                            }
                        }
                        Err(e) => Some(Err(ScopedDbError::from(e))),
                    });
                Ok(Box::new(iter))
            }
            None => {
                let iter = self
                    .db_default
                    .iter(txn)?
                    .map(|result| result.map_err(ScopedDbError::from));
                Ok(Box::new(iter))
            }
        }
    }

    /// Iterate over a range of entries in a specific scope or the default database.
    pub fn range<'sbd_ref, 'txn_ref, 'bounds_ref, R>(
        &'sbd_ref self,
        txn: &'txn_ref RoTxn<'txn_ref>,
        scope_name: Option<&str>,
        range: &'bounds_ref R,
    ) -> Result<
        Box<dyn Iterator<Item = Result<(&'txn_ref [u8], V), ScopedDbError>> + 'txn_ref>,
        ScopedDbError,
    >
    where
        R: RangeBounds<&'bounds_ref [u8]> + 'bounds_ref,
    {
        match scope_name {
            Some("") => Err(ScopedDbError::EmptyScopeDisallowed),
            Some(actual_scope) => {
                let mut hasher = self.scope_hasher.write().unwrap();
                let scope_hash = hasher.hash(actual_scope)?;

                // Transform the range bounds to work with our (u32, &[u8]) key structure
                use std::ops::Bound;
                let transformed_start = match range.start_bound() {
                    Bound::Included(key) => Bound::Included((scope_hash, *key)),
                    Bound::Excluded(key) => Bound::Excluded((scope_hash, *key)),
                    Bound::Unbounded => Bound::Included((scope_hash, [].as_slice())),
                };

                let transformed_end = match range.end_bound() {
                    Bound::Included(key) => Bound::Included((scope_hash, *key)),
                    Bound::Excluded(key) => Bound::Excluded((scope_hash, *key)),
                    // For unbounded end, we use the next scope hash to ensure we don't
                    // include keys from other scopes
                    Bound::Unbounded => Bound::Excluded((scope_hash + 1, [].as_slice())),
                };

                let transformed_range = (transformed_start, transformed_end);

                let iter =
                    self.db_scoped
                        .range(txn, &transformed_range)?
                        .map(|result| match result {
                            Ok(((_, key), value)) => Ok((key, value)),
                            Err(e) => Err(ScopedDbError::from(e)),
                        });
                Ok(Box::new(iter))
            }
            None => {
                // Use adapter to convert RangeBounds<&[u8]> to RangeBounds<[u8]>
                let adapter = HeedRangeAdapter::new(range);
                let iter = self
                    .db_default
                    .range(txn, &adapter)?
                    .map(|result| match result {
                        Ok((key, value)) => Ok((key, value)),
                        Err(e) => Err(ScopedDbError::from(e)),
                    });
                Ok(Box::new(iter))
            }
        }
    }
}

impl<V> Clone for ScopedBytesKeyDatabase<V>
where
    V: Serialize + for<'de> Deserialize<'de> + 'static,
{
    fn clone(&self) -> Self {
        Self {
            db_scoped: self.db_scoped.clone(),
            db_default: self.db_default.clone(),
            scope_hasher: RwLock::new(ScopeHasher::new()), // Create fresh hasher
            _phantom: PhantomData,
        }
    }
}

/// Maximum performance scoped database for pure byte operations with Redis-like isolation.
///
/// Ideal for applications working directly with binary data, this database type
/// provides complete scope isolation while avoiding all serialization overhead.
/// Perfect for hash tables, binary protocols, or raw data storage.
#[derive(Debug)]
pub struct ScopedBytesDatabase {
    db_scoped: HeedDatabase<ScopedBytesCodec, Bytes>,
    db_default: HeedDatabase<Bytes, Bytes>,
    scope_hasher: RwLock<ScopeHasher>,
}

impl ScopedBytesDatabase {
    pub fn new(env: &Env, name: &str) -> Result<Self, ScopedDbError> {
        let mut wtxn = env.write_txn()?;
        let db = Self::create(env, name, &mut wtxn)?;
        wtxn.commit()?;
        Ok(db)
    }

    /// Create a new ScopedBytesDatabase with a provided transaction
    pub fn create(env: &Env, name: &str, txn: &mut RwTxn) -> Result<Self, ScopedDbError> {
        // Create database names from base name
        let default_name = format!("{}_default", name);
        let scoped_name = format!("{}_scoped", name);

        let db_default = env
            .database_options()
            .types::<Bytes, Bytes>()
            .name(&default_name)
            .create(txn)?;

        let db_scoped = env
            .database_options()
            .types::<ScopedBytesCodec, Bytes>()
            .name(&scoped_name)
            .create(txn)?;

        Ok(Self {
            db_scoped,
            db_default,
            scope_hasher: RwLock::new(ScopeHasher::new()),
        })
    }

    pub fn put(
        &self,
        txn: &mut RwTxn<'_>,
        scope_name: Option<&str>,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), ScopedDbError> {
        match scope_name {
            Some("") => Err(ScopedDbError::EmptyScopeDisallowed),
            Some(actual_scope) => {
                let mut hasher = self.scope_hasher.write().unwrap();
                let scope_hash = hasher.hash(actual_scope)?;

                self.db_scoped
                    .put(txn, &(scope_hash, key), value)
                    .map_err(ScopedDbError::from)
            }
            None => self
                .db_default
                .put(txn, key, value)
                .map_err(ScopedDbError::from),
        }
    }

    pub fn get<'txn>(
        &self,
        txn: &'txn RoTxn<'txn>,
        scope_name: Option<&str>,
        key: &[u8],
    ) -> Result<Option<&'txn [u8]>, ScopedDbError> {
        match scope_name {
            Some("") => Err(ScopedDbError::EmptyScopeDisallowed),
            Some(actual_scope) => {
                let mut hasher = self.scope_hasher.write().unwrap();
                let scope_hash = hasher.hash(actual_scope)?;

                self.db_scoped
                    .get(txn, &(scope_hash, key))
                    .map_err(ScopedDbError::from)
            }
            None => self.db_default.get(txn, key).map_err(ScopedDbError::from),
        }
    }

    /// Delete a key-value pair from the database.
    pub fn delete(
        &self,
        txn: &mut RwTxn<'_>,
        scope_name: Option<&str>,
        key: &[u8],
    ) -> Result<bool, ScopedDbError> {
        match scope_name {
            Some("") => Err(ScopedDbError::EmptyScopeDisallowed),
            Some(actual_scope) => {
                let mut hasher = self.scope_hasher.write().unwrap();
                let scope_hash = hasher.hash(actual_scope)?;

                self.db_scoped
                    .delete(txn, &(scope_hash, key))
                    .map_err(ScopedDbError::from)
            }
            None => self
                .db_default
                .delete(txn, key)
                .map_err(ScopedDbError::from),
        }
    }

    /// Clear all entries within a specific scope or the default database.
    pub fn clear(
        &self,
        txn: &mut RwTxn<'_>,
        scope_name: Option<&str>,
    ) -> Result<(), ScopedDbError> {
        match scope_name {
            Some("") => Err(ScopedDbError::EmptyScopeDisallowed),
            Some(actual_scope) => {
                let mut hasher = self.scope_hasher.write().unwrap();
                let scope_hash = hasher.hash(actual_scope)?;

                // We need to collect entries to delete before modifying the database
                let mut entries_to_delete = Vec::new();

                // Use a temporary reference to the transaction for iteration
                let txn_ref = &*txn;
                for result in self.db_scoped.iter(txn_ref)? {
                    let ((entry_scope_hash, key), _value) = result?;
                    if entry_scope_hash == scope_hash {
                        // Clone the key to avoid lifetime issues
                        entries_to_delete.push((entry_scope_hash, key.to_vec()));
                    }
                }

                // Now delete the collected entries
                for (scope_hash, key) in entries_to_delete {
                    self.db_scoped.delete(txn, &(scope_hash, &key[..]))?;
                }

                Ok(())
            }
            None => self.db_default.clear(txn).map_err(ScopedDbError::from),
        }
    }

    /// Iterate over entries in a specific scope or the default database.
    pub fn iter<'txn>(
        &self,
        txn: &'txn RoTxn<'txn>,
        scope_name: Option<&str>,
    ) -> Result<
        Box<dyn Iterator<Item = Result<(&'txn [u8], &'txn [u8]), ScopedDbError>> + 'txn>,
        ScopedDbError,
    > {
        match scope_name {
            Some("") => Err(ScopedDbError::EmptyScopeDisallowed),
            Some(actual_scope) => {
                let mut hasher = self.scope_hasher.write().unwrap();
                let scope_hash = hasher.hash(actual_scope)?;

                let iter = self
                    .db_scoped
                    .iter(txn)?
                    .filter_map(move |result| match result {
                        Ok(((entry_scope_hash, key), value)) => {
                            if entry_scope_hash == scope_hash {
                                Some(Ok((key, value)))
                            } else {
                                None
                            }
                        }
                        Err(e) => Some(Err(ScopedDbError::from(e))),
                    });
                Ok(Box::new(iter))
            }
            None => {
                let iter = self
                    .db_default
                    .iter(txn)?
                    .map(|result| result.map_err(ScopedDbError::from));
                Ok(Box::new(iter))
            }
        }
    }

    /// Iterate over a range of entries in a specific scope or the default database.
    pub fn range<'sbd_ref, 'txn_ref, 'bounds_ref, R>(
        &'sbd_ref self,
        txn: &'txn_ref RoTxn<'txn_ref>,
        scope_name: Option<&str>,
        range: &'bounds_ref R,
    ) -> Result<
        Box<
            dyn Iterator<Item = Result<(&'txn_ref [u8], &'txn_ref [u8]), ScopedDbError>> + 'txn_ref,
        >,
        ScopedDbError,
    >
    where
        R: RangeBounds<&'bounds_ref [u8]> + 'bounds_ref,
    {
        match scope_name {
            Some("") => Err(ScopedDbError::EmptyScopeDisallowed),
            Some(actual_scope) => {
                let mut hasher = self.scope_hasher.write().unwrap();
                let scope_hash = hasher.hash(actual_scope)?;

                // Transform the range bounds to work with our (u32, &[u8]) key structure
                use std::ops::Bound;
                let transformed_start = match range.start_bound() {
                    Bound::Included(key) => Bound::Included((scope_hash, *key)),
                    Bound::Excluded(key) => Bound::Excluded((scope_hash, *key)),
                    Bound::Unbounded => Bound::Included((scope_hash, [].as_slice())),
                };

                let transformed_end = match range.end_bound() {
                    Bound::Included(key) => Bound::Included((scope_hash, *key)),
                    Bound::Excluded(key) => Bound::Excluded((scope_hash, *key)),
                    // For unbounded end, we use the next scope hash to ensure we don't
                    // include keys from other scopes
                    Bound::Unbounded => Bound::Excluded((scope_hash + 1, [].as_slice())),
                };

                let transformed_range = (transformed_start, transformed_end);

                let iter =
                    self.db_scoped
                        .range(txn, &transformed_range)?
                        .map(|result| match result {
                            Ok(((_, key), value)) => Ok((key, value)),
                            Err(e) => Err(ScopedDbError::from(e)),
                        });
                Ok(Box::new(iter))
            }
            None => {
                // Use adapter to convert RangeBounds<&[u8]> to RangeBounds<[u8]>
                let adapter = HeedRangeAdapter::new(range);
                let iter = self
                    .db_default
                    .range(txn, &adapter)?
                    .map(|result| match result {
                        Ok((key, value)) => Ok((key, value)),
                        Err(e) => Err(ScopedDbError::from(e)),
                    });
                Ok(Box::new(iter))
            }
        }
    }
}
impl Clone for ScopedBytesDatabase {
    fn clone(&self) -> Self {
        Self {
            db_scoped: self.db_scoped.clone(),
            db_default: self.db_default.clone(),
            scope_hasher: RwLock::new(ScopeHasher::new()), // Create fresh hasher
        }
    }
}
