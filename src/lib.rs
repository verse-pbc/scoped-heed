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
//! use scoped_heed::{scoped_database_options, ScopedDbError, Scope};
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
//! let tenant1 = Scope::named("tenant1")?;
//! let tenant2 = Scope::named("tenant2")?;
//! db.put(&mut wtxn, &tenant1, &"key1".to_string(), &"value1".to_string())?;
//! db.put(&mut wtxn, &tenant2, &"key1".to_string(), &"value2".to_string())?;
//! wtxn.commit()?;
//!
//! // Each scope is completely isolated
//! let rtxn = env.read_txn()?;
//! let val1 = db.get(&rtxn, &tenant1, &"key1".to_string())?; // Some("value1")
//! let val2 = db.get(&rtxn, &tenant2, &"key1".to_string())?; // Some("value2")
//! # Ok(())
//! # }
//! ```
//!
//! ## Database Types
//!
//! The library provides three database implementations optimized for different use cases:
//!
//! 1. **Generic Database** (`ScopedDatabase<K, V>`)
//!    - Supports any Serde-compatible types for keys and values
//!    - Most flexible option
//!    - Suitable for complex data structures
//!
//! 2. **Bytes Key Database** (`ScopedBytesKeyDatabase<V>`)
//!    - Uses raw byte slices for keys, serialized values
//!    - Optimized for byte-based keys (hashes, IDs)
//!    - ~38x faster key decoding than generic version
//!
//! 3. **Raw Bytes Database** (`ScopedBytesDatabase`)
//!    - Both keys and values are raw bytes
//!    - Maximum performance with zero serialization
//!    - ~1.8x faster writes than generic version
//!
//! ## Key Encoding
//!
//! Scoped entries use a 32-bit hash prefix for efficient scope identification:
//! - Default scope: keys are stored as-is
//! - Named scopes: `[scope_hash: 4 bytes][original_key_data]`

use serde::{Deserialize, Serialize};
use std::error::Error as StdError;
use std::fmt;

/// Iterator result type for generic database operations, returning key-value pairs
pub type IterResult<'txn, K, V> = Result<Box<dyn Iterator<Item = Result<(K, V), ScopedDbError>> + 'txn>, ScopedDbError>;

/// Iterator result type for bytes key database operations
pub type BytesKeyIterResult<'txn, V> = Result<Box<dyn Iterator<Item = Result<(&'txn [u8], V), ScopedDbError>> + 'txn>, ScopedDbError>;

/// Iterator result type for bytes database operations
pub type BytesIterResult<'txn> = Result<Box<dyn Iterator<Item = Result<(&'txn [u8], &'txn [u8]), ScopedDbError>> + 'txn>, ScopedDbError>;

pub mod builder;
pub mod scope;
pub mod global_registry;
pub mod scoped_database;
pub mod scoped_bytes_key_database;
pub mod scoped_bytes_database;
pub mod utils;

pub use builder::scoped_database_options;
pub use scope::Scope;
pub use global_registry::{GlobalScopeRegistry, ScopeEmptinessChecker};
pub use scoped_database::ScopedDatabase;
pub use scoped_bytes_key_database::ScopedBytesKeyDatabase;
pub use scoped_bytes_database::ScopedBytesDatabase;
pub use utils::{HeedRangeAdapter, ScopedBytesCodec};

/// Tuple type for scoped keys: (scope_hash, original_key)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScopedKey<K> {
    pub scope_hash: u32,
    pub key: K,
}

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