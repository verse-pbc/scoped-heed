use crate::{ScopedBytesDatabase, ScopedBytesKeyDatabase, ScopedDatabase, ScopedDbError};
use heed::{Env, RwTxn};
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

/// Builder for creating scoped databases with flexible type configurations
pub struct ScopedDatabaseOptions<'env> {
    env: &'env Env,
}

impl<'env> ScopedDatabaseOptions<'env> {
    /// Create a new options builder
    pub fn new(env: &'env Env) -> Self {
        Self { env }
    }

    /// Configure database with generic key and value types using SerdeBincode
    /// Keys and values are serialized using bincode
    pub fn types<K, V>(self) -> TypedOptions<'env, K, V>
    where
        K: Serialize + for<'de> Deserialize<'de> + Clone + 'static,
        V: Serialize + for<'de> Deserialize<'de> + 'static,
    {
        TypedOptions {
            env: self.env,
            name: None,
            _phantom: PhantomData,
        }
    }

    /// Configure database with raw byte slice keys (&[u8]) and serialized values
    /// Keys are stored as-is without serialization, values use bincode
    pub fn bytes_keys<V>(self) -> BytesKeysOptions<'env, V>
    where
        V: Serialize + for<'de> Deserialize<'de> + 'static,
    {
        BytesKeysOptions {
            env: self.env,
            name: None,
            _phantom: PhantomData,
        }
    }

    /// Configure database with raw byte slice keys and values (no serialization)
    /// Both keys and values are stored as raw bytes without any encoding
    pub fn raw_bytes(self) -> RawBytesOptions<'env> {
        RawBytesOptions {
            env: self.env,
            name: None,
        }
    }
}

/// Options for generic typed databases (serialized keys and values)
pub struct TypedOptions<'env, K, V> {
    env: &'env Env,
    name: Option<String>,
    _phantom: PhantomData<(K, V)>,
}

impl<K, V> TypedOptions<'_, K, V>
where
    K: Serialize + for<'de> Deserialize<'de> + Clone + 'static,
    V: Serialize + for<'de> Deserialize<'de> + 'static,
{
    /// Set the database name
    pub fn name(mut self, name: &str) -> Self {
        self.name = Some(name.to_string());
        self
    }

    /// Create the database with the current transaction
    pub fn create(self, txn: &mut RwTxn) -> Result<ScopedDatabase<K, V>, ScopedDbError> {
        let name = self
            .name
            .ok_or_else(|| ScopedDbError::InvalidInput("Database name is required".into()))?;

        ScopedDatabase::create(self.env, &name, txn)
    }
}

/// Options for databases with byte keys and serialized values
pub struct BytesKeysOptions<'env, V> {
    env: &'env Env,
    name: Option<String>,
    _phantom: PhantomData<V>,
}

impl<V> BytesKeysOptions<'_, V>
where
    V: Serialize + for<'de> Deserialize<'de> + 'static,
{
    /// Set the database name
    pub fn name(mut self, name: &str) -> Self {
        self.name = Some(name.to_string());
        self
    }

    /// Create the database with the current transaction
    pub fn create(self, txn: &mut RwTxn) -> Result<ScopedBytesKeyDatabase<V>, ScopedDbError> {
        let name = self
            .name
            .ok_or_else(|| ScopedDbError::InvalidInput("Database name is required".into()))?;

        ScopedBytesKeyDatabase::create(self.env, &name, txn)
    }
}

/// Options for pure raw bytes databases (no serialization)
pub struct RawBytesOptions<'env> {
    env: &'env Env,
    name: Option<String>,
}

impl RawBytesOptions<'_> {
    /// Set the database name
    pub fn name(mut self, name: &str) -> Self {
        self.name = Some(name.to_string());
        self
    }

    /// Create the database with the current transaction
    pub fn create(self, txn: &mut RwTxn) -> Result<ScopedBytesDatabase, ScopedDbError> {
        let name = self
            .name
            .ok_or_else(|| ScopedDbError::InvalidInput("Database name is required".into()))?;

        ScopedBytesDatabase::create(self.env, &name, txn)
    }
}

/// Module-level function to create scoped database options
pub fn scoped_database_options(env: &Env) -> ScopedDatabaseOptions {
    ScopedDatabaseOptions::new(env)
}
