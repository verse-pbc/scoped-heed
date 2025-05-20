use crate::{ScopedBytesDatabase, ScopedBytesKeyDatabase, ScopedDatabase, ScopedDbError, GlobalScopeRegistry};
use std::sync::Arc;
use heed::{Env, RwTxn};
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;

/// Builder for creating scoped databases with flexible type configurations
pub struct ScopedDatabaseOptions<'env> {
    env: &'env Env,
    global_registry: Arc<GlobalScopeRegistry>,
}

impl<'env> ScopedDatabaseOptions<'env> {
    /// Create a new options builder
    pub fn new(env: &'env Env, global_registry: Arc<GlobalScopeRegistry>) -> Self {
        Self { env, global_registry }
    }
    
    /// Alias for backward compatibility
    pub fn with_registry(self, _registry: Arc<GlobalScopeRegistry>) -> Self {
        // Registry is already provided at construction time
        self
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
            global_registry: self.global_registry,
            scope_registry: None,
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
            global_registry: self.global_registry,
            scope_registry: None,
            _phantom: PhantomData,
        }
    }

    /// Configure database with raw byte slice keys and values (no serialization)
    /// Both keys and values are stored as raw bytes without any encoding
    pub fn raw_bytes(self) -> RawBytesOptions<'env> {
        RawBytesOptions {
            env: self.env,
            name: None,
            global_registry: self.global_registry,
            scope_registry: None,
        }
    }
}

/// Options for generic typed databases (serialized keys and values)
pub struct TypedOptions<'env, K, V> {
    env: &'env Env,
    name: Option<String>,
    global_registry: Arc<GlobalScopeRegistry>,
    scope_registry: Option<Arc<RwLock<ScopeRegistry>>>,
    _phantom: PhantomData<(K, V)>,
}

impl<K, V> TypedOptions<'_, K, V>
where
    K: Serialize + for<'de> Deserialize<'de> + Clone + Default + 'static,
    V: Serialize + for<'de> Deserialize<'de> + 'static,
{
    /// Set the database name
    pub fn name(mut self, name: &str) -> Self {
        self.name = Some(name.to_string());
        self
    }
    
    /// Set a custom scope registry instance
    pub fn with_registry(mut self, registry: Arc<RwLock<ScopeRegistry>>) -> Self {
        self.scope_registry = Some(registry);
        self
    }

    /// Create the database with the current transaction
    pub fn create(self, txn: &mut RwTxn) -> Result<ScopedDatabase<K, V>, ScopedDbError> {
        let name = self
            .name
            .ok_or_else(|| ScopedDbError::InvalidInput("Database name is required".into()))?;

        if let Some(scope_registry) = self.scope_registry {
            ScopedDatabase::create_internal(self.env, &name, txn, scope_registry, self.global_registry)
        } else {
            // Create a new registry if none was provided
            let registry = Arc::new(RwLock::new(ScopeRegistry::new()));
            ScopedDatabase::create_internal(self.env, &name, txn, registry, self.global_registry)
        }
    }
}

/// Options for databases with byte keys and serialized values
pub struct BytesKeysOptions<'env, V> {
    env: &'env Env,
    name: Option<String>,
    global_registry: Arc<GlobalScopeRegistry>,
    scope_registry: Option<Arc<RwLock<ScopeRegistry>>>,
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
    
    /// Set a custom scope registry instance
    pub fn with_registry(mut self, registry: Arc<RwLock<ScopeRegistry>>) -> Self {
        self.scope_registry = Some(registry);
        self
    }

    /// Create the database with the current transaction
    pub fn create(self, txn: &mut RwTxn) -> Result<ScopedBytesKeyDatabase<V>, ScopedDbError> {
        let name = self
            .name
            .ok_or_else(|| ScopedDbError::InvalidInput("Database name is required".into()))?;

        // Check if we have an existing create function that takes a registry
        match crate::scoped_bytes_key_database::ScopedBytesKeyDatabase::create(self.env, &name, txn, self.registry) {
            Ok(db) => Ok(db),
            Err(e) => Err(e)
        }
    }
}

/// Options for pure raw bytes databases (no serialization)
pub struct RawBytesOptions<'env> {
    env: &'env Env,
    name: Option<String>,
    global_registry: Arc<GlobalScopeRegistry>,
    scope_registry: Option<Arc<RwLock<ScopeRegistry>>>,
}

impl RawBytesOptions<'_> {
    /// Set the database name
    pub fn name(mut self, name: &str) -> Self {
        self.name = Some(name.to_string());
        self
    }
    
    /// Set a custom scope registry instance
    pub fn with_registry(mut self, registry: Arc<RwLock<ScopeRegistry>>) -> Self {
        self.scope_registry = Some(registry);
        self
    }

    /// Create the database with the current transaction
    pub fn create(self, txn: &mut RwTxn) -> Result<ScopedBytesDatabase, ScopedDbError> {
        let name = self
            .name
            .ok_or_else(|| ScopedDbError::InvalidInput("Database name is required".into()))?;

        // Check if we have an existing create function that takes a registry
        match crate::scoped_bytes_database::ScopedBytesDatabase::create(self.env, &name, txn, self.registry) {
            Ok(db) => Ok(db),
            Err(e) => Err(e)
        }
    }
}

/// Module-level function to create scoped database options
pub fn scoped_database_options(env: &Env, global_registry: Arc<GlobalScopeRegistry>) -> ScopedDatabaseOptions {
    ScopedDatabaseOptions::new(env, global_registry)
}
