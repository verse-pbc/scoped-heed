use crate::{Scope, ScopedDbError};
use heed::types::SerdeBincode;
use heed::{Database as HeedDatabase, Env, RoTxn, RwTxn};

/// A centralized registry for managing scope metadata at the environment level.
///
/// The `GlobalScopeRegistry` provides a single source of truth for all scope names and
/// their hashes across the entire LMDB environment. This allows for a unified view of
/// all scopes and enables global operations like listing all scopes or pruning empty
/// scopes across all database instances.
///
/// # Example
///
/// ```rust,no_run
/// # use scoped_heed::{GlobalScopeRegistry, ScopedBytesDatabase, Scope, ScopedDbError};
/// # use heed::EnvOpenOptions;
/// # use std::sync::Arc;
/// # fn main() -> Result<(), ScopedDbError> {
/// # let env = unsafe { EnvOpenOptions::new().map_size(10*1024*1024).max_dbs(5).open("./db")? };
/// // Initialize the global registry
/// let mut wtxn = env.write_txn()?;
/// let global_registry = Arc::new(GlobalScopeRegistry::new(&env, &mut wtxn)?);
///
/// // Create databases with the shared registry
/// let db_users = ScopedBytesDatabase::create(&env, "users", &mut wtxn, Some(global_registry.clone()))?;
/// let db_posts = ScopedBytesDatabase::create(&env, "posts", &mut wtxn, Some(global_registry.clone()))?;
/// wtxn.commit()?;
///
/// // Add data to different scopes
/// let mut wtxn = env.write_txn()?;
/// let tenant1 = Scope::named("tenant1")?;
/// db_users.put(&mut wtxn, &tenant1, b"user1", b"alice")?;
/// db_posts.put(&mut wtxn, &tenant1, b"post1", b"hello world")?;
/// wtxn.commit()?;
///
/// // List all scopes in the environment
/// let rtxn = env.read_txn()?;
/// let all_scopes = global_registry.list_all_scopes(&rtxn)?;
/// // all_scopes contains [Scope::Default, Scope::Named { name: "tenant1", hash: ... }]
/// # Ok(())
/// # }
/// ```
/// Trait for database types that can check if a scope is empty
pub trait ScopeEmptinessChecker {
    /// Check if a scope is empty in this database
    fn is_scope_empty_in_db(&self, txn: &RoTxn, scope: &Scope) -> Result<bool, ScopedDbError>;
}

#[derive(Debug)]
pub struct GlobalScopeRegistry {
    metadata_db: HeedDatabase<SerdeBincode<u32>, SerdeBincode<String>>,
}

impl GlobalScopeRegistry {
    /// The name of the LMDB database used for global scope metadata
    pub const GLOBAL_METADATA_DB_NAME: &'static str = "__global_scope_metadata";

    /// Creates a new global scope registry.
    ///
    /// This method creates or opens the shared LMDB database for storing scope metadata.
    ///
    /// # Arguments
    ///
    /// * `env` - The LMDB environment
    /// * `txn` - A write transaction for the environment
    ///
    /// # Returns
    ///
    /// A new `GlobalScopeRegistry` instance
    pub fn new(env: &Env, txn: &mut RwTxn) -> Result<Self, ScopedDbError> {
        let metadata_db = env
            .database_options()
            .types::<SerdeBincode<u32>, SerdeBincode<String>>()
            .name(Self::GLOBAL_METADATA_DB_NAME)
            .create(txn)?;

        Ok(Self { metadata_db })
    }

    /// Registers a scope in the global metadata database.
    ///
    /// This method is automatically called by `ScopedDatabase` methods during write
    /// operations to ensure all used scopes are properly registered. You can also
    /// call it directly to register a scope before using it.
    ///
    /// # Arguments
    ///
    /// * `txn` - A write transaction
    /// * `scope` - The scope to register
    ///
    /// # Errors
    ///
    /// Returns an error if there's a hash collision between different scope names.
    pub fn register_scope(&self, txn: &mut RwTxn, scope: &Scope) -> Result<(), ScopedDbError> {
        if let Scope::Named { name, hash } = scope {
            // Check if this hash already exists
            if let Some(existing_name) = self.metadata_db.get(txn, hash)? {
                // If it exists but points to a different scope name, we have a collision
                if &existing_name != name {
                    return Err(ScopedDbError::InvalidInput(format!(
                        "Hash collision detected between '{}' and '{}'",
                        name, existing_name
                    )));
                }
            } else {
                // Register new scope in metadata database
                self.metadata_db.put(txn, hash, name)?;
            }
        }
        Ok(())
    }

    /// Gets the name of a scope by its hash.
    ///
    /// # Arguments
    ///
    /// * `txn` - A read transaction
    /// * `hash` - The hash of the scope
    ///
    /// # Returns
    ///
    /// The name of the scope if found, or `None` if not registered
    pub fn get_scope_name(&self, txn: &RoTxn, hash: &u32) -> Result<Option<String>, ScopedDbError> {
        self.metadata_db.get(txn, hash).map_err(ScopedDbError::from)
    }

    /// Looks up a scope's hash by its name.
    ///
    /// # Arguments
    ///
    /// * `txn` - A read transaction
    /// * `name` - The name of the scope
    ///
    /// # Returns
    ///
    /// The hash of the scope if found, or `None` if not registered
    pub fn lookup_scope_hash(&self, txn: &RoTxn, name: &str) -> Result<Option<u32>, ScopedDbError> {
        for result in self.metadata_db.iter(txn)? {
            let (hash, stored_name) = result?;
            if stored_name == name {
                return Ok(Some(hash));
            }
        }
        Ok(None)
    }

    /// Lists all scopes registered in the global metadata database.
    ///
    /// # Arguments
    ///
    /// * `txn` - A read transaction
    ///
    /// # Returns
    ///
    /// A vector of all registered scopes, including the default scope
    pub fn list_all_scopes(&self, txn: &RoTxn) -> Result<Vec<Scope>, ScopedDbError> {
        let mut scopes = Vec::new();

        // Always include the default scope
        scopes.push(Scope::Default);

        // Add all named scopes from the metadata database
        for result in self.metadata_db.iter(txn)? {
            let (hash, name) = result?;
            scopes.push(Scope::Named { name, hash });
        }

        Ok(scopes)
    }

    /// Checks if a scope is empty across multiple database instances.
    ///
    /// This is a utility method for checking if a scope is truly empty
    /// across different database instances. It takes a closure that performs
    /// the check for a specific database, allowing for customized logic.
    ///
    /// # Arguments
    ///
    /// * `txn` - A read transaction
    /// * `scope` - The scope to check
    /// * `is_scope_empty_in_db` - A closure that checks if the scope is empty in a specific database
    ///
    /// # Returns
    ///
    /// `true` if the scope is empty in all databases checked by the closure, `false` otherwise
    pub fn is_scope_empty<F>(
        &self,
        txn: &RoTxn,
        scope: &Scope,
        mut is_scope_empty_in_db: F,
    ) -> Result<bool, ScopedDbError>
    where
        F: FnMut(&RoTxn, &Scope) -> Result<bool, ScopedDbError>,
    {
        is_scope_empty_in_db(txn, scope)
    }

    /// Checks if a scope exists in the registry.
    ///
    /// # Arguments
    ///
    /// * `txn` - A read transaction
    /// * `scope` - The scope to check
    ///
    /// # Returns
    ///
    /// `true` if the scope is registered, `false` otherwise
    pub fn scope_exists(&self, txn: &RoTxn, scope: &Scope) -> Result<bool, ScopedDbError> {
        match scope {
            Scope::Default => Ok(true), // Default scope always exists
            Scope::Named { hash, .. } => Ok(self.metadata_db.get(txn, hash)?.is_some()),
        }
    }

    /// Unregisters a scope from the global metadata database.
    ///
    /// This method removes a scope's registration from the global registry.
    /// It should be used with caution, and only when you're sure the scope
    /// is empty across all databases using this registry.
    ///
    /// # Arguments
    ///
    /// * `txn` - A write transaction
    /// * `hash` - The hash of the scope to unregister
    ///
    /// # Returns
    ///
    /// `Ok(())` if the scope was successfully unregistered, or `Err` if an error occurred
    pub fn unregister_scope(&self, txn: &mut RwTxn, hash: &u32) -> Result<(), ScopedDbError> {
        // Check if the hash exists before attempting to delete
        if self.metadata_db.get(txn, hash)?.is_some() {
            self.metadata_db.delete(txn, hash)?;
            Ok(())
        } else {
            // Not an error if the scope doesn't exist
            Ok(())
        }
    }

    /// Prunes scopes that are empty across all provided database instances.
    ///
    /// This method provides a globally safe way to prune scope metadata by verifying
    /// that a scope is empty across all provided database instances before removing it.
    /// This ensures that scopes still in use by any database are preserved.
    ///
    /// # Arguments
    ///
    /// * `txn` - A write transaction
    /// * `databases` - A slice of objects implementing the ScopeEmptinessChecker trait
    ///
    /// # Returns
    ///
    /// The number of scopes that were pruned, or an error if one occurred.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use scoped_heed::{GlobalScopeRegistry, ScopedBytesDatabase, ScopedDatabase, ScopedDbError, Scope, ScopeEmptinessChecker};
    /// # use heed::EnvOpenOptions;
    /// # use std::sync::Arc;
    /// # fn main() -> Result<(), ScopedDbError> {
    /// # let env = unsafe { EnvOpenOptions::new().map_size(10*1024*1024).max_dbs(3).open("./db")? };
    /// # let mut wtxn = env.write_txn()?;
    /// # let registry = Arc::new(GlobalScopeRegistry::new(&env, &mut wtxn)?);
    /// # let db1: ScopedBytesDatabase = ScopedBytesDatabase::new_with_registry(&env, "db1", registry.clone())?;
    /// # let db2: ScopedDatabase<String, String> = ScopedDatabase::new_with_registry(&env, "db2", registry.clone())?;
    /// # wtxn.commit()?;
    ///
    /// let mut wtxn = env.write_txn()?;
    ///
    /// // Create array of database references implementing ScopeEmptinessChecker
    /// let databases: [&dyn ScopeEmptinessChecker; 2] = [&db1, &db2];
    ///
    /// // Prune scopes that are empty across all databases
    /// let pruned_count = registry.prune_globally_unused_scopes(&mut wtxn, &databases)?;
    /// println!("Pruned {} globally unused scopes", pruned_count);
    ///
    /// wtxn.commit()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn prune_globally_unused_scopes(
        &self,
        txn: &mut RwTxn,
        databases: &[&dyn ScopeEmptinessChecker],
    ) -> Result<usize, ScopedDbError> {
        if databases.is_empty() {
            return Ok(0);
        }

        let mut pruned_count = 0;
        let scopes = self.list_all_scopes(txn)?;

        // Skip the default scope - it's always needed
        for scope in scopes.iter().filter(|s| !matches!(s, Scope::Default)) {
            // Assume the scope is empty until we find otherwise
            let mut is_empty = true;

            // Check each database to see if the scope is empty
            for db in databases {
                if !db.is_scope_empty_in_db(txn, scope)? {
                    is_empty = false;
                    break;
                }
            }

            // If the scope is empty in all databases, unregister it
            if is_empty {
                if let Scope::Named { hash, .. } = scope {
                    self.unregister_scope(txn, hash)?;
                    pruned_count += 1;
                }
            }
        }

        Ok(pruned_count)
    }
}

impl Clone for GlobalScopeRegistry {
    fn clone(&self) -> Self {
        Self {
            metadata_db: self.metadata_db,
        }
    }
}
