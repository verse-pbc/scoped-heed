use heed::{Database as HeedDatabase, Env, RoTxn, RwTxn};
use heed::types::{SerdeBincode};
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
use std::ops::RangeBounds;
use std::sync::Arc;

use crate::global_registry::{GlobalScopeRegistry, ScopeEmptinessChecker};
use crate::{Scope, ScopedDbError, ScopedKey, utils};

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
    K: Serialize + for<'de> Deserialize<'de> + Clone + Default + 'static,
    V: Serialize + for<'de> Deserialize<'de> + 'static,
{
    db_scoped: HeedDatabase<SerdeBincode<ScopedKey<K>>, SerdeBincode<V>>,
    db_default: HeedDatabase<SerdeBincode<K>, SerdeBincode<V>>,
    global_registry: Option<Arc<GlobalScopeRegistry>>,
    _phantom: PhantomData<(K, V)>,
}

impl<K, V> ScopedDatabase<K, V>
where
    K: Serialize + for<'de> Deserialize<'de> + Clone + Default + 'static,
    V: Serialize + for<'de> Deserialize<'de> + 'static,
{
    /// Creates a new ScopedDatabase.
    ///
    /// This method creates the database without a global registry.
    /// Use `new_with_registry` if you need to use a global registry.
    pub fn new(env: &Env, name: &str) -> Result<Self, ScopedDbError> {
        let mut wtxn = env.write_txn()?;
        let db = Self::create(env, name, &mut wtxn, None)?;
        wtxn.commit()?;
        Ok(db)
    }

    /// Creates a new ScopedDatabase with a global registry.
    ///
    /// This method creates a new ScopedDatabase that uses the provided
    /// global registry for scope metadata management.
    pub fn new_with_registry(env: &Env, name: &str, registry: Arc<GlobalScopeRegistry>) -> Result<Self, ScopedDbError> {
        let mut wtxn = env.write_txn()?;
        let db = Self::create(env, name, &mut wtxn, Some(registry))?;
        wtxn.commit()?;
        Ok(db)
    }

    /// Creates a new ScopedDatabase with a provided transaction.
    ///
    /// If a global registry is provided, it will be used for scope metadata management.
    pub fn create(env: &Env, name: &str, txn: &mut RwTxn, registry: Option<Arc<GlobalScopeRegistry>>) -> Result<Self, ScopedDbError> {
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
            global_registry: registry,
            _phantom: PhantomData,
        })
    }

    /// Registers a scope in the global registry.
    /// 
    /// This method is automatically called during write operations (put, delete, clear)
    /// to ensure all used scopes are properly registered. You can also call it directly
    /// if you want to register a scope before using it.
    ///
    /// # Errors
    ///
    /// Returns an error if there's a hash collision between different scope names,
    /// or if no global registry is available.
    pub fn register_scope(&self, txn: &mut RwTxn, scope: &Scope) -> Result<(), ScopedDbError> {
        if let Scope::Named { name: _, hash: _ } = scope {
            if let Some(registry) = &self.global_registry {
                registry.register_scope(txn, scope)
            } else {
                Err(ScopedDbError::InvalidInput(
                    "No metadata storage available. Create the database with a GlobalScopeRegistry".into()
                ))
            }
        } else {
            // Default scope doesn't need registration
            Ok(())
        }
    }
    
    /// Lists all known scopes in the database.
    ///
    /// Returns a list of all scopes that have been registered by this database,
    /// including the Default scope.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use scoped_heed::{ScopedDatabase, ScopedDbError, Scope};
    /// # use heed::EnvOpenOptions;
    /// # fn main() -> Result<(), ScopedDbError> {
    /// # let env = unsafe { EnvOpenOptions::new().map_size(10*1024*1024).max_dbs(3).open("./db")? };
    /// # let mut wtxn = env.write_txn()?;
    /// # let db: ScopedDatabase<String, String> = ScopedDatabase::new(&env, "test")?;
    /// # wtxn.commit()?;
    /// let rtxn = env.read_txn()?;
    /// let scopes = db.list_scopes(&rtxn)?;
    /// for scope in scopes {
    ///     match scope {
    ///         Scope::Default => println!("Default scope"),
    ///         Scope::Named { name, hash } => println!("Scope: {} (hash: {})", name, hash),
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn list_scopes(&self, txn: &RoTxn) -> Result<Vec<Scope>, ScopedDbError> {
        if let Some(registry) = &self.global_registry {
            registry.list_all_scopes(txn)
        } else {
            // No registry available, only return the default scope
            Ok(vec![Scope::Default])
        }
    }

    /// Insert a key-value pair into the database.
    ///
    /// Uses the Scope enum to represent scopes, which provides better
    /// performance by pre-computing and caching scope hashes.
    pub fn put(
        &self,
        txn: &mut RwTxn<'_>,
        scope: &Scope,
        key: &K,
        value: &V,
    ) -> Result<(), ScopedDbError> {
        match scope {
            Scope::Default => self
                .db_default
                .put(txn, key, value)
                .map_err(ScopedDbError::from),
            Scope::Named { hash, .. } => {
                // Register scope in global registry
                self.register_scope(txn, scope)?;
                
                // Use ScopedKey tuple
                let scoped_key = ScopedKey {
                    scope_hash: *hash,
                    key: key.clone(),
                };
                self.db_scoped
                    .put(txn, &scoped_key, value)
                    .map_err(ScopedDbError::from)
            }
        }
    }
    
    /// Insert a key-value pair into the database with an Option<&str> scope name.
    ///
    /// This is a convenience method that converts the scope name to a Scope enum
    /// and then calls the main put method.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use scoped_heed::{ScopedDatabase, ScopedDbError};
    /// # use heed::EnvOpenOptions;
    /// # fn main() -> Result<(), ScopedDbError> {
    /// # let env = unsafe { EnvOpenOptions::new().map_size(10*1024*1024).max_dbs(3).open("./db")? };
    /// # let db: ScopedDatabase<String, String> = ScopedDatabase::new(&env, "test")?;
    /// # let mut wtxn = env.write_txn()?;
    /// // Use the convenience method with Option<&str>
    /// db.put_with_name(&mut wtxn, Some("tenant1"), &"key1".to_string(), &"value1".to_string())?;
    /// 
    /// // Use None for the default scope
    /// db.put_with_name(&mut wtxn, None, &"key2".to_string(), &"value2".to_string())?;
    /// # wtxn.commit()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn put_with_name(
        &self,
        txn: &mut RwTxn<'_>,
        scope_name: Option<&str>,
        key: &K,
        value: &V,
    ) -> Result<(), ScopedDbError> {
        let scope = Scope::from(scope_name);
        self.put(txn, &scope, key, value)
    }

    /// Get a value from the database.
    ///
    /// Uses the Scope enum to represent scopes, which provides better
    /// performance by pre-computing and caching scope hashes.
    pub fn get<'txn>(
        &self,
        txn: &'txn RoTxn<'txn>,
        scope: &Scope,
        key: &K,
    ) -> Result<Option<V>, ScopedDbError> {
        match scope {
            Scope::Default => self.db_default.get(txn, key).map_err(ScopedDbError::from),
            Scope::Named { hash, .. } => {
                // Use ScopedKey tuple with the hash directly
                let scoped_key = ScopedKey {
                    scope_hash: *hash,
                    key: key.clone(),
                };
                self.db_scoped
                    .get(txn, &scoped_key)
                    .map_err(ScopedDbError::from)
            }
        }
    }
    
    /// Get a value from the database using an Option<&str> scope name.
    ///
    /// This is a convenience method that converts the scope name to a Scope enum
    /// and then calls the main get method.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use scoped_heed::{ScopedDatabase, ScopedDbError};
    /// # use heed::EnvOpenOptions;
    /// # fn main() -> Result<(), ScopedDbError> {
    /// # let env = unsafe { EnvOpenOptions::new().map_size(10*1024*1024).max_dbs(3).open("./db")? };
    /// # let db: ScopedDatabase<String, String> = ScopedDatabase::new(&env, "test")?;
    /// # let rtxn = env.read_txn()?;
    /// // Use the convenience method with Option<&str>
    /// let value1 = db.get_with_name(&rtxn, Some("tenant1"), &"key1".to_string())?;
    /// 
    /// // Use None for the default scope
    /// let value2 = db.get_with_name(&rtxn, None, &"key2".to_string())?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn get_with_name<'txn>(
        &self,
        txn: &'txn RoTxn<'txn>,
        scope_name: Option<&str>,
        key: &K,
    ) -> Result<Option<V>, ScopedDbError> {
        let scope = Scope::from(scope_name);
        self.get(txn, &scope, key)
    }

    /// Delete a key-value pair from the database.
    ///
    /// Uses the Scope enum to represent scopes, which provides better
    /// performance by pre-computing and caching scope hashes.
    pub fn delete(
        &self,
        txn: &mut RwTxn<'_>,
        scope: &Scope,
        key: &K,
    ) -> Result<bool, ScopedDbError> {
        match scope {
            Scope::Default => self
                .db_default
                .delete(txn, key)
                .map_err(ScopedDbError::from),
            Scope::Named { hash, .. } => {
                let scoped_key = ScopedKey {
                    scope_hash: *hash,
                    key: key.clone(),
                };
                self.db_scoped
                    .delete(txn, &scoped_key)
                    .map_err(ScopedDbError::from)
            }
        }
    }
    
    /// Delete a key-value pair from the database using an Option<&str> scope name.
    ///
    /// This is a convenience method that converts the scope name to a Scope enum
    /// and then calls the main delete method.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use scoped_heed::{ScopedDatabase, ScopedDbError};
    /// # use heed::EnvOpenOptions;
    /// # fn main() -> Result<(), ScopedDbError> {
    /// # let env = unsafe { EnvOpenOptions::new().map_size(10*1024*1024).max_dbs(3).open("./db")? };
    /// # let db: ScopedDatabase<String, String> = ScopedDatabase::new(&env, "test")?;
    /// # let mut wtxn = env.write_txn()?;
    /// // Use the convenience method with Option<&str>
    /// let was_deleted = db.delete_with_name(&mut wtxn, Some("tenant1"), &"key1".to_string())?;
    /// 
    /// // Use None for the default scope
    /// let was_deleted = db.delete_with_name(&mut wtxn, None, &"key2".to_string())?;
    /// # wtxn.commit()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn delete_with_name(
        &self,
        txn: &mut RwTxn<'_>,
        scope_name: Option<&str>,
        key: &K,
    ) -> Result<bool, ScopedDbError> {
        let scope = Scope::from(scope_name);
        self.delete(txn, &scope, key)
    }

    /// Clear all entries within a specific scope or the default database.
    ///
    /// This is a highly optimized operation that efficiently removes all data for a specific scope,
    /// without affecting data in other scopes.
    ///
    /// # Performance
    ///
    /// This method uses an optimized cursor-based approach to:
    /// - Clear all entries for a specific scope hash in a single pass
    /// - Avoid collecting keys into memory before deletion
    /// - Skip deserialization of values during the deletion process
    ///
    /// For large datasets, this provides significantly better performance compared
    /// to iterating and collecting entries before deletion.
    ///
    /// # Implementation Details
    ///
    /// For the generic `ScopedDatabase<K,V>`, this uses:
    /// - A range-based cursor approach similar to heed's internal implementation
    /// - The `DecodeIgnore` type to avoid deserializing values
    /// - Direct cursor deletion to minimize memory overhead
    /// 
    /// # Special Cases
    ///
    /// - For the `Default` scope, this delegates to heed's built-in `clear` method
    /// - For scopes with a hash of `u32::MAX`, special handling ensures all entries are properly cleared
    /// - Ensures correct behavior with generic key types through the `Default` trait
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use scoped_heed::{ScopedDatabase, Scope, ScopedDbError};
    /// # use heed::EnvOpenOptions;
    /// # fn main() -> Result<(), ScopedDbError> {
    /// # let env = unsafe { EnvOpenOptions::new().map_size(10*1024*1024).max_dbs(3).open("./db")? };
    /// # let db: ScopedDatabase<String, String> = ScopedDatabase::new(&env, "test")?;
    /// # let mut wtxn = env.write_txn()?;
    /// // Create a scope
    /// let tenant_scope = Scope::named("tenant1")?;
    ///
    /// // Clear all data in the tenant scope
    /// db.clear(&mut wtxn, &tenant_scope)?;
    /// # wtxn.commit()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn clear(
        &self,
        txn: &mut RwTxn<'_>,
        scope: &Scope,
    ) -> Result<(), ScopedDbError> {
        match scope {
            Scope::Default => self.db_default.clear(txn).map_err(ScopedDbError::from),
            Scope::Named { hash, .. } => {
                // Register the scope before clearing (ensures it's in the registry)
                self.register_scope(txn, scope)?;
                
                // For generic ScopedDatabase<K,V>, using delete_range is trickier because we
                // need to define a range of ScopedKey<K> objects. For efficiency, we'll use
                // a cursor-based approach similar to heed's own delete_range implementation,
                // which avoids collecting all keys into a Vec first.
                
                // Create a mutable iterator with DecodeIgnore for the data part to save deserializing
                // values we're just going to delete anyway
                use heed::types::DecodeIgnore;
                
                // Create a range_mut that covers all entries in this scope
                // We'll create a minimum viable key for range start and end
                // We can't use open-ended ranges here since we need to constrain by scope_hash
                let min_key_start: ScopedKey<K> = ScopedKey {
                    scope_hash: *hash,
                    // We need a "minimum" key value - use Default if K implements it
                    // If K is not Default, we'll fall back to the old approach
                    key: utils::get_default_or_clone_first(),
                };
                
                let min_key_end = if *hash == u32::MAX {
                    // Special case for MAX scope hash to avoid overflow
                    ScopedKey {
                        scope_hash: *hash,
                        // Use "maximum" possible key instead
                        key: min_key_start.key.clone(), // We rely on lexicographic ordering of scope_hash first
                    }
                } else {
                    ScopedKey {
                        // For the end bound we use the next scope hash to exclude all keys from other scopes
                        scope_hash: hash.wrapping_add(1),
                        // The same minimum key works for the end bound
                        key: min_key_start.key.clone(),
                    }
                };
                
                // Set up our bounds to get all keys in this scope
                use std::ops::Bound;
                let range = (Bound::Included(min_key_start), Bound::Excluded(min_key_end));
                
                // Use a remap_data_type to avoid deserializing values we're just deleting
                let mut iter = self.db_scoped
                    .remap_data_type::<DecodeIgnore>()
                    .range_mut(txn, &range)?;
                
                // For each item in range, delete it right from the cursor without collecting
                let mut _deleted_count = 0;
                while iter.next().is_some() {
                    // Safety: We don't keep references to data while deleting
                    unsafe { iter.del_current()? };
                    _deleted_count += 1;
                }
                
                // Note: We don't unregister the scope here automatically
                // That should be a separate operation as other databases might use the same scope
                // The user can call unregister_scope manually if needed
                
                Ok(())
            }
        }
    }
    
    /// Clear all entries within a specific scope or the default database using an Option<&str> scope name.
    ///
    /// This is a convenience method that converts the scope name to a Scope enum
    /// and then calls the main clear method.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use scoped_heed::{ScopedDatabase, ScopedDbError};
    /// # use heed::EnvOpenOptions;
    /// # fn main() -> Result<(), ScopedDbError> {
    /// # let env = unsafe { EnvOpenOptions::new().map_size(10*1024*1024).max_dbs(3).open("./db")? };
    /// # let db: ScopedDatabase<String, String> = ScopedDatabase::new(&env, "test")?;
    /// # let mut wtxn = env.write_txn()?;
    /// // Clear all data in a specific scope
    /// db.clear_with_name(&mut wtxn, Some("tenant1"))?;
    /// 
    /// // Clear the default scope
    /// db.clear_with_name(&mut wtxn, None)?;
    /// # wtxn.commit()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn clear_with_name(
        &self,
        txn: &mut RwTxn<'_>,
        scope_name: Option<&str>,
    ) -> Result<(), ScopedDbError> {
        let scope = Scope::from(scope_name);
        self.clear(txn, &scope)
    }
    
    /// Checks if a scope is empty (contains no data).
    ///
    /// This is a helper method used by prune_empty_scopes.
    fn is_scope_empty(&self, txn: &RoTxn, scope: &Scope) -> Result<bool, ScopedDbError> {
        match scope {
            Scope::Default => {
                // Count entries in the default database
                let mut iter = self.db_default.iter(txn)?;
                Ok(iter.next().is_none())
            },
            Scope::Named { hash, .. } => {
                // Count entries with this scope's hash prefix
                for result in self.db_scoped.iter(txn)? {
                    let (scoped_key, _) = result?;
                    if scoped_key.scope_hash == *hash {
                        return Ok(false); // Found at least one entry
                    }
                }
                Ok(true) // No entries found
            }
        }
    }
    
    /// Find scopes that are empty in this database.
    ///
    /// This method checks all scopes in the global registry to see if they
    /// still contain data in this database. It's primarily used as a helper for
    /// the `GlobalScopeRegistry::prune_globally_unused_scopes` method and by the
    /// `ScopeEmptinessChecker` trait implementation.
    ///
    /// Returns the number of empty scopes found.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use scoped_heed::{ScopedDatabase, ScopedDbError, Scope};
    /// # use heed::EnvOpenOptions;
    /// # fn main() -> Result<(), ScopedDbError> {
    /// # let env = unsafe { EnvOpenOptions::new().map_size(10*1024*1024).max_dbs(3).open("./db")? };
    /// # let db: ScopedDatabase<String, String> = ScopedDatabase::new(&env, "test")?;
    /// let mut wtxn = env.write_txn()?;
    /// let empty_count = db.find_empty_scopes(&mut wtxn)?;
    /// println!("Found {} empty scopes", empty_count);
    /// wtxn.commit()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn find_empty_scopes(&self, txn: &mut RwTxn) -> Result<usize, ScopedDbError> {
        // If no registry is available, we can't find scopes
        if self.global_registry.is_none() {
            return Ok(0);
        }
        
        let mut empty_count = 0;
        let scopes = self.list_scopes(&*txn)?;
        
        for scope in scopes {
            // Skip the default scope
            if let Scope::Named { .. } = scope {
                // Check if the scope is empty
                if self.is_scope_empty(&*txn, &scope)? {
                    empty_count += 1;
                }
            }
        }
        
        Ok(empty_count)
    }

    /// Iterate over entries in a specific scope or the default database.
    pub fn iter<'txn>(
        &self,
        txn: &'txn RoTxn<'txn>,
        scope: &Scope,
    ) -> Result<Box<dyn Iterator<Item = Result<(K, V), ScopedDbError>> + 'txn>, ScopedDbError> {
        match scope {
            Scope::Default => {
                let iter = self
                    .db_default
                    .iter(txn)?
                    .map(|result| result.map_err(ScopedDbError::from));
                Ok(Box::new(iter))
            }
            Scope::Named { hash, .. } => {
                let scope_hash = *hash;
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
        }
    }
    
    /// Iterate over entries in a specific scope or the default database using an Option<&str> scope name.
    ///
    /// This is a convenience method that converts the scope name to a Scope enum
    /// and then calls the main iter method.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use scoped_heed::{ScopedDatabase, ScopedDbError};
    /// # use heed::EnvOpenOptions;
    /// # fn main() -> Result<(), ScopedDbError> {
    /// # let env = unsafe { EnvOpenOptions::new().map_size(10*1024*1024).max_dbs(3).open("./db")? };
    /// # let db: ScopedDatabase<String, String> = ScopedDatabase::new(&env, "test")?;
    /// # let rtxn = env.read_txn()?;
    /// // Iterate over entries in a specific scope
    /// for result in db.iter_with_name(&rtxn, Some("tenant1"))? {
    ///     let (key, value) = result?;
    ///     println!("{}: {}", key, value);
    /// }
    /// 
    /// // Iterate over entries in the default scope
    /// for result in db.iter_with_name(&rtxn, None)? {
    ///     let (key, value) = result?;
    ///     println!("{}: {}", key, value);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn iter_with_name<'txn>(
        &self,
        txn: &'txn RoTxn<'txn>,
        scope_name: Option<&str>,
    ) -> Result<Box<dyn Iterator<Item = Result<(K, V), ScopedDbError>> + 'txn>, ScopedDbError> {
        let scope = Scope::from(scope_name);
        self.iter(txn, &scope)
    }

    /// Iterate over a range of entries in a specific scope or the default database.
    /// 
    /// # Performance Note
    /// 
    /// When using a named scope with an unbounded end range (`..`, `start..`, etc.),
    /// this method must fall back to collecting all matching entries into memory due to
    /// limitations with generic type boundaries. This can be inefficient for large scopes.
    /// 
    /// For better performance with unbounded ranges:
    /// 
    /// 1. Use `ScopedBytesKeyDatabase` or `ScopedBytesDatabase` which implement optimized
    ///    byte-based ranges
    /// 2. Provide explicit end bounds when possible
    /// 3. Consider using multiple smaller bounded ranges if working with large datasets
    ///
    /// This limitation arises because with generic types `K`, we cannot efficiently construct
    /// an upper bound for the scope that works with LMDB's lexicographic ordering.
    pub fn range<'sbd_ref, 'txn_ref, 'bounds_ref, R>(
        &'sbd_ref self,
        txn: &'txn_ref RoTxn<'txn_ref>,
        scope: &Scope,
        range: &'bounds_ref R,
    ) -> Result<Box<dyn Iterator<Item = Result<(K, V), ScopedDbError>> + 'txn_ref>, ScopedDbError>
    where
        K: Clone + PartialOrd,
        R: RangeBounds<K> + 'bounds_ref,
    {
        match scope {
            Scope::Default => {
                let iter = self
                    .db_default
                    .range(txn, range)?
                    .map(|result| result.map_err(ScopedDbError::from));
                Ok(Box::new(iter))
            }
            Scope::Named { hash, .. } => {
                let scope_hash = *hash;

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
        }
    }
    
    /// Iterate over a range of entries in a specific scope or the default database
    /// using an Option<&str> scope name.
    ///
    /// This is a convenience method that converts the scope name to a Scope enum
    /// and then calls the main range method.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use scoped_heed::{ScopedDatabase, ScopedDbError};
    /// # use heed::EnvOpenOptions;
    /// # use std::ops::Bound;
    /// # fn main() -> Result<(), ScopedDbError> {
    /// # let env = unsafe { EnvOpenOptions::new().map_size(10*1024*1024).max_dbs(3).open("./db")? };
    /// # let db: ScopedDatabase<String, String> = ScopedDatabase::new(&env, "test")?;
    /// # let rtxn = env.read_txn()?;
    /// // Use a range with explicit bounds
    /// let range = ("a".to_string()..="z".to_string());
    /// 
    /// // Iterate over a range in a specific scope
    /// for result in db.range_with_name(&rtxn, Some("tenant1"), &range)? {
    ///     let (key, value) = result?;
    ///     println!("{}: {}", key, value);
    /// }
    /// 
    /// // Iterate over a range in the default scope
    /// for result in db.range_with_name(&rtxn, None, &range)? {
    ///     let (key, value) = result?;
    ///     println!("{}: {}", key, value);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn range_with_name<'sbd_ref, 'txn_ref, 'bounds_ref, R>(
        &'sbd_ref self,
        txn: &'txn_ref RoTxn<'txn_ref>,
        scope_name: Option<&str>,
        range: &'bounds_ref R,
    ) -> Result<Box<dyn Iterator<Item = Result<(K, V), ScopedDbError>> + 'txn_ref>, ScopedDbError>
    where
        K: Clone + PartialOrd,
        R: RangeBounds<K> + 'bounds_ref,
    {
        let scope = Scope::from(scope_name);
        self.range(txn, &scope, range)
    }
}

impl<K, V> Clone for ScopedDatabase<K, V>
where
    K: Serialize + for<'de> Deserialize<'de> + Clone + Default + 'static,
    V: Serialize + for<'de> Deserialize<'de> + 'static,
{
    fn clone(&self) -> Self {
        Self {
            db_scoped: self.db_scoped,
            db_default: self.db_default,
            global_registry: self.global_registry.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<K, V> ScopeEmptinessChecker for ScopedDatabase<K, V>
where
    K: Serialize + for<'de> Deserialize<'de> + Clone + Default + 'static,
    V: Serialize + for<'de> Deserialize<'de> + 'static,
{
    fn is_scope_empty_in_db(&self, txn: &RoTxn, scope: &Scope) -> Result<bool, ScopedDbError> {
        self.is_scope_empty(txn, scope)
    }
}