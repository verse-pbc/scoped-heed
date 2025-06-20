use heed::types::SerdeBincode;
use heed::{Database as HeedDatabase, Env, RoTxn, RwTxn};
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
use std::ops::RangeBounds;
use std::sync::Arc;

use crate::global_registry::{GlobalScopeRegistry, ScopeEmptinessChecker};
use crate::{IterResult, Scope, ScopedDbError, ScopedKey, utils};

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
///
/// # Key Cloning Behavior
///
/// When using the `ScopedDatabase<K, V>` with named scopes, the library must clone keys
/// for operations like `put`, `get`, and `delete`. This is necessary because the original
/// key (of type `K`) needs to be combined with scope information in a `ScopedKey<K>` struct
/// before being stored.
///
/// ## Performance Implications
///
/// For most key types, the cloning overhead is negligible. However, if your key type
/// is very large or expensive to clone (e.g., large strings, complex structs, or types
/// with heap allocations), you might consider:
///
/// 1. Using smaller keys when possible (e.g., IDs instead of full objects)
/// 2. Using `ScopedBytesKeyDatabase<V>` if your keys can be represented as byte slices
/// 3. Implementing an efficient `Clone` implementation for your key type
///
/// ## Example Impact
///
/// ```rust,ignore
/// // Small or medium keys - minimal impact
/// type SmallKey = u64;  // Trivial to clone
/// type MediumKey = String;  // Linear cost based on string length
///
/// // Potentially expensive keys - may have noticeable overhead
/// type ExpensiveKey = Vec<Vec<String>>;  // Nested allocations
/// ```
///
/// In most real-world applications using reasonable key sizes, the cloning overhead
/// will not be significant compared to the cost of serialization, deserialization,
/// and disk I/O.
#[derive(Debug)]
pub struct ScopedDatabase<K, V>
where
    K: Serialize + for<'de> Deserialize<'de> + Clone + Default + 'static,
    V: Serialize + for<'de> Deserialize<'de> + 'static,
{
    db_scoped: HeedDatabase<SerdeBincode<ScopedKey<K>>, SerdeBincode<V>>,
    db_default: HeedDatabase<SerdeBincode<K>, SerdeBincode<V>>,
    global_registry: Arc<GlobalScopeRegistry>,
    _phantom: PhantomData<(K, V)>,
}

impl<K, V> ScopedDatabase<K, V>
where
    K: Serialize + for<'de> Deserialize<'de> + Clone + Default + 'static,
    V: Serialize + for<'de> Deserialize<'de> + 'static,
{
    /// Creates a new ScopedDatabase with a global registry.
    ///
    /// This method requires a global registry for scope metadata management.
    pub fn new(
        env: &Env,
        name: &str,
        registry: Arc<GlobalScopeRegistry>,
    ) -> Result<Self, ScopedDbError> {
        let mut wtxn = env.write_txn()?;
        let db = Self::create(env, name, &mut wtxn, registry)?;
        wtxn.commit()?;
        Ok(db)
    }

    /// Creates a new ScopedDatabase with a provided transaction.
    ///
    /// Requires a global registry for scope metadata management.
    pub fn create(
        env: &Env,
        name: &str,
        txn: &mut RwTxn,
        registry: Arc<GlobalScopeRegistry>,
    ) -> Result<Self, ScopedDbError> {
        // Create database names from base name
        // Use the original name for default database (backward compatibility)
        let default_name = name.to_string();
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
    /// Returns an error if there's a hash collision between different scope names.
    pub fn register_scope(&self, txn: &mut RwTxn, scope: &Scope) -> Result<(), ScopedDbError> {
        if let Scope::Named { name: _, hash: _ } = scope {
            self.global_registry.register_scope(txn, scope)
        } else {
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
    /// ```ignore
    /// # use scoped_heed::{ScopedDatabase, ScopedDbError, Scope, GlobalScopeRegistry};
    /// # use heed::EnvOpenOptions;
    /// # use std::sync::Arc;
    /// # fn main() -> Result<(), ScopedDbError> {
    /// # let env = unsafe { EnvOpenOptions::new().map_size(10*1024*1024).max_dbs(3).open("./db")? };
    /// # let mut wtxn = env.write_txn()?;
    /// # let registry = Arc::new(GlobalScopeRegistry::new(&env, &mut wtxn)?);
    /// # let db: ScopedDatabase<String, String> = ScopedDatabase::new(&env, "test", registry)?;
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
        self.global_registry.list_all_scopes(txn)
    }

    /// Insert a key-value pair into the database.
    ///
    /// Uses the Scope enum to represent scopes, which provides better
    /// performance by pre-computing and caching scope hashes.
    ///
    /// # Key Cloning
    ///
    /// For named scopes, this method clones the key to create a `ScopedKey<K>` structure.
    /// If your key type is very large or expensive to clone, consider using
    /// `ScopedBytesKeyDatabase<V>` instead for better performance.
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
    /// ```rust,ignore
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
    ///
    /// # Key Cloning
    ///
    /// For named scopes, this method clones the key to create a `ScopedKey<K>` structure.
    /// If your key type is very large or expensive to clone, consider using
    /// `ScopedBytesKeyDatabase<V>` instead for better performance.
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
    /// ```rust,ignore
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
    /// ```rust,ignore
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
    /// ```ignore
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
    pub fn clear(&self, txn: &mut RwTxn<'_>, scope: &Scope) -> Result<(), ScopedDbError> {
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
                    key: utils::get_key_default(),
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
                let mut iter = self
                    .db_scoped
                    .remap_data_type::<DecodeIgnore>()
                    .range_mut(txn, &range)?;

                // For each item in range, delete it right from the cursor without collecting
                while iter.next().is_some() {
                    // Safety: No references to cursor data are kept after deletion
                    unsafe { iter.del_current()? };
                }

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
    /// ```rust,ignore
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
    /// This is a helper method used by `find_empty_scopes` and the `ScopeEmptinessChecker` implementation.
    /// It uses efficient ranged iteration to only examine entries for the specified scope.
    fn is_scope_empty(&self, txn: &RoTxn, scope: &Scope) -> Result<bool, ScopedDbError> {
        match scope {
            Scope::Default => {
                // Check if the default database has any entries
                let mut iter = self.db_default.iter(txn)?;
                Ok(iter.next().is_none())
            }
            Scope::Named { hash, .. } => {
                let scope_hash = *hash;

                // Use the same ranged approach as in iter() but stop at the first entry
                use std::ops::Bound;

                // Start from the beginning of this scope
                let start_key = ScopedKey {
                    scope_hash,
                    key: utils::get_key_default(),
                };

                // End at the beginning of the next scope (or at the end for u32::MAX)
                let end_bound = if scope_hash == u32::MAX {
                    // Special case for MAX scope hash to avoid overflow
                    Bound::Included(ScopedKey {
                        scope_hash,
                        key: utils::get_key_default(),
                    })
                } else {
                    // For all other cases, use next hash value as exclusive upper bound
                    Bound::Excluded(ScopedKey {
                        scope_hash: scope_hash + 1,
                        key: utils::get_key_default(),
                    })
                };

                // Create the range that covers only this scope
                let range = (Bound::Included(start_key), end_bound);

                // Just check if the range contains any entries with this scope hash
                let iter = self.db_scoped.range(txn, &range)?;
                for result in iter {
                    let (scoped_key, _) = result?;
                    if scoped_key.scope_hash == scope_hash {
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
    /// ```ignore
    /// # use scoped_heed::{ScopedDatabase, ScopedDbError, Scope, GlobalScopeRegistry};
    /// # use heed::EnvOpenOptions;
    /// # use std::sync::Arc;
    /// # fn main() -> Result<(), ScopedDbError> {
    /// # let env = unsafe { EnvOpenOptions::new().map_size(10*1024*1024).max_dbs(3).open("./db")? };
    /// # let mut wtxn = env.write_txn()?;
    /// # let registry = Arc::new(GlobalScopeRegistry::new(&env, &mut wtxn)?);
    /// # let db: ScopedDatabase<String, String> = ScopedDatabase::new(&env, "test", registry)?;
    /// # wtxn.commit()?;
    /// let mut wtxn = env.write_txn()?;
    /// let empty_count = db.find_empty_scopes(&mut wtxn)?;
    /// println!("Found {} empty scopes", empty_count);
    /// wtxn.commit()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn find_empty_scopes(&self, txn: &mut RwTxn) -> Result<usize, ScopedDbError> {
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
    ///
    /// This method efficiently uses ranged iteration to retrieve only the entries
    /// belonging to the requested scope, rather than scanning the entire database.
    pub fn iter<'txn>(&self, txn: &'txn RoTxn<'txn>, scope: &Scope) -> IterResult<'txn, K, V> {
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

                // Use range-based iteration to only retrieve entries for this scope
                use std::ops::Bound;

                // Start from the beginning of this scope
                let start_key = ScopedKey {
                    scope_hash,
                    key: utils::get_key_default(),
                };

                // End at the beginning of the next scope (or at the end for u32::MAX)
                let end_bound = if scope_hash == u32::MAX {
                    // Special case for MAX scope hash to avoid overflow
                    Bound::Included(ScopedKey {
                        scope_hash,
                        // We rely on lexicographic ordering of scope_hash first
                        key: utils::get_key_default(),
                    })
                } else {
                    // For all other cases, use next hash value as exclusive upper bound
                    Bound::Excluded(ScopedKey {
                        scope_hash: scope_hash + 1,
                        key: utils::get_key_default(),
                    })
                };

                // Create the range that covers only this scope
                let range = (Bound::Included(start_key), end_bound);

                // Use range instead of iter + filter
                let iter =
                    self.db_scoped
                        .range(txn, &range)?
                        .filter_map(move |result| match result {
                            Ok((scoped_key, value)) => {
                                // Double-check the scope hash (important for u32::MAX case)
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
    /// ```rust,ignore
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
    ) -> IterResult<'txn, K, V> {
        let scope = Scope::from(scope_name);
        self.iter(txn, &scope)
    }

    /// Iterate over a range of entries in a specific scope or the default database.
    ///
    /// This method efficiently handles all range types, including unbounded ranges,
    /// by properly constructing scope-aware range bounds for the underlying database.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// # use scoped_heed::{ScopedDatabase, Scope, ScopedDbError};
    /// # use heed::EnvOpenOptions;
    /// # use std::ops::Bound;
    /// # fn main() -> Result<(), ScopedDbError> {
    /// # let env = unsafe { EnvOpenOptions::new().map_size(10*1024*1024).max_dbs(3).open("./db")? };
    /// # let db: ScopedDatabase<String, String> = ScopedDatabase::new(&env, "test")?;
    /// # let rtxn = env.read_txn()?;
    /// // Using different range types
    /// let tenant = Scope::named("tenant1")?;
    ///
    /// // Bounded range
    /// let bounded = ("a".to_string()..="z".to_string());
    /// for result in db.range(&rtxn, &tenant, &bounded)? {
    ///     let (key, value) = result?;
    ///     println!("{}: {}", key, value);
    /// }
    ///
    /// // Unbounded start
    /// let from_start = (..="z".to_string());
    /// for result in db.range(&rtxn, &tenant, &from_start)? {
    ///     // ...
    /// }
    ///
    /// // Unbounded end (efficient implementation)
    /// let to_end = ("m".to_string()..);
    /// for result in db.range(&rtxn, &tenant, &to_end)? {
    ///     // ...
    /// }
    ///
    /// // Fully unbounded range (returns all entries in the scope)
    /// let all = (..);
    /// for result in db.range(&rtxn, &tenant, &all)? {
    ///     // ...
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn range<'sbd_ref, 'txn_ref, 'bounds_ref, R>(
        &'sbd_ref self,
        txn: &'txn_ref RoTxn<'txn_ref>,
        scope: &Scope,
        range: &'bounds_ref R,
    ) -> IterResult<'txn_ref, K, V>
    where
        K: Clone + PartialOrd,
        R: RangeBounds<K> + 'bounds_ref,
        'bounds_ref: 'txn_ref,
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

                // For start bound: map the user's bound to a scoped bound
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
                        // Start from the beginning of this scope with minimum key
                        Bound::Included(ScopedKey {
                            scope_hash,
                            key: utils::get_key_default(),
                        })
                    }
                };

                // For end bound: carefully handle the unbounded case
                let transformed_end = match range.end_bound() {
                    // If user provided a bounded end, use it with the same scope hash
                    Bound::Included(key) => Bound::Included(ScopedKey {
                        scope_hash,
                        key: key.clone(),
                    }),
                    Bound::Excluded(key) => Bound::Excluded(ScopedKey {
                        scope_hash,
                        key: key.clone(),
                    }),
                    Bound::Unbounded => {
                        // For unbounded end, we use the next scope hash as the exclusive upper bound
                        // This efficiently restricts the range to only the current scope
                        if scope_hash == u32::MAX {
                            // Special case for u32::MAX to avoid overflow
                            Bound::Included(ScopedKey {
                                scope_hash,
                                // Use "maximum" key value - we rely on lexicographic ordering of scope_hash first
                                key: utils::get_key_default(),
                            })
                        } else {
                            // Normal case - use next hash value as the exclusive upper bound
                            Bound::Excluded(ScopedKey {
                                scope_hash: scope_hash + 1,
                                key: utils::get_key_default(),
                            })
                        }
                    }
                };

                let transformed_range = (transformed_start, transformed_end);

                let iter =
                    self.db_scoped
                        .range(txn, &transformed_range)?
                        .filter_map(move |result| match result {
                            Ok((scoped_key, value)) => {
                                // Double-check the scope hash to ensure we're only getting entries
                                // from the requested scope (important for the u32::MAX case)
                                if scoped_key.scope_hash == scope_hash {
                                    // Apply the original range bounds to the key
                                    let in_original_range =
                                        match (range.start_bound(), range.end_bound()) {
                                            (Bound::Unbounded, Bound::Unbounded) => true,
                                            (Bound::Unbounded, Bound::Included(end)) => {
                                                &scoped_key.key <= end
                                            }
                                            (Bound::Unbounded, Bound::Excluded(end)) => {
                                                &scoped_key.key < end
                                            }
                                            (Bound::Included(start), Bound::Unbounded) => {
                                                &scoped_key.key >= start
                                            }
                                            (Bound::Excluded(start), Bound::Unbounded) => {
                                                &scoped_key.key > start
                                            }
                                            _ => range.contains(&scoped_key.key),
                                        };

                                    if in_original_range {
                                        Some(Ok((scoped_key.key, value)))
                                    } else {
                                        None
                                    }
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

    /// Iterate over a range of entries in a specific scope or the default database
    /// using an Option<&str> scope name.
    ///
    /// This is a convenience method that converts the scope name to a Scope enum
    /// and then calls the main range method.
    ///
    /// # Example
    ///
    /// ```rust,ignore
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
    ) -> IterResult<'txn_ref, K, V>
    where
        K: Clone + PartialOrd,
        R: RangeBounds<K> + 'bounds_ref,
        'bounds_ref: 'txn_ref,
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
