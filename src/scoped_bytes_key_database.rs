use heed::types::{Bytes, SerdeBincode};
use heed::{Database as HeedDatabase, Env, RoTxn, RwTxn};
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
use std::ops::RangeBounds;
use std::sync::Arc;

use crate::global_registry::{GlobalScopeRegistry, ScopeEmptinessChecker};
use crate::{BytesKeyIterResult, Scope, ScopedBytesCodec, ScopedDbError, utils::HeedRangeAdapter};

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
    global_registry: Arc<GlobalScopeRegistry>,
    _phantom: PhantomData<V>,
}

impl<V> ScopedBytesKeyDatabase<V>
where
    V: Serialize + for<'de> Deserialize<'de> + 'static,
{
    /// Create a new ScopedBytesKeyDatabase with a provided transaction
    ///
    /// Requires a global registry for scope metadata management.
    /// This method is intended to be called through the builder pattern.
    pub(crate) fn create(
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
    /// ```ignore
    /// # use scoped_heed::{ScopedBytesKeyDatabase, ScopedDbError, Scope, GlobalScopeRegistry};
    /// # use heed::EnvOpenOptions;
    /// # use std::sync::Arc;
    /// # fn main() -> Result<(), ScopedDbError> {
    /// # let env = unsafe { EnvOpenOptions::new().map_size(10*1024*1024).max_dbs(3).open("./db")? };
    /// # let mut wtxn = env.write_txn()?;
    /// # let registry = Arc::new(GlobalScopeRegistry::new(&env, &mut wtxn)?);
    /// # let db: ScopedBytesKeyDatabase<String> = ScopedBytesKeyDatabase::new(&env, "test", registry)?;
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

                // Use range-based approach to efficiently check for entries with this scope
                use std::ops::Bound;

                // Create a range that covers only entries with this scope hash
                let start_bound = Bound::Included((scope_hash, &[][..]));

                // End just before the next scope hash would begin, handling u32::MAX safely
                let end_bound = if scope_hash == u32::MAX {
                    // Special case - check up to the maximum possible key value
                    Bound::Included((scope_hash, &[0xFF][..]))
                } else {
                    // Normal case - use the next hash with empty key as exclusive upper bound
                    Bound::Excluded((scope_hash + 1, &[][..]))
                };

                let range = (start_bound, end_bound);

                // Just check if the range contains any entries
                let iter = self.db_scoped.range(txn, &range)?;
                for result in iter {
                    let ((entry_scope_hash, _), _) = result?;
                    if entry_scope_hash == scope_hash {
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
    /// # use scoped_heed::{ScopedBytesKeyDatabase, ScopedDbError, Scope, GlobalScopeRegistry};
    /// # use heed::EnvOpenOptions;
    /// # use std::sync::Arc;
    /// # fn main() -> Result<(), ScopedDbError> {
    /// # let env = unsafe { EnvOpenOptions::new().map_size(10*1024*1024).max_dbs(3).open("./db")? };
    /// # let mut wtxn = env.write_txn()?;
    /// # let registry = Arc::new(GlobalScopeRegistry::new(&env, &mut wtxn)?);
    /// # let db: ScopedBytesKeyDatabase<String> = ScopedBytesKeyDatabase::new(&env, "test", registry)?;
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

    /// Insert a key-value pair into the database.
    pub fn put(
        &self,
        txn: &mut RwTxn<'_>,
        scope: &Scope,
        key: &[u8],
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

                self.db_scoped
                    .put(txn, &(*hash, key), value)
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
    /// # use scoped_heed::{ScopedBytesKeyDatabase, ScopedDbError};
    /// # use heed::EnvOpenOptions;
    /// # fn main() -> Result<(), ScopedDbError> {
    /// # let env = unsafe { EnvOpenOptions::new().map_size(10*1024*1024).max_dbs(3).open("./db")? };
    /// # let db: ScopedBytesKeyDatabase<String> = ScopedBytesKeyDatabase::new(&env, "test")?;
    /// # let mut wtxn = env.write_txn()?;
    /// // Use the convenience method with Option<&str>
    /// db.put_with_name(&mut wtxn, Some("tenant1"), b"key1", &"value1".to_string())?;
    ///
    /// // Use None for the default scope
    /// db.put_with_name(&mut wtxn, None, b"key2", &"value2".to_string())?;
    /// # wtxn.commit()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn put_with_name(
        &self,
        txn: &mut RwTxn<'_>,
        scope_name: Option<&str>,
        key: &[u8],
        value: &V,
    ) -> Result<(), ScopedDbError> {
        let scope = Scope::from(scope_name);
        self.put(txn, &scope, key, value)
    }

    /// Get a value from the database.
    pub fn get<'txn>(
        &self,
        txn: &'txn RoTxn<'txn>,
        scope: &Scope,
        key: &[u8],
    ) -> Result<Option<V>, ScopedDbError> {
        match scope {
            Scope::Default => self.db_default.get(txn, key).map_err(ScopedDbError::from),
            Scope::Named { hash, .. } => self
                .db_scoped
                .get(txn, &(*hash, key))
                .map_err(ScopedDbError::from),
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
    /// # use scoped_heed::{ScopedBytesKeyDatabase, ScopedDbError};
    /// # use heed::EnvOpenOptions;
    /// # fn main() -> Result<(), ScopedDbError> {
    /// # let env = unsafe { EnvOpenOptions::new().map_size(10*1024*1024).max_dbs(3).open("./db")? };
    /// # let db: ScopedBytesKeyDatabase<String> = ScopedBytesKeyDatabase::new(&env, "test")?;
    /// # let rtxn = env.read_txn()?;
    /// // Use the convenience method with Option<&str>
    /// let value1 = db.get_with_name(&rtxn, Some("tenant1"), b"key1")?;
    ///
    /// // Use None for the default scope
    /// let value2 = db.get_with_name(&rtxn, None, b"key2")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn get_with_name<'txn>(
        &self,
        txn: &'txn RoTxn<'txn>,
        scope_name: Option<&str>,
        key: &[u8],
    ) -> Result<Option<V>, ScopedDbError> {
        let scope = Scope::from(scope_name);
        self.get(txn, &scope, key)
    }

    /// Delete a key-value pair from the database with a Scope enum.
    pub fn delete(
        &self,
        txn: &mut RwTxn<'_>,
        scope: &Scope,
        key: &[u8],
    ) -> Result<bool, ScopedDbError> {
        match scope {
            Scope::Default => self
                .db_default
                .delete(txn, key)
                .map_err(ScopedDbError::from),
            Scope::Named { hash, .. } => self
                .db_scoped
                .delete(txn, &(*hash, key))
                .map_err(ScopedDbError::from),
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
    /// # use scoped_heed::{ScopedBytesKeyDatabase, ScopedDbError};
    /// # use heed::EnvOpenOptions;
    /// # fn main() -> Result<(), ScopedDbError> {
    /// # let env = unsafe { EnvOpenOptions::new().map_size(10*1024*1024).max_dbs(3).open("./db")? };
    /// # let db: ScopedBytesKeyDatabase<String> = ScopedBytesKeyDatabase::new(&env, "test")?;
    /// # let mut wtxn = env.write_txn()?;
    /// // Use the convenience method with Option<&str>
    /// let was_deleted = db.delete_with_name(&mut wtxn, Some("tenant1"), b"key1")?;
    ///
    /// // Use None for the default scope
    /// let was_deleted = db.delete_with_name(&mut wtxn, None, b"key2")?;
    /// # wtxn.commit()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn delete_with_name(
        &self,
        txn: &mut RwTxn<'_>,
        scope_name: Option<&str>,
        key: &[u8],
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
    /// This method uses LMDB's efficient `delete_range` operation to:
    /// - Clear all entries with a matching scope hash in a single operation
    /// - Avoid the O(N) cost of iterating and collecting keys before deletion
    /// - Skip deserialization overhead for keys and values
    ///
    /// For large datasets, this provides orders of magnitude better performance compared
    /// to iterating through entries one by one.
    ///
    /// # Special Cases
    ///
    /// - For the `Default` scope, this delegates to heed's built-in `clear` method
    /// - For scopes with a hash of `u32::MAX`, special handling ensures all entries are properly cleared
    ///
    /// # Example
    ///
    /// ```ignore
    /// # use scoped_heed::{ScopedBytesKeyDatabase, Scope, ScopedDbError};
    /// # use heed::EnvOpenOptions;
    /// # fn main() -> Result<(), ScopedDbError> {
    /// # let env = unsafe { EnvOpenOptions::new().map_size(10*1024*1024).max_dbs(3).open("./db")? };
    /// # let db: ScopedBytesKeyDatabase<String> = ScopedBytesKeyDatabase::new(&env, "test")?;
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
                // Register the scope (ensures it's in the registry)
                self.register_scope(txn, scope)?;

                // Use delete_range to efficiently remove all keys with the specified hash prefix
                // Create a range that covers all entries for this scope hash
                use std::ops::Bound;

                // Start from the beginning of this scope (hash + empty key)
                let start_bound = Bound::Included((*hash, &[][..]));

                // End just before the next scope hash would begin, handling u32::MAX safely
                let end_bound = if *hash == u32::MAX {
                    // Special case - use maximum possible key value
                    Bound::Included((*hash, &[0xFF][..]))
                } else {
                    // Normal case - use the next hash with empty key as exclusive upper bound
                    Bound::Excluded((hash.wrapping_add(1), &[][..]))
                };

                let range = (start_bound, end_bound);

                // Use delete_range which is much more efficient than collecting and deleting
                self.db_scoped.delete_range(txn, &range)?;

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
    /// ```rust,ignore
    /// # use scoped_heed::{ScopedBytesKeyDatabase, ScopedDbError};
    /// # use heed::EnvOpenOptions;
    /// # fn main() -> Result<(), ScopedDbError> {
    /// # let env = unsafe { EnvOpenOptions::new().map_size(10*1024*1024).max_dbs(3).open("./db")? };
    /// # let db: ScopedBytesKeyDatabase<String> = ScopedBytesKeyDatabase::new(&env, "test")?;
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

    /// Iterate over entries in a specific scope or the default database.
    ///
    /// This method efficiently uses ranged iteration to retrieve only the entries
    /// belonging to the requested scope, rather than scanning the entire database.
    pub fn iter<'txn>(&self, txn: &'txn RoTxn<'txn>, scope: &Scope) -> BytesKeyIterResult<'txn, V> {
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

                // Use range-based iteration for better performance
                use std::ops::Bound;

                // Create a range that covers only entries with this scope hash
                let start_bound = Bound::Included((scope_hash, &[][..]));

                // End just before the next scope hash would begin, handling u32::MAX safely
                let end_bound = if scope_hash == u32::MAX {
                    // Special case - use maximum possible key value
                    Bound::Included((scope_hash, &[0xFF][..]))
                } else {
                    // Normal case - use the next hash with empty key as exclusive upper bound
                    Bound::Excluded((scope_hash + 1, &[][..]))
                };

                let range = (start_bound, end_bound);

                // Use range instead of iter + filter
                let iter =
                    self.db_scoped
                        .range(txn, &range)?
                        .filter_map(move |result| match result {
                            Ok(((entry_scope_hash, key), value)) => {
                                // Double-check scope hash (important for u32::MAX case)
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
    /// # use scoped_heed::{ScopedBytesKeyDatabase, ScopedDbError};
    /// # use heed::EnvOpenOptions;
    /// # fn main() -> Result<(), ScopedDbError> {
    /// # let env = unsafe { EnvOpenOptions::new().map_size(10*1024*1024).max_dbs(3).open("./db")? };
    /// # let db: ScopedBytesKeyDatabase<String> = ScopedBytesKeyDatabase::new(&env, "test")?;
    /// # let rtxn = env.read_txn()?;
    /// // Iterate over entries in a specific scope
    /// for result in db.iter_with_name(&rtxn, Some("tenant1"))? {
    ///     let (key, value) = result?;
    ///     println!("{:?}: {}", key, value);
    /// }
    ///
    /// // Iterate over entries in the default scope
    /// for result in db.iter_with_name(&rtxn, None)? {
    ///     let (key, value) = result?;
    ///     println!("{:?}: {}", key, value);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn iter_with_name<'txn>(
        &self,
        txn: &'txn RoTxn<'txn>,
        scope_name: Option<&str>,
    ) -> BytesKeyIterResult<'txn, V> {
        let scope = Scope::from(scope_name);
        self.iter(txn, &scope)
    }

    /// Iterate over a range of entries in a specific scope or the default database.
    pub fn range<'sbd_ref, 'txn_ref, 'bounds_ref, R>(
        &'sbd_ref self,
        txn: &'txn_ref RoTxn<'txn_ref>,
        scope: &Scope,
        range: &'bounds_ref R,
    ) -> BytesKeyIterResult<'txn_ref, V>
    where
        R: RangeBounds<&'bounds_ref [u8]> + 'bounds_ref,
    {
        match scope {
            Scope::Default => {
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
            Scope::Named { hash, .. } => {
                let scope_hash = *hash;

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
                    Bound::Unbounded => {
                        // Special case for u32::MAX to avoid overflow
                        if scope_hash == u32::MAX {
                            // Use a different approach for u32::MAX
                            Bound::Included((scope_hash, &[0xFF][..]))
                        } else {
                            Bound::Excluded((scope_hash + 1, [].as_slice()))
                        }
                    }
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
    /// # use scoped_heed::{ScopedBytesKeyDatabase, ScopedDbError};
    /// # use heed::EnvOpenOptions;
    /// # use std::ops::Bound;
    /// # fn main() -> Result<(), ScopedDbError> {
    /// # let env = unsafe { EnvOpenOptions::new().map_size(10*1024*1024).max_dbs(3).open("./db")? };
    /// # let db: ScopedBytesKeyDatabase<String> = ScopedBytesKeyDatabase::new(&env, "test")?;
    /// # let rtxn = env.read_txn()?;
    /// // Define range from "a" to "z" inclusive
    /// let range = (b"a".as_slice()..=b"z".as_slice());
    ///
    /// // Iterate over a range in a specific scope
    /// for result in db.range_with_name(&rtxn, Some("tenant1"), &range)? {
    ///     let (key, value) = result?;
    ///     println!("{:?}: {}", key, value);
    /// }
    ///
    /// // Iterate over a range in the default scope
    /// for result in db.range_with_name(&rtxn, None, &range)? {
    ///     let (key, value) = result?;
    ///     println!("{:?}: {}", key, value);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn range_with_name<'sbd_ref, 'txn_ref, 'bounds_ref, R>(
        &'sbd_ref self,
        txn: &'txn_ref RoTxn<'txn_ref>,
        scope_name: Option<&str>,
        range: &'bounds_ref R,
    ) -> BytesKeyIterResult<'txn_ref, V>
    where
        R: RangeBounds<&'bounds_ref [u8]> + 'bounds_ref,
    {
        let scope = Scope::from(scope_name);
        self.range(txn, &scope, range)
    }
}

impl<V> Clone for ScopedBytesKeyDatabase<V>
where
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

impl<V> ScopeEmptinessChecker for ScopedBytesKeyDatabase<V>
where
    V: Serialize + for<'de> Deserialize<'de> + 'static,
{
    fn is_scope_empty_in_db(&self, txn: &RoTxn, scope: &Scope) -> Result<bool, ScopedDbError> {
        self.is_scope_empty(txn, scope)
    }
}
