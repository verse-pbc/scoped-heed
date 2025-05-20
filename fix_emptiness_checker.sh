#\!/bin/bash

# Fix ScopedBytesDatabase implementation
cat > /tmp/scoped_bytes_database_emptiness_checker.txt << 'INNER'
impl ScopeEmptinessChecker for ScopedBytesDatabase {
    fn is_scope_empty_in_db(&self, txn: &RoTxn, scope: &Scope) -> Result<bool, ScopedDbError> {
        match scope {
            Scope::Default => {
                // Check if the default database has any entries
                let iter = self.db_default.iter(txn)?;
                Ok(iter.next().is_none())
            }
            Scope::Named { hash, .. } => {
                // Use a prefix based on the hash to find entries
                let mut prefix = Vec::with_capacity(4);
                prefix.extend_from_slice(&hash.to_be_bytes());
                let iter = self.db_scoped.prefix_iter(txn, &prefix)?;
                Ok(iter.next().is_none())
            }
        }
    }
}
INNER

# Fix ScopedBytesKeyDatabase implementation
cat > /tmp/scoped_bytes_key_database_emptiness_checker.txt << 'INNER'
impl<V> ScopeEmptinessChecker for ScopedBytesKeyDatabase<V>
where
    V: Serialize + for<'de> Deserialize<'de> + 'static,
{
    fn is_scope_empty_in_db(&self, txn: &RoTxn, scope: &Scope) -> Result<bool, ScopedDbError> {
        match scope {
            Scope::Default => {
                // Check if the default database has any entries
                let iter = self.db_default.iter(txn)?;
                Ok(iter.next().is_none())
            }
            Scope::Named { hash, .. } => {
                // Use a prefix based on the hash to find entries
                let mut prefix = Vec::with_capacity(4);
                prefix.extend_from_slice(&hash.to_be_bytes());
                let iter = self.db_scoped.prefix_iter(txn, &prefix)?;
                Ok(iter.next().is_none())
            }
        }
    }
}
INNER

# Fix ScopedDatabase implementation
cat > /tmp/scoped_database_emptiness_checker.txt << 'INNER'
impl<K, V> ScopeEmptinessChecker for ScopedDatabase<K, V>
where
    K: Serialize + for<'de> Deserialize<'de> + Clone + Default + 'static,
    V: Serialize + for<'de> Deserialize<'de> + 'static,
{
    fn is_scope_empty_in_db(&self, txn: &RoTxn, scope: &Scope) -> Result<bool, ScopedDbError> {
        match scope {
            Scope::Default => {
                // Check if the default database has any entries
                let iter = self.db_default.iter(txn)?;
                Ok(iter.next().is_none())
            }
            Scope::Named { hash, .. } => {
                // Use a prefix based on the hash to find entries
                let mut prefix = Vec::with_capacity(4);
                prefix.extend_from_slice(&hash.to_be_bytes());
                let iter = self.db_scoped.prefix_iter(txn, &prefix)?;
                Ok(iter.next().is_none())
            }
        }
    }
}
INNER

# Apply the fixes to each file
sed -i '' -e '/^impl ScopeEmptinessChecker for ScopedBytesDatabase {/,/^}/ {
    s/^impl ScopeEmptinessChecker for ScopedBytesDatabase {.*$/'"$(cat /tmp/scoped_bytes_database_emptiness_checker.txt | sed -e 's/[\&$]/\\&/g')"'/
}' src/scoped_bytes_database.rs

sed -i '' -e '/^impl<V> ScopeEmptinessChecker for ScopedBytesKeyDatabase<V>/,/^}/ {
    s/^impl<V> ScopeEmptinessChecker for ScopedBytesKeyDatabase<V>.*$/'"$(cat /tmp/scoped_bytes_key_database_emptiness_checker.txt | sed -e 's/[\&$]/\\&/g')"'/
}' src/scoped_bytes_key_database.rs

sed -i '' -e '/^impl<K, V> ScopeEmptinessChecker for ScopedDatabase<K, V>/,/^}/ {
    s/^impl<K, V> ScopeEmptinessChecker for ScopedDatabase<K, V>.*$/'"$(cat /tmp/scoped_database_emptiness_checker.txt | sed -e 's/[\&$]/\\&/g')"'/
}' src/scoped_database.rs

