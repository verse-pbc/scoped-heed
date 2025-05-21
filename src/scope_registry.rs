use std::collections::HashMap;
use std::hash::Hasher;
use crate::ScopedDbError;

/// Manages scope hashes to avoid hash collisions.
#[derive(Debug)]
pub struct ScopeRegistry {
    scope_to_hash: HashMap<String, u32>,
    hash_to_scope: HashMap<u32, String>,
}

impl ScopeRegistry {
    /// Create a new ScopeRegistry instance
    pub fn new() -> Self {
        Self {
            scope_to_hash: HashMap::new(),
            hash_to_scope: HashMap::new(),
        }
    }

    /// Compute the hash for a scope name, ensuring no collisions
    ///
    /// # Arguments
    ///
    /// * `scope` - The scope name to hash
    ///
    /// # Returns
    ///
    /// The 32-bit hash value associated with the scope name
    ///
    /// # Errors
    ///
    /// Returns an error if a hash collision is detected between different scope names
    pub fn hash(&mut self, scope: &str) -> Result<u32, ScopedDbError> {
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

    /// Gets all registered scope names and their hashes
    pub fn get_all_scopes(&self) -> &HashMap<String, u32> {
        &self.scope_to_hash
    }

    /// Lookup a scope name by its hash value
    ///
    /// # Arguments
    ///
    /// * `hash` - The hash value to look up
    ///
    /// # Returns
    ///
    /// The scope name associated with the hash, or None if not found
    pub fn get_scope_name(&self, hash: u32) -> Option<&String> {
        self.hash_to_scope.get(&hash)
    }
}

impl Clone for ScopeRegistry {
    fn clone(&self) -> Self {
        Self {
            scope_to_hash: self.scope_to_hash.clone(),
            hash_to_scope: self.hash_to_scope.clone(),
        }
    }
}