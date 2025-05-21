use crate::ScopedDbError;
use std::hash::Hasher;
use twox_hash::XxHash32;

/// Represents either a named scope with a cached hash or the default (unscoped) database.
///
/// This enum replaces the previous `Option<&str>` pattern for working with scopes.
/// It provides direct access to the hash value, avoiding redundant hash calculations
/// when the same scope is used multiple times.
///
/// # Performance
///
/// Using the `Scope` enum provides a significant performance improvement over
/// the previous `Option<&str>` approach because:
///
/// 1. Hash values are computed once and cached, avoiding redundant calculations
/// 2. The `xxHash32` algorithm provides superior performance compared to the
///    standard Rust hasher
/// 3. Operations like `get`, `put`, and `delete` no longer need to acquire a
///    write lock on the `ScopeHasher` for every operation
///
/// # Hash Collisions
/// 
/// This library uses a 32-bit hash (via xxHash32) for identifying scopes, which
/// provides a good balance between performance, storage efficiency, and collision resistance.
/// 
/// ## Collision Probability
/// 
/// With a 32-bit hash space (4 billion possible values):
/// - With 1,000 scopes: Collision probability is very low (about 0.0001%)
/// - With 10,000 scopes: Collision probability is roughly 0.01%
/// - With 100,000 scopes: Collision probability is approximately 1%
/// 
/// ## Collision Handling
/// 
/// The library's `GlobalScopeRegistry` takes an important safety precaution: it 
/// detects hash collisions between different scope names and immediately returns
/// an error if a collision is found, preventing any potential data corruption.
/// 
/// For example, if by rare chance "scope1" and "scope2" both generate the same hash value,
/// the system will detect this during the first attempt to use the second scope and
/// return a `ScopedDbError::InvalidInput` error with a clear message identifying the
/// collision.
/// 
/// ## Recommended Practice
/// 
/// To minimize collision risks:
/// 1. Keep the total number of unique scopes below 10,000 if possible
/// 2. Use descriptive but focused scope names (avoid excessively long names)
/// 3. Use a consistent naming scheme for scopes
/// 
/// When a hash collision occurs, you'll need to adjust one of the colliding scope names.
/// This is a rare occurrence but important to understand if you're working with
/// a very large number of scopes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Scope {
    /// The default (unscoped) database
    Default,
    /// A named scope with a pre-computed hash
    Named {
        /// The name of the scope
        name: String,
        /// Cached xxHash value for the scope
        hash: u32,
    },
}

impl Scope {
    /// Create a named scope from a string
    ///
    /// This method computes and caches the xxHash32 value for the scope name,
    /// allowing it to be reused across multiple database operations without
    /// recalculating the hash each time.
    ///
    /// # Errors
    ///
    /// Returns `ScopedDbError::EmptyScopeDisallowed` if the name is empty.
    ///
    /// # Example
    ///
    /// ```
    /// # use scoped_heed::Scope;
    /// let tenant_scope = Scope::named("tenant1").unwrap();
    /// // The hash is computed once and stored in the Scope
    /// ```
    #[inline]
    pub fn named(name: &str) -> Result<Self, ScopedDbError> {
        if name.is_empty() {
            return Err(ScopedDbError::EmptyScopeDisallowed);
        }

        let hash = compute_xxhash(name.as_bytes());
        Ok(Self::Named {
            name: name.to_string(),
            hash,
        })
    }

    // Removed unused with_hash function

    /// Get the scope name if this is a named scope
    ///
    /// Returns `None` for the default scope.
    #[inline]
    pub fn name(&self) -> Option<&str> {
        match self {
            Self::Default => None,
            Self::Named { name, .. } => Some(name),
        }
    }

    /// Get the scope hash if this is a named scope
    ///
    /// Returns `None` for the default scope.
    #[inline]
    pub fn hash(&self) -> Option<u32> {
        match self {
            Self::Default => None,
            Self::Named { hash, .. } => Some(*hash),
        }
    }

    /// Check if this is the default scope
    ///
    /// Returns `true` for the default scope, `false` for named scopes.
    #[inline]
    pub fn is_default(&self) -> bool {
        matches!(self, Self::Default)
    }
}

impl From<&str> for Scope {
    #[inline]
    fn from(name: &str) -> Self {
        if name.is_empty() {
            Self::Default
        } else {
            match Self::named(name) {
                Ok(scope) => scope,
                Err(_) => Self::Default, // This shouldn't happen for non-empty strings
            }
        }
    }
}

impl<'a> From<Option<&'a str>> for Scope {
    #[inline]
    fn from(name: Option<&'a str>) -> Self {
        match name {
            Some(name) => name.into(),
            None => Self::Default,
        }
    }
}

/// Compute a 32-bit xxHash value for the given bytes
///
/// This uses the xxHash32 algorithm, which is significantly faster than
/// the standard Rust DefaultHasher while still providing good hash distribution.
/// The 32-bit hash is sufficient for our scope identification purposes and
/// allows for compact key prefixing.
#[inline]
pub fn compute_xxhash(data: &[u8]) -> u32 {
    let mut hasher = XxHash32::with_seed(0); // Use a fixed seed for consistency
    hasher.write(data);
    hasher.finish() as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scope_default() {
        let scope = Scope::Default;
        assert!(scope.is_default());
        assert_eq!(scope.name(), None);
        assert_eq!(scope.hash(), None);
    }

    #[test]
    fn test_scope_named() {
        let scope = Scope::named("tenant").unwrap();
        assert!(!scope.is_default());
        assert_eq!(scope.name(), Some("tenant"));
        assert!(scope.hash().is_some());
    }

    #[test]
    fn test_scope_empty_name() {
        let result = Scope::named("");
        assert!(matches!(result, Err(ScopedDbError::EmptyScopeDisallowed)));
    }

    #[test]
    fn test_scope_from_str() {
        let scope: Scope = "tenant".into();
        assert_eq!(scope.name(), Some("tenant"));

        let default: Scope = "".into();
        assert!(default.is_default());
    }

    #[test]
    fn test_scope_from_option_str() {
        let scope: Scope = Some("tenant").into();
        assert_eq!(scope.name(), Some("tenant"));

        let default: Scope = Option::<&str>::None.into();
        assert!(default.is_default());
    }

    #[test]
    fn test_compute_xxhash() {
        // Test consistency - same input should give same output
        let hash1 = compute_xxhash(b"test");
        let hash2 = compute_xxhash(b"test");
        assert_eq!(hash1, hash2);

        // Different inputs should (most likely) give different outputs
        let hash3 = compute_xxhash(b"different");
        assert_ne!(hash1, hash3);
    }

    // Test for with_hash removed since the function is no longer used
}
