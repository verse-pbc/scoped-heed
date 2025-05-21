# scoped-heed

[![crates.io](https://img.shields.io/crates/v/scoped-heed.svg)](https://crates.io/crates/scoped-heed)
[![docs.rs](https://docs.rs/scoped-heed/badge.svg)](https://docs.rs/scoped-heed)
[![License: MIT/Apache-2.0](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](LICENSE)

Redis-like namespace isolation for the heed LMDB wrapper, providing isolated scopes within a single LMDB environment.

## Features

- **Complete Scope Isolation**: Each scope acts as an isolated namespace with separate storage
- **Default Scope Support**: Backward compatibility with standard heed databases
- **Generic Type Support**: Works with any Serde-compatible types for keys and values
- **Range Queries**: Efficient range operations within scopes
- **Performance Options**: Multiple database implementations optimized for different use cases
- **Global Registry**: Central management for scope discovery and lifecycle operations

## Database Types

The library provides three database implementations:

1. **`ScopedDatabase<K, V>`**: Fully generic keys and values using Serde
2. **`ScopedBytesKeyDatabase<V>`**: Raw byte keys with serialized values
3. **`ScopedBytesDatabase`**: Raw bytes for both keys and values

## Scope Isolation Model

Scopes provide Redis-like isolation:
- Each scope maintains complete data separation
- Identical keys can exist in different scopes with different values
- Operations on one scope never affect data in other scopes
- Scope lifecycle management (creating, listing, pruning)

Common use cases:
- Multi-tenant applications
- Test isolation
- Service component separation

## Technical Implementation

Keys in scoped databases are prefixed with a 32-bit hash of the scope name:
```
[scope_hash: 4 bytes][original_key_data]
```

The `GlobalScopeRegistry` component:
- Tracks all scopes across multiple databases
- Enables listing and discovering scopes
- Provides cross-database operations for pruning empty scopes

## Usage

### Basic Example

```rust
use scoped_heed::{scoped_database_options, ScopedDbError, Scope, GlobalScopeRegistry};
use heed::EnvOpenOptions;
use std::sync::Arc;

fn main() -> Result<(), ScopedDbError> {
    let env = unsafe {
        EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024)
            .max_dbs(5) // The number of dbs to create plus the scopes registry and default internal db
            .open("./db")?
    };

    // Create global registry
    let mut txn = env.write_txn()?;
    let registry = Arc::new(GlobalScopeRegistry::new(&env, &mut txn)?);
    txn.commit()?;

    let mut txn = env.write_txn()?;

    // Create database with String keys and values
    let db = scoped_database_options(&env, registry.clone())
        .types::<String, String>()
        .name("config")
        .create(&mut txn)?;

    // Default scope
    db.put(&mut txn, &Scope::Default, &"key1".to_string(), &"value1".to_string())?;

    // Named scope using Scope enum
    let tenant_scope = Scope::named("tenant1")?;
    db.put(&mut txn, &tenant_scope, &"key1".to_string(), &"tenant1_value1".to_string())?;

    // Or use the convenience method with string
    db.put_with_name(&mut txn, "tenant2", &"key1".to_string(), &"tenant2_value1".to_string())?;

    txn.commit()?;
    Ok(())
}
```

### Byte Keys Example

```rust
// Database with byte keys and values
let db = scoped_database_options(&env, registry.clone())
    .raw_bytes()
    .name("cache")
    .create(&mut txn)?;

// No serialization overhead - use with_name convenience method
db.put_with_name(&mut txn, "cache", b"session_123", b"user_data")?;
```

### Multi-tenant Example

```rust
// Each tenant's data is isolated
db.put_with_name(&mut txn, "tenant_a", &"config", &"settings_a")?;
db.put_with_name(&mut txn, "tenant_b", &"config", &"settings_b")?;

// Same key, different scopes, different values
let a = db.get_with_name(&rtxn, "tenant_a", &"config")?; // "settings_a"
let b = db.get_with_name(&rtxn, "tenant_b", &"config")?; // "settings_b"
```

## Database Operations

```rust
// Basic operations with Scope enum
let scope = Scope::named("scope")?;
db.put(&mut wtxn, &scope, &key, &value)?;
let value = db.get(&rtxn, &scope, &key)?;
db.delete(&mut wtxn, &scope, &key)?;
db.clear(&mut wtxn, &scope)?;

// Simpler operations with string convenience methods
db.put_with_name(&mut wtxn, "scope", &key, &value)?;
let value = db.get_with_name(&rtxn, "scope", &key)?;
db.delete_with_name(&mut wtxn, "scope", &key)?;
db.clear_with_name(&mut wtxn, "scope")?;

// Iteration
for result in db.iter(&rtxn, &scope)? {
    let (key, value) = result?;
}

// Find and prune empty scopes globally
let databases: [&dyn ScopeEmptinessChecker; 2] = [&users_db, &products_db];
let pruned_count = registry.prune_globally_unused_scopes(&mut wtxn, &databases)?;
```


## Performance

- **Generic database**: Most flexible, uses serialization
- **Byte keys database**: Faster key operations (~38x faster decoding)
- **Raw bytes database**: Fastest, no serialization overhead


## Error Handling

`ScopedDbError` wraps heed errors and adds:
- `EmptyScopeDisallowed`: Empty strings not allowed as scope names
- `InvalidInput`: Input validation errors

Use `None` for the default scope, not empty strings.

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
scoped-heed = "0.2.0-alpha.1"
```

## Examples

The library includes example implementations showing different aspects of the functionality:

- **Multi-tenant Data Management** - Complete data isolation between tenants
  ```bash
  cargo run --example multi_tenant
  ```

- **Scope Management** - Scope registration, listing, and lifecycle management
  ```bash
  cargo run --example scope_management
  ```

- **Performance Optimizations** - Comparison of different database implementations
  ```bash
  cargo run --example performance_optimizations
  ```

- **Parallel Processing** - Thread safety with worker-specific scopes
  ```bash
  cargo run --example parallel_processing
  ```

## License

MIT OR Apache-2.0