# scoped-heed

[![crates.io](https://img.shields.io/crates/v/scoped-heed.svg)](https://crates.io/crates/scoped-heed)
[![docs.rs](https://docs.rs/scoped-heed/badge.svg)](https://docs.rs/scoped-heed)
[![License: MIT/Apache-2.0](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](LICENSE)

Namespace isolation for the heed LMDB wrapper. Provides multiple logical databases within a single LMDB environment.

## Features

- **Scope Isolation**: Each scope acts as an isolated namespace with no cross-scope access
- **Default Scope**: Compatible with standard heed databases
- **Generic Types**: Supports any Serde-compatible types
- **Range Queries**: Efficient iteration within scopes
- **Hash-based Keys**: Uses 32-bit hashes for scope identification
- **Global Registry**: Required centralized registry for scope discovery and enumeration
- **Safe Pruning**: Cross-database scope emptiness checking

## Design

The library provides three database implementations:

1. **`ScopedDatabase<K, V>`**: Serialized keys and values (using SerdeBincode)
2. **`ScopedBytesKeyDatabase<V>`**: Byte slice keys, serialized values
3. **`ScopedBytesDatabase`**: Raw byte slices for both keys and values

### Scope Isolation

Scopes work like separate databases:
- Each scope is isolated within the same LMDB environment
- No cross-scope queries or operations
- Keys can be identical across different scopes
- Clearing a scope only affects that scope

Use cases:
- Multi-tenant applications
- Test isolation
- Component separation

### Implementation

Scoped entries have keys prefixed with a 32-bit hash of the scope name:
```
[scope_hash: 4 bytes][original_key_data]
```

A `GlobalScopeRegistry` is required for all database instances to maintain scope discoverability:
- Provides the ability to list and discover all scopes in the environment
- Essential for traversing and managing isolated scopes
- Enables operations across all scopes, such as enumeration and pruning

### Database Types

**Serialized Types** (`.types::<K,V>()`):
- Keys and values are serialized using bincode
- Supports any Serde-compatible type

**Byte Keys** (`.bytes_keys::<V>()`):
- Keys are raw `&[u8]`, values are serialized
- Better performance for byte-based keys

**Raw Bytes** (`.raw_bytes()`):
- Both keys and values are raw `&[u8]`
- No serialization overhead

### Database Naming

Each ScopedDatabase creates two internal databases:
- `{name}_default` - For unscoped data
- `{name}_scoped` - For scoped data

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
            .max_dbs(5) // Need an extra db for global registry
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

For stable API, use the latest stable version:

```toml
[dependencies]
scoped-heed = "0.1.1"
```

## Examples

Run examples with:

```bash
cargo run --example basic_usage
cargo run --example multi_tenant
cargo run --example bytes_optimization
```

## License

MIT OR Apache-2.0