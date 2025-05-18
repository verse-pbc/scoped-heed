# scoped-heed

A Rust library that adds namespace (scope) support to the heed LMDB wrapper, allowing you to organize data into isolated scopes while maintaining compatibility with standard heed databases.

## Features

- 🔍 **Scope-based Access**: Organize your data with named scopes (namespaces)
- 🏷️ **Default Scope Support**: Maintains compatibility with standard heed databases
- 📦 **Generic Key/Value Types**: Use any type that implements Serialize/Deserialize
- ⚡ **Efficient Operations**: Optimized iteration and bulk operations per scope
- 🔄 **Seamless Integration**: Drop-in compatibility with existing heed environments
- 🛡️ **Type-safe API**: Leverage Rust's type system for compile-time guarantees
- 🔐 **Hash-based Scoping**: Uses 32-bit hashes for efficient scope identification
- 🎯 **Tuple-based Keys**: Leverages heed's SerdeBincode for internal tuple representation

## Design

The library provides three main database implementations:

1. **Generic `ScopedDatabase<K, V>`**: Uses `SerdeBincode` for both keys and values (most flexible)
2. **Performance-optimized `ScopedBytesKeyDatabase<V>`**: Uses native byte slices for keys, generic values
3. **Fully-optimized `ScopedBytesDatabase`**: Uses native byte slices for both keys and values

### How It Works

All scoped entries have their keys internally prefixed with a 32-bit hash of the scope name. This ensures:
- Data isolation between scopes
- Efficient iteration within a scope
- No key collisions between scopes

#### Key Encoding

For scoped entries, the actual key stored in LMDB is structured as:
```
[scope_hash: 4 bytes][original_key_data]
```

Where:
- `scope_hash`: 32-bit hash of the scope name (little-endian)
- `original_key_data`: Your actual key, encoded based on the database type

Example:
- Scope: `"tenant1"` → hash: `0x12AB34CD`
- User key: `"user123"`
- Stored key: `[0xCD, 0x34, 0xAB, 0x12][encoded "user123"]`

#### Database Types

The builder pattern provides three configuration options:

1. **Serialized Types** (`.types::<K,V>()`):
   - Keys and values are serialized using bincode
   - Scoped key format: `[scope_hash: 4 bytes][bincode(key)]`
   - Most flexible, supports any Serde-compatible type

2. **Byte Keys** (`.bytes_keys::<V>()`):
   - Keys are raw `&[u8]`, values are serialized
   - Scoped key format: `[scope_hash: 4 bytes][key_length: 8 bytes][key_bytes]`
   - Optimized for byte-based keys like hashes or IDs

3. **Raw Bytes** (`.raw_bytes()`):
   - Both keys and values are raw `&[u8]`
   - Scoped key format: `[scope_hash: 4 bytes][key_length: 8 bytes][key_bytes]`
   - Maximum performance, no serialization overhead

### Database Naming

When creating a ScopedDatabase, you provide a base name that is used to create two internal databases:
- `{name}_default` - For unscoped data
- `{name}_scoped` - For scoped data

For example, `ScopedDatabase::new(&env, "users")` creates:
- `users_default` - Stores data without scopes
- `users_scoped` - Stores data with scope prefixes

This allows multiple ScopedDatabase instances to coexist in the same environment without conflicts.

## Usage

### Using the Builder Pattern

```rust
use scoped_heed::{scoped_database_options, ScopedDbError};
use heed::EnvOpenOptions;

fn main() -> Result<(), ScopedDbError> {
    let env = unsafe {
        EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024)
            .max_dbs(4)
            .open("./db")?
    };

    let mut txn = env.write_txn()?;
    
    // Create a database with String keys and String values
    let db = scoped_database_options(&env)
        .types::<String, String>()
        .name("config")
        .create(&mut txn)?;
    
    // Default scope - key stored as-is
    db.put(&mut txn, None, &"key1".to_string(), &"value1".to_string())?;
    
    // Named scope - key prefixed with 32-bit hash of "tenant1"
    db.put(&mut txn, Some("tenant1"), &"key1".to_string(), &"tenant1_value1".to_string())?;
    
    txn.commit()?;

    // Read values
    let rtxn = env.read_txn()?;
    let default_value = db.get(&rtxn, None, &"key1".to_string())?;
    let scoped_value = db.get(&rtxn, Some("tenant1"), &"key1".to_string())?;
    
    Ok(())
}
```

### Performance-Optimized Bytes Database

```rust
use scoped_heed::{scoped_database_options, ScopedDbError};
use heed::EnvOpenOptions;
use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize)]
struct GameData {
    score: u32,
    level: u8,
}

fn main() -> Result<(), ScopedDbError> {
    let env = unsafe {
        EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024)
            .max_dbs(4)
            .open("./db")?
    };
    
    let mut txn = env.write_txn()?;
    
    // Database with byte keys and serialized values
    let db = scoped_database_options(&env)
        .bytes_keys::<GameData>()
        .name("game")
        .create(&mut txn)?;
    
    // Keys are raw bytes, values are serialized
    let data = GameData { score: 1500, level: 5 };
    
    // Default scope - key stored as-is: b"player1"
    db.put(&mut txn, None, b"player1", &data)?;
    
    // Scoped - key stored as: [hash("tournament1")][8 bytes length][b"player1"]
    db.put(&mut txn, Some("tournament1"), b"player1", &data)?;
    
    txn.commit()?;
    
    // Zero-copy key access
    let rtxn = env.read_txn()?;
    let result = db.get(&rtxn, Some("tournament1"), b"player1")?;
    
    Ok(())
}
```

### Fully-Optimized Bytes Database

```rust
use scoped_heed::{scoped_database_options, ScopedDbError};
use heed::EnvOpenOptions;

fn main() -> Result<(), ScopedDbError> {
    let env = unsafe {
        EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024)
            .max_dbs(4)
            .open("./db")?
    };
    
    let mut txn = env.write_txn()?;
    
    // Database with raw byte keys and values (no serialization)
    let db = scoped_database_options(&env)
        .raw_bytes()
        .name("cache")
        .create(&mut txn)?;
    
    // Direct byte operations with zero overhead
    // Default scope - key stored as-is: b"key1"
    db.put(&mut txn, None, b"key1", b"value1")?;
    
    // Scoped - key stored as: [hash("cache")][8 bytes length][b"session_123"]
    db.put(&mut txn, Some("cache"), b"session_123", b"user_data")?;
    
    txn.commit()?;
    
    // Zero-copy reads for both keys and values
    let rtxn = env.read_txn()?;
    let value: Option<&[u8]> = db.get(&rtxn, Some("cache"), b"session_123")?;
    
    Ok(())
}
```

### Custom Types with Serde

```rust
use scoped_heed::{scoped_database_options, ScopedDbError};
use serde::{Serialize, Deserialize};
use heed::EnvOpenOptions;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UserKey {
    user_id: u64,
    field: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct UserData {
    name: String,
    age: u32,
}

fn main() -> Result<(), ScopedDbError> {
    let env = unsafe {
        EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024)
            .max_dbs(4)
            .open("./db")?
    };
    
    let mut txn = env.write_txn()?;
    
    // Database with custom types
    let db = scoped_database_options(&env)
        .types::<UserKey, UserData>()
        .name("users")
        .create(&mut txn)?;
    
    let key = UserKey {
        user_id: 12345,
        field: "profile".to_string(),
    };
    
    let data = UserData {
        name: "Alice".to_string(),
        age: 30,
    };
    
    // Scoped key stored as: [hash("org1")][bincode(UserKey)]
    db.put(&mut txn, Some("org1"), &key, &data)?;
    txn.commit()?;
    
    Ok(())
}
```

## API Traits

All database types implement common Rust traits for better usability:
- `Debug` - For debugging and logging
- `Clone` - For copying database handles (creates a fresh scope hasher)

Note: When cloning a database, the new instance gets its own scope hasher to avoid concurrent access issues with the `RwLock`.

## Internal Representation

The library uses different encoding strategies for optimal performance:

### Generic Database
```rust
// For scoped entries with SerdeBincode:
SerdeBincode<ScopedKey<K>> where ScopedKey = { scope_hash: u32, key: K }

// Encoded as: [bincode serialization of the struct]
```

### Optimized Bytes Database  
```rust
// For scoped entries with manual encoding:
[scope_hash: u32_le][key_length: u64_le][key_bytes: &[u8]]

// Example: scope_hash=0x12345678, key=b"test"
// Bytes: [0x78, 0x56, 0x34, 0x12, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x74, 0x65, 0x73, 0x74]
```

The manual encoding produces byte-for-byte identical output to the bincode version for compatibility.

## Performance Considerations

Choose the right implementation based on your use case:

### Use `ScopedDatabase<K,V>` when:
- You need complex key types (structs, enums)
- Keys are variable-length strings
- Development convenience is prioritized
- Serialization overhead is acceptable

### Use `ScopedBytesKeyDatabase<V>` when:
- Keys are byte sequences (hashes, IDs)
- Performance is critical
- You want to avoid allocations
- Key operations are in hot paths

Based on benchmarks, performance comparison:

### `ScopedBytesKeyDatabase<V>` vs `ScopedDatabase<K,V>`:
- **1.0x** write operations (similar performance)
- **1.1x faster** read operations
- **2.5x faster** key encoding
- **38x faster** key decoding

### `ScopedBytesDatabase` (pure bytes) vs `ScopedDatabase<K,V>`:
- **1.8x faster** write operations
- **1.3x faster** read operations
- Zero serialization overhead for both keys and values

Performance gains come from:
- No Serde serialization overhead
- Zero allocations for fixed-size keys
- Direct memory operations
- Optimized codec implementation

## Multi-tenant Example

```rust
use scoped_heed::{ScopedStrDatabase, ScopedDbError};

fn main() -> Result<(), ScopedDbError> {
    let env = /* setup environment */;
    let db = ScopedStrDatabase::new(&env)?;
    
    let mut wtxn = env.write_txn()?;
    
    // Each tenant gets their own scope
    // Internally uses SerdeBincode tuples for isolation
    db.put(&mut wtxn, Some("tenant1"), &"config".to_string(), &"value1".to_string())?;
    db.put(&mut wtxn, Some("tenant2"), &"config".to_string(), &"value2".to_string())?;
    
    // Global data
    db.put(&mut wtxn, None, &"system".to_string(), &"global".to_string())?;
    
    wtxn.commit()?;
    
    // Iterate over a specific tenant's data
    let rtxn = env.read_txn()?;
    for result in db.iter(&rtxn, Some("tenant1"))? {
        let (key, value) = result?;
        println!("Tenant1: {} = {}", key, value);
    }
    
    Ok(())
}
```

## Error Handling

The library provides a custom error type `ScopedDbError`:

```rust
pub enum ScopedDbError {
    Heed(heed::Error),
    EmptyScopeDisallowed,
    InvalidInput(String),
    Encoding(String),
}
```

Note: Empty strings are not allowed as scope names - use `None` for the default scope.

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
scoped-heed = "0.1.0"
```

## Examples

The repository includes several examples:

- `basic_usage` - Simple key-value operations with generic types
- `bytes_optimization` - Demonstrates the performance-optimized bytes implementation
- `pure_bytes` - Shows the fully-optimized bytes-only database

Run examples with:

```bash
cargo run --example basic_usage
cargo run --example bytes_optimization
cargo run --example pure_bytes
```

## Benchmarks

Performance benchmarks are included to compare all three implementations:

```bash
cargo bench
```

The benchmarks measure:
- Full database write/read operations 
- Key encoding/decoding in isolation
- Real-world performance differences

View detailed results in `target/criterion/report/index.html` after running.

## Implementation Notes

- Generic version uses heed's native `SerdeBincode` for tuple serialization
- Bytes version implements custom `BytesEncode`/`BytesDecode` for optimal performance
- Scope hashes are 32-bit for minimal overhead (4 bytes per scoped entry)
- Both implementations produce identical binary layouts
- Compatible with all Serde-supported types (for values)
- Thread-safe scope hasher with collision detection
- Configurable database naming prevents conflicts in shared environments
- All database types implement `Debug` and `Clone` for easy debugging and testing

## License

[MIT License](LICENSE)