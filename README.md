# Scoped Heed Library

This library provides a `ScopedDatabase` wrapper around the `heed` LMDB abstraction library, enabling namespaced or "scoped" data management within a Rust application.

## Purpose

The primary goal of this library is to offer:
1.  A `ScopedDatabase` abstraction that allows for:
    *   A **default (unscoped)** key-value store.
    *   Multiple **named, scoped** key-value stores (e.g., for different tenants, users, or data types).
2.  Compatibility of the `ScopedDatabase`\'s default scope with data stored in a plainly named `heed` database.
3.  Efficient operations (like iteration or clearing) on all data belonging to a specific named scope.

## `ScopedDatabase` Mechanism

The `ScopedDatabase` (defined in `src/lib.rs`) provides its functionality by managing two underlying `heed` databases within a single LMDB environment:

1.  **`db_default` (for the default/None scope)**:
    *   This is a standard `HeedDatabase<DefaultKeyStrCodec, Str>`.
    *   It maps to an LMDB database named `my_default_db`.
    *   Keys are stored directly as provided.
    *   This allows easy interaction with potentially pre-existing data stored in a `heed` database named `my_default_db`.

2.  **`db_scoped` (for all named scopes)**:
    *   This is a `HeedDatabase<ScopedKeyCodec, Str>`.
    *   It maps to a single LMDB database named `my_scoped_db`.
    *   To support multiple logical scopes (e.g., "users", "products") within this single physical database, it uses the `ScopedKeyCodec`. This codec prefixes the user\'s key with the scope name: `[length_of_scope_name_u32_be][scope_name_bytes][original_key_bytes]`.
    *   This prefixing allows efficient operations (like iteration or clearing) on all data belonging to a specific named scope.

## Key Features

*   **`src/lib.rs`**: Defines the `ScopedDatabase` struct, its methods, and the custom `ScopedKeyCodec` and `DefaultKeyStrCodec`.
*   **Examples**: See the `examples/` directory for demonstrations of how to use `ScopedDatabase`.
*   **Tests**: Integration tests in `tests/integration_test.rs` verify the core functionality.

## Usage

### Add to `Cargo.toml`
```toml
[dependencies]
scoped-heed = { path = "path/to/scoped-heed" } # Or from crates.io if published
heed = "0.20" # Ensure compatible heed version
```

### Basic Example
```rust
use scoped_heed::{ScopedDatabase, ScopedDbError};
use heed::{Env, EnvOpenOptions, RwTxn, RoTxn};
use std::fs;
use std::path::Path;

fn main() -> Result<(), ScopedDbError> {
    let db_path = Path::new("./my_scoped_app_db");
    fs::create_dir_all(db_path).expect("Failed to create DB directory");

    let env = unsafe {
        EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024) // 10MB
            .max_dbs(3) // Minimum needed for ScopedDatabase (default, scoped, and internal meta)
            .open(db_path)?
    };

    let scoped_db = ScopedDatabase::new(&env)?;

    // Write data to a named scope
    let mut wtxn = env.write_txn()?;
    scoped_db.put(&mut wtxn, Some("users"), "user:123", "Alice")?;
    scoped_db.put(&mut wtxn, Some("products"), "product:456", "Laptop")?;
    wtxn.commit()?;

    // Read data from a named scope
    let rtxn = env.read_txn()?;
    let user = scoped_db.get(&rtxn, Some("users"), "user:123")?;
    println!("User: {:?}", user.as_deref()); // Should print: User: Some("Alice")

    let product = scoped_db.get(&rtxn, Some("products"), "product:456")?;
    println!("Product: {:?}", product.as_deref()); // Should print: Product: Some("Laptop")

    // Iterate over a scope
    println!("Iterating over \'users\' scope:");
    for result in scoped_db.iter(&rtxn, Some("users"))? {
        let (key, value) = result?;
        println!("  {}: {}", key, value);
    }

    // Clear a scope
    let mut wtxn_clear = env.write_txn()?;
    let cleared_count = scoped_db.clear_scope(&mut wtxn_clear, Some("users"))?;
    wtxn_clear.commit()?;
    println!("Cleared {} items from \'users\' scope.", cleared_count);

    Ok(())
}
```

## Building and Testing the Library

### Prerequisites
*   Rust programming language and Cargo (Rust\'s package manager). See [rustup.rs](https://rustup.rs/) for installation.

### Compile and Test
Navigate to the `scoped-heed` directory and run:
```bash
cargo build
cargo test
```

### Run Examples
To run an example (e.g., `scoped_demo.rs` in `examples/`):
```bash
cargo run --example scoped_demo
```
This will execute the example program, which may print output to the console and create database files in the `scoped-heed` directory (e.g., inside a `target/` subdirectory or directly, depending on the example).

## Project Structure (Simplified for Library)

```
scoped-heed/
├── Cargo.toml          # Project manifest
├── README.md           # This file
├── src/
│   └── lib.rs          # Defines ScopedDatabase and its codecs
├── examples/
│   └── scoped_demo.rs  # Example usage
└── tests/
    └── integration_test.rs # Integration tests for ScopedDatabase
```