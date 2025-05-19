# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Common Development Commands

### Build and Run
- `cargo build` - Build the library in debug mode
- `cargo build --release` - Build in release mode
- `cargo run --example scoped_demo` - Run the example demonstration

### Testing
- `cargo test` - Run all tests (unit and integration)
- `cargo test -- --nocapture` - Run tests with println! output visible
- `cargo test --lib` - Run only library unit tests
- `cargo test integration_test` - Run specific integration test

### Linting and Formatting
- `cargo fmt` - Format code according to Rust standards
- `cargo clippy` - Run linter for common mistakes
- `cargo clippy -- -D warnings` - Treat all clippy warnings as errors

### Documentation
- `cargo doc` - Generate documentation
- `cargo doc --open` - Generate and open documentation in browser

## Architecture Overview

scoped-heed is a Rust library that provides namespace support for the heed LMDB database library. It enables:

1. **Default (unscoped) database** - For compatibility with existing heed databases
2. **Named scopes** - For multi-tenant or categorized data storage
3. **Efficient scope operations** - Iteration and clearing by scope

### Core Components

- `ScopedDatabase<KC, DC>`: Main struct managing scoped data storage (generic for default DB, string-based for scoped DB)
- `ScopedHashCodec`: Specialized codec for encoding/decoding scoped keys using 32-bit hashes (string keys only)
- `ScopedKeyWithHashCodec<KC>`: Future generic codec implementation (documented but not active)
- `ScopeHasher`: Manages hash mappings and collision detection for scope names
- `ScopedDbError`: Custom error type including hash collision and scope length errors
- `ScopedHashIter`: Iterator for scoped and default databases

### Key Implementation Details

- The library is generic over key codecs (`KC`) and data codecs (`DC`) for the default database
- The scoped database currently uses string keys and values only (technical limitation)
- Scope encoding uses 32-bit hash format: `[scope_hash_4bytes][original_key_bytes]`
- Scope names are limited to 20 characters to reduce collision likelihood
- Database names: `"my_default_db"` (unscoped), `"my_scoped_db"` (scoped)
- The library requires minimum 3 max_dbs in heed environment configuration
- Empty scope strings (`""`) are invalid for named scopes
- `None` represents the default/unscoped database
- Special handling for `Str` key codec in iteration and clear operations
- Uses unsafe code for type transmutations when working with `Str` codec

### Typical Usage Pattern

```rust
// Initialize
let env = unsafe {
    EnvOpenOptions::new()
        .map_size(10 * 1024 * 1024)
        .max_dbs(3)
        .open(db_path)?
};
let scoped_db: ScopedDatabase<Str, Str> = ScopedDatabase::new(&env)?;

// Basic operations
let mut wtxn = env.write_txn()?;

// Default scope (None)
scoped_db.put(&mut wtxn, None, "key1", "value1")?;

// Named scope
scoped_db.put(&mut wtxn, Some("tenant"), "key1", "tenant_value1")?;

// Read with transactions
wtxn.commit()?;
let rtxn = env.read_txn()?;
let value = scoped_db.get(&rtxn, Some("tenant"), "key1")?;

// Iterate over scope
for result in scoped_db.iter(&rtxn, Some("tenant"))? {
    let (key, value) = result?;
    println!("{}: {}", key, value);
}

// Clear entire scope
let mut wtxn = env.write_txn()?;
let cleared = scoped_db.clear_scope(&mut wtxn, Some("tenant"))?;
wtxn.commit()?;
```

### Available Examples

- `basic_usage.rs` - Demonstrates fundamental operations
- `scoped_demo.rs` - Shows scoped database usage
- `multi_tenant.rs` - Multi-tenant application pattern
- `iteration_patterns.rs` - Different iteration approaches
- `legacy_compatibility.rs` - Working with existing LMDB databases
- `error_handling.rs` - Proper error handling patterns

### Error Handling

The library uses `ScopedDbError` enum with these variants:
- `Heed(heed::Error)` - Wraps underlying heed errors
- `EmptyScopeDisallowed` - Empty string scope names are not allowed
- `InvalidInput(String)` - For various input validation failures

Always handle empty scope strings properly - use `None` for default scope, not `Some("")`.

## Important Notes

- Always run `cargo fmt` before committing
- Ensure `cargo clippy` passes with no warnings
- Test both unit tests and integration tests before major changes
- The codebase uses Rust edition 2024
- The default database is generic over heed codecs (KC, DC), scoped database uses Str codec
- Multiple `ScopedDatabase` instances can coexist in one environment
- Scope operations use hash-based lookups for efficient filtering
- Legacy database compatibility is maintained through the default database
- Hash collisions are detected and reported as errors
- Future work could make the scoped database fully generic (see ScopedKeyWithHashCodec)

If you need to inspect code of a crate installed locally, you could use this tool:

```
/Users/daniel/code/scan_crate/target/release/scan_crate --help
Command-line arguments

Usage: scan_crate [OPTIONS]

Options:
      --crate-spec <CRATE_SPEC>
          Crate specification (e.g., "serde", "serde@1.0.130")
      --query <QUERY>
          Regex query to search for within the crate source code
      --case-sensitive <CASE_SENSITIVE>
          Optional flag for case-sensitive search (defaults to false) [possible values: true, false]
      --mcp
          Run as an MCP server instead of performing a direct inspection
```

it will give you file paths and line numbers where the searhced regexp is found in that crate codebase
