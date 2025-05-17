# ScopedHeed Examples

This directory contains examples demonstrating how to use the `scoped-heed` library.

## Available Examples

### 1. basic_usage.rs
- Simple introduction to ScopedDatabase
- Basic CRUD operations (Create, Read, Update, Delete)
- Working with both default (None) scope and named scopes
- Shows how values are updated and deleted

### 2. multi_tenant.rs
- Demonstrates multi-tenant use case
- Managing data for different organizations/tenants
- Scope-specific data isolation
- Bulk operations like clearing entire scopes
- Perfect for SaaS applications

### 3. iteration_patterns.rs
- Various ways to iterate through data
- Filtering entries during iteration
- Collecting and sorting results
- Counting entries across scopes
- Aggregating data from multiple scopes
- Complex queries across the database

### 4. legacy_compatibility.rs
- How to work with existing heed databases
- Opening legacy databases with ScopedDatabase
- Maintaining backward compatibility
- Migration strategies from unscoped to scoped data
- Gradual adoption patterns

### 5. error_handling.rs
- Comprehensive error handling examples
- Invalid input validation
- Transaction error handling
- Non-existent key/scope behavior
- Recovery strategies
- Custom error conversion

### 6. scoped_demo.rs (Original comprehensive example)
- Complete demonstration of all features
- Multiple test scenarios
- Raw heed API comparisons
- Legacy compatibility tests

## Running Examples

To run any example:

```bash
cargo run --example basic_usage
cargo run --example multi_tenant
cargo run --example iteration_patterns
cargo run --example legacy_compatibility
cargo run --example error_handling
cargo run --example scoped_demo
```

## Notes

- All examples create temporary databases in the current directory
- Databases are automatically cleaned up after execution
- Examples include assertions to verify correct behavior
- Error messages and status updates are printed to help understand the flow