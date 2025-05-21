# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0-alpha.1] - 2024-05-20

### Added
- Added `GlobalScopeRegistry` for centralized scope metadata management
- Added `ScopeEmptinessChecker` trait for unified scope emptiness checking
- Added `_with_name` convenience methods to all database types for direct string-based scope operations
- Added `prune_globally_unused_scopes` for safely pruning empty scopes across databases
- Added test coverage for u32::MAX hash edge cases

### Removed
- **BREAKING**: Removed `prune_empty_scopes` methods from database implementations
  - Use `GlobalScopeRegistry::prune_globally_unused_scopes` instead for safer pruning

### Changed
- **BREAKING**: All database types now require a `GlobalScopeRegistry` for scope operations
- Improved documentation for clear() methods explaining optimized implementations

## [0.1.1] - 2024-05-19

### Changed
- Pinned heed dependency to version 0.20.5 for stability and compatibility with dependent projects

## [0.1.0] - 2024-01-19

### Added
- Initial release of scoped-heed
- Redis-like database isolation for LMDB via heed
- Completely isolated scopes within a single environment
- Support for generic key/value types with SerdeBincode
- Optimized implementations for byte keys and raw bytes
- Comprehensive examples demonstrating various use cases
- Full test coverage for scope isolation
- Benchmarks comparing performance of different implementations

### Features
- `ScopedDatabase<K, V>` - Generic typed database with serialized keys and values
- `ScopedBytesKeyDatabase<V>` - Performance-optimized database with byte keys
- `ScopedBytesDatabase` - Fully-optimized database with raw byte keys and values
- Hash-based scope encoding for efficient lookups
- Builder pattern for easy database configuration
- Support for range queries within scopes
- Clone trait implementations for all database types

[Unreleased]: https://github.com/verse-pbc/scoped-heed/compare/v0.2.0-alpha.1...HEAD
[0.2.0-alpha.1]: https://github.com/verse-pbc/scoped-heed/compare/v0.1.1...v0.2.0-alpha.1
[0.1.1]: https://github.com/verse-pbc/scoped-heed/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/verse-pbc/scoped-heed/releases/tag/v0.1.0