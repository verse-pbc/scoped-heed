#\!/bin/bash

# Update doctest lines for scoped_bytes_database.rs
sed -i '' 's/# use scoped_heed::{ScopedBytesDatabase, ScopedDbError, Scope};/# use scoped_heed::{ScopedBytesDatabase, ScopedDbError, Scope, GlobalScopeRegistry};\n# use std::sync::Arc;/g' src/scoped_bytes_database.rs
sed -i '' 's/# let db: ScopedBytesDatabase = ScopedBytesDatabase::new(&env, "test")?;/# let mut wtxn = env.write_txn()?;\n# let registry = Arc::new(GlobalScopeRegistry::new(\&env, \&mut wtxn)?);\n# wtxn.commit()?;\n# let db: ScopedBytesDatabase = ScopedBytesDatabase::new(\&env, "test", registry.clone())?;/g' src/scoped_bytes_database.rs
sed -i '' 's/# let db = ScopedBytesDatabase::new(&env, "test")?;/# let mut wtxn = env.write_txn()?;\n# let registry = Arc::new(GlobalScopeRegistry::new(\&env, \&mut wtxn)?);\n# wtxn.commit()?;\n# let db = ScopedBytesDatabase::new(\&env, "test", registry.clone())?;/g' src/scoped_bytes_database.rs

# Update doctest lines for scoped_bytes_key_database.rs
sed -i '' 's/# use scoped_heed::{ScopedBytesKeyDatabase, ScopedDbError, Scope};/# use scoped_heed::{ScopedBytesKeyDatabase, ScopedDbError, Scope, GlobalScopeRegistry};\n# use std::sync::Arc;/g' src/scoped_bytes_key_database.rs
sed -i '' 's/# let db: ScopedBytesKeyDatabase<String> = ScopedBytesKeyDatabase::new(&env, "test")?;/# let mut wtxn = env.write_txn()?;\n# let registry = Arc::new(GlobalScopeRegistry::new(\&env, \&mut wtxn)?);\n# wtxn.commit()?;\n# let db: ScopedBytesKeyDatabase<String> = ScopedBytesKeyDatabase::new(\&env, "test", registry.clone())?;/g' src/scoped_bytes_key_database.rs
sed -i '' 's/# let db = ScopedBytesKeyDatabase::<String>::new(&env, "test")?;/# let mut wtxn = env.write_txn()?;\n# let registry = Arc::new(GlobalScopeRegistry::new(\&env, \&mut wtxn)?);\n# wtxn.commit()?;\n# let db = ScopedBytesKeyDatabase::<String>::new(\&env, "test", registry.clone())?;/g' src/scoped_bytes_key_database.rs

# Update doctest lines for scoped_database.rs
sed -i '' 's/# use scoped_heed::{ScopedDatabase, ScopedDbError, Scope};/# use scoped_heed::{ScopedDatabase, ScopedDbError, Scope, GlobalScopeRegistry};\n# use std::sync::Arc;/g' src/scoped_database.rs
sed -i '' 's/# let db: ScopedDatabase<String, String> = ScopedDatabase::new(&env, "test")?;/# let mut wtxn = env.write_txn()?;\n# let registry = Arc::new(GlobalScopeRegistry::new(\&env, \&mut wtxn)?);\n# wtxn.commit()?;\n# let db: ScopedDatabase<String, String> = ScopedDatabase::new(\&env, "test", registry.clone())?;/g' src/scoped_database.rs
sed -i '' 's/# let db = ScopedDatabase::<String, String>::new(&env, "test")?;/# let mut wtxn = env.write_txn()?;\n# let registry = Arc::new(GlobalScopeRegistry::new(\&env, \&mut wtxn)?);\n# wtxn.commit()?;\n# let db = ScopedDatabase::<String, String>::new(\&env, "test", registry.clone())?;/g' src/scoped_database.rs

# Update scoped_database_options in global_registry.rs
sed -i '' 's/scoped_database_options(&env)/let registry = Arc::new(GlobalScopeRegistry::new(\&env, \&mut wtxn)?);\nscoped_database_options(\&env, registry.clone())/g' src/global_registry.rs

chmod +x update_doctests.sh
