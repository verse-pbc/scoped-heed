#\!/bin/bash

# Fix ScopedBytesDatabase implementation
sed -i '' 's/fn is_scope_empty_in_db(&self, txn: &RoTxn, scope: &Scope) -> Result<bool, ScopedDbError> {.*}/fn is_scope_empty_in_db(&self, txn: &RoTxn, scope: &Scope) -> Result<bool, ScopedDbError> {\n        match scope {\n            Scope::Default => {\n                let iter = self.db_default.iter(txn)?.into_iter();\n                Ok(iter.next().is_none())\n            }\n            Scope::Named { hash, .. } => {\n                let prefix = self.scope_registry.read().unwrap().get_prefix(*hash);\n                let iter = self.db_scoped.prefix_iter(txn, &prefix)?.into_iter();\n                Ok(iter.next().is_none())\n            }\n        }\n    }/' src/scoped_bytes_database.rs

# Fix ScopedBytesKeyDatabase implementation
sed -i '' 's/fn is_scope_empty_in_db(&self, txn: &RoTxn, scope: &Scope) -> Result<bool, ScopedDbError> {.*}/fn is_scope_empty_in_db(&self, txn: &RoTxn, scope: &Scope) -> Result<bool, ScopedDbError> {\n        match scope {\n            Scope::Default => {\n                let iter = self.db_default.iter(txn)?.into_iter();\n                Ok(iter.next().is_none())\n            }\n            Scope::Named { hash, .. } => {\n                let prefix = self.scope_registry.read().unwrap().get_prefix(*hash);\n                let iter = self.db_scoped.prefix_iter(txn, &prefix)?.into_iter();\n                Ok(iter.next().is_none())\n            }\n        }\n    }/' src/scoped_bytes_key_database.rs

# Fix ScopedDatabase implementation
sed -i '' 's/fn is_scope_empty_in_db(&self, txn: &RoTxn, scope: &Scope) -> Result<bool, ScopedDbError> {.*}/fn is_scope_empty_in_db(&self, txn: &RoTxn, scope: &Scope) -> Result<bool, ScopedDbError> {\n        match scope {\n            Scope::Default => {\n                let iter = self.db_default.iter(txn)?.into_iter();\n                Ok(iter.next().is_none())\n            }\n            Scope::Named { hash, .. } => {\n                let prefix = self.scope_registry.read().unwrap().get_prefix(*hash);\n                let iter = self.db_scoped.prefix_iter(txn, &prefix)?.into_iter();\n                Ok(iter.next().is_none())\n            }\n        }\n    }/' src/scoped_database.rs

chmod +x fix_is_scope_empty_in_db.sh
