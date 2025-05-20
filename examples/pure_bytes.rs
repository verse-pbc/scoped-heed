use heed::EnvOpenOptions;
use scoped_heed::{Scope, scoped_database_options, GlobalScopeRegistry};
use std::sync::Arc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a temporary directory for the database
    let db_path = "/tmp/scoped_heed_pure_bytes";
    if std::path::Path::new(db_path).exists() {
        std::fs::remove_dir_all(db_path)?;
    }
    std::fs::create_dir_all(db_path)?;

    // Initialize the environment
    let env = unsafe {
        EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024) // 10MB
            .max_dbs(3)
            .open(db_path)?
    };

    // Create a global registry
    let mut wtxn = env.write_txn()?;
    let registry = Arc::new(GlobalScopeRegistry::new(&env, &mut wtxn)?);
    wtxn.commit()?;

    // Create a pure bytes database - optimal for binary data
    let mut wtxn = env.write_txn()?;
    let db = scoped_database_options(&env, registry.clone())
        .raw_bytes()
        .name("binary")
        .create(&mut wtxn)?;
    wtxn.commit()?;

    // Create scopes for different services
    let auth_scope = Scope::named("auth_service")?;
    let cache_scope = Scope::named("cache_service")?;
    let metrics_scope = Scope::named("metrics_service")?;

    // Binary data operations demonstrating Redis-like isolation
    {
        let mut wtxn = env.write_txn()?;

        // Each service component gets its own isolated scope

        // Authentication service scope
        let auth_token = b"token_abc123";
        let auth_data = b"\x01\x23\x45\x67\x89\xab\xcd\xef"; // encrypted token
        db.put(&mut wtxn, &auth_scope, auth_token, auth_data)?;

        // Cache service scope - can use same key names without collision
        let cache_token = b"token_abc123"; // Same key name!
        let cache_data = b"cached_user_session_data";
        db.put(&mut wtxn, &cache_scope, cache_token, cache_data)?;

        // Metrics service scope - again same key names, different data
        let metric_token = b"token_abc123"; // Same key name again!
        let metric_data = &42u32.to_le_bytes(); // different type of data
        db.put(
            &mut wtxn,
            &metrics_scope,
            metric_token,
            metric_data,
        )?;

        // Configuration in default scope
        let config_key = b"max_connections";
        let config_value = &1000u32.to_le_bytes();
        db.put(&mut wtxn, &Scope::Default, config_key, config_value)?;

        wtxn.commit()?;
    }

    // Read binary data back - demonstrating complete isolation
    {
        let rtxn = env.read_txn()?;

        println!("=== Redis-like Scope Isolation with Binary Data ===\n");

        // Same key "token_abc123" has different values in each scope
        let key = b"token_abc123";

        // Auth service sees encrypted token
        if let Some(data) = db.get(&rtxn, &auth_scope, key)? {
            println!("Auth service token: {:?} (encrypted binary)", data);
        }

        // Cache service sees session data
        if let Some(data) = db.get(&rtxn, &cache_scope, key)? {
            println!(
                "Cache service token: {}",
                std::str::from_utf8(data).unwrap_or("<binary>")
            );
        }

        // Metrics service sees numeric data
        if let Some(data) = db.get(&rtxn, &metrics_scope, key)? {
            if data.len() == 4 {
                let value = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
                println!("Metrics service token: {} (as u32)", value);
            }
        }

        // Config from default scope
        if let Some(data) = db.get(&rtxn, &Scope::Default, b"max_connections")? {
            if data.len() == 4 {
                let value = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
                println!("\nDefault scope config - max connections: {}", value);
            }
        }

        println!(
            "\n✅ Same key name 'token_abc123' stores completely different data in each scope!"
        );
    }

    println!("\nPure bytes database advantages with scope isolation:");
    println!("- Complete Redis-like isolation between scopes");
    println!("- No serialization overhead for keys or values");
    println!("- Direct memory operations only");
    println!("- Perfect for binary data like hashes, tokens, metrics");
    println!("- Each service component gets its own isolated namespace");

    // Clean up
    drop(env);
    std::fs::remove_dir_all(db_path)?;

    Ok(())
}