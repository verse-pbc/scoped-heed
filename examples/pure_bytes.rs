use heed::EnvOpenOptions;
use scoped_heed::scoped_database_options;

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

    // Create a pure bytes database - optimal for binary data
    let mut wtxn = env.write_txn()?;
    let db = scoped_database_options(&env)
        .raw_bytes()
        .name("binary")
        .create(&mut wtxn)?;
    wtxn.commit()?;

    // Binary data operations
    {
        let mut wtxn = env.write_txn()?;
        
        // Store binary hash values
        let hash1 = b"\x12\x34\x56\x78\x9a\xbc\xde\xf0";
        let data1 = b"\xff\xee\xdd\xcc\xbb\xaa\x99\x88";
        
        // Default scope
        db.put(&mut wtxn, None, hash1, data1)?;
        
        // Named scope for cache
        let cache_key = b"session_123456";
        let cache_data = b"user_session_data_here";
        db.put(&mut wtxn, Some("cache"), cache_key, cache_data)?;
        
        // Named scope for metrics
        let metric_key = b"cpu_usage";
        let metric_value = &50u32.to_le_bytes(); // 50 as 32-bit little-endian
        db.put(&mut wtxn, Some("metrics"), metric_key, metric_value)?;
        
        wtxn.commit()?;
    }

    // Read binary data back
    {
        let rtxn = env.read_txn()?;
        
        // Read from default scope
        let hash1 = b"\x12\x34\x56\x78\x9a\xbc\xde\xf0";
        if let Some(data) = db.get(&rtxn, None, hash1)? {
            println!("Default data: {:?}", data);
        }
        
        // Read from cache scope
        if let Some(session) = db.get(&rtxn, Some("cache"), b"session_123456")? {
            println!("Cache data: {}", std::str::from_utf8(session).unwrap_or("<binary>"));
        }
        
        // Read from metrics scope
        if let Some(cpu_bytes) = db.get(&rtxn, Some("metrics"), b"cpu_usage")? {
            if cpu_bytes.len() == 4 {
                let cpu_value = u32::from_le_bytes([cpu_bytes[0], cpu_bytes[1], cpu_bytes[2], cpu_bytes[3]]);
                println!("CPU usage: {}%", cpu_value);
            }
        }
    }

    println!("\nPure bytes database advantages:");
    println!("- No serialization overhead for keys or values");
    println!("- Direct memory operations only");
    println!("- Perfect for binary data like hashes, raw bytes");
    println!("- Optimal performance for hot paths");

    // Clean up
    drop(env);
    std::fs::remove_dir_all(db_path)?;

    Ok(())
}