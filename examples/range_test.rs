use scoped_heed::{ScopedBytesDatabase, ScopedBytesKeyDatabase};
use heed::EnvOpenOptions;
use tempfile::TempDir;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a temporary directory for the test
    let dir = TempDir::new()?;
    let env = unsafe {
        EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024)
            .max_dbs(3)
            .open(dir.path())?
    };

    // Test ScopedBytesDatabase
    {
        let db = ScopedBytesDatabase::new(&env, "test_bytes")?;
        let mut wtxn = env.write_txn()?;
        
        // Insert test data in default scope
        db.put(&mut wtxn, None, b"apple", b"value1")?;
        db.put(&mut wtxn, None, b"banana", b"value2")?;
        db.put(&mut wtxn, None, b"cherry", b"value3")?;
        db.put(&mut wtxn, None, b"date", b"value4")?;
        
        // Insert test data in scoped database
        db.put(&mut wtxn, Some("fruit"), b"apple", b"scoped_value1")?;
        db.put(&mut wtxn, Some("fruit"), b"banana", b"scoped_value2")?;
        db.put(&mut wtxn, Some("fruit"), b"cherry", b"scoped_value3")?;
        db.put(&mut wtxn, Some("fruit"), b"date", b"scoped_value4")?;
        
        wtxn.commit()?;
        
        // Test range query on default scope
        let rtxn = env.read_txn()?;
        let range = b"apple".as_ref()..=b"cherry".as_ref();
        
        println!("Testing range query on default scope:");
        for result in db.range(&rtxn, None, &range)? {
            let (key, value) = result?;
            println!("  {} -> {}", 
                String::from_utf8_lossy(key), 
                String::from_utf8_lossy(value));
        }
        
        println!("\nTesting range query on scoped database:");
        for result in db.range(&rtxn, Some("fruit"), &range)? {
            let (key, value) = result?;
            println!("  {} -> {}", 
                String::from_utf8_lossy(key), 
                String::from_utf8_lossy(value));
        }
    }

    // Test ScopedBytesKeyDatabase
    {
        let db: ScopedBytesKeyDatabase<String> = ScopedBytesKeyDatabase::new(&env, "test_bytes_key")?;
        let mut wtxn = env.write_txn()?;
        
        // Insert test data in default scope
        db.put(&mut wtxn, None, b"apple", &"value1".to_string())?;
        db.put(&mut wtxn, None, b"banana", &"value2".to_string())?;
        db.put(&mut wtxn, None, b"cherry", &"value3".to_string())?;
        db.put(&mut wtxn, None, b"date", &"value4".to_string())?;
        
        wtxn.commit()?;
        
        // Test range query
        let rtxn = env.read_txn()?;
        let range = b"banana".as_ref()..b"date".as_ref();
        
        println!("\nTesting ScopedBytesKeyDatabase range query:");
        for result in db.range(&rtxn, None, &range)? {
            let (key, value) = result?;
            println!("  {} -> {}", 
                String::from_utf8_lossy(key), 
                value);
        }
    }
    
    println!("\nAll range tests completed successfully!");
    Ok(())
}