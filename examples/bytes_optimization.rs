use heed::EnvOpenOptions;
use scoped_heed::scoped_database_options;
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Document {
    id: u64,
    title: String,
    content: String,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a temporary directory for the database
    let db_path = "/tmp/scoped_heed_bytes_example";
    if std::path::Path::new(db_path).exists() {
        std::fs::remove_dir_all(db_path)?;
    }
    std::fs::create_dir_all(db_path)?;

    // Initialize the environment
    let env = unsafe {
        EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024) // 10MB
            .max_dbs(6) // need more dbs for both implementations
            .open(db_path)?
    };

    // Test generic database with Vec<u8> keys
    println!("Testing generic ScopedDatabase with Vec<u8> keys...");
    let mut wtxn = env.write_txn()?;
    let generic_db = scoped_database_options(&env)
        .types::<Vec<u8>, Document>()
        .name("docs_generic")
        .create(&mut wtxn)?;
    wtxn.commit()?;
    
    {
        let mut wtxn = env.write_txn()?;
        
        let doc1 = Document {
            id: 1,
            title: "Introduction".to_string(),
            content: "This is the introduction.".to_string(),
        };
        
        // Key as Vec<u8>
        let key = b"doc1".to_vec();
        generic_db.put(&mut wtxn, Some("tenant_a"), &key, &doc1)?;
        
        wtxn.commit()?;
    }
    
    // Test optimized bytes database
    println!("Testing optimized ScopedBytesKeyDatabase...");
    let mut wtxn = env.write_txn()?;
    let bytes_db = scoped_database_options(&env)
        .bytes_keys::<Document>()
        .name("docs_bytes")
        .create(&mut wtxn)?;
    wtxn.commit()?;
    
    {
        let mut wtxn = env.write_txn()?;
        
        let doc2 = Document {
            id: 2,
            title: "Chapter 1".to_string(),
            content: "This is chapter 1.".to_string(),
        };
        
        // Key as &[u8] - no allocation needed
        bytes_db.put(&mut wtxn, Some("tenant_b"), b"doc2", &doc2)?;
        
        wtxn.commit()?;
    }
    
    // Read data back
    {
        let rtxn = env.read_txn()?;
        
        // Read from generic database
        let key = b"doc1".to_vec();
        let doc1 = generic_db.get(&rtxn, Some("tenant_a"), &key)?;
        println!("Generic DB - doc1: {:?}", doc1);
        
        // Read from bytes database (no allocation for key)
        let doc2 = bytes_db.get(&rtxn, Some("tenant_b"), b"doc2")?;
        println!("Bytes DB - doc2: {:?}", doc2);
    }
    
    println!("\nThe ScopedBytesKeyDatabase avoids:");
    println!("1. Serde serialization overhead for keys");
    println!("2. Allocations for fixed-size keys");
    println!("3. General-purpose encoding machinery");
    println!("\nWhile maintaining byte-for-byte compatibility with the generic version!");

    // Clean up
    drop(env);
    std::fs::remove_dir_all(db_path)?;

    Ok(())
}