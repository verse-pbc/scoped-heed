use scoped_heed::{scoped_database_options, ScopedDbError};
use heed::EnvOpenOptions;
use std::time::Instant;

fn main() -> Result<(), ScopedDbError> {
    // Create a test database
    let db_path = "./test_range_performance";
    if std::path::Path::new(db_path).exists() {
        std::fs::remove_dir_all(db_path).unwrap();
    }
    std::fs::create_dir_all(db_path).unwrap();

    let env = unsafe {
        EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024)
            .max_dbs(3)
            .open(db_path)?
    };

    // Create a scoped bytes database
    let mut wtxn = env.write_txn()?;
    let bytes_db = scoped_database_options(&env)
        .bytes_keys::<String>()
        .name("perf_test")
        .create(&mut wtxn)?;
    wtxn.commit()?;

    // Insert test data
    let mut wtxn = env.write_txn()?;
    
    // Insert data in multiple scopes
    for scope_id in 0..10 {
        let scope_name = format!("scope{}", scope_id);
        for i in 0..1000 {
            let key = format!("key{:06}", i).into_bytes();
            let value = format!("value_{}_{}",scope_id, i);
            bytes_db.put(&mut wtxn, Some(&scope_name), &key, &value)?;
        }
    }
    
    // Insert data in default scope
    for i in 0..1000 {
        let key = format!("key{:06}", i).into_bytes();
        let value = format!("default_value_{}", i);
        bytes_db.put(&mut wtxn, None, &key, &value)?;
    }
    
    wtxn.commit()?;

    // Test range query performance
    let rtxn = env.read_txn()?;
    
    // Range query within a scope
    let start_key = b"key000100";
    let end_key = b"key000200";
    let range = start_key.as_ref()..=end_key.as_ref();
    
    println!("Starting range query test...");
    
    // Time the range query
    let start_time = Instant::now();
    let mut count = 0;
    for result in bytes_db.range(&rtxn, Some("scope5"), &range)? {
        let (_key, _value) = result?;
        count += 1;
    }
    let duration = start_time.elapsed();
    
    println!("Range query returned {} items in {:?}", count, duration);
    println!("This uses heed's native range implementation for better performance!");
    
    // Compare with full scan (what the old implementation would do)
    let start_time = Instant::now();
    let mut count = 0;
    for result in bytes_db.iter(&rtxn, Some("scope5"))? {
        let (key, _value) = result?;
        if key >= start_key && key <= end_key {
            count += 1;
        }
    }
    let duration = start_time.elapsed();
    
    println!("\nFull scan + filter returned {} items in {:?}", count, duration);
    println!("The difference shows the benefit of using native range queries!");

    // Clean up
    drop(rtxn);
    drop(env);
    std::fs::remove_dir_all(db_path).unwrap();

    Ok(())
}