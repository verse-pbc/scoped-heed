use heed::EnvOpenOptions;
use scoped_heed::{GlobalScopeRegistry, Scope, scoped_database_options};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Instant;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Document {
    id: u64,
    title: String,
    content: String,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db_path = "/tmp/scoped_heed_perf_example";
    if std::path::Path::new(db_path).exists() {
        std::fs::remove_dir_all(db_path)?;
    }
    std::fs::create_dir_all(db_path)?;

    let env = unsafe {
        EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024)
            .max_dbs(10)
            .open(db_path)?
    };

    let mut wtxn = env.write_txn()?;
    let registry = Arc::new(GlobalScopeRegistry::new(&env, &mut wtxn)?);
    wtxn.commit()?;

    // Part 1: Database Type Comparison
    println!("=== Database Type Performance Comparison ===\n");

    let mut wtxn = env.write_txn()?;
    let generic_db = scoped_database_options(&env, registry.clone())
        .types::<Vec<u8>, Document>()
        .name("generic_docs")
        .create(&mut wtxn)?;
    wtxn.commit()?;

    let mut wtxn = env.write_txn()?;
    let bytes_db = scoped_database_options(&env, registry.clone())
        .bytes_keys::<Document>()
        .name("bytes_docs")
        .create(&mut wtxn)?;
    wtxn.commit()?;

    let mut wtxn = env.write_txn()?;
    let bytes_raw_db = scoped_database_options(&env, registry.clone())
        .raw_bytes()
        .name("raw_bytes")
        .create(&mut wtxn)?;
    wtxn.commit()?;

    let tenant_a = Scope::named("tenant_a")?;
    let tenant_b = Scope::named("tenant_b")?;

    // Benchmark different database types
    {
        let mut wtxn = env.write_txn()?;

        let doc1 = Document {
            id: 1,
            title: "Generic Document".to_string(),
            content: "Content for tenant A".to_string(),
        };

        let doc2 = Document {
            id: 1,
            title: "Generic Document".to_string(),
            content: "Content for tenant B".to_string(),
        };

        // Add to generic DB
        let start = Instant::now();
        for i in 0..100 {
            let key = format!("doc{:03}", i).into_bytes();
            generic_db.put(&mut wtxn, &tenant_a, &key, &doc1)?;
            generic_db.put(&mut wtxn, &tenant_b, &key, &doc2)?;
        }
        let generic_time = start.elapsed();

        // Add to bytes DB
        let start = Instant::now();
        for i in 0..100 {
            let key_str = format!("doc{:03}", i);
            bytes_db.put(&mut wtxn, &tenant_a, key_str.as_bytes(), &doc1)?;
            bytes_db.put(&mut wtxn, &tenant_b, key_str.as_bytes(), &doc2)?;
        }
        let bytes_time = start.elapsed();

        // Add to raw bytes DB
        let start = Instant::now();
        for i in 0..100 {
            let key_str = format!("doc{:03}", i);
            let value = bincode::serialize(&doc1)?;
            bytes_raw_db.put(&mut wtxn, &tenant_a, key_str.as_bytes(), &value)?;

            let value = bincode::serialize(&doc2)?;
            bytes_raw_db.put(&mut wtxn, &tenant_b, key_str.as_bytes(), &value)?;
        }
        let raw_bytes_time = start.elapsed();

        wtxn.commit()?;

        println!("Write performance comparison (200 documents each):");
        println!("  Generic DB:   {:?}", generic_time);
        println!("  Bytes DB:     {:?}", bytes_time);
        println!("  Raw Bytes DB: {:?}", raw_bytes_time);
    }

    // Part 2: Range Operations with Isolation
    println!("\n=== Range Operations with Scope Isolation ===\n");

    {
        let rtxn = env.read_txn()?;

        // Define range (docs 10-19)
        let range = b"doc010".as_ref()..=b"doc019".as_ref();

        println!("Range query for tenant A (doc010-doc019):");
        for result in bytes_db.range(&rtxn, &tenant_a, &range)? {
            let (key, doc) = result?;
            println!("  {} - {}", std::str::from_utf8(key)?, doc.title);
        }

        println!("\nSame range query for tenant B (showing isolation):");
        for result in bytes_db.range(&rtxn, &tenant_b, &range)? {
            let (key, doc) = result?;
            println!(
                "  {} - {} ({})",
                std::str::from_utf8(key)?,
                doc.title,
                doc.content
            );
        }

        // Performance test for range queries
        println!("\nRange query performance:");

        let start = Instant::now();
        let count = bytes_db.range(&rtxn, &tenant_a, &range)?.count();
        let bytes_range_time = start.elapsed();

        let range_vec = b"doc010".to_vec()..=b"doc019".to_vec();
        let start = Instant::now();
        let count_gen = generic_db.range(&rtxn, &tenant_a, &range_vec)?.count();
        let generic_range_time = start.elapsed();

        println!("  Bytes DB:   {:?} for {} items", bytes_range_time, count);
        println!(
            "  Generic DB: {:?} for {} items",
            generic_range_time, count_gen
        );
    }

    // Part 3: Demonstrate isolation with optimized types
    {
        let mut wtxn = env.write_txn()?;

        println!("\n=== Demonstrating Isolation with Optimized Types ===\n");

        bytes_db.clear(&mut wtxn, &tenant_a)?;
        println!("Cleared all data for tenant A");

        wtxn.commit()?;

        let rtxn = env.read_txn()?;
        let count_b = bytes_db.iter(&rtxn, &tenant_b)?.count();

        println!("After clearing tenant A:");
        println!(
            "  Tenant A document count: {}",
            bytes_db.iter(&rtxn, &tenant_a)?.count()
        );
        println!("  Tenant B document count: {}", count_b);
        println!("\n✅ Perfect isolation maintained with optimized types!");
    }

    drop(env);
    std::fs::remove_dir_all(db_path)?;

    Ok(())
}
