use heed::EnvOpenOptions;
use scoped_heed::scoped_database_options;
use serde::{Deserialize, Serialize};

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
    println!("=== Performance Comparison with Scope Isolation ===\n");

    let mut wtxn = env.write_txn()?;
    let generic_db = scoped_database_options(&env)
        .types::<Vec<u8>, Document>()
        .name("docs_generic")
        .create(&mut wtxn)?;
    wtxn.commit()?;

    // Test optimized bytes database
    let mut wtxn = env.write_txn()?;
    let bytes_db = scoped_database_options(&env)
        .bytes_keys::<Document>()
        .name("docs_bytes")
        .create(&mut wtxn)?;
    wtxn.commit()?;

    // Demonstrate scope isolation with both database types
    {
        let mut wtxn = env.write_txn()?;

        // Create same document for different customers - complete isolation
        let doc_intro = Document {
            id: 1,
            title: "Customer Guide".to_string(),
            content: "Welcome to our service!".to_string(),
        };

        // Customer A gets their own version in generic DB
        let key = b"intro".to_vec();
        generic_db.put(&mut wtxn, Some("customer_a"), &key, &doc_intro)?;

        // Customer B gets a different version in generic DB
        let doc_custom = Document {
            id: 1,
            title: "Enterprise Guide".to_string(),
            content: "Welcome to the enterprise tier!".to_string(),
        };
        generic_db.put(&mut wtxn, Some("customer_b"), &key, &doc_custom)?;

        // Same isolation pattern with bytes DB
        bytes_db.put(
            &mut wtxn,
            Some("customer_a"),
            b"manual",
            &Document {
                id: 2,
                title: "User Manual".to_string(),
                content: "Basic user instructions".to_string(),
            },
        )?;

        bytes_db.put(
            &mut wtxn,
            Some("customer_b"),
            b"manual",
            &Document {
                id: 2,
                title: "Admin Manual".to_string(),
                content: "Advanced administration guide".to_string(),
            },
        )?;

        wtxn.commit()?;
    }

    // Read data back showing complete isolation
    {
        let rtxn = env.read_txn()?;

        println!("Generic DB - Scope Isolation:");
        let key = b"intro".to_vec();
        let customer_a_doc = generic_db.get(&rtxn, Some("customer_a"), &key)?;
        let customer_b_doc = generic_db.get(&rtxn, Some("customer_b"), &key)?;
        println!("  Customer A intro: {:?}", customer_a_doc.map(|d| d.title));
        println!("  Customer B intro: {:?}", customer_b_doc.map(|d| d.title));

        println!("\nBytes DB - Scope Isolation:");
        let customer_a_manual = bytes_db.get(&rtxn, Some("customer_a"), b"manual")?;
        let customer_b_manual = bytes_db.get(&rtxn, Some("customer_b"), b"manual")?;
        println!(
            "  Customer A manual: {:?}",
            customer_a_manual.map(|d| d.title)
        );
        println!(
            "  Customer B manual: {:?}",
            customer_b_manual.map(|d| d.title)
        );
    }

    println!("\n✅ Both database types provide complete scope isolation!");
    println!("\nPerformance benefits of ScopedBytesKeyDatabase:");
    println!("1. No serialization overhead for keys");
    println!("2. Zero allocations for fixed-size keys");
    println!("3. Direct memory operations");
    println!("\nWhile maintaining the same Redis-like isolation semantics!");

    // Clean up
    drop(env);
    std::fs::remove_dir_all(db_path)?;

    Ok(())
}
