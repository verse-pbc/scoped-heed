use heed::EnvOpenOptions;
use scoped_heed::ScopedBytesDatabase;
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    // Create a temporary directory
    let db_path = tempfile::tempdir()?;

    // Open the environment
    let env = unsafe {
        EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024)
            .max_dbs(3)
            .open(db_path.path())?
    };

    // Create a scoped database
    let db = ScopedBytesDatabase::new(&env, "test")?;

    // Test with write transaction
    {
        // This will create a transaction with whatever Tls type heed provides
        let mut wtxn = env.write_txn()?;

        // These calls should now work with generic transaction types
        db.put(&mut wtxn, Some("scope1"), b"key1", b"value1")?;
        db.put(&mut wtxn, None, b"key2", b"value2")?;

        wtxn.commit()?;
    }

    // Test with read transaction
    {
        // This will create a transaction with whatever Tls type heed provides
        let rtxn = env.read_txn()?;

        // These calls should now work with generic transaction types
        let val1 = db.get(&rtxn, Some("scope1"), b"key1")?;
        let val2 = db.get(&rtxn, None, b"key2")?;

        assert_eq!(val1, Some(&b"value1"[..]));
        assert_eq!(val2, Some(&b"value2"[..]));

        println!("Value in scope1: {:?}", val1);
        println!("Value in default scope: {:?}", val2);
    }

    // Test iteration with generic transactions
    {
        let rtxn = env.read_txn()?;

        println!("\nIterating over scope1:");
        for result in db.iter(&rtxn, Some("scope1"))? {
            let (key, value) = result?;
            println!(
                "Key: {:?}, Value: {:?}",
                std::str::from_utf8(key).unwrap_or("<invalid>"),
                std::str::from_utf8(value).unwrap_or("<invalid>")
            );
        }

        println!("\nIterating over default scope:");
        for result in db.iter(&rtxn, None)? {
            let (key, value) = result?;
            println!(
                "Key: {:?}, Value: {:?}",
                std::str::from_utf8(key).unwrap_or("<invalid>"),
                std::str::from_utf8(value).unwrap_or("<invalid>")
            );
        }
    }

    // Test range query with generic transactions
    {
        let rtxn = env.read_txn()?;

        println!("\nRange query in scope1:");
        let range = b"k".as_slice()..b"l".as_slice();
        for result in db.range(&rtxn, Some("scope1"), &range)? {
            let (key, value) = result?;
            println!(
                "Key: {:?}, Value: {:?}",
                std::str::from_utf8(key).unwrap_or("<invalid>"),
                std::str::from_utf8(value).unwrap_or("<invalid>")
            );
        }
    }

    println!("\nAll tests passed successfully!");
    Ok(())
}
