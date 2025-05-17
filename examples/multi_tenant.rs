use std::fs;
use std::path::Path;

use heed::EnvOpenOptions;
use scoped_heed::{ScopedDatabase, ScopedDbError};

fn main() -> Result<(), ScopedDbError> {
    // Create database directory
    let db_path = Path::new("./example_multi_tenant_db");
    fs::create_dir_all(db_path)
        .map_err(|e| ScopedDbError::InvalidInput(format!("Failed to create DB dir: {}", e)))?;

    // Open environment with more max_dbs for multiple tenants
    let env = unsafe {
        EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024) // 10MB
            .max_dbs(10) // Allow for multiple tenants
            .open(db_path)?
    };

    let scoped_db = ScopedDatabase::new(&env)?;

    // Simulate multi-tenant application with different customer data
    let tenants = vec!["acme_corp", "techstartup_inc", "bigcorp_ltd"];
    
    // Store user data for each tenant
    println!("Storing data for multiple tenants...");
    {
        let mut wtxn = env.write_txn()?;
        
        // Acme Corp users
        scoped_db.put(&mut wtxn, Some("acme_corp"), "user:alice", "alice@acme.com")?;
        scoped_db.put(&mut wtxn, Some("acme_corp"), "user:bob", "bob@acme.com")?;
        scoped_db.put(&mut wtxn, Some("acme_corp"), "config:theme", "blue")?;
        
        // TechStartup Inc users
        scoped_db.put(&mut wtxn, Some("techstartup_inc"), "user:charlie", "charlie@techstartup.com")?;
        scoped_db.put(&mut wtxn, Some("techstartup_inc"), "user:diana", "diana@techstartup.com")?;
        scoped_db.put(&mut wtxn, Some("techstartup_inc"), "config:theme", "green")?;
        
        // BigCorp Ltd users
        scoped_db.put(&mut wtxn, Some("bigcorp_ltd"), "user:eve", "eve@bigcorp.com")?;
        scoped_db.put(&mut wtxn, Some("bigcorp_ltd"), "user:frank", "frank@bigcorp.com")?;
        scoped_db.put(&mut wtxn, Some("bigcorp_ltd"), "user:grace", "grace@bigcorp.com")?;
        scoped_db.put(&mut wtxn, Some("bigcorp_ltd"), "config:theme", "red")?;
        
        wtxn.commit()?;
    }

    // Query data for specific tenant
    println!("\nQuerying tenant-specific data:");
    {
        let rtxn = env.read_txn()?;
        
        // Get all users for acme_corp
        println!("\nAcme Corp users:");
        for result in scoped_db.iter(&rtxn, Some("acme_corp"))? {
            let (key, value) = result?;
            if key.starts_with("user:") {
                println!("  {} -> {}", key, value);
            }
        }
        
        // Get theme configuration for each tenant
        println!("\nTheme configurations:");
        for tenant in &tenants {
            let theme = scoped_db.get(&rtxn, Some(tenant), "config:theme")?;
            println!("  {} theme: {:?}", tenant, theme);
        }
    }

    // Update tenant-specific configuration
    println!("\nUpdating TechStartup Inc theme to 'dark'");
    {
        let mut wtxn = env.write_txn()?;
        scoped_db.put(&mut wtxn, Some("techstartup_inc"), "config:theme", "dark")?;
        wtxn.commit()?;
    }

    // Remove all data for a specific tenant
    println!("\nRemoving all data for BigCorp Ltd (tenant offboarding)");
    {
        let mut wtxn = env.write_txn()?;
        let deleted_count = scoped_db.clear_scope(&mut wtxn, Some("bigcorp_ltd"))?;
        println!("  Deleted {} entries for BigCorp Ltd", deleted_count);
        wtxn.commit()?;
    }

    // Verify deletion
    {
        let rtxn = env.read_txn()?;
        let count = scoped_db.iter(&rtxn, Some("bigcorp_ltd"))?.count();
        println!("\nEntries remaining for BigCorp Ltd: {}", count);
        
        // Other tenants should remain intact
        let acme_count = scoped_db.iter(&rtxn, Some("acme_corp"))?.count();
        let tech_count = scoped_db.iter(&rtxn, Some("techstartup_inc"))?.count();
        println!("Entries for Acme Corp: {}", acme_count);
        println!("Entries for TechStartup Inc: {}", tech_count);
    }

    println!("\nMulti-tenant example completed successfully!");
    
    // Clean up
    let _ = fs::remove_dir_all(db_path);
    
    Ok(())
}