use heed::EnvOpenOptions;
use scoped_heed::{GlobalScopeRegistry, Scope, scoped_database_options};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Barrier};
use std::thread;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProcessingTask {
    id: u32,
    status: String,
    data: Vec<u32>,
    result: Option<u32>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db_path = "/tmp/parallel_isolation_example";
    if std::path::Path::new(db_path).exists() {
        std::fs::remove_dir_all(db_path)?;
    }
    std::fs::create_dir_all(db_path)?;

    type ThreadResult = Result<(), Box<dyn std::error::Error + Send + Sync + 'static>>;

    let env = Arc::new(unsafe {
        EnvOpenOptions::new()
            .map_size(20 * 1024 * 1024)
            .max_dbs(5)
            .open(db_path)?
    });

    let mut wtxn = env.write_txn()?;
    let registry = Arc::new(GlobalScopeRegistry::new(&env, &mut wtxn)?);
    wtxn.commit()?;

    let mut wtxn = env.write_txn()?;
    let tasks_db = Arc::new(
        scoped_database_options(&env, registry.clone())
            .types::<u32, ProcessingTask>()
            .name("tasks")
            .create(&mut wtxn)?,
    );
    wtxn.commit()?;

    println!("=== Parallel Processing with Scope Isolation ===\n");

    // Create initial tasks in default scope
    {
        let mut wtxn = env.write_txn()?;

        for i in 1..=10 {
            let task = ProcessingTask {
                id: i,
                status: "pending".to_string(),
                data: (1..=i).collect(),
                result: None,
            };

            tasks_db.put(&mut wtxn, &Scope::Default, &i, &task)?;
        }

        wtxn.commit()?;
        println!("Created 10 processing tasks in default scope");
    }

    const WORKER_COUNT: usize = 3;
    let barrier = Arc::new(Barrier::new(WORKER_COUNT));
    let mut handles = vec![];

    // Launch worker threads - each with its own scope
    for worker_id in 1..=WORKER_COUNT {
        let env = Arc::clone(&env);
        let tasks_db = Arc::clone(&tasks_db);
        let barrier = Arc::clone(&barrier);

        let handle = thread::spawn(move || -> ThreadResult {
            let worker_scope = Scope::named(&format!("worker_{}", worker_id))
                .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync + 'static>)?;
            println!(
                "Worker {} started with scope: {}",
                worker_id,
                worker_scope.name().unwrap_or("default")
            );

            // Assign tasks to this worker
            {
                let rtxn = env.read_txn().map_err(|e| {
                    Box::new(e) as Box<dyn std::error::Error + Send + Sync + 'static>
                })?;
                let mut assigned_tasks = vec![];

                for result in tasks_db.iter(&rtxn, &Scope::Default).map_err(|e| {
                    Box::new(e) as Box<dyn std::error::Error + Send + Sync + 'static>
                })? {
                    let (task_id, task) = result.map_err(|e| {
                        Box::new(e) as Box<dyn std::error::Error + Send + Sync + 'static>
                    })?;
                    if task_id % (WORKER_COUNT as u32) == (worker_id as u32 - 1) {
                        assigned_tasks.push((task_id, task));
                    }
                }

                println!(
                    "Worker {} assigned {} tasks: {:?}",
                    worker_id,
                    assigned_tasks.len(),
                    assigned_tasks.iter().map(|(id, _)| id).collect::<Vec<_>>()
                );

                let mut wtxn = env.write_txn().map_err(|e| {
                    Box::new(e) as Box<dyn std::error::Error + Send + Sync + 'static>
                })?;
                for (task_id, mut task) in assigned_tasks {
                    task.status = "assigned".to_string();
                    tasks_db
                        .put(&mut wtxn, &worker_scope, &task_id, &task)
                        .map_err(|e| {
                            Box::new(e) as Box<dyn std::error::Error + Send + Sync + 'static>
                        })?;
                }
                wtxn.commit().map_err(|e| {
                    Box::new(e) as Box<dyn std::error::Error + Send + Sync + 'static>
                })?;
            }

            barrier.wait();

            // Process tasks
            {
                let mut wtxn = env.write_txn().map_err(|e| {
                    Box::new(e) as Box<dyn std::error::Error + Send + Sync + 'static>
                })?;

                let rtxn = env.read_txn().map_err(|e| {
                    Box::new(e) as Box<dyn std::error::Error + Send + Sync + 'static>
                })?;
                for result in tasks_db.iter(&rtxn, &worker_scope).map_err(|e| {
                    Box::new(e) as Box<dyn std::error::Error + Send + Sync + 'static>
                })? {
                    let (task_id, mut task) = result.map_err(|e| {
                        Box::new(e) as Box<dyn std::error::Error + Send + Sync + 'static>
                    })?;

                    println!("Worker {} processing task {}", worker_id, task_id);
                    thread::sleep(std::time::Duration::from_millis(100 * u64::from(task_id)));

                    let sum: u32 = task.data.iter().sum();
                    task.result = Some(sum);
                    task.status = "completed".to_string();

                    tasks_db
                        .put(&mut wtxn, &worker_scope, &task_id, &task)
                        .map_err(|e| {
                            Box::new(e) as Box<dyn std::error::Error + Send + Sync + 'static>
                        })?;
                }

                wtxn.commit().map_err(|e| {
                    Box::new(e) as Box<dyn std::error::Error + Send + Sync + 'static>
                })?;
            }

            barrier.wait();

            // Return results to default scope
            {
                let mut wtxn = env.write_txn().map_err(|e| {
                    Box::new(e) as Box<dyn std::error::Error + Send + Sync + 'static>
                })?;
                let rtxn = env.read_txn().map_err(|e| {
                    Box::new(e) as Box<dyn std::error::Error + Send + Sync + 'static>
                })?;

                for result in tasks_db.iter(&rtxn, &worker_scope).map_err(|e| {
                    Box::new(e) as Box<dyn std::error::Error + Send + Sync + 'static>
                })? {
                    let (task_id, task) = result.map_err(|e| {
                        Box::new(e) as Box<dyn std::error::Error + Send + Sync + 'static>
                    })?;

                    tasks_db
                        .put(&mut wtxn, &Scope::Default, &task_id, &task)
                        .map_err(|e| {
                            Box::new(e) as Box<dyn std::error::Error + Send + Sync + 'static>
                        })?;
                    println!(
                        "Worker {} completed task {} with result: {:?}",
                        worker_id, task_id, task.result
                    );
                }

                wtxn.commit().map_err(|e| {
                    Box::new(e) as Box<dyn std::error::Error + Send + Sync + 'static>
                })?;
            }

            // Clean up worker's scope
            {
                let mut wtxn = env.write_txn().map_err(|e| {
                    Box::new(e) as Box<dyn std::error::Error + Send + Sync + 'static>
                })?;
                tasks_db.clear(&mut wtxn, &worker_scope).map_err(|e| {
                    Box::new(e) as Box<dyn std::error::Error + Send + Sync + 'static>
                })?;
                println!("Worker {} cleaned up tasks", worker_id);
                wtxn.commit().map_err(|e| {
                    Box::new(e) as Box<dyn std::error::Error + Send + Sync + 'static>
                })?;
            }

            Ok(())
        });

        handles.push(handle);
    }

    // Wait for all worker threads to complete
    for handle in handles {
        match handle.join() {
            Ok(result) => {
                if let Err(e) = result {
                    eprintln!("Worker error: {}", e);
                }
            }
            Err(e) => eprintln!("Thread join error: {:?}", e),
        }
    }

    // Verify all tasks are completed
    {
        let rtxn = env.read_txn()?;
        println!("\nFinal task status:");

        for result in tasks_db.iter(&rtxn, &Scope::Default)? {
            let (task_id, task) = result?;
            println!(
                "Task {}: status={}, result={:?}",
                task_id, task.status, task.result
            );
        }

        println!("\n✅ All tasks processed successfully with scope isolation!");
    }

    drop(env);
    std::fs::remove_dir_all(db_path)?;

    Ok(())
}
