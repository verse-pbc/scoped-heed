use criterion::{Criterion, black_box, criterion_group, criterion_main};
use heed::EnvOpenOptions;
use scoped_heed::{Scope, ScopedBytesDatabase, ScopedDatabase};
use tempfile::TempDir;

fn benchmark_scope_creation(c: &mut Criterion) {
    c.bench_function("scope_creation_from_str", |b| {
        b.iter(|| {
            let scope = Scope::named("tenant_with_very_long_name").unwrap();
            black_box(scope);
        });
    });

    c.bench_function("scope_creation_from_option_str", |b| {
        b.iter(|| {
            let scope_opt: Scope = Some("tenant_with_very_long_name").into();
            black_box(scope_opt);
        });
    });
}

fn benchmark_scope_enum_vs_option_str(c: &mut Criterion) {
    let dir = TempDir::new().unwrap();
    let env = unsafe {
        EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024)
            .max_dbs(3)
            .open(dir.path())
            .unwrap()
    };

    let db: ScopedDatabase<Vec<u8>, String> = ScopedDatabase::new(&env, "bench_enum").unwrap();
    let key = b"test_key_12345".to_vec();
    let value = "test_value".to_string();

    // Create scope object
    let tenant_scope = Scope::named("tenant_scope").unwrap();

    // ---- Write benchmarks ----

    // 1. Using Option<&str> API
    c.bench_function("write_option_str_api", |b| {
        b.iter(|| {
            let mut wtxn = env.write_txn().unwrap();
            db.put(&mut wtxn, &tenant_scope, &key, &value).unwrap();
            wtxn.commit().unwrap();
        });
    });

    // 2. Using Scope enum API
    c.bench_function("write_scope_enum_api", |b| {
        b.iter(|| {
            let mut wtxn = env.write_txn().unwrap();
            db.put(&mut wtxn, &tenant_scope, &key, &value).unwrap();
            wtxn.commit().unwrap();
        });
    });

    // ---- Read benchmarks ----

    // Prepare some data
    {
        let mut wtxn = env.write_txn().unwrap();
        db.put(&mut wtxn, &tenant_scope, &key, &value).unwrap();
        wtxn.commit().unwrap();
    }

    // 1. Using Option<&str> API
    c.bench_function("read_option_str_api", |b| {
        b.iter(|| {
            let rtxn = env.read_txn().unwrap();
            let _result = db.get(&rtxn, &tenant_scope, &key).unwrap();
            black_box(_result);
        });
    });

    // 2. Using Scope enum API
    c.bench_function("read_scope_enum_api", |b| {
        b.iter(|| {
            let rtxn = env.read_txn().unwrap();
            let _result = db.get(&rtxn, &tenant_scope, &key).unwrap();
            black_box(_result);
        });
    });
}

fn benchmark_scope_enum_with_bytes_db(c: &mut Criterion) {
    let dir = TempDir::new().unwrap();
    let env = unsafe {
        EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024)
            .max_dbs(3)
            .open(dir.path())
            .unwrap()
    };

    let db = ScopedBytesDatabase::new(&env, "bench_bytes_enum").unwrap();
    let key = b"test_key_12345";
    let value = b"test_value";

    // Create scope object
    let tenant_scope = Scope::named("tenant_scope").unwrap();

    // ---- Write benchmarks ----

    // 1. Using Option<&str> API
    c.bench_function("bytes_write_option_str", |b| {
        b.iter(|| {
            let mut wtxn = env.write_txn().unwrap();
            db.put(&mut wtxn, &tenant_scope, key, value).unwrap();
            wtxn.commit().unwrap();
        });
    });

    // 2. Using Scope enum API
    c.bench_function("bytes_write_scope_enum", |b| {
        b.iter(|| {
            let mut wtxn = env.write_txn().unwrap();
            db.put(&mut wtxn, &tenant_scope, key, value).unwrap();
            wtxn.commit().unwrap();
        });
    });

    // ---- Read benchmarks ----

    // Prepare some data
    {
        let mut wtxn = env.write_txn().unwrap();
        db.put(&mut wtxn, &tenant_scope, key, value).unwrap();
        wtxn.commit().unwrap();
    }

    // 1. Using Option<&str> API
    c.bench_function("bytes_read_option_str", |b| {
        b.iter(|| {
            let rtxn = env.read_txn().unwrap();
            let _result = db.get(&rtxn, &tenant_scope, key).unwrap();
            black_box(_result);
        });
    });

    // 2. Using Scope enum API
    c.bench_function("bytes_read_scope_enum", |b| {
        b.iter(|| {
            let rtxn = env.read_txn().unwrap();
            let _result = db.get(&rtxn, &tenant_scope, key).unwrap();
            black_box(_result);
        });
    });
}

fn benchmark_hash_performance(c: &mut Criterion) {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::Hasher;
    use twox_hash::XxHash32;

    let scope_name = "tenant_with_a_very_long_name_for_benchmarking";

    // Benchmark DefaultHasher (used in the legacy ScopeHasher)
    c.bench_function("std_default_hasher", |b| {
        b.iter(|| {
            let mut hasher = DefaultHasher::new();
            hasher.write(scope_name.as_bytes());
            let hash = hasher.finish() as u32;
            black_box(hash);
        });
    });

    // Benchmark XxHash32 (used in the new Scope enum)
    c.bench_function("xxhash32", |b| {
        b.iter(|| {
            let mut hasher = XxHash32::default();
            hasher.write(scope_name.as_bytes());
            let hash = hasher.finish() as u32;
            black_box(hash);
        });
    });
}

criterion_group!(
    benches,
    benchmark_scope_creation,
    benchmark_scope_enum_vs_option_str,
    benchmark_scope_enum_with_bytes_db,
    benchmark_hash_performance
);
criterion_main!(benches);
