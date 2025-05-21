use criterion::{Criterion, black_box, criterion_group, criterion_main};
use heed::EnvOpenOptions;
use scoped_heed::{Scope, ScopedBytesDatabase, ScopedBytesKeyDatabase, ScopedDatabase};
use tempfile::TempDir;

fn benchmark_generic_database(c: &mut Criterion) {
    let dir = TempDir::new().unwrap();
    let env = unsafe {
        EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024)
            .max_dbs(3)
            .open(dir.path())
            .unwrap()
    };

    let db: ScopedDatabase<Vec<u8>, String> = ScopedDatabase::new(&env, "bench_generic").unwrap();
    let key = b"test_key_12345".to_vec();
    let value = "test_value".to_string();

    let scope1 = Scope::named("scope1").unwrap();

    c.bench_function("generic_db_write", |b| {
        b.iter(|| {
            let mut wtxn = env.write_txn().unwrap();
            db.put(&mut wtxn, &scope1, &key, &value).unwrap();
            wtxn.commit().unwrap();
        });
    });

    // Prepare some data for read benchmark
    {
        let mut wtxn = env.write_txn().unwrap();
        db.put(&mut wtxn, &scope1, &key, &value).unwrap();
        wtxn.commit().unwrap();
    }

    c.bench_function("generic_db_read", |b| {
        b.iter(|| {
            let rtxn = env.read_txn().unwrap();
            let _result = db.get(&rtxn, &scope1, &key).unwrap();
            black_box(_result);
        });
    });
}

fn benchmark_bytes_database(c: &mut Criterion) {
    let dir = TempDir::new().unwrap();
    let env = unsafe {
        EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024)
            .max_dbs(3)
            .open(dir.path())
            .unwrap()
    };

    let db: ScopedBytesKeyDatabase<String> =
        ScopedBytesKeyDatabase::new(&env, "bench_bytes").unwrap();
    let key = b"test_key_12345";
    let value = "test_value".to_string();

    let scope1 = Scope::named("scope1").unwrap();

    c.bench_function("bytes_db_write", |b| {
        b.iter(|| {
            let mut wtxn = env.write_txn().unwrap();
            db.put(&mut wtxn, &scope1, key, &value).unwrap();
            wtxn.commit().unwrap();
        });
    });

    // Prepare some data for read benchmark
    {
        let mut wtxn = env.write_txn().unwrap();
        db.put(&mut wtxn, &scope1, key, &value).unwrap();
        wtxn.commit().unwrap();
    }

    c.bench_function("bytes_db_read", |b| {
        b.iter(|| {
            let rtxn = env.read_txn().unwrap();
            let _result = db.get(&rtxn, &scope1, key).unwrap();
            black_box(_result);
        });
    });
}

fn benchmark_key_encoding_only(c: &mut Criterion) {
    use bincode;
    use scoped_heed::{ScopedBytesCodec, ScopedKey};

    let scope_hash = 0x12345678u32;
    let key_bytes = b"test_key_12345";

    c.bench_function("bincode_encode_key", |b| {
        b.iter(|| {
            let scoped_key = ScopedKey {
                scope_hash,
                key: key_bytes.to_vec(),
            };
            let encoded = bincode::serialize(&scoped_key).unwrap();
            black_box(encoded);
        });
    });

    c.bench_function("manual_encode_key", |b| {
        b.iter(|| {
            let encoded = ScopedBytesCodec::encode(scope_hash, key_bytes);
            black_box(encoded);
        });
    });

    // Benchmark decoding
    let bincode_encoded = bincode::serialize(&ScopedKey {
        scope_hash,
        key: key_bytes.to_vec(),
    })
    .unwrap();

    let manual_encoded = ScopedBytesCodec::encode(scope_hash, key_bytes);

    c.bench_function("bincode_decode_key", |b| {
        b.iter(|| {
            let decoded: ScopedKey<Vec<u8>> = bincode::deserialize(&bincode_encoded).unwrap();
            black_box(decoded);
        });
    });

    c.bench_function("manual_decode_key", |b| {
        b.iter(|| {
            let (hash, key) = ScopedBytesCodec::decode(&manual_encoded).unwrap();
            black_box((hash, key));
        });
    });
}

fn benchmark_fully_optimized_bytes(c: &mut Criterion) {
    let dir = TempDir::new().unwrap();
    let env = unsafe {
        EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024)
            .max_dbs(3)
            .open(dir.path())
            .unwrap()
    };

    let db = ScopedBytesDatabase::new(&env, "bench_pure").unwrap();
    let key = b"test_key_12345";
    let value = b"test_value";

    let scope1 = Scope::named("scope1").unwrap();

    c.bench_function("pure_bytes_db_write", |b| {
        b.iter(|| {
            let mut wtxn = env.write_txn().unwrap();
            db.put(&mut wtxn, &scope1, key, value).unwrap();
            wtxn.commit().unwrap();
        });
    });

    // Prepare some data for read benchmark
    {
        let mut wtxn = env.write_txn().unwrap();
        db.put(&mut wtxn, &scope1, key, value).unwrap();
        wtxn.commit().unwrap();
    }

    c.bench_function("pure_bytes_db_read", |b| {
        b.iter(|| {
            let rtxn = env.read_txn().unwrap();
            let _result = db.get(&rtxn, &scope1, key).unwrap();
            black_box(_result);
        });
    });
}

criterion_group!(
    benches,
    benchmark_generic_database,
    benchmark_bytes_database,
    benchmark_fully_optimized_bytes,
    benchmark_key_encoding_only
);
criterion_main!(benches);
