use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use kvs::engines::KvsEngineBackend;
use rand::distributions::Alphanumeric;
use rand::prelude::*;
use std::collections::HashMap;
use std::time::Duration;
use tempfile::TempDir;

fn prebuilt_kvs() -> HashMap<String, String> {
    let mut rng = StdRng::from_seed([0u8; 32]);
    let mut kv_map = HashMap::with_capacity(100);
    for _ in (1..100).into_iter() {
        let k_size = rng.gen_range(1..=1000);
        let k: String = (&mut rng)
            .sample_iter(Alphanumeric)
            .take(k_size)
            .map(char::from)
            .collect();
        let v_size = rng.gen_range(1..=1000);
        let v: String = (&mut rng)
            .sample_iter(Alphanumeric)
            .take(v_size)
            .map(char::from)
            .collect();
        kv_map.insert(k, v);
    }
    kv_map
}

pub fn write(c: &mut Criterion) {
    let mut g = c.benchmark_group("write");
    g.measurement_time(Duration::from_secs(30));
    g.bench_function("kvs", |b| {
        b.iter_batched(
            || {
                let tmpdir = TempDir::new().unwrap();
                let kv_store = kvs::open(tmpdir.path(), KvsEngineBackend::Kvs).unwrap();
                let kv_map = prebuilt_kvs();
                (kv_store, kv_map, tmpdir)
            },
            |(mut kv_store, kv_map, _tmpdir)| {
                kv_map
                    .into_iter()
                    .for_each(|(k, v)| kv_store.set(k, v).unwrap());
            },
            BatchSize::SmallInput,
        );
    });
    g.bench_function("sled", |b| {
        b.iter_batched(
            || {
                let tmpdir = TempDir::new().unwrap();
                let kv_store = kvs::open(tmpdir.path(), KvsEngineBackend::Sled).unwrap();
                let kv_map = prebuilt_kvs();
                (kv_store, kv_map, tmpdir)
            },
            |(mut kv_store, kv_map, _tmpdir)| {
                kv_map
                    .into_iter()
                    .for_each(|(k, v)| kv_store.set(k, v).unwrap());
            },
            BatchSize::SmallInput,
        );
    });
    g.finish();
}

criterion_group!(benches, write);
criterion_main!(benches);
