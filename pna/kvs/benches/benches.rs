use criterion::{criterion_group, criterion_main};

criterion_group!(benches, engines::write);
criterion_main!(benches);

mod engines {
    use criterion::{BatchSize, Criterion};
    use kvs::{KvStore, KvsEngine, SledKvsEngine};
    use rand::distributions::Alphanumeric;
    use rand::prelude::*;
    use std::collections::HashMap;
    use tempfile::TempDir;

    pub fn write(c: &mut Criterion) {
        let mut rng = StdRng::from_seed([0u8; 32]);
        let kv_map = prebuilt_kv_map(&mut rng, 100, 100000, 100000);

        let mut g = c.benchmark_group("write");
        g.bench_function("kvs", |b| {
            b.iter_batched(
                || {
                    let tmpdir = TempDir::new().unwrap();
                    let kv_store = KvStore::open(tmpdir.path()).unwrap();
                    // collect tmpdir so that it is only going to be dropped when to benchmarch ends
                    (kv_store, tmpdir)
                },
                |(mut kv_store, _tmpdir)| {
                    kv_map
                        .iter()
                        .for_each(|(k, v)| kv_store.set(k.clone(), v.clone()).unwrap())
                },
                BatchSize::SmallInput,
            );
        });
        g.bench_function("sled", |b| {
            b.iter_batched(
                || {
                    let tmpdir = TempDir::new().unwrap();
                    let kv_store = SledKvsEngine::open(tmpdir.path()).unwrap();
                    // collect tmpdir so that it is only going to be dropped when to benchmarch ends
                    (kv_store, tmpdir)
                },
                |(mut kv_store, _tmpdir)| {
                    kv_map
                        .iter()
                        .for_each(|(k, v)| kv_store.set(k.clone(), v.clone()).unwrap())
                },
                BatchSize::SmallInput,
            );
        });
        g.finish();
    }

    fn prebuilt_kv_map<R>(
        rng: &mut R,
        size: usize,
        max_key_size: usize,
        max_val_size: usize,
    ) -> HashMap<String, String>
    where
        R: Rng,
    {
        let mut kv_map = HashMap::with_capacity(size);
        for _ in (0..size).into_iter() {
            let (k, v) = rand_key_value(rng, max_key_size, max_val_size);
            kv_map.insert(k, v);
        }
        kv_map
    }

    fn rand_key_value<R>(rng: &mut R, max_key_size: usize, max_val_size: usize) -> (String, String)
    where
        R: Rng,
    {
        let k_size = rng.gen_range(1..=max_key_size);
        let k: String = rng
            .sample_iter(Alphanumeric)
            .take(k_size)
            .map(char::from)
            .collect();

        let v_size = rng.gen_range(1..=max_val_size);
        let v: String = rng
            .sample_iter(Alphanumeric)
            .take(v_size)
            .map(char::from)
            .collect();

        (k, v)
    }
}
