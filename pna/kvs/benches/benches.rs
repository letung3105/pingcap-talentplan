use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, Bencher, Criterion, Throughput,
};
use kvs::engines::KvsEngineBackend;
use rand::distributions::Alphanumeric;
use rand::prelude::*;
use tempfile::TempDir;

criterion_main!(benches);
criterion_group! {
    name = benches;
    config = Criterion::default().significance_level(0.05).sample_size(1000);
    targets = engines::write, engines::read
}

mod engines {
    use kvs::{KvStore, KvsEngine, SledKvsEngine};

    use super::*;

    const KEY_SIZE: usize = 1000;
    const VAL_SIZE: usize = 1000;

    pub fn write(c: &mut Criterion) {
        let bench =
            |b: &mut Bencher,
             &(engine_backend, key_size, val_size): &(KvsEngineBackend, usize, usize)| {
                let tmpdir = TempDir::new().unwrap();
                let mut kvs_engine: Box<dyn KvsEngine> = match engine_backend {
                    KvsEngineBackend::Kvs => Box::new(KvStore::open(tmpdir.path()).unwrap()),
                    KvsEngineBackend::Sled => Box::new(SledKvsEngine::open(tmpdir.path()).unwrap()),
                };
                let mut rng = StdRng::from_seed([0u8; 32]);

                b.iter_batched(
                    || rand_key_value(&mut rng, key_size, val_size),
                    |(k, v)| {
                        kvs_engine.set(black_box(k), black_box(v)).unwrap();
                    },
                    BatchSize::SmallInput,
                );
            };

        let mut g = c.benchmark_group("write");
        g.throughput(Throughput::Bytes((KEY_SIZE + VAL_SIZE) as u64));
        g.bench_with_input("kvs", &(KvsEngineBackend::Kvs, KEY_SIZE, VAL_SIZE), bench);
        g.throughput(Throughput::Bytes((KEY_SIZE + VAL_SIZE) as u64));
        g.bench_with_input("sled", &(KvsEngineBackend::Sled, KEY_SIZE, VAL_SIZE), bench);
        g.finish();
    }

    pub fn read(c: &mut Criterion) {
        let bench =
            |b: &mut Bencher,
             &(engine_backend, kv_pairs): &(KvsEngineBackend, &Vec<(String, String)>)| {
                let tmpdir = TempDir::new().unwrap();
                let mut kvs_engine: Box<dyn KvsEngine> = match engine_backend {
                    KvsEngineBackend::Kvs => Box::new(KvStore::open(tmpdir.path()).unwrap()),
                    KvsEngineBackend::Sled => Box::new(SledKvsEngine::open(tmpdir.path()).unwrap()),
                };
                let mut rng = StdRng::from_seed([0u8; 32]);
                kv_pairs
                    .iter()
                    .for_each(|(k, v)| kvs_engine.set(k.clone(), v.clone()).unwrap());

                b.iter_batched(
                    || kv_pairs.choose(&mut rng).cloned().unwrap(),
                    |(k, v)| {
                        assert_eq!(Some(v), kvs_engine.get(black_box(k)).unwrap());
                    },
                    BatchSize::SmallInput,
                );
            };

        let mut rng = StdRng::from_seed([0u8; 32]);
        let kv_pairs = prebuilt_kv_pairs(&mut rng, 100, KEY_SIZE, VAL_SIZE);

        let mut g = c.benchmark_group("read");
        g.throughput(Throughput::Bytes(KEY_SIZE as u64));
        g.bench_with_input("kvs", &(KvsEngineBackend::Kvs, &kv_pairs), bench);
        g.throughput(Throughput::Bytes((KEY_SIZE + VAL_SIZE) as u64));
        g.bench_with_input("sled", &(KvsEngineBackend::Sled, &kv_pairs), bench);
        g.finish();
    }

    fn prebuilt_kv_pairs<R>(
        rng: &mut R,
        size: usize,
        key_size: usize,
        val_size: usize,
    ) -> Vec<(String, String)>
    where
        R: Rng,
    {
        (0..size)
            .into_iter()
            .map(|_| rand_key_value(rng, key_size, val_size))
            .collect()
    }

    fn rand_key_value<R>(rng: &mut R, key_size: usize, val_size: usize) -> (String, String)
    where
        R: Rng,
    {
        let key: String = rng
            .sample_iter(Alphanumeric)
            .take(key_size)
            .map(char::from)
            .collect();
        let val: String = rng
            .sample_iter(Alphanumeric)
            .take(val_size)
            .map(char::from)
            .collect();
        (key, val)
    }
}
