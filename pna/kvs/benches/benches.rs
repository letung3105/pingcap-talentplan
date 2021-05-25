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
    use super::*;

    // NOTE: Running the benchmarks with a maximum key/value size of 100000 as stated in the project
    // requirements will cause sled to hang, this might be because of we are flushing the in-memory
    // data on every write/remove
    const MAX_KEY_SIZE: usize = 100000;
    const MAX_VAL_SIZE: usize = 100000;

    pub fn write(c: &mut Criterion) {
        let mut rng = StdRng::from_seed([0u8; 32]);
        let (kv_pairs, bytes) = prebuilt_kv_pairs(&mut rng, 1000, MAX_KEY_SIZE, MAX_VAL_SIZE);

        let mut g = c.benchmark_group("engines::write");
        g.throughput(Throughput::Bytes(bytes as u64));
        g.bench_with_input("kvs", &(KvsEngineBackend::Kvs, &kv_pairs), write_bench);
        g.bench_with_input("sled", &(KvsEngineBackend::Sled, &kv_pairs), write_bench);
        g.finish();
    }

    fn write_bench(
        b: &mut Bencher,
        &(engine_backend, kv_pairs): &(KvsEngineBackend, &Vec<(String, String)>),
    ) {
        b.iter_batched(
            || {
                let tmpdir = TempDir::new().unwrap();
                let kv_store = kvs::open(tmpdir.path(), engine_backend).unwrap();
                // collect tmpdir so that it is only going to be dropped when to benchmarch ends
                (kv_store, kv_pairs.clone(), tmpdir)
            },
            |(mut kv_store, kv_pairs, _tmpdir)| {
                kv_pairs
                    .into_iter()
                    .for_each(|(k, v)| kv_store.set(black_box(k), black_box(v)).unwrap());
            },
            BatchSize::LargeInput,
        );
    }

    pub fn read(c: &mut Criterion) {
        let mut rng = StdRng::from_seed([0u8; 32]);
        let (kv_pairs, bytes) = prebuilt_kv_pairs(&mut rng, 1000, MAX_KEY_SIZE, MAX_VAL_SIZE);

        let mut g = c.benchmark_group("engines::read");
        g.throughput(Throughput::Bytes(bytes as u64));
        g.bench_with_input("kvs", &(KvsEngineBackend::Kvs, &kv_pairs), read_bench);
        g.bench_with_input("sled", &(KvsEngineBackend::Sled, &kv_pairs), read_bench);
        g.finish();
    }

    fn read_bench(
        b: &mut Bencher,
        &(engine_backend, kv_pairs): &(KvsEngineBackend, &Vec<(String, String)>),
    ) {
        let tmpdir = TempDir::new().unwrap();
        let mut kv_store = kvs::open(tmpdir.path(), engine_backend).unwrap();
        kv_pairs
            .iter()
            .for_each(|(k, v)| kv_store.set(k.clone(), v.clone()).unwrap());

        let mut shuffling_rng = StdRng::from_seed([0u8; 32]);
        b.iter_batched(
            || {
                let mut shuffled_kv_pairs = kv_pairs.clone();
                shuffled_kv_pairs.shuffle(&mut shuffling_rng);
                shuffled_kv_pairs
            },
            |shuffled_kv_pairs| {
                shuffled_kv_pairs
                    .into_iter()
                    .for_each(|(k, v)| assert_eq!(Some(v), kv_store.get(black_box(k)).unwrap()))
            },
            BatchSize::LargeInput,
        );
    }

    fn prebuilt_kv_pairs<R>(
        rng: &mut R,
        size: usize,
        max_key_size: usize,
        max_val_size: usize,
    ) -> (Vec<(String, String)>, usize)
    where
        R: Rng,
    {
        let mut bytes = 0;
        let kv_pairs = (0..size)
            .into_iter()
            .map(|_| {
                let (k, v) = rand_key_value(rng, max_key_size, max_val_size);
                bytes += k.as_bytes().len() + v.as_bytes().len();
                (k, v)
            })
            .collect();
        (kv_pairs, bytes)
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
