mod common;

use common::*;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use kvs::{KvStore, KvsEngine};
use rand::prelude::StdRng;
use rand::prelude::*;
use rayon::ThreadPoolBuilder;
use tempfile::TempDir;

criterion_main!(concurrent);
criterion_group! {
    name = concurrent;
    config = Criterion::default().significance_level(0.05).sample_size(500);
    targets = write_concurrent_rayon_kv_store
}

pub fn write_concurrent_kv_store(c: &mut Criterion) {
    const ITER: usize = 100;
    const KEY_SIZE: usize = 100;
    const VAL_SIZE: usize = 100;

    let mut rng = StdRng::from_seed([0u8; 32]);
    let kv_pairs = prebuilt_kv_pairs(&mut rng, ITER, KEY_SIZE, VAL_SIZE);

    let mut g = c.benchmark_group("write_concurrent_shared_queue_kv_store");
    g.throughput(Throughput::Elements(ITER as u64));

    // TODO: get number of CPU cores
    [1, 2, 4, 6, 8].iter().for_each(|nthreads| {
        g.bench_with_input(
            BenchmarkId::from_parameter(nthreads),
            nthreads,
            |b, &nthreads| {
                let tmpdir = TempDir::new().unwrap();
                let pool = ThreadPoolBuilder::new()
                    .num_threads(nthreads)
                    .build()
                    .unwrap();

                b.iter(|| {
                    pool.scope(|s| {
                        let engine = KvStore::open(tmpdir.path()).unwrap();
                        kv_pairs.iter().for_each(|(k, v)| {
                            let engine = engine.clone();
                            s.spawn(move |_| engine.set(k.clone(), v.clone()).unwrap());
                        });
                    })
                });
            },
        );
    });
    g.finish();
}
