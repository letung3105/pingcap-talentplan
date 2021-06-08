mod common;

use common::*;
use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, Bencher, BenchmarkId, Criterion,
    Throughput,
};
use kvs::{engines::Engine, KvsEngine};
use rand::prelude::*;
use rayon::ThreadPoolBuilder;
use tempfile::TempDir;

const ITER: usize = 1000;
const KEY_SIZE: usize = 1000;
const VAL_SIZE: usize = 1000;

pub fn concurrent_write_bulk(c: &mut Criterion) {
    let mut g = c.benchmark_group("concurrent_write_bulk");
    g.throughput(Throughput::Bytes((ITER * (KEY_SIZE + VAL_SIZE)) as u64));

    let phys_cpus = num_cpus::get_physical();
    (2..=phys_cpus*2).into_iter().step_by(2).for_each(|nthreads| {
        g.bench_with_input(
            BenchmarkId::new("kvs", nthreads),
            &(Engine::Kvs, nthreads),
            concurrent_write_bulk_bench,
        );
        g.bench_with_input(
            BenchmarkId::new("sled", nthreads),
            &(Engine::Sled, nthreads),
            concurrent_write_bulk_bench,
        );
    });
    g.finish();
}

fn concurrent_write_bulk_bench(b: &mut Bencher, (engine, nthreads): &(Engine, usize)) {
    let mut rng = StdRng::from_seed([0u8; 32]);
    let kv_pairs = prebuilt_kv_pairs(&mut rng, ITER, KEY_SIZE, VAL_SIZE);
    let pool = ThreadPoolBuilder::new()
        .num_threads(*nthreads)
        .build()
        .unwrap();

    match *engine {
        Engine::Kvs => {
            pool.install(|| {
                b.iter_batched(
                    || {
                        let (engine, tmpdir) = prep_kv_store();
                        (engine, kv_pairs.clone(), tmpdir)
                    },
                    concurrent_write_bulk_bench_iter,
                    BatchSize::SmallInput,
                )
            });
        }
        Engine::Sled => {
            pool.install(|| {
                b.iter_batched(
                    || {
                        let (engine, tmpdir) = prep_sled();
                        (engine, kv_pairs.clone(), tmpdir)
                    },
                    concurrent_write_bulk_bench_iter,
                    BatchSize::SmallInput,
                )
            });
        }
    }
}

fn concurrent_write_bulk_bench_iter<E>((engine, kv_pairs, _tmpdir): (E, Vec<(String, String)>, TempDir))
where
    E: KvsEngine,
{
    rayon::scope(move |s| {
        kv_pairs.into_iter().for_each(|(k, v)| {
            let engine = engine.clone();
            s.spawn(move |_| engine.set(black_box(k), black_box(v)).unwrap());
        });
    });
}

pub fn concurrent_read_bulk(c: &mut Criterion) {
    let mut g = c.benchmark_group("concurrent_read_bulk");
    g.throughput(Throughput::Bytes((ITER * (KEY_SIZE)) as u64));

    let phys_cpus = num_cpus::get_physical();
    (2..=phys_cpus*2).into_iter().step_by(2).for_each(|nthreads| {
        g.bench_with_input(
            BenchmarkId::new("kvs", nthreads),
            &(Engine::Kvs, nthreads),
            concurrent_read_bulk_bench,
        );
        g.bench_with_input(
            BenchmarkId::new("sled", nthreads),
            &(Engine::Sled, nthreads),
            concurrent_read_bulk_bench,
        );
    });
    g.finish();
}

fn concurrent_read_bulk_bench(b: &mut Bencher, (engine, nthreads): &(Engine, usize)) {
    let mut rng = StdRng::from_seed([0u8; 32]);
    let kv_pairs = prebuilt_kv_pairs(&mut rng, ITER, KEY_SIZE, VAL_SIZE);
    let pool = ThreadPoolBuilder::new()
        .num_threads(*nthreads)
        .build()
        .unwrap();

    match *engine {
        Engine::Kvs => {
            let (engine, _tmpdir) = prep_kv_store();
            kv_pairs
                .iter()
                .cloned()
                .for_each(|(k, v)| engine.set(k, v).unwrap());

            pool.install(move || {
                b.iter_batched(
                    || {
                        let mut kv_pairs = kv_pairs.clone();
                        kv_pairs.shuffle(&mut rng);
                        (engine.clone(), kv_pairs)
                    },
                    concurrent_read_bulk_bench_iter,
                    BatchSize::SmallInput,
                )
            });
        }
        Engine::Sled => {
            let (engine, _tmpdir) = prep_sled();
            kv_pairs
                .iter()
                .cloned()
                .for_each(|(k, v)| engine.set(k, v).unwrap());

            pool.install(move || {
                b.iter_batched(
                    || {
                        let mut kv_pairs = kv_pairs.clone();
                        kv_pairs.shuffle(&mut rng);
                        (engine.clone(), kv_pairs)
                    },
                    concurrent_read_bulk_bench_iter,
                    BatchSize::SmallInput,
                )
            });
        }
    }
}

fn concurrent_read_bulk_bench_iter<E>((engine, kv_pairs): (E, Vec<(String, String)>))
where
    E: KvsEngine,
{
    rayon::scope(move |s| {
        kv_pairs.into_iter().for_each(|(k, v)| {
            let engine = engine.clone();
            s.spawn(move |_| assert_eq!(Some(v), engine.get(black_box(k)).unwrap()));
        });
    })
}

criterion_main!(benches);
criterion_group!(benches, concurrent_write_bulk, concurrent_read_bulk,);
