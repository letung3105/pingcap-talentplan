mod common;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use kvs::networking::{JsonKvsClient, JsonKvsServer, KvsClient, KvsServer};
use kvs::thread_pool::ThreadPool;
use kvs::KvStore;
use rayon::prelude::*;
use tempfile::TempDir;

use kvs::thread_pool::SharedQueueThreadPool;

criterion_main!(sequential);
criterion_group! {
    name = sequential;
    config = Criterion::default().significance_level(0.05).sample_size(1000);
    targets = write_concurrent_shared_queue_kv_store
}

pub fn write_concurrent_shared_queue_kv_store(c: &mut Criterion) {
    const ITER: usize = 1000;
    const ADDR: ([u8; 4], u16) = ([0, 0, 0, 0], 4000);

    // TODO: get number of CPU cores
    let nthreads_sample = [1u32, 2, 4, 6, 8];
    nthreads_sample.iter().for_each(|nthreads| {
        c.bench_with_input(
            BenchmarkId::new("write_concurrent_shared_queue_kv_store", nthreads),
            nthreads,
            |b, &nthreads| {
                let tmpdir = TempDir::new().unwrap();
                let engine = KvStore::open(tmpdir.path()).unwrap();
                let pool = SharedQueueThreadPool::new(nthreads).unwrap();
                let mut server = JsonKvsServer::new(engine, pool, None);

                let pool_bench_client = rayon::ThreadPoolBuilder::new()
                    .num_threads(nthreads as usize)
                    .build()
                    .unwrap();
                pool_bench_client.spawn(move || server.serve(ADDR).unwrap());

                // TODO: not running, benchmark halts
                pool_bench_client.install(|| {
                    b.iter(|| {
                        (0..ITER).into_par_iter().for_each(|_| {
                            let mut client = JsonKvsClient::connect(ADDR).unwrap();
                            client.set("todo".to_string(), "todo".to_string()).unwrap();
                        });
                    })
                });
            },
        );
    });
}
