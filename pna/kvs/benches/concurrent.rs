mod common;

use common::*;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use kvs::networking::{JsonKvsClient, JsonKvsServer, KvsClient, KvsServer};
use kvs::thread_pool::ThreadPool;
use kvs::KvStore;
use rand::prelude::StdRng;
use rand::prelude::*;
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
    const KEY_SIZE: usize = 1000;
    const VAL_SIZE: usize = 1000;
    const ADDR: ([u8; 4], u16) = ([0, 0, 0, 0], 4000);

    let mut rng = StdRng::from_seed([0u8; 32]);
    let kv_pairs = prebuilt_kv_pairs(&mut rng, 100, KEY_SIZE, VAL_SIZE);

    let mut g = c.benchmark_group("write_concurrent_shared_queue_kv_store");
    g.throughput(Throughput::Elements(ITER as u64));

    // TODO: get number of CPU cores
    [1, 2, 4, 6, 8].iter().for_each(|nthreads| {
        g.bench_with_input(
            BenchmarkId::from_parameter(nthreads),
            nthreads,
            |b, &nthreads| {
                let tmpdir = TempDir::new().unwrap();
                let engine = KvStore::open(tmpdir.path()).unwrap();
                let pool = SharedQueueThreadPool::new(nthreads).unwrap();
                let mut server = JsonKvsServer::new(engine, pool, None);

                let server_thread_handle = std::thread::spawn(move || server.serve(ADDR).unwrap());
                rayon::scope(|_s| {
                    b.iter(|| {
                        kv_pairs.clone().into_par_iter().for_each(|(k, v)| {
                            let mut client = JsonKvsClient::connect(ADDR).unwrap();
                            client.set(k, v).unwrap();
                        });
                    })
                });
                server_thread_handle.thread().
            },
        );
    });
    g.finish();
}
