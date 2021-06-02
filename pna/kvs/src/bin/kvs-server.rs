#[macro_use]
extern crate slog;

use kvs::engines::{choose_engine_backend, KvsEngineBackend};
use kvs::thread_pool::{NaiveThreadPool, ThreadPool};
use kvs::{KvStore, KvsServer, Result, SledKvsEngine};
use slog::Drain;
use std::env;
use std::net::SocketAddr;
use structopt::StructOpt;

fn main() {
    let decorator = slog_term::TermDecorator::new().stderr().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let logger = slog::Logger::root(drain, o!("version" => env!("CARGO_PKG_VERSION")));

    if let Err(err) = run(logger) {
        eprintln!("{}", err);
        std::process::exit(1);
    }
}

fn run(logger: slog::Logger) -> Result<()> {
    let opt = ServerCliOpt::from_args();
    let current_dir = env::current_dir()?;

    let engine_backend = choose_engine_backend(&current_dir, opt.engine_backend)?;
    let thread_pool = NaiveThreadPool::new(4)?;
    match engine_backend {
        KvsEngineBackend::Kvs => {
            let kvs_engine = KvStore::open(&current_dir)?;
            let logger = logger.new(o!( "engine" => engine_backend.as_str()));
            let mut kvs_server = KvsServer::new(kvs_engine, thread_pool, Some(logger));
            kvs_server.serve(opt.server_addr)?;
        }
        KvsEngineBackend::Sled => {
            let kvs_engine = SledKvsEngine::open(&current_dir)?;
            let logger = logger.new(o!( "engine" => engine_backend.as_str()));
            let mut kvs_server = KvsServer::new(kvs_engine, thread_pool, Some(logger));
            kvs_server.serve(opt.server_addr)?;
        }
    };

    Ok(())
}

#[derive(StructOpt)]
struct ServerCliOpt {
    #[structopt(
        long = "addr",
        about = "IP address of the key-value store",
        default_value = "127.0.0.1:4000"
    )]
    server_addr: SocketAddr,

    #[structopt(
        long = "engine",
        about = "Name of the engine that is used for the key-value store"
    )]
    engine_backend: Option<KvsEngineBackend>,
}
