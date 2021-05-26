#[macro_use]
extern crate slog;

use kvs::engines::{choose_engine_backend, KvsEngineBackend};
use kvs::{KvStore, KvsEngine, KvsServer, Result, SledKvsEngine};
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
    let kvs_engine: Box<dyn KvsEngine> = match engine_backend {
        KvsEngineBackend::Kvs => Box::new(KvStore::open(&current_dir)?),
        KvsEngineBackend::Sled => Box::new(SledKvsEngine::open(&current_dir)?),
    };

    let logger = logger.new(o!( "engine" => engine_backend.as_str()));
    let mut kvs_server = KvsServer::new(kvs_engine, Some(logger));
    kvs_server.serve(opt.server_addr)?;
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
