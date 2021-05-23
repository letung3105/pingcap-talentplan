#[macro_use]
extern crate slog;

use kvs::{Error, ErrorKind, KvsEngineBackend, KvsServer, Result, KVS_ENGINE_BACKEND_FILENAME};
use slog::Drain;
use std::env;
use std::fs;
use std::net::SocketAddr;
use std::str::FromStr;
use structopt::StructOpt;

fn main() {
    if let Err(err) = run() {
        eprintln!("{}", err);
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let opt = ServerCliOpt::from_args();
    let current_dir = env::current_dir()?;

    // check the directory for the previously used key-value store engine, an error is returned if
    // previously used engine is different than the provided one
    let engine_backend_path = current_dir.join(KVS_ENGINE_BACKEND_FILENAME);
    let engine_backend = match fs::read_to_string(engine_backend_path) {
        Ok(prev_engine_backend) => {
            let prev_engine_backend = KvsEngineBackend::from_str(&prev_engine_backend)?;
            let engine_backend = opt.engine_backend.unwrap_or(prev_engine_backend);
            if engine_backend != prev_engine_backend {
                return Err(Error::from(ErrorKind::MismatchedKvsEngineBackend));
            }
            engine_backend
        }
        Err(_) => opt.engine_backend.unwrap_or(KvsEngineBackend::Kvs),
    };

    let decorator = slog_term::TermDecorator::new().stderr().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let logger = slog::Logger::root(
        drain,
        o!("version" => env!("CARGO_PKG_VERSION"),
           "engine" => engine_backend.as_str()),
    );

    let kvs_engine = kvs::open(current_dir, engine_backend)?;
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
