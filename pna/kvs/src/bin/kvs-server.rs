#[macro_use]
extern crate slog;

use kvs::engines::Engine;
use kvs::networking::JsonKvsServer;
use kvs::thread_pool::{NaiveThreadPool, ThreadPool};
use kvs::{KvStore, KvsEngine, KvsServer, Result, SledKvsEngine};
use slog::Drain;
use std::env;
use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use structopt::StructOpt;

const KVS_ENGINE_FILENAME: &str = "KVS_ENGINE";

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
    let cli_options = ServerCliOpt::from_args();
    let current_dir = env::current_dir()?;

    let current_engine = current_directory_engine(&current_dir)?;
    let engine = match cli_options.engine {
        None => current_engine.unwrap_or(Engine::Kvs),
        Some(selected_engine) => match current_engine {
            None => selected_engine,
            Some(current_engine) => {
                if selected_engine == current_engine {
                    selected_engine
                } else {
                    eprintln!(
                        "Path's engine is different from the chosen engine, {} vs. {}",
                        current_engine.as_str(),
                        selected_engine.as_str()
                    );
                    std::process::exit(1);
                }
            }
        },
    };

    let engine_path = current_dir.join(KVS_ENGINE_FILENAME);
    fs::write(engine_path, engine.as_str())?;

    let pool = NaiveThreadPool::new(4)?;
    let logger = logger.new(o!( "engine" => engine.as_str()));
    match engine {
        Engine::Kvs => run_with(cli_options.addr, KvStore::open(&current_dir)?, pool, logger),
        Engine::Sled => {
            let db = sled::Config::default().path(current_dir).open()?;
            run_with(cli_options.addr, SledKvsEngine::new(db), pool, logger)
        }
    }
}

fn run_with<E, P>(addr: SocketAddr, engine: E, pool: P, logger: slog::Logger) -> Result<()>
where
    E: KvsEngine,
    P: ThreadPool,
{
    let mut kvs_server = JsonKvsServer::new(engine, pool, Some(logger));
    kvs_server.serve(addr)
}

fn current_directory_engine<P>(path: P) -> Result<Option<Engine>>
where
    P: Into<PathBuf>,
{
    let engine_path = path.into().join(KVS_ENGINE_FILENAME);
    if !engine_path.exists() {
        return Ok(None);
    }

    match fs::read_to_string(engine_path)?.parse() {
        Ok(engine) => Ok(Some(engine)),
        Err(_) => Ok(None),
    }
}

#[derive(StructOpt)]
struct ServerCliOpt {
    #[structopt(
        long = "addr",
        about = "IP address of the key-value store",
        default_value = "127.0.0.1:4000"
    )]
    addr: SocketAddr,

    #[structopt(
        long = "engine",
        about = "Name of the engine that is used for the key-value store"
    )]
    engine: Option<Engine>,
}
