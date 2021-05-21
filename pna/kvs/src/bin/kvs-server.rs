use kvs::{KvsEngine, KvsEngineVariant, KvsServer, SledKvsEngine};
use std::env;
use std::net::SocketAddr;
use structopt::StructOpt;

fn main() {
    if let Err(err) = run() {
        println!("{}", err);
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let opt = ServerCliOpt::from_args();
    let data_path = env::current_dir()?;

    let kvs_engine = KvsEngine::open(data_path, opt.engine_variant)?;
    let mut kvs_server = KvsServer::new(kvs_engine);
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
        about = "Name of the engine that is used for the key-value store",
        default_value = "kvs"
    )]
    engine_variant: KvsEngineVariant,
}

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
enum Error {
    IOError(std::io::Error),
    KvsError(kvs::Error),
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IOError(err) => write!(f, "IO error {}", err),
            Self::KvsError(err) => write!(f, "Key-value store error {}", err),
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::IOError(err)
    }
}

impl From<kvs::Error> for Error {
    fn from(err: kvs::Error) -> Self {
        Error::KvsError(err)
    }
}
