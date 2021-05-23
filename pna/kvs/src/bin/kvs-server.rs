use kvs::{KvsEngineVariant, KvsServer, Result};
use std::env;
use std::net::SocketAddr;
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

    let kvs_engine = kvs::open(current_dir, opt.engine_variant)?;
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
        about = "Name of the engine that is used for the key-value store"
    )]
    engine_variant: Option<KvsEngineVariant>,
}
