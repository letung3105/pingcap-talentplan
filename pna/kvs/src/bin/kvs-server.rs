use kvs::{KvsEngineVariant, KvsServer};
use std::env;
use std::net::SocketAddr;
use structopt::StructOpt;

fn main() {
    if let Err(err) = run() {
        println!("{}", err);
        std::process::exit(1);
    }
}

fn run() -> kvs::Result<()> {
    let opt = ServerCliOpt::from_args();
    let data_path = env::current_dir()?;
    let mut kvs_server = KvsServer::new(opt.engine_variant, data_path)?;
    kvs_server.serve(opt.server_addr)
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
