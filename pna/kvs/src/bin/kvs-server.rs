use kvs::KvsEngineVariant;
use std::net::SocketAddr;
use structopt::StructOpt;

fn main() {
    if let Err(err) = run() {
        println!("{}", err);
        std::process::exit(1);
    }
}

fn run() -> kvs::Result<()> {
    let _opt = ServerCliOpt::from_args();
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
