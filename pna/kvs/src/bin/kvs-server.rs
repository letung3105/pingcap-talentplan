use kvs::engines::KVS_ENGINE_VARIANT_FILE;
use kvs::{Error, ErrorKind, KvsEngine, KvsEngineVariant, KvsServer, Result};
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
    let variant = if fs::read_dir(&current_dir)?.next().is_some() {
        let kvs_variant_path = current_dir.join(KVS_ENGINE_VARIANT_FILE);
        let prev_variant_str = fs::read_to_string(kvs_variant_path)?;
        let prev_variant = KvsEngineVariant::from_str(&prev_variant_str)?;

        let variant = opt.engine_variant.unwrap_or(prev_variant);
        if variant != prev_variant {
            return Err(Error::from(ErrorKind::MismatchedKvsEngine));
        }
        variant
    } else {
        opt.engine_variant.unwrap_or(KvsEngineVariant::Kvs)
    };

    let kvs_engine = KvsEngine::open(current_dir, variant)?;
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
