use std::fmt::{self, Display, Formatter};
use std::net::SocketAddr;
use std::str::FromStr;
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
        long,
        about = "Name of the engine that is used for the key-value store",
        default_value = "kvs"
    )]
    engine: ServerEngine,
}

enum ServerEngine {
    Kvs,
    Sled,
}

impl FromStr for ServerEngine {
    type Err = ParseServerEngineError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let name = s.to_lowercase();
        match name.as_str() {
            "kvs" => Ok(Self::Kvs),
            "sled" => Ok(Self::Sled),
            _ => Err(ParseServerEngineError),
        }
    }
}

#[derive(Debug)]
struct ParseServerEngineError;

impl std::error::Error for ParseServerEngineError {}

impl Display for ParseServerEngineError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "Could not parse server engine")
    }
}
