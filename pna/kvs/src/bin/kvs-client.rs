use kvs::KvsClient;
use std::net::SocketAddr;
use structopt::StructOpt;

fn main() {
    if let Err(err) = run() {
        println!("{}", err);
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let opt = ClientCliOpt::from_args();

    match opt.sub_cmd {
        ClientCliSubCommand::Set { key, val, addr } => {
            let kvs_client = KvsClient::new(addr);
            kvs_client.set_req(key, val)?;
        }
        ClientCliSubCommand::Get { key, addr } => {
            let kvs_client = KvsClient::new(addr);
            match kvs_client.get_req(key)? {
                Some(val) => println!("{}", val),
                None => println!("Key not found"),
            }
        }
        ClientCliSubCommand::Rm { key, addr } => {
            let kvs_client = KvsClient::new(addr);
            kvs_client.remove_req(key)?;
        }
    }

    Ok(())
}

#[derive(StructOpt)]
struct ClientCliOpt {
    #[structopt(subcommand)]
    sub_cmd: ClientCliSubCommand,
}

#[derive(StructOpt)]
enum ClientCliSubCommand {
    #[structopt(about = "Set a value to a key in the key-value store")]
    Set {
        #[structopt(name = "KEY")]
        key: String,
        #[structopt(name = "VALUE")]
        val: String,
        #[structopt(
            long = "addr",
            about = "IP address of the key-value store",
            default_value = "127.0.0.1:4000"
        )]
        addr: SocketAddr,
    },

    #[structopt(about = "Get a value from a key in the key-value store")]
    Get {
        #[structopt(name = "KEY")]
        key: String,
        #[structopt(
            long = "addr",
            about = "IP address of the key-value store",
            default_value = "127.0.0.1:4000"
        )]
        addr: SocketAddr,
    },

    #[structopt(about = "Remove a key from the key-value store")]
    Rm {
        #[structopt(name = "KEY")]
        key: String,
        #[structopt(
            long = "addr",
            about = "IP address of the key-value store",
            default_value = "127.0.0.1:4000"
        )]
        addr: SocketAddr,
    },
}

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
enum Error {
    KvsError(kvs::Error),
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::KvsError(err) => write!(f, "Key-value store error {}", err),
        }
    }
}

impl From<kvs::Error> for Error {
    fn from(err: kvs::Error) -> Self {
        Error::KvsError(err)
    }
}
