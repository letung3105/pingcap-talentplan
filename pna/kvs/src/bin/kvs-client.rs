use kvs::{Error, ErrorKind, KvsClient};
use std::net::SocketAddr;
use structopt::StructOpt;

// TODO: check if the flow is correct according to the requirements

fn main() {
    if let Err(err) = run() {
        println!("{}", err);
        std::process::exit(1);
    }
}

fn run() -> kvs::Result<()> {
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
                None => println!("{}", Error::new(ErrorKind::KeyNotFound)),
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
