use kvs::{Error, ErrorKind, KvStore, Result};
use structopt::StructOpt;

fn main() {
    if let Err(err) = run() {
        println!("{}", err);
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let dir = std::env::current_dir()?;
    let mut kvs = KvStore::open(dir)?;

    match CliOpt::from_args().sub_cmd {
        CliSubCommand::Set { key, val } => {
            kvs.set(key, val)?;
        }
        CliSubCommand::Get { key } => match kvs.get(key)? {
            Some(val) => println!("{}", val),
            None => println!("{}", Error::new(ErrorKind::KeyNotFound)),
        },
        CliSubCommand::Rm { key } => {
            kvs.remove(key)?;
        }
    }

    Ok(())
}

#[derive(Debug, StructOpt)]
struct CliOpt {
    #[structopt(subcommand)]
    sub_cmd: CliSubCommand,
}

#[derive(Debug, StructOpt)]
enum CliSubCommand {
    Set {
        #[structopt(name = "KEY")]
        key: String,
        #[structopt(name = "VALUE")]
        val: String,
    },
    Get {
        #[structopt(name = "KEY")]
        key: String,
    },
    Rm {
        #[structopt(name = "KEY")]
        key: String,
    },
}
