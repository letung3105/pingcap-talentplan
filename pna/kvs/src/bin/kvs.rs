use kvs::KvStore;
use structopt::StructOpt;

fn main() {
    if let Err(err) = run() {
        println!("{}", err);
        std::process::exit(1);
    }
}

fn run() -> kvs::Result<()> {
    let kvs_dir = std::env::current_dir()?;
    let mut kvs = KvStore::open(kvs_dir)?;

    match KvStoreOpt::from_args().sub_command {
        KvStoreSubCommand::Set { key, value } => {
            kvs.set(key, value)?;
        }
        KvStoreSubCommand::Get { key } => match kvs.get(key)? {
            Some(value) => println!("{}", value),
            None => println!("{}", kvs::KvStoreError::KeyNotFound),
        },
        KvStoreSubCommand::Rm { key } => {
            kvs.remove(key)?;
        }
    }

    Ok(())
}

#[derive(Debug, StructOpt)]
struct KvStoreOpt {
    #[structopt(subcommand)]
    sub_command: KvStoreSubCommand,
}

#[derive(Debug, StructOpt)]
enum KvStoreSubCommand {
    Set {
        #[structopt(name = "KEY")]
        key: String,
        #[structopt(name = "VALUE")]
        value: String,
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
