use kvs::KvStore;
use structopt::StructOpt;

fn main() -> kvs::Result<()> {
    let mut kvs = KvStore::open("dbinstance/logfile")?;
    match KvStoreOpt::from_args().sub_command {
        KvStoreSubCommand::Set { key, value } => kvs.set(key, value)?,
        KvStoreSubCommand::Get { key } => match kvs.get(key.clone())? {
            None => return Err(kvs::KvStoreError::KeyNotFound(key)),
            Some(value) => println!("{} = {}", key, value),
        },
        KvStoreSubCommand::Rm { key } => kvs.remove(key)?,
    }
    Ok(())
}

#[derive(Debug, StructOpt)]
pub struct KvStoreOpt {
    #[structopt(subcommand)]
    pub sub_command: KvStoreSubCommand,
}

#[derive(Debug, StructOpt)]
pub enum KvStoreSubCommand {
    /// Set the value at the given key, if a value already exists at the given key,
    /// it is updated
    Set {
        /// Value of the key
        #[structopt(name = "KEY")]
        key: String,
        /// The value to be set
        #[structopt(name = "VALUE")]
        value: String,
    },

    /// Get the value at the given key
    Get {
        /// Value of the key
        #[structopt(name = "KEY")]
        key: String,
    },

    /// Remove the value at the given key
    Rm {
        /// Value of the key
        #[structopt(name = "KEY")]
        key: String,
    },
}
