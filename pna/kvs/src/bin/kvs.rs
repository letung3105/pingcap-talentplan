use structopt::StructOpt;

fn main() {
    match KvStoreOpt::from_args().sub_command {
        KvStoreSubCommand::Set { key: _, value: _ } => {
            eprintln!("unimplemented");
            std::process::exit(1);
        }
        KvStoreSubCommand::Get { key: _ } => {
            eprintln!("unimplemented");
            std::process::exit(1);
        }
        KvStoreSubCommand::Rm { key: _ } => {
            eprintln!("unimplemented");
            std::process::exit(1);
        }
    }
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
