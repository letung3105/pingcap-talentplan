use bb03::{BluisClient, TEST_ADDR};
use structopt::StructOpt;

fn main() -> bb03::Result<()> {
    let opt = BluisOpt::from_args();
    let client = BluisClient::new(TEST_ADDR)?;
    match opt.subcommand {
        BluisSubCommand::Ping { message } => {
            println!("Command is 'PING {}'", message);
            client.ping(message)?;
        }
    }
    Ok(())
}

#[derive(Debug, StructOpt)]
#[structopt()]
struct BluisOpt {
    #[structopt(subcommand)]
    subcommand: BluisSubCommand,
}

#[derive(Debug, StructOpt)]
enum BluisSubCommand {
    Ping {
        #[structopt(name = "MESSAGE")]
        message: String,
    },
}
