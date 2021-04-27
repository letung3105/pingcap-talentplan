use structopt::StructOpt;
use bb03::{Result, BluisClient};

fn main() -> bb03::Result<()> {
    let opt = BluiscOpt::from_args();
    let client = BluisClient::new();

    match opt.sub_command {
        BluiscSubCommand::Ping { message } => {
            client.ping(message)?;
        }
    }

    Ok(())
}

#[derive(StructOpt)]
#[structopt()]
struct BluiscOpt {
    #[structopt(subcommand)]
    sub_command: BluiscSubCommand,
}

#[derive(StructOpt)]
enum BluiscSubCommand {
    Ping {
        #[structopt(name = "MESSAGE")]
        message: String,
    },
}
