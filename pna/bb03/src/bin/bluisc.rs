use bb03::{BluisClient, Result, TEST_ADDR};
use structopt::StructOpt;

fn main() -> Result<()> {
    let opt = BluiscOpt::from_args();
    let mut client = BluisClient::new(TEST_ADDR)?;

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
        message: Option<String>,
    },
}
