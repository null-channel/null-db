use std::collections::HashMap;
use clap::{AppSettings, Parser, Subcommand};

#[derive(Parser)]
#[clap(name = "nulldb")]
#[clap(about = "A fictional versioning CLI", long_about = None)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {

    Put {
        key: String,
        value: String,
        #[clap(long, default_value = "localhost")]
        host: String,
    },

    Get { 
        key: String,
        #[clap(long, default_value = "localhost")]
        host: String,
    }

}

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long, default_value = "localhost")]
    host: String,
    #[clap(short, long, default_value = "data")]
    data: String,
    #[clap(short, long, default_value = "key")]
    key: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    let args = Cli::parse();

    match &args.command {
        Commands::Put { key, value, host } => {
            println!("putting data {}", value);
            let client = reqwest::Client::new();
            let res = client.post(format!("http://{}/{}",host, key))
                .body("the exact body that is sent")
                .send()
                .await?;
        }

        Commands::Get { key , host} => {
            println!("getting data for key {}", key);
            let resp = reqwest::get(format!("http://{}\n",key))
            .await?
            .json::<HashMap<String, String>>()
            .await?;
        }
    }

    Ok(())
}

