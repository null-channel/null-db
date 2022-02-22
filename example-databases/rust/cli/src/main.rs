#![feature(core)]
#![feature(io)]
#![feature(std_misc)]

use std::collections::HashMap;
use clap::{AppSettings, Parser, Subcommand};
use std::rand::{self, Rng};
use uuid::Uuid;
use std::old_io::Timer;
use std::old_io::timer;
use std::time::duration::Duration;
use std::iter;
use std::sync::mpsc;

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
    },

    Delete {
        key: String,
        #[clap(long, default_value = "localhost")]
        host: String,
    },

    Bench {
        #[clap(long, default_value_t = 100)]
        records: i32,
        #[clap(long, default_value_t = 10)]
        duration: i32,
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
            let data = value.clone();
            let resp = client.post(format!("http://{}:8080/{}\n",host, key))
                .body(data)
                .send()
                .await?
                .text().await?;

            println!("{}",resp)
        }

        Commands::Get { key , host} => {
            println!("getting data for key {}", key);
            let resp = reqwest::get(format!("http://{}:8080/{}\n",host, key))
            .await?
            .text()
            .await?;
            println!("key {}:{}",key,resp)
        }

        Commands::Delete { key, host} => {
            println!("deleting data for key {}", key);
            let client = reqwest::Client::new();
            let resp = client.delete(format!("http://{}:8080/{}\n",host, key)).send()
            .await?
            .text()
            .await?;
        }

        Commands::Bench {records,duration,host} => {
            println!("benchmarking database")
            benchmark(records,duration);
        }
    }

    Ok(())
}

async fn benchmark(records: i32, duration: i32) => Option<()> {
    let interval = Duration::milliseconds(1000);
    let metronome: mpsc::Receiver<()> = timer.periodic(interval);

    println!("Countdown");
    for i in iter::range_step(duration, 0, -1) {
        // This loop will run once every second
        let _ = metronome.recv();
        while i < records {
            let client = reqwest::Client::new();
                let data = value.clone();
                client.post(format!("http://localhost:8080/{}\n", Uuid::new_v4()))
                    .body(getRandomString(rand::random();))
                    .send();
        }
        println!("{}", i);
    }
    return Some(())
}

fn getRandomString(length: i32) -> String {
    rand::thread_rng()
        .gen_ascii_chars()
        .take(length)
        .collect::<String>()
}

