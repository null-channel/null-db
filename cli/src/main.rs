
use std::collections::HashMap;
use clap::{AppSettings, Parser, Subcommand};
use uuid::Uuid;
use std::time::Duration;
use std::iter;
use std::sync::mpsc;
use std::{thread, time};
use std::convert::TryInto;
use rand::Rng;
use rand::prelude::*;
use rand::{thread_rng};
use rand::distributions::{Alphanumeric, Uniform, Standard};

mod null_client;

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
            println!("benchmarking database");
            benchmark(*records,*duration,host.to_string());
        }
    }

    Ok(())
}

async fn benchmark(records: i32, duration: i32, host: String) -> Option<()> {

    let client = null_client::NullClient::new(format!("http://{}:8080/", host).to_string());

    println!("Countdown");
    let now = time::Instant::now();
    let mut rng = rand::thread_rng();
    for i in 1..duration {
        let then = time::Instant::now();

        // This loop will run once every second
        for r in 1..records {
            let client = reqwest::Client::new();
                client.post(format!("http://localhost:8080/{}\n", Uuid::new_v4()))
                    .body(get_random_string(rng.gen::<usize>()))
                    .send();
        
        }

        let dur: u64 = ((time::Instant::now()-then).as_millis()).try_into().unwrap();
        let sleep_time: u64 = 1000 - dur;

        if sleep_time > 0 {
            let sleep_dura = time::Duration::from_nanos(sleep_time);
            thread::sleep(sleep_dura);
        }
    }
    
    return Some(())
}

fn get_random_string(length: usize) -> String {
    let chars: Vec<u8> = rand::thread_rng().sample_iter(&Alphanumeric).take(length).collect();
    let s = std::str::from_utf8(&chars).unwrap().to_string();
    return s;
}

