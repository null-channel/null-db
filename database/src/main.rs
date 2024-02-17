use std::path::PathBuf;
use env_logger;
use errors::NullDbReadError;

use crate::nulldb::create_db;
use actix_web::{
    delete, get, post,
    web::{self, Data},
    App, HttpResponse, HttpServer, Responder, Result,
};
use tokio::sync::mpsc::Sender;
extern crate lazy_static;

use raft::grpcserver::RaftEvent;
mod file_reader;
use clap::Parser;
use file_reader::EasyReader;
use nulldb::{Config, NullDB};
mod errors;
mod file;
mod file_compactor;
mod index;
mod nulldb;
mod raft;
mod record;
mod utils;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long)]
    compaction: bool,
    #[clap(short, long)]
    #[arg(default_value=get_work_dir().into_os_string())]
    dir: PathBuf,
    #[clap(short, long)]
    roster: String,
    #[clap(short, long)]
    id: String,
    #[clap(short, long)]
    #[arg(default_value = "html")]
    encoding: String,
}

#[actix_web::main]
async fn main() -> Result<(), std::io::Error> {
    env_logger::init();
    let args = Args::parse();

    let nodes = args.roster.split(",").collect::<Vec<&str>>();

    let (sender, receiver) = tokio::sync::mpsc::channel(1000);
    let config = Config::new(args.dir, args.compaction, args.encoding.clone());
    let raft_config = raft::config::RaftConfig::new(args.id.clone(), nodes.clone());
    let db_mutex = create_db(config).expect("could not start db");
    let mut raft = raft::RaftNode::new(raft_config, receiver, db_mutex.clone());
    let tx = sender.clone();
    tokio::spawn(async move {
        let _ = raft.run(tx).await;
    });

    let sender_ark = Data::new(sender.clone());
    println!("starting web server");
    HttpServer::new(move || {
        App::new()
            .app_data(sender_ark.clone())
            .service(get_value_for_key)
            .service(put_value_for_key)
            .service(delete_value_for_key)
            .service(compact_data)
            .service(get_index)
    })
    .bind(format!("0.0.0.0:8080"))?
    .run()
    .await
}

fn get_work_dir() -> PathBuf {
    std::env::current_dir().unwrap()
}

#[get("/v1/data/{key}")]
async fn get_value_for_key(
    //db: Data<NullDB>,
    sender: Data<Sender<RaftEvent>>,
    request: web::Path<String>,
) -> impl Responder {
    let (tx, receiver) = tokio::sync::oneshot::channel();
    let event = RaftEvent::GetEntry(request.into_inner(), tx);
    let _ret = sender.send(event).await;
    match receiver.await {
        Err(e) => {
            HttpResponse::InternalServerError().body(format!("Issue getting value for key: {}", e))
        }
        Ok(value) => {
            match value {
                Ok(res) => HttpResponse::Ok().body(res.get_value().unwrap_or("No Value".to_string())),
                Err(e) => HttpResponse::InternalServerError().body(format!("Issue getting value for key: {}", e)),
            }
        }
    }
}

#[get("/")]
async fn get_index() -> impl Responder {
    HttpResponse::Ok().body("Hello world!")
}

#[post("/v1/data/{key}")]
pub async fn put_value_for_key(
    sender: Data<Sender<RaftEvent>>,
    key: web::Path<String>,
    req_body: String,
) -> impl Responder {
    println!("putting data {}", req_body);
    let (tx, receiver) = tokio::sync::oneshot::channel();
    let event = RaftEvent::NewEntry {
        key: key.clone(),
        value: req_body.clone(),
        sender: tx,
    };
    let _ = sender.send(event).await;

    let ret = receiver.await;

    match ret {
        Err(e) => HttpResponse::InternalServerError().body(format!("Issue writing: {}", e)),
        Ok(res) => {
            match res {
                Ok(_) => HttpResponse::Ok().body("Record written".to_string()),
                Err(e) => {
                    match e {
                        NullDbReadError::NotLeader => HttpResponse::MisdirectedRequest().body("I'm not the leader"),
                        _ => HttpResponse::InternalServerError().body(format!("Issue writing")),
                    }
                }
            }
        }
    }
}

#[delete("v1/data/{key}")]
pub async fn delete_value_for_key(db: Data<NullDB>, key: web::Path<String>) -> impl Responder {
    let ret = db.delete_record(key.clone());
    match ret {
        Err(e) => HttpResponse::InternalServerError().body(format!("Issue deleting: {}", e)),
        Ok(_) => HttpResponse::Ok().body("It has been deleted!"),
    }
}

#[get("/v1/management/compact")]
pub async fn compact_data(db: Data<NullDB>) -> impl Responder {
    println!("compacting!");
    let res = file_compactor::compactor(db.clone());

    if let Ok(_) = res {
        return HttpResponse::Ok();
    }

    HttpResponse::InternalServerError()
}

#[cfg(test)]
mod tests {

    use rand::{thread_rng, Rng};
    use std::env;
    use std::fs;
    use std::path;
    use std::path::PathBuf;

    use crate::file::FileEngine;
    use crate::file::Record;
    use crate::nulldb::create_db;
    use crate::nulldb::Config;
    use crate::nulldb::NullDB;

    use actix_web::web::Data;
    use rand::distributions::Alphanumeric;
    use tempfile::TempDir;
    #[test]
    fn get_value_for_key() {
        if let Ok(cargo_path) = env::var("CARGO_MANIFEST_DIR") {
            let tmp_dir = TempDir::new().expect("could not get temp dir");
            let _workdir = setup_base_data(tmp_dir.path(), cargo_path);

            let config = Config::new(tmp_dir.into_path(), false, "html".to_string());
            let db = create_db(config).expect("could not start database");

            let result = db
                .get_value_for_key("name".to_string())
                .expect("should retrive value");

            check_record(&result, "name", "name:marek");
        }
    }

    #[test]
    fn test_put_get_key_multi() {
        if let Ok(path) = env::var("CARGO_MANIFEST_DIR") {
            // Create a directory inside of `std::env::temp_dir()`
            let tmp_dir = TempDir::new().expect("could not get temp dir");
            let _workdir = setup_base_data(tmp_dir.path(), path);

            let config = Config::new(tmp_dir.into_path(), false, "html".to_string());
            let db = create_db(config).expect("could not start database");

            put_lots_of_data(&db, 10000, db.get_file_engine());
            let result = db
                .get_value_for_key("name".to_string())
                .expect("should retrive value");

            check_record(&result, "name", "name:marek");
        }
    }

    fn check_record(record: &Record, key: &str, value: &str) {
        assert_eq!(record.get_key(), key);
        assert_eq!(record.get_value().unwrap(), value);
    }

    #[test]
    fn test_average_insert_time_one_million() {
        if let Ok(path) = env::var("CARGO_MANIFEST_DIR") {
            // Create a directory inside of `std::env::temp_dir()`
            let tmp_dir = TempDir::new().expect("could not get temp dir");
            let _workdir = setup_base_data(tmp_dir.path(), path);

            let config = Config::new(tmp_dir.into_path(), false, "html".to_string());
            let db = create_db(config).expect("could not start database");

            for i in 0..10 {
                let start = std::time::Instant::now();

                put_lots_of_data(&db, 10000, db.get_file_engine());

                let end = (std::time::Instant::now() - start).as_millis();
                println!("iteration {}(ms): {}", i, end);
            }

            let start = std::time::Instant::now();
            let _result = db
                .get_value_for_key("name".to_string())
                .expect("should retrive value");
            let end = (std::time::Instant::now() - start).as_nanos();
            println!("get value for name duration nanos: {}", end);
        }
    }

    fn put_lots_of_data(ndb: &Data<NullDB>, counter: i32, file_engine: FileEngine) {
        let mut rng = thread_rng();
        for i in 0..counter {
            let index = i.try_into().unwrap();
            let r = file_engine.new_record(
                get_random_string(rng.gen_range(1..10)),
                index,
                None,
                Some(get_random_string(10)),
            );
            ndb.write_value_to_log(
                r
            )
            .expect("failed to write to log");
        }
    }

    fn setup_base_data(dir: &path::Path, cargo_path: String) {
        println!(
            "{}",
            format!(
                "{}/{}",
                cargo_path, "recources/test-segments/1-1.nullsegment"
            )
        );
        println!("{}", get_dir(dir, "1-1.nullsegment").to_str().unwrap());

        fs::copy(
            format!(
                "{}/{}",
                cargo_path, "recources/test-segments/1-1.nullsegment"
            ),
            get_dir(dir, "1-1.nullsegment"),
        )
        .unwrap();
        fs::copy(
            format!(
                "{}/{}",
                cargo_path, "recources/test-segments/1-2.nullsegment"
            ),
            get_dir(dir, "1-2.nullsegment"),
        )
        .unwrap();
        fs::copy(
            format!(
                "{}/{}",
                cargo_path, "recources/test-segments/2-1.nullsegment"
            ),
            get_dir(dir, "2-1.nullsegment"),
        )
        .unwrap();
    }

    fn get_dir(d: &path::Path, name: &str) -> PathBuf {
        let mut dir = PathBuf::new();
        dir.push(d);
        dir.push(name);
        dir
    }

    fn get_random_string(length: usize) -> String {
        let chars: Vec<u8> = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(length)
            .collect();
        let s = std::str::from_utf8(&chars).unwrap().to_string();
        return s;
    }
}
