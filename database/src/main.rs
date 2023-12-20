use std::path::PathBuf;

use actix_web::{
    delete, get, post,
    web::{self, Data},
    App, HttpResponse, HttpServer, Responder, Result,
};
extern crate lazy_static;

use std::convert::TryInto;
use std::time;
mod file_reader;
//use easy_reader::EasyReader;
use clap::Parser;
use file_reader::EasyReader;
use nulldb::{create_db, Config, NullDB};
mod errors;
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
}

//#[actix_web::main]
#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    let args = Args::parse();

    let nodes = args.roster.split(",").collect::<Vec<&str>>();

    let raft_config = raft::config::RaftConfig::new(args.id.clone(), nodes.clone());
    let (sender, mut receiver) = tokio::sync::mpsc::channel(100);
    let mut raft = raft::RaftNode::new(args.id, raft_config, receiver);
    raft.run(sender).await.expect("could not start raft server");

    Ok(())

    // TODO: start server
    /*
    let config = Config::new(args.dir , args.compaction);
    let db_mutex = create_db(config).expect("could not start db");

    HttpServer::new(move || {
        App::new()
            .app_data(db_mutex.clone())
            .service(get_value_for_key)
            .service(put_value_for_key)
            .service(delete_value_for_key)
            .service(compact_data)
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
    */
}

fn get_work_dir() -> PathBuf {
    std::env::current_dir().unwrap()
}

#[get("/v1/data/{key}")]
async fn get_value_for_key(db: Data<NullDB>, request: web::Path<String>) -> impl Responder {
    let key = request.into_inner();

    let then = time::Instant::now();

    let ret = db.get_value_for_key(key.clone());

    let dur: u128 = ((time::Instant::now() - then).as_millis())
        .try_into()
        .unwrap();
    println!("duration: {}", dur);
    match ret {
        Err(e) => {
            HttpResponse::InternalServerError().body(format!("Issue getting value for key: {}", e))
        }
        Ok(value) => HttpResponse::Ok().body(value),
    }
}

#[post("/v1/data/{key}")]
pub async fn put_value_for_key(
    db: Data<NullDB>,
    key: web::Path<String>,
    req_body: String,
) -> impl Responder {
    println!("putting data {}", req_body);
    let ret = db.write_value_to_log(key.into_inner(), req_body);

    match ret {
        Err(_) => HttpResponse::InternalServerError(),
        Ok(_) => HttpResponse::Ok(),
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

            let config = Config::new(tmp_dir.into_path(), false);
            let db = create_db(config).expect("could not start database");

            let result = db
                .get_value_for_key("name".to_string())
                .expect("should retrive value");

            assert_eq!(result, "name:marek");
        }
    }

    #[test]
    fn test_put_get_key_multi() {
        if let Ok(path) = env::var("CARGO_MANIFEST_DIR") {
            // Create a directory inside of `std::env::temp_dir()`
            let tmp_dir = TempDir::new().expect("could not get temp dir");
            let _workdir = setup_base_data(tmp_dir.path(), path);

            let config = Config::new(tmp_dir.into_path(), false);
            let db = create_db(config).expect("could not start database");

            put_lots_of_data(&db, 10000);
            let result = db
                .get_value_for_key("name".to_string())
                .expect("should retrive value");

            assert_eq!(result, "name:marek");
        }
    }

    #[test]
    fn test_average_insert_time_one_million() {
        if let Ok(path) = env::var("CARGO_MANIFEST_DIR") {
            // Create a directory inside of `std::env::temp_dir()`
            let tmp_dir = TempDir::new().expect("could not get temp dir");
            let _workdir = setup_base_data(tmp_dir.path(), path);

            let config = Config::new(tmp_dir.into_path(), false);
            let db = create_db(config).expect("could not start database");

            for i in 0..10 {
                let start = std::time::Instant::now();

                put_lots_of_data(&db, 10000);

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

    fn put_lots_of_data(ndb: &Data<NullDB>, counter: i32) {
        let mut rng = thread_rng();
        for _ in 0..counter {
            ndb.write_value_to_log(
                get_random_string(rng.gen_range(1..10)),
                get_random_string(10),
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
