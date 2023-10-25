use actix_web::{
    delete, get, post,
    web::{self, Data},
    App, HttpResponse, HttpServer, Responder, Result,
};
extern crate lazy_static;
mod file_reader;
//use easy_reader::EasyReader;
use clap::Parser;
use file_reader::EasyReader;
use nulldb::{NullDB, create_db};
mod errors;
mod file_compactor;
mod nulldb;
mod record;
mod utils;
mod index;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long)]
    compaction: bool,
}

#[actix_web::main]
async fn main() -> Result<(), std::io::Error> {
    let args = Args::parse();

    let db_mutex = create_db(args.compaction).expect("could not start db");

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
}

#[get("/v1/data/{key}")]
async fn get_value_for_key(db: Data<NullDB>, request: web::Path<String>) -> impl Responder {
    let key = request.into_inner();
    let ret = db.get_value_for_key(key.clone());

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

    use std::env;
    use std::fs;
    use super::utils;

    use crate::nulldb::NullDB;
    use crate::nulldb::create_db;

    use rand::distributions::{Alphanumeric};
    use rand::{thread_rng, Rng};
    use actix_web::web::Data;
    use tempfile::TempDir;
    #[test]
    fn get_value_for_key() {
        if let Ok(path) = env::var("CARGO_MANIFEST_DIR") {
            let tmp_dir = TempDir::new().expect("could not get temp dir");
            let _workdir = setup_base_data(tmp_dir,path);

            let db = create_db(false).expect("could not start database");

            let result = db.get_value_for_key("name".to_string()).expect("should retrive value");

            assert_eq!(result,"name:marek");
        }
    }

    #[test]
    fn test_put_get_key_multi() {
        if let Ok(path) = env::var("CARGO_MANIFEST_DIR") {
            // Create a directory inside of `std::env::temp_dir()`
            let tmp_dir = TempDir::new().expect("could not get temp dir");
            let _workdir = setup_base_data(tmp_dir,path);

            let db = create_db(false).expect("could not start database");

            put_lots_of_data(db.clone());
            let result = db.get_value_for_key("name".to_string()).expect("should retrive value");

            assert_eq!(result,"name:marek");
        }
    }

    fn put_lots_of_data(ndb: Data<NullDB>) {
        for _ in 1..1000{
            ndb.write_value_to_log(get_random_string(3), get_random_string(10)).expect("failed to write to log");        
        }
    }

    fn setup_base_data(dir: TempDir, cargo_path: String) {
        assert!(env::set_current_dir(dir.path()).is_ok());

        println!("{}",format!("{}/{}",cargo_path,"recources/test-segments/1-1.nullsegment"));
        fs::copy(format!("{}/{}",cargo_path,"recources/test-segments/1-1.nullsegment"), "1-1.nullsegment").unwrap();
        fs::copy(format!("{}/{}",cargo_path,"recources/test-segments/1-2.nullsegment"), "1-2.nullsegment").unwrap();
        fs::copy(format!("{}/{}",cargo_path,"recources/test-segments/2-1.nullsegment"), "2-1.nullsegment").unwrap();
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

