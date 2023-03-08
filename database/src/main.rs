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
use nulldb::NullDB;
use std::sync::mpsc;
mod errors;
mod file_compactor;
mod nulldb;
mod record;
mod utils;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long)]
    compaction: bool,
}

#[actix_web::main]
async fn main() -> Result<(), std::io::Error> {
    let args = Args::parse();

    // get main log file on fresh boot
    let main_log = match utils::create_next_segment_file() {
        Ok(main_log) => main_log,
        Err(e) => {
            panic!("Could not create new main log file! error: {}", e);
        }
    };

    let null_db = NullDB::new(main_log);

    let db_mutex = Data::new(null_db);
    if args.compaction {
        let (_, rx) = mpsc::channel();
        let _file_compactor_thread = file_compactor::start_compaction(rx, db_mutex.clone());
    }

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
