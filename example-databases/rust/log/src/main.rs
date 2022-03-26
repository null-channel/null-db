use actix_web::{
    get, 
    post, 
    delete, 
    web::{self, Data}, 
    App, 
    Responder, 
    HttpResponse,
    HttpServer, dev::HttpResponseBuilder
};
#[macro_use]
extern crate lazy_static;
mod file_reader;
//use easy_reader::EasyReader;
use file_reader::EasyReader;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::{
    fs::{
        File,
    },
    io::{
        self,
        Error
    }
};
use std::sync::RwLock; // read heavy better for sure -- probably better period.

const TOMBSTONE: &'static str = "~tombstone~";

#[actix_web::main]
async fn main() -> std::io::Result<()> {

    let file_mutex = Data::new(RwLock::new("null.db"));

    HttpServer::new(move || {
        App::new()
            .app_data(file_mutex.clone())
            .service(get_value_for_key)
            .service(put_value_for_key)
            .service(delete_value_for_key)
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
    
}

#[get("/{key}")]
pub async fn get_value_for_key(
    file_mutex: Data<RwLock<&str>>, 
    web::Path(key): web::Path<String>
) -> impl Responder {
    //it's just protecting the OS's file access
    let reader = *file_mutex.read().unwrap();

    let file = File::open(reader).unwrap();
    let mut reader = EasyReader::new(file).unwrap();
    // Generate index (optional)
    reader.build_index();
    reader.eof();
    while let Some(line) = reader.prev_line().unwrap() {
        let split = line.split(":").collect::<Vec<&str>>();
        if split.len() == 2 {
            if split[0] == key {
                let value = split[1].to_string().clone();
                if value == TOMBSTONE {
                    return HttpResponse::Ok().body("Key not found");
                }
                return HttpResponse::Ok().body(value);
            }
        }
        println!("{}", line);
    }

    // Repeat process by seeking back by chunk_size again.;
    return HttpResponse::NotFound().body("");
}

#[post("/{key}")]
pub async fn put_value_for_key(
    file_mutex: Data<RwLock<&str>>,
    web::Path(key): web::Path<String>,
    req_body: String
) -> impl Responder {
    let writer = file_mutex.write().unwrap();
    let mut file = OpenOptions::new()
        .write(true)
        .append(true)
        .open(*writer)
        .unwrap();  

    if let Err(e) = writeln!(file,"{}:{}",key, req_body) {
        eprintln!("Couldn't write to file: {}", e);
        return HttpResponse::InternalServerError();
    }

    return HttpResponseBuilder::from(HttpResponse::Ok().body("It is saved... to disk!!!"));
}

#[delete("/{key}")]
pub async fn delete_value_for_key(
    file_mutex: Data<RwLock<&str>>,
    web::Path(key): web::Path<String>
) -> impl Responder {

    let writer = file_mutex.write().unwrap();
    let mut file = OpenOptions::new()
        .write(true)
        .append(true)
        .open(*writer)
        .unwrap();

    if let Err(e) = writeln!(file,"{}:{}",key, TOMBSTONE) {
        eprintln!("Couldn't write to file: {}", e);
        return HttpResponse::InternalServerError();
    }

    return HttpResponseBuilder::from(HttpResponse::Ok().body("Record Deleted"));
}

