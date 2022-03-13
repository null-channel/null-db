use actix_web::{
    get, 
    post, 
    delete, 
    web::{self, Data}, 
    App, 
    Responder, 
    HttpResponse,
    HttpServer
};
#[macro_use]
extern crate lazy_static;
mod file_reader;
//use easy_reader::EasyReader;
use file_reader::EasyReader;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::collections::HashSet;
use std::{
    fs::{
        File,
        write
    },
    io::{
        self,
        Error
    }
};
use std::io::BufReader;
use std::sync::RwLock; // read heavy better for sure -- probably better period.

const TOMBSTONE: &'static str = "-tombstone-";

#[actix_web::main]
async fn main() -> std::io::Result<()> {

    let file_mutex = Data::new(RwLock::new(false));

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
    file_mutex: Data<RwLock<bool>>, 
    web::Path(key): web::Path<String>
) -> impl Responder {
    //it's just protecting the OS's file access
    let _reader = file_mutex.read();

    let file = File::open("null.database").unwrap();
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
    HttpResponse::Ok().body("Key not found")
}

#[post("/{key}")]
pub async fn put_value_for_key(
    file_mutex: Data<RwLock<bool>>,
    web::Path(key): web::Path<String>,
    req_body: String
) -> impl Responder {
    let _writer = file_mutex.write();
    let mut file = OpenOptions::new()
        .write(true)
        .append(true)
        .open("null.database")
        .unwrap();  

    if let Err(e) = writeln!(file,"{}:{}",key, req_body) {
        eprintln!("Couldn't write to file: {}", e);
    }

    HttpResponse::Ok().body("It is saved... to disk!!!")
}

#[delete("/{key}")]
pub async fn delete_value_for_key(
    file_mutex: Data<RwLock<bool>>,
    web::Path(key): web::Path<String>
) -> impl Responder {
    let mut file = OpenOptions::new()
        .write(true)
        .append(true)
        .open("null.database")
        .unwrap();

    if let Err(e) = writeln!(file,"{}:{}",key, TOMBSTONE) {
        eprintln!("Couldn't write to file: {}", e);
    }

    HttpResponse::Ok().body("It has been deleted!")
}

