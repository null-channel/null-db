use actix_web::{
    get, 
    post, 
    delete, 
    web, 
    App, 
    Responder, 
    Result,
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
use std::sync::RwLock; // read heavy -- probably better period.

lazy_static! {
    static ref LOCK: RwLock<bool> = {
        let mut m = false;
        RwLock::new(m)
    };
}
#[actix_web::main]
async fn main() -> std::io::Result<()> {
        HttpServer::new(|| {
            App::new()
                .service(get_value_for_key)
                .service(put_value_for_key)
                .service(delete_value_for_key)
        })
        .bind("127.0.0.1:8080")?
        .run()
        .await
    
}

#[get("/{key}")]
pub async fn get_value_for_key(web::Path(key): web::Path<String>) -> impl Responder {

    let file = File::open("null.database").unwrap();
    let mut reader = EasyReader::new(file).unwrap();
    // Generate index (optional)
    reader.build_index();
    reader.eof();
    while let Some(line) = reader.prev_line().unwrap() {
        let split = line.split(":").collect::<Vec<&str>>();
        if split.len() == 2 {
            if split[0] == key {
                return HttpResponse::Ok().body(split[1].to_string().clone());
            }
        }
        println!("{}", line);
    }

    // Repeat process by seeking back by chunk_size again.;
    HttpResponse::Ok().body("value not found")
}

#[post("/{key}")]
pub async fn put_value_for_key(web::Path(key): web::Path<String>,req_body: String) -> impl Responder {
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
pub async fn delete_value_for_key(web::Path(key): web::Path<String>) -> impl Responder {
    let file = File::open("null.database").unwrap();
    let mut reader = EasyReader::new(file).unwrap();

    // Generate index (optional)
    reader.build_index();
    let mut lines = HashSet::new();
    while let Some(line) = reader.prev_line().unwrap() {
        let split = line.split(":").collect::<Vec<&str>>();
        if split.len() == 2 {
            if split[0] == key {
                let current_line = *reader
                .newline_map
                .get(&(reader.current_start_line_offset as usize))
                .unwrap();
                lines.insert(current_line);
            }
        }
        println!("{}", line);
    }

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open("null.database")
        .expect("file.txt doesn't exist or so");

    let mut lines = BufReader::new(file).lines()
        .flat_map(|x| {
            if let Ok(line) = x {
                let split = line.split(":").collect::<Vec<&str>>();
                if split.len() == 2 {
                    if split[0] == key {
                        return None;
                    }
                    else {
                        return Some(line);
                    }
                }
            }
            return None;
        })
        .collect::<Vec<String>>().join("\n");

    std::fs::write("null.database", lines).expect("Can't write");
    
    HttpResponse::Ok().body("It has been deleted!")
}

pub async fn manual_hello() -> impl Responder {
    HttpResponse::Ok().body("Hey there!")
}

enum PutError {
}

enum GetError {

}

async fn put_value_into_log(key: String) -> Result<(), PutError> {

    return Ok(())
}

pub async fn get_value_from_log(key: String) -> Result<String, GetError> {

    return Ok("yes".to_owned());
}