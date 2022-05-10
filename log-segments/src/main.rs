use std::mem;
use actix_web::{
    get, 
    post, 
    delete, 
    dev::HttpResponseBuilder,
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
use std::sync::mpsc;
use std::{
    fs::{
        File,
        write,
        copy 
    },
    io::{
        self,
        Error
    }
};
use clap::Parser;
use std::time::{SystemTime, UNIX_EPOCH};
use std::io::BufReader;
use std::sync::RwLock; // read heavy -- probably better period.
mod file_compactor;
mod record;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short,long)]
    compaction: bool,
}

const TOMBSTONE: &'static str = "~tombstone~";

#[actix_web::main]
async fn main() -> std::io::Result<()> {  
    let args = Args::parse();
    
    if args.compaction {
        let (tx, rx) = mpsc::channel();
        file_compactor::start_compaction(rx);
    }

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
                let val = split[1].to_string().clone();
                if val == TOMBSTONE {
                    return HttpResponse::Ok().body("value not found");
                }
                return HttpResponse::Ok().body(split[1].to_string().clone());
            }
        }
        println!("{}", line);
    }

    // Repeat process by seeking back by chunk_size again.;
    HttpResponse::Ok().body("value not found")
}

#[post("/{key}")]
pub async fn put_value_for_key(
    file_mutex: Data<RwLock<bool>>, 
    web::Path(key): web::Path<String>,
    req_body: String
) -> impl Responder {
    // Locking lets us protect the integraty of our file for now
    // 
    let _write_lock = file_mutex.write();
    let mut line_count = 0;
    {
        let file = File::open("null.database").unwrap(); 

        // make new file if over our 64 lines max
        let f = BufReader::new(file);
        line_count = f.lines().count();
    }
    
    if line_count > 64 {

        let start = SystemTime::now();
        let since_the_epoch = start
            .duration_since(UNIX_EPOCH)
            .unwrap();
        
        std::fs::copy("null.database", format!("0-{:?}.{}", since_the_epoch, file_compactor::SEGMENT_FILE_EXT)).unwrap();

        // delete all old data and write new key
        let mut tun = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open("null.database")
            .unwrap(); 
        if let Err(e) = writeln!(tun,"{}:{}",key, req_body) {
            eprintln!("Couldn't write to file: {}", e);
        }
        return HttpResponse::Ok().body("Saved and made log file");
    }

    let mut file = OpenOptions::new()
        .write(true)
        .append(true)
        .open("null.database")
        .unwrap();  
        
    if let Err(e) = writeln!(file,"{}:{}",key, req_body) {
        eprintln!("Couldn't write to file: {}", e);
    }

    //HttpResponse::Ok().body("It is saved, no log file needed")
    return HttpResponse::NotImplemented().body("");
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

#[get("/compact")]
pub async fn compact_data() -> impl Responder {

    let res = file_compactor::compactor();

    if let Ok(_) = res {
        return HttpResponse::Ok();
    }

    HttpResponse::InternalServerError()
}

