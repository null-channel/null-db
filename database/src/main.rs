use std::{mem, path::Path, ffi::OsStr, error::Error, io};
use actix_web::{
    delete,
    dev::HttpResponseBuilder,
    get, post,
    web::{self, Data},
    App, HttpResponse, HttpServer, Responder,
};
#[macro_use]
extern crate lazy_static;
mod file_reader;
//use easy_reader::EasyReader;
use clap::Parser;
use file_reader::EasyReader;
use std::collections::HashSet;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::io::BufReader;
use std::sync::mpsc;
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{
    fs::{copy, write, File},
}; // read heavy -- probably better period.
mod file_compactor;
mod record;
mod utils;
mod errors;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long)]
    compaction: bool,
}

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
            .service(compact_data)
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
}

#[get("/v1/data/{key}")]
pub async fn get_value_for_key(
    file_mutex: Data<RwLock<bool>>,
    web::Path(key): web::Path<String>,
) -> impl Responder {
    {
        // Scope so read lock drops after finished with the "main" file.
        let _reader = file_mutex.read();
        let file = File::open("null.database").unwrap();

        let res = utils::check_file_for_key(key.clone(), file);
        match res {
            Ok(value) => return HttpResponse::Ok().body(value.clone()),
            Err(errors::NullDbReadError::ValueDeleted) => {
                return HttpResponse::Ok().body("value not found")
            }
            Err(_) => (), // All other errors mean we need to check the segments!
        }
    }
    // Read lock not needed anymore

    // We did not find it in the main writable file. Lets check all the other ones now!
    let mut segment_files =
        utils::get_all_files_in_dir("./".to_owned(), utils::LOG_SEGMENT_EXT.to_owned()).unwrap();

    /*
    * unstable is faster, but could reorder "same" values.
    * We will not have same values
    * it does not matter even if we did
    * file names look like this:
    * [time].nullsegment
    */
    segment_files.sort_unstable();

    let mut iter = segment_files.into_iter();

    while let Some(file_path) = iter.next() {
        println!("{}", file_path);
        //for file_path in pack_files.clone() {

        let file = File::open(file_path.clone()).unwrap();

        let res = utils::check_file_for_key(key.clone(), file);
        match res {
            Ok(value) => return HttpResponse::Ok().body(value.clone()),
            Err(errors::NullDbReadError::ValueDeleted) => {
                return HttpResponse::Ok().body("value not found")
            }
            Err(_) => continue, // All other errors (not found in file just mean to check the next file!)
        }
    }
    HttpResponse::Ok().body("value not found")
}

#[post("/v1/data/{key}")]
pub async fn put_value_for_key(
    file_mutex: Data<RwLock<bool>>,
    web::Path(key): web::Path<String>,
    req_body: String,
) -> impl Responder {
     // Locking lets us protect the integraty of our file for now
     let _write_lock = file_mutex.write();
    
     let res = write_value_to_log(format!("{}:{}", key,req_body ));
     match res {
         Err(_) => HttpResponse::InternalServerError(),
         Ok(_) => HttpResponse::Ok(),
     }
}

#[delete("v1/data/{key}")]
pub async fn delete_value_for_key(
    file_mutex: Data<RwLock<bool>>,
    web::Path(key): web::Path<String>,
) -> impl Responder {
    let mut file = OpenOptions::new()
        .write(true)
        .append(true)
        .open("null.database")
        .unwrap();

    if let Err(e) = writeln!(file, "{}:{}", key, utils::TOMBSTONE) {
        eprintln!("Couldn't write to file: {}", e);
    }

    HttpResponse::Ok().body("It has been deleted!")
}

// only writes to the null.database, does no locking.
fn write_value_to_log(value: String) -> Result<(), io::Error> {
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
        
        std::fs::copy("null.database", format!("0-{:?}.{}", since_the_epoch, utils::LOG_SEGMENT_EXT)).unwrap();

        // delete all old data and write new key
        let mut tun = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open("null.database")
            .unwrap(); 
        if let Err(e) = writeln!(tun,"{}",value) {
            return Err(e);
        }
        return Ok(());
    }

    let mut file = OpenOptions::new()
        .write(true)
        .append(true)
        .open("null.database")
        .unwrap();  
        
    if let Err(e) = writeln!(file,"{}",value) {
        return Err(e);
    }

    return Ok(())

}

#[get("/v1/management/compact")]
pub async fn compact_data() -> impl Responder {
    println!("compacting!");
    let res = file_compactor::compactor();

    if let Ok(_) = res {
        return HttpResponse::Ok();
    }

    HttpResponse::InternalServerError()
}



