use std::{mem, path::Path, ffi::OsStr, error::Error, io};
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
use std::{
    fs::{
        File 
    }
};
use clap::Parser;
use std::time::{SystemTime, UNIX_EPOCH};
use std::io::BufReader;
use std::sync::RwLock; // read heavy -- probably better period.

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short,long)]
    compaction: bool,
}

const TOMBSTONE: &'static str = "~tombstone~";
const LOG_SEGMENT_EXT: &'static str = "nullsegment";

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

#[get("/v1/data/{key}")]
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

    // We did not find it in the main writable file. Lets check all the other ones now!
    let mut segment_files = get_all_files_in_dir("./".to_owned(),LOG_SEGMENT_EXT.to_owned()).unwrap();

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
        println!("{}",file_path);
    //for file_path in pack_files.clone() {

        let file = File::open(file_path.clone()).unwrap();

        let res = check_file_for_key(key.clone(), file);
        match res {
            Ok(value) => return HttpResponse::Ok().body(value.clone()),
            Err(NullDbReadError::ValueDeleted) => return HttpResponse::Ok().body("value not found"),
            Err(_) => continue, // All other errors (not found in file just mean to check the next file!)
        }
    }
    HttpResponse::Ok().body("value not found")
}

fn check_file_for_key(key: String, file: File) -> Result<String, NullDbReadError> {
    let mut reader = EasyReader::new(file).unwrap();
    // Generate index (optional)
    reader.build_index();
    reader.eof();
    while let Some(line) = reader.prev_line().unwrap() {
        let split = line.split(":").collect::<Vec<&str>>();
        if split.len() != 2 {
            continue;
        }
        if split[0] == key {
            let val = split[1].to_string().clone();
            if val == TOMBSTONE {
                return Err(NullDbReadError::ValueDeleted);
            }
            return Ok(split[1].to_string().clone());
        }
    }
    return Err(NullDbReadError::ValueNotFound);
}

pub enum NullDbReadError {
    ValueNotFound,
    ValueDeleted,
    IOError(io::Error),
}

#[post("/v1/data/{key}")]
pub async fn put_value_for_key(
    file_mutex: Data<RwLock<bool>>, 
    web::Path(key): web::Path<String>,
    req_body: String
) -> impl Responder {
    // Locking lets us protect the integraty of our file for now
    let _write_lock = file_mutex.write();

    if let(e) = write_value_to_log(format!("{}:{}", key,req_body )) {
        return HttpResponse::InternalServerError();
    }
  
    HttpResponse::Ok()
}

#[delete("v1/data/{key}")]
pub async fn delete_value_for_key(
    file_mutex: Data<RwLock<bool>>, 
    web::Path(key): web::Path<String>
) -> impl Responder {
    let _write_lock = file_mutex.write();

    if let(e) = write_value_to_log(format!("{}:{}", key, TOMBSTONE)) {
        return HttpResponse::InternalServerError();
    }
  
    return HttpResponse::Ok();
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
        
        std::fs::copy("null.database", format!("{:?}.{}", since_the_epoch, LOG_SEGMENT_EXT)).unwrap();

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

fn get_all_files_in_dir(path: String, ext: String) -> Result<Vec<String>,Box<Error>> {
    let paths = std::fs::read_dir(path)?;
    let mut file_paths = paths.into_iter().flat_map(|x| {
        match x {
            Ok(y) => {
                if get_extension_from_filename(y.file_name().to_str()?) == Some(&ext) {
                    return Some(y.file_name().into_string().unwrap());
                }
            }
            Err(_) => return None
        }
        return None;
    }).collect::<Vec<String>>();
    return Ok(file_paths);
}

fn get_extension_from_filename(filename: &str) -> Option<&str> {
    Path::new(filename)
        .extension()
        .and_then(OsStr::to_str)
}