use std::sync::mpsc::Receiver;
use std::time::Duration;
use std::sync::mpsc;
use std::sync::mpsc::TryRecvError;
use std::collections::HashMap;
use std::collections::HashSet;
use std::time::{SystemTime, UNIX_EPOCH};
use std::mem;
use std::io::BufReader;
use std::ffi::OsString;
use std::io::BufRead;
use std::ffi::OsStr;
use std::io::{Write, Result};
use std::io::prelude::*;
use std::{thread, time};
use std::error::Error;
use std::{
    fs::{
        self,
        File,
        write,
        copy
    },
    io::{
        self
    }
};
use std::fs::OpenOptions;
use std::path::Path;
use super::record;

pub const SEGMENT_FILE_EXT: &'static str = "nullsegment";
const MAX_FILE_SIZE: &'static usize = &(1 * 1024 * 1024); //1mb block

pub async fn start_compaction(rx: Receiver<i32>) {

    thread::spawn(move || loop {
        match rx.try_recv() {
            Err(TryRecvError::Empty) => println!("compact!"),
            _ => {
                println!("call to quite compaction");
                break;
            }
        }
        compactor();
        // TODO: Make configurable? Need configuration filz...

        println!("Suspending...");
        thread::sleep(time::Duration::from_secs(30));
    });
}

fn get_all_files_in_dir(path: String, ext: String) -> Result<Vec<String>> {
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

pub fn compactor() -> Result<()> {
    let mut segment_files = get_all_files_in_dir("./".to_owned(),SEGMENT_FILE_EXT.to_owned())?;

    /*
    * unstable is faster, but could reorder "same" values. 
    * We will not have same values
    * it does not matter even if we did
    * file names look like this:
    * [gen]-[time].nseg
    */
    segment_files.sort_unstable();

    // pack all old pack files
    let data: &mut HashSet<record::Record> = &mut HashSet::new();
    let mut latest_generation = 0;
    let mut compacted_files: Vec<String> = Vec::new();
    // Collect all new pack files first as these will cary the tombstone and should
    // be removed from all the
    let mut iter = segment_files.into_iter();
    while let Some(file_path) = iter.next() {
        println!("{}",file_path);
    //for file_path in pack_files.clone() {

        let file = OpenOptions::new()
            .read(true)
            .write(false)
            .open(file_path.clone())
            .expect("db pack file doesn't exist.");

        //file names: [gen]-[time].nullsegment
        let path = file_path.clone();
        let file_name_breakdown = path.split("-").collect::<Vec<&str>>();
        latest_generation = file_name_breakdown[0].parse().unwrap();

        // Read file into buffer reader
        let f = BufReader::new(file);
        // break it into lines
        let lines = f.lines();
        // insert each line into our set.
        for line in lines {
            if let Ok(l) = line {
                // need to use our hashing object so only the "key" is looked at. pretty cool.
                // have no idea why i'm so excited about this one single bit.
                // this is what makes software engineering fun.
                if let Some(record) = record::Record::new(l) {
                    data.insert(record);
                }
            }
        }

        compacted_files.push(file_path.clone());

        // If we are over our max file size, lets flush to disk
        // for now, we will just check at the end of each file.
        if mem::size_of_val(&data) > *MAX_FILE_SIZE {
            
            // Calculate file generation
            let file_gen = latest_generation + 1;

            // current time
            let start = SystemTime::now();
            let since_the_epoch = start
                .duration_since(UNIX_EPOCH).unwrap();

            // Create new file
            let mut new_file = OpenOptions::new()
                .write(true)
                .create(true)
                .open(format!("{}-{:?}.nullsegment",file_gen,since_the_epoch))
                .unwrap(); 
            
            // interesting we don't "care" about the order now
            // becuase all records are unique
            for r in data.iter() {
                if let Err(e) = writeln!(new_file,"{}",r.get_string()) {
                    eprintln!("Couldn't write to file: {}", e);
                }
            }
            
            // delete files saved to disk
            for f in &compacted_files {
                fs::remove_file(f)?;
            }

            data.clear();
            compacted_files.clear();
        }
    }

    if mem::size_of_val(&data) > 0 {
            
        // Calculate file generation
        let file_gen = latest_generation + 1;

        // current time
        let start = SystemTime::now();
        let since_the_epoch = start
            .duration_since(UNIX_EPOCH).unwrap();

        // Create new file
        let mut new_file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(format!("{}-{:?}.nullsegment",file_gen,since_the_epoch))
            .unwrap(); 
        
        // interesting we don't "care" about the order now
        // becuase all records are unique
        for r in data.iter() {
            if let Err(e) = writeln!(new_file,"{}",r.get_string()) {
                eprintln!("Couldn't write to file: {}", e);
            }
        }
    }
    return Ok(());
}

fn get_generation_from_filename(filename: &str) -> &str {
    let file_name = Path::new(filename)
        .file_name().unwrap()
        .to_str().unwrap();
    let split = file_name.split(":").collect::<Vec<&str>>();
    return split[0];
}

fn get_extension_from_filename(filename: &str) -> Option<&str> {
    Path::new(filename)
        .extension()
        .and_then(OsStr::to_str)
}
