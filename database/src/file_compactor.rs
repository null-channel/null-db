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
use super::utils;

pub const SEGMENT_FILE_EXT: &'static str = "nullsegment";
const MAX_FILE_SIZE: &'static usize = &(1 * 1024); //1kb block

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

pub fn compactor() -> Result<()> {
    let mut segment_files = utils::get_all_files_in_dir("./".to_owned(),SEGMENT_FILE_EXT.to_owned())?;

    /*
    * unstable is faster, but could reorder "same" values. 
    * We will not have same values
    * file names look like this:
    * [generation]-[time].nseg
    */
    segment_files.sort_unstable();

    // pack all old pack files
    let data: &mut HashSet<record::Record> = &mut HashSet::new();
    let mut latest_generation = 0;
    let mut compacted_files: Vec<String> = Vec::new();
    // Collect all new pack files first as these will cary the tombstone and should
    // be removed from all the
    let mut iter = segment_files.into_iter();
    while let Some(file_path) = iter.next_back() {
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
        
        if let Ok(gen) =file_name_breakdown[0].parse() {
            latest_generation = gen;
        }

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

            for f in &compacted_files {
                let res = fs::remove_file(f);
                if res.is_err() {
                    println!("Failed to delete old file:{:?}", res)
                }
            }
            
            // These files have been deleted, clear them!
            compacted_files.clear();
            // This data has been writen and saved to files. Clear it up!
            data.clear();
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

    println!("deleting old logs");
    // delete old compacted files now that the new files are saved to disk.
    for f in &compacted_files {
        let res = fs::remove_file(f);
        if res.is_err() {
            println!("Failed to delete old file:{:?}", res)
        }
    }

    return Ok(());
}

fn get_generation_from_filename(filename: &str) -> &str {
    let file_name = Path::new(filename)
        .file_name().unwrap()
        .to_str().unwrap();
    let split = file_name.split("-").collect::<Vec<&str>>();
    return split[0];
}

fn get_extension_from_filename(filename: &str) -> Option<&str> {
    Path::new(filename)
        .extension()
        .and_then(OsStr::to_str)
}
