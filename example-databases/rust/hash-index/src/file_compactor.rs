use std::sync::mpsc::Receiver;
use std::time::Duration;
use std::sync::mpsc;
use std::sync::mpsc::TryRecvError;
use std::collections::HashMap;
use std::collections::HashSet;
use std::time::{SystemTime, UNIX_EPOCH};
use std::io::BufReader;
use std::ffi::OsString;
use std::io::BufRead;
use std::ffi::OsStr;
use std::io::{Write, Result};
use std::io::prelude::*;
use std::{thread, time};
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
use std::fs::OpenOptions;
use std::path::Path;

pub fn start_compaction(rx: Receiver<i32>) {

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

fn compactor() {
    let paths = std::fs::read_dir("./").unwrap();

    let mut pack_files = paths.into_iter().flat_map(|x| {
        match x {
            Ok(y) => {
                if get_extension_from_filename(y.file_name().to_str().unwrap()) == Some("npack") {
                    return Some(y.file_name().into_string().unwrap());
                }
            }
            Err(_) => return None
        }
        return None;
    }).collect::<Vec<String>>();

    let new_pack_files_dir = std::fs::read_dir("./").unwrap();
    let mut new_pack_files = new_pack_files_dir.into_iter().flat_map(|x| {
        match x {
            Ok(y) => {
                if get_extension_from_filename(y.file_name().to_str().unwrap()) == Some("nnpack") {
                    println!("new pack file found");
                    return Some(y.file_name().into_string().unwrap());
                }
            }
            Err(_) => return None
        }
        return None;
    }).collect::<Vec<String>>();

    /*
    * unstable is faster, but could reorder "same" values. 
    * We will not have same values
    * it does not matter even if we did
    */
    pack_files.sort_unstable();

    // pack all old pack files
    let mut data_map = HashMap::new();
    let mut to_delete = HashSet::new();
    // Collect all new pack files first as these will cary the tombstone and should
    // be removed from all the
    for file_path in new_pack_files.clone() {

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file_path)
            .expect("db pack file doesn't exist.");

        let f = BufReader::new(file);
        let lines = f.lines();
        for line in lines {
            let line_r = line.unwrap();
            let split = line_r.split(":").collect::<Vec<&str>>();
            if split.len() == 2 {
                if !data_map.contains_key(split[0]) {
                    if split[1] == "-tombstone-" {
                        //Add this to the set to be deleted
                        to_delete.insert(split[0].to_string());
                    } else {
                        // Addthis to the data to be compacted
                        data_map.insert(split[0].to_string(),split[1].to_string());
                    }
                }
            }
        }
    }

    for file_path in pack_files.clone() {

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file_path)
            .expect("db pack file doesn't exist.");

        let f = BufReader::new(file);
        let lines = f.lines();
        for line in lines {
            let line_r = line.unwrap();
            let split = line_r.split(":").collect::<Vec<&str>>();
            if split.len() == 2 {
                if !data_map.contains_key(split[0]) {
                    if !to_delete.contains(split[0]) {
                        data_map.insert(split[0].to_string(),split[1].to_string());
                    }
                }
            }
        }
    }

    
    let start = SystemTime::now();
    let since_the_epoch = start
        .duration_since(UNIX_EPOCH)
        .unwrap();

    // write compacted data
    let mut i = 0;
    let mut leftover = data_map.len()%64;
    let data = data_map
        .iter()
        .map(|x| {
            let (key, value) = x;
            i = i + 1;
            return format!("{}:{}",key,value)
        }).collect::<Vec<String>>();
    while i < data_map.len() && (i + 64) < data_map.len() {
        let to_write = &data[i..(i+64)];
        // use i to make files sortable by "youngest" but named differently
        // only works with a single threaded compacter.
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .open(format!("{:?}{}.{}", since_the_epoch,i, "npack"))
            .unwrap();
        for line in to_write {
            if let Err(e) = writeln!(file,"{}",line) {
                eprintln!("Couldn't write to file: {}", e);
            }
        }
    }

    let start = data_map.len() - leftover;

    let to_write = &data[start..data_map.len()];
    // use i to make files sortable by "youngest" but named differently
    // only works with a single threaded compacter.
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .open(format!("{:?}{}.{}", since_the_epoch,i, "npack"))
        .unwrap();
    for line in to_write {
        if let Err(e) = writeln!(file,"{}",line) {
            eprintln!("Couldn't write to file: {}", e);
        }
    }


    // delete old npack files
    for file_path in pack_files {
        std::fs::remove_file(file_path);
    }

    // delete old npack files
    for file_path in new_pack_files {
        std::fs::remove_file(file_path);
    }

}
 

fn get_extension_from_filename(filename: &str) -> Option<&str> {
    Path::new(filename)
        .extension()
        .and_then(OsStr::to_str)
}

fn get_generation_from_filename(filename: &str) -> &str {
    let file_name = Path::new(filename)
        .file_name().unwrap()
        .to_str().unwrap();
    let split = file_name.split(":").collect::<Vec<&str>>();
    return split[0];
}

