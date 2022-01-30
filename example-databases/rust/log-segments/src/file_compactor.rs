use std::sync::mpsc::Receiver;
use std::time::Duration;
use std::sync::mpsc;
use std::sync::mpsc::TryRecvError;
use std::collections::HashMap;
use std::collections::BTreeMap;
use std::io::BufReader;
use std::ffi::OsString;
use std::io::BufRead;
use std::ffi::OsStr;
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
        thread::sleep(time::Duration::from_secs(5));
    });

}

fn compactor() {
    let paths = std::fs::read_dir("./").unwrap();

    let mut dbfiles = paths.into_iter().flat_map(|x| {
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
    
    dbfiles.sort_unstable();

    let mut data_map = HashMap::new();

    for file_path in dbfiles {

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file_path)
            .expect("db pack file doesn't exist.");

    /* 
        reader.build_index();
        reader.eof();
        while let Some(line) = reader.prev_line().unwrap() {
            
        }
        */
        let f = BufReader::new(file);
        let lines = f.lines();
        for line in lines {
            let line_r = line.unwrap();
            let split = line_r.split(":").collect::<Vec<&str>>();
                if split.len() == 2 {
                    if !data_map.contains_key(split[0]) {
                        data_map.insert(split[0].to_string(),split[1].to_string());
                    } 
                }
            }
        }
    if data_map.len() > 64 {

    }

}
 

fn get_extension_from_filename(filename: &str) -> Option<&str> {
    Path::new(filename)
        .extension()
        .and_then(OsStr::to_str)
}