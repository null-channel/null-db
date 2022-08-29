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
        let ret = compactor();
        
        if ret.is_err() {
            println!("Error compacting {:?}", ret);
        }

        println!("Suspending...");
        thread::sleep(time::Duration::from_secs(30));
    });
}

pub fn compactor() -> Result<()> {
    
    let mut segment_files = utils::get_all_files_in_dir("./".to_owned(),SEGMENT_FILE_EXT.to_owned())?;

    // stores the files for a generation
    let mut gen_name_segment_files: HashMap<i32, Vec<String>> = HashMap::new();
    // easy to iterate over list of generations
    let mut generations: HashSet<i32> = HashSet::new();

    let mut iter = segment_files.into_iter();
    while let Some(file_path) = iter.next_back() {
        //file names: [gen]-[time].nullsegment
        let path = file_path.clone();
        /*
        * file names look like this:
        * [generation]-[time].nseg
        */
        let file_name_breakdown = path.split("-").collect::<Vec<&str>>();

        if let Ok(gen_val) =file_name_breakdown[0].parse::<i32>() {
            generations.insert(gen_val);
            if let Some(generation) = gen_name_segment_files.get_mut(&gen_val) {
                generation.push(file_name_breakdown[1].to_string());
            } else { //This gen does not have a vec yet! Create it!
                let mut v = vec![file_name_breakdown[1].to_string()];
                gen_name_segment_files.insert(gen_val, v);
            }
        }
    }

    /*
    * unstable is faster, but could reorder "same" values. 
    * We will not have same values as this was from a set.
    */
    let mut gen_vec:Vec<i32> = generations.into_iter().collect();
    gen_vec.sort_unstable();

    /* Setup the variables */
    let data: &mut HashSet<record::Record> = &mut HashSet::new();
    let mut largest_generation = 0;
    let mut compacted_files: Vec<String> = Vec::new();

    //Umm... I don't know if this is the best way to do this. it's what I did though, help me?
    let mut gen_iter = gen_vec.into_iter();

    while let Some(current_gen) = gen_iter.next_back() {
        println!("Gen {} in progress!", current_gen);
        /* 
        * Power of rust, we KNOW that this is safe because we just built it...
        * but it's better to check anyhow... sometimes annoying but.
        */
        if let Some(file_name_vec) = gen_name_segment_files.get(&current_gen) {
            let mut file_name_iter = file_name_vec.into_iter();
            while let Some(file_path) = file_name_iter.next_back() {
                
                //file names: [gen]-[time].nullsegment
                let path = format!("{}-{}",current_gen,file_path.clone());
                
                println!("{}", path);

                if largest_generation < current_gen {
                    largest_generation = current_gen;
                }                
        
                let file = OpenOptions::new()
                    .read(true)
                    .write(false)
                    .open(path.clone())
                    .expect("db pack file doesn't exist.");
        
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
                            data.replace(record);
                        }
                    }
                }
        
                compacted_files.push(path.clone());
        
                // If we are over our max file size, lets flush to disk
                // for now, we will just check at the end of each file.
                if mem::size_of_val(&data) > *MAX_FILE_SIZE {
                    
                    // Calculate file generation
                    let file_gen = current_gen + 1;
        
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
                        let res = fs::remove_file(path.clone());
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
        }

    }

    /*
    * Same as above, just need to check if there is some data left over at the end
    * We will just flush it to a file. no problem.
    */
    if mem::size_of_val(&data) > 0 {
            
        // Calculate file generation
        let file_gen = 1;

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

/* 
* This test hearts my soul. It made me regret much.
* Sadly it's all I have time for if I want to get this video out.
* so it's what we get. deal with it.
* The whole compaction function needs refactored to make this better.
* and that would be good, but a whole video in and of it'self.
* maybe the next video?
*/
#[cfg(test)]
mod tests {
    use std::env;
    use std::path::Path;
    use std::fs;
    use super::utils;
    use super::compactor;
    #[test]
    fn compation() {
        if let Ok(path) = env::var("CARGO_MANIFEST_DIR") {
            let test_file_location = format!("{}/{}",path,"recources/test-clear");
            assert!(env::set_current_dir(&test_file_location).is_ok());

            assert!(fs::copy("../test-segments/1-1.nullsegment", "1-1.nullsegment").is_ok());
            assert!(fs::copy("../test-segments/1-2.nullsegment", "1-2.nullsegment").is_ok());
            assert!(fs::copy("../test-segments/2-1.nullsegment", "2-1.nullsegment").is_ok());

            compactor();

            if let Ok(files) = utils::get_all_files_in_dir("./".to_owned(),"nullsegment".to_owned()) {
                assert!(files.len() == 1);

                if let Some(f) = files.first() {
                    // remove the file so the tests passes again.
                    // should check the value of the file.. but...
                    fs::remove_file(f);
                }
            }
        }
    }
}
