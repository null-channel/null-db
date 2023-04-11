use crate::{errors, file_compactor, utils};
use anyhow::anyhow;
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::io::BufRead;
use std::sync::RwLock;
use std::{fs::File, io::BufReader};

pub struct NullDB {
    main_log_mutex: RwLock<String>,
    main_log_file_mutex: RwLock<bool>,
    main_log_memory_mutex: RwLock<HashMap<String, String>>,
}

impl NullDB {
    pub fn new(main_log: String) -> NullDB {
        NullDB {
            main_log_mutex: RwLock::new(main_log),
            main_log_file_mutex: RwLock::new(false),
            main_log_memory_mutex: RwLock::new(HashMap::new()),
        }
    }

    // gets name of main log file "right now" does not hold read lock so value maybe be stale
    pub fn get_main_log(&self) -> anyhow::Result<String> {
        match self.main_log_mutex.read() {
            Ok(main_log) => Ok(main_log.clone()),
            Err(_) => Err(anyhow!("Could not get main log file!")),
        }
    }

    // Deletes a record from the log
    pub fn delete_record(&self, key: String) -> anyhow::Result<()> {
        self.write_value_to_log(key, utils::TOMBSTONE.into())
    }

    pub fn get_value_for_key(&self, key: String) -> anyhow::Result<String> {
        // Aquire read lock on main log in memory
        let Ok(main_log) = self.main_log_memory_mutex.read() else {
            println!("Could not get main log file!");
            return Err(anyhow!("Could not get main log file!"));
        };

        // Check the main log first for key
        if let Some(value) = main_log.get(&key) {
            println!("Returned value from main log! {}", value);
            return Ok(value.clone());
        }

        // If not in main log, check all the segments
        let mut generation_mapper =
            utils::get_generations_segment_mapper(file_compactor::SEGMENT_FILE_EXT.to_owned())?;

        /*
         * unstable is faster, but could reorder "same" values.
         * We will not have same values as this was from a set.
         */
        let mut gen_vec: Vec<i32> = generation_mapper.generations.into_iter().collect();
        gen_vec.sort_unstable();

        //Umm... I don't know if this is the best way to do this. it's what I did though, help me?
        let mut gen_iter = gen_vec.into_iter();
        let Ok(main_log_filename) = self.main_log_mutex.read() else {
            return Err(anyhow!("Could not get main log file name!"));
        };

        while let Some(current_gen) = gen_iter.next() {
            println!("Gen {} in progress!", current_gen);
            /*
             * Power of rust, we KNOW that this is safe because we just built it...
             * but it's better to check anyhow... sometimes annoying but.
             */
            if let Some(file_name_vec) = generation_mapper
                .gen_name_segment_files
                .get_mut(&current_gen)
            {
                file_name_vec.sort_unstable();
                let mut file_name_iter = file_name_vec.into_iter();
                while let Some(file_path) = file_name_iter.next_back() {
                    //file names: [gen]-[time].nullsegment
                    let path = format!("{}-{}", current_gen, file_path.clone());

                    // Don't check the main log, we already did that.
                    if path == *main_log_filename {
                        continue;
                    }

                    let file = OpenOptions::new()
                        .read(true)
                        .write(false)
                        .open(path.clone())
                        .expect("db pack file doesn't exist.");

                    let res = utils::check_file_for_key(key.clone(), file);
                    match res {
                        Ok(value) => return Ok(value.clone()),
                        Err(errors::NullDbReadError::ValueDeleted) => {
                            return Ok("value not found".into())
                        }
                        Err(_) => continue, // All other errors (not found in file just mean to check the next file!)
                    }
                }
            }
        }
        Ok("value not found".into())
    }

    // Writes value to log, will create new log if over 64 lines.
    pub fn write_value_to_log(&self, key: String, value: String) -> anyhow::Result<()> {
        println!("Writing to log: {}", value);
        let line_count;
        {
            let main_log = self.main_log_mutex.read();
            let Ok(main_log) = main_log else {
                println!("Could not get main log file!");
                return Err(anyhow!("Could not get main log file!"));
            };
            let file = File::open(main_log.clone())?;
            // make new file if over our 64 lines max
            let f = BufReader::new(file);
            line_count = f.lines().count();
        }

        println!("Line count: {}", line_count);
        if line_count > 64 {
            let main_log = self.main_log_mutex.write();
            let Ok(mut main_log) = main_log else {
                return Err(anyhow!("Could not get main log file!"));
            };
            *main_log = utils::create_next_segment_file()?;
        }

        // Aquire write lock on main log file
        let Ok(_main_log_disk) = self.main_log_file_mutex.write() else {
            return Err(anyhow!("Could not get main log file!"));
        };

        // Aquire write lock on main log memory
        let Ok(mut main_log_memory) = self.main_log_memory_mutex.write() else {
            return Err(anyhow!("Could not get main log in memory!"));
        };

        // Aquire read lock on main log file name
        let Ok(main_log_name) = self.main_log_mutex.read() else {
            return Err(anyhow!("Could not get main log file name!"));
        };

        // Write to memory
        let old_value = main_log_memory.insert(key.clone(), value.clone());

        let mut file = OpenOptions::new()
            .write(true)
            .append(true)
            .open(main_log_name.clone())?;

        let ret = writeln!(file, "{}:{}", key, value);

        if ret.is_err() {
            // If we failed to write to disk, reset the memory to what it was before
            if let Some(old_value) = old_value {
                main_log_memory.insert(key.clone(), old_value);
            } else {
                main_log_memory.remove(&key);
            }
            return Err(anyhow!("Could not write to main log file!"));
        }
        Ok(())
    }
}
