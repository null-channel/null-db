use crate::{errors, file_compactor, utils};
use anyhow::anyhow;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::io::BufRead;
use std::sync::RwLock;
use std::{fs::File, io::BufReader};

pub struct NullDB {
    main_log_mutex: RwLock<String>,
    main_log_file_mutex: RwLock<bool>,
}

impl NullDB {
    pub fn new(main_log: String) -> NullDB {
        NullDB {
            main_log_mutex: RwLock::new(main_log),
            main_log_file_mutex: RwLock::new(false),
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
        self.write_value_to_log(format!("{}:{}", key, utils::TOMBSTONE))
    }

    pub fn get_value_for_key(&self, key: String) -> anyhow::Result<String> {
        // Scope so read lock drops after finished with the "main" file.
        let main_log = self.main_log_mutex.read();
        let Ok(main_log) = main_log else {
            return Err(anyhow!("Could not get main log file!"));
        };

        {
            let _file_lock = self.main_log_file_mutex.read();
            let file = File::open(main_log.clone()).unwrap();

            let res = utils::check_file_for_key(key.clone(), file);
            match res {
                Ok(value) => return Ok(value.clone()),
                Err(errors::NullDbReadError::ValueDeleted) => {
                    return Ok("value not found".into());
                }
                Err(_) => (), // All other errors mean we need to check the segments!
            }
        }
        // write lock not needed anymore

        let mut generation_mapper =
            utils::get_generations_segment_mapper(file_compactor::SEGMENT_FILE_EXT.to_owned())
                .unwrap();

        /*
         * unstable is faster, but could reorder "same" values.
         * We will not have same values as this was from a set.
         */
        let mut gen_vec: Vec<i32> = generation_mapper.generations.into_iter().collect();
        gen_vec.sort_unstable();

        //Umm... I don't know if this is the best way to do this. it's what I did though, help me?
        let mut gen_iter = gen_vec.into_iter();

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
                    if path == main_log.clone() {
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
    pub fn write_value_to_log(&self, value: String) -> anyhow::Result<()> {
        println!("Writing to log: {}", value);
        let line_count;
        {
            let main_log = self.main_log_mutex.read();
            let Ok(main_log) = main_log else {
                println!("Could not get main log file!");
                return Err(anyhow!("Could not get main log file!"));
            };
            println!("just before opening file");
            let file = File::open(main_log.clone())?;
            println!("just after opening file");
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

        let main_log = self.main_log_mutex.read();
        let Ok(main_log) = main_log else {
            return Err(anyhow!("Could not get main log file!"));
        };

        let mut file = OpenOptions::new()
            .write(true)
            .append(true)
            .open(main_log.clone())?;

        writeln!(file, "{}", value)?;
        Ok(())
    }
}
