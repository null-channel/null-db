use crate::errors::NullDbReadError;
use crate::file::{FileEngine, Record};
use crate::index;
use crate::index::*;
use crate::EasyReader;
use crate::raft::raft::LogEntry;
use crate::{errors, file_compactor, utils};
use actix_web::web::Data;
use anyhow::anyhow;
use std::collections::HashMap;
use std::convert::TryInto;
use std::fs::OpenOptions;
use std::io::{prelude::*, self};
use std::io::BufRead;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, RwLockWriteGuard};
use std::sync::RwLock;
use std::time;
use std::{fs::File, io::BufReader};

pub const TOMBSTONE: &'static str = "~tombstone~";
pub const LOG_SEGMENT_EXT: &'static str = "nullsegment";

pub struct NullDB {
    main_log_mutex: RwLock<PathBuf>,
    main_log_file_mutex: RwLock<bool>,
    main_log_memory_mutex: RwLock<HashMap<String, Record>>,
    // Segment, Index
    log_indexes: RwLock<HashMap<PathBuf, Index>>,
    pub config: RwLock<Config>,
    file_engine: FileEngine,
    pub current_raft_index: AtomicU64,
}

#[derive(Clone, Debug)]
pub struct Config {
    path: PathBuf,
    compaction: bool,
    encoding: String,
}

impl Config {
    pub fn new(path: PathBuf, compaction: bool, encoding: String) -> Config {
        Config {
            path,
            compaction,
            encoding,
        }
    }
}

// TODO: pass in PathBuff to define where this database is working
pub fn create_db(config: Config) -> anyhow::Result<Data<NullDB>> {
    let null_db = NullDB::new(config.clone());

    let Ok(null_db) = null_db else {
        panic!("Could not create indexes!!!");
    };

    let db_arc = Data::new(null_db);
    if config.compaction {
        let (_, rx) = mpsc::channel();
        let _file_compactor_thread = file_compactor::start_compaction(rx, db_arc.clone());
    }
    Ok(db_arc)
}

impl NullDB {
    pub fn get_db_path(&self) -> PathBuf {
        let Ok(config) = self.config.read() else {
            println!("could not get readlock on config!");
            panic!("we have poisiod our locks");
        };
        config.path.clone()
    }

    pub fn get_path_for_file(&self, file_name: String) -> PathBuf {
        let mut path = PathBuf::new();

        path.push(self.get_db_path());
        path.push(file_name);
        path
    }

    pub fn get_file_engine(&self) -> FileEngine {
        self.file_engine.clone()
    }

    pub fn new(config: Config) -> anyhow::Result<NullDB, errors::NullDbReadError> {
        let main_log = match Self::create_next_segment_file(config.path.as_path()) {
            Ok(main_log) => main_log,
            Err(e) => {
                panic!("Could not create new main log file! error: {}", e);
            }
        };
        let encoding = config.encoding.clone();
        let file_engine = FileEngine::new(encoding.as_str());
        let indexes = RwLock::new(generate_indexes(config.path.as_path(), &main_log, file_engine.clone())?);
        Ok(NullDB {
            main_log_mutex: RwLock::new(main_log),
            main_log_file_mutex: RwLock::new(false),
            main_log_memory_mutex: RwLock::new(HashMap::new()),
            log_indexes: indexes,
            config: RwLock::new(config),
            file_engine: FileEngine::new(encoding.as_str()),
            current_raft_index: AtomicU64::new(0),
        })
    }

    fn create_next_segment_file(path: &Path) -> anyhow::Result<PathBuf,io::Error> {
        let time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let mut seg_file = PathBuf::new();
        seg_file.push(path);
        let file_name = format!("{}-{}.{}", 0, time, LOG_SEGMENT_EXT);
        seg_file.push(file_name.clone());
        let _file = File::create(seg_file.clone())?;
        Ok(seg_file)
    }

    // gets name of main log file "right now" does not hold read lock so value maybe be stale
    pub fn get_main_log(&self) -> anyhow::Result<PathBuf> {
        match self.main_log_mutex.read() {
            Ok(main_log) => Ok(main_log.clone()),
            Err(_) => Err(anyhow!("Could not get main log file!")),
        }
    }

    // Deletes a record from the log
    pub fn delete_record(&self, key: String) -> anyhow::Result<(),NullDbReadError> {
        self.write_value_to_log(self.file_engine.new_tombstone_record(key, self.current_raft_index.load(Ordering::Relaxed)))
    }

    pub fn get_latest_record_from_disk(&self) -> Result<Record, errors::NullDbReadError> {
        let Ok(config) = self.config.read() else {
            println!("could not get readlock on config!");
            panic!("we have poisiod our locks");
        };
        // If not in main log, check all the segments
        let mut generation_mapper = utils::get_generations_segment_mapper(
            config.path.as_path(),
            file_compactor::SEGMENT_FILE_EXT.to_owned(),
        )?;

        /*
         * unstable is faster, but could reorder "same" values.
         * We will not have same values as this was from a set.
         */
        let mut gen_vec: Vec<i32> = generation_mapper.generations.into_iter().collect();
        gen_vec.sort_unstable();

        //Umm... I don't know if this is the best way to do this. it's what I did though, help me?
        let mut gen_iter = gen_vec.into_iter();
        let Ok(main_log_filename) = self.main_log_mutex.read() else {
            panic!("we have poisiod our locks... don't do this please");
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

                let then = time::Instant::now();

                while let Some(file_path) = file_name_iter.next_back() {
                    //file names: [gen]-[time].nullsegment
                    let path =
                        self.get_path_for_file(format!("{}-{}", current_gen, file_path.clone()));

                    return get_value_from_segment(path, 0, &self.file_engine);
                }
            }
        }
        Err(errors::NullDbReadError::ValueNotFound)
    }
    pub fn get_value_for_key(
        &self,
        key: String,
    ) -> anyhow::Result<Record, errors::NullDbReadError> {
        // Aquire read lock on main log in memory
        let Ok(main_log) = self.main_log_memory_mutex.read() else {
            println!("Could not get main log file!");
            panic!("we have poisiod our locks");
        };

        // Check the main log first for key
        if let Some(value) = main_log.get(&key) {
            println!(
                "Returned value from main log! {}, {}",
                value.get_id(),
                value.get_value().unwrap()
            );
            return Ok(value.clone());
        }

        let Ok(config) = self.config.read() else {
            println!("could not get readlock on config!");
            panic!("we have poisiod our locks");
        };
        // If not in main log, check all the segments
        let mut generation_mapper = utils::get_generations_segment_mapper(
            config.path.as_path(),
            file_compactor::SEGMENT_FILE_EXT.to_owned(),
        )?;

        /*
         * unstable is faster, but could reorder "same" values.
         * We will not have same values as this was from a set.
         */
        let mut gen_vec: Vec<i32> = generation_mapper.generations.into_iter().collect();
        gen_vec.sort_unstable();

        //Umm... I don't know if this is the best way to do this. it's what I did though, help me?
        let mut gen_iter = gen_vec.into_iter();
        let Ok(main_log_filename) = self.main_log_mutex.read() else {
            panic!("we have poisiod our locks... don't do this please");
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

                let then = time::Instant::now();

                while let Some(file_path) = file_name_iter.next_back() {
                    //file names: [gen]-[time].nullsegment
                    let path =
                        self.get_path_for_file(format!("{}-{}", current_gen, file_path.clone()));

                    // Don't check the main log, we already did that.
                    if path == *main_log_filename {
                        continue;
                    }

                    //Check index for value
                    let Ok(log_index) = self.log_indexes.read() else {
                        panic!("could not optain read log on indexes");
                    };

                    let index = log_index.get(&path);

                    let Some(index) = index else {
                        println!("{:?}", log_index);
                        println!("{:?}", path);
                        panic!("Index not found for log segment");
                    };

                    let Some(line_number) = index.get(&key) else {
                        continue;
                    };

                    println!(
                        "record found, file:{:?}, line_number:{}",
                        path.clone(),
                        line_number
                    );
                    let dur: u128 = ((time::Instant::now() - then).as_millis())
                        .try_into()
                        .unwrap();
                    println!("inner dur: {}", dur);
                    return get_value_from_segment(path, *line_number, &self.file_engine);
                }
            }
        }
        Err(errors::NullDbReadError::ValueNotFound)
    }

    pub fn log(&self, key: String, value: String, index: u64) -> anyhow::Result<(),NullDbReadError> {
        let tmp_index = index + 1;
        let new_record = self.file_engine.new_record(key, tmp_index, None, Some(value));
        self.write_value_to_log(new_record)?;
        Ok(())
    }

    pub fn log_entries(&self, entries: Vec<LogEntry>, index: u64) -> anyhow::Result<(),NullDbReadError> {
        
        let mut tmp_index = index + 1;
        for entry in entries {
            let new_record = self.file_engine.new_record(entry.key, tmp_index, None, Some(entry.value));
            self.write_value_to_log(new_record)?;
            tmp_index += 1;
        }

        Ok(())
    }

    // Writes value to log, will create new log if over 64 lines.
    pub fn write_value_to_log(&self, record: Record) -> anyhow::Result<(),NullDbReadError> {
        let line_count;
        {
            let main_log = self.main_log_mutex.read();
            let Ok(main_log) = main_log else {
                println!("Could not get main log file!");
                return Err(NullDbReadError::FailedToObtainMainLog);
            };
            let file = File::open(main_log.clone()).map_err(|e| {
                println!("Could not open main log file! error: {}", e);
                NullDbReadError::IOError(e)
            })?;
            // make new file if over our 64 lines max
            let f = BufReader::new(file);
            line_count = f.lines().count();
        }

        // Check if main log is "full"
        if line_count > 5120 {
            let main_log = self.main_log_mutex.write();
            let Ok(mut main_log) = main_log else {
                return Err(NullDbReadError::FailedToObtainMainLog);
            };
            let Some(index) = index::generate_index_for_segment(&main_log,self.file_engine.clone()) else {
                panic!("could not create index of main log");
            };
            self.add_index(main_log.clone(), index);

            let Ok(mut main_memory_log) = self.main_log_memory_mutex.write() else {
                println!("Could not get main log file!");
                panic!("we have poisiod our locks");
            };

            // Check the main log first for key
            main_memory_log.clear();
            let Ok(config) = self.config.read() else {
                println!("could not get readlock on config!");
                panic!("we have poisiod our locks");
            };
            *main_log = Self::create_next_segment_file(config.path.as_path()).map_err(|e| {
                println!("Could not create new main log file! error: {}", e);
                NullDbReadError::IOError(e)
            })?;
        }

        // Aquire write lock on main log file
        let Ok(_main_log_disk) = self.main_log_file_mutex.write() else {
            return Err(NullDbReadError::FailedToObtainMainLog);
        };

        // Aquire write lock on main log memory
        let Ok(mut main_log_memory) = self.main_log_memory_mutex.write() else {
            return Err(NullDbReadError::FailedToObtainMainLog);
        };

        // Aquire read lock on main log file name
        let Ok(main_log_name) = self.main_log_mutex.read() else {
            return Err(NullDbReadError::FailedToObtainMainLog);
        };

        // Write to memory
        let old_value = main_log_memory.insert(record.get_id(), record.clone());

        let mut file = OpenOptions::new()
            .write(true)
            .append(true)
            .open(main_log_name.clone()).map_err(|e| {
                println!("Could not open main log file! error: {}", e);
                NullDbReadError::IOError(e)
            })?;


        // TODO: Could write partial record to file then fail. need to try and clean up disk
        let rec = record.serialize();
        let ret = file.write_all(rec.as_slice()); 

        if let Err(e) = ret {
            return file_write_error(&mut main_log_memory, old_value, record, e);
        }

        let ret = file.write_all(b"\n");

        if let Err(e) = ret {
            return file_write_error(&mut main_log_memory, old_value, record, e);
        }

        let ret = file.flush();

        if let Err(e) = ret {
            return file_write_error(&mut main_log_memory, old_value, record, e);
        }

        Ok(())
    }

    pub fn add_index(&self, segment: PathBuf, index: Index) -> Option<Index> {
        let Ok(mut main_index) = self.log_indexes.write() else {
            panic!("could not optain write lock to index");
        };

        main_index.insert(segment, index)
    }

    pub fn remove_index(&self, segment: &PathBuf) -> Option<Index> {
        let Ok(mut main_index) = self.log_indexes.write() else {
            panic!("could not optain write lock to index");
        };

        main_index.remove(segment)
    }
}

fn file_write_error(main_log:&mut RwLockWriteGuard<HashMap<String,Record>>, old_value: Option<Record>, record: Record ,e: io::Error) -> Result<(),NullDbReadError>{
    // TODO: Could write partial record to file then fail. need to try and clean up disk
    println!("Could not open main log file! error: {}", e);
    // If we failed to write to disk, reset the memory to what it was before
    if let Some(old_value) = old_value {
        main_log.insert(record.get_id().clone(), old_value);
    } else {
        main_log.remove(&record.get_id());
    }
    return Err(NullDbReadError::IOError(e));
}

fn get_value_from_segment(
    path: PathBuf,
    line_number: usize,
    file_engine: &FileEngine,
) -> anyhow::Result<Record, errors::NullDbReadError> {
    let file = OpenOptions::new()
        .read(true)
        .write(false)
        .open(path.clone())
        .expect("db pack file doesn't exist.");

    let bb = BufReader::new(file);
    let mut buffer_iter = bb.lines();
    // .nth -> Option<Result<String,Err>>
    let value = buffer_iter.nth(line_number).expect("index missed");

    let Ok(value) = value else {
        panic!("data corrupted");
    };

    get_value_from_database(value, file_engine)
}

pub fn get_value_from_database(
    value: String,
    file_engine: &FileEngine,
) -> anyhow::Result<Record, errors::NullDbReadError> {
    file_engine.get_record_from_str(&value).map_err(|e| {
        println!("Could not parse value from database! error: {}", e);
        errors::NullDbReadError::Corrupted
    })
}

pub fn get_key_from_database_line(
    value: String,
    file_engine: FileEngine,
) -> anyhow::Result<String, errors::NullDbReadError> {
    Ok(file_engine.get_record_from_str(&value)?.get_id())
}

pub fn check_file_for_key(key: String, file: File) -> Result<String, errors::NullDbReadError> {
    let mut reader = EasyReader::new(file).unwrap();
    // Generate index (optional)
    if let Err(e) = reader.build_index() {
        return Err(errors::NullDbReadError::IOError(e));
    }
    reader.eof();
    while let Some(line) = reader.prev_line().unwrap() {
        let split = line.split(":").collect::<Vec<&str>>();
        if split.len() != 2 {
            continue;
        }
        if split[0] == key {
            let val = split[1].to_string().clone();
            if val == TOMBSTONE {
                return Err(errors::NullDbReadError::ValueDeleted);
            }
            return Ok(split[1].to_string().clone());
        }
    }
    return Err(errors::NullDbReadError::ValueNotFound);
}
