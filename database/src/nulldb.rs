use crate::errors::NullDbReadError;
use crate::file::{file_engine::FileEngine, record::Record};
use crate::index;
use crate::index::*;
use crate::raft::raft::LogEntry;
use crate::{errors, file_compactor, utils};
use actix_web::web::Data;
use anyhow::anyhow;
use log::info;
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::BufRead;
use std::io::{self, prelude::*};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
use std::sync::{mpsc, RwLockWriteGuard};
use std::time;
use std::{fs::File, io::BufReader};

pub const LOG_SEGMENT_EXT: &str = "nullsegment";

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
    let compaction = config.compaction;
    let null_db = NullDB::new(config);

    let Ok(null_db) = null_db else {
        panic!("Could not create indexes!!!");
    };

    let db_arc = Data::new(null_db);
    if compaction {
        let (_, rx) = mpsc::channel();
        let _file_compactor_thread = file_compactor::start_compaction(rx, db_arc.clone());
    }
    Ok(db_arc)
}

pub trait DatabaseLog {
    fn get_db_path(&self) -> PathBuf;
    fn get_path_for_file(&self, file_name: String) -> PathBuf;
    fn get_file_engine(&self) -> FileEngine;
    fn get_main_log(&self) -> anyhow::Result<PathBuf>;
    fn delete_record(&self, key: String) -> Result<(), NullDbReadError>;
    fn get_value_for_key(&self, key: &str) -> Result<Record, errors::NullDbReadError>;
    fn log(&self, key: String, value: String, index: u64) -> anyhow::Result<(), NullDbReadError>;
    fn log_entries(
        &self,
        entries: Vec<LogEntry>,
        index: u64,
    ) -> anyhow::Result<(), NullDbReadError>;
    fn write_value_to_log(&self, record: Record) -> Result<(), NullDbReadError>;
    fn add_index(&self, segment: PathBuf, index: Index) -> Option<Index>;
    fn remove_index(&self, segment: &Path) -> Option<Index>;
    fn get_current_raft_index(&self) -> u64;
}

impl DatabaseLog for NullDB {
    fn get_current_raft_index(&self) -> u64 {
        self.current_raft_index.load(Ordering::Relaxed)
    }

    fn get_db_path(&self) -> PathBuf {
        let Ok(config) = self.config.read() else {
            info!("could not get readlock on config!");
            panic!("we have poisiod our locks");
        };
        config.path.clone()
    }

    fn get_path_for_file(&self, file_name: String) -> PathBuf {
        self.get_db_path().join(file_name)
    }

    fn get_file_engine(&self) -> FileEngine {
        self.file_engine
    }

    // gets name of main log file "right now" does not hold read lock so value maybe be stale
    fn get_main_log(&self) -> anyhow::Result<PathBuf> {
        let Ok(main_log) = self.main_log_mutex.read() else {
            return Err(anyhow!("Could not get main log file!"));
        };

        Ok(main_log.clone())
    }

    // Deletes a record from the log
    fn delete_record(&self, key: String) -> Result<(), NullDbReadError> {
        self.write_value_to_log(
            self.file_engine
                .new_tombstone_record(key, self.current_raft_index.load(Ordering::Relaxed)),
        )
    }

    fn get_value_for_key(&self, key: &str) -> Result<Record, errors::NullDbReadError> {
        // Aquire read lock on main log in memory
        let Ok(main_log) = self.main_log_memory_mutex.read() else {
            info!("Could not get main log file!");
            panic!("we have poisiod our locks");
        };

        // Check the main log first for key
        if let Some(value) = main_log.get(key) {
            info!(
                "Returned value from main info! {}, {}",
                value.as_key(),
                value.as_value().unwrap()
            );
            return Ok(value.clone());
        }

        let Ok(config) = self.config.read() else {
            info!("could not get readlock on config!");
            panic!("we have poisiod our locks");
        };
        // If not in main log, check all the segments
        let mut generation_mapper =
            utils::get_generations_segment_mapper(&config.path, file_compactor::SEGMENT_FILE_EXT)?;

        /*
         * unstable is faster, but could reorder "same" values.
         * We will not have same values as this was from a set.
         */
        let mut gen_vec: Vec<i32> = generation_mapper.generations.into_iter().collect();
        gen_vec.sort_unstable();

        //Umm... I don't know if this is the best way to do this. it's what I did though, help me?ms
        let Ok(main_log_filename) = self.main_log_mutex.read() else {
            panic!("we have poisiod our locks... don't do this please");
        };

        for current_gen in gen_vec {
            info!("Gen {current_gen} in progress");
            /*
             * Power of rust, we KNOW that this is safe because we just built it...
             * but it's better to check anyhow... sometimes annoying but.
             */
            if let Some(file_name_vec) = generation_mapper
                .gen_name_segment_files
                .get_mut(&current_gen)
            {
                file_name_vec.sort_unstable();

                let then = time::Instant::now();

                for file_path in file_name_vec.iter_mut().rev() {
                    //file names: [gen]-[time].nullsegment
                    let path = self.get_path_for_file(format!("{current_gen}-{file_path}"));

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
                        info!("{log_index:?}");
                        info!("{path:?}");
                        panic!("Index not found for log segment");
                    };

                    let Some(line_number) = index.get(key) else {
                        continue;
                    };

                    info!("record found, file:{path:?}, line_number:{line_number}");
                    let dur: u128 = (time::Instant::now() - then).as_millis();
                    info!("inner dur: {dur}");
                    return get_value_from_segment(&path, *line_number, self.file_engine);
                }
            }
        }
        Err(errors::NullDbReadError::ValueNotFound)
    }

    fn log(&self, key: String, value: String, index: u64) -> anyhow::Result<(), NullDbReadError> {
        let tmp_index = index + 1;
        let new_record = self
            .file_engine
            .new_record(key, tmp_index, None, Some(value));
        self.write_value_to_log(new_record)?;
        Ok(())
    }

    fn log_entries(
        &self,
        entries: Vec<LogEntry>,
        index: u64,
    ) -> anyhow::Result<(), NullDbReadError> {
        let mut tmp_index = index + 1;
        for entry in entries {
            let new_record =
                self.file_engine
                    .new_record(entry.key, tmp_index, None, Some(entry.value));
            self.write_value_to_log(new_record)?;
            tmp_index += 1;
        }

        Ok(())
    }

    // Writes value to log, will create new log if over 64 lines.
    fn write_value_to_log(&self, record: Record) -> Result<(), NullDbReadError> {
        let line_count = {
            let main_log = self.main_log_mutex.read();
            let Ok(main_log) = main_log else {
                info!("Could not get main log file!");
                return Err(NullDbReadError::FailedToObtainMainLog);
            };
            let file = File::open(&*main_log).map_err(|e| {
                info!("Could not open main log file! error: {e}");
                NullDbReadError::IOError(e)
            })?;
            // make new file if over our 64 lines max
            let f = BufReader::new(file);
            f.lines().count()
        };

        // Check if main log is "full"
        if line_count > 5120 {
            let main_log = self.main_log_mutex.write();
            let Ok(mut main_log) = main_log else {
                return Err(NullDbReadError::FailedToObtainMainLog);
            };
            let Some(index) = index::generate_index_for_segment(&main_log, self.file_engine) else {
                panic!("could not create index of main log");
            };
            self.add_index(main_log.clone(), index);

            let Ok(mut main_memory_log) = self.main_log_memory_mutex.write() else {
                info!("Could not get main log file!");
                panic!("we have poisiod our locks");
            };

            // Check the main log first for key
            main_memory_log.clear();
            let Ok(config) = self.config.read() else {
                info!("could not get readlock on config!");
                panic!("we have poisiod our locks");
            };
            *main_log = Self::create_next_segment_file(config.path.clone()).map_err(|e| {
                info!("Could not create new main log file! error: {e}");
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
        let old_value = main_log_memory.insert(record.get_key(), record.clone());

        let mut file = OpenOptions::new()
            .append(true)
            .open(&*main_log_name)
            .map_err(|e| {
                info!("Could not open main log file! error: {e}");
                NullDbReadError::IOError(e)
            })?;

        // TODO: Could write partial record to file then fail. need to try and clean up disk
        let rec = record.serialize();

        if let Err(e) = file.write_all(&rec) {
            return file_write_error(&mut main_log_memory, old_value, record, e);
        }

        if let Err(e) = file.write_all(b"\n") {
            return file_write_error(&mut main_log_memory, old_value, record, e);
        }

        if let Err(e) = file.flush() {
            return file_write_error(&mut main_log_memory, old_value, record, e);
        }

        Ok(())
    }

    fn add_index(&self, segment: PathBuf, index: Index) -> Option<Index> {
        let Ok(mut main_index) = self.log_indexes.write() else {
            panic!("could not optain write lock to index");
        };

        main_index.insert(segment, index)
    }

    fn remove_index(&self, segment: &Path) -> Option<Index> {
        let Ok(mut main_index) = self.log_indexes.write() else {
            panic!("could not optain write lock to index");
        };

        main_index.remove(segment)
    }
}

impl NullDB {
    pub fn new(config: Config) -> Result<NullDB, errors::NullDbReadError> {
        let main_log = match Self::create_next_segment_file(config.path.to_owned()) {
            Ok(main_log) => main_log,
            Err(e) => {
                panic!("Could not create new main log file! error: {e}");
            }
        };
        let Ok(file_engine) = FileEngine::from_str(&config.encoding) else {
            panic!(
                "Could not create file engine from encoding: {}",
                config.encoding
            );
        };

        let indexes = RwLock::new(generate_indexes(&config.path, &main_log, file_engine)?);
        Ok(NullDB {
            main_log_mutex: RwLock::new(main_log),
            main_log_file_mutex: RwLock::new(false),
            main_log_memory_mutex: RwLock::new(HashMap::new()),
            log_indexes: indexes,
            file_engine,
            config: RwLock::new(config),
            current_raft_index: AtomicU64::new(0),
        })
    }

    fn create_next_segment_file(path: PathBuf) -> Result<PathBuf, io::Error> {
        let time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("now is before epoch")
            .as_millis();
        let seg_file = path.join(format!("0-{time}.{LOG_SEGMENT_EXT}"));
        let _file = File::create(&seg_file)?;
        Ok(seg_file)
    }
}

fn file_write_error(
    main_log: &mut RwLockWriteGuard<HashMap<String, Record>>,
    old_value: Option<Record>,
    record: Record,
    e: io::Error,
) -> Result<(), NullDbReadError> {
    // TODO: Could write partial record to file then fail. need to try and clean up disk
    info!("Could not open main log file! error: {e}");
    // If we failed to write to disk, reset the memory to what it was before
    if let Some(old_value) = old_value {
        main_log.insert(record.get_key(), old_value);
    } else {
        main_log.remove(record.as_key());
    }
    Err(NullDbReadError::IOError(e))
}

fn get_value_from_segment(
    path: &Path,
    line_number: usize,
    file_engine: FileEngine,
) -> Result<Record, errors::NullDbReadError> {
    let file = OpenOptions::new()
        .read(true)
        .write(false)
        .open(path)
        .expect("db pack file doesn't exist.");

    let bb = BufReader::new(file);
    // .nth -> Option<Result<String,Err>>
    let value = bb.lines().nth(line_number).expect("index missed");

    let Ok(value) = value else {
        panic!("data corrupted");
    };

    get_value_from_database(&value, file_engine)
}

pub fn get_value_from_database(
    value: &str,
    file_engine: FileEngine,
) -> Result<Record, errors::NullDbReadError> {
    file_engine.deserialize(value).map_err(|e| {
        info!("Could not parse value from database! error: {e}");
        errors::NullDbReadError::Corrupted
    })
}

pub fn get_key_from_database_line(
    value: &str,
    file_engine: FileEngine,
) -> Result<String, errors::NullDbReadError> {
    Ok(file_engine.deserialize(value)?.get_key())
}
