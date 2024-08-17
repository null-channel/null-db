use super::utils::get_generations_segment_mapper;
use crate::file::file_engine::FileEngine;
use crate::{errors, nulldb};
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{prelude::*, BufReader};
use std::path::{Path, PathBuf};

// TODO: update to be <String, (usize, usize)> to support byte read range queries
pub type Index = HashMap<String, usize>;

pub fn generate_indexes(
    path: &Path,
    main_log: &Path,
    file_engine: FileEngine,
) -> Result<HashMap<PathBuf, Index>, errors::NullDbReadError> {
    let mut indexes = HashMap::new();
    let mut generation_mapper =
        get_generations_segment_mapper(path, super::nulldb::LOG_SEGMENT_EXT)?;
    /*
     * unstable is faster, but could reorder "same" values.
     * We will not have same values as this was from a set.
     */
    let mut gen_vec: Vec<i32> = generation_mapper.generations.into_iter().collect();
    gen_vec.sort_unstable();

    for current_gen in gen_vec {
        println!("Gen {current_gen} in progress");
        /*
         * Power of rust, we KNOW that this is safe because we just built it...
         * but it's better to check anyhow... sometimes annoying but.
         */
        if let Some(file_name_vec) = generation_mapper
            .gen_name_segment_files
            .get_mut(&current_gen)
        {
            file_name_vec.sort_unstable();
            for file_path in file_name_vec.iter().rev() {
                //file names: [gen]-[time].nullsegment
                let path_pt1 = format!("{current_gen}-{file_path}");
                let buff_path = path.join(path_pt1);

                // Don't check the main log, we already did that.
                if buff_path == main_log {
                    continue;
                }

                if let Some(index) = generate_index_for_segment(&buff_path, file_engine) {
                    indexes.insert(buff_path, index);
                }
            }
        }
    }
    Ok(indexes)
}

pub fn generate_index_for_segment(segment_path: &Path, file_engine: FileEngine) -> Option<Index> {
    let mut index = Index::new();

    println!("File path for generate_index_for_segment: {segment_path:?}");
    let file = OpenOptions::new()
        .read(true)
        .write(false)
        .open(segment_path)
        .expect("db segment file doesn't exist.");
    let reader = BufReader::new(file);

    let mut line_num = 0;
    for line in reader.lines() {
        if let Ok(line) = line {
            // A log file with nothing written to it is fine
            // it will get deleted in next compaction
            if line.is_empty() {
                println!("empty line detected");
                continue;
            }
            if let Ok(parsed_value) = nulldb::get_key_from_database_line(&line, file_engine) {
                index.insert(parsed_value, line_num);
            } else {
                panic!("failed to parse database line to build index");
            }
        }

        line_num += 1;
    }
    //println!("file: {:?}, index: {:?}", segment_path.clone(),index);
    Some(index)
}
