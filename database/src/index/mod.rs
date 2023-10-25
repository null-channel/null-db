use std::collections::HashMap;
use crate::{errors, utils};

use super::utils::get_generations_segment_mapper;
use std::fs::OpenOptions;
use std::io::{prelude::*, BufReader};

pub type Index = HashMap<String,usize>;

pub fn generate_indexes(main_log: String) -> anyhow::Result<HashMap<String,Index>,errors::NullDbReadError> {
    let mut indexes = HashMap::new();
    let mut generation_mapper = get_generations_segment_mapper(super::utils::LOG_SEGMENT_EXT.to_owned())?;
    /*
     * unstable is faster, but could reorder "same" values.
     * We will not have same values as this was from a set.
     */
    let mut gen_vec: Vec<i32> = generation_mapper.generations.into_iter().collect();
    gen_vec.sort_unstable();

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

                if path.is_empty() {
                    panic!("segment file path was bad");
                }
                // Don't check the main log, we already did that.
                if path == *main_log {
                    continue;
                }

                if let Some(index) = generate_index_for_segment(path.clone()) {
                    indexes.insert(path.clone(), index);
                }
            }
        }
    }
    Ok(indexes)
}

pub fn generate_index_for_segment(segment_path: String) -> Option<Index> {

    let mut index = Index::new();

    let file = OpenOptions::new()
        .read(true)
        .write(false)
        .open(segment_path.clone())
        .expect("db segment file doesn't exist.");
    let reader = BufReader::new(file);

    let mut line_num = 0;

    let lines = reader.lines();

    for line in lines {
        if let Ok(line) = line {
            // A log file with nothing written to it is fine
            // it will get deleted in next compaction
            if line.len() == 0 {
                println!("empty line detected");
                continue;
            }
            if let Ok(parsed_value) = utils::get_key_from_database_line(line) {
                index.insert(parsed_value,line_num);
            } else {
                panic!("failed to parse database line to build index");
            }
        }

        line_num = line_num + 1;
    }
    println!("file: {}, index: {:?}", segment_path.clone(),index);
    Some(index)
}
