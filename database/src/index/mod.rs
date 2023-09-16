use std::collections::HashMap;
use crate::errors;

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

                let mut index = Index::new();
                // Don't check the main log, we already did that.
                if path == *main_log {
                    continue;
                }

                let file = OpenOptions::new()
                    .read(true)
                    .write(false)
                    .open(path.clone())
                    .expect("db pack file doesn't exist.");
                let reader = BufReader::new(file);

                let mut line_num = 0;
                for line in reader.lines() {
                    
                    if let Ok(line) = line {
                       index.insert(line,line_num);
                    }

                    line_num = line_num + 1;
                }
                indexes.insert(path, index);
            }
        }
    }
    Ok(indexes)
}
