use super::errors;
use super::EasyReader;
use anyhow::anyhow;
use std::collections::HashSet;
use std::ffi::OsStr;
use std::fs::File;
use std::path::Path;

use std::collections::HashMap;

pub const TOMBSTONE: &'static str = "~tombstone~";
pub const LOG_SEGMENT_EXT: &'static str = "nullsegment";

pub fn get_all_files_in_dir(path: String, ext: String) -> std::io::Result<Vec<String>> {
    let paths = std::fs::read_dir(path)?;
    let file_paths = paths
        .into_iter()
        .flat_map(|x| {
            match x {
                Ok(y) => {
                    if get_extension_from_filename(y.file_name().to_str()?) == Some(&ext) {
                        return Some(y.file_name().into_string().unwrap());
                    }
                }
                Err(_) => return None,
            }
            return None;
        })
        .collect::<Vec<String>>();
    return Ok(file_paths);
}

pub fn get_main_log_file() -> anyhow::Result<String> {
    let mut generation_mapper = get_generations_segment_mapper("nullsegment".to_owned())?;

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

        if let Some(file_name_vec) = generation_mapper
            .gen_name_segment_files
            .get_mut(&current_gen)
        {
            file_name_vec.sort_unstable();
            let mut file_name_iter = file_name_vec.into_iter();
            while let Some(file_path) = file_name_iter.next_back() {
                return Ok(format!("{}-{}", current_gen, file_path.clone()));
            }
        }
    }

    return Err(anyhow!("no files found!"));
}

pub fn get_extension_from_filename(filename: &str) -> Option<&str> {
    Path::new(filename).extension().and_then(OsStr::to_str)
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

#[derive(Debug)]
pub struct SegmentGenerationMapper {
    pub gen_name_segment_files: HashMap<i32, Vec<String>>,
    pub generations: HashSet<i32>,
}

pub fn create_next_segment_file() -> anyhow::Result<String> {
    let time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let file_name = format!("{}-{}.{}", 0, time, LOG_SEGMENT_EXT);
    let _file = File::create(file_name.clone())?;
    Ok(file_name)
}

pub fn get_generations_segment_mapper(ext: String) -> anyhow::Result<SegmentGenerationMapper> {
    let segment_files = get_all_files_in_dir("./".to_owned(), ext)?;

    let mut generations = SegmentGenerationMapper {
        gen_name_segment_files: HashMap::new(),
        generations: HashSet::new(),
    };

    let mut iter = segment_files.into_iter();
    while let Some(file_path) = iter.next_back() {
        //file names: [gen]-[time].nullsegment
        let path = file_path.clone();
        /*
         * file names look like this:
         * [generation]-[time].nseg
         */
        let file_name_breakdown = path.split("-").collect::<Vec<&str>>();

        if let Ok(gen_val) = file_name_breakdown[0].parse::<i32>() {
            generations.generations.insert(gen_val);
            if let Some(generation) = generations.gen_name_segment_files.get_mut(&gen_val) {
                generation.push(file_name_breakdown[1].to_string());
            } else {
                //This gen does not have a vec yet! Create it!
                let v = vec![file_name_breakdown[1].to_string()];
                generations.gen_name_segment_files.insert(gen_val, v);
            }
        }
    }

    return Ok(generations);
}
