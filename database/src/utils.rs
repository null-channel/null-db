use super::errors;
use super::EasyReader;
use std::collections::HashSet;
use std::ffi::OsStr;
use std::fs::File;
use std::path::{Path, PathBuf};

use std::collections::HashMap;

pub fn get_all_files_by_ext(path: &Path, ext: String) -> std::io::Result<Vec<String>> {
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

pub fn get_all_files_in_dir(path: String) -> std::io::Result<Vec<PathBuf>> {
    let paths = std::fs::read_dir(path)?;
    let file_paths = paths
        .into_iter()
        .flat_map(|x| match x {
            Ok(y) => {
                return Some(y.path());
            }
            Err(_) => return None,
        })
        .collect::<Vec<PathBuf>>();
    return Ok(file_paths);
}

pub fn get_extension_from_filename(filename: &str) -> Option<&str> {
    Path::new(filename).extension().and_then(OsStr::to_str)
}

#[derive(Debug)]
pub struct SegmentGenerationMapper {
    pub gen_name_segment_files: HashMap<i32, Vec<String>>,
    pub generations: HashSet<i32>,
}

pub fn get_generations_segment_mapper(
    path: &Path,
    ext: String,
) -> anyhow::Result<SegmentGenerationMapper, errors::NullDbReadError> {
    let segment_files =
        get_all_files_by_ext(path, ext).map_err(|e| errors::NullDbReadError::IOError(e))?;

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
