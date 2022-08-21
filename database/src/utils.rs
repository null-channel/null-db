use std::ffi::OsStr;
use std::fs::File;
use std::path::Path;
use super::EasyReader;
use super::errors;

pub const TOMBSTONE: &'static str = "~tombstone~";
pub const LOG_SEGMENT_EXT: &'static str = "nullsegment";

pub fn get_all_files_in_dir(path: String, ext: String) -> std::io::Result<Vec<String>> {
    let paths = std::fs::read_dir(path)?;
    let file_paths = paths.into_iter().flat_map(|x| {
        match x {
            Ok(y) => {
                if get_extension_from_filename(y.file_name().to_str()?) == Some(&ext) {
                    return Some(y.file_name().into_string().unwrap());
                }
            }
            Err(_) => return None
        }
        return None;
    }).collect::<Vec<String>>();
    return Ok(file_paths);
}

pub fn get_extension_from_filename(filename: &str) -> Option<&str> {
    Path::new(filename)
        .extension()
        .and_then(OsStr::to_str)
}

pub fn check_file_for_key(key: String, file: File) -> Result<String, errors::NullDbReadError> {
    let mut reader = EasyReader::new(file).unwrap();
    // Generate index (optional)
    reader.build_index();
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