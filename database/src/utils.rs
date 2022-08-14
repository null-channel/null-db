use std::ffi::OsStr;
use std::path::Path;
use std::error::Error;

pub fn get_all_files_in_dir(path: String, ext: String) -> Result<Vec<String>,Box<Error>> {
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