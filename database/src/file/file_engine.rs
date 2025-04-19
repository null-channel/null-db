use std::str::FromStr;

use super::proto;
use super::record::{HtmlRecord, JsonRecord, Record};
use crate::errors::NullDbReadError;
use anyhow::anyhow;
use prost::Message;
use quick_xml::de::from_str;

#[derive(Debug, Clone, Copy)]
pub enum FileEngine {
    Json,
    Html,
    Proto,
}

impl FromStr for FileEngine {
    type Err = anyhow::Error;

    /// new creates a new FileEngine from a string, valid options are json, html and proto.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "json" => Ok(FileEngine::Json),
            "html" => Ok(FileEngine::Html),
            "proto" => Ok(FileEngine::Proto),
            _ => Err(anyhow!("Invalid file engine")),
        }
    }
}

/// FileEngine is an enum that represents the different file engines that can be used to store records.
impl FileEngine {
    /// deserialize a string into a Record.
    ///
    /// # Errors
    ///
    /// This function will return an error if the value is not a valid record.
    pub fn deserialize(&self, value: &str) -> anyhow::Result<Record, NullDbReadError> {
        match self {
            FileEngine::Json => {
                let json: JsonRecord =
                    serde_json::from_str(value).map_err(|_e| NullDbReadError::Corrupted)?;
                Ok(Record::Json(json))
            }
            FileEngine::Html => {
                let html: HtmlRecord = from_str(value).map_err(|_e| NullDbReadError::Corrupted)?;
                Ok(Record::Html(html))
            }
            FileEngine::Proto => {
                let proto: proto::ProtoRecord = proto::ProtoRecord::decode(value.as_bytes())
                    .map_err(|_e| NullDbReadError::Corrupted)?;
                Ok(Record::Proto(proto))
            }
        }
    }

    /// new_record creates a new Record from a key, index, tombstone and value.
    pub fn new_record(
        &self,
        key: String,
        index: u64,
        tombstone: Option<bool>,
        value: Option<String>,
    ) -> Record {
        match self {
            FileEngine::Json => Record::Json(JsonRecord {
                key,
                index,
                tombstone,
                value,
            }),
            FileEngine::Html => Record::Html(HtmlRecord {
                key,
                index,
                class: None,
                value,
            }),
            FileEngine::Proto => Record::Proto(proto::ProtoRecord {
                key,
                index,
                tombstone,
                value,
            }),
        }
    }

    /// new_tombstone_record creates a new tombstone Record from a key and index.
    /// Shortcut for new_record with tombstone set to true and value set to None.
    pub fn new_tombstone_record(&self, key: String, index: u64) -> Record {
        match self {
            FileEngine::Json => Record::Json(JsonRecord {
                key,
                index,
                tombstone: Some(true),
                value: None,
            }),
            FileEngine::Html => Record::Html(HtmlRecord {
                key,
                index,
                class: Some("tombstone".to_string()),
                value: None,
            }),
            FileEngine::Proto => Record::Proto(proto::ProtoRecord {
                key,
                index,
                tombstone: Some(true),
                value: None,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{env, fs::OpenOptions, io::Write, path::Path};
    use tempfile::TempDir;

    use super::FileEngine;

    #[test]
    fn write_100000_json_records() {
        if let Ok(path) = env::var("CARGO_MANIFEST_DIR") {
            let dir = Path::new(&path).join("test_data");
            put_lots_of_data(100_000, FileEngine::Json, &dir, "json.records".to_string());
        }
    }

    #[test]
    fn write_100000_html_records() {
        if let Ok(path) = env::var("CARGO_MANIFEST_DIR") {
            let dir = Path::new(&path).join("test_data");
            put_lots_of_data(100_000, FileEngine::Html, &dir, "html.records".to_string());
        }
    }

    #[test]
    fn write_100000_proto_records() {
        if let Ok(path) = env::var("CARGO_MANIFEST_DIR") {
            let dir = Path::new(&path).join("test_data");
            put_lots_of_data(
                100_000,
                FileEngine::Proto,
                &dir,
                "proto.records".to_string(),
            );
        }
    }

    #[bench]
    fn bench_insert_1000_json(b: &mut test::Bencher) {
        // Create a directory inside of `std::env::temp_dir()`
        let tmp_dir = TempDir::new().expect("could not get temp dir");
        b.iter(|| put_lots_of_temp_data(1000, FileEngine::Json, &tmp_dir, "test.db".to_string()));
    }

    #[bench]
    fn bench_insert_1000_html(b: &mut test::Bencher) {
        // Create a directory inside of `std::env::temp_dir()`
        let tmp_dir = TempDir::new().expect("could not get temp dir");
        b.iter(|| put_lots_of_temp_data(1000, FileEngine::Html, &tmp_dir, "test.db".to_string()));
    }

    #[bench]
    fn bench_insert_1000_proto(b: &mut test::Bencher) {
        // Create a directory inside of `std::env::temp_dir()`
        let tmp_dir = TempDir::new().expect("could not get temp dir");
        b.iter(|| put_lots_of_temp_data(1000, FileEngine::Proto, &tmp_dir, "test.db".to_string()));
    }

    fn put_lots_of_temp_data(n: u64, engine: FileEngine, tmp_dir: &TempDir, file_path: String) {
        let file_path = tmp_dir.path().join(file_path);
        for i in 0..n {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            //write to file
            let record = engine.new_record(key, i, None, Some(value));
            let record_str = record.serialize();

            let mut file = OpenOptions::new()
                .write(true)
                .append(true)
                .create(true)
                .open(&file_path)
                .unwrap();
            file.write_all(&record_str).unwrap();
        }
    }

    fn put_lots_of_data(n: u64, engine: FileEngine, dir: &Path, file_path: String) {
        let file_path = dir.join(file_path);
        for i in 0..n {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            //write to file
            let record = engine.new_record(key, i, None, Some(value));
            let record_str = record.serialize();

            let mut file = OpenOptions::new()
                .write(true)
                .append(true)
                .create(true)
                .open(&file_path)
                .unwrap();
            file.write_all(&record_str).unwrap();
        }
    }
}
