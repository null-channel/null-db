use quick_xml::events::{Event, BytesEnd, BytesStart};
use quick_xml::reader::Reader;
use quick_xml::writer::Writer;
use quick_xml::se::to_string;
use std::io::Cursor;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
struct DBRecord {
    #[serde(rename = "div")]
    key: Key,
    #[serde(rename = "div")]
    value: Value,
    index: u64,
    tombstone: bool,
}

#[derive(Debug, Deserialize, Serialize)]
struct Index {
    label: String,
    index: u64,
}

#[derive(Debug, Deserialize, Serialize)]
struct Key {
    #[serde(rename = "@key")]
    label: String,
    input: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct Value {
    #[serde(rename = "@value")]
    label: String,
    input: String,
}

fn main() {

    let record = DBRecord {
        key: Key {
            // &str -> String
            label: "key".to_string(),
            input: "Actual Key".to_string(),
        },
        value: Value {
            label: "value".to_string(),
            input: "Actual Value".to_string(),
        },
        index: 0,
        tombstone: false,
    };

    let sr = to_string(&record).unwrap();

    println!("sr: {}", sr);

}
