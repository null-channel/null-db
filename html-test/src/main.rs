use quick_xml::events::{Event, BytesEnd, BytesStart};
use quick_xml::reader::Reader;
use quick_xml::writer::Writer;
use quick_xml::se::to_string;
use std::io::Cursor;
use serde::{Deserialize, Serialize};

/*
<record
  id="key"
  index="0"
  class="tombstone">
  value
</record>
*/

#[derive(Debug, Deserialize, Serialize)]
struct Record {
    #[serde(rename = "@id")]
    id: String,
    #[serde(rename = "@index")]
    index: u64,
    #[serde(rename = "@class")]
    class: Option<String>,
    #[serde(rename = "$text")]
    value: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct JsonRecord {
    id: String,
    index: u64,
    tombstone: Option<bool>,
    value: Option<String>,
}

fn main() {

    let record = Record {
        id: "key".to_string(),
        value: "value".to_string(),
        index: 0,
        class: Some("tombstone".to_string()),
    };

    let sr = to_string(&record).unwrap();

    println!("sr: {}", sr);
    let record = JsonRecord {
        id: "key".to_string(),
        value: Some("value".to_string()),
        index: 0,
        tombstone: Some(true),
    };

    let thing = serde_json::to_string(&record).unwrap();

    println!("sr: {}", thing);
}
