use quick_xml::de::from_str;
use serde::{Deserialize, Serialize};

pub mod proto {
    tonic::include_proto!("raft");
}
use prost::Message;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct HtmlRecord {
    #[serde(rename = "@id")]
    key: String,
    #[serde(rename = "@index")]
    index: u64,
    #[serde(rename = "@class")]
    class: Option<String>,
    #[serde(rename = "$text")]
    value: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct JsonRecord {
    key: String,
    index: u64,
    tombstone: Option<bool>,
    value: Option<String>,
}

#[derive(Debug, Clone)]
pub enum Record {
    Json(JsonRecord),
    Html(HtmlRecord),
    Proto(proto::ProtoRecord),
}

impl Record {
    pub fn serialize(&self) -> String {
        match self {
            Record::Json(json) => serde_json::to_string(json).unwrap(),
            Record::Html(html) => quick_xml::se::to_string(html).unwrap(),
            Record::Proto(proto) => {
                let mut buf = Vec::new();
                proto.encode(&mut buf).unwrap();
                String::from_utf8(buf).unwrap()
            }
        }
    }

    pub fn get_id(&self) -> String {
        match self {
            Record::Json(json) => json.key.clone(),
            Record::Html(html) => html.key.clone(),
            Record::Proto(proto) => proto.key.clone(),
        }
    }

    pub fn get_index(&self) -> u64 {
        match self {
            Record::Json(json) => json.index,
            Record::Html(html) => html.index,
            Record::Proto(proto) => proto.index,
        }
    }

    pub fn get_tombstone(&self) -> Option<bool> {
        match self {
            Record::Json(json) => json.tombstone,
            Record::Proto(proto) => proto.tombstone,
            Record::Html(html) => match &html.class {
                Some(class) => {
                    if class == "tombstone" {
                        Some(true)
                    } else {
                        None
                    }
                }
                None => None,
            },
        }
    }

    pub fn get_value(&self) -> Option<String> {
        match self {
            Record::Json(json) => json.value.clone(),
            Record::Html(html) => html.value.clone(),
            Record::Proto(proto) => proto.value.clone(),
        }
    }
}

pub enum FileEngine {
    Json,
    Html,
    Proto,
}

impl FileEngine {
    pub fn new(engine: &str) -> Self {
        match engine {
            "json" => FileEngine::Json,
            "html" => FileEngine::Html,
            "proto" => FileEngine::Proto,
            _ => panic!("Invalid file engine"),
        }
    }

    pub fn get_record_from_str(&self, value: &str) -> anyhow::Result<Box<Record>> {
        match self {
            FileEngine::Json => {
                let json: JsonRecord = serde_json::from_str(value)?;
                Ok(Box::new(Record::Json(json)))
            }
            FileEngine::Html => {
                let html: HtmlRecord = from_str(value)?;
                Ok(Box::new(Record::Html(html)))
            }
            FileEngine::Proto => {
                let proto: proto::ProtoRecord = proto::ProtoRecord::decode(value.as_bytes())?;
                Ok(Box::new(Record::Proto(proto)))
            }
        }
    }

    pub fn serialize(&self, record: Record) -> String {
        record.serialize()
    }

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
