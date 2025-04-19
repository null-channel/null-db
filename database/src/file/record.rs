use super::proto;
use prost::Message;
use serde::{Deserialize, Serialize};
use std::hash::{Hash, Hasher};

/// HtmlRecord is a struct that represents a record in memory.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct HtmlRecord {
    #[serde(rename = "@id")]
    pub key: String,
    #[serde(rename = "@index")]
    pub index: u64,
    #[serde(rename = "@class")]
    pub class: Option<String>,
    #[serde(rename = "$text")]
    pub value: Option<String>,
}

/// JsonRecord is a struct that represents a record in memory.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct JsonRecord {
    pub key: String,
    pub index: u64,
    pub tombstone: Option<bool>,
    pub value: Option<String>,
}

/// Record is a representation of a record in memory.
#[derive(Debug, Clone)]
pub enum Record {
    Json(JsonRecord),
    Html(HtmlRecord),
    Proto(proto::ProtoRecord),
}

/// Implementing Hash, Eq and PartialEq for Record.
/// This is needed to be able to use Record as a key in a HashSet for compaction.
/// The key is used to uniquely identify the record while not considering the other fields.
impl Hash for Record {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Record::Json(json) => json.key.hash(state),
            Record::Html(html) => html.key.hash(state),
            Record::Proto(proto) => proto.key.hash(state),
        }
    }
}

impl Eq for Record {}

impl PartialEq for Record {
    fn eq(&self, other: &Self) -> bool {
        match self {
            Record::Json(json) => match other {
                Record::Json(other_json) => json.key == other_json.key,
                _ => false,
            },
            Record::Html(html) => match other {
                Record::Html(other_html) => html.key == other_html.key,
                _ => false,
            },
            Record::Proto(proto) => match other {
                Record::Proto(other_proto) => proto.key == other_proto.key,
                _ => false,
            },
        }
    }
}

/// A Record representation in memory of a on disk record.
/// It can be of type Json, Html or Proto.
/// The key is a string that uniquely identifies the record.
/// The index is a u64 that represents the order of the record in the log.
/// The tombstone is an optional boolean that indicates if the record is a tombstone.
/// The value is an optional string that contains the value of the record.
/// The value can be empty for tombstones.
impl Record {
    pub fn serialize(&self) -> Vec<u8> {
        match self {
            Record::Json(json) => serde_json::to_vec(json).unwrap(),
            Record::Html(html) => quick_xml::se::to_string(html).unwrap().into_bytes(),
            Record::Proto(proto) => proto.encode_to_vec(),
        }
    }

    pub fn as_key(&self) -> &str {
        match self {
            Record::Json(json) => &json.key,
            Record::Html(html) => &html.key,
            Record::Proto(proto) => &proto.key,
        }
    }

    /// to_key returns a clone of the recored's key.
    pub fn get_key(&self) -> String {
        match self {
            Record::Json(json) => json.key.clone(),
            Record::Html(html) => html.key.clone(),
            Record::Proto(proto) => proto.key.clone(),
        }
    }

    /// get_tombstone returns the tombstone of the record.
    pub fn get_tombstone(&self) -> Option<bool> {
        match self {
            Record::Json(json) => json.tombstone,
            Record::Proto(proto) => proto.tombstone,
            Record::Html(html) => match &html.class {
                Some(class) if class == "tombstone" => Some(true),
                Some(_) | None => None,
            },
        }
    }

    pub fn as_value(&self) -> Option<&str> {
        match self {
            Record::Json(json) => json.value.as_deref(),
            Record::Html(html) => html.value.as_deref(),
            Record::Proto(proto) => proto.value.as_deref(),
        }
    }

    /// get_value returns a clone of the recored's value.
    pub fn get_value(&self) -> Option<String> {
        match self {
            Record::Json(json) => json.value.clone(),
            Record::Html(html) => html.value.clone(),
            Record::Proto(proto) => proto.value.clone(),
        }
    }
}
