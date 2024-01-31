use serde::{Deserialize, Serialize};
use quick_xml::de::from_str;

#[derive(Debug, Deserialize, Serialize)]
pub struct HtmlRecord {
    #[serde(rename = "@id")]
    id: String,
    #[serde(rename = "@index")]
    index: u64,
    #[serde(rename = "@class")]
    class: Option<String>,
    #[serde(rename = "$text")]
    value: Option<String>,
}

impl RecordTrait for HtmlRecord {
    fn id(&self) -> String {
        self.id.clone()
    }

    fn index(&self) -> u64 {
        self.index
    }

    fn tombstone(&self) -> Option<bool> {
        self.class.as_ref().map(|x| x == "tombstone")
    }

    fn value(&self) -> Option<String> {
        self.value.clone()
    }

    fn serialize(&self) -> String {
        quick_xml::se::to_string(&self).unwrap()
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct JsonRecord {
    id: String,
    index: u64,
    tombstone: Option<bool>,
    value: Option<String>,
}

impl RecordTrait for JsonRecord {
    fn id(&self) -> String {
        self.id.clone()
    }

    fn index(&self) -> u64 {
        self.index
    }

    fn tombstone(&self) -> Option<bool> {
        self.tombstone
    }

    fn value(&self) -> Option<String> {
        self.value.clone()
    }

    fn serialize(&self) -> String {
        serde_json::to_string(&self).unwrap()
    }
}

pub trait RecordTrait {
    fn id(&self) -> String;
    fn index(&self) -> u64;
    fn tombstone(&self) -> Option<bool>;
    fn value(&self) -> Option<String>;
    fn serialize(&self) -> String;
}

pub enum FileEngine {
    Json,
    Html,
}

impl FileEngine {
    pub fn new(engine: &str) -> Self {
        match engine {
            "json" => FileEngine::Json,
            "html" => FileEngine::Html,
            _ => panic!("Invalid file engine"),
        }
    }

    pub fn get_record_from_str(&self, value: &str) -> anyhow::Result<Box<dyn RecordTrait>> {
        match self {
            FileEngine::Json => {
                let json : JsonRecord = serde_json::from_str(value)?;
                Ok(Box::new(json))
            }
            FileEngine::Html => {
                let html : HtmlRecord = from_str(value)?;
                Ok(Box::new(html))
            }
        }
    }

    pub fn record_to_string(&self, record: &dyn RecordTrait) -> String {
        record.serialize()
    }

    pub fn new_record(&self, id: String, index: u64, tombstone: Option<bool>, value: Option<String>) -> Box<dyn RecordTrait> {
        match self {
            FileEngine::Json => {
                Box::new(JsonRecord {
                    id,
                    index,
                    tombstone,
                    value,
                })
            }
            FileEngine::Html => {
                Box::new(HtmlRecord {
                    id,
                    index,
                    class: tombstone.map(|x| if x { "tombstone".to_string() } else { "".to_string() }),
                    value,
                })
            }
        }
    }

    pub fn new_tombstone_record(&self, id: String, index: u64) -> Box<dyn RecordTrait> {
        match self {
            FileEngine::Json => {
                Box::new(JsonRecord {
                    id,
                    index,
                    tombstone: Some(true),
                    value: None,
                })
            }
            FileEngine::Html => {
                Box::new(HtmlRecord {
                    id,
                    index,
                    class: Some("tombstone".to_string()),
                    value: None,
                })
            }
        }
    }
}
