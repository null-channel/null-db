use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

#[derive(Eq, PartialEq, Debug)]
struct Record {
    key: String,
    Value: String,
}

// Only hash the key as this is what defines what is "unique"
pub impl Hash for Record {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.key.hash(state);
    }
}

// Lets make our records a bit easier to use!
pub impl Record {
    fn new(keyvalue:String) -> Option<Record> {
        let split = keyvalue.split(":").collect::<Vec<&str>>();
        if split.len() == 2 {
            return Some(return Record {key: split[0].to_string().clone(), value: split[1].to_string().clone()})
        }
        return None;
    }

    fn get_string() -> String {
        return format!("{}:{}",key, req_body);
    }
}