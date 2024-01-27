use serde::{Deserialize, Serialize};


#[derive(Debug, Clone, Serialize, Deserialize)]
struct Record {
    raft: Raft,
    metadata: Metadata,
    key: Key,
    value: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Raft {
    index: u64,
}

impl Raft {
    fn new(index: u64) -> Self {
        Raft { index }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Metadata {
    version: u8,
    tombstone: bool,
}

impl Metadata {
    fn new(version: u8, tombstone: bool) -> Self {
        Metadata { version, tombstone }
    }
}

type Key = String;
type Value = String;

