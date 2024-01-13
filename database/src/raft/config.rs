pub mod raft {
    tonic::include_proto!("raft");
}

#[derive(Default, Clone)]
pub struct RaftConfig {
    pub roster: Vec<String>,
    pub candidate_id: String,
}

impl RaftConfig {
    pub fn new(candidate_id: String, roster: Vec<&str>) -> Self {
        Self {
            roster: roster.iter().map(|x| x.to_string()).collect(),
            candidate_id,
        }
    }
}
