use std::time::{Duration, Instant};
use log::info;

use actix_web::web::Data;

use crate::{raft::raft, nulldb::NullDB, errors::NullDbReadError};

use super::{config::RaftConfig, grpcserver::RaftEvent, RaftClients, State};

pub struct LeaderState {
    pub term: u32,
    pub log_index: u64,
    pub last_heartbeat: Instant,
}

impl LeaderState {
    pub fn new(last_heartbeat: Instant, term: u32) -> LeaderState {
        LeaderState {
            term,
            log_index: 0,
            last_heartbeat,
        }
    }

    pub async fn tick(&mut self, config: &RaftConfig, clients: &mut RaftClients) -> Option<State> {
        if self.last_heartbeat.elapsed() > Duration::from_millis(100) {
            info!("Sending heartbeat");
            self.last_heartbeat = Instant::now();
            for nodes in clients.values_mut() {
                let mut node = nodes.clone();
                let request = tonic::Request::new(raft::AppendEntriesRequest {
                    term: self.term,
                    leader_id: config.candidate_id.clone(),
                    prev_log_index: self.log_index,
                    prev_log_term: self.term,
                    entries: vec![],
                    leader_commit: 0,
                });
                let response = node.append_entries(request).await.unwrap();
                if !response.get_ref().success {
                    info!(
                        "Becoming Follower. Failed to send heartbeat. +++++++!!!!!!!!!+++++++"
                    );
                    return None;
                }
            }
        }
        None
    }

    pub async fn on_message(
        &mut self,
        message: RaftEvent,
        config: &RaftConfig,
        clients: &mut RaftClients,
        log: Data<NullDB>,
    ) -> Option<State> {
        match message {
            RaftEvent::VoteRequest(request, sender) => {
                info!("Got a vote request: {:?}", request);
                if request.term > self.term {
                    self.term = request.term;
                    let reply = raft::VoteReply {
                        term: self.term,
                        vote_granted: true,
                    };
                    sender.send(reply).unwrap();
                    return Some(State::Follower(crate::raft::follower::FollowerState::new(
                        Instant::now(),
                        self.term,
                    )));
                }
                let reply = raft::VoteReply {
                    term: self.term,
                    vote_granted: false,
                };
                sender.send(reply).unwrap();
            }
            RaftEvent::AppendEntriesRequest(request, sender) => {
                println!("Got an append entries request: {:?}", request);
                let reply = raft::AppendEntriesReply {
                    term: self.term,
                    success: true,
                };
                sender.send(reply).unwrap();
                println!("Becoming Follower again. Failed to become leader because a leader already exists. +++++++!!!!!!!!!+++++++");
            }
            RaftEvent::NewEntry { key, value, sender } => {
                println!("Got a new entry: {}:{}", key, value);
                //log entry

                let res = log.log(key.clone(), value.clone(), self.log_index);

                if let Err(err) = res {
                    sender.send(Err(err)).unwrap();
                    return None;
                }

                let mut success = 1;
                // Send append entries to all other nodes
                for nodes in clients.values_mut() {
                    let mut node = nodes.clone();
                    let request = tonic::Request::new(raft::AppendEntriesRequest {
                        term: self.term,
                        leader_id: config.candidate_id.clone(),
                        prev_log_index: self.log_index,
                        prev_log_term: self.term,
                        entries: vec![raft::LogEntry {
                            key: key.clone(),
                            value: value.clone(),
                        }],
                        leader_commit: 0,
                    });
                    let response = node.append_entries(request).await.unwrap();

                    if response.get_ref().success {
                        success += 1;
                    }
                }

                if success > config.roster.len() / 2 {
                    sender.send(Ok(())).unwrap();
                } else {
                    sender.send(Err(NullDbReadError::FailedToReplicate)).unwrap();
                }
            }
            RaftEvent::GetEntry(key, sender) => {
                println!("Got a get entry request: {:?}", key);
                if let Ok(entry) = log.get_value_for_key(key) {
                    sender.send(Ok(entry.clone())).unwrap();
                } else {
                    sender.send(Err(NullDbReadError::ValueNotFound)).unwrap();
                }
            }
        }
        None
    }
}
