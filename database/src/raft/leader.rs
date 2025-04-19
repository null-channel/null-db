use super::{config::RaftConfig, grpcserver::RaftEvent, RaftClients, State};
use crate::{errors::NullDbReadError, nulldb::DatabaseLog, raft::raft};
use actix_web::web::Data;
use log::info;
use std::time::{Duration, Instant};

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

    pub async fn tick(&mut self, config: &RaftConfig, clients: RaftClients) -> Option<State> {
        if self.last_heartbeat.elapsed() > Duration::from_millis(100) {
            info!("Sending heartbeat");
            self.last_heartbeat = Instant::now();
            let clients_clone = { clients.lock().unwrap().clone() };
            for mut nodes in clients_clone.into_values() {
                // Send an empty append entries to all other nodes
                // This is a heartbeat, so we don't need to send any entries
                // If the other nodes don't get this they will change state.
                let request = tonic::Request::new(raft::AppendEntriesRequest {
                    term: self.term,
                    leader_id: config.candidate_id.clone(),
                    prev_log_index: self.log_index,
                    prev_log_term: self.term,
                    entries: vec![],
                    leader_commit: 0,
                });
                let response = nodes.append_entries(request).await.unwrap();
                if !response.get_ref().success {
                    //TODO: What to do if a node fails to respond to a heartbeat
                    // We could check if we can even reach quorum.
                    return None;
                }
            }
        }
        None
    }

    pub async fn on_message<T: DatabaseLog>(
        &mut self,
        message: RaftEvent,
        config: &RaftConfig,
        clients: RaftClients,
        log: Data<T>,
    ) -> Option<State> {
        match message {
            RaftEvent::VoteRequest(request, sender) => {
                info!("Got a vote request: {request:?}");
                // If the term is greater than the current term, become a follower
                // this would be the case if a new leader has been elected during split brain
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
                // Otherwise No, we are not going to vote for you
                // We are the leader
                let reply = raft::VoteReply {
                    term: self.term,
                    vote_granted: false,
                };
                sender.send(reply).unwrap();
            }
            // Will append the entries to the log
            // the leader is the only state that can append
            RaftEvent::AppendEntriesRequest(request, sender) => {
                info!("Got an append entries request: {request:?}");
                let reply = raft::AppendEntriesReply {
                    term: self.term,
                    success: true,
                };
                sender.send(reply).unwrap();
            }
            // New entry to be added to the log
            // This is the only way to add entries to the log
            // The leader will then replicate the entry to all other nodes
            // If a majority of nodes have the entry, the leader will send a success message to the client
            // The Leader is the only one that can add entries to the log
            RaftEvent::NewEntry { key, value, sender } => {
                info!("Got a new entry: {key}:{value}");

                // Add the entry to the log (the database)
                let res = log.log(key.clone(), value.clone(), self.log_index);

                if let Err(err) = res {
                    sender.send(Err(err)).unwrap();
                    return None;
                }

                // This counts the votes for successful replication
                // We have replicated it to ourselves
                // so we start with a value of 1, representing ourselves
                let mut success = 1;
                // Send append entries to all other nodes

                let mut clients_clone = { clients.lock().unwrap().clone() };
                for nodes in clients_clone.values_mut() {
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
                    let response = nodes.append_entries(request).await.unwrap();

                    if response.get_ref().success {
                        success += 1;
                    }
                }

                // If there is no roster, we are running in single node mode and have reach consensus
                let Some(roster) = config.roster.clone() else {
                    sender.send(Ok(())).unwrap();
                    return None;
                };

                // If a majority of nodes have the entry, send a success message to the client
                if success > roster.len() / 2 {
                    sender.send(Ok(())).unwrap();
                } else {
                    sender
                        .send(Err(NullDbReadError::FailedToReplicate))
                        .unwrap();
                }
            }
            RaftEvent::GetEntry(key, sender) => {
                println!("Got a get entry request: {:?}", key);
                if let Ok(entry) = log.get_value_for_key(&key) {
                    sender.send(Ok(entry)).unwrap();
                } else {
                    sender.send(Err(NullDbReadError::ValueNotFound)).unwrap();
                }
            }
        }
        None
    }
}
