use std::time::Instant;

use crate::raft::{follower::FollowerState, leader::LeaderState, raft};

use super::{config::RaftConfig, grpcserver::RaftEvent, RaftClients, RaftLog, State};
pub struct CandidateState {
    pub yes_votes: i32,
    pub no_votes: i32,
    pub current_term: i32,
}

impl CandidateState {
    pub fn new(current_term: i32) -> CandidateState {
        CandidateState {
            yes_votes: 1,
            no_votes: 0,
            current_term,
        }
    }

    pub async fn tick(
        &mut self,
        config: &RaftConfig,
        log: &RaftLog,
        clients: &mut RaftClients,
    ) -> Option<State> {
        for nodes in clients.values_mut() {
            let mut node = nodes.clone();
            let request = tonic::Request::new(raft::VoteRequest {
                term: self.current_term,
                candidate_id: config.candidate_id.clone(),
                last_log_index: log.log_index,
                last_log_term: self.current_term,
            });
            let response = node.vote(request).await.unwrap().into_inner();
            if response.vote_granted {
                self.yes_votes += 1;
            } else {
                self.no_votes += 1;
            }
            if response.term > self.current_term {
                println!("Becoming Follower. Lost election. +++++++!!!!!!!!!+++++++");
                return Some(State::Follower(FollowerState::new(
                    Instant::now(),
                    response.term,
                )));
            }
            let length = log.len() as i32;

            if self.yes_votes > length / 2 {
                println!("Becoming Leader. Won election. +++++++!!!!!!!!!+++++++");
                return Some(State::Leader(LeaderState::new(
                    Instant::now(),
                    self.current_term,
                )));
            }
        }
        None
    }

    pub fn on_message(&mut self, message: RaftEvent) -> Option<State> {
        match message {
            RaftEvent::VoteRequest(request, sender) => {
                println!("vote request: {:?}", request);
                println!("voting no");
                let reply = raft::VoteReply {
                    term: self.current_term,
                    vote_granted: false,
                };
                sender.send(reply).unwrap()
            }
            RaftEvent::AppendEntriesRequest(request, sender) => {
                println!("Got an append entries request: {:?}", request);
                let reply = raft::AppendEntriesReply {
                    term: self.current_term,
                    success: true,
                };
                sender.send(reply).unwrap();
                println!("Becoming Follower again. Failed to become leader because a leader already exists. +++++++!!!!!!!!!+++++++");
            }
            RaftEvent::NewEntry { key: _, value, sender } => {
                println!("Got a new entry: {:?}", value);
                let reply = "I am not the leader".to_string();
                sender.send(reply).unwrap();
            }
            RaftEvent::GetEntry(key, sender) => {
                println!("Got a get entry request: {:?}", key);
                sender.send("Not the leader".to_string()).unwrap();
            }
        }
        None
    }
}
