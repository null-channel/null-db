use std::{time::Instant, sync::atomic::Ordering};
use log::info;
use actix_web::web::Data;
use tokio::sync::oneshot::Receiver;

use crate::{raft::{follower::FollowerState, leader::LeaderState, raft}, nulldb::NullDB, errors::NullDbReadError};

use super::{config::RaftConfig, grpcserver::RaftEvent, RaftClients, State, raft::VoteReply};
pub struct CandidateState {
    pub has_voted: bool,
    pub yes_votes: i32,
    pub no_votes: i32,
    pub current_term: u32,
    votes: Vec<Receiver<VoteReply>>,
}

impl CandidateState {
    pub fn new(current_term: u32) -> CandidateState {
        CandidateState {
            has_voted: false,
            yes_votes: 1,
            no_votes: 0,
            current_term,
            votes: vec![],
        }
    }

    pub async fn tick(
        &mut self,
        config: &RaftConfig,
        log: Data<NullDB>,
        clients: &mut RaftClients,
    ) -> Option<State> {
        let num_clients = clients.len() as i32;

        let mut voters = vec![];
        while let Some(mut vote) = self.votes.pop() {
            info!("Checking vote inner loop");
            let response = vote.try_recv();
            let Ok(response) = response else {
                info!("Vote not ready");
                voters.push(vote);
                continue;
            };

            if response.vote_granted {
                self.yes_votes += 1;
            } else {
                self.no_votes += 1;
            }
            
            if response.term > self.current_term {
                info!("Becoming Follower. Lost election due to term. +++++++!!!!!!!!!+++++++");
                return Some(State::Follower(FollowerState::new(
                    Instant::now(),
                    response.term,
                )));
            }

            if self.yes_votes > (num_clients / 2) {
                info!("Becoming Leader. Won election. +++++++!!!!!!!!!+++++++");
                return Some(State::Leader(LeaderState::new(
                    Instant::now(),
                    self.current_term,
                )));
            }

            if self.no_votes > (num_clients / 2) {
                info!("Becoming Follower. Lost election. !!!!!!!!!+++++++!!!!!!!!!!");
                return Some(State::Follower(FollowerState::new(
                    Instant::now(),
                    response.term,
                )));
            }
        }
        self.votes = voters;
        info!("Number of votes: {:?}", self.votes);

        if self.has_voted {
            return None;
        }
        info!("Sending vote requests to all nodes");
        for nodes in clients.values_mut() {
            info!("Sending vote request to node: {:?}", nodes);
            let node = nodes.clone();
            let request = tonic::Request::new(raft::VoteRequest {
                term: self.current_term,
                candidate_id: config.candidate_id.clone(),
                last_log_index: log.current_raft_index.load(Ordering::Relaxed),
                last_log_term: self.current_term,
            });

            info!("Sending vote request to node: {:?}", node);
            let (sender, receiver) = tokio::sync::oneshot::channel();
            self.votes.push(receiver);
            let mut n = node.clone();
            tokio::spawn(async move {
                info!("inside spawn: {:?}", node);
                let response = n.vote(request).await.unwrap().into_inner();
                sender.send(response).unwrap();
            });
        }
        self.has_voted = true;
        None
    }

    pub fn on_message(&mut self, message: RaftEvent) -> Option<State> {
        match message {
            RaftEvent::VoteRequest(request, sender) => {
                info!("vote request: {:?}", request);
                info!("voting no");
                let reply = raft::VoteReply {
                    term: self.current_term,
                    vote_granted: false,
                };
                sender.send(reply).unwrap()
            }
            RaftEvent::AppendEntriesRequest(request, sender) => {
                info!("Got an append entries request: {:?}", request);
                let reply = raft::AppendEntriesReply {
                    term: self.current_term,
                    success: true,
                };
                sender.send(reply).unwrap();
                info!("Becoming Follower again. Failed to become leader because a leader already exists. +++++++!!!!!!!!!+++++++");
            }
            RaftEvent::NewEntry {
                key: _,
                value,
                sender,
            } => {
                info!("Got a new entry: {:?}", value);
                sender.send(Err(NullDbReadError::NotLeader)).unwrap();
            }
            RaftEvent::GetEntry(key, sender) => {
                info!("Got a get entry request: {:?}", key);
                sender.send(Err(NullDbReadError::NotLeader)).unwrap();
            }
        }
        None
    }
}
