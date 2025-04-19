use super::{config::RaftConfig, grpcserver::RaftEvent, raft::VoteReply, RaftClients, State};
use crate::{
    errors::NullDbReadError,
    nulldb::DatabaseLog,
    nulldb::NullDB,
    raft::{follower::FollowerState, leader::LeaderState, raft},
};
use actix_web::web::Data;
use log::info;
use std::{sync::atomic::Ordering, time::Instant};
use tokio::sync::oneshot::Receiver;

pub struct CandidateState {
    pub has_requested_votes: bool,
    pub yes_votes: i32,
    pub no_votes: i32,
    pub current_term: u32,
    votes: Vec<Receiver<VoteReply>>,
}

impl CandidateState {
    pub fn new(current_term: u32) -> CandidateState {
        CandidateState {
            // We start with one vote, our own
            yes_votes: 1,
            has_requested_votes: false,
            no_votes: 0,
            current_term,
            votes: vec![],
        }
    }

    pub async fn tick(
        &mut self,
        config: &RaftConfig,
        // TODO: This should be "DatabaseLog" not "NullDB"
        log: Data<NullDB>,
        clients: RaftClients,
    ) -> Option<State> {
        // Lets hold an election for ourselves

        // Get all the clients for the nodes in the cluster
        let clients_clone = { clients.lock().unwrap().clone() };
        let num_clients = clients_clone.len() as i32;

        // If we haven't requested votes yet, lets do it!
        // For this term we will only request votes once,
        // if we don't get enough votes we will become a candidate again in the next term.
        if !self.has_requested_votes {
            info!("Sending vote requests to all nodes");
            for mut node in clients_clone.into_values() {
                let request = tonic::Request::new(raft::VoteRequest {
                    term: self.current_term,
                    candidate_id: config.candidate_id.clone(),
                    last_log_index: log.current_raft_index.load(Ordering::Relaxed),
                    last_log_term: self.current_term,
                });

                info!("Sending vote request to node: {node:?}");
                let (sender, receiver) = tokio::sync::oneshot::channel();
                self.votes.push(receiver);
                tokio::spawn(async move {
                    info!("inside spawn: {node:?}");
                    let response = node.vote(request).await.unwrap().into_inner();
                    sender.send(response).unwrap();
                });
            }
            self.has_requested_votes = true;
            return None;
        }

        // Check if we have enough votes to become a leader!!!!
        let mut voters = vec![];
        while let Some(mut vote) = self.votes.pop() {
            info!("Checking vote inner loop");
            let Ok(response) = vote.try_recv() else {
                info!("Vote not ready");
                voters.push(vote);
                continue;
            };

            // If the vote was granted, increment the yes votes
            if response.vote_granted {
                self.yes_votes += 1;
            } else {
                self.no_votes += 1;
            }

            // if voter term is greater than ours, we should become a follower
            if response.term > self.current_term {
                info!("Becoming Follower. Lost election due to term.");
                return Some(State::Follower(FollowerState::new(
                    Instant::now(),
                    response.term,
                )));
            }

            // if we have more than half the votes, we should become a leader
            if self.yes_votes > (num_clients / 2) {
                info!("Becoming Leader. Won election.");
                return Some(State::Leader(LeaderState::new(
                    Instant::now(),
                    self.current_term,
                )));
            }

            // if we lost more than half the votes, we should become a follower
            if self.no_votes > (num_clients / 2) {
                info!("Becoming Follower. Lost election.");
                return Some(State::Follower(FollowerState::new(
                    Instant::now(),
                    response.term,
                )));
            }
        }
        // If we haven't received all the votes yet, we should wait for the next tick
        self.votes = voters;
        info!("Number of votes: {:?}", self.votes);
        None
    }

    pub fn on_message<T: DatabaseLog>(
        &mut self,
        message: RaftEvent,
        log: Data<T>,
    ) -> Option<State> {
        match message {
            // If we get a vote request, we should vote no because we are a candidate
            RaftEvent::VoteRequest(request, sender) => {
                info!("vote request: {request:?}");
                info!("voting no");
                let reply = raft::VoteReply {
                    term: self.current_term,
                    vote_granted: false,
                };
                sender.send(reply).unwrap()
            }
            // If we get an append entries request, we should save the data and become a follower
            RaftEvent::AppendEntriesRequest(request, sender) => {
                info!("Got an append entries request: {request:?}");
                let res = log.log_entries(request.entries, log.get_current_raft_index());

                // If the append failed, return failure to the leader
                if let Err(e) = res {
                    println!("Failed to append entries: {e:?}");
                    let reply = raft::AppendEntriesReply {
                        term: self.current_term,
                        success: false,
                    };
                    sender.send(reply).unwrap();
                    return None;
                }

                // if the append worked return success to the leader
                let reply = raft::AppendEntriesReply {
                    term: self.current_term,
                    success: true,
                };
                sender.send(reply).unwrap();
                info!("Becoming Follower again. Failed to become leader because a leader already exists. +++++++!!!!!!!!!+++++++");
                return Some(State::Follower(FollowerState::new(
                    Instant::now(),
                    self.current_term,
                )));
            }

            // New entry to be added to the log
            // This is only usable if you are in "leader" state
            // we could proxy this to the leader, but we are not going to do that
            // because that seems like a lot of work for me.
            RaftEvent::NewEntry {
                key: _,
                value,
                sender,
            } => {
                info!("Got a new entry, not the leader. Entry value: {value:?}");
                sender.send(Err(NullDbReadError::NotLeader)).unwrap();
            }

            // Get entry request can only be handled by the leader
            RaftEvent::GetEntry(key, sender) => {
                info!("Got a request for a value, not the leader rejecting: {key:?}");
                sender.send(Err(NullDbReadError::NotLeader)).unwrap();
            }
        }
        None
    }
}
