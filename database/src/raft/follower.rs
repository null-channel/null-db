use super::{grpcserver::RaftEvent, State, TIME_OUT};
use crate::{
    errors::NullDbReadError,
    nulldb::DatabaseLog,
    raft::{candidate::CandidateState, raft},
};
use actix_web::web::Data;
use log::info;
use std::time::Instant;

pub struct FollowerState {
    pub last_heartbeat: Instant,
    pub term: u32,
    pub voted: bool,
}

impl FollowerState {
    pub fn new(last_heartbeat: Instant, term: u32) -> FollowerState {
        FollowerState {
            last_heartbeat,
            term,
            voted: false,
        }
    }

    /// tick, when in followers state, only checks if the leader has sent a heartbeat
    pub fn tick(&mut self) -> Option<State> {
        // If we haven't heard from the leader in a while, we should become a candidate
        if self.last_heartbeat.elapsed() > TIME_OUT {
            info!(
                "Becoming Candidate. Failed to get heartbeat from leader. +++++++!!!!!!!!!+++++++"
            );
            return Some(State::Candidate(CandidateState::new(self.term)));
        }
        None
    }

    /// on_message, when in followers state, only handles vote requests and append entries requests
    pub fn on_message<T: DatabaseLog>(
        &mut self,
        message: RaftEvent,
        log: Data<T>,
    ) -> Option<State> {
        match message {
            RaftEvent::VoteRequest(request, sender) => {
                info!("Got a vote request: {request:?}");
                // If the request is from a node with a lower term we should vote no.
                if request.term < self.term {
                    info!("voting no because the term is lower than ours");
                    let reply = raft::VoteReply {
                        term: self.term,
                        vote_granted: false,
                    };
                    sender.send(reply).unwrap();
                    // we will stay a follower
                    return None;
                }

                // if we have not already voted, vote yes
                if !self.voted {
                    info!("voting yes");
                    self.voted = true;
                    self.term = request.term;
                    let reply = raft::VoteReply {
                        term: self.term,
                        vote_granted: true,
                    };

                    // Reset the heartbeat timer
                    // this is what signles that we should go for leadership.
                    self.last_heartbeat = Instant::now();
                    sender.send(reply).unwrap();
                    // We will stay a follower
                    return None;
                }

                // if we have already voted, vote no
                info!("voting no because we already voted");
                let reply = raft::VoteReply {
                    term: self.term,
                    vote_granted: false,
                };
                // Reset the heartbeat timer as a valid vote request was received
                self.last_heartbeat = Instant::now();
                sender.send(reply).unwrap();
                // we will stay a follower
                return None;
            }
            // Leader replication request
            RaftEvent::AppendEntriesRequest(request, sender) => {
                info!("Got an append entries request!");
                // Append the entries to the log
                let res = log.log_entries(request.entries, log.get_current_raft_index());

                // If the append failed, return failure to the leader
                if let Err(e) = res {
                    println!("Failed to append entries: {e:?}");
                    let reply = raft::AppendEntriesReply {
                        term: self.term,
                        success: false,
                    };
                    sender.send(reply).unwrap();
                    return None;
                }
                let reply = raft::AppendEntriesReply {
                    term: self.term,
                    success: true,
                };
                sender.send(reply).unwrap();
                // Reset the heartbeat timer for all valid requests from the leader
                self.last_heartbeat = Instant::now();
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
