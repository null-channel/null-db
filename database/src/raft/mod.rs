use std::{
    collections::HashMap,
    time::{Duration, Instant},
};
use rand_chacha::ChaCha8Rng;
use rand::prelude::*;
use raft::raft_server::RaftServer;
use tokio::{sync::mpsc::{self, Receiver, Sender}, task::JoinSet};
use tonic::{transport::Server, Request, Response, Status};

//use hello_world::HelloRequest;

pub mod raft {
    tonic::include_proto!("raft");
}
pub mod config;
mod grpcserver;
use config::RaftConfig;

use crate::raft::grpcserver::RaftGRPCServer;

use self::grpcserver::RaftEvent;

const TIME_OUT: Duration = Duration::from_secs(5);

pub struct RaftNode {
    state: State,
    raft_clients:
        HashMap<String, raft::raft_client::RaftClient<tonic::transport::channel::Channel>>,
    log: Vec<String>,
    config: RaftConfig,
    current_term: i32,
    log_index: i32,
    receiver: Receiver<RaftEvent>,
}

impl RaftNode {
    pub fn new(node_id: String, config: RaftConfig, receiver: Receiver<RaftEvent>) -> Self {
        Self {
            state: State::Follower,
            raft_clients: HashMap::new(),
            log: Vec::new(),
            config,
            current_term: 0,
            log_index: 0,
            receiver,
        }
    }

    pub async fn run(&mut self, sender: Sender<RaftEvent>) -> Result<(), Status> {
        // Start the gRPC server
        let port = self.config.candidate_id.clone();
        tokio::spawn(async move {
            let res = start_raft_server(port, sender).await;
            if let Err(e) = res {
                println!("Error: {:?}", e);
            }
        });

        // Wait for the gRPC server to start
        // TODO: fix so there are retries if failed to connect
        tokio::time::sleep(Duration::from_millis(3000)).await;

        // Connect to all other nodes
        for node in self.config.roster.clone() {
            let nameport = node.split(":").collect::<Vec<&str>>();
            let ip = format!(
                "http://{}:{}",
                nameport[0].to_string(),
                nameport[1].to_string()
            );
            println!("Connecting to {}", ip);
            let raft_client = raft::raft_client::RaftClient::connect(ip).await.unwrap();
            self.raft_clients.insert(node.to_string(), raft_client);
        }

        loop {
            let next_state = match &mut self.state {
                State::Follower => {
                    let then = Instant::now();
                    self.follower_run(then).await
                }
                State::Candidate => self.candidate_run().await,
                State::Leader => self.leader_run().await,
            };

            // Do whatever logic to change state.
            self.change_state(next_state);
        }
    }

    async fn leader_run(&mut self) -> State {
        println!("Does Leader Stuff");

        // Send heartbeats to all other nodes
        // TODO: Send heartbeats to all other nodes
        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;

            for nodes in self.raft_clients.values_mut() {
                // TODO: need last log index and term
                let mut node = nodes.clone();
                let request = tonic::Request::new(raft::AppendEntriesRequest {
                    term: self.current_term,
                    leader_id: self.config.candidate_id.clone().parse().unwrap(),
                    prev_log_index: self.log_index,
                    prev_log_term: self.current_term,
                    entries: Vec::new(),
                    leader_commit: 0,
                });
                let _response = node.append_entries(request).await.unwrap();
            }
            // Send heartbeats to all other nodes
            break;
        }
        State::Leader
    }

    async fn candidate_run(&mut self) -> State {
        println!("Does Candidate Stuff");

        // Send vote requests to all other nodes
        let random_secs = {
            let mut rng = ChaCha8Rng::seed_from_u64(self.config.candidate_id.parse().unwrap());
            rng.gen_range(1..2500)
        };

        tokio::time::sleep(Duration::from_millis(random_secs)).await;

        // Check for messages from other nodes
        match self.receiver.try_recv() {
            Ok(RaftEvent::VoteRequest(request, sender)) => {
                println!("Got a vote request: {:?}", request);
                if request.term > self.current_term {
                    self.current_term = request.term;
                    let reply = raft::VoteReply {
                        term: self.current_term,
                        vote_granted: true,
                    };
                    sender.send(reply).unwrap();
                    return State::Follower;
                }
            }
            Ok(RaftEvent::AppendEntriesRequest(request, sender)) => {
                println!("Got an append entries request: {:?}", request);
                let reply = raft::AppendEntriesReply {
                    term: self.current_term,
                    success: true,
                };
                sender.send(reply).unwrap();
                println!("Becoming Follower again. Failed to become leader because a leader already exists. +++++++!!!!!!!!!+++++++");
            }
            Err(_) => {
            }
        }
        // Wait for responses from all other nodes
        self.current_term += 1;
        let mut set = JoinSet::new();
        for nodes in self.raft_clients.values_mut() {
            // TODO: need last log index and term
            let mut node = nodes.clone();
            let request = tonic::Request::new(raft::VoteRequest {
                candidate_id: self.config.candidate_id.clone().parse().unwrap(),
                term: self.current_term,
                last_log_index: self.log_index,
                last_log_term: self.current_term,
            });
            set.spawn(async move {
                let response = node.vote(request).await.unwrap();
                println!("RESPONSE={:?}", response);
                response
            });
        }

        // Check for messages from other nodes
        match self.receiver.try_recv() {
            Ok(RaftEvent::VoteRequest(request, sender)) => {
                println!("Got a vote request: {:?}", request);
                let reply = raft::VoteReply {
                    term: self.current_term,
                    vote_granted: false,
                };
                sender.send(reply).unwrap()
            }
            Ok(RaftEvent::AppendEntriesRequest(request, sender)) => {
                println!("Got an append entries request: {:?}", request);
                let reply = raft::AppendEntriesReply {
                    term: self.current_term,
                    success: true,
                };
                sender.send(reply).unwrap();
                println!("Becoming Follower again. Failed to become leader because a leader already exists. +++++++!!!!!!!!!+++++++");
            }
            Err(_) => {
            }
        }
        let mut votes = 0;
        while let Some(res) = set.join_next().await {
            if res.is_err() {
                println!("Error: {:?}", res);
                continue;
            }
            if let Ok(response) = res {
                if response.get_ref().vote_granted {
                    votes += 1;
                }
            };
        }

        if votes > self.config.roster.len() / 2 {
            print!("Becoming Leader");
            return State::Leader;
        }

        // Check for messages from other nodes
        match self.receiver.try_recv() {
            Ok(RaftEvent::VoteRequest(request, sender)) => {
                println!("vote request: {:?}", request);
                println!("voting no");
                let reply = raft::VoteReply {
                    term: self.current_term,
                    vote_granted: false,
                };
                sender.send(reply).unwrap()
            }
            Ok(RaftEvent::AppendEntriesRequest(request, sender)) => {
                println!("Got an append entries request: {:?}", request);
                let reply = raft::AppendEntriesReply {
                    term: self.current_term,
                    success: true,
                };
                sender.send(reply).unwrap();
                println!("Becoming Follower again. Failed to become leader because a leader already exists. +++++++!!!!!!!!!+++++++");
            }
            Err(_) => {
            }
        }
        println!("Lost vote, Becoming Follower again.");
        State::Follower
    }

    async fn follower_run(&mut self, then: Instant) -> State {
        println!("Does Follower Stuff");

        let mut last_heartbeat = then;
        loop {
            let now = Instant::now();
            if now.duration_since(last_heartbeat) > TIME_OUT {
                println!("Timeout");
                return State::Candidate;
            }

            // Check for messages from other nodes
            match self.receiver.try_recv() {
                Ok(RaftEvent::VoteRequest(request, sender)) => {
                    println!("Got a vote request: {:?}", request);
                    if request.term > self.current_term {
                        println!("voting yes");
                        self.current_term = request.term;
                        let reply = raft::VoteReply {
                            term: self.current_term,
                            vote_granted: true,
                        };
                        sender.send(reply).unwrap();
                        continue;
                    }
                    println!("voting no");
                    let reply = raft::VoteReply {
                        term: self.current_term,
                        vote_granted: false,
                    };
                    sender.send(reply).unwrap()
                }
                Ok(RaftEvent::AppendEntriesRequest(request, sender)) => {
                    println!("Got an append entries request!");
                    let reply = raft::AppendEntriesReply {
                        term: self.current_term,
                        success: true,
                    };
                    sender.send(reply).unwrap();
                    let now = Instant::now();
                    last_heartbeat = now;
                }
                Err(_) => {
                }
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    fn change_state(&mut self, state: State) {
        self.state = state;
    }
}

pub enum State {
    Follower,
    Candidate,
    Leader,
}

impl State {
    pub fn to_string(&self) -> String {
        match self {
            State::Follower => "Follower".to_string(),
            State::Candidate => "Candidate".to_string(),
            State::Leader => "Leader".to_string(),
        }
    }
}

pub async fn start_raft_server(
    port: String,
    sender: Sender<RaftEvent>,
) -> Result<(), Box<dyn std::error::Error>> {
    let raft_server = RaftGRPCServer { event_sender: sender };
    let addr = format!("0.0.0.0:{}", port).parse().unwrap();
    let server = RaftServer::new(raft_server);
    Server::builder().add_service(server).serve(addr).await?;
    println!("Raft server listening on: {}", addr);
    Ok(())
}
