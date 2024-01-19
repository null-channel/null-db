mod candidate;
pub mod config;
mod follower;
pub mod grpcserver;
mod leader;

use raft::raft_server::RaftServer;
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};
use tokio::sync::mpsc::{Receiver, Sender};
use tonic::{transport::Server, Status};
pub mod raft {
    tonic::include_proto!("raft");
}

use self::{
    candidate::CandidateState, follower::FollowerState, grpcserver::RaftEvent, leader::LeaderState,
};
use crate::raft::grpcserver::RaftGRPCServer;
use config::RaftConfig;
const TIME_OUT: Duration = Duration::from_secs(1);

type RaftClients =
    HashMap<String, raft::raft_client::RaftClient<tonic::transport::channel::Channel>>;

pub struct RaftNode {
    state: State,
    raft_clients: RaftClients,
    log: RaftLog,
    config: RaftConfig,
    receiver: Receiver<RaftEvent>,
}

pub struct RaftLog {
    pub log_index: i32,
    pub log: HashMap<String, String>,
}

impl RaftLog {
    pub fn new() -> RaftLog {
        RaftLog {
            log_index: 0,
            log: HashMap::new(),
        }
    }

    pub fn get(&self, key: &String) -> Option<&String> {
        self.log.get(key)
    }

    pub fn len(&self) -> usize {
        self.log.len()
    }

    pub fn push(&mut self, entry: (String, String)) {
        self.log.insert(entry.0, entry.1);
        self.log_index += 1;
    }

    pub fn push_entries(&mut self, entries: Vec<raft::LogEntry>) {
        for entry in entries {
            self.push((entry.key, entry.value));
        }
        self.log_index += 1;
    }
}

impl RaftNode {
    pub fn new(config: RaftConfig, receiver: Receiver<RaftEvent>) -> Self {
        Self {
            state: State::Follower(FollowerState::new(Instant::now(), 0)),
            raft_clients: HashMap::new(),
            log: RaftLog::new(),
            config,
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
        tokio::time::sleep(Duration::from_millis(2000)).await;

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
            let state = self.run_tick().await;
            self.next_state(state);
        }
    }

    async fn run_tick(&mut self) -> Option<State> {
        let state = self
            .state
            .tick(&self.config, &self.log, &mut self.raft_clients)
            .await;
        if state.is_some() {
            return state;
        }

        match self.receiver.try_recv() {
            Ok(event) => {
                self.state
                    .on_message(event, &self.config, &mut self.raft_clients, &mut self.log)
                    .await
            }
            Err(_) => None,
        }
    }

    fn next_state(&mut self, state: Option<State>) {
        if let Some(state) = state {
            self.state = state;
        }
    }
}

pub enum State {
    Follower(FollowerState),
    Candidate(CandidateState),
    Leader(LeaderState),
}

impl State {
    pub async fn on_message(
        &mut self,
        message: RaftEvent,
        config: &RaftConfig,
        clients: &mut RaftClients,
        log: &mut RaftLog,
    ) -> Option<State> {
        match self {
            State::Follower(follower) => follower.on_message(message, log),
            State::Candidate(candidate) => candidate.on_message(message),
            State::Leader(leader) => leader.on_message(message, config, clients, log).await,
        }
    }

    pub async fn tick(
        &mut self,
        config: &RaftConfig,
        log: &RaftLog,
        clients: &mut RaftClients,
    ) -> Option<State> {
        match self {
            State::Follower(follower) => follower.tick(),
            State::Candidate(candidate) => candidate.tick(config, log, clients).await,
            State::Leader(leader) => leader.tick(config, clients).await,
        }
    }
}

pub async fn start_raft_server(
    port: String,
    sender: Sender<RaftEvent>,
) -> Result<(), Box<dyn std::error::Error>> {
    let raft_server = RaftGRPCServer {
        event_sender: sender,
    };
    let addr = format!("0.0.0.0:{}", port).parse().unwrap();
    let server = RaftServer::new(raft_server);
    Server::builder().add_service(server).serve(addr).await?;
    println!("Raft server listening on: {}", addr);
    Ok(())
}