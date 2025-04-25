use self::{
    candidate::CandidateState, follower::FollowerState, grpcserver::RaftEvent, leader::LeaderState,
};
use crate::{nulldb::NullDB, raft::grpcserver::RaftGRPCServer};
use actix_web::web::Data;
use config::RaftConfig;
use log::info;
use raft::raft_server::RaftServer;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use tokio::sync::mpsc::{Receiver, Sender};
use tokio_util::sync::CancellationToken;
use tonic::{transport::Server, Status};

mod candidate;
pub mod config;
mod follower;
pub mod grpcserver;
mod leader;

pub mod raft {
    #![allow(clippy::module_inception)]

    tonic::include_proto!("raft");
}

const TIME_OUT: Duration = Duration::from_secs(1);

type RaftClients =
    Arc<Mutex<HashMap<String, raft::raft_client::RaftClient<tonic::transport::channel::Channel>>>>;

/// RaftNode is the main struct that represents a Raft node.
/// It contains the state of the node, the RaftClients to connect to other nodes,
/// the log, the configuration and a receiver to receive messages.
pub struct RaftNode {
    state: State,
    raft_clients: RaftClients,
    log: Data<NullDB>,
    config: RaftConfig,
    receiver: Receiver<RaftEvent>,
}

impl RaftNode {
    /// Create a new RaftNode with a configuration, a receiver and a log.
    pub fn new(config: RaftConfig, receiver: Receiver<RaftEvent>, log: Data<NullDB>) -> Self {
        let state = match config.roster {
            Some(_) => State::Follower(FollowerState::new(Instant::now(), 0)),
            None => State::Leader(LeaderState::new(Instant::now(), 0)),
        };
        Self {
            state,
            raft_clients: Arc::new(Mutex::new(HashMap::new())),
            log,
            config,
            receiver,
        }
    }

    /// Run the RaftNode.
    /// This function will start the gRPC server, connect to all other nodes and
    /// start the main loop of the RaftNode.
    pub async fn run(
        &mut self,
        sender: Sender<RaftEvent>,
        cancel: CancellationToken,
    ) -> Result<(), RaftConnectionError> {
        // Start the gRPC server
        let id = self.config.candidate_id.clone();
        tokio::spawn(async move {
            let id_vec = id.split(":").collect::<Vec<&str>>();
            let port = id_vec[1];
            let res = start_raft_server(&port, sender).await;
            if let Err(e) = res {
                println!("Error: {:?}", e);
            }
        });

        // Connect to all other nodes
        let config = self.config.clone();
        let clients = connect_to_raft_servers(config).await?;
        {
            let mut lk = self.raft_clients.clone();
            let mut raft_clients = lk.lock().unwrap();
            raft_clients.extend(clients);
        }

        // Main loop
        println!("Starting raft main loop");
        loop {
            if cancel.is_cancelled() {
                break;
            }
            let state = self.tick().await;
            self.next_state(state);
        }
        Ok(())
    }

    /// Run a tick of the RaftNode.
    /// This function will check if there are any messages in the receiver and
    /// call the tick function of the current state.
    /// It will return a new state if a transition is needed.
    /// If no transition is needed, it will return None.
    async fn tick(&mut self) -> Option<State> {
        // Check if there are any messages in the receiver
        // These messages are from the other nodes.
        // If there are messages, call the on_message function of the current state.
        // TODO: don't needto wait for the message to be processed to start processing the next one
        // TODO: use a buffer to store the messages to batch?
        while let Ok(event) = self.receiver.try_recv() {
            info!("Got a message");
            self.state
                .on_message(
                    event,
                    &self.config,
                    self.raft_clients.clone(),
                    self.log.clone(),
                )
                .await;
        }

        // Call the tick function of the current state
        // Tick will check the nodes understanding of the current state of the cluster
        let state = self
            .state
            .tick(&self.config, self.log.clone(), self.raft_clients.clone())
            .await;

        #[allow(clippy::let_and_return)]
        // If state is None, then this fn returns None and no state transition is needed
        state
    }

    /// Transition to a new state if needed.
    fn next_state(&mut self, state: Option<State>) {
        if let Some(state) = state {
            self.state = state;
        }
    }
}

/// The state of the Raft node.
/// The state is responsible for handling messages and timeouts.
pub enum State {
    Follower(FollowerState),
    Candidate(CandidateState),
    Leader(LeaderState),
}

impl State {
    /// Handle a RaftEvent message.
    pub async fn on_message(
        &mut self,
        message: RaftEvent,
        config: &RaftConfig,
        clients: RaftClients,
        log: Data<NullDB>,
    ) -> Option<State> {
        match self {
            State::Follower(follower) => follower.on_message(message, log),
            State::Candidate(candidate) => candidate.on_message(message, log),
            State::Leader(leader) => leader.on_message(message, config, clients, log).await,
        }
    }

    /// Tick checks the current status of the node
    /// optionally returns a state if a transition is needed.
    pub async fn tick(
        &mut self,
        config: &RaftConfig,
        log: Data<NullDB>,
        clients: RaftClients,
    ) -> Option<State> {
        match self {
            State::Follower(follower) => follower.tick(),
            State::Candidate(candidate) => candidate.tick(config, log, clients).await,
            State::Leader(leader) => leader.tick(config, clients).await,
        }
    }
}

#[derive(Debug)]
pub enum RaftConnectionError {
    EmptyRoster,
    MaxAttemptsReached(String),
    JoinError,
}

async fn connect_to_raft_servers(
    config: RaftConfig,
) -> Result<
    HashMap<String, raft::raft_client::RaftClient<tonic::transport::channel::Channel>>,
    RaftConnectionError,
> {
    let Some(roster) = config.roster else {
        return Err(RaftConnectionError::EmptyRoster);
    };

    let mut tasks = tokio::task::JoinSet::new();
    for node in roster {
        if node == config.candidate_id {
            continue;
        }
        tasks.spawn(async move { (node.clone(), connect_to_raft_server(node).await) });
    }

    let mut clients = HashMap::new();
    for (node, client) in tasks.join_all().await {
        clients.insert(node, client?);
    }
    Ok(clients)
}

pub async fn connect_to_raft_server(
    node: String,
) -> Result<raft::raft_client::RaftClient<tonic::transport::Channel>, RaftConnectionError> {
    // try to connect to the node
    // if it fails, the node is not up yet
    // so we will try again in the next iteration after a backoff period
    let mut attempt = 0;
    let max_attempts = 6;
    while attempt < max_attempts {
        println!("Connecting to {node}");
        match raft::raft_client::RaftClient::connect(format!("http://{node}")).await {
            Ok(raft_client) => {
                return Ok(raft_client);
            }
            Err(e) => println!("Error connecting to {node}: {e}"),
        }
        tokio::time::sleep(Duration::from_secs(2 ^ attempt)).await;
        attempt += 1;
    }
    Err(RaftConnectionError::MaxAttemptsReached(node))
}

/// Start raft gRPC server
pub async fn start_raft_server(
    port: &str,
    sender: Sender<RaftEvent>,
) -> Result<(), Box<dyn std::error::Error>> {
    let raft_server = RaftGRPCServer {
        event_sender: sender,
    };
    let addr = format!("0.0.0.0:{port}").parse().unwrap();
    let server = RaftServer::new(raft_server);
    println!("Raft server listening on: {addr}");
    Server::builder().add_service(server).serve(addr).await?;
    Ok(())
}
