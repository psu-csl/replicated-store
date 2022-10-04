mod client_manager;
mod kvstore;
mod log;
mod multipaxos;

use crate::replicant::client_manager::ClientManager;
use crate::replicant::kvstore::memkvstore::MemKVStore;
use crate::replicant::log::Log;
use crate::replicant::multipaxos::MultiPaxos;
use ::log::info;
use serde_json::Value as json;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::oneshot::{self, Receiver, Sender};

struct ReplicantInner {
    id: i64,
    log: Arc<Log>,
    ip_port: String,
    multi_paxos: Arc<MultiPaxos>,
    client_manager: ClientManager,
}

impl ReplicantInner {
    fn new(config: &json) -> Self {
        let id = config["id"].as_i64().unwrap();
        let peers = config["peers"].as_array().unwrap();
        let ip_port = peers[id as usize].as_str().unwrap().to_string();
        let log = Arc::new(Log::new(Box::new(MemKVStore::new())));
        let multi_paxos = Arc::new(MultiPaxos::new(log.clone(), config));
        let num_peers = peers.len() as i64;
        let client_manager =
            ClientManager::new(id, num_peers, multi_paxos.clone());

        Self {
            id,
            log,
            ip_port,
            multi_paxos,
            client_manager,
        }
    }

    async fn executor_task_fn(&self) {
        loop {
            let r = self.log.execute().await;
            if r.is_none() {
                break;
            }
            let (id, result) = r.unwrap();
        }
    }

    async fn server_task_fn(&self, mut shutdown: Receiver<()>) {
        let mut addr: SocketAddr = self.ip_port.parse().unwrap();
        addr.set_port(addr.port() + 1);
        let listener = TcpListener::bind(addr).await.unwrap();

        loop {
            tokio::select! {
                Ok((client, _)) = listener.accept() => {
                    self.client_manager.start(client)
                },
                _ = &mut shutdown => break,
            }
        }
    }
}

pub struct Replicant {
    replicant: Arc<ReplicantInner>,
}

pub struct Shutdown {
    multi_paxos: Sender<()>,
    server: Sender<()>,
}

impl Replicant {
    pub fn new(config: &json) -> Self {
        Self {
            replicant: Arc::new(ReplicantInner::new(config)),
        }
    }

    pub fn start(&self) -> Shutdown {
        let multi_paxos = self.replicant.multi_paxos.start();
        self.start_executor_task();
        let server = self.start_server_task();
        Shutdown {
            multi_paxos,
            server,
        }
    }

    pub fn stop(&self, shutdown: Shutdown) {
        self.stop_server_task(shutdown.server);
        self.stop_executor_task();
        self.replicant.multi_paxos.stop(shutdown.multi_paxos);
    }

    fn start_server_task(&self) -> Sender<()> {
        info!("{} starting server task", self.replicant.id);
        let (shutdown_send, shutdown_recv) = oneshot::channel();
        let replicant = self.replicant.clone();
        tokio::spawn(async move {
            replicant.server_task_fn(shutdown_recv).await;
        });
        shutdown_send
    }

    fn stop_server_task(&self, shutdown: Sender<()>) {
        info!("{} stopping server task", self.replicant.id);
        shutdown.send(()).unwrap();
        self.replicant.client_manager.stop_all();
    }

    fn start_executor_task(&self) {
        info!("{} starting executor task", self.replicant.id);
        let replicant = self.replicant.clone();
        tokio::spawn(async move {
            replicant.executor_task_fn().await;
        });
    }

    fn stop_executor_task(&self) {
        info!("{} stopping executor task", self.replicant.id);
        self.replicant.log.stop();
    }
}
