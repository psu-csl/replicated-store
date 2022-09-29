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
use tokio::sync::oneshot::Sender;

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
        let client_manager = ClientManager::new(id, peers.len() as i64);

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

    async fn server_task_fn(&self) {
        let mut addr: SocketAddr = self.ip_port.parse().unwrap();
        addr.set_port(addr.port() + 1);
        let listener = TcpListener::bind(addr).await.unwrap();

        loop {
            match listener.accept().await {
                Ok((client, _)) => {
                    self.client_manager.start(client, self.multi_paxos.clone())
                }
                Err(_) => break,
            }
        }
    }
}

pub struct Replicant {
    replicant: Arc<ReplicantInner>,
}

impl Replicant {
    pub fn new(config: &json) -> Self {
        Self {
            replicant: Arc::new(ReplicantInner::new(&config)),
        }
    }

    pub fn start(&self) -> Sender<()> {
        let tx = self.replicant.multi_paxos.start();
        self.start_executor_task();
        self.start_server_task();
        tx
    }

    pub fn stop(&self, tx: Sender<()>) {
        self.stop_server_task();
        self.stop_executor_task();
        self.replicant.multi_paxos.stop(tx);
    }

    pub fn start_executor_task(&self) {
        info!("{} starting executor task", self.replicant.id);
        let replicant = self.replicant.clone();
        tokio::spawn(async move {
            replicant.executor_task_fn().await;
        });
    }

    pub fn stop_executor_task(&self) {
        info!("{} stopping executor task", self.replicant.id);
        self.replicant.log.stop();
    }

    pub fn start_server_task(&self) {
        info!("{} starting server task", self.replicant.id);
        let replicant = self.replicant.clone();
        tokio::spawn(async move {
            replicant.server_task_fn().await;
        });
    }

    pub fn stop_server_task(&self) {
        info!("{} stopping server task", self.replicant.id);
    }
}
