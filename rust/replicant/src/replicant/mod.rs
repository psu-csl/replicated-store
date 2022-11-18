mod client_manager;
mod kvstore;
mod log;
mod multipaxos;

use crate::replicant::client_manager::ClientManager;
use crate::replicant::kvstore::memkvstore::MemKVStore;
use crate::replicant::log::Log;
use crate::replicant::multipaxos::{MultiPaxos, MultiPaxosHandle};
use ::log::info;
use serde_json::Value as json;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::oneshot::{self, Receiver, Sender};
use tokio::task::JoinHandle;

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
            let (id, value) = r.unwrap();
            let client_manager = self.client_manager.client_manager.clone();
            tokio::spawn(async move {
                match value {
                    Ok(value) => client_manager.write(id, value).await,
                    Err(e) => client_manager.write(id, e.to_string()).await,
                }
            });
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

pub struct ReplicantHandle {
    multi_paxos_handle: MultiPaxosHandle,
    executor_task_handle: JoinHandle<()>,
    server_task_handle: (JoinHandle<()>, Sender<()>),
}

impl Replicant {
    pub fn new(config: &json) -> Self {
        Self {
            replicant: Arc::new(ReplicantInner::new(config)),
        }
    }

    pub fn start(&self) -> ReplicantHandle {
        ReplicantHandle {
            multi_paxos_handle: self.replicant.multi_paxos.start(),
            executor_task_handle: self.start_executor_task(),
            server_task_handle: self.start_server_task(),
        }
    }

    pub async fn stop(&self, handle: ReplicantHandle) {
        self.stop_server_task(handle.server_task_handle).await;
        self.stop_executor_task(handle.executor_task_handle).await;
        self.replicant
            .multi_paxos
            .stop(handle.multi_paxos_handle)
            .await;
    }

    fn start_server_task(&self) -> (JoinHandle<()>, Sender<()>) {
        info!("{} starting server task", self.replicant.id);
        let (shutdown_send, shutdown_recv) = oneshot::channel();
        let replicant = self.replicant.clone();
        let handle = tokio::spawn(async move {
            replicant.server_task_fn(shutdown_recv).await;
        });
        (handle, shutdown_send)
    }

    async fn stop_server_task(&self, handle: (JoinHandle<()>, Sender<()>)) {
        info!("{} stopping server task", self.replicant.id);
        let (handle, shutdown) = handle;
        shutdown.send(()).unwrap();
        handle.await.unwrap();
        self.replicant.client_manager.stop_all();
    }

    fn start_executor_task(&self) -> JoinHandle<()> {
        info!("{} starting executor task", self.replicant.id);
        let replicant = self.replicant.clone();
        tokio::spawn(async move {
            replicant.executor_task_fn().await;
        })
    }

    async fn stop_executor_task(&self, handle: JoinHandle<()>) {
        info!("{} stopping executor task", self.replicant.id);
        self.replicant.log.stop();
        handle.await.unwrap();
    }
}
