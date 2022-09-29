pub mod kvstore;
pub mod log;
pub mod multipaxos;

use crate::replicant::kvstore::memkvstore::MemKVStore;
use crate::replicant::log::Log;
use crate::replicant::multipaxos::MultiPaxos;
use ::log::info;
use serde_json::Value as json;
use std::sync::Arc;
use tokio::sync::oneshot::Sender;

struct ReplicantInner {
    id: i64,
    num_peers: i64,
    log: Arc<Log>,
    ip_port: String,
    multi_paxos: MultiPaxos,
}

impl ReplicantInner {
    fn new(config: &json) -> Self {
        let id = config["id"].as_i64().unwrap();
        let peers = config["peers"].as_array().unwrap();
        let num_peers = peers.len() as i64;
        let ip_port = peers[id as usize].as_str().unwrap().to_string();
        let log = Arc::new(Log::new(Box::new(MemKVStore::new())));
        let multi_paxos_log = log.clone();
        Self {
            id,
            num_peers,
            log,
            ip_port,
            multi_paxos: MultiPaxos::new(multi_paxos_log, config),
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
        tx
    }

    pub fn stop(&self, tx: Sender<()>) {
        self.stop_executor_task();
        self.replicant.multi_paxos.stop(tx);
    }

    pub fn start_executor_task(&self) {
        info!("{} starting executor task", self.replicant.id);
    }

    pub fn stop_executor_task(&self) {
        info!("{} stopping executor task", self.replicant.id);
        self.replicant.log.stop();
    }
}
