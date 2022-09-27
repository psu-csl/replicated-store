pub mod kvstore;
pub mod log;
pub mod multipaxos;

use crate::replicant::kvstore::memkvstore::MemKVStore;
use crate::replicant::log::Log;
use crate::replicant::multipaxos::MultiPaxos;
use serde_json::Value as json;
use std::sync::Arc;
use tokio::sync::oneshot::Sender;

pub struct Replicant {
    id: i64,
    num_peers: i64,
    log: Arc<Log>,
    ip_port: String,
    multi_paxos: MultiPaxos,
}

impl Replicant {
    pub fn new(config: &json) -> Self {
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

    pub fn start(&self) -> Sender<()> {
        self.multi_paxos.start()
    }

    pub fn stop(&self, tx: Sender<()>) {
        self.multi_paxos.stop(tx);
    }
}
