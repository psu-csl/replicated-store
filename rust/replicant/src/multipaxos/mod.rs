use super::log::Log;
use crate::kvstore::memkvstore::MemKVStore;
use log::debug;
use serde_json::json;
use serde_json::Value as json;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Condvar, Mutex};
use std::{thread, time};

tonic::include_proto!("multipaxos");

struct MultiPaxos {
    ready: AtomicBool,
    state: Mutex<State>,
    log: Arc<Log>,
    id: i64,
    commit_received: AtomicBool,
    commit_interval: i64,
    prepare_thread_running: AtomicBool,
    commit_thread_running: AtomicBool,
    cv_leader: Condvar,
    cv_follower: Condvar,
}

#[derive(Default)]
struct State {
    ballot: i64,
    rpc_server_running: bool,
}

const ID_BITS: i64 = 0xff;
const ROUND_INCREMENT: i64 = ID_BITS + 1;
const MAX_NUM_PEERS: i64 = 0xf;

fn leader(ballot: i64) -> i64 {
    ballot & ID_BITS
}

fn is_leader(ballot: i64, id: i64) -> bool {
    leader(ballot) == id
}

fn is_someone_else_leader(ballot: i64, id: i64) -> bool {
    !is_leader(ballot, id) && leader(ballot) < MAX_NUM_PEERS
}

impl MultiPaxos {
    fn new(log: Arc<Log>, config: &json) -> Self {
        Self {
            ready: AtomicBool::new(false),
            state: Mutex::new(State::default()),
            log: log,
            id: config["id"].as_i64().unwrap(),
            commit_received: AtomicBool::new(false),
            commit_interval: config["commit_interval"].as_i64().unwrap(),
            prepare_thread_running: AtomicBool::new(false),
            commit_thread_running: AtomicBool::new(false),
            cv_leader: Condvar::new(),
            cv_follower: Condvar::new(),
        }
    }

    fn next_ballot(&self) -> i64 {
        let state = self.state.lock().unwrap();
        let mut next_ballot = state.ballot;
        next_ballot += ROUND_INCREMENT;
        next_ballot = (next_ballot & !ID_BITS) | self.id;
        next_ballot
    }

    fn become_leader(&mut self, next_ballot: i64) {
        let mut state = self.state.lock().unwrap();
        debug!(
            "{} became a leader: ballot: {} -> {}",
            self.id, state.ballot, next_ballot
        );
        state.ballot = next_ballot;
        *self.ready.get_mut() = false;
        self.cv_leader.notify_one();
    }

    fn become_follower(&self, state: &mut State, next_ballot: i64) {
        let prev_leader = leader(state.ballot);
        let next_leader = leader(next_ballot);
        if next_leader != self.id && (prev_leader == self.id || prev_leader == MAX_NUM_PEERS) {
            debug!(
                "{} became a follower: ballot: {} -> {}",
                self.id, state.ballot, next_ballot
            );
            self.cv_follower.notify_one();
        }
        state.ballot = next_ballot;
    }

    fn ballot(&mut self) -> (i64, bool) {
        let state = self.state.lock().unwrap();
        (state.ballot, *self.ready.get_mut())
    }

    fn wait_until_leader(&mut self) {
        let mut state = self.state.lock().unwrap();
        while *self.commit_thread_running.get_mut() && !is_leader(state.ballot, self.id) {
            state = self.cv_leader.wait(state).unwrap();
        }
    }

    fn wait_until_follower(&mut self) {
        let mut state = self.state.lock().unwrap();
        while *self.prepare_thread_running.get_mut() && is_leader(state.ballot, self.id) {
            state = self.cv_follower.wait(state).unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    pub const NUM_PEERS: i64 = 3;

    fn make_config(id: i64, num_peers: i64) -> json {
        let mut peers = Vec::new();
        for i in 0..num_peers {
            peers.push(format!(r#"127.0.0.1{}000"#, i));
        }
        json!({
            "id": id,
            "threadpool_size": 8,
            "commit_interval": 300,
            "peers": peers
        })
    }

    fn make_peers(log: Arc<Log>) -> Vec<MultiPaxos> {
        let mut peers = vec![];
        for id in 0..NUM_PEERS {
            peers.push(MultiPaxos::new(log.clone(), &make_config(id, NUM_PEERS)));
        }
        peers
    }

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[test]
    fn next_ballot() {
        init();
        let store = Box::new(MemKVStore::new());
        let peers = make_peers(Arc::new(Log::new(store)));

        for id in 0..NUM_PEERS {
            let ballot = id + ROUND_INCREMENT;
            assert_eq!(ballot, peers[id as usize].next_ballot())
        }
    }
}
