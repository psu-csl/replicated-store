use super::log::Log;
use crate::kvstore::memkvstore::MemKVStore;
use log::debug;
use rand::distributions::{Distribution, Uniform};
use rand::rngs::ThreadRng;
use serde_json::json;
use serde_json::Value as json;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::{thread, time};

tonic::include_proto!("multipaxos");

struct MultiPaxos {
    ready: AtomicBool,
    state: Mutex<State>,
    log: Arc<Log>,
    id: i64,
    commit_received: AtomicBool,
    commit_interval: u64,
    rng: ThreadRng,
    dist: Uniform<u64>,
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
        let ci = config["commit_interval"].as_u64().unwrap();
        Self {
            ready: AtomicBool::new(false),
            state: Mutex::new(State::default()),
            log: log,
            id: config["id"].as_i64().unwrap(),
            commit_received: AtomicBool::new(false),
            commit_interval: ci,
            rng: rand::thread_rng(),
            dist: Uniform::new(0, ci / 2),
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

    fn sleep_for_commit_interval(&self) {
        thread::sleep(time::Duration::from_millis(self.commit_interval));
    }

    fn sleep_for_random_interval(&mut self) {
        let sleep_time =
            self.commit_interval + self.commit_interval / 2 + self.dist.sample(&mut self.rng);
        thread::sleep(time::Duration::from_millis(sleep_time));
    }

    fn received_commit(&self) -> bool {
        self.commit_received.swap(false, Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    pub const NUM_PEERS: i64 = 3;

    fn make_config(id: i64, num_peers: i64) -> json {
        let mut peers = Vec::new();
        for i in 0..num_peers {
            peers.push(format!("127.0.0.1:1{}000", i));
        }
        json!({
            "id": id,
            "threadpool_size": 8,
            "commit_interval": 300,
            "peers": peers
        })
    }

    #[derive(Default)]
    struct TestState {
        configs: Vec<json>,
        logs: Vec<Arc<Log>>,
        peers: Vec<MultiPaxos>,
    }

    fn init() -> TestState {
        let _ = env_logger::builder().is_test(true).try_init();
        let mut state = TestState::default();
        for id in 0..NUM_PEERS {
            let config = make_config(id, NUM_PEERS);
            let log = Arc::new(Log::new(Box::new(MemKVStore::new())));
            let peer = MultiPaxos::new(log.clone(), &config);

            state.configs.push(config);
            state.logs.push(log);
            state.peers.push(peer);
        }
        state
    }

    #[test]
    fn next_ballot() {
        let state = init();

        for id in 0..NUM_PEERS {
            let ballot = id + ROUND_INCREMENT;
            assert_eq!(ballot, state.peers[id as usize].next_ballot());
        }
    }
}
