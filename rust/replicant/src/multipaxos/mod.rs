use super::log::{Log, MapLog};
use crate::kvstore::memkvstore::MemKVStore;
use log::debug;
use rand::distributions::{Distribution, Uniform};
use serde_json::json;
use serde_json::Value as json;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::{thread, time};

tonic::include_proto!("multipaxos");

pub struct MultiPaxos {
    multi_paxos: Arc<MultiPaxosInner>,
    prepare_thread: Option<thread::JoinHandle<()>>,
    commit_thread: Option<thread::JoinHandle<()>>,
}

#[derive(Default)]
struct AtomicState {
    ballot: i64,
    rpc_server_running: bool,
}

struct MultiPaxosInner {
    atomic_state: Mutex<AtomicState>,
    ready: AtomicBool,
    log: Log,
    id: i64,
    commit_received: AtomicBool,
    commit_interval: u64,
    prepare_thread_running: AtomicBool,
    commit_thread_running: AtomicBool,
    cv_leader: Condvar,
    cv_follower: Condvar,
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
    pub fn new(log: Log, config: &json) -> Self {
        Self {
            multi_paxos: Arc::new(MultiPaxosInner::new(log, config)),
            prepare_thread: None,
            commit_thread: None,
        }
    }

    pub fn start_prepare_thread(&mut self) {
        debug!("{} starting prepare thread", self.multi_paxos.id);
        self.multi_paxos
            .prepare_thread_running
            .store(true, Ordering::Relaxed);
        self.prepare_thread = {
            let multi_paxos = self.multi_paxos.clone();
            Some(std::thread::spawn(move || multi_paxos.prepare_thread_fn()))
        }
    }

    pub fn stop_prepare_thread(&mut self) {
        debug!("{} stopping prepare thread", self.multi_paxos.id);
        assert!(self
            .multi_paxos
            .prepare_thread_running
            .load(Ordering::Relaxed));
        self.multi_paxos
            .prepare_thread_running
            .store(false, Ordering::Relaxed);
        self.multi_paxos.cv_follower.notify_one();
        self.prepare_thread.take().unwrap().join().unwrap();
    }
}

impl MultiPaxosInner {
    fn new(log: Log, config: &json) -> Self {
        let ci = config["commit_interval"].as_u64().unwrap();
        Self {
            atomic_state: Mutex::new(AtomicState::default()),
            ready: AtomicBool::new(false),
            log: log,
            id: config["id"].as_i64().unwrap(),
            commit_received: AtomicBool::new(false),
            commit_interval: ci,
            prepare_thread_running: AtomicBool::new(false),
            commit_thread_running: AtomicBool::new(false),
            cv_leader: Condvar::new(),
            cv_follower: Condvar::new(),
        }
    }

    fn next_ballot(&self) -> i64 {
        let atomic_state = self.atomic_state.lock().unwrap();
        let mut next_ballot = atomic_state.ballot;
        next_ballot += ROUND_INCREMENT;
        next_ballot = (next_ballot & !ID_BITS) | self.id;
        next_ballot
    }

    fn become_leader(&self, next_ballot: i64) {
        let mut atomic_state = self.atomic_state.lock().unwrap();
        debug!(
            "{} became a leader: ballot: {} -> {}",
            self.id, atomic_state.ballot, next_ballot
        );
        atomic_state.ballot = next_ballot;
        self.ready.store(false, Ordering::Relaxed);
        self.cv_leader.notify_one();
    }

    fn become_follower(&self, atomic_state: &mut AtomicState, next_ballot: i64) {
        let prev_leader = leader(atomic_state.ballot);
        let next_leader = leader(next_ballot);
        if next_leader != self.id && (prev_leader == self.id || prev_leader == MAX_NUM_PEERS) {
            debug!(
                "{} became a follower: ballot: {} -> {}",
                self.id, atomic_state.ballot, next_ballot
            );
            self.cv_follower.notify_one();
        }
        atomic_state.ballot = next_ballot;
    }

    fn ballot(&self) -> (i64, bool) {
        let atomic_state = self.atomic_state.lock().unwrap();
        (atomic_state.ballot, self.ready.load(Ordering::Relaxed))
    }

    fn wait_until_leader(&self) {
        let mut atomic_state = self.atomic_state.lock().unwrap();
        while self.commit_thread_running.load(Ordering::Relaxed)
            && !is_leader(atomic_state.ballot, self.id)
        {
            atomic_state = self.cv_leader.wait(atomic_state).unwrap();
        }
    }

    fn wait_until_follower(&self) {
        let mut atomic_state = self.atomic_state.lock().unwrap();
        while self.prepare_thread_running.load(Ordering::Relaxed)
            && is_leader(atomic_state.ballot, self.id)
        {
            atomic_state = self.cv_follower.wait(atomic_state).unwrap();
        }
    }

    fn sleep_for_commit_interval(&self) {
        thread::sleep(time::Duration::from_millis(self.commit_interval));
    }

    fn sleep_for_random_interval(&self) {
        let ci = self.commit_interval as u64;
        let dist = Uniform::new(0, (ci / 2) as u64);
        let sleep_time = ci + ci / 2 + dist.sample(&mut rand::thread_rng());
        thread::sleep(time::Duration::from_millis(sleep_time));
    }

    fn received_commit(&self) -> bool {
        self.commit_received.swap(false, Ordering::Relaxed)
    }

    fn start_prepare_thread(&self) {
        debug!("{} starting prepare thread", self.id);
        self.prepare_thread_running.store(true, Ordering::Relaxed);
    }

    fn prepare_thread_fn(&self) {
        while self.prepare_thread_running.load(Ordering::Relaxed) {
            self.wait_until_follower();
            while self.prepare_thread_running.load(Ordering::Relaxed) {
                self.sleep_for_random_interval();
                if self.received_commit() {
                    continue;
                }
                let next_ballot = self.next_ballot();
                if let Some(log) = self.run_prepare_phase(next_ballot) {
                    self.become_leader(next_ballot);
                    self.replay(next_ballot, &log);
                }
            }
        }
    }

    fn run_prepare_phase(&self, ballot: i64) -> Option<MapLog> {
        None
    }

    fn replay(&self, ballot: i64, log: &MapLog) {}
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
        peers: Vec<MultiPaxos>,
    }

    fn init() -> TestState {
        let _ = env_logger::builder().is_test(true).try_init();
        let mut state = TestState::default();
        for id in 0..NUM_PEERS {
            let config = make_config(id, NUM_PEERS);
            let log = Log::new(Box::new(MemKVStore::new()));
            let peer = MultiPaxos::new(log, &config);

            state.configs.push(config);
            state.peers.push(peer);
        }
        state
    }

    #[test]
    fn next_ballot() {
        let state = init();

        for id in 0..NUM_PEERS {
            let ballot = id + ROUND_INCREMENT;
            assert_eq!(ballot, state.peers[id as usize].multi_paxos.next_ballot());
        }
    }
}
