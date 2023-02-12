use crate::replicant::kvstore::memkvstore::MemKVStore;
use crate::replicant::log::{insert, Log};
use log::info;
use parking_lot::Mutex;
use rand::distributions::{Distribution, Uniform};
use serde_json::json;
use serde_json::Value as json;
use std::cmp;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::oneshot::{self, Sender};
use tokio::sync::{mpsc, Notify};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{sleep, Duration};
use crate::replicant::multipaxos::msg::{Command, Instance};
use crate::replicant::multipaxos::msg::{MessageType, ResponseType};
use crate::replicant::multipaxos::msg::{AcceptRequest, AcceptResponse};
use crate::replicant::multipaxos::msg::{CommitRequest, CommitResponse};
use crate::replicant::multipaxos::msg::{PrepareRequest, PrepareResponse};
use crate::replicant::multipaxos::tcp::TcpLink;

pub mod msg;
pub mod tcp;

const ID_BITS: i64 = 0xff;
const ROUND_INCREMENT: i64 = ID_BITS + 1;
const MAX_NUM_PEERS: i64 = 0xf;

fn extract_leader(ballot: i64) -> i64 {
    ballot & ID_BITS
}

fn is_leader(ballot: i64, id: i64) -> bool {
    extract_leader(ballot) == id
}

fn is_someone_else_leader(ballot: i64, id: i64) -> bool {
    !is_leader(ballot, id) && extract_leader(ballot) < MAX_NUM_PEERS
}

#[derive(PartialEq, Eq, Debug)]
pub enum ResultType {
    Ok,
    Retry,
    SomeoneElseLeader(i64),
}

struct Peer {
    id: i64,
    stub: Arc<TcpLink>,
}

struct MultiPaxosInner {
    ballot: AtomicI64,
    log: Arc<Log>,
    id: i64,
    commit_received: AtomicBool,
    commit_interval: u64,
    port: SocketAddr,
    peers: Vec<Peer>,
    next_request_id: AtomicU64,
    channels: Arc<tokio::sync::Mutex<HashMap<u64, mpsc::Sender<String>>>>,
    prepare_task_running: AtomicBool,
    commit_task_running: AtomicBool,
    became_leader: Notify,
    became_follower: Notify,
}

async fn make_stub(
    port: &json,
    channels: Arc<tokio::sync::Mutex<HashMap<u64, mpsc::Sender<String>>>>
) -> Arc<TcpLink> {
    Arc::new(TcpLink::new(port.as_str().unwrap(), channels).await)
}

impl MultiPaxosInner {
    async fn new(log: Arc<Log>, config: &json) -> Self {
        let commit_interval = config["commit_interval"].as_u64().unwrap();
        let id = config["id"].as_i64().unwrap();
        let port = config["peers"][id as usize]
            .as_str()
            .unwrap()
            .parse()
            .unwrap();

        let mut rpc_peers = Vec::new();
        let channels = Arc::new(tokio::sync::Mutex::new(HashMap::new()));
        for (id, port) in config["peers"].as_array().unwrap().iter().enumerate()
        {
            rpc_peers.push(Peer {
                id: id as i64,
                stub: make_stub(port, channels.clone()).await,
            });
        }
        Self {
            ballot: AtomicI64::new(MAX_NUM_PEERS),
            log,
            id,
            commit_received: AtomicBool::new(false),
            commit_interval,
            port,
            peers: rpc_peers,
            next_request_id: AtomicU64::new(1),
            channels,
            prepare_task_running: AtomicBool::new(false),
            commit_task_running: AtomicBool::new(false),
            became_leader: Notify::new(),
            became_follower: Notify::new(),
        }
    }

    fn ballot(&self) -> i64 {
        self.ballot.load(Ordering::Relaxed)
    }

    fn next_ballot(&self) -> i64 {
        let mut next_ballot = self.ballot();
        next_ballot += ROUND_INCREMENT;
        next_ballot = (next_ballot & !ID_BITS) | self.id;
        next_ballot
    }

    fn become_leader(&self, new_ballot: i64, new_last_index: i64) {
        info!(
            "{} became a leader: ballot: {} -> {}",
            self.id, self.ballot(), new_ballot
        );
        self.log.set_last_index(new_last_index);
        self.ballot.store(new_ballot, Ordering::Relaxed);
        self.became_leader.notify_one();
    }

    fn become_follower(&self, new_ballot: i64) {
        if new_ballot <= self.ballot() {
            return;
        }
        let old_leader_id = extract_leader(self.ballot());
        let new_leader_id = extract_leader(new_ballot);
        if new_leader_id != self.id
            && (old_leader_id == self.id || old_leader_id == MAX_NUM_PEERS)
        {
            info!(
                "{} became a follower: ballot: {} -> {}",
                self.id, self.ballot(), new_ballot
            );
            self.became_follower.notify_one();
        }
        self.ballot.store(new_ballot, Ordering::Relaxed);
    }

    async fn sleep_for_commit_interval(&self) {
        sleep(Duration::from_millis(self.commit_interval)).await;
    }

    async fn sleep_for_random_interval(&self) {
        let ci = self.commit_interval as u64;
        let dist = Uniform::new(0, (ci / 2) as u64);
        let sleep_time = ci + ci / 2 + dist.sample(&mut rand::thread_rng());
        sleep(Duration::from_millis(sleep_time)).await;
    }

    fn received_commit(&self) -> bool {
        self.commit_received.swap(false, Ordering::Relaxed)
    }

    async fn prepare_task_fn(&self) {
        while self.prepare_task_running.load(Ordering::Relaxed) {
            self.became_follower.notified().await;
            while self.prepare_task_running.load(Ordering::Relaxed) {
                self.sleep_for_random_interval().await;
                if self.received_commit() {
                    continue;
                }
                let next_ballot = self.next_ballot();
                if let Some((last_index, log)) =
                    self.run_prepare_phase(next_ballot).await
                {
                    self.become_leader(next_ballot, last_index);
                    self.replay(next_ballot, log).await;
                    break;
                }
            }
        }
    }

    async fn commit_task_fn(&self) {
        while self.commit_task_running.load(Ordering::Relaxed) {
            self.became_leader.notified().await;
            let mut gle = self.log.global_last_executed();
            while self.commit_task_running.load(Ordering::Relaxed) {
                let ballot = self.ballot();
                if !is_leader(ballot, self.id) {
                    break;
                }
                gle = self.run_commit_phase(ballot, gle).await;
                self.sleep_for_commit_interval().await;
            }
        }
    }

    async fn run_prepare_phase(
        &self,
        ballot: i64,
    ) -> Option<(i64, HashMap<i64, Instance>)> {
        let num_peers = self.peers.len();
        let mut responses = JoinSet::new();
        let mut num_oks = 0;
        let mut log = HashMap::new();
        let mut last_index = 0;

        if ballot > self.ballot() {
            num_oks += 1;
            log = self.log.get_log();
            last_index = self.log.last_index();
            if num_oks > num_peers / 2 {
                return Some((last_index, log));
            }
        } else {
            return None;
        }

        let request = serde_json::to_string(&PrepareRequest {
            ballot,
            sender: self.id,
        }).unwrap();
        let (channel_id, mut response_recv) = self.add_channel(num_peers).await;
        self.peers.iter().for_each(|peer| {
            if peer.id != self.id {
                let request = request.clone();
                let stub = peer.stub.clone();
                responses.spawn(async move {
                    stub.send_await_response(
                        MessageType::PrepareRequest, channel_id, &request).await;
                });
                info!("{} sent prepare request to {}", self.id, peer.id);
            }
        });

        loop {
            let response_result = response_recv.recv().await.unwrap();
            let prepare_response: PrepareResponse =
                serde_json::from_str(&response_result).unwrap();
            if prepare_response.r#type == ResponseType::Ok as i32 {
                num_oks += 1;
                for instance in prepare_response.instances {
                    last_index = cmp::max(last_index, instance.index);
                    insert(&mut log, instance);
                }
            } else {
                self.become_follower(prepare_response.ballot);
                break;
            }
            if num_oks > num_peers / 2 {
                self.remove_channel(channel_id, response_recv);
                return Some((last_index, log));
            }
        }
        self.remove_channel(channel_id, response_recv);
        None
    }

    async fn run_accept_phase(
        &self,
        ballot: i64,
        index: i64,
        command: &Command,
        client_id: i64,
    ) -> ResultType {
        let num_peers = self.peers.len();
        let mut responses = JoinSet::new();
        let mut num_oks = 0;
        let mut current_leader = self.id;

        if ballot == self.ballot() {
            num_oks += 1;
            let instance = Instance::inprogress(
                ballot, index, command, client_id,
            );
            self.log.append(instance);
            if num_oks > num_peers / 2 {
                self.log.commit(index).await;
                return ResultType::Ok;
            }
        } else {
            let current_leader_id = extract_leader(self.ballot());
            return ResultType::SomeoneElseLeader(current_leader_id);
        }

        let request = serde_json::to_string(&AcceptRequest {
            instance: Some(Instance::inprogress(
                ballot, index, command, client_id,
            )),
            sender: self.id,
        }).unwrap();
        let (channel_id, mut response_recv) = self.add_channel(num_peers).await;
        self.peers.iter().for_each(|peer| {
            if peer.id != self.id {
                let request = request.clone();
                let stub = peer.stub.clone();
                responses.spawn(async move {
                    stub.send_await_response(
                        MessageType::AcceptRequest, channel_id, &request).await;
                });
                info!("{} sent accept request to {}", self.id, peer.id);
            }
        });

        loop {
            let response_result = response_recv.recv().await.unwrap();
            let accept_response: AcceptResponse =
                serde_json::from_str(&response_result).unwrap();
            if accept_response.r#type == ResponseType::Ok as i32 {
                num_oks += 1;
            } else {
                self.become_follower(accept_response.ballot);
                current_leader = extract_leader(self.ballot());
                break;
            }
            if num_oks > num_peers / 2 {
                self.log.commit(index).await;
                self.remove_channel(channel_id, response_recv);
                responses.detach_all();
                return ResultType::Ok;
            }
        }
        if current_leader != self.id {
            self.remove_channel(channel_id, response_recv);
            return ResultType::SomeoneElseLeader(current_leader);
        }
        self.remove_channel(channel_id, response_recv);
        ResultType::Retry
    }

    async fn run_commit_phase(
        &self,
        ballot: i64,
        global_last_executed: i64,
    ) -> i64 {
        let num_peers = self.peers.len();
        let mut responses = JoinSet::new();
        let mut num_oks = 0;
        let mut min_last_executed = self.log.last_executed();

        num_oks += 1;
        self.log.trim_until(global_last_executed);
        if num_oks == num_peers {
            return min_last_executed;
        }

        let request = serde_json::to_string(&CommitRequest {
            ballot,
            last_executed: min_last_executed,
            global_last_executed,
            sender: self.id,
        }).unwrap();
        let (channel_id, mut response_recv) = self.add_channel(num_peers).await;
        self.peers.iter().for_each(|peer| {
            if peer.id != self.id {
                let request = request.clone();
                let stub = peer.stub.clone();
                responses.spawn(async move {
                    stub.send_await_response(
                        MessageType::CommitRequest, channel_id, &request).await;
                });
                info!("{} sent commit request to {}", self.id, peer.id);
            }
        });

        loop {
            let response_result = response_recv.recv().await.unwrap();
            let commit_response: CommitResponse =
                serde_json::from_str(&response_result).unwrap();
            if commit_response.r#type == ResponseType::Ok as i32 {
                num_oks += 1;
                if commit_response.last_executed < min_last_executed {
                    min_last_executed = commit_response.last_executed;
                }
            } else {
                if commit_response.ballot > self.ballot() {
                    self.become_follower(commit_response.ballot);
                    break;
                }
            }
            if num_oks == num_peers {
                self.remove_channel(channel_id, response_recv);
                return min_last_executed;
            }
        }
        self.remove_channel(channel_id, response_recv);
        global_last_executed
    }

    async fn replay(&self, ballot: i64, log: HashMap<i64, Instance>) {
        for (_, instance) in log {
            let command = instance.command.unwrap();
            let mut r;
            loop {
                r = self
                    .run_accept_phase(
                        ballot,
                        instance.index,
                        &command,
                        instance.client_id,
                    )
                    .await;
                if r != ResultType::Retry {
                    break;
                }
            }
            if let ResultType::SomeoneElseLeader(_) = r {
                return;
            }
        }
    }

    async fn add_channel(&self,
                         num_peers: usize
    ) -> (u64, mpsc::Receiver<String>) {
        let (response_send, response_recv) = mpsc::channel(num_peers);
        let channel_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let mut channels = self.channels.lock().await;
        channels.insert(channel_id, response_send);
        (channel_id, response_recv)
    }

    async fn remove_channel(&self, channel_id: u64,
                            mut response_recv: mpsc::Receiver<String>
    ) {
        let mut channels = self.channels.lock().await;
        channels.remove(&channel_id);
        response_recv.close();
    }
}

pub struct MultiPaxos {
    inner: Arc<MultiPaxosInner>,
}

pub struct MultiPaxosHandle {
    prepare_task_handle: JoinHandle<()>,
    commit_task_handle: JoinHandle<()>,
}

impl MultiPaxos {
    pub async fn new(log: Arc<Log>, config: &json) -> Self {
        Self {
            inner: Arc::new(
                MultiPaxosInner::new(log, config).await),
        }
    }

    pub fn start(&self) -> MultiPaxosHandle {
        MultiPaxosHandle {
            prepare_task_handle: self.start_prepare_task(),
            commit_task_handle: self.start_commit_task(),
        }
    }

    pub async fn stop(&self, handle: MultiPaxosHandle) {
        self.stop_commit_task(handle.commit_task_handle).await;
        self.stop_prepare_task(handle.prepare_task_handle).await;
    }

    fn start_prepare_task(&self) -> JoinHandle<()> {
        info!("{} starting prepare task", self.inner.id);
        self.inner
            .prepare_task_running
            .store(true, Ordering::Relaxed);
        let inner = self.inner.clone();
        tokio::spawn(async move {
            inner.became_follower.notify_one();
            inner.prepare_task_fn().await;
        })
    }

    async fn stop_prepare_task(&self, handle: JoinHandle<()>) {
        info!("{} stopping prepare task", self.inner.id);
        assert!(self.inner.prepare_task_running.load(Ordering::Relaxed));
        self.inner
            .prepare_task_running
            .store(false, Ordering::Relaxed);
        self.inner.became_follower.notify_one();
        handle.await.unwrap();
    }

    fn start_commit_task(&self) -> JoinHandle<()> {
        self.inner
            .commit_task_running
            .store(true, Ordering::Relaxed);
        let inner = self.inner.clone();
        tokio::spawn(async move {
            inner.commit_task_fn().await;
        })
    }

    async fn stop_commit_task(&self, handle: JoinHandle<()>) {
        info!("{} stopping commit task", self.inner.id);
        assert!(self.inner.commit_task_running.load(Ordering::Relaxed));
        self.inner
            .commit_task_running
            .store(false, Ordering::Relaxed);
        self.inner.became_leader.notify_one();
        handle.await.unwrap();
    }

    pub async fn replicate(
        &self,
        command: &Command,
        client_id: i64,
    ) -> ResultType {
        let ballot = self.inner.ballot();
        if is_leader(ballot, self.inner.id) {
            return self
                .inner
                .run_accept_phase(
                    ballot,
                    self.inner.log.advance_last_index(),
                    command,
                    client_id,
                )
                .await;
        }
        if is_someone_else_leader(ballot, self.inner.id) {
            return ResultType::SomeoneElseLeader(extract_leader(ballot));
        }
        ResultType::Retry
    }

    pub async fn prepare(&self, request: PrepareRequest) -> PrepareResponse {
        info!("{} <--prepare-- {}", self.inner.id, request.sender);

        if request.ballot > self.inner.ballot() {
            self.inner.become_follower(request.ballot);
            return PrepareResponse {
                r#type: ResponseType::Ok as i32,
                ballot: self.inner.ballot(),
                instances: self.inner.log.instances(),
            };
        }
        PrepareResponse {
            r#type: ResponseType::Reject as i32,
            ballot: self.inner.ballot(),
            instances: vec![],
        }
    }

    pub async fn accept(&self, request: AcceptRequest) -> AcceptResponse {
        info!("{} <--accept--- {}", self.inner.id, request.sender);

        let mut response = AcceptResponse {
            r#type: ResponseType::Ok as i32,
            ballot: self.inner.ballot(),
        };
        let instance = request.instance.unwrap();
        let instance_ballot = instance.ballot;
        if instance.ballot >= self.inner.ballot() {
            self.inner.log.append(instance);
            if instance_ballot > self.inner.ballot() {
                self.inner.become_follower(instance_ballot);
            }
        }
        if instance_ballot < self.inner.ballot() {
            response.ballot = self.inner.ballot();
            response.r#type = ResponseType::Reject as i32;
        }
        response
    }

    pub async fn commit(&self, request: CommitRequest) -> CommitResponse {
        info!("{} <--commit--- {}", self.inner.id, request.sender);

        if request.ballot >= self.inner.ballot() {
            self.inner.commit_received.store(true, Ordering::Relaxed);
            self.inner
                .log
                .commit_until(request.last_executed, request.ballot);
            self.inner.log.trim_until(request.global_last_executed);
            if request.ballot > self.inner.ballot() {
                self.inner.become_follower(request.ballot);
            }
            return CommitResponse {
                r#type: ResponseType::Ok as i32,
                ballot: self.inner.ballot(),
                last_executed: self.inner.log.last_executed(),
            };
        }
        CommitResponse {
            r#type: ResponseType::Reject as i32,
            ballot: self.inner.ballot(),
            last_executed: 0,
        }
    }

    fn next_ballot(&self) -> i64 {
        self.inner.next_ballot()
    }

    fn become_leader(&self, new_ballot: i64, new_last_index: i64) {
        self.inner.become_leader(new_ballot, new_last_index);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

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

    fn is_leader(peer: &MultiPaxos) -> bool {
        let ballot = peer.inner.ballot();
        super::is_leader(ballot, peer.inner.id)
    }

    fn leader(peer: &MultiPaxos) -> i64 {
        let ballot = peer.inner.ballot();
        super::extract_leader(ballot)
    }

    async fn send_prepare(
        stub: &mut MultiPaxosRpcClient<Channel>,
        ballot: i64,
    ) -> PrepareResponse {
        let request = Request::new(PrepareRequest { ballot, sender: 0 });
        let r = stub.prepare(request).await;
        r.unwrap().into_inner()
    }

    async fn send_accept(
        stub: &mut MultiPaxosRpcClient<Channel>,
        instance: Instance,
    ) -> AcceptResponse {
        let request = Request::new(AcceptRequest {
            instance: Some(instance),
            sender: 0,
        });
        let r = stub.accept(request).await;
        r.unwrap().into_inner()
    }

    async fn send_commit(
        stub: &mut MultiPaxosRpcClient<Channel>,
        ballot: i64,
        last_executed: i64,
        global_last_executed: i64,
    ) -> CommitResponse {
        let request = Request::new(CommitRequest {
            ballot,
            last_executed,
            global_last_executed,
            sender: 0,
        });
        let r = stub.commit(request).await;
        r.unwrap().into_inner()
    }

    fn init() -> (json, MultiPaxos, MultiPaxos, MultiPaxos) {
        let _ = env_logger::builder().is_test(true).try_init();
        let config = make_config(0, NUM_PEERS);
        let peer0 = MultiPaxos::new(
            Arc::new(Log::new(Box::new(MemKVStore::new()))),
            &config,
        );
        let peer1 = MultiPaxos::new(
            Arc::new(Log::new(Box::new(MemKVStore::new()))),
            &make_config(1, NUM_PEERS),
        );
        let peer2 = MultiPaxos::new(
            Arc::new(Log::new(Box::new(MemKVStore::new()))),
            &make_config(2, NUM_PEERS),
        );

        (config, peer0, peer1, peer2)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn next_ballot() {
        let (_, peer0, peer1, peer2) = init();

        let id = 0;
        let ballot = id + ROUND_INCREMENT;
        assert_eq!(ballot, peer0.next_ballot());

        let id = 1;
        let ballot = id + ROUND_INCREMENT;
        assert_eq!(ballot, peer1.next_ballot());

        let id = 2;
        let ballot = id + ROUND_INCREMENT;
        assert_eq!(ballot, peer2.next_ballot());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn request_with_lower_ballot_ignored() {
        let (config, peer0, peer1, _) = init();
        let mut stub = make_stub(&config["peers"][0]);

        let handle0 = peer0.start_rpc_server_task();

        peer0.become_leader(peer0.next_ballot(), peer0.inner.log.last_index());
        peer0.become_leader(peer0.next_ballot(), peer0.inner.log.last_index());

        let stale_ballot = peer1.next_ballot();

        let r = send_prepare(&mut stub, stale_ballot).await;
        assert_eq!(ResponseType::Reject as i32, r.r#type);
        assert!(is_leader(&peer0));

        let index = peer0.inner.log.advance_last_index();
        let instance =
            Instance::inprogress(stale_ballot, index, &Command::get(""), 0);

        let r = send_accept(&mut stub, instance).await;
        assert_eq!(ResponseType::Reject as i32, r.r#type);
        assert!(is_leader(&peer0));
        assert_eq!(None, peer0.inner.log.at(index));

        let r = send_commit(&mut stub, stale_ballot, 0, 0).await;
        assert_eq!(ResponseType::Reject as i32, r.r#type);
        assert!(is_leader(&peer0));

        peer0.stop_rpc_server_task(handle0).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn request_with_higher_ballot_change_leader_to_follower() {
        let (config, peer0, peer1, _) = init();
        let mut stub = make_stub(&config["peers"][0]);

        let handle0 = peer0.start_rpc_server_task();

        peer0.become_leader(peer0.next_ballot(), peer0.inner.log.last_index());
        assert!(is_leader(&peer0));

        let r = send_prepare(&mut stub, peer1.next_ballot()).await;
        assert_eq!(ResponseType::Ok as i32, r.r#type, );
        assert!(!is_leader(&peer0));
        assert_eq!(1, leader(&peer0));

        peer1.become_leader(peer1.next_ballot(), peer1.inner.log.last_index());

        peer0.become_leader(peer0.next_ballot(), peer0.inner.log.last_index());
        assert!(is_leader(&peer0));
        let index = peer0.inner.log.advance_last_index();
        let instance = Instance::inprogress_get(peer1.next_ballot(), index);
        let r = send_accept(&mut stub, instance).await;
        assert_eq!(ResponseType::Ok as i32, r.r#type);

        peer1.become_leader(peer1.next_ballot(), peer1.inner.log.last_index());

        peer0.become_leader(peer0.next_ballot(), peer0.inner.log.last_index());
        assert!(is_leader(&peer0));
        let r = send_commit(&mut stub, peer1.next_ballot(), 0, 0).await;
        assert_eq!(ResponseType::Ok as i32, r.r#type);
        assert!(!is_leader(&peer0));
        assert_eq!(1, leader(&peer0));

        peer0.stop_rpc_server_task(handle0).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn commit_commits_and_trims() {
        let (config, peer0, _, _) = init();
        let mut stub = make_stub(&config["peers"][0]);

        let handle0 = peer0.start_rpc_server_task();

        let ballot = peer0.next_ballot();
        let index1 = peer0.inner.log.advance_last_index();
        peer0
            .inner
            .log
            .append(Instance::inprogress_get(ballot, index1));
        let index2 = peer0.inner.log.advance_last_index();
        peer0
            .inner
            .log
            .append(Instance::inprogress_get(ballot, index2));
        let index3 = peer0.inner.log.advance_last_index();
        peer0
            .inner
            .log
            .append(Instance::inprogress_get(ballot, index3));

        let r = send_commit(&mut stub, ballot, index2, 0).await;
        assert_eq!(ResponseType::Ok as i32, r.r#type);
        assert_eq!(0, r.last_executed);
        assert!(peer0.inner.log.at(index1).unwrap().is_committed());
        assert!(peer0.inner.log.at(index2).unwrap().is_committed());
        assert!(peer0.inner.log.at(index3).unwrap().is_inprogress());

        peer0.inner.log.execute().await;
        peer0.inner.log.execute().await;

        let r = send_commit(&mut stub, ballot, index2, index2).await;
        assert_eq!(ResponseType::Ok as i32, r.r#type);
        assert_eq!(index2, r.last_executed);
        assert_eq!(None, peer0.inner.log.at(index1));
        assert_eq!(None, peer0.inner.log.at(index2));
        assert!(peer0.inner.log.at(index3).unwrap().is_inprogress());

        peer0.stop_rpc_server_task(handle0).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn prepare_responds_with_correct_instances() {
        let (config, peer0, _, _) = init();
        let mut stub = make_stub(&config["peers"][0]);

        let handle0 = peer0.start_rpc_server_task();

        let ballot = peer0.next_ballot();

        let index1 = peer0.inner.log.advance_last_index();
        let instance1 = Instance::inprogress_get(ballot, index1);
        peer0.inner.log.append(instance1.clone());

        let index2 = peer0.inner.log.advance_last_index();
        let instance2 = Instance::inprogress_get(ballot, index2);
        peer0.inner.log.append(instance2.clone());

        let index3 = peer0.inner.log.advance_last_index();
        let instance3 = Instance::inprogress_get(ballot, index3);
        peer0.inner.log.append(instance3.clone());

        let r = send_prepare(&mut stub, ballot).await;
        assert_eq!(ResponseType::Ok as i32, r.r#type);
        assert_eq!(3, r.instances.len());
        assert_eq!(instance1, r.instances[0]);
        assert_eq!(instance2, r.instances[1]);
        assert_eq!(instance3, r.instances[2]);

        let r = send_commit(&mut stub, ballot, index2, 0).await;
        assert_eq!(ResponseType::Ok as i32, r.r#type);

        peer0.inner.log.execute().await;
        peer0.inner.log.execute().await;

        let ballot = peer0.next_ballot();

        let r = send_prepare(&mut stub, ballot).await;
        assert_eq!(ResponseType::Ok as i32, r.r#type);
        assert_eq!(3, r.instances.len());
        assert!(r.instances[0].is_executed());
        assert!(r.instances[1].is_executed());
        assert!(r.instances[2].is_inprogress());

        let r = send_commit(&mut stub, ballot, index2, 2).await;
        assert_eq!(ResponseType::Ok as i32, r.r#type);

        let ballot = peer0.next_ballot();

        let r = send_prepare(&mut stub, ballot).await;
        assert_eq!(ResponseType::Ok as i32, r.r#type);
        assert_eq!(1, r.instances.len());
        assert_eq!(instance3, r.instances[0]);

        peer0.stop_rpc_server_task(handle0).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn accept_appends_to_log() {
        let (config, peer0, _, _) = init();
        let mut stub = make_stub(&config["peers"][0]);

        let handle0 = peer0.start_rpc_server_task();

        let ballot = peer0.next_ballot();

        let index1 = peer0.inner.log.advance_last_index();
        let instance1 = Instance::inprogress_get(ballot, index1);

        let index2 = peer0.inner.log.advance_last_index();
        let instance2 = Instance::inprogress_get(ballot, index2);

        let r = send_accept(&mut stub, instance1.clone()).await;
        assert_eq!(ResponseType::Ok as i32, r.r#type);
        assert_eq!(instance1, peer0.inner.log.at(index1).unwrap());
        assert_eq!(None, peer0.inner.log.at(index2));

        let r = send_accept(&mut stub, instance2.clone()).await;
        assert_eq!(ResponseType::Ok as i32, r.r#type);
        assert_eq!(instance1, peer0.inner.log.at(index1).unwrap());
        assert_eq!(instance2, peer0.inner.log.at(index2).unwrap());

        peer0.stop_rpc_server_task(handle0).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn prepare_response_with_higher_ballot_changes_leader_to_follower() {
        let (config, peer0, peer1, peer2) = init();
        let mut stub1 = make_stub(&config["peers"][1]);

        let handle0 = peer0.start_rpc_server_task();
        let handle1 = peer1.start_rpc_server_task();
        let handle2 = peer2.start_rpc_server_task();

        let mut peer0_ballot = peer0.next_ballot();
        peer0.become_leader(peer0_ballot, peer0.inner.log.last_index());
        peer1.become_leader(peer1.next_ballot(), peer1.inner.log.last_index());
        let mut peer2_ballot = peer2.next_ballot();
        peer2.become_leader(peer2_ballot, peer2.inner.log.last_index());
        peer2_ballot = peer2.next_ballot();
        peer2.become_leader(peer2_ballot, peer2.inner.log.last_index());

        let r = send_commit(&mut stub1, peer2_ballot, 0, 0).await;
        assert_eq!(ResponseType::Ok as i32, r.r#type);
        assert!(!is_leader(&peer1));
        assert_eq!(2, leader(&peer1));

        assert!(is_leader(&peer0));
        peer0_ballot = peer0.next_ballot();
        peer0.inner.run_prepare_phase(peer0_ballot).await;
        assert!(!is_leader(&peer0));
        assert_eq!(2, leader(&peer0));

        peer0.stop_rpc_server_task(handle0).await;
        peer1.stop_rpc_server_task(handle1).await;
        peer2.stop_rpc_server_task(handle2).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn accept_response_with_higher_ballot_changes_leader_to_follower() {
        let (config, peer0, peer1, peer2) = init();
        let mut stub1 = make_stub(&config["peers"][1]);

        let handle0 = peer0.start_rpc_server_task();
        let handle1 = peer1.start_rpc_server_task();
        let handle2 = peer2.start_rpc_server_task();

        let peer0_ballot = peer0.next_ballot();
        peer0.become_leader(peer0_ballot, peer0.inner.log.last_index());
        peer1.become_leader(peer1.next_ballot(), peer1.inner.log.last_index());
        let peer2_ballot = peer2.next_ballot();
        peer2.become_leader(peer2_ballot, peer2.inner.log.last_index());

        let r = send_commit(&mut stub1, peer2_ballot, 0, 0).await;
        assert_eq!(ResponseType::Ok as i32, r.r#type);
        assert!(!is_leader(&peer1));
        assert_eq!(2, leader(&peer1));

        assert!(is_leader(&peer0));
        peer0
            .inner
            .run_accept_phase(peer0_ballot, 1, &Command::get(""), 0)
            .await;
        assert!(!is_leader(&peer0));
        assert_eq!(2, leader(&peer0));

        peer0.stop_rpc_server_task(handle0).await;
        peer1.stop_rpc_server_task(handle1).await;
        peer2.stop_rpc_server_task(handle2).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn commit_response_with_higher_ballot_changes_leader_to_follower() {
        let (config, peer0, peer1, peer2) = init();
        let mut stub1 = make_stub(&config["peers"][1]);

        let handle0 = peer0.start_rpc_server_task();
        let handle1 = peer1.start_rpc_server_task();
        let handle2 = peer2.start_rpc_server_task();

        let peer0_ballot = peer0.next_ballot();
        peer0.become_leader(peer0_ballot, peer0.inner.log.last_index());
        peer1.become_leader(peer1.next_ballot(), peer1.inner.log.last_index());
        let peer2_ballot = peer2.next_ballot();
        peer2.become_leader(peer2_ballot, peer2.inner.log.last_index());

        let r = send_commit(&mut stub1, peer2_ballot, 0, 0).await;
        assert_eq!(ResponseType::Ok as i32, r.r#type);
        assert!(!is_leader(&peer1));
        assert_eq!(2, leader(&peer1));

        assert!(is_leader(&peer0));
        peer0.inner.run_commit_phase(peer0_ballot, 0).await;
        assert!(!is_leader(&peer0));
        assert_eq!(2, leader(&peer0));

        peer0.stop_rpc_server_task(handle0).await;
        peer1.stop_rpc_server_task(handle1).await;
        peer2.stop_rpc_server_task(handle2).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn run_prepare_phase() {
        let (_, peer0, peer1, _) = init();

        let handle0 = peer0.start_rpc_server_task();

        let peer0_ballot = peer0.next_ballot();
        peer0.become_leader(peer0_ballot, peer0.inner.log.last_index());
        let peer1_ballot = peer1.next_ballot();
        peer1.become_leader(peer1_ballot, peer1.inner.log.last_index());

        let index1 = 1;
        let i1 = Instance::inprogress_put(peer0_ballot, index1);

        peer0.inner.log.append(i1.clone());
        peer1.inner.log.append(i1.clone());

        let index2 = 2;
        let i2 = Instance::inprogress_get(peer0_ballot, index2);

        peer1.inner.log.append(i2.clone());

        let index3 = 3;
        let peer0_i3 = Instance::committed_del(peer0_ballot, index3);
        let peer1_i3 = Instance::inprogress_del(peer1_ballot, index3);

        peer0.inner.log.append(peer0_i3.clone());
        peer1.inner.log.append(peer1_i3.clone());

        let index4 = 4;
        let peer0_i4 = Instance::executed_del(peer0_ballot, index4);
        let peer1_i4 = Instance::inprogress_del(peer1_ballot, index4);

        peer0.inner.log.append(peer0_i4.clone());
        peer1.inner.log.append(peer1_i4.clone());

        let index5 = 5;
        let peer0_ballot = peer0.next_ballot();
        peer0.become_leader(peer0_ballot, peer0.inner.log.last_index());
        let peer1_ballot = peer1.next_ballot();
        peer1.become_leader(peer1_ballot, peer1.inner.log.last_index());

        let peer0_i5 = Instance::inprogress_get(peer0_ballot, index5);
        let peer1_i5 = Instance::inprogress_put(peer1_ballot, index5);

        peer0.inner.log.append(peer0_i5.clone());
        peer1.inner.log.append(peer1_i5.clone());

        let ballot = peer0.next_ballot();

        assert_eq!(None, peer0.inner.run_prepare_phase(ballot).await);

        let handle1 = peer1.start_rpc_server_task();

        sleep(Duration::from_secs(2)).await;

        let ballot = peer0.next_ballot();

        let (last_index, log) =
            peer0.inner.run_prepare_phase(ballot).await.unwrap();

        assert_eq!(index5, last_index);
        assert_eq!(&i1, log.get(&index1).unwrap());
        assert_eq!(&i2, log.get(&index2).unwrap());
        assert_eq!(peer0_i3.command, log.get(&index3).unwrap().command);
        assert_eq!(peer0_i4.command, log.get(&index4).unwrap().command);
        assert_eq!(&peer1_i5, log.get(&index5).unwrap());

        peer0.stop_rpc_server_task(handle0).await;
        peer1.stop_rpc_server_task(handle1).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn run_accept_phase() {
        let (_, peer0, peer1, peer2) = init();

        let handle0 = peer0.start_rpc_server_task();

        let ballot = peer0.next_ballot();
        peer0.become_leader(ballot, peer0.inner.log.last_index());
        let index = peer0.inner.log.advance_last_index();

        let result = peer0
            .inner
            .run_accept_phase(ballot, index, &Command::get(""), 0)
            .await;

        assert_eq!(ResultType::Retry, result);

        assert!(peer0.inner.log.at(index).unwrap().is_inprogress());
        assert_eq!(None, peer1.inner.log.at(index));
        assert_eq!(None, peer2.inner.log.at(index));

        let handle1 = peer1.start_rpc_server_task();

        sleep(Duration::from_secs(2)).await;

        let result = peer0
            .inner
            .run_accept_phase(ballot, index, &Command::get(""), 0)
            .await;

        assert_eq!(ResultType::Ok, result);

        assert!(peer0.inner.log.at(index).unwrap().is_committed());
        assert!(peer1.inner.log.at(index).unwrap().is_inprogress());
        assert_eq!(None, peer2.inner.log.at(index));

        peer0.stop_rpc_server_task(handle0).await;
        peer1.stop_rpc_server_task(handle1).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn run_commit_phase() {
        let (_, peer0, peer1, peer2) = init();

        let handle0 = peer0.start_rpc_server_task();
        let handle1 = peer1.start_rpc_server_task();

        let peers = vec![&peer0, &peer1, &peer2];

        let ballot = peer0.next_ballot();

        for index in 1..=3 {
            for peer in 0..NUM_PEERS {
                if index == 3 && peer == 2 {
                    continue;
                }
                let peer = peers[peer as usize];
                peer.inner
                    .log
                    .append(Instance::committed_get(ballot, index));
                peer.inner.log.execute().await;
            }
        }

        let gle = 0;
        let gle = peer0.inner.run_commit_phase(ballot, gle).await;
        assert_eq!(0, gle);

        let handle2 = peer2.start_rpc_server_task();

        peer2.inner.log.append(Instance::inprogress_get(ballot, 3));

        sleep(Duration::from_secs(2)).await;

        let gle = peer0.inner.run_commit_phase(ballot, gle).await;
        assert_eq!(2, gle);

        peer2.inner.log.execute().await;

        let gle = peer0.inner.run_commit_phase(ballot, gle).await;
        assert_eq!(3, gle);

        peer0.stop_rpc_server_task(handle0).await;
        peer1.stop_rpc_server_task(handle1).await;
        peer2.stop_rpc_server_task(handle2).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn replay() {
        let (_, peer0, peer1, _) = init();

        let handle0 = peer0.start_rpc_server_task();
        let handle1 = peer1.start_rpc_server_task();

        let ballot = peer0.next_ballot();
        peer0.become_leader(ballot, peer0.inner.log.last_index());

        let index1 = 1;
        let mut i1 = Instance::committed_put(ballot, index1);
        let index2 = 2;
        let mut i2 = Instance::executed_get(ballot, index2);
        let index3 = 3;
        let mut i3 = Instance::inprogress_del(ballot, index3);

        let indexes = vec![index1, index2, index3];
        let instances = vec![i1.clone(), i2.clone(), i3.clone()];
        let log: HashMap<_, _> =
            indexes.into_iter().zip(instances.into_iter()).collect();

        assert_eq!(None, peer0.inner.log.at(index1));
        assert_eq!(None, peer0.inner.log.at(index2));
        assert_eq!(None, peer0.inner.log.at(index3));

        assert_eq!(None, peer1.inner.log.at(index1));
        assert_eq!(None, peer1.inner.log.at(index2));
        assert_eq!(None, peer1.inner.log.at(index3));

        let new_ballot = peer0.next_ballot();
        peer0.become_leader(new_ballot, peer0.inner.log.last_index());
        peer0.inner.replay(new_ballot, log).await;

        i1.ballot = new_ballot;
        i2.ballot = new_ballot;
        i3.ballot = new_ballot;

        i1.commit();
        i2.commit();
        i3.commit();

        assert_eq!(i1, peer0.inner.log.at(index1).unwrap());
        assert_eq!(i2, peer0.inner.log.at(index2).unwrap());
        assert_eq!(i3, peer0.inner.log.at(index3).unwrap());

        i1.state = InstanceState::Inprogress as i32;
        i2.state = InstanceState::Inprogress as i32;
        i3.state = InstanceState::Inprogress as i32;

        assert_eq!(i1, peer1.inner.log.at(index1).unwrap());
        assert_eq!(i2, peer1.inner.log.at(index2).unwrap());
        assert_eq!(i3, peer1.inner.log.at(index3).unwrap());

        peer0.stop_rpc_server_task(handle0).await;
        peer1.stop_rpc_server_task(handle1).await;
    }

    fn one_leader(peers: &Vec<&MultiPaxos>) -> Option<i64> {
        let assumed_leader = leader(peers[0]);
        let mut num_leaders = 0;
        for p in peers.iter() {
            if is_leader(p) {
                num_leaders += 1;
                if num_leaders > 1 || p.inner.id != assumed_leader {
                    return None;
                }
            } else if leader(p) != assumed_leader {
                return None;
            }
        }
        Some(assumed_leader)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn replicate() {
        let (config, peer0, peer1, peer2) = init();

        let handle0 = peer0.start();

        sleep(Duration::from_secs(1)).await;

        let command = Command::get("");
        let result = peer0.replicate(&command, 0).await;
        assert_eq!(ResultType::Retry, result);

        let handle1 = peer1.start();
        let handle2 = peer2.start();

        let commit_interval = config["commit_interval"].as_u64().unwrap();
        let commit_interval_3x = 3 * commit_interval;

        sleep(Duration::from_millis(commit_interval_3x)).await;

        let peers = vec![&peer0, &peer1, &peer2];
        let leader = one_leader(&peers);
        assert!(leader.is_some());

        let leader = leader.unwrap() as usize;

        let result = peers[leader].replicate(&command, 0).await;
        assert_eq!(ResultType::Ok, result);

        let nonleader = (leader + 1) % NUM_PEERS as usize;

        let result = peers[nonleader].replicate(&command, 0).await;
        assert!(ResultType::SomeoneElseLeader(leader as i64) == result);

        peer0.stop(handle0).await;
        peer1.stop(handle1).await;
        peer2.stop(handle2).await;
    }
}
