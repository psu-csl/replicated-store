use super::log::{insert, Log, MapLog};
use crate::kvstore::memkvstore::MemKVStore;
use futures::executor::block_on;
use futures_util::stream::FuturesUnordered;
use futures_util::FutureExt;
use futures_util::StreamExt;
use log::debug;
use rand::distributions::{Distribution, Uniform};
use rpc::multi_paxos_rpc_client::MultiPaxosRpcClient;
use rpc::multi_paxos_rpc_server::{MultiPaxosRpc, MultiPaxosRpcServer};
use rpc::{AcceptRequest, AcceptResponse};
use rpc::{Command, Instance, ResponseType};
use rpc::{CommitRequest, CommitResponse};
use rpc::{PrepareRequest, PrepareResponse};
use serde_json::json;
use serde_json::Value as json;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::{thread, time};
use tokio::{sync::oneshot, task};
use tonic::transport::{Channel, Endpoint, Server};
use tonic::{Request, Response, Status};

pub mod rpc {
    tonic::include_proto!("multipaxos");
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

#[derive(PartialEq)]
enum ResultType {
    Ok,
    Retry,
    SomeoneElseLeader(i64),
}

struct RpcPeer {
    id: i64,
    stub: MultiPaxosRpcClient<Channel>,
}

struct MultiPaxosInner {
    ballot: Mutex<i64>,
    ready: AtomicBool,
    log: Log,
    id: i64,
    commit_received: AtomicBool,
    commit_interval: u64,
    port: SocketAddr,
    rpc_peers: Mutex<Vec<RpcPeer>>,
    prepare_thread_running: AtomicBool,
    commit_thread_running: AtomicBool,
    cv_leader: Condvar,
    cv_follower: Condvar,
}

fn make_stub(port: &json) -> MultiPaxosRpcClient<Channel> {
    let address = format!("http://{}", port.as_str().unwrap());
    let channel = Endpoint::from_shared(address).unwrap().connect_lazy();
    MultiPaxosRpcClient::new(channel)
}

impl MultiPaxosInner {
    fn new(log: Log, config: &json) -> Self {
        let commit_interval = config["commit_interval"].as_u64().unwrap();
        let id = config["id"].as_i64().unwrap();
        let port = config["peers"][id as usize]
            .as_str()
            .unwrap()
            .parse()
            .unwrap();

        let mut rpc_peers = Vec::new();
        for (id, port) in config["peers"].as_array().unwrap().iter().enumerate()
        {
            rpc_peers.push(RpcPeer {
                id: id as i64,
                stub: make_stub(port),
            });
        }
        Self {
            ballot: Mutex::new(0),
            ready: AtomicBool::new(false),
            log: log,
            id: id,
            commit_received: AtomicBool::new(false),
            commit_interval: commit_interval,
            port: port,
            rpc_peers: Mutex::new(Vec::new()),
            prepare_thread_running: AtomicBool::new(false),
            commit_thread_running: AtomicBool::new(false),
            cv_leader: Condvar::new(),
            cv_follower: Condvar::new(),
        }
    }

    fn next_ballot(&self) -> i64 {
        let ballot = self.ballot.lock().unwrap();
        let mut next_ballot = *ballot;
        next_ballot += ROUND_INCREMENT;
        next_ballot = (next_ballot & !ID_BITS) | self.id;
        next_ballot
    }

    fn become_leader(&self, next_ballot: i64) {
        let mut ballot = self.ballot.lock().unwrap();
        debug!(
            "{} became a leader: ballot: {} -> {}",
            self.id, *ballot, next_ballot
        );
        *ballot = next_ballot;
        self.ready.store(false, Ordering::Relaxed);
        self.cv_leader.notify_one();
    }

    fn become_follower(&self, ballot: &mut i64, next_ballot: i64) {
        let prev_leader = leader(*ballot);
        let next_leader = leader(next_ballot);
        if next_leader != self.id
            && (prev_leader == self.id || prev_leader == MAX_NUM_PEERS)
        {
            debug!(
                "{} became a follower: ballot: {} -> {}",
                self.id, *ballot, next_ballot
            );
            self.cv_follower.notify_one();
        }
        *ballot = next_ballot;
    }

    fn ballot(&self) -> (i64, bool) {
        let ballot = self.ballot.lock().unwrap();
        (*ballot, self.ready.load(Ordering::Relaxed))
    }

    fn wait_until_leader(&self) {
        let mut ballot = self.ballot.lock().unwrap();
        while self.commit_thread_running.load(Ordering::Relaxed)
            && !is_leader(*ballot, self.id)
        {
            ballot = self.cv_leader.wait(ballot).unwrap();
        }
    }

    fn wait_until_follower(&self) {
        let mut ballot = self.ballot.lock().unwrap();
        while self.prepare_thread_running.load(Ordering::Relaxed)
            && is_leader(*ballot, self.id)
        {
            ballot = self.cv_follower.wait(ballot).unwrap();
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
                    self.replay(next_ballot, log);
                    break;
                }
            }
        }
    }

    fn commit_thread_fn(&self) {
        while self.commit_thread_running.load(Ordering::Relaxed) {
            self.wait_until_leader();
            let mut gle = self.log.global_last_executed();
            while self.commit_thread_running.load(Ordering::Relaxed) {
                let (ballot, _) = self.ballot();
                if !is_leader(ballot, self.id) {
                    break;
                }
                gle = self.run_commit_phase(ballot, gle);
                self.sleep_for_commit_interval();
            }
        }
    }

    fn run_prepare_phase(&self, ballot: i64) -> Option<MapLog> {
        let mut peers = self.rpc_peers.lock().unwrap();
        let num_peers = peers.len();
        let mut responses = FuturesUnordered::new();
        for peer in peers.iter_mut() {
            let request = Request::new(PrepareRequest {
                ballot: ballot,
                sender: self.id,
            });
            responses.push(peer.stub.prepare(request));
            debug!("{} sent prepare request to {}", self.id, peer.id);
        }
        let mut num_oks = 0;
        let mut log = MapLog::new();

        block_on(async {
            while let Some(response) = responses.next().await {
                match response {
                    Ok(response) => {
                        let response = response.into_inner();
                        if response.r#type == ResponseType::Ok as i32 {
                            num_oks += 1;
                            for instance in response.instances {
                                insert(&mut log, instance);
                            }
                        } else {
                            let mut ballot = self.ballot.lock().unwrap();
                            if response.ballot > *ballot {
                                self.become_follower(
                                    &mut *ballot,
                                    response.ballot,
                                );
                                break;
                            }
                        }
                    }
                    Err(_) => (),
                }
                if num_oks > num_peers / 2 {
                    return Some(log);
                }
            }
            None
        })
    }

    fn run_accept_phase(
        &self,
        ballot: i64,
        index: i64,
        command: &Command,
        client_id: i64,
    ) -> ResultType {
        let mut peers = self.rpc_peers.lock().unwrap();
        let num_peers = peers.len();
        let mut responses = FuturesUnordered::new();

        for peer in peers.iter_mut() {
            let request = Request::new(AcceptRequest {
                instance: Some(Instance::inprogress(
                    ballot, index, command, client_id,
                )),
                sender: self.id,
            });
            responses.push(peer.stub.accept(request));
            debug!("{} sent accept request to {}", self.id, peer.id);
        }

        let mut num_oks = 0;
        let mut current_leader = self.id;

        block_on(async {
            while let Some(response) = responses.next().await {
                match response {
                    Ok(response) => {
                        let response = response.into_inner();
                        if response.r#type == ResponseType::Ok as i32 {
                            num_oks += 1;
                        } else {
                            let mut ballot = self.ballot.lock().unwrap();
                            if response.ballot > *ballot {
                                self.become_follower(
                                    &mut *ballot,
                                    response.ballot,
                                );
                                current_leader = leader(*ballot);
                                break;
                            }
                        }
                    }
                    Err(_) => (),
                }
                if num_oks > num_peers / 2 {
                    self.log.commit(index);
                    return ResultType::Ok;
                }
            }
            if current_leader != self.id {
                return ResultType::SomeoneElseLeader(current_leader);
            }
            ResultType::Retry
        })
    }

    fn run_commit_phase(&self, ballot: i64, global_last_executed: i64) -> i64 {
        let mut peers = self.rpc_peers.lock().unwrap();
        let num_peers = peers.len();
        let mut responses = FuturesUnordered::new();
        let mut min_last_executed = self.log.last_executed();

        for peer in peers.iter_mut() {
            let request = Request::new(CommitRequest {
                ballot: ballot,
                last_executed: min_last_executed,
                global_last_executed: global_last_executed,
                sender: self.id,
            });
            responses.push(peer.stub.commit(request));
            debug!("{} sent commit request to {}", self.id, peer.id);
        }

        let mut num_oks = 0;

        block_on(async {
            while let Some(response) = responses.next().await {
                match response {
                    Ok(response) => {
                        let response = response.into_inner();
                        if response.r#type == ResponseType::Ok as i32 {
                            num_oks += 1;
                            if response.last_executed < min_last_executed {
                                min_last_executed = response.last_executed;
                            }
                        } else {
                            let mut ballot = self.ballot.lock().unwrap();
                            if response.ballot > *ballot {
                                self.become_follower(
                                    &mut *ballot,
                                    response.ballot,
                                );
                                break;
                            }
                        }
                    }
                    Err(_) => (),
                }
                if num_oks == num_peers {
                    return min_last_executed;
                }
            }
            global_last_executed
        })
    }

    fn replay(&self, ballot: i64, log: MapLog) {
        for (_, instance) in log {
            let command = instance.command.unwrap();
            let mut r;
            loop {
                r = self.run_accept_phase(
                    ballot,
                    instance.index,
                    &command,
                    instance.client_id,
                );
                if r != ResultType::Retry {
                    break;
                }
            }
            if let ResultType::SomeoneElseLeader(_) = r {
                return;
            }
        }
        self.ready.store(true, Ordering::Relaxed);
        debug!("{} leader is ready to serve", self.id);
    }
}

struct RpcWrapper(Arc<MultiPaxosInner>);

#[tonic::async_trait]
impl MultiPaxosRpc for RpcWrapper {
    async fn prepare(
        &self,
        request: Request<PrepareRequest>,
    ) -> Result<Response<PrepareResponse>, Status> {
        let request = request.into_inner();
        debug!("{} <--prepare-- ", request.sender);

        let mut ballot = self.0.ballot.lock().unwrap();
        if request.ballot > *ballot {
            self.0.become_follower(&mut *ballot, request.ballot);
            return Ok(Response::new(PrepareResponse {
                r#type: ResponseType::Ok as i32,
                ballot: *ballot,
                instances: self.0.log.instances(),
            }));
        }
        Ok(Response::new(PrepareResponse {
            r#type: ResponseType::Reject as i32,
            ballot: *ballot,
            instances: vec![],
        }))
    }

    async fn accept(
        &self,
        request: Request<AcceptRequest>,
    ) -> Result<Response<AcceptResponse>, Status> {
        let request = request.into_inner();
        debug!("{} <--accept---", request.sender);

        let instance = request.instance.unwrap();
        let mut ballot = self.0.ballot.lock().unwrap();
        if instance.ballot >= *ballot {
            self.0.become_follower(&mut *ballot, instance.ballot);
            self.0.log.append(instance);
            return Ok(Response::new(AcceptResponse {
                r#type: ResponseType::Ok as i32,
                ballot: *ballot,
            }));
        }
        Ok(Response::new(AcceptResponse {
            r#type: ResponseType::Reject as i32,
            ballot: *ballot,
        }))
    }

    async fn commit(
        &self,
        request: Request<CommitRequest>,
    ) -> Result<Response<CommitResponse>, Status> {
        let request = request.into_inner();
        debug!("{} <--commit---", request.sender);

        let mut ballot = self.0.ballot.lock().unwrap();
        if request.ballot >= *ballot {
            self.0.commit_received.store(true, Ordering::Relaxed);
            self.0.become_follower(&mut *ballot, request.ballot);
            self.0
                .log
                .commit_until(request.last_executed, request.ballot);
            self.0.log.trim_until(request.global_last_executed);
            return Ok(Response::new(CommitResponse {
                r#type: ResponseType::Ok as i32,
                ballot: *ballot,
                last_executed: self.0.log.last_executed(),
            }));
        }
        Ok(Response::new(CommitResponse {
            r#type: ResponseType::Reject as i32,
            ballot: *ballot,
            last_executed: 0,
        }))
    }
}

pub struct MultiPaxos {
    multi_paxos: Arc<MultiPaxosInner>,
    rpc_server_tx: Option<oneshot::Sender<()>>,
    prepare_thread: Option<thread::JoinHandle<()>>,
    commit_thread: Option<thread::JoinHandle<()>>,
}

impl MultiPaxos {
    pub fn new(log: Log, config: &json) -> Self {
        Self {
            multi_paxos: Arc::new(MultiPaxosInner::new(log, &config)),
            rpc_server_tx: None,
            prepare_thread: None,
            commit_thread: None,
        }
    }

    pub fn start(&mut self) {
        self.start_prepare_thread();
        self.start_commit_thread();
        self.start_rpc_server();
    }

    pub fn stop(&mut self) {
        self.stop_rpc_server();
        self.stop_commit_thread();
        self.stop_prepare_thread();
    }

    fn start_rpc_server(&mut self) {
        debug!(
            "{} starting rpc server at {}",
            self.multi_paxos.id, self.multi_paxos.port
        );

        let (tx, rx) = oneshot::channel();
        self.rpc_server_tx.replace(tx);

        let port = self.multi_paxos.port;
        let service =
            MultiPaxosRpcServer::new(RpcWrapper(Arc::clone(&self.multi_paxos)));
        tokio::spawn(async move {
            Server::builder()
                .add_service(service)
                .serve_with_shutdown(port, rx.map(drop))
                .await
                .unwrap();
        });
    }

    fn stop_rpc_server(&mut self) {
        debug!(
            "{} stopping rpc server at {}",
            self.multi_paxos.id, self.multi_paxos.port
        );
        self.rpc_server_tx.take().unwrap().send(()).unwrap();
    }

    fn start_prepare_thread(&mut self) {
        debug!("{} starting prepare thread", self.multi_paxos.id);
        self.multi_paxos
            .prepare_thread_running
            .store(true, Ordering::Relaxed);
        self.prepare_thread = {
            let multi_paxos = self.multi_paxos.clone();
            Some(std::thread::spawn(move || multi_paxos.prepare_thread_fn()))
        }
    }

    fn stop_prepare_thread(&mut self) {
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

    fn start_commit_thread(&mut self) {
        debug!("{} starting commit thread", self.multi_paxos.id);
        self.multi_paxos
            .commit_thread_running
            .store(true, Ordering::Relaxed);
        self.commit_thread = {
            let multi_paxos = self.multi_paxos.clone();
            Some(std::thread::spawn(move || multi_paxos.commit_thread_fn()))
        }
    }

    fn stop_commit_thread(&mut self) {
        debug!("{} stopping commit thread", self.multi_paxos.id);
        assert!(self
            .multi_paxos
            .commit_thread_running
            .load(Ordering::Relaxed));
        self.multi_paxos
            .commit_thread_running
            .store(false, Ordering::Relaxed);
        self.multi_paxos.cv_leader.notify_one();
        self.commit_thread.take().unwrap().join().unwrap();
    }

    pub fn replicate(&self, command: &Command, client_id: i64) -> ResultType {
        let (ballot, ready) = self.multi_paxos.ballot();
        if is_leader(ballot, self.multi_paxos.id) {
            if ready {
                return self.multi_paxos.run_accept_phase(
                    ballot,
                    self.multi_paxos.log.advance_last_index(),
                    command,
                    client_id,
                );
            }
            return ResultType::Retry;
        }
        if is_someone_else_leader(ballot, self.multi_paxos.id) {
            return ResultType::SomeoneElseLeader(leader(ballot));
        }
        return ResultType::Retry;
    }

    fn next_ballot(&self) -> i64 {
        self.multi_paxos.next_ballot()
    }

    fn become_leader(&self, next_ballot: i64) {
        self.multi_paxos.become_leader(next_ballot);
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

    fn send_prepare(
        stub: &mut MultiPaxosRpcClient<Channel>,
        ballot: i64,
    ) -> PrepareResponse {
        let request = Request::new(PrepareRequest {
            ballot: ballot,
            sender: 0,
        });
        let r = block_on(stub.prepare(request));
        r.unwrap().into_inner()
    }

    fn init() -> (json, MultiPaxos, MultiPaxos, MultiPaxos) {
        let _ = env_logger::builder().is_test(true).try_init();
        let config = make_config(0, NUM_PEERS);
        let peer0 =
            MultiPaxos::new(Log::new(Box::new(MemKVStore::new())), &config);
        let peer1 = MultiPaxos::new(
            Log::new(Box::new(MemKVStore::new())),
            &make_config(1, NUM_PEERS),
        );
        let peer2 = MultiPaxos::new(
            Log::new(Box::new(MemKVStore::new())),
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
    async fn request_with_lower_ballot_ignored() {
        let (config, mut peer0, peer1, _) = init();
        let mut stub = make_stub(&config["peers"][0]);

        peer0.start_rpc_server();

        peer0.become_leader(peer0.next_ballot());
        peer0.become_leader(peer0.next_ballot());

        let stale_ballot = peer1.next_ballot();

        let r = send_prepare(&mut stub, stale_ballot);
        assert_eq!(r.r#type, ResponseType::Reject as i32);

        peer0.stop_rpc_server();
    }
}
