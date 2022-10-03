use crate::replicant::multipaxos::MultiPaxos;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::str;
use std::sync::{Arc, Mutex};
use tokio::io::AsyncReadExt;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};

struct Client {
    id: i64,
    socket: OwnedReadHalf,
    client_manager: Arc<ClientManagerInner>,
    multi_paxos: Arc<MultiPaxos>,
}

impl Client {
    fn new(
        id: i64,
        socket: OwnedReadHalf,
        client_manager: Arc<ClientManagerInner>,
        multi_paxos: Arc<MultiPaxos>,
    ) -> Self {
        Self {
            id,
            socket,
            client_manager,
            multi_paxos,
        }
    }

    async fn start(&mut self) {
        let mut buf = vec![0; 1024];
        loop {
            let num_read = self.socket.read(&mut buf).await.unwrap();
            if num_read == 0 {
                break;
            }
            println!("read {}", str::from_utf8(&buf).unwrap());
        }
        self.client_manager.stop(self.id);
    }
}

struct ClientManagerInner {
    next_id: Mutex<i64>,
    num_peers: i64,
    clients: Mutex<HashMap<i64, OwnedWriteHalf>>,
}

impl ClientManagerInner {
    fn new(id: i64, num_peers: i64) -> Self {
        Self {
            next_id: Mutex::new(id),
            num_peers,
            clients: Mutex::new(HashMap::new()),
        }
    }

    fn next_client_id(&self) -> i64 {
        let mut next_id = self.next_id.lock().unwrap();
        let id = *next_id;
        *next_id += self.num_peers;
        id
    }

    fn insert(&self, write_half: OwnedWriteHalf) -> i64 {
        let id = self.next_client_id();
        let mut clients = self.clients.lock().unwrap();
        let v = clients.insert(id, write_half);
        drop(clients);
        assert!(v.is_none());
        id
    }

    fn stop(&self, id: i64) {
        let mut clients = self.clients.lock().unwrap();
        let v = clients.remove(&id);
        drop(clients);
        assert!(v.is_some());
        println!("client_manager stopped client {}", id);
    }

    fn stop_all(&self) {
        let mut clients = self.clients.lock().unwrap();
        clients.clear();
    }
}

pub struct ClientManager {
    client_manager: Arc<ClientManagerInner>,
}

impl ClientManager {
    pub fn new(id: i64, num_peers: i64) -> Self {
        Self {
            client_manager: Arc::new(ClientManagerInner::new(id, num_peers)),
        }
    }

    pub fn start(&self, client: TcpStream, multi_paxos: Arc<MultiPaxos>) {
        let (read_half, write_half) = client.into_split();
        let id = self.client_manager.insert(write_half);
        let client_manager = self.client_manager.clone();
        tokio::spawn(async move {
            Client::new(id, read_half, client_manager, multi_paxos)
                .start()
                .await;
        });
        println!("client_manager started client {}", id);
    }

    pub fn stop_all(&self) {
        self.client_manager.stop_all();
    }
}
