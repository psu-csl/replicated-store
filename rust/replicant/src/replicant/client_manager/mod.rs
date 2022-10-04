use crate::replicant::multipaxos::rpc::Command;
use crate::replicant::multipaxos::MultiPaxos;
use crate::replicant::multipaxos::ResultType;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::str;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};

struct Client {
    id: i64,
    read_half: BufReader<OwnedReadHalf>,
    client_manager: Arc<ClientManagerInner>,
    multi_paxos: Arc<MultiPaxos>,
}

fn parse(request: &str) -> Option<Command> {
    let tokens: Vec<&str> = request.split(' ').collect();
    if tokens.len() == 2 {
        if tokens[0] == "get" {
            return Some(Command::get(tokens[1]));
        }
        if tokens[0] == "del" {
            return Some(Command::del(tokens[1]));
        }
        return None;
    }
    if tokens.len() == 3 && tokens[0] == "put" {
        return Some(Command::put(tokens[1], tokens[2]));
    }
    None
}

impl Client {
    fn new(
        id: i64,
        read_half: OwnedReadHalf,
        multi_paxos: Arc<MultiPaxos>,
        client_manager: Arc<ClientManagerInner>,
    ) -> Self {
        Self {
            id,
            read_half: BufReader::new(read_half),
            multi_paxos,
            client_manager,
        }
    }

    async fn start(&mut self) {
        let mut request = String::new();
        while let Ok(n) = self.read_half.read_line(&mut request).await {
            if n == 0 {
                break;
            }
            if let Some(command) = parse(&request) {
                let response =
                    match self.multi_paxos.replicate(&command, self.id).await {
                        ResultType::Ok => None,
                        ResultType::Retry => Some("retry"),
                        ResultType::SomeoneElseLeader(_) => {
                            Some("leader is ...")
                        }
                    };
                if let Some(response) = response {
                    self.client_manager
                        .write(self.id, response.as_bytes())
                        .await;
                }
            } else {
                self.client_manager.write(self.id, b"bad command").await;
            }
        }
        self.client_manager.stop(self.id);
    }
}

struct ClientManagerInner {
    next_id: Mutex<i64>,
    num_peers: i64,
    clients: Mutex<HashMap<i64, Arc<tokio::sync::Mutex<OwnedWriteHalf>>>>,
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

    async fn write(&self, client_id: i64, buf: &[u8]) {
        let client;
        {
            client = if let Some(client) =
                self.clients.lock().unwrap().get(&client_id)
            {
                Some(client.clone())
            } else {
                None
            }
        }

        if let Some(client) = client {
            let mut client = client.lock().await;
            match client.write(buf).await {
                Ok(_) => (),
                Err(_) => self.stop(client_id),
            }
        }
    }

    fn stop(&self, id: i64) {
        let mut clients = self.clients.lock().unwrap();
        let client = clients.remove(&id);
        drop(clients);
        assert!(client.is_some());
        println!("client_manager stopped client {}", id);
    }

    fn stop_all(&self) {
        let mut clients = self.clients.lock().unwrap();
        clients.clear();
    }
}

pub struct ClientManager {
    client_manager: Arc<ClientManagerInner>,
    multi_paxos: Arc<MultiPaxos>,
}

impl ClientManager {
    pub fn new(id: i64, num_peers: i64, multi_paxos: Arc<MultiPaxos>) -> Self {
        Self {
            client_manager: Arc::new(ClientManagerInner::new(id, num_peers)),
            multi_paxos,
        }
    }

    pub fn start(&self, stream: TcpStream) {
        let (read_half, write_half) = stream.into_split();
        let id = self.client_manager.next_client_id();

        let mut client = Client::new(
            id,
            read_half,
            self.multi_paxos.clone(),
            self.client_manager.clone(),
        );

        let write_half = Arc::new(tokio::sync::Mutex::new(write_half));

        let mut clients = self.client_manager.clients.lock().unwrap();
        let prev = clients.insert(id, write_half);
        drop(clients);
        assert!(prev.is_none());

        tokio::spawn(async move {
            client.start().await;
        });
        println!("client_manager started client {}", id);
    }

    pub async fn write(&self, client_id: i64, buf: &[u8]) {
        self.client_manager.write(client_id, buf).await
    }

    pub fn stop_all(&self) {
        self.client_manager.stop_all();
    }
}
