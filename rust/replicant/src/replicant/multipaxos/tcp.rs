use std::collections::HashMap;
use std::sync::Arc;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Sender, UnboundedReceiver, UnboundedSender};
use crate::replicant::multipaxos::msg::MessageType;

#[derive(Serialize, Deserialize)]
pub struct Message {
    pub r#type: u8,
    pub channel_id: u64,
    pub msg: String,
}

async fn handle_outgoing_requests(
    mut write_half: OwnedWriteHalf,
    mut request_recv: UnboundedReceiver<String>
) {
    while let Some(mut request) = request_recv.recv().await {
        request.push('\n');
        write_half.write_all(request.as_bytes()).await
            .expect("peer cannot send msg");
    }
}

async fn handle_incoming_responses(
    read_half: OwnedReadHalf,
    channels: Arc<tokio::sync::Mutex<HashMap<u64, Sender<String>>>>
) {
    let mut read_half = BufReader::new(read_half);
    let mut line = String::new();
    while let Ok(n) = read_half.read_line(&mut line).await {
        if n == 0 {
            break;
        }
        let request: Message = match serde_json::from_str(&line) {
            Ok(request) => request,
            Err(_) => continue
        };
        let channels = channels.lock().await;
        let response_send = channels.get(&request.channel_id);
        if let Some(response_send) = response_send {
            response_send.send(request.msg).await;
        }
        drop(channels);
        line.clear();
    }
    let mut channels = channels.lock().await;
    channels.clear();
}

pub struct TcpLink {
    request_sender: UnboundedSender<String>,
}

impl TcpLink {
    pub async fn new(
        addr: &str,
        channels: Arc<tokio::sync::Mutex<HashMap<u64, Sender<String>>>>
    ) -> Self {
        let stream;
        loop {
            match TcpStream::connect(addr).await {
                Ok(s) => {stream = s; stream.set_nodelay(true); break;},
                _ => (),
            }
        }
        let (read_half, write_half) = stream.into_split();
        tokio::spawn(async move {
            handle_incoming_responses(read_half, channels).await;
        });
        let (request_sender, request_recv) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            handle_outgoing_requests(write_half, request_recv).await;
        });
        Self {
            request_sender,
        }
    }

    pub async fn send_await_response(
        &self,
        msg_type: MessageType,
        channel_id: u64,
        msg: &str)
    {
        let tcp_request = serde_json::to_string(&Message{
            r#type: msg_type as u8,
            channel_id,
            msg: msg.to_string(),
        }).unwrap();
        self.request_sender.send(tcp_request).
            expect("cannot send request via channel");
    }
}
