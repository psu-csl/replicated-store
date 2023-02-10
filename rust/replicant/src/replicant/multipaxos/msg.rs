use serde::{Deserialize, Serialize};
use crate::replicant::multipaxos::tcp::Message;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResponseType {
    Ok = 0,
    Reject = 1,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[repr(i32)]
pub enum CommandType {
    Get = 0,
    Put = 1,
    Del = 2,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum InstanceState {
    Inprogress = 0,
    Committed = 1,
    Executed = 2,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Command {
    pub r#type: i32,
    pub key: String,
    pub value: String,
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct Instance {
    pub ballot: i64,
    pub index: i64,
    pub client_id: i64,
    pub state: i32,
    pub command: Option<Command>,
}

pub trait MultiPaxosMessage {
    fn to_json_string(&self, msg_type: MessageType) -> Message {
        // let json_string = serde_json::to_string(&self).unwrap();
        let msg = Message {
            r#type: msg_type as u8,
            channel_id: 0,
            msg: "json_string".to_string(),
        };
        msg
    }
}

#[derive(Serialize, Deserialize)]
pub struct PrepareRequest {
    pub ballot: i64,
    pub sender: i64,
}

impl MultiPaxosMessage for PrepareRequest {}

#[derive(Serialize, Deserialize)]
pub struct PrepareResponse {
    pub r#type: i32,
    pub ballot: i64,
    pub instances: Vec<Instance>,
}

impl MultiPaxosMessage for PrepareResponse {}

#[derive(Serialize, Deserialize)]
pub struct AcceptRequest {
    pub instance: Option<Instance>,
    pub sender: i64,
}

impl MultiPaxosMessage for AcceptRequest {}

#[derive(Serialize, Deserialize)]
pub struct AcceptResponse {
    pub r#type: i32,
    pub ballot: i64,
}

impl MultiPaxosMessage for AcceptResponse {}

#[derive(Serialize, Deserialize)]
pub struct CommitRequest {
    pub ballot: i64,
    pub last_executed: i64,
    pub global_last_executed: i64,
    pub sender: i64,
}

impl MultiPaxosMessage for CommitRequest {}

#[derive(Serialize, Deserialize)]
pub struct CommitResponse {
    pub r#type: i32,
    pub ballot: i64,
    pub last_executed: i64,
}

impl MultiPaxosMessage for CommitResponse {}

#[derive(Copy, Clone)]
#[repr(u8)]
pub enum MessageType {
    PrepareRequest = 1,
    PrepareResponse,
    AcceptRequest,
    AcceptResponse,
    CommitRequest,
    CommitResponse,
}
