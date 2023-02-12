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

#[derive(Serialize, Deserialize)]
pub struct PrepareRequest {
    pub ballot: i64,
    pub sender: i64,
}

#[derive(Serialize, Deserialize)]
pub struct PrepareResponse {
    pub r#type: i32,
    pub ballot: i64,
    pub instances: Vec<Instance>,
}

#[derive(Serialize, Deserialize)]
pub struct AcceptRequest {
    pub instance: Option<Instance>,
    pub sender: i64,
}

#[derive(Serialize, Deserialize)]
pub struct AcceptResponse {
    pub r#type: i32,
    pub ballot: i64,
}

#[derive(Serialize, Deserialize)]
pub struct CommitRequest {
    pub ballot: i64,
    pub last_executed: i64,
    pub global_last_executed: i64,
    pub sender: i64,
}

#[derive(Serialize, Deserialize)]
pub struct CommitResponse {
    pub r#type: i32,
    pub ballot: i64,
    pub last_executed: i64,
}

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
