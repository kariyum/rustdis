use std::collections::HashMap;

use serde::{Deserialize, Serialize};

pub mod broadcast;
pub mod init;
pub mod message_consumer;
pub mod toplogy;
pub mod unique_ids;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RequestMessage {
    pub src: String,
    pub dest: String,
    pub body: RequestBody,
    pub id: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub struct RequestBody {
    pub msg_id: u32,
    #[serde(flatten)]
    pub payload: RequestPayload,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RequestPayload {
    Echo {
        echo: String,
    },

    Init {
        node_id: String,
        node_ids: Vec<String>,
    },

    Generate,
    Broadcast(Broadcast),
    Read,
    Topology(Topology),
    Sync(Sync),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Topology {
    topology: HashMap<String, Vec<String>>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Hash, Eq, PartialEq)]
pub struct Broadcast {
    pub message: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone, Hash, Eq, PartialEq, Default)]
pub struct Sync {
    pub message: Vec<u32>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ResponseBody {
    pub msg_id: u32,
    pub in_reply_to: u32,
    #[serde(flatten)]
    pub payload: ResponsePayload,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ResponsePayload {
    EchoOk { echo: String },
    InitOk,
    GenerateOk { id: String },
    BroadcastOk,
    ReadOk { messages: Vec<u32> },
    TopologyOk,
    SyncOk { messages: Vec<u32> },
}

#[derive(Serialize, Deserialize, Debug)]
pub struct BroadcastRequest {
    pub dest: String,
    pub sync: BroadcastPayload,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
pub enum BroadcastPayload {
    Broadcast(Broadcast),
    Sync(Sync),
}

pub struct SyncGossip {
    pub dest: String,
    pub sync: RequestPayload,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
pub enum Message {
    Response {
        payload: ResponsePayload,
        request: RequestMessage,
    },
    Request(BroadcastRequest),
    Ack(ResponseMessage),
    Log(String),
    Retry,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
pub enum Log {
    Response(ResponseMessage),
    Request(RequestMessage),
    Requests(Vec<RequestMessage>),
    Log(String),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ResponseMessage {
    src: String,
    dest: String,
    body: ResponseBody,
}
