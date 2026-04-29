use std::collections::HashMap;

use serde::{Deserialize, Serialize};

pub mod broadcast;
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
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RequestBody {
    Echo {
        msg_id: u32,
        echo: String,
    },

    Init {
        msg_id: u32,
        node_id: String,
        node_ids: Vec<String>,
    },

    Generate(Generate),

    Broadcast(Broadcast),

    Read(Read),

    Topology(Topology),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Topology {
    msg_id: u32,
    topology: HashMap<String, Vec<String>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Generate {
    msg_id: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Read {
    msg_id: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone, Hash, Eq, PartialEq)]
pub struct Broadcast {
    pub msg_id: u32,
    pub message: u32,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ResponseBody {
    EchoOk {
        msg_id: u32,
        echo: String,
        in_reply_to: u32,
    },

    InitOk {
        in_reply_to: u32,
    },

    GenerateOk {
        in_reply_to: u32,
        id: String,
    },

    BroadcastOk(BroadcastOk),

    ReadOk {
        in_reply_to: u32,
        messages: Vec<u32>,
    },

    TopologyOk {
        in_reply_to: u32,
    },
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BroadcastOk {
    pub in_reply_to: u32,
}
