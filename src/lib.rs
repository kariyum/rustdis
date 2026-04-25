use serde::{Deserialize, Serialize};
use serde_json::Value;

pub mod broadcast;
pub mod unique_ids;

#[derive(Serialize, Deserialize, Debug)]
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

    Topology {
        msg_id: u32,
        #[serde(flatten)]
        topology: Value,
    },
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Generate {
    msg_id: u32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Read {
    msg_id: u32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Broadcast {
    msg_id: u32,
    message: u32,
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

    BroadcastOk {
        in_reply_to: u32,
    },

    ReadOk {
        in_reply_to: u32,
        messages: Vec<u32>,
    },

    TopologyOk {
        in_reply_to: u32,
    },
}
