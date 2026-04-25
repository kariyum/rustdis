use serde::{Deserialize, Serialize};

pub mod generate;

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
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Generate {
    msg_id: u32,
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
}
