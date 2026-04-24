use std::{io, sync::mpsc::TryIter};

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
struct RequestMessage {
    src: String,
    dest: String,
    body: Body,
}

#[derive(Serialize, Deserialize, Debug)]
struct ResponseMessage {
    src: String,
    dest: String,
    body: Response,
}
#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Body {
    Echo {
        msg_id: u32,
        echo: String,
    },

    Init {
        msg_id: u32,
        node_id: String,
        node_ids: Vec<String>,
    },
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Response {
    EchoOk {
        msg_id: u32,
        echo: String,
        in_reply_to: u32,
    },

    InitOk {
        in_reply_to: u32,
    },
}

fn main() -> io::Result<()> {
    let mut buf = String::new();
    let mut trimmed;
    let mut local_msg_id = 1;

    io::stdin().read_line(&mut buf)?;
    trimmed = buf.trim();
    let msg: RequestMessage = serde_json::from_str(trimmed)
        .unwrap_or_else(|err| panic!("Failed to deserialize '{}' with error '{}'", trimmed, err));

    let node_id = if let Body::Init {
        msg_id,
        node_id,
        node_ids,
    } = msg.body
    {
        let response = ResponseMessage {
            body: Response::InitOk {
                in_reply_to: msg_id,
            },
            dest: msg.src,
            src: node_id.clone(),
        };

        println!("{}", serde_json::to_string(&response).unwrap());
        eprintln!(
            "responded with {}",
            serde_json::to_string(&response).unwrap()
        );
        node_id
    } else {
        panic!("Expected first message to be init");
    };

    loop {
        buf.clear();
        io::stdin().read_line(&mut buf)?;
        trimmed = buf.trim();
        let msg: RequestMessage = serde_json::from_str(trimmed).unwrap_or_else(|err| {
            panic!("Failed to deserialize '{}' with error '{}'", trimmed, err)
        });

        let response_body: Response = match msg.body {
            Body::Echo { msg_id, echo } => Response::EchoOk {
                msg_id: local_msg_id,
                echo,
                in_reply_to: msg_id,
            },

            Body::Init {
                msg_id,
                node_id,
                node_ids,
            } => panic!("Unexpected Init message type"),
        };

        let response = ResponseMessage {
            body: response_body,
            dest: msg.src,
            src: node_id.clone(),
        };

        println!("{}", serde_json::to_string(&response).unwrap());
        eprintln!(
            "responded with {}",
            serde_json::to_string(&response).unwrap()
        );
    }
}

