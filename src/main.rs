use rustdis::{RequestBody, ResponseBody, generate::handle_generate};
use serde::{Deserialize, Serialize};
use std::io;

#[derive(Serialize, Deserialize, Debug)]
struct RequestMessage {
    src: String,
    dest: String,
    body: RequestBody,
}

#[derive(Serialize, Deserialize, Debug)]
struct ResponseMessage {
    src: String,
    dest: String,
    body: ResponseBody,
}

fn main() -> io::Result<()> {
    let mut buf = String::new();
    let mut trimmed;
    let mut local_msg_id = 1;

    io::stdin().read_line(&mut buf)?;
    trimmed = buf.trim();
    let msg: RequestMessage = serde_json::from_str(trimmed)
        .unwrap_or_else(|err| panic!("Failed to deserialize '{}' with error '{}'", trimmed, err));

    let node_id = if let RequestBody::Init {
        msg_id,
        node_id,
        node_ids: _,
    } = msg.body
    {
        let response = ResponseMessage {
            body: ResponseBody::InitOk {
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
        local_msg_id += 1;
        io::stdin().read_line(&mut buf)?;
        trimmed = buf.trim();
        let msg: RequestMessage = serde_json::from_str(trimmed).unwrap_or_else(|err| {
            panic!("Failed to deserialize '{}' with error '{}'", trimmed, err)
        });

        let response_body: ResponseBody = match msg.body {
            RequestBody::Echo { msg_id, echo } => ResponseBody::EchoOk {
                msg_id: local_msg_id,
                echo,
                in_reply_to: msg_id,
            },

            RequestBody::Init { .. } => panic!("Unexpected Init message type"),

            RequestBody::Generate(generate) => handle_generate(node_id.clone(), generate),
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

// INIT: {"src": "c1", "dest": "n0", "body": {"type": "init", "msg_id": 1, "node_id": "n0", "node_ids": ["n0"]}}
// GENERATE: { "src": "c1", "dest": "n0", "body": { "type": "generate", "msg_id": 2 } }

