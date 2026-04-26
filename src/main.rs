use rustdis::{
    RequestBody, RequestMessage, ResponseBody,
    broadcast::{BroadcastConsumer, BroadcastProducer, BroadcastState},
    toplogy::{TopologyState, TopologyTrait},
    unique_ids::handle_generate,
};
use serde::{Deserialize, Serialize};
use std::io;

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
    eprintln!("<- {}", trimmed);
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
        eprintln!("-> {}", serde_json::to_string(&response).unwrap());
        node_id
    } else {
        panic!("Expected first message to be init");
    };

    let mut broadcast_state = BroadcastState::default();
    let mut topology_state = TopologyState::default();
    loop {
        buf.clear();
        local_msg_id += 1;
        io::stdin().read_line(&mut buf)?;
        trimmed = buf.trim();
        eprintln!("<- {}", trimmed);
        if let Ok(msg) = serde_json::from_str::<RequestMessage>(trimmed) {
            let response_body: ResponseBody = match msg.body {
                RequestBody::Echo { msg_id, echo } => ResponseBody::EchoOk {
                    msg_id: local_msg_id,
                    echo,
                    in_reply_to: msg_id,
                },

                RequestBody::Init { .. } => panic!("Unexpected Init message type"),

                RequestBody::Generate(generate) => handle_generate(node_id.clone(), generate),

                RequestBody::Broadcast(broadcast) => {
                    let body =
                        broadcast_state.consume_broadcast(broadcast.clone(), msg.src.clone());
                    broadcast_state
                        .notify_nodes(
                            &mut local_msg_id,
                            node_id.clone(),
                            &topology_state,
                            broadcast,
                        )
                        .into_iter()
                        .for_each(|msg| {
                            println!("{}", serde_json::to_string(&msg).unwrap());
                            eprintln!("-> {}", serde_json::to_string(&msg).unwrap());
                        });
                    body
                }

                RequestBody::Read(read) => broadcast_state.handle_read(read),

                RequestBody::Topology(topology) => topology_state.handle_toplogy(topology),
            };

            let response = ResponseMessage {
                body: response_body,
                dest: msg.src,
                src: node_id.clone(),
            };

            println!("{}", serde_json::to_string(&response).unwrap());
            eprintln!("-> {}", serde_json::to_string(&response).unwrap());
        } else if let Ok(_) = serde_json::from_str::<ResponseMessage>(trimmed) {
            ()
        } else {
            panic!("Failed to deserialize '{}'", trimmed)
        }
    }
}

/*
INIT: {"src": "c1", "dest": "n0", "body": {"type": "init", "msg_id": 1, "node_id": "n0", "node_ids": ["n0"]}}
GENERATE: { "src": "c1", "dest": "n0", "body": { "type": "generate", "msg_id": 2 } }
TOPOLOGY: {"id":2,"src":"c1","dest":"n0","body":{"type":"topology","topology":{"n0":[]},"msg_id":1}}
*/
