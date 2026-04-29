use itertools::Itertools;
use rustdis::{
    Broadcast, BroadcastOk, RequestBody, RequestMessage, ResponseBody,
    broadcast::{BroadcastConsumer, BroadcastState, propagate_broadcast},
    toplogy::{TopologyState, TopologyTrait},
    unique_ids::handle_generate,
};
use serde::{Deserialize, Serialize};
use std::{io, sync::Arc, time::Duration};
use tokio::{
    sync::{Mutex, mpsc},
    time,
};

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
enum Message {
    Response(ResponseMessage),
    Request(RequestMessage),
    Log(String),
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
enum BroadcastMessage {
    Broadcast(Broadcast),
    BroadcastOk(ResponseMessage),
    Retry,
}

#[derive(Serialize, Deserialize, Debug)]
struct ResponseMessage {
    src: String,
    dest: String,
    body: ResponseBody,
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let mut buf = String::new();
    let mut local_msg_id = 1;

    io::stdin().read_line(&mut buf)?;
    let trimmed = buf.trim();
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
    let topology_state = Arc::new(Mutex::new(TopologyState::default()));
    let topology_state_main = topology_state.clone();

    let (tx_logger, mut rx_logger) = mpsc::channel::<Message>(1000);
    let (tx_broadcast, mut rx_broadcast) = mpsc::channel::<BroadcastMessage>(1000);

    let tx_broadcast_ticker = tx_broadcast.clone();
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_millis(500));
        loop {
            interval.tick().await;
            tx_broadcast_ticker
                .send(BroadcastMessage::Retry)
                .await
                .unwrap();
        }
    });

    let node_id_1 = node_id.clone();
    let tx_logger_clone = tx_logger.clone();
    tokio::spawn(async move {
        let mut buf = String::new();
        let mut trimmed;
        loop {
            buf.clear();
            local_msg_id += 1;
            io::stdin().read_line(&mut buf).unwrap();
            trimmed = buf.trim();
            tx_logger_clone
                .send(Message::Log(trimmed.to_string()))
                .await
                .unwrap();
            if let Ok(msg) = serde_json::from_str::<RequestMessage>(trimmed) {
                let response_body: ResponseBody = match msg.body {
                    RequestBody::Echo { msg_id, echo } => ResponseBody::EchoOk {
                        msg_id: local_msg_id,
                        echo,
                        in_reply_to: msg_id,
                    },

                    RequestBody::Init { .. } => panic!("Unexpected Init message type"),

                    RequestBody::Generate(generate) => handle_generate(node_id_1.clone(), generate),

                    RequestBody::Broadcast(broadcast) => {
                        let was_inserted = broadcast_state.consume_broadcast(broadcast.message);
                        if was_inserted {
                            tx_broadcast
                                .send(BroadcastMessage::Broadcast(broadcast.clone()))
                                .await
                                .unwrap();
                        }
                        ResponseBody::BroadcastOk(BroadcastOk {
                            in_reply_to: broadcast.msg_id,
                        })
                    }

                    RequestBody::Read(read) => broadcast_state.handle_read(read),

                    RequestBody::Topology(topology) => {
                        topology_state_main.lock().await.handle_toplogy(topology)
                    }
                };

                let response = ResponseMessage {
                    body: response_body,
                    dest: msg.src,
                    src: node_id_1.clone(),
                };

                tx_logger_clone
                    .send(Message::Response(response))
                    .await
                    .unwrap()
            } else if let Ok(msg) = serde_json::from_str::<ResponseMessage>(trimmed) {
                match msg.body {
                    ResponseBody::BroadcastOk(_) => tx_broadcast
                        .send(BroadcastMessage::BroadcastOk(msg))
                        .await
                        .unwrap(),

                    _ => (),
                }
            } else {
                panic!("Failed to deserialize '{}'", trimmed)
            }
        }
    });

    let topology_state_broadcast_handler = topology_state.clone();
    tokio::spawn(async move {
        let mut pending_acknowledgment: Vec<RequestMessage> = Vec::new();
        while let Some(broadcast) = rx_broadcast.recv().await {
            match broadcast {
                BroadcastMessage::BroadcastOk(broadcast_ok_msg) => {
                    if let ResponseBody::BroadcastOk(BroadcastOk { in_reply_to }) =
                        broadcast_ok_msg.body
                    {
                        let (index, _) = pending_acknowledgment
                            .iter()
                            .find_position(|msg| {
                                if let RequestBody::Broadcast(broadcast) = &msg.body {
                                    broadcast.msg_id == in_reply_to
                                } else {
                                    false
                                }
                            })
                            .unwrap();
                        pending_acknowledgment.swap_remove(index);
                    }
                }
                BroadcastMessage::Broadcast(broadcast) => {
                    // 1. prepare broadcast messages
                    // 2. set expectations for broadcast_ok
                    let requests = propagate_broadcast(
                        &mut local_msg_id,
                        node_id.clone(),
                        &*topology_state_broadcast_handler.lock().await,
                        broadcast,
                    )
                    .await;
                    requests.iter().for_each(|request_message| {
                        pending_acknowledgment.push(request_message.clone());
                    });
                    tx_logger
                        .send(Message::Log(format!(
                            "Got broadcast message (broadcast) {}",
                            pending_acknowledgment.len()
                        )))
                        .await
                        .unwrap();

                    // for msg in requests {
                    //     tx_logger.send(Message::Request(msg)).await.unwrap();
                    // }
                }
                BroadcastMessage::Retry => {
                    tx_logger
                        .send(Message::Log(format!(
                            "Received tick!: Pending {}",
                            pending_acknowledgment.len()
                        )))
                        .await
                        .unwrap();
                    for msg in &pending_acknowledgment {
                        tx_logger.send(Message::Request(msg.clone())).await.unwrap();
                    }
                }
            }
        }
    });

    while let Some(message) = rx_logger.recv().await {
        if let Message::Log(log) = message {
            eprintln!("<- {}", log);
        } else {
            println!("{}", serde_json::to_string(&message).unwrap());
            eprintln!("-> {}", serde_json::to_string(&message).unwrap());
        }
    }

    Ok(())
}

/*
INIT: {"src": "c1", "dest": "n0", "body": {"type": "init", "msg_id": 1, "node_id": "n0", "node_ids": ["n0"]}}
GENERATE: { "src": "c1", "dest": "n0", "body": { "type": "generate", "msg_id": 2 } }
TOPOLOGY: {"id":2,"src":"c1","dest":"n0","body":{"type":"topology","topology":{"n0":[]},"msg_id":1}}
*/
