use rustdis::{
    Broadcast, BroadcastPayload, BroadcastRequest, RequestBody, RequestMessage, RequestPayload,
    ResponseBody, ResponsePayload,
    broadcast::{
        BroadcastConsumer, BroadcastState, MessageBroadcast, Syncable, handle_broadcast_request,
        propagate_broadcast,
    },
    toplogy::{TopologyState, TopologyTrait},
    unique_ids::handle_generate,
};
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncWriteExt, Stderr, Stdout},
    sync::{Mutex, mpsc},
    time,
};

#[derive(Serialize, Deserialize, Debug)]
#[serde(untagged)]
enum Message {
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
enum Log {
    Response(ResponseMessage),
    Request(RequestMessage),
    Requests(Vec<RequestMessage>),
    Log(String),
}

#[derive(Serialize, Deserialize, Debug)]
struct ResponseMessage {
    src: String,
    dest: String,
    body: ResponseBody,
}

async fn send(msg: Log, writer: &mut Stdout, stderr_writer: &mut Stderr) -> () {
    match msg {
        Log::Response(..) | Log::Request(..) => {
            writer
                .write_all(format!("{}\n", serde_json::to_string(&msg).unwrap()).as_bytes())
                .await
                .unwrap();
            stderr_writer
                .write_all(format!("-> {}\n", serde_json::to_string(&msg).unwrap()).as_bytes())
                .await
                .unwrap();
        }
        Log::Requests(ref requests) => {
            stderr_writer
                .write_all(format!("SYNCING {}\n", requests.len()).as_bytes())
                .await
                .unwrap();
            for msg in requests.iter() {
                writer
                    .write_all(format!("{}\n", serde_json::to_string(msg).unwrap()).as_bytes())
                    .await
                    .unwrap();
                stderr_writer
                    .write_all(format!("-> {}\n", serde_json::to_string(msg).unwrap()).as_bytes())
                    .await
                    .unwrap();
            }
        }
        Log::Log(str) => stderr_writer
            .write_all(format!("{}\n", &str).as_bytes())
            .await
            .unwrap(),
    }
}

use tokio::io::AsyncBufReadExt;
#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let mut buf = String::new();
    let mut reader = tokio::io::BufReader::new(tokio::io::stdin());
    let mut writer = tokio::io::stdout();
    let mut stderr_writer = tokio::io::stderr();

    let mut broadcast_state = BroadcastState::default();
    let topology_state = Arc::new(Mutex::new(TopologyState::default()));
    let topology_state_main = topology_state.clone();

    let (tx_logger, mut rx_logger) = mpsc::unbounded_channel::<Message>();

    reader.read_line(&mut buf).await?;
    let trimmed = buf.trim();
    eprintln!("<- {}", trimmed);
    let msg: RequestMessage = serde_json::from_str(trimmed)
        .unwrap_or_else(|err| panic!("Failed to deserialize '{}' with error '{}'", trimmed, err));

    let node_id = if let RequestPayload::Init {
        node_id,
        node_ids: _,
    } = msg.clone().body.payload
    {
        let response = Message::Response {
            payload: ResponsePayload::InitOk,
            request: msg,
        };
        tx_logger.send(response).unwrap();
        node_id
    } else {
        panic!("Expected first message to be init");
    };

    let tx_logger_ticker = tx_logger.clone();
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_millis(100));
        loop {
            interval.tick().await;
            tx_logger_ticker.send(Message::Retry).unwrap();
        }
    });

    let node_id_1 = node_id.clone();
    let node_id_2 = node_id.clone();
    let tx_logger_clone = tx_logger.clone();
    tokio::spawn(async move {
        let mut buf = String::new();
        let mut trimmed;
        loop {
            buf.clear();
            reader.read_line(&mut buf).await.unwrap();
            trimmed = buf.trim();
            tx_logger_clone
                .send(Message::Log(format!("<- {}", trimmed.to_string())))
                .unwrap();
            if let Ok(msg) = serde_json::from_str::<RequestMessage>(trimmed) {
                let response_payload: ResponsePayload = match msg.clone().body.payload {
                    RequestPayload::Echo { echo } => ResponsePayload::EchoOk { echo },
                    RequestPayload::Init { .. } => panic!("Unexpected Init message type"),
                    RequestPayload::Generate => handle_generate(node_id_1.clone()),
                    RequestPayload::Broadcast(broadcast) => {
                        let was_inserted = broadcast_state.consume_broadcast(broadcast.message);
                        if was_inserted {
                            let requests = propagate_broadcast(
                                &node_id_1,
                                &msg.src,
                                &*topology_state_main.lock().await,
                                &broadcast,
                            )
                            .await;
                            for msg in requests {
                                tx_logger_clone.send(Message::Request(msg)).unwrap();
                            }
                        }
                        ResponsePayload::BroadcastOk
                    }
                    RequestPayload::Read => broadcast_state.handle_read(),
                    RequestPayload::Topology(topology) => {
                        topology_state_main.lock().await.handle_toplogy(topology)
                    }
                    RequestPayload::Sync(sync) => {
                        let topology = topology_state_main.lock().await;
                        for message in sync.message.clone() {
                            if broadcast_state.consume_broadcast(message) {
                                let requests = propagate_broadcast(
                                    &node_id_1,
                                    &msg.src,
                                    &*topology,
                                    &Broadcast { message },
                                )
                                .await;
                                for req in requests {
                                    tx_logger_clone.send(Message::Request(req)).unwrap();
                                }
                            }
                        }
                        ResponsePayload::SyncOk {
                            messages: sync.message,
                        }
                    }
                };

                tx_logger_clone
                    .send(Message::Response {
                        payload: response_payload,
                        request: msg,
                    })
                    .unwrap();
            } else if let Ok(msg) = serde_json::from_str::<ResponseMessage>(trimmed) {
                tx_logger_clone.send(Message::Ack(msg)).unwrap();
            } else {
                panic!("Failed to deserialize '{}'", trimmed)
            }
        }
    });

    let mut message_broadcast = MessageBroadcast::default();
    let mut local_msg_id = 0;
    while let Some(message) = rx_logger.recv().await {
        let msg = match message {
            Message::Log(log) => Some(Log::Log(log)),
            Message::Request(BroadcastRequest { dest, sync }) => {
                let payload = handle_broadcast_request(sync);
                let message = RequestMessage {
                    src: node_id_2.clone(),
                    dest: dest,
                    body: RequestBody {
                        msg_id: local_msg_id,
                        payload,
                    },
                    id: local_msg_id,
                };
                message_broadcast.sent(&message);

                local_msg_id += 1;
                Some(Log::Request(message))
            }
            Message::Response { payload, request } => {
                let message = ResponseMessage {
                    src: node_id_2.clone(),
                    dest: request.src,
                    body: ResponseBody {
                        msg_id: local_msg_id,
                        in_reply_to: request.body.msg_id,
                        payload,
                    },
                };
                local_msg_id += 1;
                Some(Log::Response(message))
            }
            Message::Ack(msg) => {
                message_broadcast.ack(&msg.src, msg.body.in_reply_to);
                if let ResponsePayload::SyncOk { messages } = &msg.body.payload {
                    message_broadcast.ack_sync(msg.src.clone(), messages.clone());
                }
                Some(Log::Log(format!(
                    "<- {}",
                    serde_json::to_string(&msg).unwrap()
                )))
            }
            Message::Retry => {
                let messages = message_broadcast.sync();
                let requests = messages
                    .into_iter()
                    .map(|msg| {
                        let req = RequestMessage {
                            src: node_id_2.clone(),
                            dest: msg.dest,
                            body: RequestBody {
                                msg_id: local_msg_id,
                                payload: msg.sync,
                            },
                            id: local_msg_id,
                        };
                        message_broadcast.sent(&req);

                        local_msg_id += 1;
                        req
                    })
                    .collect();
                Some(Log::Requests(requests))
            }
        };

        if let Some(msg) = msg {
            send(msg, &mut writer, &mut stderr_writer).await;
        }
    }
    Ok(())
}

/*
INIT: {"src": "c1", "dest": "n0", "body": {"type": "init", "msg_id": 1, "node_id": "n0", "node_ids": ["n0"]}}
GENERATE: { "src": "c1", "dest": "n0", "body": { "type": "generate", "msg_id": 2 } }
TOPOLOGY: {"id":2,"src":"c1","dest":"n0","body":{"type":"topology","topology":{"n0":[]},"msg_id":1}}
*/
