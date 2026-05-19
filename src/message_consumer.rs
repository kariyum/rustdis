use tokio::io::AsyncBufReadExt;
use tokio::io::{BufReader, Stdin};
use tokio::sync::mpsc::UnboundedSender;

use crate::broadcast::{BroadcastConsumer, BroadcastState, propagate_broadcast};
use crate::toplogy::TopologyState;
use crate::unique_ids::handle_generate;
use crate::{Broadcast, Message, RequestMessage, RequestPayload, ResponseMessage, ResponsePayload};

pub async fn socket_message_consumer(
    mut reader: BufReader<Stdin>,
    message_tx: UnboundedSender<Message>,
    node_id: &str,
    topology_state: &TopologyState,
) {
    let mut broadcast_state = BroadcastState::default();
    let mut buf = String::new();
    let mut trimmed;
    loop {
        buf.clear();
        reader.read_line(&mut buf).await.unwrap();
        trimmed = buf.trim();
        message_tx
            .send(Message::Log(format!("<- {}", trimmed.to_string())))
            .unwrap();
        if let Ok(msg) = serde_json::from_str::<RequestMessage>(trimmed) {
            let response_payload: ResponsePayload = match msg.clone().body.payload {
                RequestPayload::Echo { echo } => ResponsePayload::EchoOk { echo },
                RequestPayload::Init { .. } => panic!("Unexpected Init message type"),
                RequestPayload::Generate => handle_generate(node_id),
                RequestPayload::Broadcast(broadcast) => {
                    let was_inserted = broadcast_state.consume_broadcast(broadcast.message);
                    if was_inserted {
                        let requests =
                            propagate_broadcast(node_id, &msg.src, &*topology_state, &broadcast)
                                .await;
                        for msg in requests {
                            message_tx.send(Message::Request(msg)).unwrap();
                        }
                    }
                    ResponsePayload::BroadcastOk
                }
                RequestPayload::Read => broadcast_state.handle_read(),
                RequestPayload::Topology(topology) => {
                    todo!()
                    // TODO remove this
                    // topology_state_main.lock().await.handle_toplogy(topology)
                }
                RequestPayload::Sync(sync) => {
                    for message in sync.message.clone() {
                        if broadcast_state.consume_broadcast(message) {
                            let requests = propagate_broadcast(
                                &node_id,
                                &msg.src,
                                topology_state,
                                &Broadcast { message },
                            )
                            .await;
                            for req in requests {
                                message_tx.send(Message::Request(req)).unwrap();
                            }
                        }
                    }
                    ResponsePayload::SyncOk {
                        messages: sync.message,
                    }
                }
            };

            message_tx
                .send(Message::Response {
                    payload: response_payload,
                    request: msg,
                })
                .unwrap();
        } else if let Ok(msg) = serde_json::from_str::<ResponseMessage>(trimmed) {
            message_tx.send(Message::Ack(msg)).unwrap();
        } else {
            panic!("Failed to deserialize '{}'", trimmed)
        }
    }
}
