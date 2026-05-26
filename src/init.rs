use tokio::io::{BufReader, Stdin};
use tokio::sync::mpsc::UnboundedSender;

use crate::toplogy::{TopologyState, TopologyTrait};
use crate::{Init, Message, RequestMessage, RequestPayload, ResponsePayload};
use tokio::io::AsyncBufReadExt;

pub async fn init(reader: &mut BufReader<Stdin>, message_tx: &UnboundedSender<Message>) -> Init {
    let node_id = process_init_msg(reader, &message_tx).await;

    let mut topology_state = TopologyState::default();
    process_topology(&mut topology_state, reader, message_tx).await;

    Init {
        node_id,
        topology_state,
    }
}

pub async fn process_topology(
    topology_state: &mut TopologyState,
    reader: &mut BufReader<Stdin>,
    message_tx: &UnboundedSender<Message>,
) {
    let mut buf = String::new();
    reader
        .read_line(&mut buf)
        .await
        .expect("failed to read from stdin");
    let trimmed = buf.trim();
    let msg: RequestMessage = serde_json::from_str(trimmed)
        .unwrap_or_else(|err| panic!("Failed to deserialize '{}' with error '{}'", trimmed, err));

    if let RequestPayload::Topology(topology) = msg.clone().body.payload {
        topology_state.handle_toplogy(topology);
        let response = Message::Response {
            payload: ResponsePayload::TopologyOk,
            request: msg,
        };
        message_tx.send(response).unwrap();
    } else {
        panic!("Expected first message to be init");
    };
}

pub async fn process_init_msg(
    reader: &mut BufReader<Stdin>,
    message_tx: &UnboundedSender<Message>,
) -> String {
    let mut buf = String::new();
    reader
        .read_line(&mut buf)
        .await
        .expect("failed to read from stdin");
    let trimmed = buf.trim();
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
        message_tx.send(response).unwrap();
        node_id
    } else {
        panic!("Expected first message to be init");
    };
    node_id
}
