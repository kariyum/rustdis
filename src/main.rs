use rustdis::{
    Log, Message,
    broadcast::MessageBroadcast,
    init::{process_init_msg, process_topology},
    message_consumer,
    socket_consumer::handle_socket_message,
    toplogy::TopologyState,
};
use std::time::Duration;
use tokio::{
    io::{AsyncWriteExt, Stderr, Stdout},
    sync::mpsc,
    time,
};

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

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let mut reader = tokio::io::BufReader::new(tokio::io::stdin());
    let mut writer = tokio::io::stdout();
    let mut stderr_writer = tokio::io::stderr();

    let (socket_tx, mut socket_rx) = mpsc::unbounded_channel::<Message>();
    let node_id = process_init_msg(&mut reader, &socket_tx).await;
    let node_id: &'static str = Box::leak(node_id.into_boxed_str());
    let handler = tokio::spawn(async move {
        let mut message_broadcast = MessageBroadcast::default();
        let mut local_msg_id = 0;
        while let Some(message) = socket_rx.recv().await {
            let msg =
                handle_socket_message(message, node_id, &mut local_msg_id, &mut message_broadcast);
            if let Some(msg) = msg {
                send(msg, &mut writer, &mut stderr_writer).await;
            }
        }
    });

    let mut topology_state = TopologyState::default();
    process_topology(&mut topology_state, &mut reader, &socket_tx).await;

    let retry_tx = socket_tx.clone();
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_millis(100));
        loop {
            interval.tick().await;
            retry_tx.send(Message::Retry).unwrap();
        }
    });

    tokio::spawn(async move {
        message_consumer::socket_message_consumer(reader, socket_tx, node_id, &topology_state)
            .await;
    });

    handler.await.expect("socket io actor failed");
    Ok(())
}

/*
INIT: {"src": "c1", "dest": "n0", "body": {"type": "init", "msg_id": 1, "node_id": "n0", "node_ids": ["n0"]}}
GENERATE: { "src": "c1", "dest": "n0", "body": { "type": "generate", "msg_id": 2 } }
TOPOLOGY: {"id":2,"src":"c1","dest":"n0","body":{"type":"topology","topology":{"n0":[]},"msg_id":1}}
*/
