use std::collections::{HashMap, HashSet};

use crate::{
    Broadcast, BroadcastPayload, BroadcastRequest, RequestMessage, RequestPayload, ResponsePayload,
    Sync, SyncGossip,
    toplogy::{TopologyState, TopologyTrait},
};
pub trait BroadcastConsumer {
    fn consume_broadcast(&mut self, message: u32) -> bool;
    fn handle_read(&self) -> ResponsePayload;
    fn sync(&mut self, msgs: Vec<u32>) -> ();
}

pub trait BroadcastProducer {
    fn notify_nodes(
        &mut self,
        broadcast: &Broadcast,
        neighbors: &Vec<String>,
    ) -> Vec<BroadcastRequest>;
}

pub struct BroadcastState {
    msgs: Vec<u32>,
    msg_ids: HashSet<u32>,
}

impl BroadcastConsumer for BroadcastState {
    fn consume_broadcast(&mut self, message: u32) -> bool {
        if self.msg_ids.insert(message) {
            self.msgs.push(message);
            true
        } else {
            false
        }
    }

    fn handle_read(&self) -> ResponsePayload {
        ResponsePayload::ReadOk {
            messages: self.msgs.clone(),
        }
    }

    fn sync(&mut self, msgs: Vec<u32>) -> () {
        msgs.iter().for_each(|msg| {
            self.consume_broadcast(*msg);
        });
    }
}

impl Default for BroadcastState {
    fn default() -> Self {
        BroadcastState {
            msgs: vec![],
            msg_ids: HashSet::new(),
        }
    }
}

pub trait Syncable {
    fn sync(&mut self) -> Vec<SyncGossip>;
    fn ack(&mut self, from: &String, msg_id: u32) -> ();
    fn sent(&mut self, request_message: &RequestMessage);
    fn ack_sync(&mut self, from: String, messages: Vec<u32>);
}

#[derive(Debug, Eq, PartialEq, Hash)]
struct Msg {
    id: u32,
    msg: u32,
}

#[derive(Debug)]
pub struct MessageBroadcast {
    pending_acknowledgment: HashMap<String, Vec<Msg>>,
    gossip: HashMap<String, Sync>,
}

impl Default for MessageBroadcast {
    fn default() -> Self {
        MessageBroadcast {
            pending_acknowledgment: HashMap::new(),
            gossip: HashMap::new(),
        }
    }
}

impl Syncable for MessageBroadcast {
    fn sync(&mut self) -> Vec<SyncGossip> {
        // 1. group
        self.pending_acknowledgment
            .iter_mut()
            .for_each(|(dest, msgs_pending_ack)| {
                let old: HashSet<u32> = msgs_pending_ack.iter().map(|msg| msg.msg).collect();
                let gossip_for_dest = self
                    .gossip
                    .entry(dest.clone())
                    .or_insert_with(|| Sync { message: vec![] });

                let existing: HashSet<u32> = gossip_for_dest.message.iter().cloned().collect();
                for msg in old {
                    if !existing.contains(&msg) {
                        gossip_for_dest.message.push(msg);
                    }
                }
                msgs_pending_ack.clear();
            });

        // 2. send
        self.gossip
            .iter()
            .filter(|(_, sync)| !sync.message.is_empty())
            .map(|(dest, sync)| SyncGossip {
                dest: dest.clone(),
                sync: RequestPayload::Sync(sync.clone()),
            })
            .collect()
    }

    fn ack(&mut self, from: &String, msg_id: u32) -> () {
        self.pending_acknowledgment
            .entry(from.clone())
            .and_modify(|msg_ids| {
                if let Some(index) = msg_ids.iter().position(|msg| msg.id == msg_id) {
                    msg_ids.swap_remove(index);
                }
            });
    }

    fn ack_sync(&mut self, from: String, messages: Vec<u32>) -> () {
        let set: HashSet<u32> = messages.into_iter().collect();
        self.gossip.entry(from).and_modify(|sync| {
            sync.message.retain(|msg| !set.contains(msg));
        });
    }

    fn sent(&mut self, request_message: &RequestMessage) -> () {
        match &request_message.body.payload {
            RequestPayload::Broadcast(b) => {
                let msg = Msg {
                    id: request_message.body.msg_id,
                    msg: b.message,
                };
                self.pending_acknowledgment
                    .entry(request_message.dest.clone())
                    .or_default()
                    .push(msg);
            }
            RequestPayload::Sync(_) => {}
            _ => {}
        }
    }
}

pub async fn propagate_broadcast(
    current_node_id: &str,
    exclude_node_id: &str,
    topology_state: &TopologyState,
    broadcast: &Broadcast,
) -> Vec<BroadcastRequest> {
    topology_state
        .get_nearby_nodes(current_node_id)
        .into_iter()
        .filter(|neighbor| neighbor != exclude_node_id)
        .map(|receiver_node| BroadcastRequest {
            dest: receiver_node,
            sync: BroadcastPayload::Broadcast(broadcast.clone()),
        })
        .collect()
}

pub fn handle_broadcast_request(broadcast_payload: BroadcastPayload) -> RequestPayload {
    match broadcast_payload {
        BroadcastPayload::Broadcast(broadcast) => RequestPayload::Broadcast(broadcast),
        BroadcastPayload::Sync(sync) => RequestPayload::Sync(sync),
    }
}
