use std::collections::HashSet;

use crate::{
    Broadcast, Read, RequestBody, RequestMessage, ResponseBody,
    toplogy::{TopologyState, TopologyTrait},
};
pub trait BroadcastConsumer {
    fn consume_broadcast(&mut self, message: u32) -> bool;
    fn handle_read(&self, read: Read) -> ResponseBody;
}

pub trait BroadcastProducer {
    fn notify_nodes(
        &mut self,
        local_msg_id: &mut u32,
        src_node_id: String,
        topology: &TopologyState,
        broadcast: Broadcast,
    ) -> Vec<RequestMessage>;
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

    fn handle_read(&self, read: Read) -> ResponseBody {
        ResponseBody::ReadOk {
            in_reply_to: read.msg_id,
            messages: self.msgs.clone(),
        }
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

impl BroadcastProducer for BroadcastState {
    fn notify_nodes(
        &mut self,
        local_msg_id: &mut u32,
        src_node_id: String,
        topology_state: &TopologyState,
        broadcast: Broadcast,
    ) -> Vec<RequestMessage> {
        if self.msg_ids.insert(broadcast.message) {
            *local_msg_id = *local_msg_id + 1;
            topology_state
                .get_nearby_nodes(src_node_id.clone())
                .iter()
                .map(|receiver_node| RequestMessage {
                    id: *local_msg_id,
                    src: src_node_id.clone(),
                    dest: receiver_node.clone(),
                    body: RequestBody::Broadcast(broadcast.clone()),
                })
                .collect()
        } else {
            vec![]
        }
    }
}

pub async fn propagate_broadcast(
    local_msg_id: &mut u32,
    src_node_id: String,
    topology_state: &TopologyState,
    broadcast: Broadcast,
) -> Vec<RequestMessage> {
    topology_state
        .get_nearby_nodes(src_node_id.clone())
        .iter()
        .map(|receiver_node| {
            *local_msg_id = *local_msg_id + 1;
            RequestMessage {
                id: *local_msg_id,
                src: src_node_id.clone(),
                dest: receiver_node.clone(),
                body: RequestBody::Broadcast(Broadcast {
                    msg_id: *local_msg_id,
                    message: broadcast.message,
                }),
            }
        })
        .collect()
}
