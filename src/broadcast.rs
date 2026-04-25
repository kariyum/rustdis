use crate::{
    Broadcast, Read, RequestBody, RequestMessage, ResponseBody,
    toplogy::{TopologyState, TopologyTrait},
};

pub trait BroadcastConsumer {
    fn handle_broadcast(&mut self, broadcast: Broadcast) -> ResponseBody;
    fn handle_read(&self, read: Read) -> ResponseBody;
}

pub trait BroadcastProducer {
    fn notify_nodes(
        &self,
        local_msg_id: &mut u32,
        src_node_id: String,
        topology: TopologyState,
    ) -> Vec<RequestMessage>;
}

pub struct BroadcastState {
    msgs: Vec<u32>,
}

impl BroadcastConsumer for BroadcastState {
    fn handle_broadcast(&mut self, Broadcast { msg_id, message }: Broadcast) -> ResponseBody {
        self.msgs.push(message);
        ResponseBody::BroadcastOk {
            in_reply_to: msg_id,
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
        BroadcastState { msgs: vec![] }
    }
}

impl BroadcastProducer for BroadcastState {
    fn notify_nodes(
        &self,
        local_msg_id: &mut u32,
        src_node_id: String,
        topology: TopologyState,
    ) -> Vec<RequestMessage> {
        topology
            .get_nearby_nodes(src_node_id.clone())
            .into_iter()
            .zip(self.msgs.clone().into_iter()) // TODO update logic it shouldn't be zip here
            .map(|(receiver_node, msg)| RequestMessage {
                src: src_node_id.clone(),
                dest: receiver_node.clone(),
                body: RequestBody::Broadcast(Broadcast {
                    msg_id: local_msg_id.clone(),
                    message: msg.clone(),
                }),
            })
            .collect()
    }
}
