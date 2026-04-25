use crate::{Broadcast, Read, ResponseBody};

pub trait BroadcastTrait {
    fn handle_broadcast(&mut self, broadcast: Broadcast) -> ResponseBody;
    fn handle_read(&self, read: Read) -> ResponseBody;
}

pub struct BroadcastState {
    msgs: Vec<u32>,
}

impl BroadcastTrait for BroadcastState {
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
