use std::collections::HashMap;

use crate::{ResponseBody, Topology};

pub trait TopologyTrait {
    fn handle_toplogy(&mut self, topology: Topology) -> ResponseBody;

    fn get_nearby_nodes(&self, src_node_id: String) -> Vec<String>;
}

pub struct TopologyState(pub HashMap<String, Vec<String>>);

impl TopologyTrait for TopologyState {
    fn handle_toplogy(&mut self, topology: Topology) -> ResponseBody {
        self.0 = topology.topology;
        ResponseBody::TopologyOk {
            in_reply_to: topology.msg_id,
        }
    }

    fn get_nearby_nodes(&self, src_node_id: String) -> Vec<String> {
        self.0.get(&src_node_id).unwrap_or(&vec![]).clone()
    }
}

impl Default for TopologyState {
    fn default() -> Self {
        TopologyState(HashMap::default())
    }
}
