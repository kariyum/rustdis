use std::collections::HashMap;

use crate::{ResponsePayload, Topology};

pub trait TopologyTrait {
    fn handle_toplogy(&mut self, topology: Topology) -> ResponsePayload;

    fn get_nearby_nodes(&self, src_node_id: &str) -> Vec<String>;
}

pub struct TopologyState(pub HashMap<String, Vec<String>>);

impl TopologyTrait for TopologyState {
    fn handle_toplogy(&mut self, topology: Topology) -> ResponsePayload {
        self.0 = topology.topology;
        ResponsePayload::TopologyOk
    }

    fn get_nearby_nodes(&self, src_node_id: &str) -> Vec<String> {
        self.0
            .get(src_node_id)
            .unwrap_or(&vec![])
            .clone()
            .into_iter()
            .filter(|node_id| *node_id != src_node_id.to_string())
            .collect()
    }
}

impl Default for TopologyState {
    fn default() -> Self {
        TopologyState(HashMap::default())
    }
}
