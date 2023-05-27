use crate::ir::{graph::Subgraph, nodes::Node, GraphExt};
use petgraph::Direction;
use std::collections::BTreeMap;

impl Subgraph {
    pub(super) fn passthrough_sums(&mut self) {
        let mut passthrough_sums = BTreeMap::new();

        // Collect all passthrough sums, e.g. `sum([x])`
        for (&node_id, node) in self.nodes_mut() {
            match node {
                Node::Sum(sum) => {
                    if let &[input] = sum.inputs() {
                        passthrough_sums.insert(node_id, input);
                    }
                }

                Node::Subgraph(subgraph) => subgraph.subgraph_mut().passthrough_sums(),

                _ => {}
            }
        }

        // Remove all uses of the redundant sum node with its single input
        if !passthrough_sums.is_empty() {
            self.map_inputs_mut(|node| {
                if let Some(&redirect) = passthrough_sums.get(node) {
                    *node = redirect;
                }
            });

            let mut edges = Vec::new();
            for (old_node, new_node) in passthrough_sums {
                edges.extend(
                    self.edges_mut()
                        .edges_directed(old_node, Direction::Outgoing)
                        .map(|(src, dest, _)| (src, dest)),
                );

                for (src, dest) in edges.drain(..) {
                    self.edges_mut().remove_edge(src, dest);
                    self.edges_mut().add_edge(new_node, dest, ());
                }
            }
        }
    }
}
