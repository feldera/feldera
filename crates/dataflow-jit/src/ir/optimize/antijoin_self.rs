use crate::ir::{graph::Subgraph, nodes::Node, GraphExt};
use std::collections::BTreeMap;

impl Subgraph {
    pub(super) fn remove_self_antijoins(&mut self) {
        // Collect all self-antijoins, e.g. `R ▷ R`
        let mut redundant_antijoins = Vec::new();
        for (&node_id, node) in self.nodes_mut() {
            match node {
                Node::Antijoin(antijoin) => {
                    if antijoin.lhs() == antijoin.rhs() {
                        redundant_antijoins.push(node_id)
                    }
                }

                Node::Subgraph(subgraph) => subgraph.subgraph_mut().remove_self_antijoins(),

                _ => {}
            }
        }

        // Fetch/create empty streams and replace all uses of self-antijoins with
        // uses of empty streams, later passes will remove nodes that depend on empty
        // streams
        if !redundant_antijoins.is_empty() {
            let mut replacements = BTreeMap::new();
            for antijoin in redundant_antijoins {
                let layout = self.nodes()[&antijoin].as_antijoin().unwrap().layout();
                let empty = self.empty_stream(layout);
                replacements.insert(antijoin, empty);
            }

            self.map_inputs_mut(|node| {
                if let Some(&redirect) = replacements.get(node) {
                    *node = redirect;
                }
            });
        }
    }
}
