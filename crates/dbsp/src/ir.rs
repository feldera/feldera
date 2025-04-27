use std::collections::{BTreeMap, BTreeSet};

use feldera_ir::{LirCircuit, LirEdge, LirNode, LirStreamId, MirNodeId};

use crate::{
    circuit::{circuit_builder::CircuitBase, GlobalNodeId, NodeId},
    Circuit, Stream,
};

/// Node label indicating that this node implements the output of an
/// MIR operator.
pub const LABEL_MIR_NODE_ID: &str = "mir_node";

impl<C, T> Stream<C, T>
where
    C: Circuit,
{
    /// Sets `id` as both the MIR node id and persistent id of the
    /// source node of the stream.
    pub fn set_persistent_mir_id(&self, id: &str) {
        // There can be at most one MIR label per node.
        assert!(self.get_label(LABEL_MIR_NODE_ID).is_none());
        self.set_label(LABEL_MIR_NODE_ID, id);
        self.set_persistent_id(Some(id));
    }

    /// Get the MIR node id label if any.
    pub fn get_mir_node_id(&self) -> Option<String> {
        self.get_label(LABEL_MIR_NODE_ID)
    }
}

impl dyn CircuitBase {
    fn lir_edges(&self) -> Vec<LirEdge> {
        self.edges()
            .iter()
            .map(|edge| LirEdge {
                stream_id: edge
                    .stream
                    .as_ref()
                    .map(|stream| LirStreamId::new(stream.stream_id().id())),
                from: edge.origin.lir_node_id(),
                to: self.global_id().child(edge.to).lir_node_id(),
            })
            .collect()
    }

    fn lir_edges_recursive(&self) -> Vec<LirEdge> {
        let mut lir_edges = self.lir_edges();
        self.map_subcircuits(&mut |circuit| {
            let mut edges = circuit.lir_edges_recursive();
            lir_edges.append(&mut edges);
            Ok(())
        })
        .unwrap();

        lir_edges
    }

    /// For each node in the circuit, compute the set of MIR nodes that it implements (partially or completely).
    ///
    /// Invokes the method recursively for all subcircuits.
    fn mir_refinement_map(&self, refinement_map: &mut BTreeMap<GlobalNodeId, BTreeSet<String>>) {
        // Collect all nodes wih MIR labels.
        let mut mir_nodes: Vec<(NodeId, GlobalNodeId, MirNodeId)> = Vec::new();

        self.map_local_nodes(&mut |node| {
            if let Some(mir_node) = node.get_label(LABEL_MIR_NODE_ID) {
                mir_nodes.push((
                    node.local_id(),
                    node.global_id().clone(),
                    mir_node.to_string(),
                ));
            };
            Ok(())
        })
        .unwrap();

        // Mark all ancestors reachable before hitting another node with MIR label as
        // implementing the same MIR node.
        for (node_id, global_id, mir_node_id) in mir_nodes {
            // New nodes discovered at each iteration.
            let mut frontier: BTreeSet<NodeId> = BTreeSet::new();

            // All traversed nodes.
            let mut nodes: BTreeSet<NodeId> = BTreeSet::new();

            frontier.insert(node_id);
            nodes.insert(node_id);

            // Add the node itself to the refinement map.
            refinement_map
                .entry(global_id.clone())
                .or_default()
                .insert(mir_node_id.clone());

            while !frontier.is_empty() {
                let mut new_frontier: BTreeSet<NodeId> = BTreeSet::new();

                for node_id in frontier {
                    // All immediate predecessors.
                    let inputs = self
                        .edges()
                        .inputs_of(node_id)
                        .map(|edge| edge.from)
                        .collect::<Vec<_>>();

                    // Nodes with a depenency on the current node. Ensures that we include the
                    // input half of strict operators like Z1.
                    let depentents = self
                        .edges()
                        .depend_on(node_id)
                        .map(|edge| edge.to)
                        .collect::<Vec<_>>();

                    for &node_id in inputs.iter().chain(depentents.iter()) {
                        // Ignore nodes that have their own MIR labels and subcircuits.
                        // (the assumption is that output nodes of a subcircuit should have their
                        // own MIR labels, so we shouldn't need to traverse into the subcircuit).
                        let mut include = false;
                        self.apply_local_node_mut(node_id, &mut |node| {
                            include =
                                node.get_label(LABEL_MIR_NODE_ID).is_none() && !node.is_circuit()
                        });

                        if include {
                            let global_id = self.global_id().child(node_id);

                            if !nodes.contains(&node_id) {
                                nodes.insert(node_id);
                                new_frontier.insert(node_id);
                                refinement_map
                                    .entry(global_id)
                                    .or_default()
                                    .insert(mir_node_id.clone());
                            }
                        }
                    }
                }

                frontier = new_frontier;
            }

            // Process child circuits.
            self.map_subcircuits(&mut |subcircuit| {
                subcircuit.mir_refinement_map(refinement_map);
                Ok(())
            })
            .unwrap();
        }
    }

    /// Export circuit in lir format.
    pub fn to_lir(&self) -> LirCircuit {
        let mut refinement_map = BTreeMap::new();
        self.mir_refinement_map(&mut refinement_map);

        let mut nodes: Vec<LirNode> = Vec::new();

        self.map_nodes_recursive(&mut |node| {
            let lir_node = LirNode {
                id: node.global_id().lir_node_id(),
                operation: node.name().into_owned(),
                implements: refinement_map
                    .get(node.global_id())
                    .into_iter()
                    .flatten()
                    .cloned()
                    .collect(),
            };
            nodes.push(lir_node);
            Ok(())
        })
        .unwrap();

        let edges = self.lir_edges_recursive();
        LirCircuit { nodes, edges }
    }
}
