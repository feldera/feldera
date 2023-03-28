//! Perform tree shaking to remove unreachable nodes from the dataflow graph
//!
//! Nodes will be kept if they're reachable from an input of some kind
//! (sources, constant streams, etc.) and from an output, with a few exceptions,
//! e.g. `antijoin(x, unreachable_or_empty)` is not eliminated and is instead
//! turned into `x`. Side effects also need to be considered, but that's another
//! can of worms
//!
//! So really this isn't a reachability check so much as it's an "produces
//! outputs" check which has slightly different semantics

use crate::ir::{graph::Subgraph, nodes::Node, GraphExt, NodeId};
use petgraph::{
    algo::{toposort, DfsSpace},
    Direction,
};
use std::collections::{BTreeMap, BTreeSet, HashSet};

impl Subgraph {
    pub(super) fn shake_dead_nodes(&mut self) {
        let (mut scratch, mut unreachable) = (DfsSpace::default(), Vec::new());
        self.shake_dead_nodes_inner(&mut scratch, &mut unreachable);
    }

    fn shake_dead_nodes_inner(
        &mut self,
        scratch: &mut DfsSpace<NodeId, HashSet<NodeId>>,
        unreachable: &mut Vec<NodeId>,
    ) {
        debug_assert!(unreachable.is_empty());

        let order = toposort(self.edges(), Some(scratch)).unwrap();

        // TODO: This should be done iteratively, removing dead nodes within subgraphs
        // as well as unused subgraph inputs/outputs
        self.remove_source_unreachable_nodes(&order, unreachable);
        self.remove_sink_unreachable_nodes(&order, unreachable);

        for node in self.nodes_mut().values_mut() {
            if let Node::Subgraph(subgraph) = node {
                subgraph
                    .subgraph_mut()
                    .shake_dead_nodes_inner(scratch, unreachable);
            }
        }
    }

    fn remove_source_unreachable_nodes(&mut self, order: &[NodeId], unreachable: &mut Vec<NodeId>) {
        debug_assert!(unreachable.is_empty());

        let mut source_reachable = BTreeSet::new();
        let mut redundant_antijoins = BTreeMap::new();

        for &node_id in order {
            let node = &self.nodes()[&node_id];

            // An antijoin against an empty stream yields the input stream, e.g.
            // `R ▷ empty ≡ R`
            if let Node::Antijoin(antijoin) = node {
                let lhs_reachable = source_reachable.contains(&antijoin.lhs());
                let rhs_reachable = source_reachable.contains(&antijoin.rhs());

                if lhs_reachable {
                    source_reachable.insert(node_id);
                    if !rhs_reachable {
                        redundant_antijoins.insert(node_id, antijoin.lhs());
                    }

                    continue;
                }
            }

            if node
                .as_constant()
                // Don't mark empty streams as reachable
                .map_or(true, |constant| !constant.value().is_empty())
                && (node.is_source()
                    || node.is_source_map()
                    || node.is_delayed_feedback()
                    || node.is_delta_0()
                    || self
                        .edges()
                        .edges_directed(node_id, Direction::Incoming)
                        .all(|(src, ..)| source_reachable.contains(&src)))
            {
                source_reachable.insert(node_id);
            } else {
                unreachable.push(node_id);
            }
        }

        if !redundant_antijoins.is_empty() {
            // Reroute all antijoins against empty streams to the original stream
            self.map_inputs_mut(|node| {
                if let Some(&redirect) = redundant_antijoins.get(node) {
                    *node = redirect;
                }
            });
        }

        // Remove unreachable nodes
        self.nodes_mut().retain(|node_id, _| {
            if !source_reachable.contains(node_id) {
                tracing::debug!("removing node {node_id} (reason: unreachable from source)");
                false
            } else {
                true
            }
        });

        // Remove unreachable edges
        for node in unreachable.drain(..) {
            tracing::debug!("removing edges for node {node} (reason: unreachable from source)");
            self.edges_mut().remove_node(node);
        }
    }

    fn remove_sink_unreachable_nodes(&mut self, order: &[NodeId], unreachable: &mut Vec<NodeId>) {
        debug_assert!(unreachable.is_empty());

        let mut sink_reachable = BTreeSet::new();
        for &node_id in order.iter().rev() {
            let node = &self.nodes()[&node_id];
            if node.is_sink()
                || self
                    .edges()
                    .edges_directed(node_id, Direction::Outgoing)
                    .any(|(_, dest, _)| sink_reachable.contains(&dest))
            {
                sink_reachable.insert(node_id);
            } else {
                unreachable.push(node_id);
            }
        }

        // Remove unreachable nodes
        self.nodes_mut().retain(|node_id, _| {
            if !sink_reachable.contains(node_id) {
                tracing::debug!("removing node {node_id} (reason: unreachable from sink)");
                false
            } else {
                true
            }
        });

        // Remove unreachable edges
        for node in unreachable.drain(..) {
            tracing::debug!("removing edges for node {node} (reason: unreachable from sink)");
            self.edges_mut().remove_node(node);
        }
    }
}
