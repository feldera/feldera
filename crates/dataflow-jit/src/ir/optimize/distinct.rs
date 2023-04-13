//! Remove distinct over distinct streams

use crate::ir::{graph::Subgraph, literal::StreamLiteral, nodes::Node, GraphExt, NodeId};
use petgraph::{
    algo::{toposort, DfsSpace},
    Direction,
};
use std::collections::{BTreeMap, BTreeSet, HashSet};

impl Subgraph {
    pub(super) fn remove_redundant_distinct(&mut self) {
        let mut redirects = BTreeMap::new();

        {
            let mut is_distinct = BTreeSet::new();
            let mut buffer = DfsSpace::default();

            // Iteratively collect distinct nodes and propagate their distinct-ness
            // as much as possible (in and out of subgraphs, backwards through feedbacks,
            // etc.)
            self.collect_distinct_nodes(&mut is_distinct, &mut redirects, &mut buffer);
        }

        if !redirects.is_empty() {
            // Perform all redirects, redirecting consumers of a redundant nodes
            // (e.g. turning `distinct(distinct(x))` into `distinct(x)`)
            self.map_inputs_mut(|node| {
                if let Some(&redirect) = redirects.get(node) {
                    *node = redirect;
                }
            });

            let mut edges = Vec::new();
            for (old_node, new_node) in redirects {
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

    fn collect_distinct_nodes(
        &self,
        is_distinct: &mut BTreeSet<NodeId>,
        redirects: &mut BTreeMap<NodeId, NodeId>,
        buffer: &mut DfsSpace<NodeId, HashSet<NodeId>>,
    ) -> bool {
        // TODO: Could reuse toposort vectors
        let order = toposort(self.edges(), Some(buffer)).expect("cyclic dataflow graph");

        let mut changed = false;
        for _ in 0..100 {
            if !self.collect_distinct_nodes_inner(&order, is_distinct, redirects, buffer) {
                break;
            } else {
                changed = true;
            }
        }

        changed
    }

    fn collect_distinct_nodes_inner(
        &self,
        order: &[NodeId],
        is_distinct: &mut BTreeSet<NodeId>,
        redirects: &mut BTreeMap<NodeId, NodeId>,
        buffer: &mut DfsSpace<NodeId, HashSet<NodeId>>,
    ) -> bool {
        let mut changed = false;

        // Collect all distinct nodes and splices that we need to make (replace
        // now-redundant nodes)
        for &node_id in order {
            // TODO: Look at the inside of `Constant`s to see if they're distinct
            // TODO: Antijoins don't effect distinct (I think?)
            match &self.nodes()[&node_id] {
                // Distinct produces a distinct stream
                Node::Distinct(distinct) => {
                    // Mark this node as distinct
                    if is_distinct.insert(node_id) {
                        changed = true;
                        tracing::trace!("marking distinct node {node_id} as distinct");

                        // If the input to the distinct node is itself distinct, eliminate this
                        // distinct node
                        if is_distinct.contains(&distinct.input()) {
                            tracing::trace!(
                                "distinct node {node_id} is redundant, its input {} is distinct",
                                distinct.input(),
                            );

                            redirects.insert(node_id, distinct.input());
                        }
                    }
                }

                // Min and Max preserve the distinct-ness of their input streams
                Node::Min(min) => {
                    if is_distinct.contains(&min.input()) && is_distinct.insert(node_id) {
                        tracing::trace!(
                            "marking min node {node_id} as distinct, its input stream {} is distinct",
                            min.input(),
                        );
                        changed = true;
                    }
                }
                Node::Max(max) => {
                    if is_distinct.contains(&max.input()) && is_distinct.insert(node_id) {
                        tracing::trace!(
                            "marking max node {node_id} as distinct, its input stream {} is distinct",
                            max.input(),
                        );
                        changed = true;
                    }
                }

                // Filter preserves the distinct-ness of its input stream
                Node::Filter(filter) => {
                    if is_distinct.contains(&filter.input()) && is_distinct.insert(node_id) {
                        tracing::trace!(
                            "marking filter node {node_id} as distinct, its input stream {} is distinct",
                            filter.input(),
                        );
                        changed = true;
                    }
                }

                // Delta0 preserves the distinct-ness of its input stream
                Node::Delta0(delta0) => {
                    if is_distinct.contains(&delta0.input()) && is_distinct.insert(node_id) {
                        tracing::trace!(
                            "marking delta0 node {node_id} as distinct, its input stream {} is distinct",
                            delta0.input(),
                        );
                        changed = true;
                    }
                }

                // Antijoin preserves it's left hand stream's distinct-ness
                // (distinct is automatically applied to the right hand stream)
                Node::Antijoin(antijoin) => {
                    if is_distinct.contains(&antijoin.lhs()) && is_distinct.insert(node_id) {
                        tracing::trace!(
                            "marking antijoin node {node_id} as distinct, its left hand input stream {} is distinct",
                            antijoin.lhs(),
                        );
                        changed = true;
                    }
                }

                Node::Subgraph(subgraph) => {
                    // Propagate input distincts
                    for (input, &inner) in subgraph.input_nodes() {
                        if is_distinct.contains(input) {
                            changed |= is_distinct.insert(inner);
                        }
                    }

                    // Run distinct propagation within subgraphs
                    changed |=
                        subgraph
                            .subgraph()
                            .collect_distinct_nodes(is_distinct, redirects, buffer);

                    // FIXME: This is unsound since the stream given to a feedback node
                    // could depend on the distinct-ness of the feedback itself, in the
                    // case of
                    // ```
                    // feedback_head = feedback()
                    // x = feedback_head.filter(...) // Preserves distinct-ness
                    // x_distinct = x.distinct()
                    // feedback_head.connect(x_distinct)
                    // ```
                    // The `x.distinct()` would be removed since `x` is already distinct
                    // since filter preserves distinct-ness, causing `x.distinct()` to be
                    // replaced with `x` and making `feedback_head` no longer distinct.
                    //
                    // // Propagate distinct back through feedback nodes
                    // for (source, &feedback) in subgraph.feedback_connections() {
                    //     if is_distinct.contains(source) {
                    //         changed |= is_distinct.insert(feedback);
                    //     }
                    // }

                    // Propagate distinct out of subgraphs
                    for (inner, &output) in subgraph.output_nodes() {
                        if is_distinct.contains(inner) {
                            changed |= is_distinct.insert(output);
                        }
                    }
                }

                Node::Constant(constant) => {
                    if constant.consolidated() && !is_distinct.contains(&node_id) {
                        // If all tuples/rows within the stream have a weight of 1, the stream is
                        // distinct
                        let constant_is_distinct = match constant.value() {
                            StreamLiteral::Set(set) => set.iter().all(|&(_, weight)| weight == 1),
                            StreamLiteral::Map(map) => map.iter().all(|&(.., weight)| weight == 1),
                        };

                        if constant_is_distinct {
                            let assert = is_distinct.insert(node_id);
                            debug_assert!(assert);

                            tracing::trace!("marking constant node {node_id} as distinct");
                            changed = true;
                        }
                    }
                }

                _ => {}
            }
        }

        changed
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        ir::{ColumnType, Constant, Graph, GraphExt, RowLayoutBuilder},
        utils,
    };

    #[test]
    fn distinct_propagation() {
        utils::test_logger();

        let mut graph = Graph::new();

        let u32 = graph.layout_cache().add(
            RowLayoutBuilder::new()
                .with_column(ColumnType::U32, false)
                .build(),
        );

        let source = graph.source(u32);
        let distinct_1 = graph.distinct(source);
        let filtered = graph.filter(distinct_1, {
            let mut builder = graph.function_builder().with_return_type(ColumnType::Bool);
            let input = builder.add_input(u32);

            let input = builder.load(input, 0);
            let one_hundred = builder.constant(Constant::U32(100));
            let less_than = builder.lt(input, one_hundred);
            builder.ret(less_than);
            builder.build()
        });
        let distinct_2 = graph.distinct(filtered);
        let sink = graph.sink(distinct_2);

        graph.optimize();

        assert_eq!(graph.nodes()[&sink].clone().unwrap_sink().input(), filtered);
    }
}
