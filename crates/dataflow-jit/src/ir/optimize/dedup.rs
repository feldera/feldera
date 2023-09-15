//! Node deduplication

use crate::ir::{
    graph::Subgraph,
    nodes::{ConstantStream, Distinct, StreamDistinct, Subgraph as SubgraphNode},
    visit::MutNodeVisitor,
    GraphExt, NodeId,
};
use petgraph::Direction;
use std::collections::{btree_map::Entry, BTreeMap};

impl Subgraph {
    pub(super) fn dedup_nodes(&mut self) {
        let mut collector = NodeCollector::default();
        self.accept_mut(&mut collector);

        let NodeCollector { replacements, .. } = collector;
        if !replacements.is_empty() {
            tracing::debug!("dedup removed {} nodes", replacements.len());

            self.map_inputs_mut(|node_id| {
                if let Some(&replacement) = replacements.get(node_id) {
                    *node_id = replacement;
                }
            });

            let mut edges = Vec::new();
            for (old_node, new_node) in replacements {
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

/// A limited visitor to collect and deduplicate nodes
#[derive(Debug, Default)]
struct NodeCollector {
    constant: BTreeMap<ConstantStream, NodeId>,
    distinct: BTreeMap<Distinct, NodeId>,
    stream_distinct: BTreeMap<StreamDistinct, NodeId>,
    replacements: BTreeMap<NodeId, NodeId>,
}

impl MutNodeVisitor for NodeCollector {
    fn visit_constant(&mut self, node_id: NodeId, constant: &mut ConstantStream) {
        if let Some(&canon) = self.constant.get(constant) {
            tracing::trace!("deduplicating constant nodes {node_id} and {canon}");
            self.replacements.insert(node_id, canon);
        } else {
            self.constant.insert(constant.clone(), node_id);
        }
    }

    fn visit_distinct(&mut self, node_id: NodeId, distinct: &mut Distinct) {
        match self.distinct.entry(distinct.clone()) {
            Entry::Vacant(vacant) => {
                vacant.insert(node_id);
            }
            Entry::Occupied(occupied) => {
                tracing::trace!(
                    "deduplicating distinct nodes {node_id} and {}",
                    occupied.get(),
                );
                self.replacements.insert(node_id, *occupied.get());
            }
        }
    }

    fn visit_stream_distinct(&mut self, node_id: NodeId, distinct: &mut StreamDistinct) {
        match self.stream_distinct.entry(distinct.clone()) {
            Entry::Vacant(vacant) => {
                vacant.insert(node_id);
            }
            Entry::Occupied(occupied) => {
                tracing::trace!(
                    "deduplicating stream_distinct nodes {node_id} and {}",
                    occupied.get(),
                );
                self.replacements.insert(node_id, *occupied.get());
            }
        }
    }

    fn visit_subgraph(&mut self, _node_id: NodeId, subgraph: &mut SubgraphNode) {
        subgraph.subgraph_mut().dedup_nodes();
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        ir::{
            literal::{NullableConstant, RowLiteral, StreamCollection, StreamLiteral},
            nodes::{ConstantStream, StreamLayout},
            ColumnType, Constant, Graph, GraphExt, RowLayoutBuilder,
        },
        utils,
    };

    #[test]
    fn node_deduplication() {
        utils::test_logger();

        let mut graph = Graph::new();

        let u32 = graph.layout_cache().add(
            RowLayoutBuilder::new()
                .with_column(ColumnType::U32, false)
                .build(),
        );

        let source = graph.source(u32);
        let distinct2 = graph.distinct(source, StreamLayout::Set(u32));
        let distinct3 = graph.distinct(source, StreamLayout::Set(u32));
        let sink1 = graph.sink(distinct2, "D2", StreamLayout::Set(u32));
        let sink2 = graph.sink(distinct3, "D3", StreamLayout::Set(u32));

        let constant = StreamLiteral::new(
            StreamLayout::Set(u32),
            StreamCollection::Set(vec![(
                RowLiteral::new(vec![NullableConstant::NonNull(Constant::U32(1))]),
                1,
            )]),
        );
        let empty1 = graph.add_node(ConstantStream::new(constant.clone()));
        let empty2 = graph.add_node(ConstantStream::new(constant));
        let sink3 = graph.sink(empty1, "E1", StreamLayout::Set(u32));
        let sink4 = graph.sink(empty2, "E2", StreamLayout::Set(u32));

        graph.optimize();

        assert_eq!(
            graph.nodes()[&sink1].clone().unwrap_sink().input(),
            graph.nodes()[&sink2].clone().unwrap_sink().input(),
        );
        assert_eq!(
            graph.nodes()[&sink3].clone().unwrap_sink().input(),
            graph.nodes()[&sink4].clone().unwrap_sink().input(),
        );
    }
}
