mod distinct;
mod shake;

use crate::ir::{Graph, GraphExt};

// TODO: Pull distincts behind filters where possible
// TODO: Fuse filters, maps and filter maps together
// TODO: Turn zero-or-one flat maps into filter_maps
// TODO: Turn `x - (x ⨝ y)` into `x ▷ y`
pub(super) fn optimize_graph(graph: &mut Graph) {
    let graph = graph.graph_mut();

    graph.optimize();
    graph.remove_redundant_distinct();
    graph.shake_dead_nodes();
}
