mod antijoin_self;
mod distinct;
mod shake;

use crate::ir::{Graph, GraphExt};

// TODO: Pull distincts behind filters where possible
// TODO: Fuse filters, maps and filter maps together
// TODO: Turn zero-or-one flat maps into filter_maps
// TODO: Turn `x - (x ⨝ y)` into `x ▷ y`
// TODO: Turn folds that produce minimum values into `min` nodes
// TODO: Deduplicate constant nodes
// TODO: Deduplicate nodes with identical functions & inputs,
// e.g. deduplicating two different `delta0(x)`s
pub(super) fn optimize_graph(graph: &mut Graph) {
    let graph = graph.graph_mut();

    graph.optimize();
    graph.remove_redundant_distinct();
    graph.remove_self_antijoins();
    graph.shake_dead_nodes();
}
