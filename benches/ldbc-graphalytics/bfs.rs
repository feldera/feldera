//! An implementation of LDBC Graphalytics Breadth-First Search
//!
//! Benchmark specified in LDBC reference sections [2.3.1] and [A.1].
//!
//! [2.3.1]: https://arxiv.org/pdf/2011.15028v4.pdf#subsection.2.3.1
//! [A.1]: https://arxiv.org/pdf/2011.15028v4.pdf#section.A.1

use crate::data::{Distance, DistanceSet, Edges, Node, Vertices};
use dbsp::{operator::recursive::RecursiveStreams, time::NestedTimestamp32, Circuit, Stream};

type Distances<P> = Stream<Circuit<P>, DistanceSet>;

// TODO: This straight up won't work on the quantities of data required by the
//       higher scale factor benchmarks, there's simply too much data. We'll
//       have to process batched data sourced from a custom operator that slowly
//       loads the input data so we don't OOM
pub fn bfs<P>(roots: Vertices<P>, vertices: Vertices<P>, edges: Edges<P>) -> Distances<P>
where
    P: Clone + 'static,
    Distances<Circuit<P>>: RecursiveStreams<Circuit<Circuit<P>>, Output = Distances<P>>,
{
    // Initialize the roots to have a distance of zero
    let root_nodes = roots.index_with_generic::<DistanceSet, _>(|&root| ((root, 0), ()));

    let distances = root_nodes
        .circuit()
        // TODO: Can recurse over the indexed version of `nodes` instead of having to re-index it?
        .recursive(|scope, nodes: Distances<_>| {
            // Import the nodes and edges into the recursive scope
            let root_nodes = root_nodes.delta0(scope);
            let edges = edges.delta0(scope);

            let distances = nodes
                .index::<Node, Distance>()
                // Iterate over each edge within the graph, increasing the distance on each step
                .join::<NestedTimestamp32, _, _, DistanceSet>(&edges, |_, &dist, &dest| {
                    (dest, dist + 1)
                })
                // Add in the root nodes
                .plus(&root_nodes)
                .index::<Node, Distance>()
                // Select only the shortest distance to continue iterating with, `distances`
                // is sorted so we can simply select the first element
                .aggregate_incremental_nested(|&node, distances| (node, *distances[0].0));

            Ok(distances)
        })
        .expect("failed to build dfs recursive scope");

    // Collect all reachable nodes
    let reachable_nodes = distances.map_keys(|&(node, _)| node);
    // Find all unreachable nodes (vertices not included in `distances`) and give them a weight of -1
    let unreachable_nodes =
        antijoin(&vertices, &reachable_nodes).map_keys(|&node| (node, i64::MAX as Distance));

    distances.plus(&unreachable_nodes)
}

// FIXME: Replace with a dedicated antijoin
fn antijoin<P>(vertices: &Vertices<P>, reachable_nodes: &Vertices<P>) -> Vertices<P>
where
    P: Clone + 'static,
{
    let reachable_nodes = vertices.semijoin::<(), _, _, _>(reachable_nodes, |&vertex, &()| vertex);
    vertices.minus(&reachable_nodes)
}
