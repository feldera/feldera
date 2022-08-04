//! An implementation of LDBC Graphalytics Breadth-First Search
//!
//! Benchmark specified in LDBC reference sections [2.3.1] and [A.1].
//!
//! [2.3.1]: https://arxiv.org/pdf/2011.15028v4.pdf#subsection.2.3.1
//! [A.1]: https://arxiv.org/pdf/2011.15028v4.pdf#section.A.1

use crate::data::{Distance, DistanceSet, EdgeMap, Edges, Node, VertexSet, Vertices, Weight};
use dbsp::{
    algebra::HasOne,
    operator::{recursive::RecursiveStreams, FilterMap},
    time::NestedTimestamp32,
    trace::{Batch, BatchReader, Builder, Cursor},
    Circuit, Stream,
};

type Distances<P> = Stream<Circuit<P>, DistanceSet<Weight>>;

// TODO: This straight up won't work on the quantities of data required by the
//       higher scale factor benchmarks, there's simply too much data. We'll
//       have to process batched data sourced from a custom operator that slowly
//       loads the input data so we don't OOM
pub fn bfs<P>(roots: Vertices<P>, vertices: Vertices<P>, edges: Edges<P>) -> Distances<P>
where
    P: Clone + 'static,
    Distances<Circuit<P>>: RecursiveStreams<Circuit<Circuit<P>>, Output = Distances<P>>,
{
    let vertices = vertices.apply(|vertices| {
        let mut builder = <VertexSet<Weight> as Batch>::Builder::with_capacity((), vertices.len());

        let mut cursor = vertices.cursor();
        while cursor.key_valid() {
            builder.push((*cursor.key(), (), Weight::one()));
            cursor.step_key();
        }

        builder.done()
    });
    let edges = edges.apply(|edges| {
        let mut builder = <EdgeMap<Weight> as Batch>::Builder::with_capacity((), edges.len());

        let mut cursor = edges.cursor();
        while cursor.key_valid() {
            while cursor.val_valid() {
                builder.push((*cursor.key(), *cursor.val(), Weight::one()));
                cursor.step_val();
            }
            cursor.step_key();
        }

        builder.done()
    });

    // Initialize the roots to have a distance of zero
    let root_nodes = roots.apply(|vertices| {
        let mut builder =
            <DistanceSet<Weight> as Batch>::Builder::with_capacity((), vertices.len());

        let mut cursor = vertices.cursor();
        while cursor.key_valid() {
            builder.push(((*cursor.key(), 0), (), Weight::one()));
            cursor.step_key();
        }

        builder.done()
    });

    let distances = root_nodes
        .circuit()
        // TODO: Can recurse over the indexed version of `nodes` instead of having to re-index it?
        // TODO: Can we use zst weights here?
        .recursive(|scope, nodes: Distances<_>| {
            // Import the nodes and edges into the recursive scope
            let root_nodes = root_nodes.delta0(scope);
            let edges = edges.delta0(scope);

            let distances = nodes
                .index::<Node, Distance>()
                // Iterate over each edge within the graph, increasing the distance on each step
                .join::<NestedTimestamp32, _, _, DistanceSet<Weight>>(&edges, |_, &dist, &dest| {
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
    let reachable_nodes = distances.map(|&(node, _)| node);
    // Find all unreachable nodes (vertices not included in `distances`) and give
    // them a weight of -1
    let unreachable_nodes =
        antijoin(&vertices, &reachable_nodes).map(|&node| (node, i64::MAX as Distance));

    distances.plus(&unreachable_nodes)
}

// FIXME: Replace with a dedicated antijoin
fn antijoin<P>(
    vertices: &Vertices<P, Weight>,
    reachable_nodes: &Vertices<P, Weight>,
) -> Vertices<P, Weight>
where
    P: Clone + 'static,
{
    let reachable_nodes =
        vertices.stream_join::<_, _, _>(reachable_nodes, |&vertex, &(), &()| vertex);
    vertices.minus(&reachable_nodes)
}
