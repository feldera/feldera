//! An implementation of LDBC Graphalytics Breadth-First Search
//!
//! Benchmark specified in LDBC reference sections [2.3.1] and [A.1].
//!
//! [2.3.1]: https://arxiv.org/pdf/2011.15028v4.pdf#subsection.2.3.1
//! [A.1]: https://arxiv.org/pdf/2011.15028v4.pdf#section.A.1

use crate::data::{Distance, DistanceMap, DistanceSet, EdgeMap, Edges, Node, VertexSet, Vertices};
use dbsp::utils::Tup2;
use dbsp::{
    circuit::WithClock,
    operator::{recursive::RecursiveStreams, FilterMap, Min},
    trace::{Batch, BatchReader, Builder, Cursor},
    ChildCircuit, DBTimestamp, Stream, Timestamp,
};
use std::iter::once;

type Distances<P, D = i32> = Stream<ChildCircuit<P>, DistanceSet<D>>;

// TODO: This straight up won't work on the quantities of data required by the
//       higher scale factor benchmarks, there's simply too much data. We'll
//       have to process batched data sourced from a custom operator that slowly
//       loads the input data so we don't OOM
pub fn bfs<P>(roots: Vertices<P>, vertices: Vertices<P>, edges: Edges<P>) -> Distances<P, i8>
where
    P: WithClock + Clone + 'static,
    <<P as WithClock>::Time as Timestamp>::Nested: DBTimestamp,
    <<<P as WithClock>::Time as Timestamp>::Nested as Timestamp>::Nested: DBTimestamp,
    Distances<ChildCircuit<P>>:
        RecursiveStreams<ChildCircuit<ChildCircuit<P>>, Output = Distances<P>>,
{
    // Initialize the roots to have a distance of zero
    let roots = roots
        .apply(|roots| {
            let mut builder = <DistanceSet<i32> as Batch>::Builder::with_capacity((), roots.len());

            let mut cursor = roots.cursor();
            while cursor.key_valid() {
                builder.push((Tup2(*cursor.key(), 0), 1));
                cursor.step_key();
            }

            builder.done()
        })
        .shard();

    let edges = edges
        .apply(|edges| {
            let mut builder = <EdgeMap<i32> as Batch>::Builder::with_capacity((), edges.len());

            let mut cursor = edges.cursor();
            while cursor.key_valid() {
                while cursor.val_valid() {
                    builder.push(((*cursor.key(), *cursor.val()), 1));
                    cursor.step_val();
                }
                cursor.step_key();
            }

            builder.done()
        })
        .shard();

    let distances = roots
        .circuit()
        // TODO: Can recurse over the indexed version of `nodes` instead of having to re-index it?
        // TODO: Can we use zst weights here?
        .recursive(|scope, nodes: Distances<_>| {
            // Import the nodes and edges into the recursive scope
            let roots = roots.delta0(scope);
            let edges = edges.delta0(scope);

            let distances = nodes
                .index()
                // Iterate over each edge within the graph, increasing the distance on each step
                .join_generic(&edges, |_, &dist, &dest| once((Tup2(dest, dist + 1), ())))
                // Add in the root nodes
                .plus(&roots)
                .index::<Node, Distance>()
                // Select only the shortest distance to continue iterating.
                .aggregate_generic::<_, DistanceMap<i32>>(Min)
                .map(|(&node, &distance)| Tup2(node, distance));
            Ok(distances)
        })
        .expect("failed to build dfs recursive scope");

    // Convert the `i32` weights of distances to integer weights so we can use
    // negation on it
    //
    // TODO: Could probably be optimized to use `.apply_owned()` and take the key
    //       vector from the input
    // TODO: Ideally we'd use a bitset instead of a `Vec<i8>` here
    let distances = distances
        .shard()
        .apply(|distances| {
            let mut builder =
                <DistanceSet<i8> as Batch>::Builder::with_capacity((), distances.len());

            let mut cursor = distances.cursor();
            while cursor.key_valid() {
                builder.push((*cursor.key(), 1));
                cursor.step_key();
            }

            builder.done()
        })
        .mark_sharded();

    // Add weights to each vertex
    // TODO: Ideally we could actually just use `i32` weights for this too
    let vertices = vertices
        .apply(|vertices| {
            let mut builder = <VertexSet<i8> as Batch>::Builder::with_capacity((), vertices.len());

            let mut cursor = vertices.cursor();
            while cursor.key_valid() {
                builder.push((*cursor.key(), 1));
                cursor.step_key();
            }

            builder.done()
        })
        .mark_sharded();

    // Collect all reachable nodes
    let reachable_nodes = distances.map(|&Tup2(node, _)| node);
    // Find all unreachable nodes (vertices not included in `distances`) and give
    // them a weight of -1
    let unreachable_nodes =
        antijoin(&vertices, &reachable_nodes).map(|&node| Tup2(node, i64::MAX as Distance));

    distances.plus(&unreachable_nodes)
}

// FIXME: Replace with a dedicated antijoin
fn antijoin<P>(vertices: &Vertices<P, i8>, reachable_nodes: &Vertices<P, i8>) -> Vertices<P, i8>
where
    P: WithClock + Clone + 'static,
{
    let reachable_nodes =
        vertices.monotonic_stream_join::<_, _, _>(reachable_nodes, |&vertex, &(), &()| vertex);
    vertices.minus(&reachable_nodes)
}
