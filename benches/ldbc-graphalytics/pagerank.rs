use crate::data::{Edges, Node, Rank, RankPairs, RankSet, Ranks, Streamed, Vertices};
use dbsp::{
    algebra::HasOne,
    circuit::operator_traits::Data,
    operator::{DelayedFeedback, FilterMap, Generator},
    trace::{Batch, BatchReader, Batcher, Builder, Cursor},
    OrdIndexedZSet, OrdZSet,
};
use std::{
    cmp::{min, Ordering},
    hash::Hash,
};

/// Specified in the [LDBC Spec][0] ([Pseudo-code][1])
///
/// [0]: https://arxiv.org/pdf/2011.15028v4.pdf#subsection.2.3.2
/// [1]: https://arxiv.org/pdf/2011.15028v4.pdf#section.A.2
pub fn pagerank<P>(
    pagerank_iters: usize,
    damping_factor: f64,
    vertices: Vertices<P>,
    edges: Edges<P>,
) -> RankPairs<P>
where
    P: Clone + 'static,
{
    assert_ne!(pagerank_iters, 0);
    assert!((0.0..=1.0).contains(&damping_factor));

    pagerank_differential(pagerank_iters, damping_factor, vertices, edges).index()
}

fn pagerank_differential<P>(
    pagerank_iters: usize,
    damping_factor: f64,
    vertices: Vertices<P>,
    edges: Edges<P>,
) -> Ranks<P>
where
    P: Clone + 'static,
{
    type Weights = OrdZSet<Node, Rank>;
    type Weighted<S> = Streamed<S, Weights>;

    // Vertices weighted by F64s instead of isizes
    let weighted_vertices = vertices
        .shard()
        .apply(|vertices| {
            let mut builder = <Weights as Batch>::Builder::with_capacity((), vertices.len());

            let mut cursor = vertices.cursor();
            while cursor.key_valid() {
                let node = *cursor.key();
                builder.push((node, Rank::one()));
                cursor.step_key();
            }

            builder.done()
        })
        .shard();

    // Vertices weighted by the damping factor divided by the total number of
    // vertices
    let damped_div_total_vertices = vertices
        .apply(move |vertices| {
            let total_vertices = vertices.len();
            let weight = Rank::new(damping_factor) / total_vertices as f64;
            let mut builder = <OrdIndexedZSet<(), Node, Rank> as Batch>::Builder::with_capacity(
                (),
                total_vertices,
            );

            let mut cursor = vertices.cursor();
            while cursor.key_valid() {
                let node = *cursor.key();
                builder.push((((), node), weight));
                cursor.step_key();
            }

            builder.done()
        })
        .shard();

    // Initially each vertex is assigned a value so that the sum of all vertexes is
    // one, `PR(𝑣)₀ = 1 ÷ |𝑉|`
    let initial_weights = vertices
        .apply(|vertices| {
            let total_vertices = vertices.len();
            let initial_weight = Rank::one() / total_vertices as f64;

            // We can use a builder here since the cursor yields ordered values
            let mut builder = <Weights as Batch>::Builder::with_capacity((), total_vertices);

            let mut cursor = vertices.cursor();
            while cursor.key_valid() {
                let node = *cursor.key();
                builder.push((node, initial_weight));
                cursor.step_key();
            }

            builder.done()
        })
        .shard();

    // Calculate the teleport, `(1 - d) ÷ |𝑉|`
    let teleport = vertices
        .apply(move |vertices| {
            let total_vertices = vertices.len();
            let teleport = (Rank::one() - damping_factor) / total_vertices as f64;

            // We can use a builder here since the cursor yields ordered values
            let mut builder = <Weights as Batch>::Builder::with_capacity((), total_vertices);

            let mut cursor = vertices.cursor();
            while cursor.key_valid() {
                let node = *cursor.key();
                builder.push((node, teleport));
                cursor.step_key();
            }

            builder.done()
        })
        .shard();

    // Count the number of outgoing edges for each node
    let outgoing_edge_counts = edges
        .shard()
        .apply(|weights| {
            // We can use a builder here since the cursor yields ordered values
            let mut builder = <Weights as Batch>::Builder::with_capacity((), weights.len());

            let mut cursor = weights.cursor();
            while cursor.key_valid() {
                let node = *cursor.key();

                let mut total_outputs = 0usize;
                while cursor.val_valid() {
                    total_outputs += 1;
                    cursor.step_val();
                }

                builder.push((node, Rank::new(total_outputs as f64)));
                cursor.step_key();
            }

            builder.done()
        })
        .shard();

    // Find all dangling nodes (nodes without outgoing edges)
    let dangling_nodes = weighted_vertices.minus(
        &outgoing_edge_counts
            .distinct()
            .semijoin_stream::<_, OrdZSet<_, _>>(&weighted_vertices)
            .map(|&(node, _)| node),
    );

    let weights = vertices
        .circuit()
        .iterate_with_condition(|scope| {
            let initial_weights = initial_weights.delta0(scope);

            let edges = edges.delta0(scope).integrate();
            let teleport = teleport.delta0(scope).integrate();
            let dangling_nodes = dangling_nodes.delta0(scope).integrate();
            let outgoing_edge_counts = outgoing_edge_counts.delta0(scope).integrate();
            let damped_div_total_vertices = damped_div_total_vertices.delta0(scope).integrate();

            // Create a feedback for the weights
            let weights_var: DelayedFeedback<_, Weights> = DelayedFeedback::new(scope);
            let weights: Weighted<_> =
                weights_var
                    .stream()
                    .apply2(&initial_weights, |weights, initial_weights| {
                        if initial_weights.len() != 0 {
                            initial_weights.clone()
                        } else {
                            weights.clone()
                        }
                    });

            let importance = scope.region("importance", || {
                // Find the weight pushed out to each edge by taking the weight of the node for
                // the previous iteration and dividing it by the number of
                // outgoing edges it has prev_iter_weight / total_outgoing_edges
                let weight_per_edge = div_join_stream(&weights, &outgoing_edge_counts);

                // Calculate the importance of each node, the sum of all weights from each
                // incoming edge multiplied by the damping factor
                // damping_factor * sum(incoming_edge_weights)
                //
                // This is the big kahuna in regards to performance: %99.9 of our runtime
                // (214 seconds out of 221 seconds, for example) is spent here, most of which
                // is spent consolidating the join's outputs
                weight_per_edge
                    .stream_join::<_, _, Weights>(&edges, |_, _, &dest| dest)
                    .apply_owned(move |mut importance| {
                        // TODO: Try using the `std::simd` api
                        for weight in importance.layer.diffs_mut() {
                            *weight = damping_factor * *weight;
                        }

                        importance
                    })
            });

            let redistributed = scope.region("redistributed", || {
                // Sum up the weights of all dangling nodes, `sum(dangling_nodes)`
                let dangling_sum =
                    dangling_nodes.stream_join::<_, _, OrdZSet<_, _>>(&weights, |_, _, _| ());

                // (damping_factor / total_vertices) * sum(dangling_nodes)
                damped_div_total_vertices
                    .stream_join::<_, _, OrdZSet<_, _>>(&dangling_sum, |_, &node, _| node)
            });

            let page_rank = teleport.sum([&importance, &redistributed]);
            weights_var.connect(&page_rank);

            // Ensure we do only `iters` iterations of pagerank
            let mut current_iter = 0;
            let condition = scope
                .add_source(Generator::new(move || {
                    let iter = current_iter;
                    current_iter += 1;
                    iter
                }))
                .condition(move |&iter| iter == pagerank_iters - 1);

            Ok((condition, page_rank.export()))
        })
        .unwrap();

    // Hoist the weights out of the weight and into the value
    weights.shard().apply(|weights| {
        let mut batch = Vec::with_capacity(weights.len());
        let mut weights = weights.cursor();
        while weights.key_valid() {
            batch.push(((*weights.key(), weights.weight()), 1));
            weights.step_key();
        }

        let mut batcher = <RankSet as Batch>::Batcher::new(());
        batcher.push_batch(&mut batch);
        batcher.seal()
    })
}

// This code implements a join with weight division instead of multiplication
fn div_join_stream<S, K>(
    lhs: &Streamed<S, OrdZSet<K, Rank>>,
    rhs: &Streamed<S, OrdZSet<K, Rank>>,
) -> Streamed<S, OrdZSet<K, Rank>>
where
    S: Clone + 'static,
    K: Hash + Ord + Copy + Data + Send,
{
    lhs.shard().apply2(&rhs.shard(), |lhs, rhs| {
        let capacity = min(lhs.len(), rhs.len());
        let mut builder = <OrdZSet<K, Rank> as Batch>::Builder::with_capacity((), capacity);

        let (mut lhs, mut rhs) = (lhs.cursor(), rhs.cursor());
        while lhs.key_valid() && rhs.key_valid() {
            match lhs.key().cmp(rhs.key()) {
                Ordering::Less => lhs.seek_key(rhs.key()),
                Ordering::Greater => rhs.seek_key(lhs.key()),
                Ordering::Equal => {
                    // Note: We don't have to check for value validity here because `()` always has
                    //       a valid value
                    debug_assert!(lhs.val_valid() && rhs.val_valid());

                    let (lhs_weight, rhs_weight) = (lhs.weight(), rhs.weight());
                    builder.push((*lhs.key(), lhs_weight / rhs_weight));

                    lhs.step_key();
                    rhs.step_key();
                }
            }
        }

        builder.done()
    })
}
