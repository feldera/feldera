use crate::data::{Edges, Node, Rank, RankPairs, RankSet, Ranks, Streamed, Vertices, Weight, F64};
use dbsp::{
    algebra::HasOne,
    circuit::operator_traits::Data,
    operator::{DelayedFeedback, FilterMap, Generator},
    trace::{
        layers::Trie, ord::indexed_zset_batch::OrdIndexedZSetBuilder, Batch, BatchReader, Batcher,
        Builder, Cursor,
    },
    zset_set, OrdIndexedZSet, OrdZSet,
};
use std::{
    cmp::{min, Ordering},
    fmt::{self, Display},
    hash::Hash,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum PageRankKind {
    Scoped,
    Static,
    Diffed,
}

impl Display for PageRankKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self {
            Self::Scoped => "scoped",
            Self::Static => "static",
            Self::Diffed => "diffed",
        };
        f.write_str(kind)
    }
}

/// Specified in the [LDBC Spec][0] ([Pseudo-code][1])
///
/// [0]: https://arxiv.org/pdf/2011.15028v4.pdf#subsection.2.3.2
/// [1]: https://arxiv.org/pdf/2011.15028v4.pdf#section.A.2
// FIXME: Doesn't work with more than one worker
pub fn pagerank<P>(
    pagerank_iters: usize,
    damping_factor: f64,
    directed: bool,
    vertices: Vertices<P>,
    edges: Edges<P>,
    kind: PageRankKind,
) -> RankPairs<P>
where
    P: Clone + 'static,
{
    assert_ne!(pagerank_iters, 0);
    assert!((0.0..=1.0).contains(&damping_factor));

    // Initially each vertex is assigned a value so that the sum of all vertexes is
    // one, `PR(𝑣)₀ = 1 ÷ |𝑉|`
    let initial_weights: Ranks<_> = vertices.apply(|vertices| {
        let total_vertices = vertices.layer.keys();
        let initial_weight = F64::new(1.0 / total_vertices as f64);

        // We can use a builder here since the cursor yields ordered values
        let mut output = Vec::with_capacity(total_vertices);

        let mut cursor = vertices.cursor();
        while cursor.key_valid() {
            let node = *cursor.key();
            output.push((((node, initial_weight), ()), 1));
            cursor.step_key();
        }

        let mut batcher = <OrdZSet<_, _> as Batch>::Batcher::new(());
        batcher.push_batch(&mut output);
        batcher.seal()
    });

    // Find the total number of vertices within the graph
    let total_vertices = vertices.apply(|vertices| {
        let total_vertices = vertices.layer.keys();
        let mut builder = <OrdIndexedZSet<_, _, _> as Batch>::Builder::with_capacity((), 1);
        builder.push(((), total_vertices, 1));
        builder.done()
    });

    // Calculate the teleport, `(1 - 𝑑) ÷ |V|`
    let teleport = total_vertices.map_index(move |(&node, &total_vertices)| {
        let teleport = F64::new((1.0 - damping_factor) / total_vertices as f64);
        (node, teleport)
    });

    // Count the number of outgoing edges for each node
    let outgoing_sans_dangling = edges.apply(|weights| {
        // We can use a builder here since the cursor yields ordered values
        let mut output = OrdIndexedZSetBuilder::<_, _, _, usize>::with_capacity((), weights.len());

        let mut cursor = weights.cursor();
        while cursor.key_valid() {
            let node = *cursor.key();

            let mut total_outputs = 0;
            while cursor.val_valid() {
                total_outputs += 1;
                cursor.step_val();
            }

            output.push((node, total_outputs, 1));
            cursor.step_key();
        }

        output.done()
    });

    // Find all dangling nodes (nodes without outgoing edges)
    let dangling_nodes =
        vertices.minus(&outgoing_sans_dangling.semijoin_stream_core(&vertices, |&node, _| node));

    // Make the edge weights for all dangling nodes, we only need to calculate this
    // once
    let dangling_edge_weights = dangling_nodes.map(|&node| (node, F64::new(0.0)));

    let reversed = if directed {
        edges.apply(|edges| {
            let mut output = Vec::with_capacity(edges.len());

            let mut cursor = edges.cursor();
            while cursor.key_valid() {
                while cursor.val_valid() {
                    output.push(((*cursor.val(), ()), 1));
                    cursor.step_val();
                }
                cursor.step_key();
            }

            let mut batcher = <OrdZSet<_, _> as Batch>::Batcher::new(());
            batcher.push_batch(&mut output);
            batcher.seal()
        })

    // Undirected graphs already have all the edges we need
    } else {
        edges.map(|(&src, _)| src)
    }
    .distinct();

    // Find vertices without any incoming edges
    let zero_incoming = vertices
        .minus(&reversed.semijoin::<(), _, _, _>(&vertices, |&node, _| node))
        .map(|&node| ((), (node, F64::new(0.0))));

    // Create a stream containing one `((), 0)` pair
    let zero = vertices
        .circuit()
        .add_source(Generator::new(|| zset_set! { ((), F64::new(0.0)) }));

    match kind {
        PageRankKind::Scoped => pagerank_scoped_iteration(
            pagerank_iters,
            damping_factor,
            edges,
            vertices,
            zero,
            teleport,
            zero_incoming,
            total_vertices,
            outgoing_sans_dangling,
            dangling_nodes,
            initial_weights,
            dangling_edge_weights,
        ),

        PageRankKind::Static => pagerank_static_iteration(
            pagerank_iters,
            damping_factor,
            edges,
            zero,
            teleport,
            zero_incoming,
            total_vertices,
            outgoing_sans_dangling,
            dangling_nodes,
            initial_weights,
            dangling_edge_weights,
        ),

        PageRankKind::Diffed => {
            pagerank_differential(pagerank_iters, damping_factor, vertices, edges)
        }
    }
    .index()
}

#[allow(clippy::too_many_arguments, clippy::type_complexity)]
fn pagerank_scoped_iteration<P>(
    pagerank_iters: usize,
    damping_factor: f64,
    edges: Edges<P>,
    vertices: Vertices<P>,
    zero: Streamed<P, OrdZSet<((), Rank), Weight>>,
    teleport: Streamed<P, OrdIndexedZSet<(), Rank, Weight>>,
    zero_incoming: Streamed<P, OrdZSet<((), (Node, Rank)), Weight>>,
    total_vertices: Streamed<P, OrdIndexedZSet<(), usize, Weight>>,
    outgoing_sans_dangling: Streamed<P, OrdIndexedZSet<Node, usize, Weight>>,
    dangling_nodes: Vertices<P>,
    initial_weights: Ranks<P>,
    dangling_edge_weights: Ranks<P>,
) -> Ranks<P>
where
    P: Clone + 'static,
{
    vertices
        .circuit()
        .iterate_with_condition(|scope| {
            let zero = zero.delta0(scope).integrate();
            let edges = edges.delta0(scope).integrate();
            let teleport = teleport.delta0(scope).integrate();
            let zero_incoming = zero_incoming.delta0(scope).integrate();
            let total_vertices = total_vertices.delta0(scope).integrate();
            let outgoing_sans_dangling = outgoing_sans_dangling.delta0(scope).integrate();
            let dangling_nodes = dangling_nodes.delta0(scope).integrate();
            let initial_weights = initial_weights.delta0(scope);
            let dangling_edge_weights = dangling_edge_weights.delta0(scope).integrate();

            // Create a feedback for the weights
            let weights_var: DelayedFeedback<_, RankSet> = DelayedFeedback::new(scope);
            let weights: RankPairs<_> = weights_var
                .stream()
                .apply2(&initial_weights, |weights, init_weights| {
                    if init_weights.len() != 0 {
                        init_weights.clone()
                    } else {
                        weights.clone()
                    }
                })
                .index();

            // Find the weight pushed out to each edge by taking the weight of the node for
            // the previous iteration and dividing it by the number of outgoing
            // edges it has
            let weight_per_edge = weights
                .stream_join::<_, _, RankSet>(
                    &outgoing_sans_dangling,
                    |&node, &previous_weight: &F64, &outgoing_edges| {
                        // We shouldn't get any dangling nodes here
                        debug_assert_ne!(outgoing_edges, 0);
                        (node, previous_weight / outgoing_edges as f64)
                    },
                )
                .plus(&dangling_edge_weights)
                .index();

            // Calculate the importance of each node, the sum of all weights from each
            // incoming edge multiplied by the damping factor
            let importance = weight_per_edge
                .stream_join::<_, _, RankSet>(&edges, |_, &weight, &dest| (dest, weight))
                .index()
                .aggregate::<_, OrdZSet<_, _>>(move |&node, incoming: &mut Vec<(&F64, Weight)>| {
                    let summed_weight: F64 = incoming
                        .iter()
                        .map(|&(&weight, diff)| weight * diff as f64)
                        .sum();

                    ((), (node, damping_factor * summed_weight))
                })
                // Add back in nodes without incoming edges
                .plus(&zero_incoming)
                .index();

            // Calculate the redistributed weights, the sum of all dangling vertices'
            // weights divided by the total number of nodes
            // TODO: Multiply the sum by `damping_factor / total_vertices`
            let redistributed = dangling_nodes
                .stream_join::<_, _, OrdZSet<_, _>>(&weights, |_, &(), &weight| ((), weight))
                // Ensure that even if there's no dangling vertices the stream still exists
                .plus(&zero)
                .index::<(), Rank>()
                .aggregate::<_, OrdZSet<_, _>>(|&(), dangling_weights: &mut Vec<(&F64, Weight)>| {
                    let summed_weight = dangling_weights
                        .iter()
                        .map(|&(&weight, diff)| weight * diff as f64)
                        .sum();

                    ((), summed_weight)
                })
                .index::<(), Rank>()
                .stream_join::<_, _, OrdZSet<_, _>>(
                    &total_vertices,
                    move |&(), redistributed, &total_vertices| {
                        let redistributed_weight =
                            (damping_factor / total_vertices as f64) * redistributed;
                        ((), redistributed_weight)
                    },
                )
                .index::<(), Rank>();

            // Sum up the teleport, importance and redistributed weight for each node
            let page_rank = teleport
                .stream_join::<_, _, OrdZSet<_, _>>(
                    &importance,
                    |&(), &teleport, &(node, importance)| ((), (node, teleport + importance)),
                )
                .index::<(), (Node, Rank)>()
                .stream_join::<_, _, RankSet>(
                    &redistributed,
                    |_, &(node, rank), &redistributed| (node, rank + redistributed),
                );
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
        .unwrap()
}

#[allow(clippy::too_many_arguments, clippy::type_complexity)]
fn pagerank_static_iteration<P>(
    pagerank_iters: usize,
    damping_factor: f64,
    edges: Edges<P>,
    zero: Streamed<P, OrdZSet<((), Rank), Weight>>,
    teleport: Streamed<P, OrdIndexedZSet<(), Rank, Weight>>,
    zero_incoming: Streamed<P, OrdZSet<((), (Node, Rank)), Weight>>,
    total_vertices: Streamed<P, OrdIndexedZSet<(), usize, Weight>>,
    outgoing_sans_dangling: Streamed<P, OrdIndexedZSet<Node, usize, Weight>>,
    dangling_nodes: Vertices<P>,
    mut weights: Ranks<P>,
    dangling_edge_weights: Ranks<P>,
) -> Ranks<P>
where
    P: Clone + 'static,
{
    for _ in 0..pagerank_iters {
        let weights_index = weights.index();

        // Find the weight pushed out to each edge by taking the weight of the node for
        // the previous iteration and dividing it by the number of outgoing
        // edges it has
        let weight_per_edge = weights_index
            .stream_join::<_, _, RankSet>(
                &outgoing_sans_dangling,
                |&node, &previous_weight: &F64, &outgoing_edges| {
                    // We shouldn't get any dangling nodes here
                    debug_assert_ne!(outgoing_edges, 0);
                    (node, previous_weight / outgoing_edges as f64)
                },
            )
            .plus(&dangling_edge_weights)
            .index();

        // Calculate the importance of each node, the sum of all weights from each
        // incoming edge multiplied by the damping factor
        let importance = weight_per_edge
            .stream_join::<_, _, RankSet>(&edges, |_, &weight, &dest| (dest, weight))
            .index()
            .aggregate::<_, OrdZSet<_, _>>(move |&node, incoming: &mut Vec<(&F64, Weight)>| {
                let summed_weight: F64 = incoming
                    .iter()
                    .map(|&(&weight, diff)| weight * diff as f64)
                    .sum();

                ((), (node, damping_factor * summed_weight))
            })
            // Add back in nodes without incoming edges
            .plus(&zero_incoming)
            .index();

        // Calculate the redistributed weights, the sum of all dangling vertices'
        // weights divided by the total number of nodes
        // TODO: Multiply the sum by `damping_factor / total_vertices`
        let redistributed = dangling_nodes
            .stream_join::<_, _, OrdZSet<_, _>>(&weights_index, |_, &(), &weight| ((), weight))
            // Ensure that even if there's no dangling vertices the stream still exists
            .plus(&zero)
            .index::<(), Rank>()
            .aggregate::<_, OrdZSet<_, _>>(|&(), dangling_weights: &mut Vec<(&F64, Weight)>| {
                let summed_weight = dangling_weights
                    .iter()
                    .map(|&(&weight, diff)| weight * diff as f64)
                    .sum();

                ((), summed_weight)
            })
            .index::<(), Rank>()
            .stream_join::<_, _, OrdZSet<_, _>>(
                &total_vertices,
                move |&(), redistributed, &total_vertices| {
                    let redistributed_weight =
                        (damping_factor / total_vertices as f64) * redistributed;

                    ((), redistributed_weight)
                },
            )
            .index::<(), Rank>();

        // Sum up the teleport, importance and redistributed weight for each node
        let page_rank = teleport
            .stream_join::<_, _, OrdZSet<_, _>>(
                &importance,
                |&(), &teleport, &(node, importance)| ((), (node, teleport + importance)),
            )
            .index::<(), (Node, Rank)>()
            .stream_join::<_, _, RankSet>(&redistributed, |_, &(node, rank), &redistributed| {
                (node, rank + redistributed)
            });

        weights = page_rank;
    }

    weights
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
    let weighted_vertices = vertices.shard().apply(|vertices| {
        let mut builder = <Weights as Batch>::Builder::with_capacity((), vertices.len());

        let mut cursor = vertices.cursor();
        while cursor.key_valid() {
            let node = *cursor.key();
            builder.push((node, (), Rank::one()));
            cursor.step_key();
        }

        builder.done()
    });

    // Vertices weighted by the damping factor
    let damped_vertices = vertices.shard().apply(move |vertices| {
        let mut builder = <Weights as Batch>::Builder::with_capacity((), vertices.len());

        let mut cursor = vertices.cursor();
        while cursor.key_valid() {
            let node = *cursor.key();
            builder.push((node, (), Rank::new(damping_factor)));
            cursor.step_key();
        }

        builder.done()
    });

    // Vertices weighted by the damping factor divided by the total number of
    // vertices
    let damped_div_total_vertices = vertices.apply(move |vertices| {
        let total_vertices = vertices.len();
        let weight = Rank::new(damping_factor) / total_vertices as f64;
        let mut builder =
            <OrdIndexedZSet<(), Node, Rank> as Batch>::Builder::with_capacity((), total_vertices);

        let mut cursor = vertices.cursor();
        while cursor.key_valid() {
            let node = *cursor.key();
            builder.push(((), node, weight));
            cursor.step_key();
        }

        builder.done()
    });

    // Initially each vertex is assigned a value so that the sum of all vertexes is
    // one, `PR(𝑣)₀ = 1 ÷ |𝑉|`
    let initial_weights = vertices.apply(|vertices| {
        let total_vertices = vertices.len();
        let initial_weight = Rank::one() / total_vertices as f64;

        // We can use a builder here since the cursor yields ordered values
        let mut builder = <Weights as Batch>::Builder::with_capacity((), total_vertices);

        let mut cursor = vertices.cursor();
        while cursor.key_valid() {
            let node = *cursor.key();
            builder.push((node, (), initial_weight));
            cursor.step_key();
        }

        builder.done()
    });

    // Calculate the teleport, `(1 - d) ÷ |𝑉|`
    let teleport = vertices.apply(move |vertices| {
        let total_vertices = vertices.len();
        let teleport = (Rank::one() - damping_factor) / total_vertices as f64;

        // We can use a builder here since the cursor yields ordered values
        let mut builder = <Weights as Batch>::Builder::with_capacity((), total_vertices);

        let mut cursor = vertices.cursor();
        while cursor.key_valid() {
            let node = *cursor.key();
            builder.push((node, (), teleport));
            cursor.step_key();
        }

        builder.done()
    });

    // Count the number of outgoing edges for each node
    let outgoing_edge_counts = edges.shard().apply(|weights| {
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

            builder.push((node, (), Rank::new(total_outputs as f64)));
            cursor.step_key();
        }

        builder.done()
    });

    // Find all dangling nodes (nodes without outgoing edges)
    let dangling_nodes = weighted_vertices.minus(
        &outgoing_edge_counts
            .distinct()
            .semijoin_stream_core(&weighted_vertices, |&node, _| node),
    );

    let edge_weights = edges.shard().apply(|edges| {
        let mut builder =
            <OrdIndexedZSet<Node, Node, Rank> as Batch>::Builder::with_capacity((), edges.len());

        let mut cursor = edges.cursor();
        while cursor.key_valid() {
            let src = *cursor.key();
            while cursor.val_valid() {
                let dest = *cursor.val();
                let weight = Rank::new(cursor.weight() as f64);
                builder.push((src, dest, weight));
                cursor.step_val();
            }
            cursor.step_key();
        }

        builder.done()
    });

    let weights = vertices
        .circuit()
        .iterate_with_condition(|scope| {
            let initial_weights = initial_weights.delta0(scope);
            let teleport = teleport.delta0(scope).integrate();
            let edge_weights = edge_weights.delta0(scope).integrate();
            let dangling_nodes = dangling_nodes.delta0(scope).integrate();
            let damped_vertices = damped_vertices.delta0(scope).integrate();
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

                // Aggregate the weights of all incoming edges for any given vertex
                // sum(incoming_edge_weights)
                let incoming_vertex_weights =
                    weight_per_edge.stream_join::<_, _, Weights>(&edge_weights, |_, _, &dest| dest);

                // Calculate the importance of each node, the sum of all weights from each
                // incoming edge multiplied by the damping factor
                // damping_factor * sum(incoming_edge_weights)
                damped_vertices
                    .stream_join::<_, _, Weights>(&incoming_vertex_weights, |&node, _, _| node)
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
            batch.push((((*weights.key(), weights.weight()), ()), 1));
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
                    if lhs.val_valid() {
                        let lhs_weight = lhs.weight();
                        if rhs.val_valid() {
                            let rhs_weight = rhs.weight();
                            builder.push((*lhs.key(), (), lhs_weight / rhs_weight));
                        }
                    }

                    lhs.step_key();
                    rhs.step_key();
                }
            }
        }

        builder.done()
    })
}
