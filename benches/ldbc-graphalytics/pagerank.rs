use crate::data::{Edges, Node, Streamed, Vertices, Weight};
use dbsp::{
    operator::{DelayedFeedback, Generator},
    trace::{
        layers::Trie, ord::indexed_zset_batch::OrdIndexedZSetBuilder, Batch, BatchReader, Batcher,
        Builder, Cursor,
    },
    zset_set, OrdIndexedZSet, OrdZSet,
};
use deepsize::DeepSizeOf;
use ordered_float::OrderedFloat;
use std::{
    fmt::{self, Debug, Display},
    io::Write,
    ops::Add,
};

/// Pagerank must use 64bit float values
type Rank = F64;

type RankSet = OrdZSet<(Node, Rank), Weight>;
type RankMap = OrdIndexedZSet<Node, Rank, Weight>;

type Ranks<P> = Streamed<P, RankSet>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageRankKind {
    Scoped,
    Static,
}

/// Specified in https://arxiv.org/pdf/2011.15028v4.pdf#subsection.2.3.2
pub fn pagerank<P>(
    pagerank_iters: usize,
    damping_factor: f64,
    directed: bool,
    vertices: Vertices<P>,
    edges: Edges<P>,
    kind: PageRankKind,
) -> Streamed<P, RankMap>
where
    P: Clone + 'static,
{
    assert!((0.0..=1.0).contains(&damping_factor));

    // Initially each vertex is assigned a value so that the sum of all vertexes is one,
    // `PR(𝑣)₀ = 1 ÷ |𝑉|`
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

    // initial_weights.inspect(|weights| {
    //     let mut stdout = std::io::stdout().lock();

    //     let mut cursor = weights.cursor();
    //     while cursor.key_valid() {
    //         let (key, value) = *cursor.key();
    //         let weight = cursor.weight();
    //         writeln!(stdout, "initial_weights: {key}, {value} ({weight:+})").unwrap();
    //         cursor.step_key();
    //     }

    //     stdout.flush().unwrap();
    // });

    // Find the total number of vertices within the graph
    let total_vertices = vertices.apply(|vertices| {
        let total_vertices = vertices.layer.keys();
        let mut builder = <OrdIndexedZSet<_, _, _> as Batch>::Builder::with_capacity((), 1);
        builder.push(((), total_vertices, 1));
        builder.done()
    });

    // Calculate the teleport, `(1 - 𝑑) ÷ |V|`
    let teleport =
        total_vertices.map_values::<OrdIndexedZSet<_, _, _>, _>(move |&(), &total_vertices| {
            F64::new((1.0 - damping_factor) / total_vertices as f64)
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

    // Make the edge weights for all dangling nodes, we only need to calculate this once
    let dangling_edge_weights = dangling_nodes.map_keys(|&node| (node, F64::new(0.0)));

    // dangling_nodes.inspect(|weights| {
    //     let mut stdout = std::io::stdout().lock();
    //
    //     let mut cursor = weights.cursor();
    //     while cursor.key_valid() {
    //         let node = *cursor.key();
    //         let weight = cursor.weight();
    //         writeln!(stdout, "dangling_nodes: {node} ({weight:+})").unwrap();
    //         cursor.step_key();
    //     }
    //
    //     stdout.flush().unwrap();
    // });

    let reversed = if directed {
        edges
            .apply(|edges| {
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
            .distinct()

    // Undirected graphs already have all the edges we need
    } else {
        edges.map_values(|_, _| ())
    };

    // Find vertices without any incoming edges
    let zero_incoming = vertices
        .minus(&reversed.semijoin::<(), _, _, _>(&vertices, |&node, _| node))
        .map_keys::<OrdZSet<_, _>, _>(|&node| ((), (node, F64::new(0.0))));

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
        )
        .index(),
    }
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
) -> Streamed<P, RankMap>
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
            let weights_var = DelayedFeedback::new(scope);
            let weights: &Streamed<_, RankMap> = weights_var.stream();

            // Find the weight pushed out to each edge by taking the weight of the node for the previous
            // iteration and dividing it by the number of outgoing edges it has
            let weight_per_edge = weights
                .stream_join::<_, _, RankSet>(
                    &outgoing_sans_dangling,
                    |&node, &previous_weight: &F64, &outgoing_edges| {
                        // We shouldn't get any dangling nodes here
                        debug_assert_ne!(outgoing_edges, 0);

                        let weight = F64::new(previous_weight.inner() / outgoing_edges as f64);
                        (node, weight)
                    },
                )
                .plus(&dangling_edge_weights)
                .index();

            weight_per_edge.inspect(|weights| {
                let mut stdout = std::io::stdout().lock();

                let mut cursor = weights.cursor();
                while cursor.key_valid() {
                    let key = *cursor.key();
                    while cursor.val_valid() {
                        let value = *cursor.val();
                        let weight = cursor.weight();
                        writeln!(stdout, "weight_per_edge: {key}, {value} ({weight:+})").unwrap();
                        cursor.step_val();
                    }
                    cursor.step_key();
                }

                stdout.flush().unwrap();
            });

            // Calculate the importance of each node, the sum of all weights from each incoming edge
            // multiplied by the damping factor
            let importance = weight_per_edge
                .stream_join::<_, _, RankSet>(&edges, |_, &weight, &dest| {
                    // println!("{dest} -> {weight}");
                    (dest, weight)
                })
                .index()
                .aggregate::<_, OrdZSet<_, _>>(move |&node, incoming: &mut Vec<(&F64, Weight)>| {
                    let summed_weight: f64 = incoming
                        .iter()
                        .map(|&(&weight, diff)| weight.inner() * diff as f64)
                        .sum();

                    // println!("importance: {node} -> {}", damping_factor * summed_weight);
                    ((), (node, F64::new(damping_factor * summed_weight)))
                })
                // Add back in nodes without incoming edges
                .plus(&zero_incoming)
                .index();

            importance.inspect(|weights| {
                let mut stdout = std::io::stdout().lock();

                let mut cursor = weights.cursor();
                while cursor.key_valid() {
                    while cursor.val_valid() {
                        let (key, value) = *cursor.val();
                        let weight = cursor.weight();
                        writeln!(stdout, "importance: {key}, {value} ({weight:+})").unwrap();
                        cursor.step_val();
                    }
                    cursor.step_key();
                }

                stdout.flush().unwrap();
            });

            // Calculate the redistributed weights, the sum of all dangling vertices' weights
            // divided by the total number of nodes
            // TODO: Multiply the sum by `damping_factor / total_vertices`
            let redistributed = dangling_nodes
                .stream_join::<_, _, OrdZSet<_, _>>(weights, |_, &(), &weight| ((), weight))
                // Ensure that even if there's no dangling vertices the stream still exists
                .plus(&zero)
                .index::<(), Rank>()
                .aggregate::<_, OrdZSet<_, _>>(|&(), dangling_weights: &mut Vec<(&F64, Weight)>| {
                    let summed_weight: f64 = dangling_weights
                        .iter()
                        .map(|&(&weight, diff)| weight.inner() * diff as f64)
                        .sum();

                    // println!("redistributed: {summed_weight}");
                    ((), F64::new(summed_weight))
                })
                .index::<(), Rank>()
                .stream_join::<_, _, OrdZSet<_, _>>(
                    &total_vertices,
                    move |&(), redistributed, &total_vertices| {
                        let redistributed_weight =
                            (damping_factor / total_vertices as f64) * redistributed.inner();
                        ((), F64::new(redistributed_weight))
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
                })
                .plus(&initial_weights)
                .index();
            weights_var.connect(&page_rank);

            // Ensure we do only `iters` iterations of pagerank
            let mut current_iter = 0;
            let condition = scope
                .add_source(Generator::new(move || {
                    let iter = current_iter;
                    current_iter += 1;
                    iter
                }))
                .condition(move |&iter| iter > pagerank_iters);

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

        // Find the weight pushed out to each edge by taking the weight of the node for the previous
        // iteration and dividing it by the number of outgoing edges it has
        let weight_per_edge = weights_index
            .stream_join::<_, _, RankSet>(
                &outgoing_sans_dangling,
                |&node, &previous_weight: &F64, &outgoing_edges| {
                    // We shouldn't get any dangling nodes here
                    debug_assert_ne!(outgoing_edges, 0);

                    let weight = F64::new(previous_weight.inner() / outgoing_edges as f64);
                    (node, weight)
                },
            )
            .plus(&dangling_edge_weights)
            .index();

        // Calculate the importance of each node, the sum of all weights from each incoming edge
        // multiplied by the damping factor
        let importance = weight_per_edge
            .stream_join::<_, _, RankSet>(&edges, |_, &weight, &dest| {
                // println!("{dest} -> {weight}");
                (dest, weight)
            })
            .index()
            .aggregate::<_, OrdZSet<_, _>>(move |&node, incoming: &mut Vec<(&F64, Weight)>| {
                let summed_weight: f64 = incoming
                    .iter()
                    .map(|&(&weight, diff)| {
                        if diff.is_negative() {
                            0.0
                        } else {
                            weight.inner() * diff as f64
                        }
                    })
                    .sum();

                // println!("importance: {node} -> {}", damping_factor * summed_weight);
                ((), (node, F64::new(damping_factor * summed_weight)))
            })
            // Add back in nodes without incoming edges
            .plus(&zero_incoming)
            .index();

        // Calculate the redistributed weights, the sum of all dangling vertices' weights
        // divided by the total number of nodes
        // TODO: Multiply the sum by `damping_factor / total_vertices`
        let redistributed = dangling_nodes
            .stream_join::<_, _, OrdZSet<_, _>>(&weights_index, |_, &(), &weight| ((), weight))
            // Ensure that even if there's no dangling vertices the stream still exists
            .plus(&zero)
            .index::<(), Rank>()
            .aggregate::<_, OrdZSet<_, _>>(|&(), dangling_weights: &mut Vec<(&F64, Weight)>| {
                let summed_weight: f64 = dangling_weights
                    .iter()
                    .map(|&(&weight, diff)| {
                        if diff.is_negative() {
                            0.0
                        } else {
                            weight.inner() * diff as f64
                        }
                    })
                    .sum();

                ((), F64::new(summed_weight))
            })
            .index::<(), Rank>()
            .stream_join::<_, _, OrdZSet<_, _>>(
                &total_vertices,
                move |&(), redistributed, &total_vertices| {
                    let redistributed_weight =
                        (damping_factor / total_vertices as f64) * redistributed.inner();

                    ((), F64::new(redistributed_weight))
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

#[derive(Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct F64(OrderedFloat<f64>);

impl F64 {
    pub const fn new(float: f64) -> Self {
        Self(OrderedFloat(float))
    }

    #[rustfmt::skip]
    pub const fn inner(self) -> f64 {
        self.0.0
    }
}

impl Add for F64 {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Self(self.0 + rhs.0)
    }
}

impl DeepSizeOf for F64 {
    fn deep_size_of_children(&self, _context: &mut deepsize::Context) -> usize {
        0
    }
}

impl Debug for F64 {
    #[rustfmt::skip]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Debug::fmt(&self.0.0, f)
    }
}

impl Display for F64 {
    #[rustfmt::skip]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.0.0, f)
    }
}
