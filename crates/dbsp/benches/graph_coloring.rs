//! Benchmark comparing two graph 2-coloring (bipartiteness) approaches:
//!
//! - **recursive**: `Circuit::recursive`, which splices an implicit `distinct`
//!   onto every recursive output stream.
//!   See [`build_recursive_variant`].
//! - **iterate**: `Circuit::iterate` with a manual feedback loop that carries
//!   only the per-iteration frontier and deduplicates with an explicit
//!   `distinct`. See [`build_iterate_variant`].
//! - **recursion**: `Circuit::recursion` (the `RecursionBuilder` API) over a
//!   *tuple* of the two mutually recursive relations (`red`, `blue`).  Unlike
//!   the transitive-closure benchmarks, the `distinct` is kept (both variants
//!   need it here for termination), so this variant differs from `recursive`
//!   only in the entry API, not the work performed.
//!   See [`build_recursion_variant`].
//!
//! Unlike the transitive-closure benchmarks, both variants perform the same
//! deduplication work (implicit vs. explicit `distinct`).  Hence, we should see
//! comparable run times.  The coloring propagates from a single seed node
//! along directed edges: a red node paints its out-neighbors blue and vice
//! versa.  On a strongly connected graph with odd cycles every reachable node
//! ends up both red and blue, so both variants converge to the same colorings
//! (the benchmark asserts this before measuring).
//!
//! Run with, e.g.:
//! `cargo bench --bench graph_coloring`

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use dbsp::{
    Circuit, CircuitBase, Consensus, DBSPHandle, NestedCircuit, NumEntries, OrdIndexedZSet,
    OrdZSet, OutputHandle, Runtime, Stream, ZSetHandle, ZWeight, mimalloc::MiMalloc, operator::Z1,
    trace::BatchReaderFactories, typed_batch::SpineSnapshot, utils::Tup2,
};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use std::cell::Cell;

#[global_allocator]
static ALLOC: MiMalloc = MiMalloc;

type NodeId = u64;
type Edge = Tup2<NodeId, NodeId>;

struct BipartiteGraphCircuit {
    handle: DBSPHandle,
    init_input: ZSetHandle<NodeId>,
    edges_input: ZSetHandle<Edge>,
    red_output: OutputHandle<SpineSnapshot<OrdZSet<NodeId>>>,
    blue_output: OutputHandle<SpineSnapshot<OrdZSet<NodeId>>>,
}

/// Number of DBSP worker threads used for every circuit in this benchmark.
const WORKERS: usize = 4;

/// Deterministic seed so every run sees the same graphs.
const SEED: u64 = 0x0BADC0DE_DEADBEEF;

/// Out-edges emitted per node by [`generate_graph`].
const OUT_DEGREE: usize = 3;

/// A single-transaction workload: the seed node(s) plus all edges of one graph.
#[derive(Clone)]
struct Workload {
    init: Vec<Tup2<NodeId, ZWeight>>,
    edges: Vec<Tup2<Edge, ZWeight>>,
}

/// Generates a random directed graph plus a single seed node (node `0`, painted
/// red) as one insertion batch.
///
/// Each node emits `out_degree` edges to distinct random targets (excluding
/// itself).  With `out_degree >= 2` and a few hundred nodes the graph is
/// strongly connected with overwhelming probability and contains odd cycles, so
/// the coloring reaches every node in both colors.  That makes the two variants
/// produce identical red/blue sets regardless of the exact structure.
fn generate_graph(nodes: NodeId, out_degree: usize) -> Workload {
    let mut rng = ChaCha8Rng::seed_from_u64(SEED);
    // At most `nodes - 1` distinct targets exist per node (self is excluded).
    let fanout = out_degree.min(nodes.saturating_sub(1) as usize);
    let mut edges = Vec::with_capacity(nodes as usize * fanout);

    for from in 0..nodes {
        let mut targets = Vec::with_capacity(fanout);
        while targets.len() < fanout {
            let to = rng.gen_range(0..nodes);
            if to != from && !targets.contains(&to) {
                targets.push(to);
            }
        }
        for to in targets {
            edges.push(Tup2(Tup2(from, to), 1));
        }
    }

    // Seed the discovery by marking node `0` red.
    let init = vec![Tup2(0, 1)];

    Workload { init, edges }
}

/// Feeds a whole graph into a freshly built circuit in one transaction and
/// returns the number of `(red, blue)` color marks discovered.
///
/// The circuit (and its worker threads) is dropped on return, so the caller
/// must supply a fresh circuit for each measured iteration.
fn drive_once(mut circuit: BipartiteGraphCircuit, mut workload: Workload) -> (usize, usize) {
    circuit.init_input.append(&mut workload.init);
    circuit.edges_input.append(&mut workload.edges);
    circuit
        .handle
        .transaction()
        .expect("transaction should succeed");
    let red = circuit
        .red_output
        .concat()
        .consolidate()
        .num_entries_shallow();
    let blue = circuit
        .blue_output
        .concat()
        .consolidate()
        .num_entries_shallow();
    (red, blue)
}

/// Cross-checks that both variants compute the same coloring on the given graph,
/// returning the number of `(red, blue)` color marks (used both as a sanity check
/// and to label the benchmark with the output cardinality).
fn assert_variants_agree(workload: &Workload) -> (usize, usize) {
    let recursive = build_recursive_variant().expect("build recursive circuit");
    let iterate = build_iterate_variant().expect("build iterate circuit");
    let recursion = build_recursion_variant().expect("build recursion circuit");

    let mut recursive_circuit = recursive.handle;
    let mut iterate_circuit = iterate.handle;
    let mut recursion_circuit = recursion.handle;

    recursive.init_input.append(&mut workload.init.clone());
    recursive.edges_input.append(&mut workload.edges.clone());
    iterate.init_input.append(&mut workload.init.clone());
    iterate.edges_input.append(&mut workload.edges.clone());
    recursion.init_input.append(&mut workload.init.clone());
    recursion.edges_input.append(&mut workload.edges.clone());
    recursive_circuit.transaction().unwrap();
    iterate_circuit.transaction().unwrap();
    recursion_circuit.transaction().unwrap();

    let recursive_red = recursive.red_output.concat().consolidate();
    let recursive_blue = recursive.blue_output.concat().consolidate();
    let iterate_red = iterate.red_output.concat().consolidate();
    let iterate_blue = iterate.blue_output.concat().consolidate();
    let recursion_red = recursion.red_output.concat().consolidate();
    let recursion_blue = recursion.blue_output.concat().consolidate();

    assert_eq!(
        recursive_red, iterate_red,
        "recursive and iterate variants must agree on the red coloring"
    );
    assert_eq!(
        recursive_blue, iterate_blue,
        "recursive and iterate variants must agree on the blue coloring"
    );
    assert_eq!(
        recursive_red, recursion_red,
        "recursive and recursion variants must agree on the red coloring"
    );
    assert_eq!(
        recursive_blue, recursion_blue,
        "recursive and recursion variants must agree on the blue coloring"
    );

    (
        recursive_red.num_entries_shallow(),
        recursive_blue.num_entries_shallow(),
    )
}

/// The graph sizes (node counts) we benchmark.
const GRAPH_SIZES: &[NodeId] = &[500, 1000, 2000];

fn graph_coloring_benches(c: &mut Criterion) {
    let mut group = c.benchmark_group("graph-coloring");
    // Building a circuit spins up worker threads, so keep the sample count
    // modest to bound the wall-clock time of the whole benchmark.
    group.sample_size(30);

    for &nodes in GRAPH_SIZES {
        let workload = generate_graph(nodes, OUT_DEGREE);
        let (red_marks, blue_marks) = assert_variants_agree(&workload);
        let shape = format!("n{nodes}");
        let edges = workload.edges.len();
        println!("graph {shape}: {edges} edges, {red_marks} red / {blue_marks} blue color marks",);

        // Report throughput in edges processed per second, which scales with the
        // graph size and the join work both variants perform.
        group.throughput(Throughput::Elements(edges as u64));

        group.bench_with_input(
            BenchmarkId::new("recursive", &shape),
            &workload,
            |b, workload| {
                b.iter_batched(
                    || {
                        (
                            build_recursive_variant().expect("build recursive circuit"),
                            workload.clone(),
                        )
                    },
                    |(circuit, workload)| drive_once(circuit, workload),
                    BatchSize::PerIteration,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("iterate", &shape),
            &workload,
            |b, workload| {
                b.iter_batched(
                    || {
                        (
                            build_iterate_variant().expect("build iterate circuit"),
                            workload.clone(),
                        )
                    },
                    |(circuit, workload)| drive_once(circuit, workload),
                    BatchSize::PerIteration,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("recursion", &shape),
            &workload,
            |b, workload| {
                b.iter_batched(
                    || {
                        (
                            build_recursion_variant().expect("build recursion circuit"),
                            workload.clone(),
                        )
                    },
                    |(circuit, workload)| drive_once(circuit, workload),
                    BatchSize::PerIteration,
                );
            },
        );
    }

    group.finish();
}

criterion_group!(benches, graph_coloring_benches);
criterion_main!(benches);

/// Helper function to advance the frontier of red and blue nodes by exploring
/// one more hop in the graph along `edges`.
fn hop(
    frontier: &Stream<NestedCircuit, OrdZSet<NodeId>>,
    edges: &Stream<NestedCircuit, OrdIndexedZSet<NodeId, NodeId>>,
) -> Stream<NestedCircuit, OrdZSet<NodeId>> {
    frontier
        .map_index(|node| (*node, *node))
        .join(edges, |_from, _node, to| *to)
}

/// Computes the red/blue graph coloring using DBSP's
/// [`recursive` API](dbsp::circuit::ChildCircuit::recursive),
/// which deduplicates every recursive output stream implicitly.
fn build_recursive_variant() -> anyhow::Result<BipartiteGraphCircuit> {
    let (handle, ((init_input, edges_input), (red_output, blue_output))) =
        Runtime::init_circuit(WORKERS, move |root_circuit| {
            let (edges, edges_input) = root_circuit.add_input_zset::<Edge>();
            let (init, init_input) = root_circuit.add_input_zset::<NodeId>();

            let (red_output, blue_output) = root_circuit.recursive(
                |child_circuit,
                 (red, blue): (
                    Stream<NestedCircuit, OrdZSet<NodeId>>,
                    Stream<NestedCircuit, OrdZSet<NodeId>>,
                )| {
                    let edges = edges.delta0(child_circuit);
                    let init = init.delta0(child_circuit);
                    let edges_indexed = edges.map_index(|Tup2(from, to)| (*from, *to));

                    let new_red = init.plus(&hop(&blue, &edges_indexed));
                    let new_blue = hop(&red, &edges_indexed);

                    Ok((new_red, new_blue))
                },
            )?;

            let red_output = red_output.accumulate_output();
            let blue_output = blue_output.accumulate_output();

            Ok(((init_input, edges_input), (red_output, blue_output)))
        })?;

    Ok(BipartiteGraphCircuit {
        handle,
        init_input,
        edges_input,
        red_output,
        blue_output,
    })
}

/// Computes the red/blue graph coloring using DBSP's
/// [`recursion` builder API](dbsp::circuit::ChildCircuit::recursion) over a
/// tuple of the two mutually recursive relations.
///
/// This performs the same work as [`build_recursive_variant`] — the implicit
/// `distinct` is kept, since it is what caps every node's weight at 1 and thus
/// drives termination.  The only difference is the entry API: the two recursive
/// relations are set up with [`recursive_var`](dbsp::circuit::ChildCircuit::recursive_var)
/// and returned as a tuple, whose arity is inferred.
fn build_recursion_variant() -> anyhow::Result<BipartiteGraphCircuit> {
    let (handle, ((init_input, edges_input), (red_output, blue_output))) =
        Runtime::init_circuit(WORKERS, move |root_circuit| {
            let (edges, edges_input) = root_circuit.add_input_zset::<Edge>();
            let (init, init_input) = root_circuit.add_input_zset::<NodeId>();

            let (red_output, blue_output) = root_circuit
                .recursion(
                    // Two mutually recursive relations; the arity (2) is fixed
                    // by the returned tuple's shape.
                    |child_circuit| {
                        Ok((
                            child_circuit.recursive_var::<OrdZSet<NodeId>>(),
                            child_circuit.recursive_var::<OrdZSet<NodeId>>(),
                        ))
                    },
                    |child_circuit, (red, blue)| {
                        let edges = edges.delta0(child_circuit);
                        let init = init.delta0(child_circuit);
                        let edges_indexed = edges.map_index(|Tup2(from, to)| (*from, *to));

                        let new_red = init.plus(&hop(&blue, &edges_indexed));
                        let new_blue = hop(&red, &edges_indexed);

                        Ok((new_red, new_blue))
                    },
                )
                .finish()?;

            let red_output = red_output.accumulate_output();
            let blue_output = blue_output.accumulate_output();

            Ok(((init_input, edges_input), (red_output, blue_output)))
        })?;

    Ok(BipartiteGraphCircuit {
        handle,
        init_input,
        edges_input,
        red_output,
        blue_output,
    })
}

/// Upper bound on nested iterations before the iterate variant gives up waiting
/// for a fixed point.
const MAX_ITERATIONS: usize = 4096;

/// Computes the red/blue graph coloring using DBSP's lower level
/// [`iterate` API](dbsp::circuit::Circuit::iterate).
///
/// It uses a hybrid termination check: Either stop once the subcircuit
/// reaches a fixed point, or after [`MAX_ITERATIONS`] steps if it never does.
fn build_iterate_variant() -> anyhow::Result<BipartiteGraphCircuit> {
    let (handle, (init_input, edges_input, red_output, blue_output)) =
        Runtime::init_circuit(WORKERS, move |root_circuit| {
            let (init, init_input) = root_circuit.add_input_zset::<NodeId>();
            let (edges, edges_input) = root_circuit.add_input_zset::<Edge>();

            let (red_output, blue_output) = root_circuit.iterate(|child_circuit| {
                let edges = edges.delta0(child_circuit);
                let init = init.delta0(child_circuit);
                let edges_indexed = edges.map_index(|Tup2(from, to)| (*from, *to));

                // Feedback carries only the frontier (the delta from the last step).
                let (red, red_feedback) =
                    child_circuit.add_feedback(Z1::new(OrdZSet::<NodeId>::default()));
                let (blue, blue_feedback) =
                    child_circuit.add_feedback(Z1::new(OrdZSet::<NodeId>::default()));

                // Extend each frontier by one hop and deduplicate with `distinct`,
                // which caps every node's weight at 1 and stops nodes discovered
                // earlier from being re-emitted.
                let new_red = init.plus(&hop(&blue, &edges_indexed)).distinct();
                let new_blue = hop(&red, &edges_indexed).distinct();

                red_feedback.connect(&new_red);
                blue_feedback.connect(&new_blue);

                // Accumulate every frontier across the nested clock cycles into the
                // full colorings and export them to the parent.
                let red = Stream::dyn_integrate_trace(
                    &new_red.inner(),
                    &BatchReaderFactories::new::<NodeId, (), ZWeight>(),
                );
                let blue = Stream::dyn_integrate_trace(
                    &new_blue.inner(),
                    &BatchReaderFactories::new::<NodeId, (), ZWeight>(),
                );

                let child_circuit = child_circuit.clone();
                let consensus = Consensus::new("iterate");
                let step = Cell::new(0usize);

                let termination_check = async move || {
                    let step_count = step.get() + 1;
                    step.set(step_count);

                    // The step bound is data-independent, hence identical on every worker;
                    // the fixed-point check is per-worker.
                    let fixedpoint = child_circuit.check_fixedpoint(0);
                    let iteration_ceiling = step_count >= MAX_ITERATIONS;
                    let done = consensus.check(fixedpoint || iteration_ceiling).await?;
                    if done {
                        // Reset the step counter for the next transaction.
                        step.set(0);
                    }

                    Ok(done)
                };

                Ok((termination_check, (red.export(), blue.export())))
            })?;

            let red_output = Stream::dyn_consolidate(
                &red_output,
                &BatchReaderFactories::new::<NodeId, (), ZWeight>(),
            )
            .typed::<OrdZSet<NodeId>>()
            .accumulate_output();
            let blue_output = Stream::dyn_consolidate(
                &blue_output,
                &BatchReaderFactories::new::<NodeId, (), ZWeight>(),
            )
            .typed::<OrdZSet<NodeId>>()
            .accumulate_output();

            Ok((init_input, edges_input, red_output, blue_output))
        })?;

    Ok(BipartiteGraphCircuit {
        handle,
        init_input,
        edges_input,
        red_output,
        blue_output,
    })
}
