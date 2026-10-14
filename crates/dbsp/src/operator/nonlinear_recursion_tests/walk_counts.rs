//! Counting walks of each length by repeated squaring, with bag semantics.
//!
//! The number of walks of length `a + b` from `x` to `z` is the sum, over the
//! nodes `y`, of the number of walks of length `a` from `x` to `y` times the
//! number of walks of length `b` from `y` to `z`: the adjacency matrix raised
//! to the power `a + b` is the product of its powers `a` and `b`.  Splitting
//! each walk after its first `a` legs, where `a` is `b` or `b + 1`, counts each
//! walk once.  In the rules below, `walks(x, y, n)` stands for the walks of
//! length `n` from `x` to `y`:
//!
//! ```text
//! walks(x, y, 1)     :- edges(x, y).
//! walks(x, z, a + b) :- walks(x, y, a), walks(y, z, b), a = b.
//! walks(x, z, a + b) :- walks(x, y, a), walks(y, z, b), a = b + 1.
//! ```
//!
//! Relations are bags, not sets: the weight of a fact is the sum, over its
//! derivations, of the product of the weights of the facts that each
//! derivation matches, so the weight of `walks(x, z, n)` is the number of
//! walks of length `n` from `x` to `z`.  The recursion joins the walks with
//! themselves, and every walk of length `L` is known by iteration
//! `ceil(log2(L))`.
//!
//! [`recursive`](crate::ChildCircuit::recursive) applies `distinct`, which
//! would turn every count into 1, so in its place the recursion is built from
//! [`fixedpoint`](crate::Circuit::fixedpoint).  The
//! [recursion builder](crate::ChildCircuit::recursion_builder) leaves the
//! `distinct` out when asked to with
//! [`without_distinct`](crate::operator::RecursionBuilder::without_distinct).
//! The recursion reaches a fixed point only on acyclic graphs.

use std::collections::{BTreeMap, BTreeSet};

use proptest::prelude::*;

use super::harness::{
    Program, RecursionApi, Transaction, ZSet, add_changes, any_config, apply_proposals, check,
    configs, fixpoint, map_steps, proposals, read_zset, set_after, workloads,
};
use crate::{
    Circuit, FallbackZSet, NestedCircuit, OutputHandle, RootCircuit, Stream, ZSetHandle, ZWeight,
    operator::DelayedFeedback,
    typed_batch::{OrdZSet, Spine, SpineSnapshot},
    utils::{Tup2, Tup3, test::CIRCUIT_CASES},
};

/// An edge: `(from, to)`.
type Edge = Tup2<u64, u64>;

/// Walks of one length between two nodes: `(from, to, length)`, weighted by
/// their number.
type Walks = Tup3<u64, u64, u64>;

/// Changes to edges in one step.
type EdgeChanges = Vec<(Edge, ZWeight)>;

/// Computes the recursion's next walks: the edges, and every walk that joins
/// two current walks whose lengths differ by at most one.
///
/// # Arguments
///
/// * `edges` - the graph's edges.
/// * `walks` - the current walks.
///
/// # Returns
///
/// The next walks, weighted by their number.
fn next_walks(
    edges: &Stream<NestedCircuit, OrdZSet<Edge>>,
    walks: &Stream<NestedCircuit, OrdZSet<Walks>>,
) -> Stream<NestedCircuit, OrdZSet<Walks>> {
    // Both inputs depend on the recursion.
    let squared = walks
        .map_index(|Tup3(from, via, a)| (*via, Tup2(*from, *a)))
        .join_flatmap(
            &walks.map_index(|Tup3(via, to, b)| (*via, Tup2(*to, *b))),
            |_via, Tup2(from, a), Tup2(to, b)| {
                (*a == *b || *a == b + 1).then(|| Tup3(*from, *to, a + b))
            },
        );
    edges
        .map(|Tup2(from, to)| Tup3(*from, *to, 1))
        .plus(&squared)
}

/// Walk counts by repeated squaring.
#[derive(Clone)]
struct WalkCounts;

impl Program for WalkCounts {
    type Input = EdgeChanges;
    type Handles = (
        ZSetHandle<Edge>,
        OutputHandle<SpineSnapshot<OrdZSet<Walks>>>,
    );
    type Output = ZSet<Walks>;

    fn build(&self, circuit: &mut RootCircuit, api: RecursionApi) -> Self::Handles {
        let (edges, edges_handle) = circuit.add_input_zset::<Edge>();
        let walks = match api {
            RecursionApi::Recursive => circuit
                .fixedpoint(|child| {
                    let walks = <DelayedFeedback<_, OrdZSet<Walks>>>::new(child);
                    let next = next_walks(&edges.delta0(child), walks.stream());
                    walks.connect(&next);
                    Ok(next
                        .integrate_trace()
                        .inner()
                        .export()
                        .typed::<Spine<FallbackZSet<Walks>>>())
                })
                .unwrap()
                .consolidate()
                .map(|walks| *walks),
            RecursionApi::Builder => circuit
                .recursion_builder(
                    |child| Ok(child.recursive_var::<OrdZSet<Walks>>()),
                    |child, walks| Ok(next_walks(&edges.delta0(child), &walks)),
                )
                .without_distinct()
                .finish()
                .unwrap(),
        };
        (
            edges_handle,
            walks.accumulate_integrate().accumulate_output(),
        )
    }

    fn push(&self, (edges, _): &Self::Handles, input: &EdgeChanges) {
        for (row, weight) in input {
            edges.push(*row, *weight);
        }
    }

    fn read(&self, (_, walks): &Self::Handles) -> ZSet<Walks> {
        read_zset(walks)
    }

    fn model(&self, inputs: &[EdgeChanges]) -> ZSet<Walks> {
        let edges = set_after(inputs.iter().map(Vec::as_slice));
        fixpoint(ZSet::new(), |walks: &ZSet<Walks>| {
            // Walks, indexed by where they start.
            let mut starting = BTreeMap::<u64, Vec<(u64, u64, ZWeight)>>::new();
            for (Tup3(via, to, b), weight) in walks {
                starting.entry(*via).or_default().push((*to, *b, *weight));
            }
            let mut next: Vec<(Walks, ZWeight)> = edges
                .iter()
                .map(|Tup2(from, to)| (Tup3(*from, *to, 1), 1))
                .collect();
            for (Tup3(from, via, a), left_weight) in walks {
                for (to, b, right_weight) in starting.get(via).into_iter().flatten() {
                    if *a == *b || *a == b + 1 {
                        next.push((Tup3(*from, *to, a + b), left_weight * right_weight));
                    }
                }
            }
            let mut next_walks = ZSet::new();
            add_changes(&mut next_walks, &next);
            next_walks
        })
    }
}

/// Changes that insert or delete edges.
///
/// # Arguments
///
/// * `edges` - the edges, as `(from, to)`.
/// * `weight` - 1 to insert, -1 to delete.
///
/// # Returns
///
/// The changes.
fn edge_changes(edges: &[(u64, u64)], weight: ZWeight) -> EdgeChanges {
    edges
        .iter()
        .map(|&(from, to)| (Tup2(from, to), weight))
        .collect()
}

/// Workloads whose later transactions make the join compute output for the
/// same later iteration in two iterations of one run.
///
/// The first transaction adds the chain `0 -> 1 -> ... -> 8`, along with a
/// second path from 0 to 4, through 10, so that some counts exceed 1.  A walk
/// of length `a + b` joins a walk of length `a` with one of length `b`, and a
/// walk of length `b + 1` may be known an iteration later than one of length
/// `b`.  So a later edge from 8, which the join meets as the last leg of new
/// walks, meets walks of length `b + 1` into 8 that the first transaction
/// found in the next iteration, and so computes output for that iteration
/// ahead of time.  In that iteration, the join computes output for it again,
/// as the walks just found meet the chain the same way.  Counts are weights,
/// so any lost update changes one.
///
/// # Returns
///
/// The workloads, each a list of transactions.
fn walk_triggers() -> Vec<Vec<Transaction<EdgeChanges>>> {
    let chain: Vec<(u64, u64)> = (0..8).map(|node| (node, node + 1)).collect();
    let setup = [chain, vec![(0, 10), (10, 11), (11, 12), (12, 4)]].concat();
    let append = |weight| edge_changes(&[(8, 9)], weight);
    vec![
        vec![vec![edge_changes(&setup, 1)], vec![append(1)]],
        // Two new edges at the end, each in its own step of one transaction.
        vec![
            vec![edge_changes(&setup, 1)],
            vec![append(1), edge_changes(&[(9, 13)], 1)],
        ],
        // A new edge at the start, whose walks only meet walks known no later,
        // so that no output is computed ahead of time.
        vec![
            vec![edge_changes(&setup, 1)],
            vec![edge_changes(&[(14, 0)], 1)],
        ],
        // Append, cut the chain, and restore it.
        vec![
            vec![edge_changes(&setup, 1)],
            vec![append(1)],
            vec![edge_changes(&[(5, 6)], -1)],
            vec![edge_changes(&[(5, 6)], 1)],
            vec![append(-1)],
        ],
    ]
}

/// Generates workloads over 8 nodes, in which an edge always leads to a node
/// with a greater number, so that the graph has no cycles.
///
/// # Returns
///
/// A strategy for workloads: up to five transactions of up to three steps.
fn walk_workloads() -> impl Strategy<Value = Vec<Transaction<EdgeChanges>>> {
    let edge = prop::sample::select(
        (0..8u64)
            .flat_map(|to| (0..to).map(move |from| Tup2(from, to)))
            .collect::<Vec<_>>(),
    );
    workloads(proposals(edge, 4), 5, 3).prop_map(|raw| {
        let mut edges = BTreeSet::new();
        map_steps(raw, |proposals| apply_proposals(&mut edges, &proposals))
    })
}

/// Walk counts survive their scripted triggers under every configuration.
#[test]
fn walk_triggers_hold() {
    for workload in walk_triggers() {
        for config in configs() {
            check(&WalkCounts, &workload, config);
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(CIRCUIT_CASES))]

    /// Walk counts of random acyclic graphs.
    #[test]
    fn walk_counts_random(workload in walk_workloads(), config in any_config()) {
        check(&WalkCounts, &workload, config);
    }
}
