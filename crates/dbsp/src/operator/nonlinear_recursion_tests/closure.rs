//! Transitive closures whose joins pair two recursive inputs.
//!
//! * [`PathDoubling`] joins the paths found so far with themselves.

use std::collections::BTreeSet;

use proptest::prelude::*;

use super::harness::{
    Program, Proposals, Transaction, ZSet, add_changes, any_config, apply_proposals, check,
    configs, fixpoint, read_zset, set_zset, workloads,
};
use crate::{
    OutputHandle, RootCircuit, Stream, ZSetHandle, ZWeight,
    typed_batch::{OrdZSet, SpineSnapshot},
    utils::{Tup2, test::CIRCUIT_CASES},
};

/// An edge or a path: `(from, to)`.
type Edge = Tup2<u64, u64>;

/// Changes to the edges in one step.
type EdgeChanges = Vec<(Edge, ZWeight)>;

/// Transitive closure by path doubling:
///
/// ```text
/// paths(x, y) :- edges(x, y).
/// paths(x, z) :- paths(x, y), paths(y, z).
/// ```
///
/// Both inputs of the join depend on the recursion, and every path of up to
/// `2^n` edges is known by iteration `n`.
#[derive(Clone)]
struct PathDoubling;

impl Program for PathDoubling {
    type Input = EdgeChanges;
    type Handles = (ZSetHandle<Edge>, OutputHandle<SpineSnapshot<OrdZSet<Edge>>>);
    type Output = ZSet<Edge>;

    fn build(&self, circuit: &mut RootCircuit) -> Self::Handles {
        let (edges, edges_handle) = circuit.add_input_zset::<Edge>();
        let paths = circuit
            .recursive(|child, paths: Stream<_, OrdZSet<Edge>>| {
                let edges = edges.delta0(child);
                let by_end = paths.map_index(|Tup2(from, via)| (*via, *from));
                let by_start = paths.map_index(|Tup2(via, to)| (*via, *to));
                Ok(edges.plus(&by_end.join(&by_start, |_via, from, to| Tup2(*from, *to))))
            })
            .unwrap();
        (
            edges_handle,
            paths.accumulate_integrate().accumulate_output(),
        )
    }

    fn push(&self, (edges, _): &Self::Handles, input: &EdgeChanges) {
        for (edge, weight) in input {
            edges.push(*edge, *weight);
        }
    }

    fn read(&self, (_, paths): &Self::Handles) -> ZSet<Edge> {
        read_zset(paths)
    }

    fn model(&self, inputs: &[EdgeChanges]) -> ZSet<Edge> {
        let mut edges = ZSet::new();
        for input in inputs {
            add_changes(&mut edges, input);
        }
        let edges: BTreeSet<Edge> = edges.into_keys().collect();
        let paths = fixpoint(BTreeSet::new(), |paths: &BTreeSet<Edge>| {
            let mut next = edges.clone();
            for Tup2(from, via) in paths {
                for Tup2(_, to) in paths.range(Tup2(*via, 0)..=Tup2(*via, u64::MAX)) {
                    next.insert(Tup2(*from, *to));
                }
            }
            next
        });
        set_zset(&paths)
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

/// Workloads whose later transactions make path doubling compute output for
/// the same later iteration in two iterations of one run.
///
/// The first transaction adds the chain `0 -> 1 -> ... -> 8`, whose paths of
/// 2, 3-4, and 5-8 edges appear in iterations 1, 2, and 3.  A later edge
/// `9 -> 0` reaches the join in one iteration, and the path `9 -> 1` it
/// yields reaches the join in the next.  Both meet paths of the chain found
/// in later iterations, so the join computes output for the same later
/// iterations in two consecutive iterations.
///
/// # Returns
///
/// The workloads, each a list of transactions.
fn doubling_triggers() -> Vec<Vec<Transaction<EdgeChanges>>> {
    let chain: Vec<(u64, u64)> = (0..8).map(|node| (node, node + 1)).collect();
    let add_chain = vec![edge_changes(&chain, 1)];
    let prepend = vec![edge_changes(&[(9, 0)], 1)];
    vec![
        // The trigger.
        vec![add_chain.clone(), prepend.clone()],
        // Two new edges, each in its own step of one transaction.
        vec![
            add_chain.clone(),
            vec![edge_changes(&[(9, 0)], 1), edge_changes(&[(10, 9)], 1)],
        ],
        // The trigger, then deletions and reinsertions that change paths
        // found in late iterations.
        vec![
            add_chain.clone(),
            prepend.clone(),
            vec![edge_changes(&[(9, 0)], -1)],
            vec![edge_changes(&[(4, 5)], -1)],
            prepend,
            vec![edge_changes(&[(4, 5)], 1)],
        ],
        // A cycle through the whole chain.
        vec![add_chain, vec![edge_changes(&[(8, 0)], 1)]],
    ]
}

/// Generates proposed changes to edges between `nodes` nodes.
///
/// # Arguments
///
/// * `nodes` - the number of nodes.
///
/// # Returns
///
/// A strategy for up to four proposals.
fn edge_proposals(nodes: u64) -> impl Strategy<Value = Proposals<Edge>> {
    prop::collection::vec(
        ((0..nodes, 0..nodes), any::<bool>())
            .prop_map(|((from, to), delete)| (Tup2(from, to), delete)),
        0..5,
    )
}

/// Generates workloads that insert and delete edges between `nodes` nodes:
/// up to five transactions of up to three steps.
///
/// # Arguments
///
/// * `nodes` - the number of nodes.
///
/// # Returns
///
/// A strategy for workloads.
fn edge_workloads(nodes: u64) -> impl Strategy<Value = Vec<Transaction<EdgeChanges>>> {
    workloads(edge_proposals(nodes), 5, 3).prop_map(|raw| {
        let mut live = BTreeSet::new();
        raw.into_iter()
            .map(|transaction| {
                transaction
                    .into_iter()
                    .map(|proposals| apply_proposals(&mut live, &proposals))
                    .collect()
            })
            .collect()
    })
}

/// Path doubling survives the scripted triggers under every configuration.
#[test]
fn path_doubling_triggers() {
    for workload in doubling_triggers() {
        for config in configs() {
            check(&PathDoubling, &workload, config);
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(CIRCUIT_CASES))]

    /// Path doubling computes the closure of random graphs, cycles included.
    #[test]
    fn path_doubling_random(workload in edge_workloads(7), config in any_config()) {
        check(&PathDoubling, &workload, config);
    }
}
