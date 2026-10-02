//! Transitive closures whose joins pair two recursive inputs.
//!
//! * [`PathDoubling`] joins the paths found so far with themselves.
//! * [`MutualPaths`] alternates between two relations, each extended with
//!   paths of the other.
//! * [`StarDoubling`] joins paths only at joinable nodes, with a three-way
//!   star join on the node where they meet.

use std::collections::BTreeSet;

use proptest::prelude::*;

use super::harness::{
    Program, RecursionApi, RecursiveWith, Transaction, ZSet, any_config, apply_proposals, check,
    configs, fixpoint, map_steps, proposals, read_zset, set_after, set_zset, workloads,
};
use crate::{
    OutputHandle, RootCircuit, Stream, ZSetHandle, ZWeight, define_inner_star_join,
    typed_batch::{OrdZSet, SpineSnapshot},
    utils::{Tup2, test::CIRCUIT_CASES},
};

define_inner_star_join!(3);

/// An edge or a path: `(from, to)`.
type Edge = Tup2<u64, u64>;

/// Changes to edges in one step.
type EdgeChanges = Vec<(Edge, ZWeight)>;

/// Changes to nodes in one step.
type NodeChanges = Vec<(u64, ZWeight)>;

/// Handle for reading the paths a program found.
type PathsHandle = OutputHandle<SpineSnapshot<OrdZSet<Edge>>>;

/// Joins paths at the node where one ends and the other starts.
///
/// # Arguments
///
/// * `left` - paths to extend.
/// * `right` - paths to extend them with.
/// * `joinable` - whether paths may be joined at a node.
///
/// # Returns
///
/// The joined paths.
fn concat(
    left: &BTreeSet<Edge>,
    right: &BTreeSet<Edge>,
    joinable: impl Fn(u64) -> bool,
) -> BTreeSet<Edge> {
    let mut paths = BTreeSet::new();
    for Tup2(from, via) in left {
        if joinable(*via) {
            for Tup2(_, to) in right.range(Tup2(*via, 0)..=Tup2(*via, u64::MAX)) {
                paths.insert(Tup2(*from, *to));
            }
        }
    }
    paths
}

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
    type Handles = (ZSetHandle<Edge>, PathsHandle);
    type Output = ZSet<Edge>;

    fn build(&self, circuit: &mut RootCircuit, api: RecursionApi) -> Self::Handles {
        let (edges, edges_handle) = circuit.add_input_zset::<Edge>();
        let paths = circuit
            .recursive_with(api, |child, paths: Stream<_, OrdZSet<Edge>>| {
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
        let edges = set_after(inputs.iter().map(Vec::as_slice));
        let paths = fixpoint(BTreeSet::new(), |paths| {
            edges
                .union(&concat(paths, paths, |_| true))
                .cloned()
                .collect()
        });
        set_zset(&paths)
    }
}

/// Two relations, each extended with paths of the other.  `e(x, y)` and
/// `f(x, y)` are edges, and `r(x, y)` and `s(x, y)` paths, from `x` to `y`:
///
/// ```text
/// r(x, y) :- e(x, y).
/// r(x, z) :- r(x, y), s(y, z).
/// s(x, y) :- f(x, y).
/// s(x, z) :- s(x, y), r(y, z).
/// ```
#[derive(Clone)]
struct MutualPaths;

impl Program for MutualPaths {
    /// Changes to `e` and `f`.
    type Input = (EdgeChanges, EdgeChanges);
    type Handles = (ZSetHandle<Edge>, ZSetHandle<Edge>, PathsHandle, PathsHandle);
    type Output = (ZSet<Edge>, ZSet<Edge>);

    fn build(&self, circuit: &mut RootCircuit, api: RecursionApi) -> Self::Handles {
        let (e, e_handle) = circuit.add_input_zset::<Edge>();
        let (f, f_handle) = circuit.add_input_zset::<Edge>();
        let (r, s) = circuit
            .recursive_with(
                api,
                |child, (r, s): (Stream<_, OrdZSet<Edge>>, Stream<_, OrdZSet<Edge>>)| {
                    let r_by_end = r.map_index(|Tup2(from, via)| (*via, *from));
                    let r_by_start = r.map_index(|Tup2(via, to)| (*via, *to));
                    let s_by_end = s.map_index(|Tup2(from, via)| (*via, *from));
                    let s_by_start = s.map_index(|Tup2(via, to)| (*via, *to));
                    let r_next = e
                        .delta0(child)
                        .plus(&r_by_end.join(&s_by_start, |_via, from, to| Tup2(*from, *to)));
                    let s_next = f
                        .delta0(child)
                        .plus(&s_by_end.join(&r_by_start, |_via, from, to| Tup2(*from, *to)));
                    Ok((r_next, s_next))
                },
            )
            .unwrap();
        (
            e_handle,
            f_handle,
            r.accumulate_integrate().accumulate_output(),
            s.accumulate_integrate().accumulate_output(),
        )
    }

    fn push(&self, (e, f, _, _): &Self::Handles, (e_changes, f_changes): &Self::Input) {
        for (edge, weight) in e_changes {
            e.push(*edge, *weight);
        }
        for (edge, weight) in f_changes {
            f.push(*edge, *weight);
        }
    }

    fn read(&self, (_, _, r, s): &Self::Handles) -> Self::Output {
        (read_zset(r), read_zset(s))
    }

    fn model(&self, inputs: &[Self::Input]) -> Self::Output {
        let e = set_after(inputs.iter().map(|(e, _)| e.as_slice()));
        let f = set_after(inputs.iter().map(|(_, f)| f.as_slice()));
        let (r, s) = fixpoint(
            (BTreeSet::new(), BTreeSet::new()),
            |(r, s): &(BTreeSet<Edge>, BTreeSet<Edge>)| {
                (
                    e.union(&concat(r, s, |_| true)).cloned().collect(),
                    f.union(&concat(s, r, |_| true)).cloned().collect(),
                )
            },
        );
        (set_zset(&r), set_zset(&s))
    }
}

/// Path doubling that joins paths only at joinable nodes, through a star join.
///
/// Every edge is a path, and two paths make a longer one when the first ends
/// where the second starts and that node is joinable.  `joinable(y)` holds
/// when paths may be joined at node `y`:
///
/// ```text
/// paths(x, y) :- edges(x, y).
/// paths(x, z) :- paths(x, y), paths(y, z), joinable(y).
/// ```
///
/// A path thus leads from `x` to `z` when a chain of edges does and every node
/// that the chain passes through is joinable.  The three atoms of the second
/// rule share the variable `y`, so one three-way star join evaluates them, and
/// two of its inputs depend on the recursion.  A star join runs on the
/// `MatchKeys` operator, which schedules output for later iterations on its
/// own, apart from the join operator that [`PathDoubling`] uses; this program
/// tests it end to end.
///
/// The joinable nodes also give workloads another way to cut paths: a node
/// that stops being joinable cuts every path through it.
#[derive(Clone)]
struct StarDoubling;

impl Program for StarDoubling {
    /// Changes to the edges, and to the nodes that paths may be joined at.
    type Input = (EdgeChanges, NodeChanges);
    type Handles = (ZSetHandle<Edge>, ZSetHandle<u64>, PathsHandle);
    type Output = ZSet<Edge>;

    fn build(&self, circuit: &mut RootCircuit, api: RecursionApi) -> Self::Handles {
        let (edges, edges_handle) = circuit.add_input_zset::<Edge>();
        let (joinable, joinable_handle) = circuit.add_input_zset::<u64>();
        let paths = circuit
            .recursive_with(api, |child, paths: Stream<_, OrdZSet<Edge>>| {
                let by_end = paths.map_index(|Tup2(from, via)| (*via, *from));
                let by_start = paths.map_index(|Tup2(via, to)| (*via, *to));
                let joinable = joinable.delta0(child).map_index(|via| (*via, ()));
                Ok(edges.delta0(child).plus(&inner_star_join3_nested(
                    &by_end,
                    &by_start,
                    &joinable,
                    |_via, from, to, _| Tup2(*from, *to),
                )))
            })
            .unwrap();
        (
            edges_handle,
            joinable_handle,
            paths.accumulate_integrate().accumulate_output(),
        )
    }

    fn push(&self, (edges, joinable, _): &Self::Handles, (edge_changes, nodes): &Self::Input) {
        for (edge, weight) in edge_changes {
            edges.push(*edge, *weight);
        }
        for (node, weight) in nodes {
            joinable.push(*node, *weight);
        }
    }

    fn read(&self, (_, _, paths): &Self::Handles) -> ZSet<Edge> {
        read_zset(paths)
    }

    fn model(&self, inputs: &[Self::Input]) -> ZSet<Edge> {
        let edges = set_after(inputs.iter().map(|(edges, _)| edges.as_slice()));
        let joinable = set_after(inputs.iter().map(|(_, nodes)| nodes.as_slice()));
        let paths = fixpoint(BTreeSet::new(), |paths| {
            edges
                .union(&concat(paths, paths, |via| joinable.contains(&via)))
                .cloned()
                .collect()
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

/// The chain `0 -> 1 -> ... -> 8`, whose paths of 2, 3-4, and 5-8 edges path
/// doubling finds in iterations 1, 2, and 3.
///
/// # Returns
///
/// Changes that insert the chain.
fn chain() -> EdgeChanges {
    let edges: Vec<(u64, u64)> = (0..8).map(|node| (node, node + 1)).collect();
    edge_changes(&edges, 1)
}

/// Workloads whose later transactions make path doubling compute output for
/// the same later iteration in two iterations of one run.
///
/// The first transaction adds [`chain`].  A later edge `9 -> 0` reaches the
/// join in one iteration, and the path `9 -> 1` it yields reaches the join in
/// the next.  Both meet paths of the chain found in later iterations, so the
/// join computes output for the same later iterations in two consecutive
/// iterations.  Paths have many derivations, and losing some of them loses no
/// path; cutting the chain must retract every derivation of the paths across
/// the cut, which exposes any that were lost.
///
/// # Returns
///
/// The workloads, each a list of transactions.
fn doubling_triggers() -> Vec<Vec<Transaction<EdgeChanges>>> {
    let prepend = vec![edge_changes(&[(9, 0)], 1)];
    let cut = |weight| vec![edge_changes(&[(4, 5)], weight)];
    vec![
        // The trigger, then the cut.
        vec![vec![chain()], prepend.clone(), cut(-1)],
        // Two new edges, each in its own step of one transaction, then the
        // cut.
        vec![
            vec![chain()],
            vec![edge_changes(&[(9, 0)], 1), edge_changes(&[(10, 9)], 1)],
            cut(-1),
        ],
        // The trigger, then deletions and reinsertions that change paths
        // found in late iterations.
        vec![
            vec![chain()],
            prepend.clone(),
            vec![edge_changes(&[(9, 0)], -1)],
            cut(-1),
            prepend,
            cut(1),
        ],
        // A cycle through the whole chain.
        vec![vec![chain()], vec![edge_changes(&[(8, 0)], 1)]],
    ]
}

/// [`doubling_triggers`] for [`MutualPaths`], with the chain in both `e` and
/// `f`, the new edges in either, and cuts in either or both.
///
/// # Returns
///
/// The workloads, each a list of transactions.
fn mutual_triggers() -> Vec<Vec<Transaction<(EdgeChanges, EdgeChanges)>>> {
    let chains = vec![(chain(), chain())];
    let in_e = |edges: &[(u64, u64)], weight| (edge_changes(edges, weight), vec![]);
    let in_f = |edges: &[(u64, u64)], weight| (vec![], edge_changes(edges, weight));
    // Cuts both chains.
    let cut = vec![(edge_changes(&[(4, 5)], -1), edge_changes(&[(4, 5)], -1))];
    vec![
        vec![chains.clone(), vec![in_e(&[(9, 0)], 1)], cut.clone()],
        vec![chains.clone(), vec![in_f(&[(9, 0)], 1)], cut.clone()],
        vec![
            chains.clone(),
            vec![in_e(&[(9, 0)], 1), in_f(&[(10, 9)], 1)],
            cut,
        ],
        vec![
            chains,
            vec![in_e(&[(9, 0)], 1)],
            vec![in_e(&[(4, 5)], -1)],
            vec![in_f(&[(4, 5)], -1)],
            vec![in_e(&[(4, 5)], 1), in_f(&[(4, 5)], 1)],
        ],
    ]
}

/// [`doubling_triggers`] for [`StarDoubling`]: the chain and the new edges,
/// with every node joinable, then node 4 no longer joinable, which cuts every
/// path through it.
///
/// # Returns
///
/// The workloads, each a list of transactions.
fn star_triggers() -> Vec<Vec<Transaction<(EdgeChanges, NodeChanges)>>> {
    let setup = vec![(chain(), (0..11).map(|node| (node, 1)).collect())];
    let prepend = vec![(edge_changes(&[(9, 0)], 1), vec![])];
    vec![
        // Two new edges, each in its own step of one transaction.
        vec![
            setup.clone(),
            vec![
                (edge_changes(&[(9, 0)], 1), vec![]),
                (edge_changes(&[(10, 9)], 1), vec![]),
            ],
            vec![(vec![], vec![(4, -1)])],
        ],
        // The trigger, then node 4 stops being joinable and becomes joinable
        // again.
        vec![
            setup,
            prepend,
            vec![(vec![], vec![(4, -1)])],
            vec![(vec![], vec![(4, 1)])],
        ],
    ]
}

/// Generates edges between `nodes` nodes, loops and cycles included.
///
/// # Arguments
///
/// * `nodes` - the number of nodes.
///
/// # Returns
///
/// A strategy for edges.
fn edge(nodes: u64) -> impl Strategy<Value = Edge> {
    (0..nodes, 0..nodes).prop_map(|(from, to)| Tup2(from, to))
}

/// Generates workloads for [`PathDoubling`]: up to five transactions of up to
/// three steps, over 7 nodes.
///
/// # Returns
///
/// A strategy for workloads.
fn doubling_workloads() -> impl Strategy<Value = Vec<Transaction<EdgeChanges>>> {
    workloads(proposals(edge(7), 4), 5, 3).prop_map(|raw| {
        let mut edges = BTreeSet::new();
        map_steps(raw, |proposals| apply_proposals(&mut edges, &proposals))
    })
}

/// Generates workloads for [`MutualPaths`], over 6 nodes.
///
/// # Returns
///
/// A strategy for workloads.
fn mutual_workloads() -> impl Strategy<Value = Vec<Transaction<(EdgeChanges, EdgeChanges)>>> {
    workloads((proposals(edge(6), 4), proposals(edge(6), 4)), 5, 3).prop_map(|raw| {
        let (mut e, mut f) = (BTreeSet::new(), BTreeSet::new());
        map_steps(raw, |(e_proposals, f_proposals)| {
            (
                apply_proposals(&mut e, &e_proposals),
                apply_proposals(&mut f, &f_proposals),
            )
        })
    })
}

/// Generates workloads for [`StarDoubling`], over 7 nodes.
///
/// # Returns
///
/// A strategy for workloads.
fn star_workloads() -> impl Strategy<Value = Vec<Transaction<(EdgeChanges, NodeChanges)>>> {
    workloads((proposals(edge(7), 4), proposals(0..7u64, 4)), 5, 3).prop_map(|raw| {
        let (mut edges, mut joinable) = (BTreeSet::new(), BTreeSet::new());
        map_steps(raw, |(edge_proposals, node_proposals)| {
            (
                apply_proposals(&mut edges, &edge_proposals),
                apply_proposals(&mut joinable, &node_proposals),
            )
        })
    })
}

/// Path doubling survives its scripted triggers under every configuration.
#[test]
fn path_doubling_triggers() {
    for workload in doubling_triggers() {
        for config in configs() {
            check(&PathDoubling, &workload, config);
        }
    }
}

/// Mutually recursive paths survive their scripted triggers under every
/// configuration.
#[test]
fn mutual_paths_triggers() {
    for workload in mutual_triggers() {
        for config in configs() {
            check(&MutualPaths, &workload, config);
        }
    }
}

/// Star-join path doubling survives its scripted triggers under every
/// configuration.
#[test]
fn star_doubling_triggers() {
    for workload in star_triggers() {
        for config in configs() {
            check(&StarDoubling, &workload, config);
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(CIRCUIT_CASES))]

    /// Path doubling computes the closure of random graphs, cycles included.
    #[test]
    fn path_doubling_random(workload in doubling_workloads(), config in any_config()) {
        check(&PathDoubling, &workload, config);
    }

    /// Mutually recursive paths over random pairs of graphs.
    #[test]
    fn mutual_paths_random(workload in mutual_workloads(), config in any_config()) {
        check(&MutualPaths, &workload, config);
    }

    /// Star-join path doubling over random graphs and joinable nodes.
    #[test]
    fn star_doubling_random(workload in star_workloads(), config in any_config()) {
        check(&StarDoubling, &workload, config);
    }
}
