//! A bill of materials: when each part is finished, and how many leaf parts it
//! takes.
//!
//! A leaf part takes its own time to make and counts as one leaf part.  An
//! assembly is finished one time unit after the last of its parts, and takes
//! the leaf parts of all of them.  In the rules below:
//!
//! * `leaves(part, time)`: leaf part `part` takes `time` to make.
//! * `part_of(part, assembly)`: `part` is one of the parts of `assembly`.
//! * `stats(part, finished, leaf_parts)`: `part` is finished at time
//!   `finished` and takes `leaf_parts` leaf parts.
//!
//! ```text
//! stats(part, time, 1) :- leaves(part, time).
//! stats(assembly, max(finished) + 1, sum(leaf_parts)) :-
//!     part_of(part, assembly),
//!     stats(part, finished, leaf_parts).
//! ```
//!
//! The second rule groups by `assembly`, and its aggregates range over every
//! match of its body.  SQL computes both aggregates with one `GROUP BY` that
//! mixes `MAX` and `SUM`, which the SQL compiler turns into two aggregates
//! joined on the group key; both inputs of that join depend on the recursion.

use std::collections::{BTreeMap, BTreeSet};

use proptest::prelude::*;

use super::harness::{
    Program, RecursionApi, RecursiveWith, Transaction, ZSet, any_config, apply_proposals, check,
    configs, fixpoint, map_steps, proposals, read_zset, set_after, set_zset, workloads,
};
use crate::{
    OutputHandle, RootCircuit, Stream, ZSetHandle, ZWeight,
    operator::Max,
    typed_batch::{OrdZSet, SpineSnapshot},
    utils::{Tup2, Tup3, test::CIRCUIT_CASES},
};

/// A part of an assembly: `(part, assembly)`.
type PartOf = Tup2<u64, u64>;

/// A leaf part and the time it takes to make: `(part, time)`.
type Leaf = Tup2<u64, u64>;

/// When a part is finished, and how many leaf parts it takes: `(part,
/// finished, leaf parts)`.
type PartStats = Tup3<u64, u64, i64>;

/// Changes to the two inputs in one step.
#[derive(Clone, Debug, Default)]
struct BomInput {
    part_of: Vec<(PartOf, ZWeight)>,
    leaves: Vec<(Leaf, ZWeight)>,
}

/// Finishing times and leaf part counts of a bill of materials.
#[derive(Clone)]
struct BillOfMaterials;

impl Program for BillOfMaterials {
    type Input = BomInput;
    type Handles = (
        ZSetHandle<PartOf>,
        ZSetHandle<Leaf>,
        OutputHandle<SpineSnapshot<OrdZSet<PartStats>>>,
    );
    type Output = ZSet<PartStats>;

    fn build(&self, circuit: &mut RootCircuit, api: RecursionApi) -> Self::Handles {
        let (part_of, part_of_handle) = circuit.add_input_zset::<PartOf>();
        let (leaves, leaves_handle) = circuit.add_input_zset::<Leaf>();
        let stats = circuit
            .recursive_with(api, |child, stats: Stream<_, OrdZSet<PartStats>>| {
                let leaf_stats = leaves
                    .delta0(child)
                    .map(|Tup2(part, time)| Tup3(*part, *time, 1));
                // The finishing time and leaf parts of each part of each
                // assembly, indexed by the assembly.
                let part_stats = stats
                    .map_index(|Tup3(part, finished, leaf_parts)| {
                        (*part, Tup2(*finished, *leaf_parts))
                    })
                    .join_index(
                        &part_of
                            .delta0(child)
                            .map_index(|Tup2(part, assembly)| (*part, *assembly)),
                        |_part, stats, assembly| Some((*assembly, *stats)),
                    );
                let last_finished = part_stats
                    .map_index(|(assembly, Tup2(finished, _))| (*assembly, *finished))
                    .aggregate(Max);
                let leaf_parts =
                    part_stats.aggregate_linear(|Tup2(_, leaf_parts): &Tup2<u64, i64>| *leaf_parts);
                // Both inputs depend on the recursion.
                let assembly_stats = last_finished
                    .join(&leaf_parts, |assembly, finished, leaf_parts| {
                        Tup3(*assembly, finished + 1, *leaf_parts)
                    });
                Ok(leaf_stats.plus(&assembly_stats))
            })
            .unwrap();
        (
            part_of_handle,
            leaves_handle,
            stats.accumulate_integrate().accumulate_output(),
        )
    }

    fn push(&self, (part_of, leaves, _): &Self::Handles, input: &BomInput) {
        for (row, weight) in &input.part_of {
            part_of.push(*row, *weight);
        }
        for (row, weight) in &input.leaves {
            leaves.push(*row, *weight);
        }
    }

    fn read(&self, (_, _, stats): &Self::Handles) -> ZSet<PartStats> {
        read_zset(stats)
    }

    fn model(&self, inputs: &[BomInput]) -> ZSet<PartStats> {
        let part_of = set_after(inputs.iter().map(|input| input.part_of.as_slice()));
        let leaves = set_after(inputs.iter().map(|input| input.leaves.as_slice()));
        let stats = fixpoint(BTreeSet::new(), |stats: &BTreeSet<PartStats>| {
            let mut next: BTreeSet<PartStats> = leaves
                .iter()
                .map(|Tup2(part, time)| Tup3(*part, *time, 1))
                .collect();
            // The latest finishing time and the total leaf parts of the parts
            // of each assembly.
            let mut assemblies = BTreeMap::<u64, (u64, i64)>::new();
            for Tup2(part, assembly) in &part_of {
                for Tup3(_, finished, leaf_parts) in
                    stats.range(Tup3(*part, 0, i64::MIN)..=Tup3(*part, u64::MAX, i64::MAX))
                {
                    let (last_finished, total_leaf_parts) =
                        assemblies.entry(*assembly).or_default();
                    *last_finished = (*last_finished).max(*finished);
                    *total_leaf_parts += leaf_parts;
                }
            }
            next.extend(assemblies.into_iter().map(
                |(assembly, (last_finished, total_leaf_parts))| {
                    Tup3(assembly, last_finished + 1, total_leaf_parts)
                },
            ));
            next
        });
        set_zset(&stats)
    }
}

/// Changes that change the time a leaf part takes.
///
/// # Arguments
///
/// * `part` - the leaf part.
/// * `old` - the time it took.
/// * `new` - the time it takes now.
///
/// # Returns
///
/// The changes.
fn retime(part: u64, old: u64, new: u64) -> BomInput {
    BomInput {
        leaves: vec![(Tup2(part, old), -1), (Tup2(part, new), 1)],
        ..BomInput::default()
    }
}

/// Workloads whose later transactions make the join of the two aggregates
/// compute output for the same later iteration in two iterations of one run.
///
/// Assembly 20 consists of leaf part 1, of assembly 21, which consists of leaf
/// part 2, and of the last assembly of the chain `10 -> 11 -> ... -> 17`.  The
/// aggregates of assembly 20 change in iterations 1, 2, and 8 as its parts are
/// finished.  A later transaction changes the times of leaf parts 1 and 2,
/// which changes when assembly 20's first two parts are finished, and so its
/// latest finishing time in iterations 1 and 2.  Both changes meet the leaf
/// part count that the first transaction computed in iteration 8.  Every row
/// of the output has one derivation, so losing a derivation loses the row.
///
/// # Returns
///
/// The workloads, each a list of transactions.
fn bom_triggers() -> Vec<Vec<Transaction<BomInput>>> {
    let chain = (10..17).map(|part| (Tup2(part, part + 1), 1));
    let setup = BomInput {
        part_of: [Tup2(1, 20), Tup2(21, 20), Tup2(17, 20), Tup2(2, 21)]
            .into_iter()
            .map(|row| (row, 1))
            .chain(chain)
            .collect(),
        leaves: vec![(Tup2(1, 5), 1), (Tup2(2, 5), 1), (Tup2(10, 1), 1)],
    };
    // Changes the times of leaf parts 1 and 2, each given as `(old, new)`.
    let retime_both = |(old_1, new_1), (old_2, new_2)| BomInput {
        leaves: [
            retime(1, old_1, new_1).leaves,
            retime(2, old_2, new_2).leaves,
        ]
        .concat(),
        ..BomInput::default()
    };
    let detach_chain = |weight| BomInput {
        part_of: vec![(Tup2(17, 20), weight)],
        ..BomInput::default()
    };
    vec![
        vec![vec![setup.clone()], vec![retime_both((5, 6), (5, 9))]],
        // Two triggers, each in its own step of one transaction.  Were the
        // second the inverse of the first, it would lose the inverse of what
        // the first lost, and the losses would cancel.
        vec![
            vec![setup.clone()],
            vec![retime_both((5, 6), (5, 9)), retime_both((6, 7), (9, 11))],
        ],
        // A trigger; then detach the chain, restore the times, and reattach it.
        vec![
            vec![setup],
            vec![retime_both((5, 6), (5, 9))],
            vec![detach_chain(-1)],
            vec![retime_both((6, 5), (9, 5))],
            vec![detach_chain(1)],
        ],
    ]
}

/// Generates workloads over 8 parts, in which a part may only belong to an
/// assembly with a greater number, so that no part contains itself.
///
/// # Returns
///
/// A strategy for workloads: up to five transactions of up to three steps.
fn bom_workloads() -> impl Strategy<Value = Vec<Transaction<BomInput>>> {
    let part_of = prop::sample::select(
        (0..8u64)
            .flat_map(|assembly| (0..assembly).map(move |part| Tup2(part, assembly)))
            .collect::<Vec<_>>(),
    );
    let leaf = (0..8u64, 0..4u64).prop_map(|(part, time)| Tup2(part, time));
    workloads((proposals(part_of, 4), proposals(leaf, 3)), 5, 3).prop_map(|raw| {
        let (mut part_of, mut leaves) = (BTreeSet::new(), BTreeSet::new());
        map_steps(raw, |(part_of_proposals, leaf_proposals)| BomInput {
            part_of: apply_proposals(&mut part_of, &part_of_proposals),
            leaves: apply_proposals(&mut leaves, &leaf_proposals),
        })
    })
}

/// The bill of materials survives its scripted triggers under every
/// configuration.
#[test]
fn bom_triggers_hold() {
    for workload in bom_triggers() {
        for config in configs() {
            check(&BillOfMaterials, &workload, config);
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(CIRCUIT_CASES))]

    /// Random bills of materials.
    #[test]
    fn bom_random(workload in bom_workloads(), config in any_config()) {
        check(&BillOfMaterials, &workload, config);
    }
}
