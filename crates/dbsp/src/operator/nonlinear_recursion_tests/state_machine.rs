//! A state machine that advances entities through versions.
//!
//! An entity starts in version 0 if an action creates version 0 and the entity
//! has no state yet, advances to version `v + 1` if an action creates that
//! version, and otherwise stays where it is.  In the rules below:
//!
//! * `actions(id, v)`: an action creates version `v` of entity `id`.
//! * `state(id, v)`: entity `id` is in version `v`.
//! * `next_state(id, v)`: entity `id` advances to version `v`.
//!
//! ```text
//! next_state(id, w) :- state(id, v), actions(id, w), w = v + 1.
//! state(id, 0)      :- actions(id, 0), !state(id, _).
//! state(id, v)      :- next_state(id, v).
//! state(id, v)      :- state(id, v), !next_state(id, _).
//! ```
//!
//! The recursion negates itself, so no stratification applies: its result is
//! the fixed point that iterating from the empty set reaches.  Only `state` is
//! recursive: each iteration derives `next_state` from the previous
//! iteration's `state`, and then the new `state` from the previous `state` and
//! the new `next_state`.  An antijoin removes from its first input the rows
//! whose keys its second input holds, by joining the first input with the
//! distinct keys of the second.  The last rule antijoins `state` with
//! `next_state`, so both inputs of that join depend on the recursion.

use std::collections::BTreeSet;

use proptest::prelude::*;

use super::harness::{
    Program, RecursionApi, RecursiveWith, Transaction, ZSet, any_config, apply_proposals, check,
    configs, fixpoint, map_steps, proposals, read_zset, set_after, set_zset, workloads,
};
use crate::{
    OutputHandle, RootCircuit, Stream, ZSetHandle, ZWeight,
    typed_batch::{OrdZSet, SpineSnapshot},
    utils::{Tup2, test::CIRCUIT_CASES},
};

/// An action that creates a version of an entity, or the state of an entity:
/// `(id, version)`.
type Version = Tup2<u64, u64>;

/// Changes to actions in one step.
type ActionChanges = Vec<(Version, ZWeight)>;

/// A state machine over the versions of entities.
#[derive(Clone)]
struct StateMachine;

impl Program for StateMachine {
    type Input = ActionChanges;
    type Handles = (
        ZSetHandle<Version>,
        OutputHandle<SpineSnapshot<OrdZSet<Version>>>,
    );
    type Output = ZSet<Version>;

    fn build(&self, circuit: &mut RootCircuit, api: RecursionApi) -> Self::Handles {
        let (actions, actions_handle) = circuit.add_input_zset::<Version>();
        let state = circuit
            .recursive_with(api, |child, state: Stream<_, OrdZSet<Version>>| {
                let actions = actions.delta0(child);
                let state_by_id = state.map_index(|Tup2(id, version)| (*id, *version));
                let next_state = state
                    .map_index(|Tup2(id, version)| (Tup2(*id, version + 1), ()))
                    .join(
                        &actions.map_index(|action| (*action, ())),
                        |action, _, _| *action,
                    );
                let started = actions
                    .filter(|Tup2(_, version)| *version == 0)
                    .map_index(|Tup2(id, version)| (*id, *version))
                    .antijoin(&state_by_id)
                    .map(|(id, version)| Tup2(*id, *version));
                // Both inputs depend on the recursion.
                let unchanged = state_by_id
                    .antijoin(&next_state.map_index(|Tup2(id, version)| (*id, *version)))
                    .map(|(id, version)| Tup2(*id, *version));
                Ok(started.plus(&next_state).plus(&unchanged))
            })
            .unwrap();
        (
            actions_handle,
            state.accumulate_integrate().accumulate_output(),
        )
    }

    fn push(&self, (actions, _): &Self::Handles, input: &ActionChanges) {
        for (row, weight) in input {
            actions.push(*row, *weight);
        }
    }

    fn read(&self, (_, state): &Self::Handles) -> ZSet<Version> {
        read_zset(state)
    }

    fn model(&self, inputs: &[ActionChanges]) -> ZSet<Version> {
        let actions = set_after(inputs.iter().map(Vec::as_slice));
        let has_state = |state: &BTreeSet<Version>, id: u64| {
            state
                .range(Tup2(id, 0)..=Tup2(id, u64::MAX))
                .next()
                .is_some()
        };
        let state = fixpoint(BTreeSet::new(), |state: &BTreeSet<Version>| {
            let next_state: BTreeSet<Version> = state
                .iter()
                .map(|Tup2(id, version)| Tup2(*id, version + 1))
                .filter(|action| actions.contains(action))
                .collect();
            let started = actions
                .iter()
                .filter(|Tup2(id, version)| *version == 0 && !has_state(state, *id));
            let unchanged = state
                .iter()
                .filter(|Tup2(id, _)| !has_state(&next_state, *id));
            started
                .chain(&next_state)
                .chain(unchanged)
                .cloned()
                .collect()
        });
        set_zset(&state)
    }
}

/// Changes that insert or delete actions of one entity.
///
/// # Arguments
///
/// * `id` - the entity.
/// * `versions` - the versions the actions create.
/// * `weight` - 1 to insert, -1 to delete.
///
/// # Returns
///
/// The changes.
fn actions(id: u64, versions: impl IntoIterator<Item = u64>, weight: ZWeight) -> ActionChanges {
    versions
        .into_iter()
        .map(|version| (Tup2(id, version), weight))
        .collect()
}

/// Workloads whose later transactions make the join inside the last rule's
/// antijoin compute output for the same later iteration in several iterations
/// of one run.
///
/// Entities 0 and 1 each advance through versions 0-8, one version per
/// iteration, so `next_state` holds a row for each of them in iterations 1-8
/// and loses it in iteration 9.  Deleting an early action of an entity changes
/// its state in every later iteration, and each change meets, on the entity's
/// key, the row that `next_state` lost in iteration 9 of the first
/// transaction.  An entity has one state, so losing a derivation loses the
/// state.
///
/// # Returns
///
/// The workloads, each a list of transactions.
fn state_machine_triggers() -> Vec<Vec<Transaction<ActionChanges>>> {
    let setup = [actions(0, 0..=8, 1), actions(1, 0..=8, 1)].concat();
    vec![
        // Stop entity 0 in version 0.
        vec![vec![setup.clone()], vec![actions(0, [1], -1)]],
        // Delete entity 0's first action, so that it never starts.
        vec![vec![setup.clone()], vec![actions(0, [0], -1)]],
        // Stop both entities early, each in its own step of one transaction.
        vec![
            vec![setup.clone()],
            vec![actions(0, [1], -1), actions(1, [2], -1)],
        ],
        // Stop entity 0, then let it advance again.
        vec![
            vec![setup],
            vec![actions(0, [1], -1)],
            vec![actions(0, [1], 1)],
        ],
    ]
}

/// Generates workloads over 3 entities and versions 0-5.
///
/// # Returns
///
/// A strategy for workloads: up to five transactions of up to three steps.
fn state_machine_workloads() -> impl Strategy<Value = Vec<Transaction<ActionChanges>>> {
    let action = (0..3u64, 0..6u64).prop_map(|(id, version)| Tup2(id, version));
    workloads(proposals(action, 4), 5, 3).prop_map(|raw| {
        let mut actions = BTreeSet::new();
        map_steps(raw, |proposals| apply_proposals(&mut actions, &proposals))
    })
}

/// The state machine survives its scripted triggers under every
/// configuration.
#[test]
fn state_machine_triggers_hold() {
    for workload in state_machine_triggers() {
        for config in configs() {
            check(&StateMachine, &workload, config);
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(CIRCUIT_CASES))]

    /// The state machine over random actions.
    #[test]
    fn state_machine_random(workload in state_machine_workloads(), config in any_config()) {
        check(&StateMachine, &workload, config);
    }
}
