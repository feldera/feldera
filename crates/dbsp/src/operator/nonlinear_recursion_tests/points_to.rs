//! Andersen-style points-to analysis with fields.
//!
//! Variables point to objects through allocations and assignments; objects'
//! fields point to objects through stores, and variables read them through
//! loads.  In the rules below:
//!
//! * `alloc(var, obj)`: the statement `var = new obj`.
//! * `assign(to, from)`: the statement `to = from`.
//! * `store(base, field, from)`: the statement `base.field = from`.
//! * `load(to, base, field)`: the statement `to = base.field`.
//! * `points_to(var, obj)`: variable `var` may point to object `obj`.
//! * `field_points_to(base_obj, field, obj)`: field `field` of object
//!   `base_obj` may point to object `obj`.
//!
//! ```text
//! points_to(var, obj) :- alloc(var, obj).
//! points_to(to, obj)  :- assign(to, from), points_to(from, obj).
//! field_points_to(base_obj, field, obj) :-
//!     store(base, field, from),
//!     points_to(base, base_obj),
//!     points_to(from, obj).
//! points_to(to, obj) :-
//!     load(to, base, field),
//!     points_to(base, base_obj),
//!     field_points_to(base_obj, field, obj).
//! ```
//!
//! The store rule joins two points-to facts, and the load rule joins a
//! points-to fact with a field fact, so both rules join two relations that
//! depend on the recursion.

use std::collections::BTreeSet;

use proptest::prelude::*;

use super::harness::{
    Program, RecursionApi, RecursiveWith, Transaction, ZSet, any_config, apply_proposals, check,
    configs, fixpoint, map_steps, proposals, read_zset, set_after, set_zset, workloads,
};
use crate::{
    OutputHandle, RootCircuit, Stream, ZSetHandle, ZWeight,
    typed_batch::{OrdZSet, SpineSnapshot},
    utils::{Tup2, Tup3, test::CIRCUIT_CASES},
};

/// `var = new object`: `(var, object)`.
type Alloc = Tup2<u64, u64>;

/// `to = from`: `(to, from)`.
type Assign = Tup2<u64, u64>;

/// `base.field = from`: `(base, field, from)`.
type Store = Tup3<u64, u64, u64>;

/// `to = base.field`: `(to, base, field)`.
type Load = Tup3<u64, u64, u64>;

/// A variable points to an object: `(var, object)`.
type PointsTo = Tup2<u64, u64>;

/// A field of an object points to an object: `(object, field, object)`.
type FieldPointsTo = Tup3<u64, u64, u64>;

/// Changes to the four inputs in one step.
#[derive(Clone, Debug, Default)]
struct ProgramFacts {
    alloc: Vec<(Alloc, ZWeight)>,
    assign: Vec<(Assign, ZWeight)>,
    store: Vec<(Store, ZWeight)>,
    load: Vec<(Load, ZWeight)>,
}

/// Points-to analysis.
#[derive(Clone)]
struct PointsToAnalysis;

impl Program for PointsToAnalysis {
    type Input = ProgramFacts;
    type Handles = (
        ZSetHandle<Alloc>,
        ZSetHandle<Assign>,
        ZSetHandle<Store>,
        ZSetHandle<Load>,
        OutputHandle<SpineSnapshot<OrdZSet<PointsTo>>>,
        OutputHandle<SpineSnapshot<OrdZSet<FieldPointsTo>>>,
    );
    /// What every variable and every field points to.
    type Output = (ZSet<PointsTo>, ZSet<FieldPointsTo>);

    fn build(&self, circuit: &mut RootCircuit, api: RecursionApi) -> Self::Handles {
        let (alloc, alloc_handle) = circuit.add_input_zset::<Alloc>();
        let (assign, assign_handle) = circuit.add_input_zset::<Assign>();
        let (store, store_handle) = circuit.add_input_zset::<Store>();
        let (load, load_handle) = circuit.add_input_zset::<Load>();
        let (points_to, field_points_to) = circuit
            .recursive_with(
                api,
                |child,
                 (points_to, field_points_to): (
                    Stream<_, OrdZSet<PointsTo>>,
                    Stream<_, OrdZSet<FieldPointsTo>>,
                )| {
                    let by_var = points_to.map_index(|Tup2(var, object)| (*var, *object));
                    let assigned = assign
                        .delta0(child)
                        .map_index(|Tup2(to, from)| (*from, *to))
                        .join(&by_var, |_from, to, object| Tup2(*to, *object));
                    // Both inputs of the second join depend on the recursion.
                    let stored = store
                        .delta0(child)
                        .map_index(|Tup3(base, field, from)| (*base, Tup2(*field, *from)))
                        .join_index(&by_var, |_base, Tup2(field, from), base_object| {
                            Some((*from, Tup2(*base_object, *field)))
                        })
                        .join(&by_var, |_from, Tup2(base_object, field), object| {
                            Tup3(*base_object, *field, *object)
                        });
                    // So do both inputs of this one.
                    let loaded = load
                        .delta0(child)
                        .map_index(|Tup3(to, base, field)| (*base, Tup2(*to, *field)))
                        .join_index(&by_var, |_base, Tup2(to, field), base_object| {
                            Some((Tup2(*base_object, *field), *to))
                        })
                        .join(
                            &field_points_to.map_index(|Tup3(base_object, field, object)| {
                                (Tup2(*base_object, *field), *object)
                            }),
                            |_base_field, to, object| Tup2(*to, *object),
                        );
                    Ok((alloc.delta0(child).plus(&assigned).plus(&loaded), stored))
                },
            )
            .unwrap();
        (
            alloc_handle,
            assign_handle,
            store_handle,
            load_handle,
            points_to.accumulate_integrate().accumulate_output(),
            field_points_to.accumulate_integrate().accumulate_output(),
        )
    }

    fn push(&self, (alloc, assign, store, load, _, _): &Self::Handles, input: &ProgramFacts) {
        for (row, weight) in &input.alloc {
            alloc.push(*row, *weight);
        }
        for (row, weight) in &input.assign {
            assign.push(*row, *weight);
        }
        for (row, weight) in &input.store {
            store.push(*row, *weight);
        }
        for (row, weight) in &input.load {
            load.push(*row, *weight);
        }
    }

    fn read(&self, (_, _, _, _, points_to, field_points_to): &Self::Handles) -> Self::Output {
        (read_zset(points_to), read_zset(field_points_to))
    }

    fn model(&self, inputs: &[ProgramFacts]) -> Self::Output {
        let alloc = set_after(inputs.iter().map(|input| input.alloc.as_slice()));
        let assign = set_after(inputs.iter().map(|input| input.assign.as_slice()));
        let store = set_after(inputs.iter().map(|input| input.store.as_slice()));
        let load = set_after(inputs.iter().map(|input| input.load.as_slice()));
        let objects_of = |points_to: &BTreeSet<PointsTo>, var: u64| {
            points_to
                .range(Tup2(var, 0)..=Tup2(var, u64::MAX))
                .map(|Tup2(_, object)| *object)
                .collect::<Vec<_>>()
        };
        let (points_to, field_points_to) = fixpoint(
            (BTreeSet::new(), BTreeSet::new()),
            |(points_to, field_points_to): &(BTreeSet<PointsTo>, BTreeSet<FieldPointsTo>)| {
                let mut points_to_next = alloc.clone();
                for Tup2(to, from) in &assign {
                    for object in objects_of(points_to, *from) {
                        points_to_next.insert(Tup2(*to, object));
                    }
                }
                for Tup3(to, base, field) in &load {
                    for base_object in objects_of(points_to, *base) {
                        for Tup3(_, _, object) in field_points_to.range(
                            Tup3(base_object, *field, 0)..=Tup3(base_object, *field, u64::MAX),
                        ) {
                            points_to_next.insert(Tup2(*to, *object));
                        }
                    }
                }
                let mut field_points_to_next = BTreeSet::new();
                for Tup3(base, field, from) in &store {
                    for base_object in objects_of(points_to, *base) {
                        for object in objects_of(points_to, *from) {
                            field_points_to_next.insert(Tup3(base_object, *field, object));
                        }
                    }
                }
                (points_to_next, field_points_to_next)
            },
        );
        (set_zset(&points_to), set_zset(&field_points_to))
    }
}

/// Workloads whose later transactions make the store rule compute output for
/// the same later iteration in two iterations of one run.
///
/// The first transaction allocates object 100 to variable 0 and assigns it
/// along the chain `1 = 0`, ..., `8 = 7`, so variable 8 points to it from
/// iteration 8.  It also stores variables 20 and 22 into fields 0 and 1 of the
/// object that variable 8 points to, and loads those fields into variables 30
/// and 31.  A later transaction allocates objects 200 and 201 to variables 20
/// and 19, and assigns `20 = 19`: variable 20 then points to a new object in
/// each of two consecutive iterations, and both times meets its store through
/// variable 8, which the first transaction resolved in a later iteration.
/// Both changes have the same join key, so they meet on one worker however
/// many workers there are.  Every field fact has one derivation, so losing a
/// derivation loses the fact.
///
/// # Returns
///
/// The workloads, each a list of transactions.
fn points_to_triggers() -> Vec<Vec<Transaction<ProgramFacts>>> {
    let setup = ProgramFacts {
        alloc: vec![(Tup2(0, 100), 1)],
        assign: (1..=8).map(|var| (Tup2(var, var - 1), 1)).collect(),
        store: vec![(Tup3(8, 0, 20), 1), (Tup3(8, 1, 22), 1)],
        load: vec![(Tup3(30, 8, 0), 1), (Tup3(31, 8, 1), 1)],
    };
    // Makes variable `var` point to `object` directly, and to `object + 1`
    // through variable `var - 1`.
    let two_objects = |var: u64, object: u64, weight: ZWeight| ProgramFacts {
        alloc: vec![
            (Tup2(var, object), weight),
            (Tup2(var - 1, object + 1), weight),
        ],
        assign: vec![(Tup2(var, var - 1), weight)],
        ..ProgramFacts::default()
    };
    let cut_chain = |weight| ProgramFacts {
        assign: vec![(Tup2(4, 3), weight)],
        ..ProgramFacts::default()
    };
    vec![
        vec![vec![setup.clone()], vec![two_objects(20, 200, 1)]],
        // Two triggers, each in its own step of one transaction.
        vec![
            vec![setup.clone()],
            vec![two_objects(20, 200, 1), two_objects(22, 202, 1)],
        ],
        // A trigger; then retract it, cut the chain, and restore both.
        vec![
            vec![setup],
            vec![two_objects(20, 200, 1)],
            vec![two_objects(20, 200, -1)],
            vec![cut_chain(-1)],
            vec![two_objects(20, 200, 1)],
            vec![cut_chain(1)],
        ],
    ]
}

/// Generates workloads over 5 variables, 3 objects, and 2 fields.
///
/// # Returns
///
/// A strategy for workloads: up to five transactions of up to three steps.
fn points_to_workloads() -> impl Strategy<Value = Vec<Transaction<ProgramFacts>>> {
    let alloc = (0..5u64, 0..3u64).prop_map(|(var, object)| Tup2(var, 100 + object));
    let assign = (0..5u64, 0..5u64).prop_map(|(to, from)| Tup2(to, from));
    let store = (0..5u64, 0..2u64, 0..5u64).prop_map(|(base, field, from)| Tup3(base, field, from));
    let load = (0..5u64, 0..5u64, 0..2u64).prop_map(|(to, base, field)| Tup3(to, base, field));
    let step = (
        proposals(alloc, 2),
        proposals(assign, 3),
        proposals(store, 2),
        proposals(load, 2),
    );
    workloads(step, 5, 3).prop_map(|raw| {
        let mut live = (
            BTreeSet::new(),
            BTreeSet::new(),
            BTreeSet::new(),
            BTreeSet::new(),
        );
        map_steps(raw, |(alloc, assign, store, load)| ProgramFacts {
            alloc: apply_proposals(&mut live.0, &alloc),
            assign: apply_proposals(&mut live.1, &assign),
            store: apply_proposals(&mut live.2, &store),
            load: apply_proposals(&mut live.3, &load),
        })
    })
}

/// Points-to analysis survives its scripted triggers under every
/// configuration.
#[test]
fn points_to_triggers_hold() {
    for workload in points_to_triggers() {
        for config in configs() {
            check(&PointsToAnalysis, &workload, config);
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(CIRCUIT_CASES))]

    /// Points-to analysis of random programs.
    #[test]
    fn points_to_random(workload in points_to_workloads(), config in any_config()) {
        check(&PointsToAnalysis, &workload, config);
    }
}
