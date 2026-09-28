//! Trips that combine road and rail legs, with a full outer join of two
//! recursive relations inside the recursion.
//!
//! In the rules below:
//!
//! * `road(x, y)`: a road leg from `x` to `y`.
//! * `rail(x, y)`: a rail leg from `x` to `y`.
//! * `by_road(x, y)`: a trip from `x` to `y` whose last leg is by road.
//! * `by_rail(x, y)`: a trip from `x` to `y` whose last leg is by rail.
//! * `trips(x, y, road, rail)`: a trip from `x` to `y`, where `road` and
//!   `rail` tell whether its last leg can be by road and whether it can be by
//!   rail.
//!
//! ```text
//! by_road(x, y)            :- road(x, y).
//! by_road(x, z)            :- trips(x, y, _, _), road(y, z).
//! by_rail(x, y)            :- rail(x, y).
//! by_rail(x, z)            :- trips(x, y, _, _), rail(y, z).
//! trips(x, y, true, true)  :- by_road(x, y), by_rail(x, y).
//! trips(x, y, true, false) :- by_road(x, y), !by_rail(x, y).
//! trips(x, y, false, true) :- by_rail(x, y), !by_road(x, y).
//! ```
//!
//! The last three rules are a full outer join of `by_road` and `by_rail`: an
//! inner join and two antijoins, each of which joins `by_road` with `by_rail`,
//! so both inputs of each depend on the recursion.  The recursion negates
//! itself, so its result is the fixed point that iterating from the empty set
//! reaches.

use std::collections::BTreeSet;

use proptest::prelude::*;

use super::harness::{
    Program, Transaction, ZSet, any_config, apply_proposals, check, configs, fixpoint, map_steps,
    proposals, read_zset, set_after, set_zset, workloads,
};
use crate::{
    NestedCircuit, OutputHandle, RootCircuit, Stream, ZSetHandle, ZWeight,
    typed_batch::{OrdZSet, SpineSnapshot},
    utils::{Tup2, Tup4, test::CIRCUIT_CASES},
};

/// A leg, or a trip: `(from, to)`.
type Leg = Tup2<u64, u64>;

/// A trip, with whether its last leg can be by road and whether it can be by
/// rail: `(from, to, road, rail)`.
type Trip = Tup4<u64, u64, bool, bool>;

/// Changes to road and rail legs in one step.
#[derive(Clone, Debug, Default)]
struct LegChanges {
    road: Vec<(Leg, ZWeight)>,
    rail: Vec<(Leg, ZWeight)>,
}

/// The trips of a road and a rail network.
#[derive(Clone)]
struct MultimodalTrips;

/// The state of a [`MultimodalTrips`] recursion: `by_road`, `by_rail`, and
/// `trips`.
type TripsState = (BTreeSet<Leg>, BTreeSet<Leg>, BTreeSet<Trip>);

impl Program for MultimodalTrips {
    type Input = LegChanges;
    type Handles = (
        ZSetHandle<Leg>,
        ZSetHandle<Leg>,
        OutputHandle<SpineSnapshot<OrdZSet<Trip>>>,
    );
    type Output = ZSet<Trip>;

    fn build(&self, circuit: &mut RootCircuit) -> Self::Handles {
        let (road, road_handle) = circuit.add_input_zset::<Leg>();
        let (rail, rail_handle) = circuit.add_input_zset::<Leg>();
        let (_, _, trips) = circuit
            .recursive(
                |child,
                 (by_road, by_rail, trips): (
                    Stream<_, OrdZSet<Leg>>,
                    Stream<_, OrdZSet<Leg>>,
                    Stream<_, OrdZSet<Trip>>,
                )| {
                    let road = road.delta0(child);
                    let rail = rail.delta0(child);
                    // Trips, indexed by where they end.
                    let trip_ends = trips.map_index(|Tup4(from, to, _, _)| (*to, *from));
                    let extend = |legs: &Stream<NestedCircuit, OrdZSet<Leg>>| {
                        legs.plus(&trip_ends.join(
                            &legs.map_index(|Tup2(from, to)| (*from, *to)),
                            |_via, from, to| Tup2(*from, *to),
                        ))
                    };
                    // All three joins depend on the recursion on both sides.
                    let joined = by_road.map_index(|leg| (*leg, ())).outer_join(
                        &by_rail.map_index(|leg| (*leg, ())),
                        |Tup2(from, to), _, _| Tup4(*from, *to, true, true),
                        |Tup2(from, to), _| Tup4(*from, *to, true, false),
                        |Tup2(from, to), _| Tup4(*from, *to, false, true),
                    );
                    Ok((extend(&road), extend(&rail), joined))
                },
            )
            .unwrap();
        (
            road_handle,
            rail_handle,
            trips.accumulate_integrate().accumulate_output(),
        )
    }

    fn push(&self, (road, rail, _): &Self::Handles, input: &LegChanges) {
        for (row, weight) in &input.road {
            road.push(*row, *weight);
        }
        for (row, weight) in &input.rail {
            rail.push(*row, *weight);
        }
    }

    fn read(&self, (_, _, trips): &Self::Handles) -> ZSet<Trip> {
        read_zset(trips)
    }

    fn model(&self, inputs: &[LegChanges]) -> ZSet<Trip> {
        let road = set_after(inputs.iter().map(|input| input.road.as_slice()));
        let rail = set_after(inputs.iter().map(|input| input.rail.as_slice()));
        let (_, _, trips) = fixpoint(
            TripsState::default(),
            |(by_road, by_rail, trips): &TripsState| {
                let extend = |legs: &BTreeSet<Leg>| {
                    let mut extended = legs.clone();
                    for Tup4(from, via, _, _) in trips {
                        for Tup2(_, to) in legs.range(Tup2(*via, 0)..=Tup2(*via, u64::MAX)) {
                            extended.insert(Tup2(*from, *to));
                        }
                    }
                    extended
                };
                let joined = by_road
                    .union(by_rail)
                    .map(|leg @ Tup2(from, to)| {
                        Tup4(*from, *to, by_road.contains(leg), by_rail.contains(leg))
                    })
                    .collect();
                (extend(&road), extend(&rail), joined)
            },
        );
        set_zset(&trips)
    }
}

/// Changes that insert or delete road legs.
///
/// # Arguments
///
/// * `legs` - the legs, as `(from, to)`.
/// * `weight` - 1 to insert, -1 to delete.
///
/// # Returns
///
/// The changes.
fn road(legs: &[(u64, u64)], weight: ZWeight) -> LegChanges {
    LegChanges {
        road: legs
            .iter()
            .map(|&(from, to)| (Tup2(from, to), weight))
            .collect(),
        ..LegChanges::default()
    }
}

/// Changes that insert or delete rail legs.
///
/// # Arguments
///
/// * `legs` - the legs, as `(from, to)`.
/// * `weight` - 1 to insert, -1 to delete.
///
/// # Returns
///
/// The changes.
fn rail(legs: &[(u64, u64)], weight: ZWeight) -> LegChanges {
    LegChanges {
        rail: legs
            .iter()
            .map(|&(from, to)| (Tup2(from, to), weight))
            .collect(),
        ..LegChanges::default()
    }
}

/// Workloads whose later transactions make the joins of the outer join compute
/// output for the same later iteration in two iterations of one run.
///
/// A rail line `0 -> 1 -> ... -> 8` and a road `0 -> 11 -> ... -> 17 -> 8`
/// each take 8 legs, so `by_road` and `by_rail` gain the trip from 0 to 8 in
/// the same late iteration.  A later transaction adds a direct road or rail
/// leg from 0 to 8, so that one of the two relations gains the trip at once.
/// That change meets the trip in the other relation, which computes output for
/// the late iteration, and the late iteration changes the first relation
/// again, since it no longer gains the trip there.  Each trip has one row, and
/// a lost update only changes the weight of that row; deleting every rail or
/// every road derivation of the trip must change the row, which exposes the
/// loss.
///
/// # Returns
///
/// The workloads, each a list of transactions.
fn trips_triggers() -> Vec<Vec<Transaction<LegChanges>>> {
    let rail_line: Vec<(u64, u64)> = (0..8).map(|node| (node, node + 1)).collect();
    let road_line = [
        (0, 11),
        (11, 12),
        (12, 13),
        (13, 14),
        (14, 15),
        (15, 16),
        (16, 17),
        (17, 8),
    ];
    let setup = LegChanges {
        road: road(&road_line, 1).road,
        rail: rail(&rail_line, 1).rail,
    };
    vec![
        vec![
            vec![setup.clone()],
            vec![road(&[(0, 8)], 1)],
            vec![rail(&[(7, 8)], -1)],
        ],
        vec![
            vec![setup.clone()],
            vec![rail(&[(0, 8)], 1)],
            vec![road(&[(17, 8)], -1)],
        ],
        // Both direct legs, each in its own step of one transaction.
        vec![
            vec![setup],
            vec![road(&[(0, 8)], 1), rail(&[(0, 8)], 1)],
            vec![rail(&[(0, 8), (7, 8)], -1)],
        ],
    ]
}

/// Generates workloads over 6 nodes.
///
/// # Returns
///
/// A strategy for workloads: up to five transactions of up to three steps.
fn trips_workloads() -> impl Strategy<Value = Vec<Transaction<LegChanges>>> {
    let leg = (0..6u64, 0..6u64).prop_map(|(from, to)| Tup2(from, to));
    workloads((proposals(leg.clone(), 3), proposals(leg, 3)), 5, 3).prop_map(|raw| {
        let (mut road, mut rail) = (BTreeSet::new(), BTreeSet::new());
        map_steps(raw, |(road_proposals, rail_proposals)| LegChanges {
            road: apply_proposals(&mut road, &road_proposals),
            rail: apply_proposals(&mut rail, &rail_proposals),
        })
    })
}

/// Multimodal trips survive their scripted triggers under every
/// configuration.
#[test]
fn trips_triggers_hold() {
    for workload in trips_triggers() {
        for config in configs() {
            check(&MultimodalTrips, &workload, config);
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(CIRCUIT_CASES))]

    /// Multimodal trips over random networks.
    #[test]
    fn trips_random(workload in trips_workloads(), config in any_config()) {
        check(&MultimodalTrips, &workload, config);
    }
}
