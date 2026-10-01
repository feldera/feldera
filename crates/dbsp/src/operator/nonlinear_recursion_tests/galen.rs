//! The rules of the Galen medical ontology benchmark, over small domains.
//!
//! The input holds facts of six relations, and the rules derive more facts of
//! two of them, `p` and `q`.  Read as an ontology, the relations mean:
//!
//! * `p(x, y)`: class `x` is a subclass of class `y`.
//! * `q(x, r, y)`: every `x` has relationship `r` to some `y`.
//! * `c(x, y, z)`: whatever is both an `x` and a `y` is a `z`.
//! * `u(x, r, y)`: whatever has relationship `r` to some `x` is a `y`.
//! * `s(r1, r2)`: relationship `r1` implies relationship `r2`.
//! * `r(r1, r2, r3)`: relationship `r1` followed by relationship `r2` implies
//!   relationship `r3`.
//!
//! ```text
//! p(x, z)    :- p(x, y), p(y, z).                    // IR1
//! q(x, r, z) :- p(x, y), q(y, r, z).                 // IR2
//! p(x, z)    :- p(y, w), u(w, r, z), q(x, r, y).     // IR3
//! p(x, z)    :- c(y, w, z), p(x, w), p(x, y).        // IR4
//! q(x, e, z) :- q(x, r, z), s(r, e).                 // IR5
//! q(x, e, o) :- q(x, y, z), r(y, u, e), q(z, u, o).  // IR6
//! ```
//!
//! Every rule but IR5 joins two relations that depend on the recursion.  The
//! circuit joins in the order of `benches/galen.rs`.

use std::collections::BTreeSet;

use proptest::prelude::*;

use super::harness::{
    Program, Transaction, ZSet, any_config, apply_proposals, check, configs, fixpoint, map_steps,
    proposals, read_zset, set_after, set_zset, workloads,
};
use crate::{
    OutputHandle, RootCircuit, Stream, ZSetHandle, ZWeight,
    typed_batch::{OrdZSet, SpineSnapshot},
    utils::{Tup2, Tup3, test::CIRCUIT_CASES},
};

type Pair = Tup2<u64, u64>;
type Triple = Tup3<u64, u64, u64>;

/// Changes to the six inputs in one step: the base facts of `p` and `q`,
/// and the relations `r`, `c`, `u`, and `s`.
#[derive(Clone, Debug, Default)]
struct GalenInput {
    p: Vec<(Pair, ZWeight)>,
    q: Vec<(Triple, ZWeight)>,
    r: Vec<(Triple, ZWeight)>,
    c: Vec<(Triple, ZWeight)>,
    u: Vec<(Triple, ZWeight)>,
    s: Vec<(Pair, ZWeight)>,
}

/// The Galen rules.
#[derive(Clone)]
struct Galen;

impl Program for Galen {
    type Input = GalenInput;
    type Handles = (
        [ZSetHandle<Pair>; 2],
        [ZSetHandle<Triple>; 4],
        OutputHandle<SpineSnapshot<OrdZSet<Pair>>>,
        OutputHandle<SpineSnapshot<OrdZSet<Triple>>>,
    );
    type Output = (ZSet<Pair>, ZSet<Triple>);

    fn build(&self, circuit: &mut RootCircuit) -> Self::Handles {
        let (p, p_handle) = circuit.add_input_zset::<Pair>();
        let (q, q_handle) = circuit.add_input_zset::<Triple>();
        let (r, r_handle) = circuit.add_input_zset::<Triple>();
        let (c, c_handle) = circuit.add_input_zset::<Triple>();
        let (u, u_handle) = circuit.add_input_zset::<Triple>();
        let (s, s_handle) = circuit.add_input_zset::<Pair>();
        let (p_out, q_out) = circuit
            .recursive(
                |child, (p_var, q_var): (Stream<_, OrdZSet<Pair>>, Stream<_, OrdZSet<Triple>>)| {
                    let p_by_1 = p_var.map_index(|Tup2(x, y)| (*x, *y));
                    let p_by_2 = p_var.map_index(|Tup2(x, y)| (*y, *x));
                    let p_by_12 = p_var.map_index(|Tup2(x, y)| (Tup2(*x, *y), ()));
                    let q_by_1 = q_var.map_index(|Tup3(x, y, z)| (*x, Tup2(*y, *z)));
                    let q_by_2 = q_var.map_index(|Tup3(x, y, z)| (*y, Tup2(*x, *z)));
                    let q_by_12 = q_var.map_index(|Tup3(x, y, z)| (Tup2(*x, *y), *z));
                    let q_by_23 = q_var.map_index(|Tup3(x, y, z)| (Tup2(*y, *z), *x));
                    let u_by_1 = u
                        .delta0(child)
                        .map_index(|Tup3(x, y, z)| (*x, Tup2(*y, *z)));
                    let c_by_2 = c
                        .delta0(child)
                        .map_index(|Tup3(x, y, z)| (*y, Tup2(*x, *z)));
                    let r_by_1 = r
                        .delta0(child)
                        .map_index(|Tup3(x, y, z)| (*x, Tup2(*y, *z)));
                    let s_by_1 = s.delta0(child).map_index(|Tup2(x, y)| (*x, *y));

                    let ir1 = p_by_2.join(&p_by_1, |_y, x, z| Tup2(*x, *z));
                    let ir2 = p_by_2.join(&q_by_1, |_y, x, Tup2(r, z)| Tup3(*x, *r, *z));
                    let ir3 = p_by_2
                        .join_index(&u_by_1, |_w, y, Tup2(r, z)| Some((Tup2(*r, *y), *z)))
                        .join(&q_by_23, |_ry, z, x| Tup2(*x, *z));
                    let ir4 = c_by_2
                        .join_index(&p_by_2, |_w, Tup2(y, z), x| Some((Tup2(*x, *y), *z)))
                        .join(&p_by_12, |Tup2(x, _y), z, _| Tup2(*x, *z));
                    let ir5 = q_by_2.join(&s_by_1, |_r, Tup2(x, z), q| Tup3(*x, *q, *z));
                    let ir6 = q_by_2
                        .join_index(&r_by_1, |_y, Tup2(x, z), Tup2(u, e)| {
                            Some((Tup2(*z, *u), Tup2(*x, *e)))
                        })
                        .join(&q_by_12, |_zu, Tup2(x, e), o| Tup3(*x, *e, *o));

                    Ok((
                        p.delta0(child).sum([&ir1, &ir3, &ir4]),
                        q.delta0(child).sum([&ir2, &ir5, &ir6]),
                    ))
                },
            )
            .unwrap();
        (
            [p_handle, s_handle],
            [q_handle, r_handle, c_handle, u_handle],
            p_out.accumulate_integrate().accumulate_output(),
            q_out.accumulate_integrate().accumulate_output(),
        )
    }

    fn push(&self, ([p, s], [q, r, c, u], _, _): &Self::Handles, input: &GalenInput) {
        for (handle, changes) in [(p, &input.p), (s, &input.s)] {
            for (row, weight) in changes {
                handle.push(*row, *weight);
            }
        }
        for (handle, changes) in [(q, &input.q), (r, &input.r), (c, &input.c), (u, &input.u)] {
            for (row, weight) in changes {
                handle.push(*row, *weight);
            }
        }
    }

    fn read(&self, (_, _, p, q): &Self::Handles) -> Self::Output {
        (read_zset(p), read_zset(q))
    }

    fn model(&self, inputs: &[GalenInput]) -> Self::Output {
        let p0 = set_after(inputs.iter().map(|input| input.p.as_slice()));
        let q0 = set_after(inputs.iter().map(|input| input.q.as_slice()));
        let r = set_after(inputs.iter().map(|input| input.r.as_slice()));
        let c = set_after(inputs.iter().map(|input| input.c.as_slice()));
        let u = set_after(inputs.iter().map(|input| input.u.as_slice()));
        let s = set_after(inputs.iter().map(|input| input.s.as_slice()));
        let (p, q) = fixpoint(
            (BTreeSet::new(), BTreeSet::new()),
            |(p, q): &(BTreeSet<Pair>, BTreeSet<Triple>)| {
                let mut p_next = p0.clone();
                let mut q_next = q0.clone();
                for Tup2(x, y) in p {
                    // IR1
                    for Tup2(_, z) in p.range(Tup2(*y, 0)..=Tup2(*y, u64::MAX)) {
                        p_next.insert(Tup2(*x, *z));
                    }
                    // IR2
                    for Tup3(_, r, z) in q.range(Tup3(*y, 0, 0)..=Tup3(*y, u64::MAX, u64::MAX)) {
                        q_next.insert(Tup3(*x, *r, *z));
                    }
                }
                // IR3: p(x,z) :- p(y,w), u(w,r,z), q(x,r,y)
                for Tup2(y, w) in p {
                    for Tup3(_, r, z) in u.range(Tup3(*w, 0, 0)..=Tup3(*w, u64::MAX, u64::MAX)) {
                        for Tup3(x, qr, qy) in q {
                            if qr == r && qy == y {
                                p_next.insert(Tup2(*x, *z));
                            }
                        }
                    }
                }
                // IR4: p(x,z) :- c(y,w,z), p(x,w), p(x,y)
                for Tup3(y, w, z) in &c {
                    for Tup2(x, pw) in p {
                        if pw == w && p.contains(&Tup2(*x, *y)) {
                            p_next.insert(Tup2(*x, *z));
                        }
                    }
                }
                for Tup3(x, y, z) in q {
                    // IR5
                    for Tup2(_, q_label) in s.range(Tup2(*y, 0)..=Tup2(*y, u64::MAX)) {
                        q_next.insert(Tup3(*x, *q_label, *z));
                    }
                    // IR6: q(x,e,o) :- q(x,y,z), r(y,u,e), q(z,u,o)
                    for Tup3(_, u_label, e) in
                        r.range(Tup3(*y, 0, 0)..=Tup3(*y, u64::MAX, u64::MAX))
                    {
                        for Tup3(_, _, o) in
                            q.range(Tup3(*z, *u_label, 0)..=Tup3(*z, *u_label, u64::MAX))
                        {
                            q_next.insert(Tup3(*x, *e, *o));
                        }
                    }
                }
                (p_next, q_next)
            },
        );
        (set_zset(&p), set_zset(&q))
    }
}

/// Changes that insert or delete facts `p(x,y)`.
///
/// # Arguments
///
/// * `facts` - the facts, as `(x, y)`.
/// * `weight` - 1 to insert, -1 to delete.
///
/// # Returns
///
/// The changes.
fn p_facts(facts: &[(u64, u64)], weight: ZWeight) -> GalenInput {
    GalenInput {
        p: facts.iter().map(|&(x, y)| (Tup2(x, y), weight)).collect(),
        ..GalenInput::default()
    }
}

/// Changes that insert or delete facts `q(x,0,z)`.
///
/// # Arguments
///
/// * `facts` - the facts, as `(x, z)`.
/// * `weight` - 1 to insert, -1 to delete.
///
/// # Returns
///
/// The changes.
fn q_facts(facts: &[(u64, u64)], weight: ZWeight) -> GalenInput {
    GalenInput {
        q: facts
            .iter()
            .map(|&(x, z)| (Tup3(x, 0, z), weight))
            .collect(),
        ..GalenInput::default()
    }
}

/// Workloads whose later transactions make IR1 and IR6 compute output for the
/// same later iteration in two iterations of one run.
///
/// IR1 doubles paths of `p`, and, given `r(0,0,0)`, IR6 doubles paths of `q`
/// whose edges are all labeled 0.  Each workload is thus a [path
/// doubling](super::closure) trigger: the chain `0 -> ... -> 8`, then a new
/// edge `9 -> 0`.  Next to the chain in `p`, the fact `q(8,0,20)` lets IR2
/// extend `q` along the paths.  Paths have many derivations, and losing some
/// of them loses no path; deleting an edge must retract every derivation of
/// the paths over it, which exposes any that were lost.
///
/// # Returns
///
/// The workloads, each a list of transactions.
fn galen_triggers() -> Vec<Vec<Transaction<GalenInput>>> {
    let chain: Vec<(u64, u64)> = (0..8).map(|x| (x, x + 1)).collect();
    let p_setup = GalenInput {
        q: vec![(Tup3(8, 0, 20), 1)],
        ..p_facts(&chain, 1)
    };
    let q_setup = GalenInput {
        r: vec![(Tup3(0, 0, 0), 1)],
        ..q_facts(&chain, 1)
    };
    vec![
        vec![
            vec![p_setup],
            vec![p_facts(&[(9, 0)], 1)],
            vec![p_facts(&[(4, 5)], -1)],
            vec![p_facts(&[(9, 0)], -1)],
        ],
        // Two new edges, each in its own step of one transaction.
        vec![
            vec![q_setup],
            vec![q_facts(&[(9, 0)], 1), q_facts(&[(10, 9)], 1)],
            vec![q_facts(&[(4, 5)], -1)],
            vec![q_facts(&[(9, 0)], -1)],
        ],
    ]
}

/// Generates workloads over a domain of 4 values in every position.
///
/// # Returns
///
/// A strategy for workloads: up to five transactions of up to three steps.
fn galen_workloads() -> impl Strategy<Value = Vec<Transaction<GalenInput>>> {
    let pair = (0..4u64, 0..4u64).prop_map(|(x, y)| Tup2(x, y));
    let triple = (0..4u64, 0..4u64, 0..4u64).prop_map(|(x, y, z)| Tup3(x, y, z));
    let step = (
        proposals(pair.clone(), 2),
        proposals(triple.clone(), 2),
        proposals(triple.clone(), 1),
        proposals(triple.clone(), 1),
        proposals(triple, 1),
        proposals(pair, 1),
    );
    workloads(step, 5, 3).prop_map(|raw| {
        let mut live = <[BTreeSet<Triple>; 4]>::default();
        let mut live_pairs = <[BTreeSet<Pair>; 2]>::default();
        map_steps(raw, |(p, q, r, c, u, s)| GalenInput {
            p: apply_proposals(&mut live_pairs[0], &p),
            q: apply_proposals(&mut live[0], &q),
            r: apply_proposals(&mut live[1], &r),
            c: apply_proposals(&mut live[2], &c),
            u: apply_proposals(&mut live[3], &u),
            s: apply_proposals(&mut live_pairs[1], &s),
        })
    })
}

/// The Galen rules survive their scripted triggers under every
/// configuration.
#[test]
fn galen_triggers_hold() {
    for workload in galen_triggers() {
        for config in configs() {
            check(&Galen, &workload, config);
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(CIRCUIT_CASES))]

    /// The Galen rules over random facts.
    #[test]
    fn galen_random(workload in galen_workloads(), config in any_config()) {
        check(&Galen, &workload, config);
    }
}
