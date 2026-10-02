//! Transitive closures cut off after a bounded number of iterations.
//!
//! The [recursion builder](crate::ChildCircuit::recursion_builder) can stop a
//! recursion after `k` iterations, leave out its `distinct`, and report how
//! each run of the recursion ended.  These tests build a transitive closure
//! with one of two steps, a linear one and a doubling one:
//!
//! ```text
//! paths(x, y) :- edges(x, y).
//! paths(x, z) :- paths(x, y), edges(y, z).   % linear
//! paths(x, z) :- paths(x, y), paths(y, z).   % doubling
//! ```
//!
//! Iteration `n` derives paths from the paths of iteration `n - 1`, so a bound
//! of `k` keeps the paths of at most `k` edges with the linear step, and of at
//! most `2^(k - 1)` edges with the doubling step.  Without `distinct`,
//! relations are bags, and the weight of a path is its number of derivations
//! in the first `k` iterations.
//!
//! Each run of the recursion reports how many iterations it took and whether
//! the bound cut it short.  The reports of all workers must agree, a run cut
//! short must have taken exactly `k` iterations, and while no run has been
//! cut short, the output must be a fixed point.

use std::{collections::BTreeSet, num::NonZeroU64};

use proptest::prelude::*;

use super::harness::{
    Config, RecursionApi, Transaction, ZSet, apply_proposals, fixpoint, map_steps, proposals,
    read_zset, set_after, set_zset, workloads,
};
use crate::{
    OutputHandle, RootCircuit, Runtime, ZSetHandle, ZWeight,
    operator::RecursionReport,
    typed_batch::{OrdZSet, SpineSnapshot},
    utils::{Tup2, test::CIRCUIT_CASES},
};

/// An edge or a path: `(from, to)`.
type Edge = Tup2<u64, u64>;

/// Changes to edges in one step.
type EdgeChanges = Vec<(Edge, ZWeight)>;

/// Handles for pushing edges and reading the paths and the reports of a
/// closure.
type Handles = (
    ZSetHandle<Edge>,
    OutputHandle<SpineSnapshot<OrdZSet<Edge>>>,
    OutputHandle<RecursionReport>,
);

/// How an iteration extends the paths of the previous one.
#[derive(Clone, Copy, Debug)]
enum Step {
    /// With an edge.
    Linear,

    /// With a path.
    Doubling,
}

/// A transitive closure built with the recursion builder.
#[derive(Clone, Copy, Debug)]
struct BoundedClosure {
    /// How an iteration extends the paths.
    step: Step,

    /// The maximum number of iterations in a run, or `None` for no maximum.
    bound: Option<NonZeroU64>,

    /// Whether the recursion applies `distinct`.
    distinct: bool,
}

impl BoundedClosure {
    /// Builds the circuit.
    ///
    /// # Arguments
    ///
    /// * `circuit` - the root circuit to build in.
    ///
    /// # Returns
    ///
    /// Handles for pushing edges and reading the paths and the reports.
    fn build(&self, circuit: &mut RootCircuit) -> Handles {
        let (edges, edges_handle) = circuit.add_input_zset::<Edge>();
        let step = self.step;
        let builder = circuit.recursion_builder(
            |child| Ok(child.recursive_var::<OrdZSet<Edge>>()),
            move |child, paths| {
                let edges = edges.delta0(child);
                let by_end = paths.map_index(|Tup2(from, via)| (*via, *from));
                let extensions = match step {
                    Step::Linear => edges.map_index(|Tup2(via, to)| (*via, *to)),
                    Step::Doubling => paths.map_index(|Tup2(via, to)| (*via, *to)),
                };
                Ok(edges.plus(&by_end.join(&extensions, |_via, from, to| Tup2(*from, *to))))
            },
        );
        let builder = if self.distinct {
            builder
        } else {
            builder.without_distinct()
        };
        let builder = match self.bound {
            Some(bound) => builder.with_bound(bound),
            None => builder,
        };
        let (paths, reports) = builder.with_report().finish().unwrap();
        (
            edges_handle,
            paths.accumulate_integrate().accumulate_output(),
            reports.output(),
        )
    }

    /// Computes the paths of an iteration from the paths of the previous one.
    ///
    /// # Arguments
    ///
    /// * `edges` - the edges.
    /// * `paths` - the paths of the previous iteration.
    ///
    /// # Returns
    ///
    /// The paths of the iteration.
    fn next(&self, edges: &ZSet<Edge>, paths: &ZSet<Edge>) -> ZSet<Edge> {
        let extensions = match self.step {
            Step::Linear => edges,
            Step::Doubling => paths,
        };
        let mut next = edges.clone();
        for (Tup2(from, via), weight) in paths {
            for (Tup2(_, to), extension_weight) in
                extensions.range(Tup2(*via, 0)..=Tup2(*via, u64::MAX))
            {
                *next.entry(Tup2(*from, *to)).or_default() += weight * extension_weight;
            }
        }
        if self.distinct {
            // Every weight is positive, so `distinct` sets it to 1.
            next.values_mut().for_each(|weight| *weight = 1);
        }
        next
    }

    /// Computes the paths a correct circuit outputs for some edges.
    ///
    /// # Arguments
    ///
    /// * `edges` - the edges.
    ///
    /// # Returns
    ///
    /// The paths of iteration `bound`, or of the fixed point without a bound.
    fn model(&self, edges: &ZSet<Edge>) -> ZSet<Edge> {
        match self.bound {
            Some(bound) => (0..bound.get()).fold(ZSet::new(), |paths, _| self.next(edges, &paths)),
            None => fixpoint(ZSet::new(), |paths| self.next(edges, paths)),
        }
    }

    /// Checks the reports of the recursion's last run.
    ///
    /// # Arguments
    ///
    /// * `reports` - the handle that `build` returned for the reports.
    /// * `workers` - the number of workers.
    /// * `context` - describes the run in failure messages.
    ///
    /// # Returns
    ///
    /// Whether the bound cut the run short.
    ///
    /// # Panics
    ///
    /// Panics if a worker has no report, if the workers' reports differ, or if
    /// the report contradicts the bound.
    fn check_report(
        &self,
        reports: &OutputHandle<RecursionReport>,
        workers: usize,
        context: &str,
    ) -> bool {
        let reports = reports.take_from_all();
        assert_eq!(reports.len(), workers, "{context}: {reports:?}");
        let report = reports[0];
        assert!(
            reports.iter().all(|other| *other == report),
            "{context}: workers disagree: {reports:?}"
        );
        assert!(report.iterations() >= 1, "{context}: {report:?}");
        match self.bound {
            Some(bound) if report.truncated() => {
                assert_eq!(report.iterations(), bound.get(), "{context}: {report:?}")
            }
            Some(bound) => assert!(report.iterations() <= bound.get(), "{context}: {report:?}"),
            None => assert!(report.converged(), "{context}: {report:?}"),
        }
        report.truncated()
    }
}

/// Runs a closure through a workload and checks its reports after every run
/// and its output after every transaction.
///
/// The recursion runs once in every step, including the steps that commit a
/// transaction, so every step's reports are checked.
///
/// # Arguments
///
/// * `closure` - the closure to test.
/// * `workload` - the transactions to run, in order.
/// * `config` - the circuit settings to run under.
///
/// # Panics
///
/// Panics if a report is wrong, if the output differs from the model's, or if
/// the output is not a fixed point while no report says that the bound cut a
/// run short.
fn check_closure(closure: BoundedClosure, workload: &[Transaction<EdgeChanges>], config: Config) {
    let (mut circuit, (edges, paths, reports)) =
        Runtime::init_circuit(config.circuit_config(), move |circuit| {
            Ok(closure.build(circuit))
        })
        .unwrap();

    let mut history = Vec::new();
    let mut truncated = false;
    for (index, transaction) in workload.iter().enumerate() {
        let context = format!("{closure:?}, {config:?}, transaction {index}");
        circuit.start_transaction().unwrap();
        for input in transaction {
            for (edge, weight) in input {
                edges.push(*edge, *weight);
            }
            circuit.step().unwrap();
            truncated |= closure.check_report(&reports, config.workers, &context);
        }
        circuit.start_commit_transaction().unwrap();
        loop {
            let committed = circuit.step().unwrap();
            truncated |= closure.check_report(&reports, config.workers, &context);
            if committed {
                break;
            }
        }
        history.extend(transaction.iter().cloned());

        let edges = set_zset(&set_after(history.iter().map(Vec::as_slice)));
        let expected = closure.model(&edges);
        assert_eq!(read_zset(&paths), expected, "{context}");
        if !truncated {
            assert_eq!(
                closure.next(&edges, &expected),
                expected,
                "{context}: no report says that the bound cut a run short"
            );
        }
    }

    circuit.kill().unwrap();
}

/// Generates closures: either step, a bound of 1 to 5 iterations or none,
/// and `distinct` on or off.  Without a bound, `distinct` stays on, since
/// counting the derivations of a path on a cycle never reaches a fixed point.
///
/// # Returns
///
/// A strategy for closures.
fn closures() -> impl Strategy<Value = BoundedClosure> {
    (
        prop_oneof![Just(Step::Linear), Just(Step::Doubling)],
        prop::option::weighted(0.8, 1..=5u64),
        any::<bool>(),
    )
        .prop_map(|(step, bound, distinct)| BoundedClosure {
            step,
            bound: bound.and_then(NonZeroU64::new),
            distinct: distinct || bound.is_none(),
        })
}

/// Generates circuit settings: 1 to 4 workers and a chunk size of 1, 2, or
/// the default.
///
/// # Returns
///
/// A strategy for circuit settings.
fn builder_configs() -> impl Strategy<Value = Config> {
    (
        1..=4usize,
        prop::sample::select(vec![Some(1), Some(2), None]),
    )
        .prop_map(|(workers, chunk_size)| Config {
            workers,
            chunk_size,
            api: RecursionApi::Builder,
        })
}

/// Generates workloads: up to five transactions of up to three steps, over 6
/// nodes, loops and cycles included.
///
/// # Returns
///
/// A strategy for workloads.
fn closure_workloads() -> impl Strategy<Value = Vec<Transaction<EdgeChanges>>> {
    let edge = (0..6u64, 0..6u64).prop_map(|(from, to)| Tup2(from, to));
    workloads(proposals(edge, 4), 5, 3).prop_map(|raw| {
        let mut edges = BTreeSet::new();
        map_steps(raw, |proposals| apply_proposals(&mut edges, &proposals))
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(CIRCUIT_CASES))]

    /// Bounded closures of random graphs match the model and report every run
    /// consistently.
    #[test]
    fn bounded_closure_random(
        closure in closures(),
        workload in closure_workloads(),
        config in builder_configs(),
    ) {
        check_closure(closure, &workload, config);
    }
}
