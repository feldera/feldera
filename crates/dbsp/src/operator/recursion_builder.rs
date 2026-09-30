use crate::Timestamp;
use crate::circuit::Consensus;
use crate::circuit::checkpointer::Checkpoint;
use crate::circuit::circuit_builder::{CircuitBase, IterativeCircuit};
use crate::circuit::{FeedbackConnector, OwnershipPreference};
use crate::{
    ChildCircuit, Circuit, SchedulerError, Stream, ZWeight,
    operator::dynamic::{concat::dyn_concat_accumulated, distinct::DistinctFactories},
    trace::Spine,
    typed_batch::{BatchReader, DynIndexedZSet},
};
use impl_trait_for_tuples::impl_for_tuples;
use iteration_delay::IterationDelay;
use size_of::SizeOf;
use std::cell::Cell;
use std::marker::PhantomData;
use std::num::NonZeroU64;
use std::rc::Rc;

mod iteration_delay;

impl<P, T> ChildCircuit<P, T>
where
    P: 'static,
    T: Timestamp,
    Self: Circuit,
{
    /// Create a single recursive variable: an initially empty feedback stream
    /// that a [`RecursionBuilder`] later ties into a fixed-point loop.
    ///
    /// This is the building block of the unified recursion API.  Call it once
    /// per mutually recursive relation inside the builder's `init` closure; the
    /// number of calls determines the arity of the recursion, so the caller
    /// never supplies it explicitly.  See [`RecursionBuilder`] for a complete
    /// example.
    pub fn recursive_var<Z>(&self) -> RecursiveVar<Self, Z>
    where
        Z: BatchReader<R = ZWeight>,
        Z::Inner: Checkpoint + DynIndexedZSet + Send + Sync,
        Spine<Z::Inner>: SizeOf,
        <Self as Circuit>::Parent: Circuit,
    {
        let factories = DistinctFactories::new::<Z::Key, Z::Val>();
        let (delayed, feedback) =
            self.add_feedback(IterationDelay::new(&factories.input_factories));
        // The delay outputs an iteration's changes as a single spine; split
        // them into batches, as for the output of any accumulator.
        let stream =
            dyn_concat_accumulated(&factories.input_factories, [(delayed, true)]).typed::<Z>();

        RecursiveVar {
            feedback,
            factories,
            stream,
        }
    }

    /// Define a recursive computation over one or more mutually recursive
    /// streams.
    ///
    /// This is the unified entry point that subsumes both
    /// [`recursive`](Self::recursive) and
    /// [`recursive_dynamic`](Self::recursive_dynamic).  The `init` closure sets
    /// up the recursive variables with [`recursive_var`](Self::recursive_var);
    /// the shape it returns — a single [`RecursiveVar`], a tuple of them, or a
    /// [`Vec`] — fixes the arity, so none has to be supplied.  The `step`
    /// closure receives the matching feedback streams and defines the recursive
    /// computation.  In [persistent mode](crate::circuit::Mode::Persistent), the
    /// step closure must name the operators it creates; see
    /// [Persistent ids](RecursionBuilder#persistent-ids).
    ///
    /// The call returns a [`RecursionBuilder`] for this specific recursive
    /// computation, and the builder offers optional modifiers
    /// (for example [`without_distinct`](RecursionBuilder::without_distinct))
    /// before [`finish`](RecursionBuilder::finish) builds it.
    ///
    /// See [`RecursionBuilder`] for a complete example.
    pub fn recursion_builder<V, F1, F2>(
        &self,
        init: F1,
        step: F2,
    ) -> RecursionBuilder<'_, Self, F1, F2>
    where
        V: RecursionVars<IterativeCircuit<Self>>,
        F1: FnOnce(&IterativeCircuit<Self>) -> Result<V, SchedulerError>,
        F2: FnOnce(&IterativeCircuit<Self>, V::Streams) -> Result<V::Streams, SchedulerError>,
    {
        RecursionBuilder {
            circuit: self,
            init,
            step,
            distinct: true,
            bounded: None,
            report: NoReport(PhantomData),
        }
    }
}

/// A single recursive variable created by
/// [`recursive_var`](ChildCircuit::recursive_var).
///
/// It bundles an initially empty feedback stream with the factories and
/// feedback connector needed to close its loop once the step function has
/// produced the next iteration.  A [`RecursionBuilder`] performs the wiring;
/// callers only ever touch its feedback stream, which the builder passes to
/// the step closure.
pub struct RecursiveVar<C, Z>
where
    C: Circuit,
    Z: BatchReader,
    Z::Inner: DynIndexedZSet,
{
    feedback: FeedbackConnector<C, Z::Inner, Option<Spine<Z::Inner>>, IterationDelay<Z::Inner>>,
    factories: DistinctFactories<Z::Inner, C::Time>,
    stream: Stream<C, Z>,
}

impl<C, Z> RecursiveVar<C, Z>
where
    C: Circuit,
    C::Parent: Circuit,
    Z: BatchReader<R = ZWeight>,
    Z::Inner: Checkpoint + DynIndexedZSet + Send + Sync,
    Spine<Z::Inner>: SizeOf,
{
    /// Close this variable's loop: optionally apply `distinct`, connect the
    /// feedback, and export the integrated trace to the parent circuit.
    fn close(self, next: Stream<C, Z>, distinct: bool) -> ClosedVar<C, Z> {
        let RecursiveVar {
            feedback,
            factories,
            ..
        } = self;

        let next = next.inner();
        let next = if distinct {
            let persistent_id = next
                .get_persistent_id()
                .map(|name| format!("{name}.distinct"));
            next.dyn_distinct(&factories)
                .set_persistent_id(persistent_id.as_deref())
        } else {
            next
        };

        feedback.connect_with_preference(&next, OwnershipPreference::STRONGLY_PREFER_OWNED);
        let export = next
            .dyn_integrate_trace(&factories.input_factories)
            .export();

        ClosedVar { export, factories }
    }
}

/// A [`RecursiveVar`] whose loop has been closed.
///
/// Retains the factories needed to consolidate the exported trace into the
/// final output stream in the parent circuit.
pub struct ClosedVar<C, Z>
where
    C: Circuit,
    Z: BatchReader,
    Z::Inner: DynIndexedZSet,
{
    export: Stream<C::Parent, Spine<Z::Inner>>,
    factories: DistinctFactories<Z::Inner, C::Time>,
}

impl<C, Z> ClosedVar<C, Z>
where
    C: Circuit,
    C::Parent: Circuit,
    Z: BatchReader<R = ZWeight>,
    Z::Inner: Checkpoint + DynIndexedZSet + Send + Sync,
    Spine<Z::Inner>: SizeOf,
{
    fn consolidate(self) -> Stream<C::Parent, Z> {
        self.export
            .dyn_consolidate(&self.factories.input_factories)
            .typed::<Z>()
    }
}

/// Generalizes closing a recursive fixed-point loop over a group of
/// [`RecursiveVar`]s.
///
/// This is the [`RecursionBuilder`] counterpart to [`DynRecursiveStreams`]: it
/// is implemented for the shapes an `init` closure may return, namely a single
/// [`RecursiveVar`], a tuple of [`RecursiveVar`]s, or a [`Vec`] of them.
/// A single variable (or a tuple of variables) recovers the behavior of
/// [`recursive`](ChildCircuit::recursive) over one stream, whereas the vector
/// recovers [`recursive_dynamic`](ChildCircuit::recursive_dynamic) with its
/// arity inferred from the vector's length.
///
/// [`DynRecursiveStreams`]: crate::operator::dynamic::recursive::RecursiveStreams
pub trait RecursionVars<C: Circuit> {
    /// Streams handed to the step closure and returned by it.
    type Streams;

    /// Per-variable output traces exported to the parent circuit.
    type Export;

    /// Final, consolidated output streams in the parent circuit.
    type Output;

    /// The feedback streams to feed into the recursive step.
    fn streams(&self) -> Self::Streams;

    /// Close every feedback loop in the group, returning their exported traces.
    ///
    /// # Panics
    ///
    /// Panics if `next` does not contain exactly one stream per recursive
    /// variable in the group.
    fn close(self, next: Self::Streams, distinct: bool) -> Self::Export;

    /// Consolidate the exported traces into the final output streams.
    fn consolidate(exports: Self::Export) -> Self::Output;

    /// Produce a [`RecursionReport`] stream by sampling `outcome` in every
    /// step, once the nested epoch has finished.
    ///
    /// The sampler is attached to one of the recursion's *existing* export
    /// streams (never a fresh in-loop operator, which — by changing every
    /// iteration — would prevent the fixed-point check from ever succeeding),
    /// so the sample is naturally scheduled after the recursion has run in the
    /// step.  Any group with at least one variable can report.
    fn report<G>(exports: &Self::Export, outcome: G) -> Stream<C::Parent, RecursionReport>
    where
        G: Fn() -> RecursionReport + 'static;
}

impl<C, Z> RecursionVars<C> for RecursiveVar<C, Z>
where
    C: Circuit,
    C::Parent: Circuit,
    Z: BatchReader<R = ZWeight>,
    Z::Inner: Checkpoint + DynIndexedZSet + Send + Sync,
    Spine<Z::Inner>: SizeOf,
{
    type Streams = Stream<C, Z>;
    type Export = ClosedVar<C, Z>;
    type Output = Stream<C::Parent, Z>;

    fn streams(&self) -> Self::Streams {
        self.stream.clone()
    }

    fn close(self, next: Self::Streams, distinct: bool) -> Self::Export {
        RecursiveVar::close(self, next, distinct)
    }

    fn consolidate(export: Self::Export) -> Self::Output {
        ClosedVar::consolidate(export)
    }

    fn report<G>(export: &Self::Export, outcome: G) -> Stream<C::Parent, RecursionReport>
    where
        G: Fn() -> RecursionReport + 'static,
    {
        export.export.apply(move |_| outcome())
    }
}

impl<C, Z> RecursionVars<C> for Vec<RecursiveVar<C, Z>>
where
    C: Circuit,
    C::Parent: Circuit,
    Z: BatchReader<R = ZWeight>,
    Z::Inner: Checkpoint + DynIndexedZSet + Send + Sync,
    Spine<Z::Inner>: SizeOf,
{
    type Streams = Vec<Stream<C, Z>>;
    type Export = Vec<ClosedVar<C, Z>>;
    type Output = Vec<Stream<C::Parent, Z>>;

    fn streams(&self) -> Self::Streams {
        self.iter().map(|var| var.stream.clone()).collect()
    }

    fn close(self, next: Self::Streams, distinct: bool) -> Self::Export {
        assert_eq!(
            self.len(),
            next.len(),
            "the recursive step must return exactly one stream per recursive variable"
        );

        self.into_iter()
            .zip(next)
            .map(|(var, next)| var.close(next, distinct))
            .collect()
    }

    fn consolidate(exports: Self::Export) -> Self::Output {
        exports.into_iter().map(ClosedVar::consolidate).collect()
    }

    fn report<G>(exports: &Self::Export, outcome: G) -> Stream<C::Parent, RecursionReport>
    where
        G: Fn() -> RecursionReport + 'static,
    {
        // The Vec's first element creates the report; other elements are left
        // untouched, so exactly one report stream is created.
        exports
            .first()
            .expect("a recursion has at least one variable")
            .export
            .apply(move |_| outcome())
    }
}

/// A fixed-size, heterogeneous group of recursive variables.
///
/// Each element may carry a different batch type, so this recovers the
/// mutually-recursive-streams-of-different-types case handled by
/// [`recursive`](ChildCircuit::recursive) over a tuple.
#[allow(clippy::unused_unit)]
#[impl_for_tuples(1, 14)]
#[tuple_types_custom_trait_bound(RecursionVars<C>)]
impl<C: Circuit> RecursionVars<C> for Tuple {
    for_tuples!( type Streams = ( #( Tuple::Streams ),* ); );
    for_tuples!( type Export = ( #( Tuple::Export ),* ); );
    for_tuples!( type Output = ( #( Tuple::Output ),* ); );

    fn streams(&self) -> Self::Streams {
        (for_tuples!( #( self.Tuple.streams() ),* ))
    }

    fn close(self, next: Self::Streams, distinct: bool) -> Self::Export {
        (for_tuples!( #( self.Tuple.close(next.Tuple, distinct) ),* ))
    }

    fn consolidate(exports: Self::Export) -> Self::Output {
        (for_tuples!( #( Tuple::consolidate(exports.Tuple) ),* ))
    }

    fn report<G>(exports: &Self::Export, outcome: G) -> Stream<C::Parent, RecursionReport>
    where
        G: Fn() -> RecursionReport + 'static,
    {
        // Delegate to the tuple's first element; other elements are left
        // untouched, so exactly one report stream is created.
        <TupleElement0 as RecursionVars<C>>::report(&exports.0, outcome)
    }
}

/// A report on one run of a recursion, produced when reporting is enabled via
/// [`with_report`](RecursionBuilder::with_report).
///
/// The recursion runs once in every step of the parent circuit, propagating
/// the changes of that step, and the stream returned alongside the output
/// carries one report per step.  A transaction may take several steps,
/// including the steps that commit it, so the report read after a transaction
/// describes only the run in its last step.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RecursionReport {
    /// Number of iterations the run took.
    iterations: u64,

    /// Whether the run converged, as opposed to being truncated by the bound.
    converged: bool,
}

impl RecursionReport {
    /// Returns the number of iterations the run took if it converged, or
    /// `None` if the bound truncated it.
    pub fn converged_iterations(&self) -> Option<u64> {
        if self.converged {
            Some(self.iterations)
        } else {
            None
        }
    }
    /// Returns `true` if the run converged: the changes that it propagated
    /// stopped producing new ones within the bound set with
    /// [`with_bound`](RecursionBuilder::with_bound).
    ///
    /// A run propagates only the changes of its own step, so its convergence
    /// does not mean that the output is the recursion's fixed point: results
    /// that need more iterations than the bound allows are missing from the
    /// output after every run.  The output is the fixed point if no run so far
    /// has been truncated.
    pub fn converged(&self) -> bool {
        self.converged
    }
    /// Returns `true` if the bound set with
    /// [`with_bound`](RecursionBuilder::with_bound) stopped the run while the
    /// changes that it propagated were still producing new ones, so the output
    /// may lack results that further iterations would derive.
    pub fn truncated(&self) -> bool {
        !self.converged
    }
    /// Returns the number of iterations the run took.
    pub fn iterations(&self) -> u64 {
        self.iterations
    }
}

mod sealed {
    /// Private supertrait that seals [`ReportMode`](super::ReportMode) to this
    /// module: it cannot be  implemented from outside, so neither can
    /// `ReportMode`.
    pub trait Sealed {}
}

/// Sealed marker trait for a [`RecursionBuilder`]'s reporting type-state.
///
/// Implemented only by [`NoReport`] and [`Reporting`]; the sealed supertrait
/// makes it impossible to implement for any other type.  This closes the set of
/// reporting states, so a `RecursionBuilder` can never be parameterized with a
/// foreign `R` that would leave it without a
/// [`finish`](RecursionBuilder::finish) implementation.
///
/// Each state also *carries* its own reporting state: [`Reporting`] owns a
/// shared cell (allocated by [`with_report`](RecursionBuilder::with_report))
/// that the recursion records its outcome into and later samples into a stream,
/// whereas [`NoReport`] is zero-sized and does neither. Hence, a non-reporting
/// run allocates nothing, records nothing, and builds no stream.  The `Clone`
/// and `'static` supertrait bounds let a run clone this state into the
/// termination closure that the scheduler holds across nested clock cycles.
pub trait ReportMode: sealed::Sealed + Clone + 'static {
    /// Record the recursion's final outcome once it stops.  A no-op under
    /// [`NoReport`], so a non-reporting run performs no writes.
    fn record(&self, outcome: RecursionReport);

    /// Build the reporting stream from the exported traces, before the exports
    /// are consolidated.  Returns `None` under [`NoReport`] and `Some(stream)`
    /// under [`Reporting`], where it samples the recorded outcome once per
    /// step.
    fn build_report<C, V>(&self, exports: &V::Export) -> Option<Stream<C, RecursionReport>>
    where
        C: Circuit,
        V: RecursionVars<IterativeCircuit<C>>;
}

/// Type-state marker for a [`RecursionBuilder`] that does not report its
/// [`RecursionReport`]. [`finish`](RecursionBuilder::finish) returns the output
/// streams only.
///
/// Zero-sized: it carries no reporting state.  Its private field keeps it
/// non-constructible outside this module.
#[derive(Clone)]
pub struct NoReport(PhantomData<()>);

/// Type-state marker for a [`RecursionBuilder`] with reporting enabled.
/// [`finish`](RecursionBuilder::finish) additionally returns a
/// [`RecursionReport`] stream.  Enable through
/// [`with_report`](RecursionBuilder::with_report).
///
/// Carries the shared cell the recursion records its outcome into.  Its private
/// field also keeps it non-constructible outside this module.
#[derive(Clone)]
pub struct Reporting {
    /// Cell the termination check writes the final outcome into and the report
    /// stream samples after the nested epoch.  Allocated in
    /// [`with_report`](RecursionBuilder::with_report).
    recorder: Rc<Cell<RecursionReport>>,
}

impl Reporting {
    fn new() -> Self {
        Self {
            recorder: Rc::new(Cell::new(RecursionReport::default())),
        }
    }
}

impl sealed::Sealed for NoReport {}
impl sealed::Sealed for Reporting {}

impl ReportMode for NoReport {
    fn record(&self, _report: RecursionReport) {}

    fn build_report<C, V>(&self, _exports: &V::Export) -> Option<Stream<C, RecursionReport>>
    where
        C: Circuit,
        V: RecursionVars<IterativeCircuit<C>>,
    {
        None
    }
}

impl ReportMode for Reporting {
    fn record(&self, report: RecursionReport) {
        self.recorder.set(report);
    }

    fn build_report<C, V>(&self, exports: &V::Export) -> Option<Stream<C, RecursionReport>>
    where
        C: Circuit,
        V: RecursionVars<IterativeCircuit<C>>,
    {
        let recorder = self.recorder.clone();
        Some(V::report(exports, move || recorder.get()))
    }
}

/// A unified builder for recursive computations, returned by
/// [`recursion_builder`](ChildCircuit::recursion_builder).
///
/// [`RecursionBuilder`] subsumes both [`recursive`](ChildCircuit::recursive)
/// and [`recursive_dynamic`](ChildCircuit::recursive_dynamic) behind a single
/// entry point built from two closures:
///
/// 1. An **init** closure sets up the recursive variables by calling
///    [`recursive_var`](ChildCircuit::recursive_var) once per mutually
///    recursive relation.  Because the variables are *returned* rather than
///    counted, the arity of the recursion is inferred from the shape of the
///    return value; unlike `recursive_dynamic`, no explicit `arity` is needed.
/// 2. A **step** closure receives the recursive variables' feedback streams and
///    returns the next iteration.
///
/// The builder computes a fixed point of the step closure, applying an implicit
/// `distinct` to each recursive stream (disable with
/// [`without_distinct`](RecursionBuilder::without_distinct)).  Just like the
/// closures passed to `recursive`, the step closure imports base-case relations
/// from the parent circuit with [`delta0`](crate::circuit::Stream::delta0),
/// which injects them once at the first iteration.
///
/// [`with_report`](RecursionBuilder::with_report) opts into
/// [`RecursionReport`]s: `finish` then also returns a stream that reports, for
/// the recursion's run in each step, how many iterations it took and whether
/// the bound set with [`with_bound`](RecursionBuilder::with_bound) truncated
/// it.
///
/// # Persistent ids
///
/// In [persistent mode](crate::circuit::Mode::Persistent) every operator that
/// holds state must carry a persistent id, and operators inside the recursive
/// scope are no exception: without one, taking a checkpoint fails with
/// `NoPersistentId`.  The step closure is responsible for assigning them, by
/// calling [`set_persistent_id`](crate::circuit::Stream::set_persistent_id) on
/// * each feedback stream it receives, from which the operators built on it
///   derive their own ids, and
/// * every stream it creates, including the ones it returns, from which the
///   implicit `distinct`, if any, and the integral that exports the result
///   derive their own ids.
///
/// Name each feedback stream before the step closure builds anything from it:
/// the operators that maintain state over a stream derive their own ids from it
/// as they are constructed, so a name assigned later leaves them unnamed.  In
/// practice, call `set_persistent_id` on the feedback streams first thing in
/// the step closure.
///
/// The names must identify the same computation across restarts, so derive
/// them from the program (a view name, a hash of the subgraph) rather than from
/// anything positional.
///
/// The bound set with [`with_bound`](RecursionBuilder::with_bound) is part of
/// the computation, so a restart that changes it must also change persistent
/// ids: those of the recursion's streams, or those of the stateful operators
/// that consume its output, such as the output itself.  Either way, the
/// restart rebuilds the recursion from its inputs.  A restart that keeps the
/// ids resumes the recursion from state computed under the old bound, and its
/// results are wrong.
///
/// # Circuit
///
/// ```text
///      ┌───────────────────────────────────────────────────────────────┐
///      │                                                               │
///   i  │               ┌───┐                                           │
///  ────┼──►δ0─────────►│   │      ┌ ─ ─ ─ ─┐       ┌───────────────┐   │   ┌───────────┐
///      │               │ f ├─────►│distinct├──┬───►│integrate_trace├───┼──►│consolidate├───────►
///      │       ┌──────►│   │      └ ─ ─ ─ ─┘  │    └───────────────┘   │   └───────────┘
///      │       │       └───┘                  │                        │
///      │  ┌──────────────────┐                │                        │
///      │  │concat_accumulated│                │                        │
///      │  └──────────────────┘                │                        │
///      │       ▲       ┌──────────────┐       │                        │
///      │       └───────┤IterationDelay│◄──────┘                        │
///      │               └──────────────┘                                │
///      │                                                               │
///      └───────────────────────────────────────────────────────────────┘
/// ```
/// where
/// * `integrate_trace` integrates outputs computed across multiple fixed point
///   iterations.
/// * `consolidate` consolidates the output of the nested circuit into a single
///   batch.
/// * `distinct` is not inserted if the builder is configured with `without_distinct`.
///
/// # Examples
///
/// A single recursive relation (transitive closure), matching the shape handled
/// by [`recursive`](ChildCircuit::recursive):
/// ```
/// use dbsp::{
///     operator::Generator,
///     Circuit, RootCircuit, OrdZSet, zset,
///     utils::Tup2, Error as DbspError, Runtime,
/// };
///
/// type Edge = Tup2<u64, u64>;
///
/// let (mut circuit, _) = Runtime::init_circuit(1, |root_circuit| {
///     let mut edges = [zset! { Tup2(1u64, 2u64) => 1, Tup2(2, 3) => 1 }].into_iter();
///     let edges = root_circuit.add_source(Generator::new(move || edges.next().unwrap()));
///
///     // The `recursion_builder` call defines the computation; the closures'
///     // types are inferred, so no circuit or stream annotations are needed.
///     let reachable = root_circuit
///         .recursion_builder(
///             |child| Ok(child.recursive_var::<OrdZSet<Edge>>()),
///             |child, reachable| {
///                 // Name the feedback stream and every stream built from it,
///                 // so that the operators inside the scope can be checkpointed.
///                 reachable.set_persistent_id(Some("reachable"));
///
///                 let edges = edges.delta0(child);
///                 let edges_indexed = edges
///                     .map_index(|Tup2(x, y)| (*x, *y))
///                     .set_persistent_id(Some("edges_indexed"));
///                 let reachable_indexed = reachable
///                     .map_index(|&Tup2(x, y)| (y, x))
///                     .set_persistent_id(Some("reachable_indexed"));
///
///                 let reachable_next = edges.plus(
///                     &reachable_indexed.join(&edges_indexed, |_via, from, to| Tup2(*from, *to)),
///                 );
///                 reachable_next.set_persistent_id(Some("reachable_next"));
///                 Ok(reachable_next)
///             },
///         )
///         .finish()?;
///
///     Ok(reachable.output())
/// })?;
///
/// circuit.transaction().unwrap();
/// Ok::<(), DbspError>(())
/// ```
#[must_use = "a `RecursionBuilder` builds nothing until `finish` is called"]
pub struct RecursionBuilder<'a, C, F1, F2, R: ReportMode = NoReport> {
    circuit: &'a C,
    init: F1,
    step: F2,
    distinct: bool,
    bounded: Option<NonZeroU64>,
    report: R,
}

impl<'a, C, F1, F2, R> RecursionBuilder<'a, C, F1, F2, R>
where
    C: Circuit,
    R: ReportMode,
{
    /// Do not apply an implicit `distinct` to the recursive streams.
    ///
    /// Most recursive computations over Z-sets require `distinct` to converge;
    /// disable it only when the step function already guarantees that the
    /// output weights stabilize.
    pub fn without_distinct(mut self) -> Self {
        self.distinct = false;
        self
    }

    /// Stop the recursion after at most `max_iterations` fixed-point
    /// iterations, even if no fixed point has been reached.
    ///
    /// By default the recursion runs until it converges (see
    /// [`finish`](Self::finish)).  With a bound, iteration also stops once
    /// `max_iterations` nested clock cycles have elapsed, whichever comes
    /// first.  Each iteration applies the step closure to exactly the changes
    /// that the previous iteration produced, so a transitive closure bounded to
    /// `n` iterations, for example, holds exactly the paths of at most `n`
    /// edges.  This is useful to cap the cost of computations that converge
    /// slowly, or as a safety valve against non-converging steps.  Combine with
    /// [`with_report`](RecursionBuilder::with_report) to learn whether the bound
    /// truncated a run of the recursion.
    pub fn with_bound<T: Into<NonZeroU64>>(mut self, max_iterations: T) -> Self {
        self.bounded = Some(max_iterations.into());
        self
    }

    /// Build the recursion, returning the consolidated output streams together
    /// with an optional stream of [`RecursionReport`]s.
    ///
    /// Both the bounded and unbounded variants are driven through
    /// [`Circuit::iterate`], differing only in the termination check.  The
    /// check reproduces [`Circuit::fixedpoint`]'s condition — every operator is
    /// stable and all workers agree via [`Consensus`] — and additionally stops
    /// once the optional iteration bound is reached.  Because `converged` is a
    /// consensus value and the iteration counter advances in lockstep, every
    /// worker computes the same termination decision on the same iteration,
    /// which is required to avoid a deadlock.
    fn run<V>(self) -> Result<(V::Output, Option<Stream<C, RecursionReport>>), SchedulerError>
    where
        V: RecursionVars<IterativeCircuit<C>>,
        F1: FnOnce(&IterativeCircuit<C>) -> Result<V, SchedulerError>,
        F2: FnOnce(&IterativeCircuit<C>, V::Streams) -> Result<V::Streams, SchedulerError>,
    {
        let RecursionBuilder {
            circuit,
            init,
            step,
            distinct,
            bounded,
            report,
        } = self;

        // `report` carries the recorder chosen by the type-state: nothing under
        // `NoReport`, a shared `Rc<Cell<..>>` allocated by `with_report` under
        // `Reporting`. A clone goes into the termination closure to record the
        // outcome; the original samples that outcome into the report stream
        // after the epoch. So a non-reporting run allocates nothing, and its
        // `record`/`build_report` are no-ops.
        let terminate_report = report.clone();

        let exports = circuit.iterate(|child| {
            let vars = init(child)?;
            let streams = vars.streams();
            let next = step(child, streams)?;
            let exports = vars.close(next, distinct);

            let child = child.clone();
            let consensus = Consensus::new("recursion fixed point");
            // Counts iterations within the current nested epoch.  It persists
            // across transactions (the closure is built once), so it must be
            // reset when the epoch ends — otherwise the bound would be spent by
            // the first transaction and later ones would stop immediately.
            let iteration = Cell::new(0u64);

            let terminate = async move || {
                let count = iteration.get() + 1;
                iteration.set(count);

                let converged = consensus.check(child.check_fixedpoint(0)).await?;
                let stop = converged || bounded.is_some_and(|max| count >= u64::from(max));

                if stop {
                    terminate_report.record(RecursionReport {
                        iterations: count,
                        converged,
                    });
                    // Start the next transaction's epoch from zero. Explicitly
                    // tested in `with_bound_counter_resets_across_transactions`
                    // below.
                    iteration.set(0);
                }

                Ok(stop)
            };

            Ok((terminate, exports))
        })?;

        let report_stream = report.build_report::<C, V>(&exports);
        let output = V::consolidate(exports);

        Ok((output, report_stream))
    }
}

impl<'a, C, F1, F2> RecursionBuilder<'a, C, F1, F2, NoReport>
where
    C: Circuit,
{
    /// Emit a [`RecursionReport`] for every run of the recursion alongside its
    /// output.
    ///
    /// After calling this, [`finish`](Self::finish) returns a tuple whose second
    /// element is a stream carrying one [`RecursionReport`] per step.
    pub fn with_report(self) -> RecursionBuilder<'a, C, F1, F2, Reporting> {
        RecursionBuilder {
            circuit: self.circuit,
            init: self.init,
            step: self.step,
            distinct: self.distinct,
            bounded: self.bounded,
            report: Reporting::new(),
        }
    }

    /// Build the recursive computation and return the consolidated output
    /// streams exported to the parent circuit.
    ///
    /// The recursion iterates to a fixed point, or until the bound set by
    /// [`with_bound`](Self::with_bound) is reached, whichever comes first.  The
    /// concrete return type mirrors the shape produced by `init`: a single
    /// stream yields a single output stream, a vector yields a vector of output
    /// streams.
    #[track_caller]
    pub fn finish<V>(self) -> Result<V::Output, SchedulerError>
    where
        V: RecursionVars<IterativeCircuit<C>>,
        F1: FnOnce(&IterativeCircuit<C>) -> Result<V, SchedulerError>,
        F2: FnOnce(&IterativeCircuit<C>, V::Streams) -> Result<V::Streams, SchedulerError>,
    {
        Ok(self.run::<V>()?.0)
    }
}

impl<'a, C, F1, F2> RecursionBuilder<'a, C, F1, F2, Reporting>
where
    C: Circuit,
{
    /// Build the recursive computation, returning its output streams together
    /// with a stream of [`RecursionReport`]s.
    ///
    /// Like [`finish`](RecursionBuilder::finish) on the non-reporting builder,
    /// but the returned tuple's second element is a `Stream` that carries one
    /// [`RecursionReport`] per step: the number of iterations the recursion's
    /// run took and whether it converged.
    #[track_caller]
    pub fn finish<V>(self) -> Result<(V::Output, Stream<C, RecursionReport>), SchedulerError>
    where
        V: RecursionVars<IterativeCircuit<C>>,
        F1: FnOnce(&IterativeCircuit<C>) -> Result<V, SchedulerError>,
        F2: FnOnce(&IterativeCircuit<C>, V::Streams) -> Result<V::Streams, SchedulerError>,
    {
        let (output, report) = self.run::<V>()?;

        Ok((
            output,
            report.expect("reporting builds always produce an outcome stream"),
        ))
    }
}

#[cfg(test)]
mod test {
    use std::{
        net::TcpListener,
        num::NonZeroU64,
        thread,
        time::{Duration, Instant},
    };

    use crate::operator::dynamic::recursive::test::{
        reachability::{Edge, checkpoint_and_restart, edges_data, expected_reachable},
        recursion_test_config,
    };
    use crate::{
        Circuit, RootCircuit, Runtime, Stream,
        algebra::AddByRef,
        circuit::{CircuitConfig, Layout},
        operator::Generator,
        typed_batch::OrdZSet,
        utils::Tup2,
        zset,
    };

    /// Transitive closure via [`RecursionBuilder`] over a *single* recursive
    /// variable, checkpointed and restarted halfway through.  Must reproduce
    /// the output of the single-`Stream`
    /// [`recursive`](crate::ChildCircuit::recursive) implementation.
    #[test]
    fn reachability_builder() {
        checkpoint_and_restart(|circuit, skip| {
            let mut edges = edges_data().into_iter().skip(skip);
            let edges = circuit.add_source(Generator::new(move || edges.next().unwrap()));

            let reachable = circuit
                .recursion_builder(
                    // The number of `recursive_var` calls fixes the arity; here
                    // it is a single stream, so no arity has to be supplied.
                    |child| Ok(child.recursive_var::<OrdZSet<Edge>>()),
                    |child, reachable| {
                        // Checkpointing a recursive scope requires its operators
                        // to be named; see `RecursionBuilder`.
                        reachable.set_persistent_id(Some("reachable"));

                        let edges = edges.delta0(child);
                        let edges_indexed = edges
                            .map_index(|Tup2(x, y)| (*x, *y))
                            .set_persistent_id(Some("edges_indexed"));
                        let reachable_indexed = reachable
                            .map_index(|&Tup2(x, y)| (y, x))
                            .set_persistent_id(Some("reachable_indexed"));

                        let reachable_next = edges.plus(
                            &reachable_indexed
                                .join(&edges_indexed, |_via, from, to| Tup2(*from, *to)),
                        );
                        reachable_next.set_persistent_id(Some("reachable_next"));

                        Ok(reachable_next)
                    },
                )
                .finish()
                .unwrap();

            vec![reachable.accumulate_output_persistent(Some("reachable_out"))]
        });
    }

    /// Forward and backward reachability via [`RecursionBuilder`] over a *vector*
    /// of two recursive variables, checkpointed and restarted halfway through.
    /// Unlike [`recursive_dynamic`](crate::ChildCircuit::recursive_dynamic), the
    /// arity (2) is inferred from the vector returned by the init closure.  Must
    /// match the tuple/dynamic implementations.
    #[test]
    fn reachability2_builder() {
        checkpoint_and_restart(|circuit, skip| {
            let mut edges = edges_data().into_iter().skip(skip);
            let edges = circuit.add_source(Generator::new(move || edges.next().unwrap()));

            let mut reachable = circuit
                .recursion_builder(
                    |child| {
                        Ok(vec![
                            child.recursive_var::<OrdZSet<Edge>>(),
                            child.recursive_var::<OrdZSet<Edge>>(),
                        ])
                    },
                    |child, streams| {
                        let reachable = &streams[0];
                        let reachable_reverse = &streams[1];
                        reachable.set_persistent_id(Some("reachable"));
                        reachable_reverse.set_persistent_id(Some("reachable_reverse"));

                        let edges = edges.delta0(child);

                        let edges_indexed = edges
                            .map_index(|Tup2(x, y)| (*x, *y))
                            .set_persistent_id(Some("edges_indexed"));
                        let reachable_indexed = reachable
                            .map_index(|&Tup2(x, y)| (y, x))
                            .set_persistent_id(Some("reachable_indexed"));
                        let reachable_reverse_indexed = reachable_reverse
                            .map_index(|&Tup2(x, y)| (y, x))
                            .set_persistent_id(Some("reachable_reverse_indexed"));
                        let reverse_edges = edges
                            .map(|&Tup2(x, y)| Tup2(y, x))
                            .set_persistent_id(Some("reverse_edges"));
                        let reverse_edges_indexed = reverse_edges
                            .map_index(|Tup2(x, y)| (*x, *y))
                            .set_persistent_id(Some("reverse_edges_indexed"));

                        let reachable_next = edges.plus(
                            &reachable_indexed
                                .join(&edges_indexed, |_via, from, to| Tup2(*from, *to)),
                        );
                        reachable_next.set_persistent_id(Some("reachable_next"));
                        let reachable_reverse_next = reverse_edges.plus(
                            &reachable_reverse_indexed
                                .join(&reverse_edges_indexed, |_via, from, to| Tup2(*from, *to)),
                        );
                        reachable_reverse_next.set_persistent_id(Some("reachable_reverse_next"));

                        Ok(vec![reachable_next, reachable_reverse_next])
                    },
                )
                .finish()
                .unwrap();

            let reachable_reverse = reachable.pop().unwrap();
            let reachable = reachable.pop().unwrap();

            let reachable_reverse = reachable_reverse.map(|Tup2(x, y)| Tup2(*y, *x));

            vec![
                reachable.accumulate_output_persistent(Some("reachable_out")),
                reachable_reverse.accumulate_output_persistent(Some("reachable_reverse_out")),
            ]
        });
    }

    /// The same forward/backward reachability as [`reachability2_builder`], but
    /// with the two recursive variables supplied as a *tuple* instead of a
    /// `Vec`.  This exercises the tuple [`RecursionVars`](super::RecursionVars)
    /// implementation and must produce identical output, and the recursion must
    /// report convergence in every transaction, before and after the restart.
    #[test]
    fn reachability2_builder_tuple() {
        checkpoint_and_restart(|circuit, skip| {
            let mut edges = edges_data().into_iter().skip(skip);
            let edges = circuit.add_source(Generator::new(move || edges.next().unwrap()));

            let ((reachable, reachable_reverse), report) = circuit
                .recursion_builder(
                    // Two recursive variables of the same type, returned as a
                    // tuple; the arity (2) is fixed by the tuple's shape.
                    |child| {
                        Ok((
                            child.recursive_var::<OrdZSet<Edge>>(),
                            child.recursive_var::<OrdZSet<Edge>>(),
                        ))
                    },
                    |child, (reachable, reachable_reverse)| {
                        reachable.set_persistent_id(Some("reachable"));
                        reachable_reverse.set_persistent_id(Some("reachable_reverse"));

                        let edges = edges.delta0(child);

                        let edges_indexed = edges
                            .map_index(|Tup2(x, y)| (*x, *y))
                            .set_persistent_id(Some("edges_indexed"));
                        let reachable_indexed = reachable
                            .map_index(|&Tup2(x, y)| (y, x))
                            .set_persistent_id(Some("reachable_indexed"));
                        let reachable_reverse_indexed = reachable_reverse
                            .map_index(|&Tup2(x, y)| (y, x))
                            .set_persistent_id(Some("reachable_reverse_indexed"));
                        let reverse_edges = edges
                            .map(|&Tup2(x, y)| Tup2(y, x))
                            .set_persistent_id(Some("reverse_edges"));
                        let reverse_edges_indexed = reverse_edges
                            .map_index(|Tup2(x, y)| (*x, *y))
                            .set_persistent_id(Some("reverse_edges_indexed"));

                        let reachable_next = edges.plus(
                            &reachable_indexed
                                .join(&edges_indexed, |_via, from, to| Tup2(*from, *to)),
                        );
                        reachable_next.set_persistent_id(Some("reachable_next"));
                        let reachable_reverse_next = reverse_edges.plus(
                            &reachable_reverse_indexed
                                .join(&reverse_edges_indexed, |_via, from, to| Tup2(*from, *to)),
                        );
                        reachable_reverse_next.set_persistent_id(Some("reachable_reverse_next"));

                        Ok((reachable_next, reachable_reverse_next))
                    },
                )
                .with_report()
                .finish()
                .unwrap();

            report.inspect(|report| {
                assert!(report.converged());
            });

            let reachable_reverse = reachable_reverse.map(|Tup2(x, y)| Tup2(*y, *x));

            vec![
                reachable.accumulate_output_persistent(Some("reachable_out")),
                reachable_reverse.accumulate_output_persistent(Some("reachable_reverse_out")),
            ]
        });
    }

    /// A bound larger than the number of iterations needed to converge must not
    /// change the result: it reproduces [`reachability_builder`] exactly.
    #[test]
    fn with_large_bound_is_noop() {
        let edges_data = edges_data();
        let steps = edges_data.len();
        let mut edges = edges_data.into_iter();
        let mut expected_reachable = expected_reachable().into_iter();

        let config = recursion_test_config(1);
        let (mut handle, _) = Runtime::init_circuit(config, move |circuit| {
            let edges = circuit.add_source(Generator::new(move || edges.next().unwrap()));

            let reachable = circuit
                .recursion_builder(
                    |child| Ok(child.recursive_var::<OrdZSet<Edge>>()),
                    |child, reachable| {
                        let edges = edges.delta0(child);
                        let edges_indexed = edges.map_index(|Tup2(x, y)| (*x, *y));
                        let reachable_indexed = reachable.map_index(|&Tup2(x, y)| (y, x));

                        Ok(edges.plus(
                            &reachable_indexed
                                .join(&edges_indexed, |_via, from, to| Tup2(*from, *to)),
                        ))
                    },
                )
                .with_bound(NonZeroU64::new(1_000_000).unwrap())
                .finish()
                .unwrap();

            reachable
                .integrate()
                .stream_distinct()
                .inspect(move |reachable| {
                    assert_eq!(*reachable, expected_reachable.next().unwrap());
                });

            Ok(())
        })
        .unwrap();

        for _ in 0..steps {
            handle.transaction().unwrap();
        }
    }

    /// A recursion that provably never reaches a fixed point: every iteration
    /// shifts the single element up by one, so the recursive stream keeps
    /// changing.  Without a bound this would iterate forever; `with_bound`
    /// caps it at a fixed number of iterations.  The test completing at all
    /// proves the bound is enforced, and the union of the per-iteration values
    /// gives a deterministic result.
    #[test]
    fn with_bound_caps_non_converging() {
        const BOUND: NonZeroU64 = NonZeroU64::new(3).unwrap();

        let config = recursion_test_config(1);
        let (mut handle, output) = Runtime::init_circuit(config, move |circuit| {
            let mut seed = [zset! { 0usize => 1 }].into_iter();
            let seed = circuit.add_source(Generator::new(move || seed.next().unwrap_or_default()));

            let result = circuit
                .recursion_builder(
                    |child| Ok(child.recursive_var::<OrdZSet<usize>>()),
                    move |child, x| {
                        // `seed` is injected once (at iteration 0) via delta0;
                        // thereafter each iteration re-emits the previous value
                        // shifted up by one, so the stream never stabilizes.
                        let seed = seed.delta0(child);
                        Ok(seed.plus(&x.map(|v| *v + 1)))
                    },
                )
                .with_bound(BOUND)
                .finish()
                .unwrap();

            Ok(result.output())
        })
        .unwrap();

        handle.transaction().unwrap();

        // Three iterations emit 0, 1, 2 respectively.
        assert_eq!(
            output.consolidate(),
            zset! { 0usize => 1, 1usize => 1, 2usize => 1 },
        );
    }

    /// With reporting enabled, a bounded run over the non-converging recursion
    /// reports that it was truncated: `converged == false` and `iterations`
    /// equal to the bound.
    #[test]
    fn with_report_signals_truncation() {
        const BOUND: NonZeroU64 = NonZeroU64::new(3).unwrap();

        let config = recursion_test_config(1);
        let (mut handle, outcome) = Runtime::init_circuit(config, move |circuit| {
            let mut seed = [zset! { 0usize => 1 }].into_iter();
            let seed = circuit.add_source(Generator::new(move || seed.next().unwrap_or_default()));

            let (result, outcome) = circuit
                .recursion_builder(
                    |child| Ok(child.recursive_var::<OrdZSet<usize>>()),
                    move |child, x| {
                        let seed = seed.delta0(child);
                        Ok(seed.plus(&x.map(|v| *v + 1)))
                    },
                )
                .with_bound(BOUND)
                .with_report()
                .finish()
                .unwrap();

            // The output stream is unaffected by reporting; keep it alive.
            result.output();

            Ok(outcome.output())
        })
        .unwrap();

        handle.transaction().unwrap();

        let outcome = outcome.take_from_all();
        let outcome = outcome.first().expect("one worker, one outcome");
        assert!(
            outcome.truncated(),
            "a bounded non-converging recursion must report truncation"
        );
        assert!(!outcome.converged());
        assert_eq!(outcome.converged_iterations(), None);
        assert_eq!(outcome.iterations(), BOUND.get());
    }

    /// With reporting enabled, a converging recursion reports convergence and an
    /// iteration count strictly below the (generous) bound, for every
    /// transaction.
    #[test]
    fn with_report_signals_convergence() {
        let edges_data = edges_data();
        let steps = edges_data.len();
        let mut edges = edges_data.into_iter();

        let config = recursion_test_config(1);
        let (mut handle, outcome) = Runtime::init_circuit(config, move |circuit| {
            let edges = circuit.add_source(Generator::new(move || edges.next().unwrap()));

            let (reachable, outcome) = circuit
                .recursion_builder(
                    |child| Ok(child.recursive_var::<OrdZSet<Edge>>()),
                    |child, reachable| {
                        let edges = edges.delta0(child);
                        let edges_indexed = edges.map_index(|Tup2(x, y)| (*x, *y));
                        let reachable_indexed = reachable.map_index(|&Tup2(x, y)| (y, x));

                        Ok(edges.plus(
                            &reachable_indexed
                                .join(&edges_indexed, |_via, from, to| Tup2(*from, *to)),
                        ))
                    },
                )
                .with_bound(NonZeroU64::new(1_000).unwrap())
                .with_report()
                .finish()
                .unwrap();

            reachable.output();

            Ok(outcome.output())
        })
        .unwrap();

        for _ in 0..steps {
            handle.transaction().unwrap();

            let outcome = outcome.take_from_all();
            let outcome = outcome.first().expect("one worker, one outcome");
            let iterations = outcome
                .converged_iterations()
                .expect("reachability must converge within the bound");
            assert!(outcome.converged());
            assert!(!outcome.truncated());
            assert_eq!(outcome.iterations(), iterations);
            assert!(
                (1..1_000).contains(&iterations),
                "unexpected iteration count: {iterations}"
            );
        }
    }

    /// Regression test for the per-epoch iteration counter: it must reset when
    /// each transaction's nested epoch ends.  A fresh seed on every transaction
    /// keeps the recursion non-converging, so every transaction runs exactly to
    /// the bound.  If the counter leaked across transactions, the bound would be
    /// exhausted after the first one and later transactions would stop after a
    /// single iteration (reporting a growing `iterations` instead of `BOUND`).
    #[test]
    fn with_bound_counter_resets_across_transactions() {
        const BOUND: NonZeroU64 = NonZeroU64::new(3).unwrap();
        const TRANSACTIONS: usize = 4;

        let config = recursion_test_config(1);
        let (mut handle, (result, report)) = Runtime::init_circuit(config, move |circuit| {
            // A distinct seed element per transaction, so the shift recursion
            // always has work to do and never converges within the bound.
            let mut n = 0usize;
            let seed = circuit.add_source(Generator::new(move || {
                let batch = zset! { n => 1 };
                n += 1;
                batch
            }));

            let (result, report) = circuit
                .recursion_builder(
                    |child| Ok(child.recursive_var::<OrdZSet<usize>>()),
                    move |child, x| {
                        let seed = seed.delta0(child);
                        Ok(seed.plus(&x.map(|v| *v + 1)))
                    },
                )
                .with_bound(BOUND)
                .without_distinct()
                .with_report()
                .finish()
                .unwrap();

            Ok((result.accumulate_output(), report.output()))
        })
        .unwrap();

        for transaction in 0..TRANSACTIONS {
            handle.transaction().unwrap();

            let report = report.take_from_all();
            let report = report.first().expect("one worker, one outcome");
            assert!(
                report.truncated(),
                "transaction {transaction}: non-converging recursion must report truncation",
            );
            assert_eq!(
                report.iterations(),
                BOUND.get(),
                "transaction {transaction}: counter must reset to report per-transaction iterations",
            );
            let result = result.concat().consolidate();
            assert_eq!(
                result,
                OrdZSet::from_keys(
                    (),
                    (transaction..(transaction + BOUND.get() as usize))
                        .map(|i| {
                            let zweight = 1;
                            Tup2(i, zweight)
                        })
                        .collect::<Vec<_>>()
                ),
                "transaction {transaction}: invalid computation result",
            );
        }
    }

    /// Transitive closure of `edges`, built with the recursion builder.
    ///
    /// # Arguments
    ///
    /// * `circuit` - circuit to build the recursion in.
    /// * `edges` - edges of the graph.
    /// * `distinct` - whether to keep the builder's implicit `distinct`.
    /// * `bound` - optional cap on the number of iterations per transaction.
    ///
    /// # Returns
    ///
    /// The stream of changes to the closure.
    fn transitive_closure(
        circuit: &RootCircuit,
        edges: &Stream<RootCircuit, OrdZSet<Tup2<u64, u64>>>,
        distinct: bool,
        bound: Option<NonZeroU64>,
    ) -> Stream<RootCircuit, OrdZSet<Tup2<u64, u64>>> {
        let builder = circuit.recursion_builder(
            |child| Ok(child.recursive_var::<OrdZSet<Tup2<u64, u64>>>()),
            |child, paths| {
                let edges = edges.delta0(child);

                let paths_indexed = paths.map_index(|&Tup2(x, y)| (y, x));
                let edges_indexed = edges.map_index(|Tup2(x, y)| (*x, *y));

                let longer_paths =
                    paths_indexed.join(&edges_indexed, |_via, from, to| Tup2(*from, *to));
                Ok(edges.plus(&longer_paths))
            },
        );
        let builder = if distinct {
            builder
        } else {
            builder.without_distinct()
        };
        let builder = match bound {
            Some(bound) => builder.with_bound(bound),
            None => builder,
        };
        builder.finish().unwrap()
    }

    /// Two independent recursions on eight workers must not deadlock.
    ///
    /// A worker that blocks in one recursion's termination check, while a peer
    /// evaluates the other recursion, waits forever; see
    /// <https://github.com/feldera/feldera/issues/4168>, fixed for
    /// [`recursive`](crate::ChildCircuit::recursive) by making the check
    /// asynchronous.  The builder has a termination check of its own.  One of
    /// the two recursions is bounded, so its workers must also stop together
    /// when the bound cuts the recursion short.
    #[test]
    fn issue4168() {
        let config = recursion_test_config(8);
        let (mut circuit, edges_handle) = Runtime::init_circuit(config, move |circuit| {
            let (edges, edges_handle) = circuit.add_input_zset::<Tup2<u64, u64>>();

            transitive_closure(circuit, &edges, true, None);
            transitive_closure(circuit, &edges, true, NonZeroU64::new(5));

            Ok(edges_handle)
        })
        .unwrap();

        let handle = thread::spawn(move || {
            for i in 0..100 {
                edges_handle.append(&mut vec![Tup2(Tup2(i, i + 1), 1)]);
                circuit.transaction().unwrap();
            }
        });

        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(200) {
            if handle.is_finished() {
                handle.join().unwrap();
                return;
            }
            thread::sleep(Duration::from_millis(100));
        }

        panic!("Deadlock in test 'issue4168'");
    }

    /// Inserting a chain of edges and then deleting it must leave the
    /// transitive closure empty, whether the recursion keeps its `distinct` or
    /// not, and whether or not a bound cuts it short; see
    /// <https://github.com/feldera/feldera/issues/4028>.
    #[test]
    fn issue4028() {
        let insert_edges = (0..100)
            .map(|i| Tup2(Tup2(i, i + 1), 1))
            .collect::<Vec<_>>();
        let delete_edges = (0..100)
            .map(|i| Tup2(Tup2(i, i + 1), -1))
            .collect::<Vec<_>>();

        for distinct in [true, false] {
            for bound in [None, NonZeroU64::new(5)] {
                let config = recursion_test_config(1);
                let (mut root, (edges_handle, paths_handle)) =
                    Runtime::init_circuit(config, move |circuit| {
                        let (edges, edges_handle) = circuit.add_input_zset::<Tup2<u64, u64>>();
                        let paths_handle = transitive_closure(circuit, &edges, distinct, bound)
                            .integrate()
                            .output();

                        Ok((edges_handle, paths_handle))
                    })
                    .unwrap();

                for _ in 0..10 {
                    edges_handle.append(&mut insert_edges.clone());
                    root.transaction().unwrap();

                    edges_handle.append(&mut delete_edges.clone());
                    root.transaction().unwrap();

                    let paths = paths_handle.consolidate();
                    assert!(
                        paths.is_empty(),
                        "distinct: {distinct}, bound: {bound:?}: paths left behind: {paths:?}"
                    );
                }
            }
        }
    }

    /// Bound on the recursions in the tests of bounded recursions whose
    /// operators split their outputs.
    const SPLIT_OUTPUTS_BOUND: u64 = 3;

    /// Paths of at most `max_edges` edges that end in node `i + 1` of the
    /// chain `0 -> 1 -> ... -> i + 1`.
    ///
    /// # Arguments
    ///
    /// * `i` - the source node of the chain's last edge.
    /// * `max_edges` - the maximum number of edges in a path.
    ///
    /// # Returns
    ///
    /// The paths, as `(from, to)` pairs.
    fn chain_paths_to(i: u64, max_edges: u64) -> impl Iterator<Item = Tup2<u64, u64>> {
        ((i + 1).saturating_sub(max_edges)..=i).map(move |from| Tup2(from, i + 1))
    }

    /// Changes that a transitive closure bounded to [`SPLIT_OUTPUTS_BOUND`]
    /// iterations makes when edge `i -> i + 1` extends the chain `0 -> ... -> i`.
    ///
    /// # Arguments
    ///
    /// * `i` - the source node of the new edge.
    ///
    /// # Returns
    ///
    /// The paths of at most [`SPLIT_OUTPUTS_BOUND`] edges that end in node
    /// `i + 1`.
    fn bounded_paths_to(i: u64) -> OrdZSet<Tup2<u64, u64>> {
        OrdZSet::from_keys(
            (),
            chain_paths_to(i, SPLIT_OUTPUTS_BOUND)
                .map(|path| Tup2(path, 1))
                .collect(),
        )
    }

    /// A bounded recursion stops at its bound in every transaction, even when
    /// its operators split their outputs into chunks.
    ///
    /// Operators that split their outputs spread an iteration over many steps.
    /// Each iteration must still see exactly the previous iteration's output:
    /// none of its own, and none of what the last iteration of an earlier
    /// transaction produced.  A chunk size of one splits every output.  One
    /// worker exchanges nothing; several workers exchange the chunks.
    #[test]
    fn bound_holds_with_split_outputs() {
        for workers in [1, 2, 4] {
            for distinct in [true, false] {
                let config = CircuitConfig::from(workers)
                    .with_streaming_exchange(true)
                    .with_splitter_chunk_size_records(1);
                let (mut handle, (edges, paths)) = Runtime::init_circuit(config, move |circuit| {
                    let (edges, edges_handle) = circuit.add_input_zset::<Tup2<u64, u64>>();
                    let bound = NonZeroU64::new(SPLIT_OUTPUTS_BOUND);
                    let paths = transitive_closure(circuit, &edges, distinct, bound);
                    Ok((edges_handle, paths.output()))
                })
                .unwrap();

                for i in 0..12 {
                    edges.append(&mut vec![Tup2(Tup2(i, i + 1), 1)]);
                    handle.transaction().unwrap();
                    assert_eq!(
                        paths.consolidate(),
                        bounded_paths_to(i),
                        "workers: {workers}, distinct: {distinct}, transaction {i}"
                    );
                }
                handle.kill().unwrap();
            }
        }
    }

    /// A bounded recursion stops all of its variables at the same iteration.
    ///
    /// `tagged` receives each path that `paths` feeds to an iteration in two
    /// ways: directly, through an exchange, and through a join and a
    /// `distinct` that spread [`FANOUT`] copies of it over many steps.
    /// Whichever way a path comes, an iteration of `tagged` must see the paths
    /// that the previous iteration of `paths` produced, and none that the
    /// same iteration produced.
    #[test]
    fn bound_holds_across_variables() {
        /// Number of copies of each path that go through the `distinct`.
        const FANOUT: u64 = 20;
        const TRANSACTIONS: u64 = 10;

        for workers in [1, 2, 4] {
            for distinct in [true, false] {
                let config = CircuitConfig::from(workers).with_splitter_chunk_size_records(1);
                let (mut handle, (edges, tags, paths, tagged)) =
                    Runtime::init_circuit(config, move |circuit| {
                        let (edges, edges_handle) = circuit.add_input_zset::<Tup2<u64, u64>>();
                        let (tags, tags_handle) = circuit.add_input_zset::<Tup2<u64, u64>>();

                        let builder = circuit.recursion_builder(
                            |child| {
                                Ok((
                                    child.recursive_var::<OrdZSet<Tup2<u64, u64>>>(),
                                    child.recursive_var::<OrdZSet<Tup2<Tup2<u64, u64>, u64>>>(),
                                ))
                            },
                            move |child, (paths, _tagged)| {
                                let edges = edges.delta0(child);
                                let tags = tags.delta0(child);

                                let paths_by_end = paths.map_index(|&Tup2(from, to)| (to, from));
                                let edges_indexed = edges.map_index(|Tup2(from, to)| (*from, *to));
                                let longer_paths = paths_by_end
                                    .join(&edges_indexed, |_via, from, to| Tup2(*from, *to));

                                let tags_indexed = tags.map_index(|Tup2(node, tag)| (*node, *tag));
                                let copied = paths.shard().map(|path| Tup2(*path, 0));
                                let fanned_out = paths_by_end
                                    .join(&tags_indexed, |to, from, tag| {
                                        Tup2(Tup2(*from, *to), *tag)
                                    })
                                    .distinct();

                                Ok((edges.plus(&longer_paths), copied.plus(&fanned_out)))
                            },
                        );
                        let builder = if distinct {
                            builder
                        } else {
                            builder.without_distinct()
                        };
                        let (paths, tagged) = builder
                            .with_bound(NonZeroU64::new(SPLIT_OUTPUTS_BOUND).unwrap())
                            .finish()
                            .unwrap();

                        Ok((edges_handle, tags_handle, paths.output(), tagged.output()))
                    })
                    .unwrap();

                tags.append(
                    &mut (0..=TRANSACTIONS)
                        .flat_map(|node| (1..=FANOUT).map(move |tag| Tup2(Tup2(node, tag), 1)))
                        .collect(),
                );
                for i in 0..TRANSACTIONS {
                    edges.append(&mut vec![Tup2(Tup2(i, i + 1), 1)]);
                    handle.transaction().unwrap();

                    let context =
                        format!("workers: {workers}, distinct: {distinct}, transaction {i}");
                    assert_eq!(paths.consolidate(), bounded_paths_to(i), "{context}");
                    // Iterations 1 to `SPLIT_OUTPUTS_BOUND` of `tagged` see the
                    // paths from the iterations before them, which are one edge
                    // shorter.
                    let expected_tagged = OrdZSet::from_keys(
                        (),
                        chain_paths_to(i, SPLIT_OUTPUTS_BOUND - 1)
                            .flat_map(|path| (0..=FANOUT).map(move |tag| Tup2(Tup2(path, tag), 1)))
                            .collect(),
                    );
                    assert_eq!(tagged.consolidate(), expected_tagged, "{context}");
                }
                handle.kill().unwrap();
            }
        }
    }

    /// [`bound_holds_with_split_outputs`] on two hosts with two workers each,
    /// whose exchanges carry the chunks over the network.
    #[test]
    fn bound_holds_with_split_outputs_multihost() {
        const HOSTS: usize = 2;
        const WORKERS_PER_HOST: usize = 2;

        // Bind all of the listeners first, so that each runtime can connect to
        // the others while they start up one after another.
        let listeners = (0..HOSTS)
            .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
            .collect::<Vec<_>>();
        let params = listeners
            .iter()
            .map(|listener| (listener.local_addr().unwrap(), WORKERS_PER_HOST))
            .collect::<Vec<_>>();

        let mut hosts = Vec::with_capacity(HOSTS);
        for (listener, (address, _)) in listeners.into_iter().zip(&params) {
            let config = CircuitConfig::from(Layout::new_multihost(&params, *address).unwrap())
                .with_exchange_listener(listener)
                .with_streaming_exchange(true)
                .with_splitter_chunk_size_records(1);
            hosts.push(
                Runtime::init_circuit(config, |circuit| {
                    let (edges, edges_handle) = circuit.add_input_zset::<Tup2<u64, u64>>();
                    let bound = NonZeroU64::new(SPLIT_OUTPUTS_BOUND);
                    let paths = transitive_closure(circuit, &edges, true, bound);
                    Ok((edges_handle, paths.output()))
                })
                .unwrap(),
            );
        }

        for i in 0..12 {
            // The input shards the edge across the workers of both hosts.
            hosts[0].1.0.append(&mut vec![Tup2(Tup2(i, i + 1), 1)]);
            thread::scope(|scope| {
                for (handle, _) in &mut hosts {
                    scope.spawn(|| handle.transaction().unwrap());
                }
            });
            let paths = hosts
                .iter()
                .map(|(_, (_, paths))| paths.consolidate())
                .reduce(|sum, paths| sum.add_by_ref(&paths))
                .unwrap();
            assert_eq!(paths, bounded_paths_to(i), "transaction {i}");
        }

        for (handle, _) in hosts {
            handle.kill().unwrap();
        }
    }
}
