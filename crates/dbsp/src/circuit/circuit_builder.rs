//! API to construct circuits.
//!
//! The API exposes two abstractions: "circuits" and "streams", where a circuit
//! is a possibly nested dataflow graph that consists of operators connected by
//! streams:
//!
//!   * Circuits are represented by the [`Circuit`] trait, which has a single
//!     implementation [`ChildCircuit<P>`], where `P` is the type of the parent
//!     circuit.  For a root circuit, `P` is `()`, so the API provides
//!     [`RootCircuit`] as a synonym for `ChildCircuit<()>`).
//!
//!     Use [`RootCircuit::build`] to create a new root circuit.  It takes a
//!     user-provided callback, which it calls to set up operators and streams
//!     inside the circuit, and then it returns the circuit. The circuit's
//!     structure is fixed at construction and can't be changed afterward.
//!
//!   * Streams are represented by struct [`Stream<C, D>`], which is a stream
//!     that carries values of type `D` within a circuit of type `C`.  Methods
//!     and traits on `Stream` are the main way to assemble the structure of a
//!     circuit within the [`RootCircuit::build`] callback.
//!
//! The API that this directly exposes runs the circuit in the context of the
//! current thread.  To instead run the circuit in a collection of worker
//! threads, use [`Runtime::init_circuit`].
#[cfg(doc)]
use crate::{
    algebra::{IndexedZSet, ZSet},
    operator::{time_series::RelRange, Aggregator, Fold, Generator, Max, Min},
    trace::Batch,
    InputHandle, OutputHandle,
};

use crate::{
    circuit::{
        cache::{CircuitCache, CircuitStoreMarker},
        fingerprinter::Fingerprinter,
        metadata::OperatorMeta,
        operator_traits::{
            BinaryOperator, BinarySinkOperator, Data, ImportOperator, NaryOperator,
            QuaternaryOperator, SinkOperator, SourceOperator, StrictUnaryOperator, TernaryOperator,
            UnaryOperator,
        },
        schedule::{
            DynamicScheduler, Error as SchedulerError, Executor, IterativeExecutor, OnceExecutor,
            Scheduler,
        },
        trace::{CircuitEvent, SchedulerEvent},
    },
    circuit_cache_key,
    operator::communication::Exchange,
    time::{Timestamp, UnitTimestamp},
    Error as DBSPError, Error, Runtime,
};
use anyhow::Error as AnyError;
use serde::Serialize;
use std::{
    any::type_name_of_val,
    borrow::Cow,
    cell::{Ref, RefCell, RefMut, UnsafeCell},
    collections::HashMap,
    fmt,
    fmt::{Debug, Display, Write},
    iter::repeat,
    marker::PhantomData,
    panic::Location,
    rc::Rc,
    thread::panicking,
};
use typedmap::{TypedMap, TypedMapKey};
use uuid::Uuid;

/// Value stored in the stream.
struct StreamValue<D> {
    /// Value written to the stream at the current clock cycle;
    /// `None` after the last consumer has retrieved the value from the stream.
    val: Option<D>,
    /// The number of consumers connected to the stream.  Each consumer
    /// reads from the stream exactly once at every clock cycle.
    consumers: usize,
    /// The number of remaining consumers still expected to read from the stream
    /// at the current clock cycle.  This value is reset to `consumers` when
    /// a new value is written to the stream.  It is decremented on each access.
    /// The last consumer to read from the stream (`tokens` drops to 0) obtains
    /// an owned value rather than a borrow.  See description of
    /// [ownership-aware scheduling](`OwnershipPreference`) for details.
    tokens: usize,
}

impl<D> StreamValue<D> {
    const fn empty() -> Self {
        Self {
            val: None,
            consumers: 0,
            tokens: 0,
        }
    }
}

/// A `Stream<C, D>` stores the output value of type `D` of an operator in a
/// circuit with type `C`.
///
/// Circuits are synchronous, meaning that each value is produced and consumed
/// in the same clock cycle, so there can be at most one value in the stream at
/// any time.
///
/// The value type `D` may be any type, although most `Stream` methods impose
/// additional requirements.  Since a stream must yield one data item per clock
/// cycle, the rate at which data arrives is important to the choice of type.
/// If, for example, an otherwise scalar input stream might not have new data on
/// every cycle, an `Option` type could represent that, and to batch multiple
/// pieces of data in a single step, one might use [`Vec`] or another collection
/// type.
///
/// In practice, `D` is often a special collection type called an "indexed
/// Z-set", represented as trait [`IndexedZSet`].  An indexed Z-set is
/// conceptually a set of `(key, value, weight)` tuples.  Indexed Z-sets have a
/// specialization called a "non-indexed Z-set" ([`ZSet`]) that contains `key`
/// and `weight` only.  Indexed and non-indexed Z-sets are both subtraits of a
/// higher-level [`Batch`] trait.  Many operators on streams work only with an
/// indexed or non-indexed Z-set or another batch type as `D`.
///
/// # Data streams versus delta streams
///
/// A value in a `Stream` can represent data, or it can represent a delta (also
/// called an "update").  Most streams carry data types that could have either
/// meaning.  In particular, a stream of indexed or non-indexed Z-sets can carry
/// either:
///
///   * In a stream of data, the `weight` indicates the multiplicity of a key. A
///     negative `weight` has no natural interpretation and might indicate a
///     bug.
///
///   * In a stream of deltas or updates, a positive `weight` represents
///     insertions and a negative `weight` represents deletions.
///
/// There's no way to distinguish a data stream from a delta stream from just
/// the type of the `Stream` since, as described above, a stream of Z-sets can
/// be either one.  Some operators make sense for either kind of stream; for
/// example, adding streams of Z-sets with [`plus`](`Stream::plus`) works
/// equally well in either case, or even for adding a delta to data.  But other
/// operations make sense only for streams of data or only for streams of
/// deltas.  In these cases, `Stream` often provides an operator for each type
/// of stream, and the programmer must choose the right one, since the types
/// themselves don't help.
///
/// `Stream` refers to operators specifically for streams of data as
/// "nonincremental".  These operators, which have `stream` in their name,
/// e.g. `stream_join`, take streams of data as input and produce one as output.
/// They act as "lifted scalar operators" that don't maintain state across
/// invocations and only act on their immediate inputs, that is, each output is
/// produced by independently applying the operator to the individual inputs
/// received in the current step:
///
/// ```text
///       ┌─────────────┐
/// a ───►│    non-     │
///       │ incremental ├───► c
/// b ───►│  operator   │
///       └─────────────┘
/// ```
///
/// `Stream` refers to operators meant for streams of deltas as "incremental".
/// These operators take streams of deltas as input and produces a stream of
/// deltas as output.  Such operators could be implemented, inefficiently, in
/// terms of a nonincremental version by putting an integration operator before
/// each input and a differentiation operator after the output:
///
/// ```text
///        ┌───┐      ┌─────────────┐
/// Δa ───►│ I ├─────►│             │
///        └───┘      │    non-     │    ┌───┐
///                   │ incremental ├───►│ D ├───► Δc
///        ┌───┐      │  operator   │    └───┘
/// Δb ───►│ I ├─────►│             │
///        └───┘      └─────────────┘
/// ```
///
/// # Operator naming convention
///
/// `Stream` uses `_index` and `_generic` suffixes and
/// `stream_` prefix to declare variations of basic operations, e.g., `map`,
/// `map_index`, `map_generic`, `map_index_generic`, `join`, `stream_join`:
///
///   * `stream_` prefix: This prefix indicates that the operator is
///     "nonincremental", that is, that it works with streams of data, rather
///     than streams of deltas (see [Data streams versus delta streams], above).
///
///     [Data streams versus delta streams]:
///     Stream#data-streams-versus-delta-streams
///
///   * `_generic` suffix: Most operators can assemble their outputs into any
///     collection type that implements the [`Batch`] trait.  In practice, we
///     typically use [`OrdIndexedZSet`](`crate::OrdIndexedZSet`) for indexed
///     batches and [`OrdZSet`](`crate::OrdZSet`) for non-indexed batches.
///     Methods without the `_generic` suffix return these concrete types,
///     eliminating the need to type-annotate each invocation, while `_generic`
///     methods can be used to return arbitrary custom `Batch` implementations.
///
///   * `_index` suffix: Methods without the `_index` suffix return non-indexed
///     batches.  `<method>_index` methods combine the effects of `<method>` and
///     [`index`](Self::index), e.g., `stream.map_index(closure)` is
///     functionally equivalent, but more efficient than,
///     `stream.map(closure).index()`.
///
/// # Catalog of stream operators
///
/// `Stream` methods are the main way to perform
/// computations with streams.  The number of available methods can be
/// overwhelming, so the subsections below categorize them into functionally
/// related groups.
///
/// ## Input operators
///
/// Most streams are obtained via methods or traits that operate on `Stream`s.
/// The input operators create the initial input streams for these other methods
/// to work with.
///
/// [`Circuit::add_source`] is the fundamental way to add an input stream.
/// Using it directly makes sense for cases like generating input using a
/// function (perhaps using [`Generator`]) or reading data from a file.  More
/// commonly, [`RootCircuit`] offers the `add_input_*` functions as convenience
/// wrappers for `add_source`.  Each one returns a tuple of:
///
///   * A `Stream` that can be attached as input to operators in the circuit
///     (within the constructor function passed to `RootCircuit::build` only).
///
///   * An input handle that can be used to add input to the stream from outside
///     the circuit.  In a typical scenario, the closure passed to build will
///     return all of its input handles, which are used at runtime to feed new
///     inputs to the circuit at each step.  Different functions return
///     different kinds of input handles.
///
/// Use [`RootCircuit::add_input_indexed_zset`] or
/// [`RootCircuit::add_input_zset`] to crate an (indexed) Z-set input
/// stream. There's also [`RootCircuit::add_input_set`] and
/// [`RootCircuit::add_input_map`] to simplify cases where a regular set or
/// map is easier to use than a Z-set.  The latter functions maintain an extra
/// internal table tracking the contents of the set or map, so they're a second
/// choice.
///
/// For special cases, there's also [`RootCircuit::add_input_stream<T>`].  The
/// [`InputHandle`] that it returns needs to be told which workers to feed the
/// data to, which makes it harder to use.  It might be useful for feeding
/// non-relational data to the circuit, such as the current physical time.  DBSP
/// does not know how to automatically distribute such values across workers,
/// so the caller must decide whether to send the value to one specific worker
/// or to broadcast it to everyone.
///
/// It's common to pass explicit type arguments to the functions that
/// create input streams, e.g.:
///
/// ```ignore
/// circuit.add_input_indexed_zset::<KeyType, ValueType, WeightType>()
/// ```
///
/// ## Output and debugging operators
///
/// There's not much value in computations whose output can't be seen in the
/// outside world.  Use [`Stream::output`] to obtain an [`OutputHandle`] that
/// exports a stream's data.  The constructor function passed to
/// [`RootCircuit::build`] should return the `OutputHandle` (in addition to all
/// the input handles as described above).  After each step, the client code
/// should take the new data from the `OutputHandle`, typically by calling
/// [`OutputHandle::consolidate`].
///
/// Use [`Stream::inspect`] to apply a callback function to each data item in a
/// stream.  The callback function has no return value and is executed only for
/// its side effects, such as printing the data item to stdout.  The `inspect`
/// operator yields the same stream on its output.
///
/// It is not an operator, but [`Circuit::region`] can help with debugging by
/// grouping operators into a named collection.
///
/// ## Record-by-record mapping operators
///
/// These operators map one kind of batch to another, allowing the client to
/// pass in a function that looks at individual records in a Z-set or other
/// batch.  These functions apply to both streams of deltas and streams of data.
///
/// The following methods are available for streams of indexed and non-indexed
/// Z-sets that implement trait [`FilterMap`](`crate::FilterMap`).  Each of
/// these takes a function that accepts a key (for non-indexed Z-sets) or a
/// key-value pair (for indexed Z-sets):
///
///   * Use [`Stream::map`] to output a non-indexed Z-set using an
///     arbitrary mapping function, or [`Stream::map_index`] to map to
///     an indexed Z-set.
///
///   * Use [`Stream::filter`] to drop records that do not satisfy a
///     predicate function.  The output stream has the same type as the input.
///
///   * Use [`Stream::flat_map`] to output a Z-set that maps each
///     input record to any number of records, or
///     [`Stream::flat_map_index`] for indexed Z-set output.  These
///     methods also work as a `filter_map` equivalent.
///
/// ## Value-by-value mapping operators
///
/// Sometimes it makes sense to map a stream's whole data item instead of
/// breaking Z-sets down into records.  Unlike the record-by-record functions,
/// these functions works with streams that carry a type other than a Z-set or
/// batch.  These functions apply to both streams of deltas and streams of data.
///
/// Use [`Stream::apply`] to apply a mapping function to each item in a given
/// stream.  [`Stream::apply_named`], [`Stream::apply_owned`], and
/// [`Stream::apply_owned_named`] offer small variations.
///
/// Use [`Stream::apply2`] or [`Stream::apply2_owned`] to apply a mapping
/// function to pairs of items drawn from two different input streams.
///
/// ## Addition and subtraction operators
///
/// Arithmetic operators work on Z-sets (and batches) by operating on weights.
/// For example, adding two Z-sets adds the weights of records with the same
/// key-value pair.  They also work on streams of arithmetic types.
///
/// Use [`Stream::neg`] to map a stream to its negation, that is, to negate the
/// weights for a Z-set.
///
/// Use [`Stream::plus`] to add two streams and [`Stream::sum`] to add an
/// arbitrary number of streams.  Use [`Stream::minus`] to subtract streams.
///
/// There aren't any multiplication or division operators, because there is no
/// clear interpretation of them for Z-sets.  You can use [`Stream::apply`] and
/// [`Stream::apply2`], as already described, to do arbitrary arithmetic on one
/// or two streams of arithmetic types.
///
/// ## Stream type conversion operators
///
/// These operators convert among streams of deltas, streams of data, and
/// streams of "upserts".  Client code can use them, but they're more often
/// useful for testing (for example, for checking that incremental operators are
/// equivalent to non-incremental ones) or for building other operators.
///
/// Use [`Stream::integrate`] to sum up the values within an input stream.  The
/// first output value is the first input value, the second output value is the
/// sum of the first two inputs, and so on.  This effectively converts a stream
/// of deltas into a stream of data.
///
/// [`Stream::stream_fold`] generalizes integration.  On each step, it calls a
/// function to fold the input value with an accumulator and outputs the
/// accumulator.  The client provides the function and the initial value of the
/// accumulator.
///
/// Use [`Stream::differentiate`] to calculate differences between subsequent
/// values in an input stream.  The first output is the first input value, the
/// second output is the second input value minus the first input value, and so
/// on.  This effectively converts a stream of data into a stream of deltas.
/// You shouldn't ordinarily need this operator, at least not for Z-sets,
/// because DBSP operators are fully incremental.
///
/// Use [`Stream::upsert`] to convert a stream of "upserts" into a stream of
/// deltas.  The input stream carries "upserts", or assignments of values to
/// keys such that a subsequent assignment to a key assigned earlier replaces
/// the earlier value.  `upsert` turns these into a stream of deltas by
/// internally tracking upserts that have already been seen.
///
/// ## Weighting and counting operators
///
/// Use [`Stream::dyn_weigh`] to multiply the weights in an indexed Z-set by a
/// user-provided function of the key and value.  This is equally appropriate
/// for streams of data or deltas.  This method outputs a non-indexed Z-set with
/// just the keys from the input, discarding the values, which also means that
/// weights will be added together in the case of equal input keys.
///
/// `Stream` provides methods to count the number of values per key in an
/// indexed Z-set:
///
///   * To sum the weights for each value within a key, use
///     [`Stream::dyn_weighted_count`] for streams of deltas or
///     [`Stream::dyn_stream_weighted_count`] for streams of data.
///
///   * To count each value only once even for a weight greater than one, use
///     [`Stream::dyn_distinct_count`] for streams of deltas or
///     [`Stream::dyn_stream_distinct_count`] for streams of data.
///
/// The "distinct" operator on a Z-set maps positive weights to 1 and all other
/// weights to 0.  `Stream` has two implementations:
///
///   * Use [`Stream::distinct`] to incrementally process a stream of deltas. If
///     the output stream were to be integrated, it only contain records with
///     weight 0 or 1.  This operator internally integrates the stream of
///     deltas, which means its memory consumption is proportional to the
///     integrated data size.
///
///   * Use [`Stream::stream_distinct`] to non-incrementally process a stream of
///     data.  It sets each record's weight to 1 if it is positive and drops the
///     others.
///
/// ## Join on equal keys
///
/// A DBSP equi-join takes batches `a` and `b` as input, finds all pairs of a
/// record in `a` and a record in `b` with the same key, applies a given
/// function `F` to those records' key and values, and outputs a Z-set with
/// `F`'s output.
///
/// DBSP implements two kinds of joins:
///
///   * Joins of delta streams ("incremental" joins) for indexed Z-sets with the
///     same key type.  Use [`Stream::join`] for non-indexed Z-set output, or
///     [`Stream::join_index`] for indexed Z-set output.
///
///   * Joins of data streams ("nonincremental" joins), which work with any
///     indexed batches.  Use [`Stream::stream_join`], which outputs a
///     non-indexed Z-set.
///
///     `stream_join` also works for joining a stream of deltas with an
///     invariant stream of data where the latter is used as a lookup table.
///
///     If the output of the join function grows monotonically as `(k, v1, v2)`
///     tuples are fed to it in lexicographic order, then
///     [`Stream::monotonic_stream_join`] is more efficient.  One such monotonic
///     function is a join function that returns `(k, v1, v2)` itself.
///
/// One way to implement a Cartesian product is to map unindexed Z-set inputs
/// into indexed Z-sets with a unit key type, e.g. `input.index_with(|k| ((),
/// k))`, and then use `join` or `stream_join`, as appropriate.
///
/// ## Other kinds of joins
///
/// Use [`Stream::antijoin`] for antijoins of delta streams.  It takes indexed
/// Z-set `a` and Z-set `b` with the same key type and yields the subset of `a`
/// whose keys do not appear in `b`.  `b` may be indexed or non-indexed and its
/// value type does not matter.
///
/// Use [`Stream::dyn_semijoin_stream`] for semi-joins of data streams.  It
/// takes a batch `a` and non-indexed batch `b` with the same key type as `a`.
/// It outputs a non-indexed Z-set of key-value tuples that contains all the
/// pairs from `a` for which a key appears in `b`.
///
/// Use [`Stream::outer_join`] or [`Stream::outer_join_default`] for outer joins
/// of delta streams.  The former takes three functions, one for each of the
/// cases (common keys, left key only, right key only), and the latter
/// simplifies it by taking only a function for common keys and passing in the
/// default for the missing value.
///
/// DBSP implements "range join" of data streams, which joins keys in `a`
/// against ranges of keys in `b`.  [`Stream::dyn_stream_join_range`] implements
/// range join with non-indexed Z-set output,
/// [`Stream::dyn_stream_join_range_index`] with indexed output.
///
/// ## Aggregation
///
/// Aggregation applies a function (the "aggregation function") to all of the
/// values for a given key in an input stream, and outputs an indexed Z-set with
/// the same keys as the input and the function's output as values.  The output
/// of aggregation usually has fewer records than its input, because it outputs
/// only one record per input key, regardless of the number of key-value pairs
/// with that key.
///
/// DBSP implements two kinds of aggregation:
///
///   * [`Stream::dyn_aggregate`] aggregates delta streams.  It takes an
///     aggregation function as an [`Aggregator`], e.g. [`Min`], [`Max`],
///     [`Fold`], or one written by the client.
///
///     [`Stream::dyn_aggregate_linear`] is cheaper for linear aggregation
///     functions.  It's also a little easier to use with a custom aggregation
///     function, because it only takes a function rather than an [`Aggregator`]
///     object.
///
///     [`Stream::dyn_average`] calculates the average over the values for each
/// key.
///
///   * [`Stream::dyn_stream_aggregate`] aggregates data streams.  Each batch
///     from the input is separately aggregated and written to the output
///     stream.
///
///     [`Stream::dyn_stream_aggregate_linear`] applies a linear aggregation
///     function to a data stream.
///
/// These aggregation functions all partition the aggregation by key, like GROUP
/// BY in SQL.  To aggregate all records in a non-indexed Z-set, map to an
/// indexed Z-set with a unit key `()` before aggregating, then map again to
/// remove the index if necessary, e.g.:
///
/// ```ignore
/// let max_auction_count = auction_counts
///     .map_index(|(_auction, count)| ((), *count))
///     .aggregate(Max)
///     .map(|((), max_count)| *max_count);
/// ```
///
/// ## Rolling aggregates
///
/// DBSP supports rolling aggregation of time series data over a
/// client-specified "rolling window" range.  For this purpose, Rust unsigned
/// integer types model times, larger integers corresponding to later times.
/// The unit of time in use is relevant only for specifying the width of the
/// aggregation window, with [`RelRange`].
///
/// The DBSP logical time concept is unrelated to times used in rolling
/// aggregation and other time-series operators. The former is used to establish
/// the ordering in which updates are consumed by DBSP, while the latter model
/// physical times when the corresponding events were generated, observed, or
/// processed. In particular, the ordering of physical and logical timestamps
/// doesn't have to match. In other words, DBSP can process events out-of-order.
///
/// Rolling aggregation takes place within a "partition", which is any
/// convenient division of the data.  It might correspond to a tenant ID, for
/// example, if each tenant's data is to be separately aggregated.  To represent
/// partitioning, rolling aggregation introduces the
/// [`OrdPartitionedIndexedZSet`](`crate::OrdPartitionedIndexedZSet`)
/// type, which is an `IndexedZSet` with an arbitrary key type that specifies
/// the partition (it may be `()` if all data is to be within a single
/// partition) and a value type of the form `(TS, V)` where `TS` is the type
/// used for time and `V` is the client's value type.
///
/// Rolling aggregation does not reduce the size of data.  It outputs one record
/// for each input record.
///
/// DBSP has two kinds of rolling aggregation functions that differ based on
/// their tolerance for updating aggregation results when new data arrives for
/// an old moment in time:
///
///   * If the application must tolerate data arriving entirely out-of-order,
///     use [`Stream::partitioned_rolling_aggregate`].  It operates on a
///     `PartitionedIndexedZSet` and takes an [`Aggregator`] and a [`RelRange`]
///     that specifies the span of the window.  It returns another
///     `PartitionedIndexedZSet` with the results.  This operator must buffer
///     old data indefinitely since old output is always subject to revision.
///
///     [`Stream::partitioned_rolling_aggregate_linear`] is cheaper for linear
///     aggregation functions.
///
///     [`Stream::partitioned_rolling_average`] calculates the rolling average
///     over a partition.
///
///   * If the application can discard data that arrives too out-of-order, use
///     [`Stream::partitioned_rolling_aggregate_with_waterline`], which can be
///     more memory-efficient.  This form of rolling aggregation requires a
///     "waterline" stream, which is a stream of times (scalars, not batches or
///     Z sets) that reports the earliest time that can be updated.  Use
///     [`Stream::waterline_monotonic`] to conveniently produce the waterline
///     stream.
///
///     [`Stream::partitioned_rolling_aggregate_with_waterline`] operates on an
///     `IndexedZSet` and, in addition to the aggregrator, range, and waterline
///     stream, it takes a function to map a record to a partition. It discards
///     input before the waterline, partitions it, aggregates it, and returns
///     the result as a `PartitionedIndexedZSet`.
///
/// ## Windowing
///
/// Use [`Stream::dyn_window`] to extract a stream of deltas to windows from a
/// stream of deltas.  This can be useful for windowing outside the context of
/// rolling aggregation.
pub struct Stream<C, D> {
    /// Id of the operator within the local circuit that writes to the stream.
    local_node_id: NodeId,
    /// Global id of the node that writes to this stream.
    origin_node_id: GlobalNodeId,
    /// Circuit that this stream belongs to.
    circuit: C,
    /// Value stored in the stream (there can be at most one since our
    /// circuits are synchronous).
    /// We use `UnsafeCell` instead of `RefCell` to avoid runtime ownership
    /// tests. We enforce unique ownership by making sure that at most one
    /// operator can run (and access the stream) at any time.
    val: Rc<UnsafeCell<StreamValue<D>>>,
}

impl<C, D> Clone for Stream<C, D>
where
    C: Clone,
{
    fn clone(&self) -> Self {
        Self {
            local_node_id: self.local_node_id,
            origin_node_id: self.origin_node_id.clone(),
            circuit: self.circuit.clone(),
            val: self.val.clone(),
        }
    }
}

impl<C, D> Stream<C, D>
where
    C: Clone,
{
    /// Transmute a stream of `D` into a stream of `D2`.
    ///
    /// This is unsafe and dangerous for the same reasons [`std::mem:transmute`]
    /// is dangerous and should be used with care.
    ///
    /// # Safety
    ///
    /// Transmuting `D` into `D2` should be safe.
    pub(crate) unsafe fn transmute_payload<D2>(&self) -> Stream<C, D2> {
        Stream {
            local_node_id: self.local_node_id,
            origin_node_id: self.origin_node_id.clone(),
            circuit: self.circuit.clone(),
            val: std::mem::transmute::<
                Rc<UnsafeCell<StreamValue<D>>>,
                Rc<UnsafeCell<StreamValue<D2>>>,
            >(self.val.clone()),
        }
    }
}

impl<C, D> Stream<C, D> {
    /// Returns local node id of the operator or subcircuit that writes to
    /// this stream.
    ///
    /// If the stream originates in a subcircuit, returns the id of the
    /// subcircuit node.
    pub fn local_node_id(&self) -> NodeId {
        self.local_node_id
    }

    /// Returns global id of the operator that writes to this stream.
    ///
    /// If the stream originates in a subcircuit, returns id of the operator
    /// inside the subcircuit (or one of its subcircuits) that produces the
    /// contents of the stream.
    pub fn origin_node_id(&self) -> &GlobalNodeId {
        &self.origin_node_id
    }

    /// Reference to the circuit the stream belongs to.
    pub fn circuit(&self) -> &C {
        &self.circuit
    }

    pub fn ptr_eq<D2>(&self, other: &Stream<C, D2>) -> bool {
        self.origin_node_id() == other.origin_node_id()
    }
}

// Internal streams API only used inside this module.
impl<C, D> Stream<C, D>
where
    C: Circuit,
{
    /// Create a new stream within the given circuit, connected to the specified
    /// node id.
    fn new(circuit: C, node_id: NodeId) -> Self {
        Self {
            local_node_id: node_id,
            origin_node_id: GlobalNodeId::child_of(&circuit, node_id),
            circuit,
            val: Rc::new(UnsafeCell::new(StreamValue::empty())),
        }
    }

    /// Export stream to the parent circuit.
    ///
    /// Creates a stream in the parent circuit that contains the last value in
    /// `self` when the child circuit terminates.
    ///
    /// This method currently only works for streams connected to a feedback
    /// `Z1` operator and will panic for other streams.
    pub fn export(&self) -> Stream<C::Parent, D>
    where
        C::Parent: Circuit,
        D: 'static,
    {
        self.circuit()
            .cache_get_or_insert_with(
                ExportId::new(self.origin_node_id().clone()),
                || unimplemented!(),
            )
            .clone()
    }
}

impl<C, D> Stream<C, D> {
    /// Create a stream whose origin differs from its node id.
    fn with_origin(circuit: C, node_id: NodeId, origin_node_id: GlobalNodeId) -> Self {
        Self {
            local_node_id: node_id,
            origin_node_id,
            circuit,
            val: Rc::new(UnsafeCell::new(StreamValue::empty())),
        }
    }
}

impl<C, D> Stream<C, D>
where
    D: Clone,
{
    /// Consume a value from the stream.
    ///
    /// This function is invoked exactly once per clock cycle by each
    /// consumer connected to the stream.  The last consumer receives
    /// an owned value (`Cow::Owned`).
    ///
    /// # Safety
    ///
    /// The caller must have exclusive access to the current stream.
    pub(crate) unsafe fn take(&'_ self) -> Cow<'_, D> {
        let val = &mut *self.val.get();
        debug_assert_ne!(val.tokens, 0);
        val.tokens -= 1;
        if val.tokens == 0 {
            Cow::Owned(val.val.take().unwrap())
        } else {
            Cow::Borrowed(val.val.as_ref().unwrap())
        }
    }

    /// Get a reference to the content of the stream without
    /// decrementing the refcount.
    pub(crate) unsafe fn peek(&self) -> &D {
        let val = &*self.val.get();
        debug_assert_ne!(val.tokens, 0);

        val.val.as_ref().unwrap()
    }

    /// Puts a value in the stream, overwriting the previous value if any.
    ///
    /// #Safety
    ///
    /// The caller must have exclusive access to the current stream.
    unsafe fn put(&self, d: D) {
        let val = &mut *self.val.get();
        // If the stream is not connected to any consumers, drop the output
        // on the floor.
        if val.consumers > 0 {
            val.tokens = val.consumers;
            val.val = Some(d);
        }
    }
}

/// Stream whose final value is exported to the parent circuit.
///
/// The struct bundles a pair of streams emitted by a
/// [`StrictOperator`](`crate::circuit::operator_traits::StrictOperator`):
/// a `local` stream inside operator's local circuit and an
/// export stream available to the parent of the local circuit.
/// The export stream contains the final value computed by the
/// operator before `clock_end`.
pub struct ExportStream<C, D>
where
    C: Circuit,
{
    pub local: Stream<C, D>,
    pub export: Stream<C::Parent, D>,
}

/// Relative location of a circuit in the hierarchy of nested circuits.
///
/// `0` refers to the local circuit that a given node or operator belongs
/// to, `1` - to the parent of the local, circuit, `2` - to the parent's
/// parent, etc.
pub type Scope = u16;

/// Node in a circuit.  A node wraps an operator with strongly typed
/// input and output streams.
pub trait Node {
    /// Node id unique within its parent circuit.
    fn local_id(&self) -> NodeId;

    /// Global node id.
    fn global_id(&self) -> &GlobalNodeId;

    fn name(&self) -> Cow<'static, str>;

    /// `true` if the node encapsulates an asynchronous operator (see
    /// [`Operator::is_async()`](super::operator_traits::Operator::is_async)).
    /// `false` for synchronous operators and subcircuits.
    fn is_async(&self) -> bool;

    /// `true` if the node is ready to execute (see
    /// [`Operator::ready()`](super::operator_traits::Operator::ready)).
    /// Always returns `true` for synchronous operators and subcircuits.
    fn ready(&self) -> bool;

    /// Register callback to be invoked when an asynchronous operator becomes
    /// ready (see
    /// [`super::operator_traits::Operator::register_ready_callback`]).
    fn register_ready_callback(&mut self, _cb: Box<dyn Fn() + Send + Sync>) {}

    /// Evaluate the operator.  Reads one value from each input stream
    /// and pushes a new value to the output stream (except for sink
    /// operators, which don't have an output stream).
    ///
    /// # Safety
    ///
    /// Only one node may be scheduled at any given time (a node cannot invoke
    /// another node).
    unsafe fn eval(&mut self) -> Result<(), SchedulerError>;

    /// Notify the node about start of a clock epoch.
    ///
    /// The node should forward the notification to its inner operator.
    ///
    /// # Arguments
    ///
    /// * `scope` - the scope whose clock is restarting. A node gets notified
    ///   about clock events in its local circuit (scope 0) and all its
    ///   ancestors.
    fn clock_start(&mut self, scope: Scope);

    /// Notify the node about the end of a clock epoch.
    ///
    /// The node should forward the notification to its inner operator.
    ///
    /// # Arguments
    ///
    /// * `scope` - the scope whose clock is ending.
    ///
    /// # Safety
    ///
    /// Only one node may be scheduled at any given time (a node cannot invoke
    /// another node).
    unsafe fn clock_end(&mut self, scope: Scope);

    fn metadata(&self, output: &mut OperatorMeta);

    fn fixedpoint(&self, scope: Scope) -> bool;

    fn map_nodes_recursive(&self, _f: &mut dyn FnMut(&dyn Node)) {}

    fn map_nodes_recursive_mut(&self, _f: &mut dyn FnMut(&mut dyn Node)) {}

    /// Instructs the node to commit the state of its inner operator to
    /// persistent storage.
    fn commit(&mut self, cid: Uuid) -> Result<(), DBSPError>;

    /// Instructs the node to restore the state of its inner operator to
    /// the given checkpoint.
    fn restore(&mut self, cid: Uuid) -> Result<(), DBSPError>;

    /// Takes a fingerprint of the node's inner operator adds it to `fip`.
    fn fingerprint(&self, fip: &mut Fingerprinter) {
        fip.hash(type_name_of_val(self));
    }
}

/// Id of an operator, guaranteed to be unique within a circuit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[repr(transparent)]
pub struct NodeId(usize);

impl NodeId {
    pub fn new(id: usize) -> Self {
        Self(id)
    }

    /// Extracts numeric representation of the node id.
    pub fn id(&self) -> usize {
        self.0
    }

    pub(super) fn root() -> Self {
        Self(0)
    }
}

impl Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_char('n')?;
        Debug::fmt(&self.0, f)
    }
}

/// Globally unique id of a node (operator or subcircuit).
///
/// The identifier consists of a path from the top-level circuit to the node.
/// The top-level circuit has global id `[]`, an operator in the top-level
/// circuit or a sub-circuit nested inside the top-level circuit will have a
/// path of length 1, e.g., `[5]`, an operator inside the nested circuit
/// will have a path of length 2, e.g., `[5, 1]`, etc.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[repr(transparent)]
pub struct GlobalNodeId(Vec<NodeId>);

impl Display for GlobalNodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[")?;
        let path = self.path();
        for i in 0..path.len() {
            f.write_str(&path[i].0.to_string())?;
            if i < path.len() - 1 {
                f.write_str(".")?;
            }
        }
        f.write_str("]")
    }
}

impl GlobalNodeId {
    /// Generate global node id from path.
    pub fn from_path(path: &[NodeId]) -> Self {
        Self(path.to_owned())
    }

    /// Generate global node id from path.
    pub fn from_path_vec(path: Vec<NodeId>) -> Self {
        Self(path)
    }

    pub fn root() -> Self {
        Self(Vec::new())
    }

    /// Generate global node id by appending `child_id` to `self`.
    pub fn child(&self, child_id: NodeId) -> Self {
        let mut path = Vec::with_capacity(self.path().len() + 1);
        for id in self.path() {
            path.push(*id);
        }
        path.push(child_id);
        Self(path)
    }

    /// Generate global node id for a child node of `circuit`.
    pub fn child_of<C>(circuit: &C, node_id: NodeId) -> Self
    where
        C: Circuit,
    {
        let mut ids = circuit.global_node_id().path().to_owned();
        ids.push(node_id);
        Self(ids)
    }

    /// Returns local node id of `self` or `None` if `self` is the root node.
    pub fn local_node_id(&self) -> Option<NodeId> {
        self.0.last().cloned()
    }

    /// Returns parent id of `self` or `None` if `self` is the rooot node.
    pub fn parent_id(&self) -> Option<Self> {
        self.0
            .split_last()
            .map(|(_, prefix)| GlobalNodeId::from_path(prefix))
    }

    /// Get the path from global.
    pub fn path(&self) -> &[NodeId] {
        &self.0
    }

    /// Generate unique id for use in persistent storage.
    pub(crate) fn persistent_id(&self) -> String {
        let mut pid = String::with_capacity(3 + self.0.len() * 3);
        pid += format!("{}", Runtime::worker_index()).as_str();
        for e in &self.0 {
            pid += format!("-{}", &e.0.to_string()).as_str();
        }
        pid
    }
}

type CircuitEventHandler = Box<dyn Fn(&CircuitEvent)>;
type SchedulerEventHandler = Box<dyn FnMut(&SchedulerEvent<'_>)>;
type CircuitEventHandlers = Rc<RefCell<HashMap<String, CircuitEventHandler>>>;
type SchedulerEventHandlers = Rc<RefCell<HashMap<String, SchedulerEventHandler>>>;

/// Operator's preference to consume input data by value.
///
/// # Background
///
/// A stream in a circuit can be connected to multiple consumers.  It is
/// therefore generally impossible to provide each consumer with an owned copy
/// of the data without cloning it.  At the same time, many operators can be
/// more efficient when working with owned inputs.  For instance, when computing
/// a sum of two z-sets, if one of the input z-sets is owned we can just add
/// values from the other z-set to it without cloning the first z-set.  If both
/// inputs are owned then we additionally do not need to clone key/value pairs
/// when inserting them.  Furthermore, the implementation can choose to add the
/// contents of the smaller z-set to the larger one.
///
/// # Ownership-aware scheduling
///
/// To leverage such optimizations, we adopt the best-effort approach: operators
/// consume streaming data by-value when possible while falling back to
/// pass-by-reference otherwise.  In a synchronous circuit, each operator reads
/// its input stream precisely once in each clock cycle. It is therefore
/// possible to determine the last consumer at each clock cycle and give it the
/// owned value from the channel.  It is furthermore possible for the scheduler
/// to schedule operators that strongly prefer owned values last.
///
/// We capture ownership preferences at two levels.  First, each individual
/// operator that consumes one or more streams exposes its preferences on a
/// per-stream basis via an API method (e.g.,
/// [`UnaryOperator::input_preference`]).  The [`Circuit`] API allows the
/// circuit builder to override these preferences when instantiating an
/// operator, taking into account circuit topology and workload.  We express
/// preference as a numeric value.
///
/// These preferences are associated with each edge in the circuit graph.  The
/// schedulers we have built so far implement a limited form of ownership-aware
/// scheduling.  They only consider strong preferences
/// ([`OwnershipPreference::STRONGLY_PREFER_OWNED`] and stronger) and model them
/// internally as hard constraints that must be satisfied for the circuit to be
/// schedulable.  Weaker preferences are ignored.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug, Hash)]
#[repr(transparent)]
pub struct OwnershipPreference(usize);

impl OwnershipPreference {
    /// Create a new instance with given numeric preference value (higher
    /// value means stronger preference).
    pub const fn new(val: usize) -> Self {
        Self(val)
    }

    /// The operator does not gain any speed up from consuming an owned value.
    pub const INDIFFERENT: Self = Self::new(0);

    /// The operator is likely to run faster provided an owned input, but
    /// shouldn't be prioritized over more impactful operators
    ///
    /// This gives a lower priority than [`Self::PREFER_OWNED`] so that
    /// operators who need ownership can get it when available
    pub const WEAKLY_PREFER_OWNED: Self = Self::new(40);

    /// The operator is likely to run faster provided an owned input.
    ///
    /// Preference levels above `PREFER_OWNED` should not be used by operators
    /// and are reserved for use by circuit builders through the [`Circuit`]
    /// API.
    pub const PREFER_OWNED: Self = Self::new(50);

    /// The circuit will suffer a significant performance hit if the operator
    /// cannot consume data in the channel by-value.
    pub const STRONGLY_PREFER_OWNED: Self = Self::new(100);

    /// Returns the numeric value of the preference.
    pub const fn raw(&self) -> usize {
        self.0
    }
}

impl Default for OwnershipPreference {
    #[inline]
    fn default() -> Self {
        Self::INDIFFERENT
    }
}

impl Display for OwnershipPreference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::INDIFFERENT => f.write_str("Indifferent"),
            Self::WEAKLY_PREFER_OWNED => f.write_str("WeaklyPreferOwned"),
            Self::PREFER_OWNED => f.write_str("PreferOwned"),
            Self::STRONGLY_PREFER_OWNED => f.write_str("StronglyPreferOwned"),
            Self(preference) => write!(f, "Preference({preference})"),
        }
    }
}

/// An edge in a circuit graph represents a stream connecting two
/// operators or a dependency (i.e., a requirement that one operator
/// must be evaluated before the other even if they are not connected
/// by a stream).
pub struct Edge {
    /// Source node.
    pub from: NodeId,
    /// Destination node.
    pub to: NodeId,
    /// Origin node that generates the stream.  If the origin belongs
    /// to the local circuit, this is just the full path to the `from`
    /// node.
    pub origin: GlobalNodeId,
    /// Ownership preference associated with the consumer of this
    /// stream or `None` if this is a dependency edge.
    pub ownership_preference: Option<OwnershipPreference>,
}

#[allow(dead_code)]
impl Edge {
    /// `true` if `self` is a dependency edge.
    pub(super) fn is_dependency(&self) -> bool {
        self.ownership_preference.is_none()
    }

    /// `true` if `self` is a stream edge.
    pub(super) fn is_stream(&self) -> bool {
        self.ownership_preference.is_some()
    }
}

circuit_cache_key!(ExportId<C, D>(GlobalNodeId => Stream<C, D>));

/// Trait for an object that has a clock associated with it.
/// This is implemented trivially for root circuits.
pub trait WithClock {
    /// `()` for a trivial zero-dimensional clock that doesn't need to count
    /// ticks.
    type Time: Timestamp;

    /// Nesting depth of the circuit running this clock.
    ///
    /// 0 - for the top-level clock, 1 - first-level nested circuit, etc.
    const NESTING_DEPTH: usize;

    /// Returns `NESTING_DEPTH`.
    ///
    /// Helpful when using the trait via dynamic dispatch.
    fn nesting_depth(&self) -> usize {
        Self::NESTING_DEPTH
    }

    /// Current time.
    fn time(&self) -> Self::Time;
}

/// This `impl` is only needed to bootstrap the
/// recursive definition of `WithClock` for `ChildCircuit`.
/// It is never actually used at runtime.
impl WithClock for () {
    type Time = UnitTimestamp;
    const NESTING_DEPTH: usize = usize::MAX;

    fn time(&self) -> Self::Time {
        UnitTimestamp
    }
}

impl<P> WithClock for ChildCircuit<P>
where
    P: WithClock,
{
    type Time = <<P as WithClock>::Time as Timestamp>::Nested;

    const NESTING_DEPTH: usize = P::NESTING_DEPTH.wrapping_add(1);

    fn time(&self) -> Self::Time {
        self.time.borrow().clone()
    }
}

/// The circuit interface.  All DBSP computation takes place within a circuit.
///
/// Circuits can nest.  The nesting hierarchy must be known statically at
/// compile time via the `Parent` associated type, which must be `()` for a root
/// circuit and otherwise the parent circuit's type.
///
/// A circuit has a clock represented by the `Time` associated type obtained via
/// the `WithClock` supertrait.  For a root circuit, this is a trivial
/// zero-dimensional clock that doesn't need to count ticks.
///
/// There is only one implementation, [`ChildCircuit<P>`], whose `Parent` type
/// is `P`.  [`RootCircuit`] is a synonym for `ChildCircuit<()>`.
pub trait Circuit: WithClock + Clone + 'static {
    /// Parent circuit type or `()` for the root circuit.
    type Parent;

    /// Returns the parent circuit of `self`.
    fn parent(&self) -> Self::Parent;

    fn edges(&self) -> Ref<'_, [Edge]>;

    /// Global id of the circuit node.
    ///
    /// Returns [`GlobalNodeId::root()`] for the root circuit.
    fn global_id(&self) -> Ref<'_, GlobalNodeId>;

    /// Number of nodes in the circuit.
    fn num_nodes(&self) -> usize;

    /// Returns vector of local node ids in the circuit.
    fn node_ids(&self) -> Vec<NodeId>;

    /// Relative depth of `self` from the root circuit.
    ///
    /// Returns 0 if `self` is the root circuit, 1 if `self` is an immediate
    /// child of the root circuit, etc.
    fn root_scope(&self) -> Scope;

    /// Circuit's node id within the parent circuit.
    fn node_id(&self) -> NodeId;

    /// Check if `this` and `other` refer to the same circuit instance.
    fn ptr_eq(this: &Self, other: &Self) -> bool;

    /// Returns circuit event handlers attached to the circuit.
    fn circuit_event_handlers(&self) -> CircuitEventHandlers;

    /// Returns scheduler event handlers attached to the circuit.
    fn scheduler_event_handlers(&self) -> SchedulerEventHandlers;

    /// Deliver `event` to all circuit event handlers.
    fn log_circuit_event(&self, event: &CircuitEvent);

    /// Deliver `event` to all scheduler event handlers.
    fn log_scheduler_event(&self, event: &SchedulerEvent<'_>);

    /// Circuit's global node id.
    fn global_node_id(&self) -> GlobalNodeId;

    /// Lookup a value in the circuit cache or create and insert a new value
    /// if it does not exist.
    ///
    /// See [`cache`](`crate::circuit::cache`) module documentation for details.
    fn cache_get_or_insert_with<K, F>(&self, key: K, f: F) -> RefMut<'_, K::Value>
    where
        K: 'static + TypedMapKey<CircuitStoreMarker>,
        F: FnMut() -> K::Value;

    /// Invoked by the scheduler at the end of a clock cycle, after all circuit
    /// operators have been evaluated.
    fn tick(&self);

    /// Deliver `clock_start` notification to all nodes in the circuit.
    fn clock_start(&self, scope: Scope);

    /// Deliver `clock_end` notification to all nodes in the circuit.
    fn clock_end(&self, scope: Scope);

    /// `true` if the specified node is ready to execute (see
    /// [`Operator::ready()`](super::operator_traits::Operator::ready)).
    fn ready(&self, id: NodeId) -> bool;

    /// Insert a value to the circuit cache, overwriting any existing value.
    ///
    /// See [`cache`](`crate::circuit::cache`) module documentation for
    /// details.
    fn cache_insert<K>(&self, key: K, val: K::Value)
    where
        K: TypedMapKey<CircuitStoreMarker> + 'static;

    fn cache_contains<K>(&self, key: &K) -> bool
    where
        K: TypedMapKey<CircuitStoreMarker> + 'static;

    fn cache_get<K>(&self, key: &K) -> Option<K::Value>
    where
        K: TypedMapKey<CircuitStoreMarker> + 'static,
        K::Value: Clone;

    /// Connect `stream` as input to `to`.
    fn connect_stream<T>(
        &self,
        stream: &Stream<Self, T>,
        to: NodeId,
        ownership_preference: OwnershipPreference,
    );

    fn register_ready_callback(&self, id: NodeId, cb: Box<dyn Fn() + Send + Sync>);

    fn is_async_node(&self, id: NodeId) -> bool;

    /// Evaluate operator with the given id.
    ///
    /// This method should only be used by schedulers.
    fn eval_node(&self, id: NodeId) -> Result<(), SchedulerError>;

    /// Evaluate closure `f` inside a new circuit region.
    ///
    /// A region is a logical grouping of circuit nodes.  Regions are used
    /// exclusively for debugging and do not affect scheduling or evaluation
    /// of the circuit.  This function creates a new region and executes
    /// closure `f` inside it.  Any operators or subcircuits created by
    /// `f` will belong to the new region.
    #[track_caller]
    fn region<F, T>(&self, name: &str, f: F) -> T
    where
        F: FnOnce() -> T;

    /// Add a source operator to the circuit.  See [`SourceOperator`].
    fn add_source<O, Op>(&self, operator: Op) -> Stream<Self, O>
    where
        O: Data,
        Op: SourceOperator<O>;

    /// Add a pair of operators that implement cross-worker communication.
    ///
    /// Operators that exchange data across workers are split into two
    /// operators: the **sender** responsible for partitioning values read
    /// from the input stream and distributing them across workers and the
    /// **receiver**, which receives and reassembles data received from its
    /// peers.  Splitting communication into two halves allows the scheduler
    /// to schedule useful work in between them instead of blocking to wait
    /// for the receiver.
    ///
    /// Exchange operators use some form of IPC or shared memory instead of
    /// streams to communicate.  Therefore, the sender must implement trait
    /// [`SinkOperator`], while the receiver implements [`SourceOperator`].
    ///
    /// This function adds both operators to the circuit and registers a
    /// dependency between them, making sure that the scheduler will
    /// evaluate the sender before the receiver even though there is no
    /// explicit stream connecting them.
    ///
    /// Returns the output stream produced by the receiver operator.
    ///
    /// # Arguments
    ///
    /// * `sender` - the sender half of the pair.  The sender must be a sink
    ///   operator
    /// * `receiver` - the receiver half of the pair.  Must be a source
    /// * `input_stream` - stream to connect as input to the `sender`.
    fn add_exchange<I, SndOp, O, RcvOp>(
        &self,
        sender: SndOp,
        receiver: RcvOp,
        input_stream: &Stream<Self, I>,
    ) -> Stream<Self, O>
    where
        I: Data,
        O: Data,
        SndOp: SinkOperator<I>,
        RcvOp: SourceOperator<O>;

    /// Like [`Self::add_exchange`], but overrides the ownership
    /// preference on the input stream with `input_preference`.
    fn add_exchange_with_preference<I, SndOp, O, RcvOp>(
        &self,
        sender: SndOp,
        receiver: RcvOp,
        input_stream: &Stream<Self, I>,
        input_preference: OwnershipPreference,
    ) -> Stream<Self, O>
    where
        I: Data,
        O: Data,
        SndOp: SinkOperator<I>,
        RcvOp: SourceOperator<O>;

    /// Add a sink operator (see [`SinkOperator`]).
    fn add_sink<I, Op>(&self, operator: Op, input_stream: &Stream<Self, I>)
    where
        I: Data,
        Op: SinkOperator<I>;

    /// Like [`Self::add_sink`], but overrides the ownership preference on the
    /// input stream with `input_preference`.
    fn add_sink_with_preference<I, Op>(
        &self,
        operator: Op,
        input_stream: &Stream<Self, I>,
        input_preference: OwnershipPreference,
    ) where
        I: Data,
        Op: SinkOperator<I>;

    /// Add a binary sink operator (see [`BinarySinkOperator`]).
    fn add_binary_sink<I1, I2, Op>(
        &self,
        operator: Op,
        input_stream1: &Stream<Self, I1>,
        input_stream2: &Stream<Self, I2>,
    ) where
        I1: Data,
        I2: Data,
        Op: BinarySinkOperator<I1, I2>;

    /// Like [`Self::add_binary_sink`], but overrides the ownership preferences
    /// on both input streams with `input_preference1` and
    /// `input_preference2`.
    fn add_binary_sink_with_preference<I1, I2, Op>(
        &self,
        operator: Op,
        input_stream1: (&Stream<Self, I1>, OwnershipPreference),
        input_stream2: (&Stream<Self, I2>, OwnershipPreference),
    ) where
        I1: Data,
        I2: Data,
        Op: BinarySinkOperator<I1, I2>;

    /// Add a unary operator (see [`UnaryOperator`]).
    fn add_unary_operator<I, O, Op>(
        &self,
        operator: Op,
        input_stream: &Stream<Self, I>,
    ) -> Stream<Self, O>
    where
        I: Data,
        O: Data,
        Op: UnaryOperator<I, O>;

    /// Like [`Self::add_unary_operator`], but overrides the ownership
    /// preference on the input stream with `input_preference`.
    fn add_unary_operator_with_preference<I, O, Op>(
        &self,
        operator: Op,
        input_stream: &Stream<Self, I>,
        input_preference: OwnershipPreference,
    ) -> Stream<Self, O>
    where
        I: Data,
        O: Data,
        Op: UnaryOperator<I, O>;

    /// Add a binary operator (see [`BinaryOperator`]).
    fn add_binary_operator<I1, I2, O, Op>(
        &self,
        operator: Op,
        input_stream1: &Stream<Self, I1>,
        input_stream2: &Stream<Self, I2>,
    ) -> Stream<Self, O>
    where
        I1: Data,
        I2: Data,
        O: Data,
        Op: BinaryOperator<I1, I2, O>;

    /// Like [`Self::add_binary_operator`], but overrides the ownership
    /// preference on both input streams with `input_preference1` and
    /// `input_preference2` respectively.
    fn add_binary_operator_with_preference<I1, I2, O, Op>(
        &self,
        operator: Op,
        input_stream1: (&Stream<Self, I1>, OwnershipPreference),
        input_stream2: (&Stream<Self, I2>, OwnershipPreference),
    ) -> Stream<Self, O>
    where
        I1: Data,
        I2: Data,
        O: Data,
        Op: BinaryOperator<I1, I2, O>;

    /// Add a ternary operator (see [`TernaryOperator`]).
    fn add_ternary_operator<I1, I2, I3, O, Op>(
        &self,
        operator: Op,
        input_stream1: &Stream<Self, I1>,
        input_stream2: &Stream<Self, I2>,
        input_stream3: &Stream<Self, I3>,
    ) -> Stream<Self, O>
    where
        I1: Data,
        I2: Data,
        I3: Data,
        O: Data,
        Op: TernaryOperator<I1, I2, I3, O>;

    /// Like [`Self::add_ternary_operator`], but overrides the ownership
    /// preference on the input streams with specified values.
    #[allow(clippy::too_many_arguments)]
    fn add_ternary_operator_with_preference<I1, I2, I3, O, Op>(
        &self,
        operator: Op,
        input_stream1: (&Stream<Self, I1>, OwnershipPreference),
        input_stream2: (&Stream<Self, I2>, OwnershipPreference),
        input_stream3: (&Stream<Self, I3>, OwnershipPreference),
    ) -> Stream<Self, O>
    where
        I1: Data,
        I2: Data,
        I3: Data,
        O: Data,
        Op: TernaryOperator<I1, I2, I3, O>;

    /// Add a quaternary operator (see [`QuaternaryOperator`]).
    fn add_quaternary_operator<I1, I2, I3, I4, O, Op>(
        &self,
        operator: Op,
        input_stream1: &Stream<Self, I1>,
        input_stream2: &Stream<Self, I2>,
        input_stream3: &Stream<Self, I3>,
        input_stream4: &Stream<Self, I4>,
    ) -> Stream<Self, O>
    where
        I1: Data,
        I2: Data,
        I3: Data,
        I4: Data,
        O: Data,
        Op: QuaternaryOperator<I1, I2, I3, I4, O>;

    /// Like [`Self::add_quaternary_operator`], but overrides the ownership
    /// preference on the input streams with specified values.
    #[allow(clippy::too_many_arguments)]
    fn add_quaternary_operator_with_preference<I1, I2, I3, I4, O, Op>(
        &self,
        operator: Op,
        input_stream1: (&Stream<Self, I1>, OwnershipPreference),
        input_stream2: (&Stream<Self, I2>, OwnershipPreference),
        input_stream3: (&Stream<Self, I3>, OwnershipPreference),
        input_stream4: (&Stream<Self, I4>, OwnershipPreference),
    ) -> Stream<Self, O>
    where
        I1: Data,
        I2: Data,
        I3: Data,
        I4: Data,
        O: Data,
        Op: QuaternaryOperator<I1, I2, I3, I4, O>;

    /// Add a N-ary operator (see [`NaryOperator`]).
    fn add_nary_operator<'a, I, O, Op, Iter>(
        &'a self,
        operator: Op,
        input_streams: Iter,
    ) -> Stream<Self, O>
    where
        I: Data,
        O: Data,
        Op: NaryOperator<I, O>,
        Iter: IntoIterator<Item = &'a Stream<Self, I>>;

    /// Like [`Self::add_nary_operator`], but overrides the ownership
    /// preference with `input_preference`.
    fn add_nary_operator_with_preference<'a, I, O, Op, Iter>(
        &'a self,
        operator: Op,
        input_streams: Iter,
        input_preference: OwnershipPreference,
    ) -> Stream<Self, O>
    where
        I: Data,
        O: Data,
        Op: NaryOperator<I, O>,
        Iter: IntoIterator<Item = &'a Stream<Self, I>>;

    /// Add a feedback loop to the circuit.
    ///
    /// Other methods in this API only support the construction of acyclic
    /// graphs, as they require the input stream to exist before nodes that
    /// consumes it are created.  This method instantiates an operator whose
    /// input stream can be connected later, and thus may depend on
    /// the operator's output.  This enables the construction of feedback loops.
    /// Since all loops in a well-formed circuit must include a [strict
    /// operator](`crate::circuit::operator_traits::StrictOperator`), `operator`
    /// must be [strict](`crate::circuit::operator_traits::StrictOperator`).
    ///
    /// Returns the output stream of the operator and an object that can be used
    /// to later connect its input.
    ///
    /// # Examples
    /// We build the following circuit to compute the sum of input values
    /// received from `source`. `z1` stores the sum accumulated during
    /// previous timestamps.  At every timestamp, the (`+`) operator
    /// computes the sum of the new value received from source and the value
    /// stored in `z1`.
    ///
    /// ```text
    ///                ┌─┐
    /// source ───────►│+├───┬─►
    ///           ┌───►└─┘   │
    ///           │          │ z1_feedback
    /// z1_output │    ┌──┐  │
    ///           └────┤z1│◄─┘
    ///                └──┘
    /// ```
    ///
    /// ```
    /// # use dbsp::{
    /// #     operator::{Z1, Generator},
    /// #     Circuit, RootCircuit,
    /// # };
    /// # let circuit = RootCircuit::build(|circuit| {
    /// // Create a data source.
    /// let source = circuit.add_source(Generator::new(|| 10));
    /// // Create z1.  `z1_output` will contain the output stream of `z1`; `z1_feedback`
    /// // is a placeholder where we can later plug the input to `z1`.
    /// let (z1_output, z1_feedback) = circuit.add_feedback(Z1::new(0));
    /// // Connect outputs of `source` and `z1` to the plus operator.
    /// let plus = source.apply2(&z1_output, |n1: &usize, n2: &usize| n1 + n2);
    /// // Connect the output of `+` as input to `z1`.
    /// z1_feedback.connect(&plus);
    /// Ok(())
    /// # });
    /// ```
    fn add_feedback<I, O, Op>(
        &self,
        operator: Op,
    ) -> (Stream<Self, O>, FeedbackConnector<Self, I, O, Op>)
    where
        I: Data,
        O: Data,
        Op: StrictUnaryOperator<I, O>;

    /// Like `add_feedback`, but additionally makes the output of the operator
    /// available to the parent circuit.
    ///
    /// Normally a [strict
    /// operator](`crate::circuit::operator_traits::StrictOperator`) writes a
    /// value computed based on inputs from previous clock cycles to its
    /// output stream at the start of each new clock cycle.  When the local
    /// clock epoch ends, the last value computed by the operator (that
    /// would otherwise be dropped) is written to the export stream instead.
    ///
    /// # Examples
    ///
    /// See example in the [`Self::iterate`] method.
    fn add_feedback_with_export<I, O, Op>(
        &self,
        operator: Op,
    ) -> (ExportStream<Self, O>, FeedbackConnector<Self, I, O, Op>)
    where
        I: Data,
        O: Data,
        Op: StrictUnaryOperator<I, O>;

    fn connect_feedback_with_preference<I, O, Op>(
        &self,
        output_node_id: NodeId,
        operator: Rc<UnsafeCell<Op>>,
        input_stream: &Stream<Self, I>,
        input_preference: OwnershipPreference,
    ) where
        I: Data,
        O: Data,
        Op: StrictUnaryOperator<I, O>;

    /// Add a child circuit.
    ///
    /// Creates an empty circuit with `self` as parent and invokes
    /// `child_constructor` to populate the circuit.  `child_constructor`
    /// typically captures some of the streams in `self` and connects them
    /// to source nodes of the child circuit.  It is also responsible for
    /// attaching an executor to the child circuit.  The return type `T`
    /// will typically contain output streams of the child.
    ///
    /// Most users should invoke higher-level APIs like [`Circuit::iterate`]
    /// instead of using this method directly.
    fn subcircuit<F, T, E>(
        &self,
        iterative: bool,
        child_constructor: F,
    ) -> Result<T, SchedulerError>
    where
        F: FnOnce(&mut ChildCircuit<Self>) -> Result<(T, E), SchedulerError>,
        E: Executor<ChildCircuit<Self>>;

    /// Add an iteratively scheduled child circuit.
    ///
    /// Add a child circuit with a nested clock.  The child will execute
    /// multiple times for each parent timestamp, until its termination
    /// condition is satisfied.  Every time the child circuit is activated
    /// by the parent (once per parent timestamp), the executor calls
    /// [`clock_start`](`super::operator_traits::Operator::clock_start`)
    /// on each child operator.  It then calls `eval` on all
    /// child operators in a causal order and checks if the termination
    /// condition is satisfied.  If the condition is `false`, the
    /// executor `eval`s all operators again.  Once the termination
    /// condition is `true`, the executor calls `clock_end` on all child
    /// operators and returns control back to the parent scheduler.
    ///
    /// The `constructor` closure populates the child circuit and returns a
    /// closure that checks the termination condition and an arbitrary
    /// user-defined return value that typically contains output streams
    /// of the child.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::{cell::RefCell, rc::Rc};
    /// use dbsp::{
    ///     operator::{Generator, Z1},
    ///     Circuit, RootCircuit,
    /// };
    ///
    /// let circuit = RootCircuit::build(|circuit| {
    ///     // Generate sequence 0, 1, 2, ...
    ///     let mut n: usize = 0;
    ///     let source = circuit.add_source(Generator::new(move || {
    ///         let result = n;
    ///         n = n + 1;
    ///         result
    ///     }));
    ///     // Compute factorial of each number in the sequence.
    ///     let fact = circuit
    ///         .iterate(|child| {
    ///             let counter = Rc::new(RefCell::new(0));
    ///             let counter_clone = counter.clone();
    ///             let countdown = source.delta0(child).apply(move |parent_val| {
    ///                 if *parent_val > 0 {
    ///                     *counter_clone.borrow_mut() += *parent_val;
    ///                 }
    ///                 let res = *counter_clone.borrow();
    ///                 *counter_clone.borrow_mut() -= 1;
    ///                 res
    ///             });
    ///             let (z1_output, z1_feedback) = child.add_feedback_with_export(Z1::new(1));
    ///             let mul = countdown.apply2(&z1_output.local, |n1: &usize, n2: &usize| n1 * n2);
    ///             z1_feedback.connect(&mul);
    ///             Ok((move || Ok(*counter.borrow() <= 1), z1_output.export))
    ///         })
    ///         .unwrap();
    ///     fact.inspect(|n| eprintln!("Output: {}", n));
    ///     Ok(())
    /// });
    /// ```
    fn iterate<F, C, T>(&self, constructor: F) -> Result<T, SchedulerError>
    where
        F: FnOnce(&mut ChildCircuit<Self>) -> Result<(C, T), SchedulerError>,
        C: Fn() -> Result<bool, SchedulerError> + 'static;

    /// Add an iteratively scheduled child circuit.
    ///
    /// Similar to [`iterate`](`Self::iterate`), but with a user-specified
    /// [`Scheduler`] implementation.
    fn iterate_with_scheduler<F, C, T, S>(&self, constructor: F) -> Result<T, SchedulerError>
    where
        F: FnOnce(&mut ChildCircuit<Self>) -> Result<(C, T), SchedulerError>,
        C: Fn() -> Result<bool, SchedulerError> + 'static,
        S: Scheduler + 'static;

    /// Add a child circuit that will iterate to a fixed point.
    ///
    /// For each parent clock cycle, the child circuit will iterate until
    /// reaching a fixed point, i.e., a state where the outputs of all
    /// operators are guaranteed to remain the same, should the nested clock
    /// continue ticking.
    ///
    /// The fixed point check is implemented by checking the following
    /// condition:
    ///
    /// * All operators in the circuit are in such a state that, if their inputs
    ///   remain constant (i.e., all future inputs are identical to the last
    ///   input), then their outputs remain constant too.
    ///
    /// This is a necessary and sufficient condition that is also easy to check
    /// by asking each operator if it is in a stable state (via the
    /// [`Operator::fixedpoint`](`super::operator_traits::Operator::fixedpoint`)
    /// API.
    ///
    /// # Warning
    ///
    /// The cost of checking this condition precisely can be high for some
    /// operators, which implement approximate checks instead.  For instance,
    /// delay operators ([`Z1`](`crate::operator::Z1`) and
    /// [`Z1Nested`](`crate::operator::Z1Nested`)) require storing the last
    /// two versions of the state instead of one and comparing them at each
    /// cycle.  Instead, they conservatively check for _specific_ fixed points,
    /// namely fixed points where both input and output of the operator are zero
    /// As a result, the circuit may fail to detect other fixed points and may
    /// iterate forever.
    ///
    /// The goal is to evolve the design so that circuits created using the
    /// high-level API (`Stream::xxx` methods) implement accurate fixed point
    /// checks, but there are currently no guardrails in the system against
    /// constructing non-compliant circuits.
    fn fixedpoint<F, T>(&self, constructor: F) -> Result<T, SchedulerError>
    where
        F: FnOnce(&mut ChildCircuit<Self>) -> Result<T, SchedulerError>;

    /// Add a child circuit that will iterate to a fixed point.
    ///
    /// Similar to [`fixedpoint`](`Self::fixedpoint`), but with a user-specified
    /// [`Scheduler`] implementation.
    fn fixedpoint_with_scheduler<F, T, S>(&self, constructor: F) -> Result<T, SchedulerError>
    where
        F: FnOnce(&mut ChildCircuit<Self>) -> Result<T, SchedulerError>,
        S: Scheduler + 'static;

    /// Make the contents of `parent_stream` available in the nested circuit
    /// via an [`ImportOperator`].
    ///
    /// Typically invoked via a convenience wrapper, e.g., [`Stream::delta0`].
    fn import_stream<I, O, Op>(
        &self,
        operator: Op,
        parent_stream: &Stream<Self::Parent, I>,
    ) -> Stream<Self, O>
    where
        Self::Parent: Circuit,
        I: Data,
        O: Data,
        Op: ImportOperator<I, O>;

    /// Like [`Self::import_stream`] but overrides the ownership
    /// preference on the input stream with `input_preference.
    fn import_stream_with_preference<I, O, Op>(
        &self,
        operator: Op,
        parent_stream: &Stream<Self::Parent, I>,
        input_preference: OwnershipPreference,
    ) -> Stream<Self, O>
    where
        Self::Parent: Circuit,
        I: Data,
        O: Data,
        Op: ImportOperator<I, O>;
}

/// A circuit consists of nodes and edges.  An edge from
/// node1 to node2 indicates that the output stream of node1
/// is connected to an input of node2.
struct CircuitInner<P>
where
    P: WithClock,
{
    parent: P,
    root_scope: Scope,
    // Circuit's node id within the parent circuit.
    node_id: NodeId,
    global_node_id: GlobalNodeId,
    nodes: Vec<Box<dyn Node>>,
    edges: Vec<Edge>,
    circuit_event_handlers: CircuitEventHandlers,
    scheduler_event_handlers: SchedulerEventHandlers,
    store: CircuitCache,
}

impl<P> CircuitInner<P>
where
    P: WithClock,
{
    fn new(
        parent: P,
        root_scope: Scope,
        node_id: NodeId,
        global_node_id: GlobalNodeId,
        circuit_event_handlers: CircuitEventHandlers,
        scheduler_event_handlers: SchedulerEventHandlers,
    ) -> Self {
        Self {
            parent,
            root_scope,
            node_id,
            global_node_id,
            nodes: Vec::new(),
            edges: Vec::new(),
            circuit_event_handlers,
            scheduler_event_handlers,
            store: TypedMap::new(),
        }
    }

    fn add_edge(&mut self, edge: Edge) {
        self.edges.push(edge);
    }

    fn add_node<N>(&mut self, node: N)
    where
        N: Node + 'static,
    {
        self.nodes.push(Box::new(node) as Box<dyn Node>);
    }

    fn clear(&mut self) {
        self.nodes.clear();
        self.edges.clear();
        self.store.clear();
    }

    fn register_circuit_event_handler<F>(&mut self, name: &str, handler: F)
    where
        F: Fn(&CircuitEvent) + 'static,
    {
        self.circuit_event_handlers.borrow_mut().insert(
            name.to_string(),
            Box::new(handler) as Box<dyn Fn(&CircuitEvent)>,
        );
    }

    fn unregister_circuit_event_handler(&mut self, name: &str) -> bool {
        self.circuit_event_handlers
            .borrow_mut()
            .remove(name)
            .is_some()
    }

    fn register_scheduler_event_handler<F>(&mut self, name: &str, handler: F)
    where
        F: FnMut(&SchedulerEvent<'_>) + 'static,
    {
        self.scheduler_event_handlers.borrow_mut().insert(
            name.to_string(),
            Box::new(handler) as Box<dyn FnMut(&SchedulerEvent<'_>)>,
        );
    }

    fn unregister_scheduler_event_handler(&mut self, name: &str) -> bool {
        self.scheduler_event_handlers
            .borrow_mut()
            .remove(name)
            .is_some()
    }

    fn log_circuit_event(&self, event: &CircuitEvent) {
        for (_, handler) in self.circuit_event_handlers.borrow().iter() {
            handler(event)
        }
    }

    fn log_scheduler_event(&self, event: &SchedulerEvent<'_>) {
        for (_, handler) in self.scheduler_event_handlers.borrow_mut().iter_mut() {
            handler(event)
        }
    }

    fn fixedpoint(&self, scope: Scope) -> bool {
        self.nodes.iter().all(|node| {
            node.fixedpoint(scope)
            /*if !res {
                eprintln!("node {} ({})", node.global_id(), node.name());
            }*/
        })
    }
}

/// A circuit.
///
/// A single implementation that can operate as the top-level
/// circuit when instantiated with `P = ()` or a nested circuit,
/// with `P = ChildCircuit<..>` designating the parent circuit type.
pub struct ChildCircuit<P>
where
    P: WithClock,
{
    inner: Rc<RefCell<CircuitInner<P>>>,
    time: Rc<RefCell<<P::Time as Timestamp>::Nested>>,
}

/// Top-level circuit.
///
/// `RootCircuit` is a specialization of [`ChildCircuit<P>`] with `P = ()`.  It
/// forms the top level of a possibly nested DBSP circuit.  Every use of DBSP
/// needs a top-level circuit and non-recursive queries, including all of
/// standard SQL, only needs a top-level circuit.
///
/// Input enters a circuit through the top level circuit only.  `RootCircuit`
/// has `add_input_*` methods for setting up input operators, which can only be
/// called within the callback passed to `RootCircuit::build`.  The data from
/// the input operators is represented as a [`Stream`], which may be in turn be
/// used as input for further operators, which are primarily instantiated via
/// methods on [`Stream`].  Stream output may be made available outside the
/// bounds of a circuit using [`Stream::output`].
pub type RootCircuit = ChildCircuit<()>;

impl<P> Clone for ChildCircuit<P>
where
    P: WithClock,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            time: self.time.clone(),
        }
    }
}

impl<P> ChildCircuit<P>
where
    P: WithClock,
{
    /// Mutably borrow the inner circuit.
    fn inner_mut(&self) -> RefMut<'_, CircuitInner<P>> {
        self.inner.borrow_mut()
    }

    /// Immutably borrow the inner circuit.
    fn inner(&self) -> Ref<'_, CircuitInner<P>> {
        self.inner.borrow()
    }
}

impl RootCircuit {
    /// Creates a circuit and prepares it for execution by calling
    /// `constructor`.  The constructor should create input operators by calling
    /// [`RootCircuit::dyn_add_input_zset`] and related methods.  Each of these
    /// calls returns an input handle and a [`Stream`].  The `constructor` can
    /// call [`Stream`] methods to do computation, each of which yields further
    /// [`Stream`]s.  It can also use [`Stream::output`] to obtain an output
    /// handle.
    ///
    /// Returns a [`CircuitHandle`] with which the caller can control the
    /// circuit, plus a user-defined value returned by the constructor.  The
    /// `constructor` should use the latter to return the input and output
    /// handles it obtains, because these allow the caller to feed input into
    /// the circuit and read output from the circuit.
    ///
    /// The default scheduler, currently [`DynamicScheduler`], will decide the
    /// order in which to evaluate operators.  (This scheduler does not schedule
    /// processes or threads.)
    ///
    /// A client may use the returned [`CircuitHandle`] to run the circuit in
    /// the context of the current thread.  To instead run the circuit in a
    /// collection of worker threads, call [`Runtime::init_circuit`]
    /// instead.
    ///
    /// # Example
    ///
    /// ```
    /// use dbsp::{
    ///     operator::{Generator, Inspect},
    ///     Circuit, RootCircuit,
    /// };
    ///
    /// let circuit = RootCircuit::build(|circuit| {
    ///     // Add a source operator.
    ///     let source_stream = circuit.add_source(Generator::new(|| "Hello, world!".to_owned()));
    ///
    ///     // Add a unary operator and wire the source directly to it.
    ///     circuit.add_unary_operator(
    ///         Inspect::new(|n| println!("New output: {}", n)),
    ///         &source_stream,
    ///     );
    ///     Ok(())
    /// });
    /// ```
    pub fn build<F, T>(constructor: F) -> Result<(CircuitHandle, T), DBSPError>
    where
        F: FnOnce(&mut RootCircuit) -> Result<T, AnyError>,
    {
        Self::build_with_scheduler::<F, T, DynamicScheduler>(constructor)
    }

    /// Create a circuit and prepare it for execution.
    ///
    /// Similar to [`build`](`Self::build`), but with a user-specified
    /// [`Scheduler`] implementation that decides the order in which to evaluate
    /// operators.  (This scheduler does not schedule processes or threads.)
    pub fn build_with_scheduler<F, T, S>(constructor: F) -> Result<(CircuitHandle, T), DBSPError>
    where
        F: FnOnce(&mut RootCircuit) -> Result<T, AnyError>,
        S: Scheduler + 'static,
    {
        let mut circuit = RootCircuit::new();
        let res = constructor(&mut circuit).map_err(DBSPError::Constructor)?;
        let executor =
            Box::new(<OnceExecutor<S>>::new(&circuit)?) as Box<dyn Executor<RootCircuit>>;

        // Alternatively, `CircuitHandle` should expose `clock_start` and `clock_end`
        // APIs, so that the user can reset the circuit at runtime and start
        // evaluation from clean state without having to rebuild it from
        // scratch.
        circuit.log_scheduler_event(&SchedulerEvent::clock_start());
        circuit.clock_start(0);
        Ok((CircuitHandle { circuit, executor }, res))
    }
}

impl RootCircuit {
    // Create new top-level circuit.  Clients invoke this via the
    // [`RootCircuit::build`] API.
    fn new() -> Self {
        Self {
            inner: Rc::new(RefCell::new(CircuitInner::new(
                (),
                0,
                NodeId::root(),
                GlobalNodeId::root(),
                Rc::new(RefCell::new(HashMap::new())),
                Rc::new(RefCell::new(HashMap::new())),
            ))),
            time: Rc::new(RefCell::new(())),
        }
    }
}

impl RootCircuit {
    /// Attach a circuit event handler to the top-level circuit (see
    /// [`super::trace::CircuitEvent`] for a description of circuit events).
    ///
    /// This method should normally be called inside the closure passed to
    /// [`RootCircuit::build`] before adding any operators to the circuit, so
    /// that the handler gets to observe all nodes, edges, and subcircuits
    /// added to the circuit.
    ///
    /// `name` - user-readable name assigned to the handler.  If a handler with
    /// the same name exists, it will be replaced by the new handler.
    ///
    /// `handler` - user callback invoked on each circuit event (see
    /// [`super::trace::CircuitEvent`]).
    ///
    /// # Examples
    ///
    /// ```text
    /// TODO
    /// ```
    pub fn register_circuit_event_handler<F>(&self, name: &str, handler: F)
    where
        F: Fn(&CircuitEvent) + 'static,
    {
        self.inner_mut()
            .register_circuit_event_handler(name, handler);
    }

    /// Remove a circuit event handler.  Returns `true` if a handler with the
    /// specified name had previously been registered and `false` otherwise.
    pub fn unregister_circuit_event_handler(&self, name: &str) -> bool {
        self.inner_mut().unregister_circuit_event_handler(name)
    }

    /// Attach a scheduler event handler to the top-level circuit (see
    /// [`super::trace::SchedulerEvent`] for a description of scheduler
    /// events).
    ///
    /// This method can be used during circuit construction, inside the closure
    /// provided to [`RootCircuit::build`].  Use
    /// [`CircuitHandle::register_scheduler_event_handler`],
    /// [`CircuitHandle::unregister_scheduler_event_handler`] to manipulate
    /// scheduler callbacks at runtime.
    ///
    /// `name` - user-readable name assigned to the handler.  If a handler with
    /// the same name exists, it will be replaced by the new handler.
    ///
    /// `handler` - user callback invoked on each scheduler event.
    pub fn register_scheduler_event_handler<F>(&self, name: &str, handler: F)
    where
        F: FnMut(&SchedulerEvent<'_>) + 'static,
    {
        self.inner_mut()
            .register_scheduler_event_handler(name, handler);
    }

    /// Remove a scheduler event handler.  Returns `true` if a handler with the
    /// specified name had previously been registered and `false` otherwise.
    pub fn unregister_scheduler_event_handler(&self, name: &str) -> bool {
        self.inner_mut().unregister_scheduler_event_handler(name)
    }
}

impl<P> ChildCircuit<P>
where
    P: Circuit,
{
    /// Create an empty nested circuit of `parent`.
    fn with_parent(parent: P, id: NodeId) -> Self {
        let global_node_id = parent.global_node_id().child(id);
        let circuit_handlers = parent.circuit_event_handlers();
        let sched_handlers = parent.scheduler_event_handlers();
        let root_scope = parent.root_scope() + 1;

        ChildCircuit {
            inner: Rc::new(RefCell::new(CircuitInner::new(
                parent,
                root_scope,
                id,
                global_node_id,
                circuit_handlers,
                sched_handlers,
            ))),
            time: Rc::new(RefCell::new(Timestamp::clock_start())),
        }
    }

    /// `true` if `self` is a subcircuit of `other`.
    pub fn is_child_of(&self, other: &P) -> bool {
        P::ptr_eq(&self.inner().parent, other)
    }
}

// Internal API.
impl<P> ChildCircuit<P>
where
    P: WithClock,
    Self: Circuit,
{
    /// Circuit's node id within the parent circuit.
    fn node_id(&self) -> NodeId {
        self.inner().node_id
    }

    /// Register a dependency between `from` and `to` nodes.  A dependency tells
    /// the scheduler that `from` must be evaluated before `to` in each
    /// clock cycle even though there may not be an edge or a path
    /// connecting them.
    fn add_dependency(&self, from: NodeId, to: NodeId) {
        self.log_circuit_event(&CircuitEvent::dependency(
            self.global_node_id().child(from),
            self.global_node_id().child(to),
        ));

        let origin = self.global_node_id().child(from);
        self.inner_mut().add_edge(Edge {
            from,
            to,
            origin,
            ownership_preference: None,
        });
    }

    /// Add a node to the circuit.
    ///
    /// Allocates a new node id and invokes a user callback to create a new node
    /// instance. The callback may use the node id, e.g., to add an edge to
    /// this node.
    fn add_node<F, N, T>(&self, f: F) -> T
    where
        F: FnOnce(NodeId) -> (N, T),
        N: Node + 'static,
    {
        let id = self.inner().nodes.len();

        // We don't hold a reference to `self.inner()` while calling `f`, so it can
        // safely modify the circuit, e.g., add edges.
        let (node, res) = f(NodeId(id));
        self.inner_mut().add_node(node);
        res
    }

    /// Like `add_node`, but the node is not created if the closure fails.
    fn try_add_node<F, N, T, E>(&self, f: F) -> Result<T, E>
    where
        F: FnOnce(NodeId) -> Result<(N, T), E>,
        N: Node + 'static,
    {
        let id = self.inner().nodes.len();

        // We don't hold a reference to `self.inner()` while calling `f`, so it can
        // safely modify the circuit, e.g., add edges.
        let (node, res) = f(NodeId(id))?;
        self.inner_mut().add_node(node);
        Ok(res)
    }

    /// Recursively apply `f` to all nodes in `self` and its children.
    pub(crate) fn map_nodes_recursive(&self, f: &mut dyn FnMut(&dyn Node)) {
        for node in self.inner().nodes.iter() {
            f(node.as_ref());
            node.map_nodes_recursive(f);
        }
    }

    /// Recursively apply `f` to all nodes in `self` and its children mutably.
    pub(crate) fn map_nodes_recursive_mut(&mut self, f: &mut dyn FnMut(&mut dyn Node)) {
        for node in self.inner_mut().nodes.iter_mut() {
            f(node.as_mut());
            node.map_nodes_recursive_mut(f);
        }
    }

    fn clear(&mut self) {
        self.inner_mut().clear();
    }

    /// Send the specified `CircuitEvent` to all handlers attached to the
    /// circuit.
    fn log_circuit_event(&self, event: &CircuitEvent) {
        self.inner().log_circuit_event(event);
    }

    /// Send the specified `SchedulerEvent` to all handlers attached to the
    /// circuit.
    pub(super) fn log_scheduler_event(&self, event: &SchedulerEvent<'_>) {
        self.inner().log_scheduler_event(event);
    }
}

impl<P> Circuit for ChildCircuit<P>
where
    P: WithClock + Clone + 'static,
{
    type Parent = P;

    fn parent(&self) -> P {
        self.inner().parent.clone()
    }

    fn edges(&self) -> Ref<'_, [Edge]> {
        let circuit = self.inner();
        Ref::map(circuit, |c| c.edges.as_slice())
    }

    fn num_nodes(&self) -> usize {
        self.inner().nodes.len()
    }

    fn global_id(&self) -> Ref<'_, GlobalNodeId> {
        Ref::map(self.inner(), |c| &c.global_node_id)
    }

    /// Returns vector of local node ids in the circuit.
    fn node_ids(&self) -> Vec<NodeId> {
        self.inner()
            .nodes
            .iter()
            .map(|node| node.local_id())
            .collect()
    }

    fn root_scope(&self) -> Scope {
        self.inner().root_scope
    }

    fn node_id(&self) -> NodeId {
        self.inner().node_id
    }

    fn global_node_id(&self) -> GlobalNodeId {
        self.inner().global_node_id.clone()
    }

    fn ptr_eq(this: &Self, other: &Self) -> bool {
        Rc::ptr_eq(&this.inner, &other.inner)
    }

    fn circuit_event_handlers(&self) -> CircuitEventHandlers {
        self.inner().circuit_event_handlers.clone()
    }

    fn scheduler_event_handlers(&self) -> SchedulerEventHandlers {
        self.inner().scheduler_event_handlers.clone()
    }

    fn log_circuit_event(&self, event: &CircuitEvent) {
        self.inner().log_circuit_event(event);
    }

    fn log_scheduler_event(&self, event: &SchedulerEvent<'_>) {
        self.inner().log_scheduler_event(event);
    }

    fn cache_get_or_insert_with<K, F>(&self, key: K, mut f: F) -> RefMut<'_, K::Value>
    where
        K: 'static + TypedMapKey<CircuitStoreMarker>,
        F: FnMut() -> K::Value,
    {
        // Don't use `store.entry()`, since `f` may need to perform
        // its own cache lookup.
        if self.inner().store.contains_key(&key) {
            return RefMut::map(self.inner_mut(), |c| c.store.get_mut(&key).unwrap());
        }

        let new = f();

        // TODO: Use `RefMut::filter_map()` to only perform one lookup in the happy path
        //       https://github.com/rust-lang/rust/issues/81061
        RefMut::map(self.inner_mut(), |c| c.store.entry(key).or_insert(new))
    }

    fn connect_stream<T>(
        &self,
        stream: &Stream<Self, T>,
        to: NodeId,
        ownership_preference: OwnershipPreference,
    ) {
        self.log_circuit_event(&CircuitEvent::stream(
            stream.origin_node_id().clone(),
            self.global_node_id().child(to),
            ownership_preference,
        ));

        // Safe because the circuit isn't running yet.
        unsafe { (*stream.val.get()).consumers += 1 };

        debug_assert_eq!(self.global_node_id(), stream.circuit.global_node_id());
        self.inner_mut().add_edge(Edge {
            from: stream.local_node_id(),
            to,
            origin: stream.origin_node_id().clone(),
            ownership_preference: Some(ownership_preference),
        });
    }

    fn tick(&self) {
        let mut time = self.time.borrow_mut();
        *time = time.advance(0);
    }

    fn clock_start(&self, scope: Scope) {
        for node in self.inner_mut().nodes.iter_mut() {
            node.clock_start(scope);
        }
    }

    fn clock_end(&self, scope: Scope) {
        for node in self.inner_mut().nodes.iter_mut() {
            unsafe {
                node.clock_end(scope);
            }
        }

        let mut time = self.time.borrow_mut();
        *time = time.advance(scope + 1);
    }

    fn ready(&self, id: NodeId) -> bool {
        self.inner().nodes[id.0].ready()
    }

    fn cache_insert<K>(&self, key: K, val: K::Value)
    where
        K: TypedMapKey<CircuitStoreMarker> + 'static,
    {
        self.inner_mut().store.insert(key, val);
    }

    fn cache_contains<K>(&self, key: &K) -> bool
    where
        K: TypedMapKey<CircuitStoreMarker> + 'static,
    {
        self.inner_mut().store.contains_key(key)
    }

    fn cache_get<K>(&self, key: &K) -> Option<K::Value>
    where
        K: TypedMapKey<CircuitStoreMarker> + 'static,
        K::Value: Clone,
    {
        self.inner_mut().store.get(key).cloned()
    }

    fn register_ready_callback(&self, id: NodeId, cb: Box<dyn Fn() + Send + Sync>) {
        self.inner_mut().nodes[id.0].register_ready_callback(cb);
    }

    fn is_async_node(&self, id: NodeId) -> bool {
        self.inner().nodes[id.0].is_async()
    }

    fn eval_node(&self, id: NodeId) -> Result<(), SchedulerError> {
        let mut circuit = self.inner_mut();
        debug_assert!(id.0 < circuit.nodes.len());

        // Notify loggers while holding a reference to the inner circuit.
        // We normally avoid this, since a nested call from event handler
        // will panic in `self.inner()`, but we do it here as an
        // optimization.
        circuit.log_scheduler_event(&SchedulerEvent::eval_start(circuit.nodes[id.0].as_ref()));

        // Safety: `eval` cannot invoke the
        // `eval` method of another node.  To circumvent
        // this invariant the user would have to extract a
        // reference to a node and pass it to an operator,
        // but this module doesn't expose nodes, only
        // streams.
        unsafe { circuit.nodes[id.0].eval()? };

        circuit.log_scheduler_event(&SchedulerEvent::eval_end(circuit.nodes[id.0].as_ref()));

        Ok(())
    }

    #[track_caller]
    fn region<F, T>(&self, name: &str, f: F) -> T
    where
        F: FnOnce() -> T,
    {
        self.log_circuit_event(&CircuitEvent::push_region(name, Some(Location::caller())));
        let res = f();
        self.log_circuit_event(&CircuitEvent::pop_region());
        res
    }

    /// Add a source operator to the circuit.  See [`SourceOperator`].
    fn add_source<O, Op>(&self, operator: Op) -> Stream<Self, O>
    where
        O: Data,
        Op: SourceOperator<O>,
    {
        self.add_node(|id| {
            self.log_circuit_event(&CircuitEvent::operator(
                GlobalNodeId::child_of(self, id),
                operator.name(),
                operator.location(),
            ));

            let node = SourceNode::new(operator, self.clone(), id);
            let output_stream = node.output_stream();
            (node, output_stream)
        })
    }

    fn add_exchange<I, SndOp, O, RcvOp>(
        &self,
        sender: SndOp,
        receiver: RcvOp,
        input_stream: &Stream<Self, I>,
    ) -> Stream<Self, O>
    where
        I: Data,
        O: Data,
        SndOp: SinkOperator<I>,
        RcvOp: SourceOperator<O>,
    {
        let preference = sender.input_preference();
        self.add_exchange_with_preference(sender, receiver, input_stream, preference)
    }

    fn add_exchange_with_preference<I, SndOp, O, RcvOp>(
        &self,
        sender: SndOp,
        receiver: RcvOp,
        input_stream: &Stream<Self, I>,
        input_preference: OwnershipPreference,
    ) -> Stream<Self, O>
    where
        I: Data,
        O: Data,
        SndOp: SinkOperator<I>,
        RcvOp: SourceOperator<O>,
    {
        let sender_id = self.add_node(|id| {
            self.log_circuit_event(&CircuitEvent::operator(
                GlobalNodeId::child_of(self, id),
                sender.name(),
                sender.location(),
            ));

            let node = SinkNode::new(sender, input_stream.clone(), self.clone(), id);
            self.connect_stream(input_stream, id, input_preference);
            (node, id)
        });

        let output_stream = self.add_node(|id| {
            self.log_circuit_event(&CircuitEvent::operator(
                GlobalNodeId::child_of(self, id),
                receiver.name(),
                receiver.location(),
            ));

            let node = SourceNode::new(receiver, self.clone(), id);
            let output_stream = node.output_stream();
            (node, output_stream)
        });

        self.add_dependency(sender_id, output_stream.local_node_id());
        output_stream
    }

    fn add_sink<I, Op>(&self, operator: Op, input_stream: &Stream<Self, I>)
    where
        I: Data,
        Op: SinkOperator<I>,
    {
        let preference = operator.input_preference();
        self.add_sink_with_preference(operator, input_stream, preference)
    }

    fn add_sink_with_preference<I, Op>(
        &self,
        operator: Op,
        input_stream: &Stream<Self, I>,
        input_preference: OwnershipPreference,
    ) where
        I: Data,
        Op: SinkOperator<I>,
    {
        self.add_node(|id| {
            // Log the operator event before the connection event, so that handlers
            // don't observe edges that connect to nodes they haven't seen yet.
            self.log_circuit_event(&CircuitEvent::operator(
                GlobalNodeId::child_of(self, id),
                operator.name(),
                operator.location(),
            ));

            self.connect_stream(input_stream, id, input_preference);
            (
                SinkNode::new(operator, input_stream.clone(), self.clone(), id),
                (),
            )
        });
    }

    /// Add a binary sink operator (see [`BinarySinkOperator`]).
    fn add_binary_sink<I1, I2, Op>(
        &self,
        operator: Op,
        input_stream1: &Stream<Self, I1>,
        input_stream2: &Stream<Self, I2>,
    ) where
        I1: Data,
        I2: Data,
        Op: BinarySinkOperator<I1, I2>,
    {
        let (preference1, preference2) = operator.input_preference();
        self.add_binary_sink_with_preference(
            operator,
            (input_stream1, preference1),
            (input_stream2, preference2),
        )
    }

    fn add_binary_sink_with_preference<I1, I2, Op>(
        &self,
        operator: Op,
        input_stream1: (&Stream<Self, I1>, OwnershipPreference),
        input_stream2: (&Stream<Self, I2>, OwnershipPreference),
    ) where
        I1: Data,
        I2: Data,
        Op: BinarySinkOperator<I1, I2>,
    {
        let (input_stream1, input_preference1) = input_stream1;
        let (input_stream2, input_preference2) = input_stream2;

        self.add_node(|id| {
            self.log_circuit_event(&CircuitEvent::operator(
                GlobalNodeId::child_of(self, id),
                operator.name(),
                operator.location(),
            ));

            let node = BinarySinkNode::new(
                operator,
                input_stream1.clone(),
                input_stream2.clone(),
                self.clone(),
                id,
            );
            self.connect_stream(input_stream1, id, input_preference1);
            self.connect_stream(input_stream2, id, input_preference2);
            (node, ())
        });
    }

    fn add_unary_operator<I, O, Op>(
        &self,
        operator: Op,
        input_stream: &Stream<Self, I>,
    ) -> Stream<Self, O>
    where
        I: Data,
        O: Data,
        Op: UnaryOperator<I, O>,
    {
        let preference = operator.input_preference();
        self.add_unary_operator_with_preference(operator, input_stream, preference)
    }

    fn add_unary_operator_with_preference<I, O, Op>(
        &self,
        operator: Op,
        input_stream: &Stream<Self, I>,
        input_preference: OwnershipPreference,
    ) -> Stream<Self, O>
    where
        I: Data,
        O: Data,
        Op: UnaryOperator<I, O>,
    {
        self.add_node(|id| {
            self.log_circuit_event(&CircuitEvent::operator(
                GlobalNodeId::child_of(self, id),
                operator.name(),
                operator.location(),
            ));

            let node = UnaryNode::new(operator, input_stream.clone(), self.clone(), id);
            let output_stream = node.output_stream();
            self.connect_stream(input_stream, id, input_preference);
            (node, output_stream)
        })
    }

    fn add_binary_operator<I1, I2, O, Op>(
        &self,
        operator: Op,
        input_stream1: &Stream<Self, I1>,
        input_stream2: &Stream<Self, I2>,
    ) -> Stream<Self, O>
    where
        I1: Data,
        I2: Data,
        O: Data,
        Op: BinaryOperator<I1, I2, O>,
    {
        let (pref1, pref2) = operator.input_preference();
        self.add_binary_operator_with_preference(
            operator,
            (input_stream1, pref1),
            (input_stream2, pref2),
        )
    }

    fn add_binary_operator_with_preference<I1, I2, O, Op>(
        &self,
        operator: Op,
        input_stream1: (&Stream<Self, I1>, OwnershipPreference),
        input_stream2: (&Stream<Self, I2>, OwnershipPreference),
    ) -> Stream<Self, O>
    where
        I1: Data,
        I2: Data,
        O: Data,
        Op: BinaryOperator<I1, I2, O>,
    {
        let (input_stream1, input_preference1) = input_stream1;
        let (input_stream2, input_preference2) = input_stream2;

        self.add_node(|id| {
            self.log_circuit_event(&CircuitEvent::operator(
                GlobalNodeId::child_of(self, id),
                operator.name(),
                operator.location(),
            ));

            let node = BinaryNode::new(
                operator,
                input_stream1.clone(),
                input_stream2.clone(),
                self.clone(),
                id,
            );
            let output_stream = node.output_stream();
            self.connect_stream(input_stream1, id, input_preference1);
            self.connect_stream(input_stream2, id, input_preference2);
            (node, output_stream)
        })
    }

    fn add_ternary_operator<I1, I2, I3, O, Op>(
        &self,
        operator: Op,
        input_stream1: &Stream<Self, I1>,
        input_stream2: &Stream<Self, I2>,
        input_stream3: &Stream<Self, I3>,
    ) -> Stream<Self, O>
    where
        I1: Data,
        I2: Data,
        I3: Data,
        O: Data,
        Op: TernaryOperator<I1, I2, I3, O>,
    {
        let (pref1, pref2, pref3) = operator.input_preference();
        self.add_ternary_operator_with_preference(
            operator,
            (input_stream1, pref1),
            (input_stream2, pref2),
            (input_stream3, pref3),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn add_ternary_operator_with_preference<I1, I2, I3, O, Op>(
        &self,
        operator: Op,
        input_stream1: (&Stream<Self, I1>, OwnershipPreference),
        input_stream2: (&Stream<Self, I2>, OwnershipPreference),
        input_stream3: (&Stream<Self, I3>, OwnershipPreference),
    ) -> Stream<Self, O>
    where
        I1: Data,
        I2: Data,
        I3: Data,
        O: Data,
        Op: TernaryOperator<I1, I2, I3, O>,
    {
        let (input_stream1, input_preference1) = input_stream1;
        let (input_stream2, input_preference2) = input_stream2;
        let (input_stream3, input_preference3) = input_stream3;

        self.add_node(|id| {
            self.log_circuit_event(&CircuitEvent::operator(
                GlobalNodeId::child_of(self, id),
                operator.name(),
                operator.location(),
            ));

            let node = TernaryNode::new(
                operator,
                input_stream1.clone(),
                input_stream2.clone(),
                input_stream3.clone(),
                self.clone(),
                id,
            );
            let output_stream = node.output_stream();
            self.connect_stream(input_stream1, id, input_preference1);
            self.connect_stream(input_stream2, id, input_preference2);
            self.connect_stream(input_stream3, id, input_preference3);
            (node, output_stream)
        })
    }

    fn add_quaternary_operator<I1, I2, I3, I4, O, Op>(
        &self,
        operator: Op,
        input_stream1: &Stream<Self, I1>,
        input_stream2: &Stream<Self, I2>,
        input_stream3: &Stream<Self, I3>,
        input_stream4: &Stream<Self, I4>,
    ) -> Stream<Self, O>
    where
        I1: Data,
        I2: Data,
        I3: Data,
        I4: Data,
        O: Data,
        Op: QuaternaryOperator<I1, I2, I3, I4, O>,
    {
        let (pref1, pref2, pref3, pref4) = operator.input_preference();
        self.add_quaternary_operator_with_preference(
            operator,
            (input_stream1, pref1),
            (input_stream2, pref2),
            (input_stream3, pref3),
            (input_stream4, pref4),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn add_quaternary_operator_with_preference<I1, I2, I3, I4, O, Op>(
        &self,
        operator: Op,
        input_stream1: (&Stream<Self, I1>, OwnershipPreference),
        input_stream2: (&Stream<Self, I2>, OwnershipPreference),
        input_stream3: (&Stream<Self, I3>, OwnershipPreference),
        input_stream4: (&Stream<Self, I4>, OwnershipPreference),
    ) -> Stream<Self, O>
    where
        I1: Data,
        I2: Data,
        I3: Data,
        I4: Data,
        O: Data,
        Op: QuaternaryOperator<I1, I2, I3, I4, O>,
    {
        let (input_stream1, input_preference1) = input_stream1;
        let (input_stream2, input_preference2) = input_stream2;
        let (input_stream3, input_preference3) = input_stream3;
        let (input_stream4, input_preference4) = input_stream4;

        self.add_node(|id| {
            self.log_circuit_event(&CircuitEvent::operator(
                GlobalNodeId::child_of(self, id),
                operator.name(),
                operator.location(),
            ));

            let node = QuaternaryNode::new(
                operator,
                input_stream1.clone(),
                input_stream2.clone(),
                input_stream3.clone(),
                input_stream4.clone(),
                self.clone(),
                id,
            );
            let output_stream = node.output_stream();
            self.connect_stream(input_stream1, id, input_preference1);
            self.connect_stream(input_stream2, id, input_preference2);
            self.connect_stream(input_stream3, id, input_preference3);
            self.connect_stream(input_stream4, id, input_preference4);
            (node, output_stream)
        })
    }

    fn add_nary_operator<'a, I, O, Op, Iter>(
        &'a self,
        operator: Op,
        input_streams: Iter,
    ) -> Stream<Self, O>
    where
        I: Data,
        O: Data,
        Op: NaryOperator<I, O>,
        Iter: IntoIterator<Item = &'a Stream<Self, I>>,
    {
        let pref = operator.input_preference();
        self.add_nary_operator_with_preference(operator, input_streams, pref)
    }

    fn add_nary_operator_with_preference<'a, I, O, Op, Iter>(
        &'a self,
        operator: Op,
        input_streams: Iter,
        input_preference: OwnershipPreference,
    ) -> Stream<Self, O>
    where
        I: Data,
        O: Data,
        Op: NaryOperator<I, O>,
        Iter: IntoIterator<Item = &'a Stream<Self, I>>,
    {
        let input_streams: Vec<Stream<_, _>> = input_streams.into_iter().cloned().collect();
        self.add_node(|id| {
            self.log_circuit_event(&CircuitEvent::operator(
                GlobalNodeId::child_of(self, id),
                operator.name(),
                operator.location(),
            ));

            let node = NaryNode::new(operator, input_streams.clone(), self.clone(), id);
            let output_stream = node.output_stream();
            for stream in input_streams.iter() {
                self.connect_stream(stream, id, input_preference);
            }
            (node, output_stream)
        })
    }

    fn add_feedback<I, O, Op>(
        &self,
        operator: Op,
    ) -> (Stream<Self, O>, FeedbackConnector<Self, I, O, Op>)
    where
        I: Data,
        O: Data,
        Op: StrictUnaryOperator<I, O>,
    {
        self.add_node(|id| {
            self.log_circuit_event(&CircuitEvent::strict_operator_output(
                GlobalNodeId::child_of(self, id),
                operator.name(),
                operator.location(),
            ));

            let operator = Rc::new(UnsafeCell::new(operator));
            let connector = FeedbackConnector::new(id, self.clone(), operator.clone());
            let output_node = FeedbackOutputNode::new(operator, self.clone(), id);
            let local = output_node.output_stream();
            (output_node, (local, connector))
        })
    }

    fn add_feedback_with_export<I, O, Op>(
        &self,
        operator: Op,
    ) -> (ExportStream<Self, O>, FeedbackConnector<Self, I, O, Op>)
    where
        I: Data,
        O: Data,
        Op: StrictUnaryOperator<I, O>,
    {
        self.add_node(|id| {
            self.log_circuit_event(&CircuitEvent::strict_operator_output(
                GlobalNodeId::child_of(self, id),
                operator.name(),
                operator.location(),
            ));

            let operator = Rc::new(UnsafeCell::new(operator));
            let connector = FeedbackConnector::new(id, self.clone(), operator.clone());
            let output_node = FeedbackOutputNode::with_export(operator, self.clone(), id);
            let local = output_node.output_stream();
            let export = output_node.export_stream.clone().unwrap();
            (output_node, (ExportStream { local, export }, connector))
        })
    }

    fn connect_feedback_with_preference<I, O, Op>(
        &self,
        output_node_id: NodeId,
        operator: Rc<UnsafeCell<Op>>,
        input_stream: &Stream<Self, I>,
        input_preference: OwnershipPreference,
    ) where
        I: Data,
        O: Data,
        Op: StrictUnaryOperator<I, O>,
    {
        self.add_node(|id| {
            self.log_circuit_event(&CircuitEvent::strict_operator_input(
                GlobalNodeId::child_of(self, id),
                output_node_id,
            ));

            let output_node = FeedbackInputNode::new(operator, input_stream.clone(), id);
            self.connect_stream(input_stream, id, input_preference);
            self.add_dependency(output_node_id, id);
            (output_node, ())
        });
    }

    fn subcircuit<F, T, E>(
        &self,
        iterative: bool,
        child_constructor: F,
    ) -> Result<T, SchedulerError>
    where
        F: FnOnce(&mut ChildCircuit<Self>) -> Result<(T, E), SchedulerError>,
        E: Executor<ChildCircuit<Self>>,
    {
        self.try_add_node(|id| {
            let global_id = GlobalNodeId::child_of(self, id);
            self.log_circuit_event(&CircuitEvent::subcircuit(global_id.clone(), iterative));
            let mut child_circuit = ChildCircuit::with_parent(self.clone(), id);
            let (res, executor) = child_constructor(&mut child_circuit)?;
            let child = <ChildNode<Self>>::new::<E>(child_circuit, executor);
            self.log_circuit_event(&CircuitEvent::subcircuit_complete(global_id));
            Ok((child, res))
        })
    }

    fn iterate<F, C, T>(&self, constructor: F) -> Result<T, SchedulerError>
    where
        F: FnOnce(&mut ChildCircuit<Self>) -> Result<(C, T), SchedulerError>,
        C: Fn() -> Result<bool, SchedulerError> + 'static,
    {
        self.iterate_with_scheduler::<F, C, T, DynamicScheduler>(constructor)
    }

    /// Add an iteratively scheduled child circuit.
    ///
    /// Similar to [`iterate`](`Self::iterate`), but with a user-specified
    /// [`Scheduler`] implementation.
    fn iterate_with_scheduler<F, C, T, S>(&self, constructor: F) -> Result<T, SchedulerError>
    where
        F: FnOnce(&mut ChildCircuit<Self>) -> Result<(C, T), SchedulerError>,
        C: Fn() -> Result<bool, SchedulerError> + 'static,
        S: Scheduler + 'static,
    {
        self.subcircuit(true, |child| {
            let (termination_check, res) = constructor(child)?;
            let executor = <IterativeExecutor<_, S>>::new(child, termination_check)?;
            Ok((res, executor))
        })
    }

    fn fixedpoint<F, T>(&self, constructor: F) -> Result<T, SchedulerError>
    where
        F: FnOnce(&mut ChildCircuit<Self>) -> Result<T, SchedulerError>,
    {
        self.fixedpoint_with_scheduler::<F, T, DynamicScheduler>(constructor)
    }

    fn fixedpoint_with_scheduler<F, T, S>(&self, constructor: F) -> Result<T, SchedulerError>
    where
        F: FnOnce(&mut ChildCircuit<Self>) -> Result<T, SchedulerError>,
        S: Scheduler + 'static,
    {
        match Runtime::runtime() {
            // In a multithreaded environment the fixedpoint check cannot be performed locally.
            // The circuit must iterate until all peers have reached a fixed point.
            Some(runtime) if runtime.num_workers() > 1 => {
                self.subcircuit(true, |child| {
                    let res = constructor(child)?;
                    let child_clone = child.clone();

                    // Create an `Exchange` object that will be used to exchange the fixed point
                    // status with peers.
                    let worker_index = Runtime::worker_index();
                    let exchange_id = runtime.sequence_next(worker_index);
                    let exchange = Exchange::with_runtime(&runtime, exchange_id);

                    let thread = std::thread::current();
                    exchange.register_sender_callback(worker_index, move || thread.unpark());

                    let thread = std::thread::current();
                    exchange.register_receiver_callback(worker_index, move || thread.unpark());

                    let termination_check = move || {
                        // Send local fixed point status to all peers.
                        let local_fixedpoint = child_clone.inner().fixedpoint(0);
                        while !exchange.try_send_all(worker_index, &mut repeat(local_fixedpoint)) {
                            if Runtime::kill_in_progress() {
                                return Err(SchedulerError::Killed);
                            }
                            std::thread::park();
                        }
                        // Receive the fixed point status of each peer, compute global fixedpoint
                        // state as a logical and of all peer states.
                        let mut global_fixedpoint = true;
                        while !exchange.try_receive_all(worker_index, |fp| global_fixedpoint &= fp)
                        {
                            if Runtime::kill_in_progress() {
                                return Err(SchedulerError::Killed);
                            }
                            // Sleep if other threads are still working.
                            std::thread::park();
                        }
                        Ok(global_fixedpoint)
                    };
                    let executor = <IterativeExecutor<_, S>>::new(child, termination_check)?;
                    Ok((res, executor))
                })
            }
            _ => self.subcircuit(true, |child| {
                let res = constructor(child)?;
                let child_clone = child.clone();

                let termination_check = move || Ok(child_clone.inner().fixedpoint(0));
                let executor = <IterativeExecutor<_, S>>::new(child, termination_check)?;
                Ok((res, executor))
            }),
        }
    }

    fn import_stream<I, O, Op>(&self, operator: Op, parent_stream: &Stream<P, I>) -> Stream<Self, O>
    where
        Self::Parent: Circuit,
        I: Data,
        O: Data,
        Op: ImportOperator<I, O>,
    {
        let preference = operator.input_preference();
        self.import_stream_with_preference(operator, parent_stream, preference)
    }

    fn import_stream_with_preference<I, O, Op>(
        &self,
        operator: Op,
        parent_stream: &Stream<P, I>,
        input_preference: OwnershipPreference,
    ) -> Stream<Self, O>
    where
        Self::Parent: Circuit,
        I: Data,
        O: Data,
        Op: ImportOperator<I, O>,
    {
        assert!(self.is_child_of(parent_stream.circuit()));

        self.add_node(|id| {
            self.log_circuit_event(&CircuitEvent::operator(
                self.global_node_id().child(id),
                operator.name(),
                operator.location(),
            ));
            let node = ImportNode::new(operator, self.clone(), parent_stream.clone(), id);
            self.parent()
                .connect_stream(parent_stream, self.node_id(), input_preference);
            let output_stream = node.output_stream();
            (node, output_stream)
        })
    }
}

impl<P> ChildCircuit<P>
where
    P: Circuit,
{
    /// Make the contents of `parent_stream` available in the nested circuit
    /// via an [`ImportOperator`].
    ///
    /// Typically invoked via a convenience wrapper, e.g., [`Stream::delta0`].
    pub fn import_stream<I, O, Op>(
        &self,
        operator: Op,
        parent_stream: &Stream<P, I>,
    ) -> Stream<Self, O>
    where
        I: Data,
        O: Data,
        Op: ImportOperator<I, O>,
    {
        let preference = operator.input_preference();
        self.import_stream_with_preference(operator, parent_stream, preference)
    }

    /// Like [`Self::import_stream`] but overrides the ownership
    /// preference on the input stream with `input_preference.
    pub fn import_stream_with_preference<I, O, Op>(
        &self,
        operator: Op,
        parent_stream: &Stream<P, I>,
        input_preference: OwnershipPreference,
    ) -> Stream<Self, O>
    where
        I: Data,
        O: Data,
        Op: ImportOperator<I, O>,
    {
        assert!(self.is_child_of(parent_stream.circuit()));

        self.add_node(|id| {
            self.log_circuit_event(&CircuitEvent::operator(
                self.global_node_id().child(id),
                operator.name(),
                operator.location(),
            ));
            let node = ImportNode::new(operator, self.clone(), parent_stream.clone(), id);
            self.parent()
                .connect_stream(parent_stream, self.node_id(), input_preference);
            let output_stream = node.output_stream();
            (node, output_stream)
        })
    }
}

struct ImportNode<C, I, O, Op>
where
    C: Circuit,
{
    id: GlobalNodeId,
    operator: Op,
    parent_stream: Stream<C::Parent, I>,
    output_stream: Stream<C, O>,
}

impl<C, I, O, Op> ImportNode<C, I, O, Op>
where
    C: Circuit,
    C::Parent: Circuit,
{
    fn new(operator: Op, circuit: C, parent_stream: Stream<C::Parent, I>, id: NodeId) -> Self {
        assert!(Circuit::ptr_eq(&circuit.parent(), parent_stream.circuit()));

        Self {
            id: circuit.global_node_id().child(id),
            operator,
            parent_stream,
            output_stream: Stream::new(circuit, id),
        }
    }

    fn output_stream(&self) -> Stream<C, O> {
        self.output_stream.clone()
    }
}

impl<C, I, O, Op> Node for ImportNode<C, I, O, Op>
where
    C: Circuit,
    C::Parent: Circuit,
    I: Clone,
    O: Clone,
    Op: ImportOperator<I, O>,
{
    fn name(&self) -> Cow<'static, str> {
        self.operator.name()
    }

    fn local_id(&self) -> NodeId {
        self.id.local_node_id().unwrap()
    }

    fn global_id(&self) -> &GlobalNodeId {
        &self.id
    }

    fn is_async(&self) -> bool {
        self.operator.is_async()
    }

    fn ready(&self) -> bool {
        self.operator.ready()
    }

    fn register_ready_callback(&mut self, cb: Box<dyn Fn() + Send + Sync>) {
        self.operator.register_ready_callback(cb);
    }

    unsafe fn eval(&mut self) -> Result<(), SchedulerError> {
        self.output_stream.put(self.operator.eval());
        Ok(())
    }

    fn clock_start(&mut self, scope: Scope) {
        self.operator.clock_start(scope);
        if scope == 0 {
            match unsafe { self.parent_stream.take() } {
                Cow::Borrowed(val) => self.operator.import(val),
                Cow::Owned(val) => self.operator.import_owned(val),
            }
        }
    }

    unsafe fn clock_end(&mut self, scope: Scope) {
        self.operator.clock_end(scope);
    }

    fn metadata(&self, output: &mut OperatorMeta) {
        self.operator.metadata(output);
    }

    fn fixedpoint(&self, scope: Scope) -> bool {
        self.operator.fixedpoint(scope)
    }

    fn commit(&mut self, cid: Uuid) -> Result<(), DBSPError> {
        self.operator.commit(cid, self.global_id().persistent_id())
    }

    fn restore(&mut self, cid: Uuid) -> Result<(), DBSPError> {
        self.operator.restore(cid, self.global_id().persistent_id())
    }
}

struct SourceNode<C, O, Op> {
    id: GlobalNodeId,
    operator: Op,
    output_stream: Stream<C, O>,
}

impl<C, O, Op> SourceNode<C, O, Op>
where
    Op: SourceOperator<O>,
    C: Circuit,
{
    fn new(operator: Op, circuit: C, id: NodeId) -> Self {
        Self {
            id: circuit.global_node_id().child(id),
            operator,
            output_stream: Stream::new(circuit, id),
        }
    }

    fn output_stream(&self) -> Stream<C, O> {
        self.output_stream.clone()
    }
}

impl<C, O, Op> Node for SourceNode<C, O, Op>
where
    C: Circuit,
    O: Clone,
    Op: SourceOperator<O>,
{
    fn name(&self) -> Cow<'static, str> {
        self.operator.name()
    }

    fn local_id(&self) -> NodeId {
        self.id.local_node_id().unwrap()
    }

    fn global_id(&self) -> &GlobalNodeId {
        &self.id
    }

    fn is_async(&self) -> bool {
        self.operator.is_async()
    }

    fn ready(&self) -> bool {
        self.operator.ready()
    }

    fn register_ready_callback(&mut self, cb: Box<dyn Fn() + Send + Sync>) {
        self.operator.register_ready_callback(cb);
    }

    unsafe fn eval(&mut self) -> Result<(), SchedulerError> {
        self.output_stream.put(self.operator.eval());
        Ok(())
    }

    fn clock_start(&mut self, scope: Scope) {
        self.operator.clock_start(scope);
    }

    unsafe fn clock_end(&mut self, scope: Scope) {
        self.operator.clock_end(scope);
    }

    fn metadata(&self, output: &mut OperatorMeta) {
        self.operator.metadata(output);
    }

    fn fixedpoint(&self, scope: Scope) -> bool {
        self.operator.fixedpoint(scope)
    }

    fn commit(&mut self, cid: Uuid) -> Result<(), DBSPError> {
        self.operator.commit(cid, self.global_id().persistent_id())
    }

    fn restore(&mut self, cid: Uuid) -> Result<(), DBSPError> {
        self.operator.restore(cid, self.global_id().persistent_id())
    }
}

struct UnaryNode<C, I, O, Op> {
    id: GlobalNodeId,
    operator: Op,
    input_stream: Stream<C, I>,
    output_stream: Stream<C, O>,
}

impl<C, I, O, Op> UnaryNode<C, I, O, Op>
where
    Op: UnaryOperator<I, O>,
    C: Circuit,
{
    fn new(operator: Op, input_stream: Stream<C, I>, circuit: C, id: NodeId) -> Self {
        Self {
            id: circuit.global_node_id().child(id),
            operator,
            input_stream,
            output_stream: Stream::new(circuit, id),
        }
    }

    fn output_stream(&self) -> Stream<C, O> {
        self.output_stream.clone()
    }
}

impl<C, I, O, Op> Node for UnaryNode<C, I, O, Op>
where
    C: Circuit,
    I: Clone,
    O: Clone,
    Op: UnaryOperator<I, O>,
{
    fn name(&self) -> Cow<'static, str> {
        self.operator.name()
    }

    fn local_id(&self) -> NodeId {
        self.id.local_node_id().unwrap()
    }

    fn global_id(&self) -> &GlobalNodeId {
        &self.id
    }

    fn is_async(&self) -> bool {
        self.operator.is_async()
    }

    fn ready(&self) -> bool {
        self.operator.ready()
    }

    fn register_ready_callback(&mut self, cb: Box<dyn Fn() + Send + Sync>) {
        self.operator.register_ready_callback(cb);
    }

    unsafe fn eval(&mut self) -> Result<(), SchedulerError> {
        self.output_stream.put(match self.input_stream.take() {
            Cow::Owned(v) => self.operator.eval_owned(v),
            Cow::Borrowed(v) => self.operator.eval(v),
        });
        Ok(())
    }

    fn clock_start(&mut self, scope: Scope) {
        self.operator.clock_start(scope);
    }

    unsafe fn clock_end(&mut self, scope: Scope) {
        self.operator.clock_end(scope);
    }

    fn metadata(&self, output: &mut OperatorMeta) {
        self.operator.metadata(output);
    }

    fn fixedpoint(&self, scope: Scope) -> bool {
        self.operator.fixedpoint(scope)
    }

    fn commit(&mut self, cid: Uuid) -> Result<(), DBSPError> {
        self.operator.commit(cid, self.global_id().persistent_id())
    }

    fn restore(&mut self, cid: Uuid) -> Result<(), DBSPError> {
        self.operator.restore(cid, self.global_id().persistent_id())
    }
}

struct SinkNode<C, I, Op> {
    id: GlobalNodeId,
    operator: Op,
    input_stream: Stream<C, I>,
}

impl<C, I, Op> SinkNode<C, I, Op>
where
    Op: SinkOperator<I>,
    C: Circuit,
{
    fn new(operator: Op, input_stream: Stream<C, I>, circuit: C, id: NodeId) -> Self {
        Self {
            id: circuit.global_node_id().child(id),
            operator,
            input_stream,
        }
    }
}

impl<C, I, Op> Node for SinkNode<C, I, Op>
where
    C: Circuit,
    I: Clone,
    Op: SinkOperator<I>,
{
    fn name(&self) -> Cow<'static, str> {
        self.operator.name()
    }

    fn local_id(&self) -> NodeId {
        self.id.local_node_id().unwrap()
    }

    fn global_id(&self) -> &GlobalNodeId {
        &self.id
    }

    fn is_async(&self) -> bool {
        self.operator.is_async()
    }

    fn ready(&self) -> bool {
        self.operator.ready()
    }

    fn register_ready_callback(&mut self, cb: Box<dyn Fn() + Send + Sync>) {
        self.operator.register_ready_callback(cb);
    }

    unsafe fn eval(&mut self) -> Result<(), SchedulerError> {
        match self.input_stream.take() {
            Cow::Owned(v) => self.operator.eval_owned(v),
            Cow::Borrowed(v) => self.operator.eval(v),
        };
        Ok(())
    }

    fn clock_start(&mut self, scope: Scope) {
        self.operator.clock_start(scope);
    }

    unsafe fn clock_end(&mut self, scope: Scope) {
        self.operator.clock_end(scope);
    }

    fn metadata(&self, output: &mut OperatorMeta) {
        self.operator.metadata(output);
    }

    fn fixedpoint(&self, scope: Scope) -> bool {
        self.operator.fixedpoint(scope)
    }

    fn commit(&mut self, cid: Uuid) -> Result<(), DBSPError> {
        self.operator.commit(cid, self.global_id().persistent_id())
    }

    fn restore(&mut self, cid: Uuid) -> Result<(), DBSPError> {
        self.operator.restore(cid, self.global_id().persistent_id())
    }
}

struct BinarySinkNode<C, I1, I2, Op> {
    id: GlobalNodeId,
    operator: Op,
    input_stream1: Stream<C, I1>,
    input_stream2: Stream<C, I2>,
    // `true` if both input streams are aliases of the same stream.
    is_alias: bool,
}

impl<C, I1, I2, Op> BinarySinkNode<C, I1, I2, Op>
where
    I1: Clone,
    I2: Clone,
    Op: BinarySinkOperator<I1, I2>,
    C: Circuit,
{
    fn new(
        operator: Op,
        input_stream1: Stream<C, I1>,
        input_stream2: Stream<C, I2>,
        circuit: C,
        id: NodeId,
    ) -> Self {
        let is_alias = input_stream1.ptr_eq(&input_stream2);
        Self {
            id: circuit.global_node_id().child(id),
            operator,
            input_stream1,
            input_stream2,
            is_alias,
        }
    }
}

impl<C, I1, I2, Op> Node for BinarySinkNode<C, I1, I2, Op>
where
    C: Circuit,
    I1: Clone,
    I2: Clone,
    Op: BinarySinkOperator<I1, I2>,
{
    fn name(&self) -> Cow<'static, str> {
        self.operator.name()
    }

    fn local_id(&self) -> NodeId {
        self.id.local_node_id().unwrap()
    }

    fn global_id(&self) -> &GlobalNodeId {
        &self.id
    }

    fn is_async(&self) -> bool {
        self.operator.is_async()
    }

    fn ready(&self) -> bool {
        self.operator.ready()
    }

    fn register_ready_callback(&mut self, cb: Box<dyn Fn() + Send + Sync>) {
        self.operator.register_ready_callback(cb);
    }

    unsafe fn eval(&mut self) -> Result<(), SchedulerError> {
        let input1 = if self.is_alias {
            Cow::Borrowed(self.input_stream1.peek())
        } else {
            self.input_stream1.take()
        };

        let input2 = self.input_stream2.take();

        self.operator.eval(input1, input2);
        Ok(())
    }

    fn clock_start(&mut self, scope: Scope) {
        self.operator.clock_start(scope);
    }

    unsafe fn clock_end(&mut self, scope: Scope) {
        self.operator.clock_end(scope);
    }

    fn metadata(&self, output: &mut OperatorMeta) {
        self.operator.metadata(output);
    }

    fn fixedpoint(&self, scope: Scope) -> bool {
        self.operator.fixedpoint(scope)
    }

    fn commit(&mut self, cid: Uuid) -> Result<(), DBSPError> {
        self.operator.commit(cid, self.global_id().persistent_id())
    }

    fn restore(&mut self, cid: Uuid) -> Result<(), DBSPError> {
        self.operator.restore(cid, self.global_id().persistent_id())
    }
}

struct BinaryNode<C, I1, I2, O, Op> {
    id: GlobalNodeId,
    operator: Op,
    input_stream1: Stream<C, I1>,
    input_stream2: Stream<C, I2>,
    output_stream: Stream<C, O>,
    // `true` if both input streams are aliases of the same stream.
    is_alias: bool,
}

impl<C, I1, I2, O, Op> BinaryNode<C, I1, I2, O, Op>
where
    Op: BinaryOperator<I1, I2, O>,
    C: Circuit,
{
    fn new(
        operator: Op,
        input_stream1: Stream<C, I1>,
        input_stream2: Stream<C, I2>,
        circuit: C,
        id: NodeId,
    ) -> Self {
        let is_alias = input_stream1.ptr_eq(&input_stream2);
        Self {
            id: circuit.global_node_id().child(id),
            operator,
            input_stream1,
            input_stream2,
            is_alias,
            output_stream: Stream::new(circuit, id),
        }
    }

    fn output_stream(&self) -> Stream<C, O> {
        self.output_stream.clone()
    }
}

impl<C, I1, I2, O, Op> Node for BinaryNode<C, I1, I2, O, Op>
where
    C: Circuit,
    I1: Clone,
    I2: Clone,
    O: Clone,
    Op: BinaryOperator<I1, I2, O>,
{
    fn name(&self) -> Cow<'static, str> {
        self.operator.name()
    }

    fn local_id(&self) -> NodeId {
        self.id.local_node_id().unwrap()
    }

    fn global_id(&self) -> &GlobalNodeId {
        &self.id
    }

    fn is_async(&self) -> bool {
        self.operator.is_async()
    }

    fn ready(&self) -> bool {
        self.operator.ready()
    }

    fn register_ready_callback(&mut self, cb: Box<dyn Fn() + Send + Sync>) {
        self.operator.register_ready_callback(cb);
    }

    unsafe fn eval(&mut self) -> Result<(), SchedulerError> {
        // If the two input streams are aliases, we cannot remove the owned
        // value from `input_stream2`, as this will invalidate the borrow
        // from `input_stream1`.  Instead use `peek` to obtain the value by
        // reference.
        if self.is_alias {
            self.output_stream.put(
                match (self.input_stream1.take(), self.input_stream2.peek()) {
                    (Cow::Borrowed(v1), v2) => self.operator.eval(v1, v2),
                    _ => unreachable!(),
                },
            );
            // It is now safe to call `take`, and we must do so to decrement
            // the ref counter.
            let _ = self.input_stream2.take();
        } else {
            self.output_stream.put(
                match (self.input_stream1.take(), self.input_stream2.take()) {
                    (Cow::Owned(v1), Cow::Owned(v2)) => self.operator.eval_owned(v1, v2),
                    (Cow::Owned(v1), Cow::Borrowed(v2)) => self.operator.eval_owned_and_ref(v1, v2),
                    (Cow::Borrowed(v1), Cow::Owned(v2)) => self.operator.eval_ref_and_owned(v1, v2),
                    (Cow::Borrowed(v1), Cow::Borrowed(v2)) => self.operator.eval(v1, v2),
                },
            );
        }
        Ok(())
    }

    fn clock_start(&mut self, scope: Scope) {
        self.operator.clock_start(scope);
    }

    unsafe fn clock_end(&mut self, scope: Scope) {
        self.operator.clock_end(scope);
    }

    fn metadata(&self, output: &mut OperatorMeta) {
        self.operator.metadata(output);
    }

    fn fixedpoint(&self, scope: Scope) -> bool {
        self.operator.fixedpoint(scope)
    }

    fn commit(&mut self, cid: Uuid) -> Result<(), Error> {
        self.operator.commit(cid, self.global_id().persistent_id())
    }

    fn restore(&mut self, cid: Uuid) -> Result<(), DBSPError> {
        self.operator.restore(cid, self.global_id().persistent_id())
    }
}

struct TernaryNode<C, I1, I2, I3, O, Op> {
    id: GlobalNodeId,
    operator: Op,
    input_stream1: Stream<C, I1>,
    input_stream2: Stream<C, I2>,
    input_stream3: Stream<C, I3>,
    output_stream: Stream<C, O>,
    // `true` if `input_stream1` is an alias to `input_stream2` or `input_stream3`.
    is_alias1: bool,
    // `true` if `input_stream2` is an alias to `input_stream3`.
    is_alias2: bool,
}

impl<C, I1, I2, I3, O, Op> TernaryNode<C, I1, I2, I3, O, Op>
where
    I1: Clone,
    I2: Clone,
    I3: Clone,
    Op: TernaryOperator<I1, I2, I3, O>,
    C: Circuit,
{
    fn new(
        operator: Op,
        input_stream1: Stream<C, I1>,
        input_stream2: Stream<C, I2>,
        input_stream3: Stream<C, I3>,
        circuit: C,
        id: NodeId,
    ) -> Self {
        let is_alias1 =
            input_stream1.ptr_eq(&input_stream2) || input_stream1.ptr_eq(&input_stream3);
        let is_alias2 = input_stream2.ptr_eq(&input_stream3);
        Self {
            id: circuit.global_node_id().child(id),
            operator,
            input_stream1,
            input_stream2,
            input_stream3,
            is_alias1,
            is_alias2,
            output_stream: Stream::new(circuit, id),
        }
    }

    fn output_stream(&self) -> Stream<C, O> {
        self.output_stream.clone()
    }
}

impl<C, I1, I2, I3, O, Op> Node for TernaryNode<C, I1, I2, I3, O, Op>
where
    C: Circuit,
    I1: Clone,
    I2: Clone,
    I3: Clone,
    O: Clone,
    Op: TernaryOperator<I1, I2, I3, O>,
{
    fn name(&self) -> Cow<'static, str> {
        self.operator.name()
    }

    fn local_id(&self) -> NodeId {
        self.id.local_node_id().unwrap()
    }

    fn global_id(&self) -> &GlobalNodeId {
        &self.id
    }

    fn is_async(&self) -> bool {
        self.operator.is_async()
    }

    fn ready(&self) -> bool {
        self.operator.ready()
    }

    fn register_ready_callback(&mut self, cb: Box<dyn Fn() + Send + Sync>) {
        self.operator.register_ready_callback(cb);
    }

    unsafe fn eval(&mut self) -> Result<(), SchedulerError> {
        let input1 = if self.is_alias1 {
            Cow::Borrowed(self.input_stream1.peek())
        } else {
            self.input_stream1.take()
        };

        let input2 = if self.is_alias2 {
            Cow::Borrowed(self.input_stream2.peek())
        } else {
            self.input_stream2.take()
        };

        let input3 = self.input_stream3.take();

        self.output_stream
            .put(self.operator.eval(input1, input2, input3));
        Ok(())
    }

    fn clock_start(&mut self, scope: Scope) {
        self.operator.clock_start(scope);
    }

    unsafe fn clock_end(&mut self, scope: Scope) {
        self.operator.clock_end(scope);
    }

    fn metadata(&self, output: &mut OperatorMeta) {
        self.operator.metadata(output);
    }

    fn fixedpoint(&self, scope: Scope) -> bool {
        self.operator.fixedpoint(scope)
    }

    fn commit(&mut self, cid: Uuid) -> Result<(), Error> {
        self.operator.commit(cid, self.global_id().persistent_id())
    }

    fn restore(&mut self, cid: Uuid) -> Result<(), DBSPError> {
        self.operator.restore(cid, self.global_id().persistent_id())
    }
}

struct QuaternaryNode<C, I1, I2, I3, I4, O, Op> {
    id: GlobalNodeId,
    operator: Op,
    input_stream1: Stream<C, I1>,
    input_stream2: Stream<C, I2>,
    input_stream3: Stream<C, I3>,
    input_stream4: Stream<C, I4>,
    output_stream: Stream<C, O>,
    // `true` if `input_stream1` is an alias to `input_stream2`, `input_stream3` or
    // `input_stream4`.
    is_alias1: bool,
    // `true` if `input_stream2` is an alias to `input_stream3` or `input_stream4`.
    is_alias2: bool,
    // `true` if `input_stream3` is an alias to `input_stream4`.
    is_alias3: bool,
}

impl<C, I1, I2, I3, I4, O, Op> QuaternaryNode<C, I1, I2, I3, I4, O, Op>
where
    I1: Clone,
    I2: Clone,
    I3: Clone,
    I4: Clone,
    Op: QuaternaryOperator<I1, I2, I3, I4, O>,
    C: Circuit,
{
    fn new(
        operator: Op,
        input_stream1: Stream<C, I1>,
        input_stream2: Stream<C, I2>,
        input_stream3: Stream<C, I3>,
        input_stream4: Stream<C, I4>,
        circuit: C,
        id: NodeId,
    ) -> Self {
        let is_alias1 = input_stream1.ptr_eq(&input_stream2)
            || input_stream1.ptr_eq(&input_stream3)
            || input_stream1.ptr_eq(&input_stream4);
        let is_alias2 =
            input_stream2.ptr_eq(&input_stream3) || input_stream2.ptr_eq(&input_stream4);
        let is_alias3 = input_stream3.ptr_eq(&input_stream4);
        Self {
            id: circuit.global_node_id().child(id),
            operator,
            input_stream1,
            input_stream2,
            input_stream3,
            input_stream4,
            is_alias1,
            is_alias2,
            is_alias3,
            output_stream: Stream::new(circuit, id),
        }
    }

    fn output_stream(&self) -> Stream<C, O> {
        self.output_stream.clone()
    }
}

impl<C, I1, I2, I3, I4, O, Op> Node for QuaternaryNode<C, I1, I2, I3, I4, O, Op>
where
    C: Circuit,
    I1: Clone,
    I2: Clone,
    I3: Clone,
    I4: Clone,
    O: Clone,
    Op: QuaternaryOperator<I1, I2, I3, I4, O>,
{
    fn name(&self) -> Cow<'static, str> {
        self.operator.name()
    }

    fn local_id(&self) -> NodeId {
        self.id.local_node_id().unwrap()
    }

    fn global_id(&self) -> &GlobalNodeId {
        &self.id
    }

    fn is_async(&self) -> bool {
        self.operator.is_async()
    }

    fn ready(&self) -> bool {
        self.operator.ready()
    }

    fn register_ready_callback(&mut self, cb: Box<dyn Fn() + Send + Sync>) {
        self.operator.register_ready_callback(cb);
    }

    unsafe fn eval(&mut self) -> Result<(), SchedulerError> {
        let input1 = if self.is_alias1 {
            Cow::Borrowed(self.input_stream1.peek())
        } else {
            self.input_stream1.take()
        };

        let input2 = if self.is_alias2 {
            Cow::Borrowed(self.input_stream2.peek())
        } else {
            self.input_stream2.take()
        };

        let input3 = if self.is_alias3 {
            Cow::Borrowed(self.input_stream3.peek())
        } else {
            self.input_stream3.take()
        };

        let input4 = self.input_stream4.take();

        self.output_stream
            .put(self.operator.eval(input1, input2, input3, input4));
        Ok(())
    }

    fn clock_start(&mut self, scope: Scope) {
        self.operator.clock_start(scope);
    }

    unsafe fn clock_end(&mut self, scope: Scope) {
        self.operator.clock_end(scope);
    }

    fn metadata(&self, output: &mut OperatorMeta) {
        self.operator.metadata(output);
    }

    fn fixedpoint(&self, scope: Scope) -> bool {
        self.operator.fixedpoint(scope)
    }

    fn commit(&mut self, cid: Uuid) -> Result<(), Error> {
        self.operator.commit(cid, self.global_id().persistent_id())
    }

    fn restore(&mut self, cid: Uuid) -> Result<(), DBSPError> {
        self.operator.restore(cid, self.global_id().persistent_id())
    }
}

struct NaryNode<C, I, O, Op>
where
    I: Clone + 'static,
{
    id: GlobalNodeId,
    operator: Op,
    // The second field of the tuple indicates if the stream is an
    // alias to an earlier stream.
    input_streams: Vec<(Stream<C, I>, bool)>,
    // Streams that are aliases.
    aliases: Vec<usize>,
    output_stream: Stream<C, O>,
}

impl<C, I, O, Op> NaryNode<C, I, O, Op>
where
    I: Clone + 'static,
    Op: NaryOperator<I, O>,
    C: Circuit,
{
    fn new<Iter>(operator: Op, input_streams: Iter, circuit: C, id: NodeId) -> Self
    where
        Iter: IntoIterator<Item = Stream<C, I>>,
    {
        let mut input_streams: Vec<_> = input_streams
            .into_iter()
            .map(|stream| (stream, false))
            .collect();
        let mut aliases = Vec::new();
        for i in 0..input_streams.len() {
            for j in 0..i {
                if input_streams[i].0.ptr_eq(&input_streams[j].0) {
                    input_streams[i].1 = true;
                    aliases.push(i);
                    break;
                }
            }
        }
        aliases.shrink_to_fit();
        input_streams.shrink_to_fit();
        Self {
            id: circuit.global_node_id().child(id),
            operator,
            input_streams,
            aliases,
            output_stream: Stream::new(circuit, id),
        }
    }

    fn output_stream(&self) -> Stream<C, O> {
        self.output_stream.clone()
    }
}

impl<C, I, O, Op> Node for NaryNode<C, I, O, Op>
where
    C: Circuit,
    I: Clone,
    O: Clone,
    Op: NaryOperator<I, O>,
{
    fn name(&self) -> Cow<'static, str> {
        self.operator.name()
    }

    fn local_id(&self) -> NodeId {
        self.id.local_node_id().unwrap()
    }

    fn global_id(&self) -> &GlobalNodeId {
        &self.id
    }

    fn is_async(&self) -> bool {
        self.operator.is_async()
    }

    fn ready(&self) -> bool {
        self.operator.ready()
    }

    fn register_ready_callback(&mut self, cb: Box<dyn Fn() + Send + Sync>) {
        self.operator.register_ready_callback(cb);
    }

    unsafe fn eval(&mut self) -> Result<(), SchedulerError> {
        self.output_stream
            .put(
                self.operator
                    .eval(self.input_streams.iter().map(|(stream, alias)| {
                        // Don't take owned value via an alias.
                        if *alias {
                            Cow::Borrowed(stream.peek())
                        } else {
                            stream.take()
                        }
                    })),
            );

        for i in self.aliases.iter() {
            let _ = self.input_streams[*i].0.take();
        }
        Ok(())
    }

    fn clock_start(&mut self, scope: Scope) {
        self.operator.clock_start(scope);
    }

    unsafe fn clock_end(&mut self, scope: Scope) {
        self.operator.clock_end(scope);
    }

    fn metadata(&self, output: &mut OperatorMeta) {
        self.operator.metadata(output);
    }

    fn fixedpoint(&self, scope: Scope) -> bool {
        self.operator.fixedpoint(scope)
    }

    fn commit(&mut self, cid: Uuid) -> Result<(), Error> {
        self.operator.commit(cid, self.global_id().persistent_id())
    }

    fn restore(&mut self, cid: Uuid) -> Result<(), DBSPError> {
        self.operator.restore(cid, self.global_id().persistent_id())
    }
}

// The output half of a feedback node.  We implement a feedback node using a
// pair of nodes: `FeedbackOutputNode` is connected to the circuit as a source
// node (i.e., it does not have an input stream) and thus gets evaluated first
// in each time stamp.  `FeedbackInputNode` is a sink node.  This way the
// circuit graph remains acyclic and can be scheduled in a topological order.
struct FeedbackOutputNode<C, I, O, Op>
where
    C: Circuit,
{
    id: GlobalNodeId,
    operator: Rc<UnsafeCell<Op>>,
    output_stream: Stream<C, O>,
    export_stream: Option<Stream<C::Parent, O>>,
    phantom_input: PhantomData<I>,
}

impl<C, I, O, Op> FeedbackOutputNode<C, I, O, Op>
where
    C: Circuit,
    Op: StrictUnaryOperator<I, O>,
{
    fn new(operator: Rc<UnsafeCell<Op>>, circuit: C, id: NodeId) -> Self {
        Self {
            id: circuit.global_node_id().child(id),
            operator,
            output_stream: Stream::new(circuit.clone(), id),
            export_stream: None,
            phantom_input: PhantomData,
        }
    }

    fn with_export(operator: Rc<UnsafeCell<Op>>, circuit: C, id: NodeId) -> Self {
        let mut result = Self::new(operator, circuit.clone(), id);
        result.export_stream = Some(Stream::with_origin(
            circuit.parent(),
            circuit.node_id(),
            GlobalNodeId::child_of(&circuit, id),
        ));
        result
    }

    fn output_stream(&self) -> Stream<C, O> {
        self.output_stream.clone()
    }
}

impl<C, I, O, Op> Node for FeedbackOutputNode<C, I, O, Op>
where
    C: Circuit,
    I: Data,
    O: Clone,
    Op: StrictUnaryOperator<I, O>,
{
    fn name(&self) -> Cow<'static, str> {
        unsafe { &*self.operator.get() }.name()
    }

    fn local_id(&self) -> NodeId {
        self.id.local_node_id().unwrap()
    }

    fn global_id(&self) -> &GlobalNodeId {
        &self.id
    }

    fn is_async(&self) -> bool {
        unsafe { &*self.operator.get() }.is_async()
    }

    fn ready(&self) -> bool {
        unsafe { &*self.operator.get() }.ready()
    }

    fn register_ready_callback(&mut self, cb: Box<dyn Fn() + Send + Sync>) {
        unsafe { &mut *self.operator.get() }.register_ready_callback(cb);
    }

    unsafe fn eval(&mut self) -> Result<(), SchedulerError> {
        self.output_stream.put((*self.operator.get()).get_output());
        Ok(())
    }

    fn clock_start(&mut self, scope: Scope) {
        unsafe { (*self.operator.get()).clock_start(scope) }
    }

    unsafe fn clock_end(&mut self, scope: Scope) {
        if scope == 0 {
            if let Some(export_stream) = &mut self.export_stream {
                export_stream.put((*self.operator.get()).get_final_output());
            }
        }
        (*self.operator.get()).clock_end(scope);
    }

    fn metadata(&self, output: &mut OperatorMeta) {
        unsafe { (*self.operator.get()).metadata(output) }
    }

    fn fixedpoint(&self, scope: Scope) -> bool {
        unsafe { (*self.operator.get()).fixedpoint(scope) }
    }

    fn commit(&mut self, cid: Uuid) -> Result<(), Error> {
        unsafe { (*self.operator.get()).commit(cid, self.global_id().persistent_id()) }
    }

    fn restore(&mut self, cid: Uuid) -> Result<(), DBSPError> {
        unsafe { (*self.operator.get()).restore(cid, self.global_id().persistent_id()) }
    }
}

/// The input half of a feedback node
struct FeedbackInputNode<C, I, O, Op> {
    // Id of this node (the input half).
    id: GlobalNodeId,
    operator: Rc<UnsafeCell<Op>>,
    input_stream: Stream<C, I>,
    phantom_output: PhantomData<O>,
}

impl<C, I, O, Op> FeedbackInputNode<C, I, O, Op>
where
    Op: StrictUnaryOperator<I, O>,
    C: Circuit,
{
    fn new(operator: Rc<UnsafeCell<Op>>, input_stream: Stream<C, I>, id: NodeId) -> Self {
        Self {
            id: input_stream.circuit().global_node_id().child(id),
            operator,
            input_stream,
            phantom_output: PhantomData,
        }
    }
}

impl<C, I, O, Op> Node for FeedbackInputNode<C, I, O, Op>
where
    Op: StrictUnaryOperator<I, O>,
    I: Data,
{
    fn name(&self) -> Cow<'static, str> {
        unsafe { &*self.operator.get() }.name()
    }

    fn local_id(&self) -> NodeId {
        self.id.local_node_id().unwrap()
    }

    fn global_id(&self) -> &GlobalNodeId {
        &self.id
    }

    fn is_async(&self) -> bool {
        unsafe { &*self.operator.get() }.is_async()
    }

    fn ready(&self) -> bool {
        unsafe { &*self.operator.get() }.ready()
    }

    fn register_ready_callback(&mut self, cb: Box<dyn Fn() + Send + Sync>) {
        unsafe { &mut *self.operator.get() }.register_ready_callback(cb);
    }

    unsafe fn eval(&mut self) -> Result<(), SchedulerError> {
        match self.input_stream.take() {
            Cow::Owned(v) => (*self.operator.get()).eval_strict_owned(v),
            Cow::Borrowed(v) => (*self.operator.get()).eval_strict(v),
        };
        Ok(())
    }

    // Don't call `clock_start`/`clock_end` on the operator.  `FeedbackOutputNode`
    // will do that.
    fn clock_start(&mut self, _scope: Scope) {}

    unsafe fn clock_end(&mut self, _scope: Scope) {}

    fn metadata(&self, output: &mut OperatorMeta) {
        unsafe { (*self.operator.get()).metadata(output) }
    }

    fn fixedpoint(&self, scope: Scope) -> bool {
        unsafe { (*self.operator.get()).fixedpoint(scope) }
    }

    fn commit(&mut self, _cid: Uuid) -> Result<(), Error> {
        // The Z-1 operator consists of two logical parts.
        // The first part gets invoked at the start of a clock cycle to retrieve the
        // state stored at the previous clock tick. The second one gets invoked
        // to store the updated state inside the operator. We only want to
        // invoke commit on one of them, doesn't matter which (so we
        // do it in FeedbackOutputNode)
        Ok(())
    }

    fn restore(&mut self, _cid: Uuid) -> Result<(), DBSPError> {
        // See comment in `commit`.
        Ok(())
    }
}

/// Input connector of a feedback operator.
///
/// This struct is part of the mechanism for constructing a feedback loop in a
/// circuit. It is returned by [`Circuit::add_feedback`] and represents the
/// input port of an operator whose input stream does not exist yet.  Once the
/// input stream has been created, it can be connected to the operator using
/// [`FeedbackConnector::connect`]. See [`Circuit::add_feedback`] for details.
pub struct FeedbackConnector<C, I, O, Op> {
    output_node_id: NodeId,
    circuit: C,
    operator: Rc<UnsafeCell<Op>>,
    phantom_input: PhantomData<I>,
    phantom_output: PhantomData<O>,
}

impl<C, I, O, Op> FeedbackConnector<C, I, O, Op>
where
    Op: StrictUnaryOperator<I, O>,
{
    fn new(output_node_id: NodeId, circuit: C, operator: Rc<UnsafeCell<Op>>) -> Self {
        Self {
            output_node_id,
            circuit,
            operator,
            phantom_input: PhantomData,
            phantom_output: PhantomData,
        }
    }
}

impl<C, I, O, Op> FeedbackConnector<C, I, O, Op>
where
    Op: StrictUnaryOperator<I, O>,
    I: Data,
    O: Data,
    C: Circuit,
{
    /// Connect `input_stream` as input to the operator.
    ///
    /// See [`Circuit::add_feedback`] for details.
    /// Returns node id of the input node.
    pub fn connect(self, input_stream: &Stream<C, I>) {
        self.connect_with_preference(input_stream, OwnershipPreference::INDIFFERENT)
    }

    pub fn connect_with_preference(
        self,
        input_stream: &Stream<C, I>,
        input_preference: OwnershipPreference,
    ) {
        self.circuit.connect_feedback_with_preference(
            self.output_node_id,
            self.operator,
            input_stream,
            input_preference,
        );
    }
}

// A nested circuit instantiated as a node in a parent circuit.
struct ChildNode<P>
where
    P: Circuit,
{
    id: GlobalNodeId,
    circuit: ChildCircuit<P>,
    executor: Box<dyn Executor<ChildCircuit<P>>>,
}

impl<P> Drop for ChildNode<P>
where
    P: Circuit,
{
    fn drop(&mut self) {
        // Explicitly deallocate all nodes in the circuit to break
        // cyclic `Rc` references between circuits and streams.
        self.circuit.clear();
    }
}

impl<P> ChildNode<P>
where
    P: Circuit,
{
    fn new<E>(circuit: ChildCircuit<P>, executor: E) -> Self
    where
        E: Executor<ChildCircuit<P>>,
    {
        Self {
            id: circuit.global_node_id(),
            circuit,
            executor: Box::new(executor) as Box<dyn Executor<ChildCircuit<P>>>,
        }
    }
}

impl<P> Node for ChildNode<P>
where
    P: Circuit,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("Subcircuit")
    }

    fn local_id(&self) -> NodeId {
        self.id.local_node_id().unwrap()
    }

    fn global_id(&self) -> &GlobalNodeId {
        &self.id
    }

    fn is_async(&self) -> bool {
        false
    }

    fn ready(&self) -> bool {
        true
    }

    unsafe fn eval(&mut self) -> Result<(), SchedulerError> {
        self.executor.run(&self.circuit)
    }

    fn clock_start(&mut self, scope: Scope) {
        self.circuit.clock_start(scope + 1);
    }

    unsafe fn clock_end(&mut self, scope: Scope) {
        self.circuit.clock_end(scope + 1);
    }

    fn metadata(&self, _meta: &mut OperatorMeta) {}

    fn fixedpoint(&self, scope: Scope) -> bool {
        self.circuit.inner().fixedpoint(scope + 1)
    }

    fn map_nodes_recursive(&self, f: &mut dyn FnMut(&dyn Node)) {
        self.circuit.map_nodes_recursive(f);
    }

    fn commit(&mut self, _cid: Uuid) -> Result<(), Error> {
        Ok(())
    }

    fn restore(&mut self, _cid: Uuid) -> Result<(), DBSPError> {
        Ok(())
    }
}

/// Top-level circuit with executor.
///
/// This is the interface to a circuit created with [`RootCircuit::build`].
/// Call [`CircuitHandle::step`] to run the circuit in the context of the
/// current thread.
pub struct CircuitHandle {
    circuit: RootCircuit,
    executor: Box<dyn Executor<RootCircuit>>,
}

impl Drop for CircuitHandle {
    fn drop(&mut self) {
        self.circuit
            .log_scheduler_event(&SchedulerEvent::clock_end());

        // Prevent nested panic when `drop` is invoked while panicking
        // and `clock_end` triggers another panic due to violated invariants
        // since the original panic interrupted normal execution.
        if !panicking() {
            self.circuit.clock_end(0)
        }

        // We must explicitly deallocate all nodes in the circuit to break
        // cyclic `Rc` references between circuits and streams.  Alternatively,
        // we could use weak references to break cycles, but we'd have to
        // pay the cost of upgrading weak references on each access.
        self.circuit.clear();
    }
}

impl CircuitHandle {
    /// Function that drives the execution of the circuit.
    ///
    /// Every call to `step()` corresponds to one tick of the global logical
    /// clock and evaluates each operator in the circuit once.  Before calling,
    /// store the desired input value in each input stream using its input
    /// handle.  Each call stores a value in each output stream so, after
    /// calling, the client may obtain these values using their output handles.
    pub fn step(&self) -> Result<(), SchedulerError> {
        // TODO: Add a runtime check to prevent re-entering this method from an
        // operator.
        self.executor.run(&self.circuit)
    }

    pub fn commit(&mut self, cid: Uuid) -> Result<(), SchedulerError> {
        self.circuit
            .map_nodes_recursive_mut(&mut |node: &mut dyn Node| {
                node.commit(cid).expect("committed");
            });
        Ok(())
    }

    pub fn restore(&mut self, cid: Uuid) -> Result<(), SchedulerError> {
        self.circuit
            .map_nodes_recursive_mut(&mut |node: &mut dyn Node| {
                node.restore(cid).expect("restored");
            });
        Ok(())
    }

    pub fn fingerprint(&mut self) -> Result<u64, SchedulerError> {
        let mut fip = Fingerprinter::default();
        self.circuit.map_nodes_recursive(&mut |node: &dyn Node| {
            node.fingerprint(&mut fip);
        });
        Ok(fip.finish())
    }

    /// Attach a scheduler event handler to the circuit.
    ///
    /// This method is identical to
    /// [`RootCircuit::register_scheduler_event_handler`], but it can be used at
    /// runtime, after the circuit has been fully constructed.
    ///
    /// Use [`RootCircuit::register_scheduler_event_handler`],
    /// [`RootCircuit::unregister_scheduler_event_handler`], to manipulate
    /// handlers during circuit construction.
    pub fn register_scheduler_event_handler<F>(&self, name: &str, handler: F)
    where
        F: FnMut(&SchedulerEvent<'_>) + 'static,
    {
        self.circuit.register_scheduler_event_handler(name, handler);
    }

    /// Remove a scheduler event handler.
    ///
    /// This method is identical to
    /// [`RootCircuit::unregister_scheduler_event_handler`], but it can be used
    /// at runtime, after the circuit has been fully constructed.
    pub fn unregister_scheduler_event_handler(&self, name: &str) -> bool {
        self.circuit.unregister_scheduler_event_handler(name)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        circuit::schedule::{DynamicScheduler, Scheduler, StaticScheduler},
        monitor::TraceMonitor,
        operator::{Generator, Z1},
        Circuit, Error as DBSPError, RootCircuit,
    };
    use anyhow::anyhow;
    use std::{cell::RefCell, ops::Deref, rc::Rc, vec::Vec};

    // Compute the sum of numbers from 0 to 99.
    #[test]
    fn sum_circuit_static() {
        sum_circuit::<StaticScheduler>();
    }

    #[test]
    fn sum_circuit_dynamic() {
        sum_circuit::<DynamicScheduler>();
    }
    // Compute the sum of numbers from 0 to 99.
    fn sum_circuit<S>()
    where
        S: Scheduler + 'static,
    {
        let actual_output: Rc<RefCell<Vec<isize>>> = Rc::new(RefCell::new(Vec::with_capacity(100)));
        let actual_output_clone = actual_output.clone();
        let circuit = RootCircuit::build_with_scheduler::<_, _, S>(|circuit| {
            TraceMonitor::new_panic_on_error().attach(circuit, "monitor");
            let mut n: isize = 0;
            let source = circuit.add_source(Generator::new(move || {
                let result = n;
                n += 1;
                result
            }));
            let integrator = source.integrate();
            integrator.inspect(|n| println!("{}", n));
            integrator.inspect(move |n| actual_output_clone.borrow_mut().push(*n));
            Ok(())
        })
        .unwrap()
        .0;

        for _ in 0..100 {
            circuit.step().unwrap();
        }

        let mut sum = 0;
        let mut expected_output: Vec<isize> = Vec::with_capacity(100);
        for i in 0..100 {
            sum += i;
            expected_output.push(sum);
        }
        assert_eq!(&expected_output, actual_output.borrow().deref());
    }

    // Recursive circuit
    #[test]
    fn recursive_sum_circuit_static() {
        recursive_sum_circuit::<StaticScheduler>()
    }

    #[test]
    fn recursive_sum_circuit_dynamic() {
        recursive_sum_circuit::<DynamicScheduler>()
    }

    fn recursive_sum_circuit<S>()
    where
        S: Scheduler + 'static,
    {
        let actual_output: Rc<RefCell<Vec<usize>>> = Rc::new(RefCell::new(Vec::with_capacity(100)));
        let actual_output_clone = actual_output.clone();

        let circuit = RootCircuit::build_with_scheduler::<_, _, S>(|circuit| {
            TraceMonitor::new_panic_on_error().attach(circuit, "monitor");

            let mut n: usize = 0;
            let source = circuit.add_source(Generator::new(move || {
                let result = n;
                n += 1;
                result
            }));
            let (z1_output, z1_feedback) = circuit.add_feedback(Z1::new(0));
            let plus = source
                .apply2(&z1_output, |n1: &usize, n2: &usize| *n1 + *n2)
                .inspect(move |n| actual_output_clone.borrow_mut().push(*n));
            z1_feedback.connect(&plus);
            Ok(())
        })
        .unwrap()
        .0;

        for _ in 0..100 {
            circuit.step().unwrap();
        }

        let mut sum = 0;
        let mut expected_output: Vec<usize> = Vec::with_capacity(100);
        for i in 0..100 {
            sum += i;
            expected_output.push(sum);
        }
        assert_eq!(&expected_output, actual_output.borrow().deref());
    }

    #[test]
    fn factorial_static() {
        factorial::<StaticScheduler>();
    }

    #[test]
    fn factorial_dynamic() {
        factorial::<DynamicScheduler>();
    }

    // Nested circuit.  The circuit contains a source node that counts up from
    // 1. For each `n` output by the source node, the nested circuit computes
    // factorial(n) using a `NestedSource` operator that counts from n down to
    // `1` and a multiplier that multiplies the next count by the product
    // computed so far (stored in z-1).
    fn factorial<S>()
    where
        S: Scheduler + 'static,
    {
        let actual_output: Rc<RefCell<Vec<usize>>> = Rc::new(RefCell::new(Vec::with_capacity(100)));
        let actual_output_clone = actual_output.clone();

        let circuit = RootCircuit::build_with_scheduler::<_, _, S>(|circuit| {
            TraceMonitor::new_panic_on_error().attach(circuit, "monitor");

            let mut n: usize = 0;
            let source = circuit.add_source(Generator::new(move || {
                n += 1;
                n
            }));
            let fact = circuit
                .iterate_with_condition_and_scheduler::<_, _, S>(|child| {
                    let mut counter = 0;
                    let countdown = source.delta0(child).apply_mut(move |parent_val| {
                        if *parent_val > 0 {
                            counter = *parent_val;
                        };
                        let res = counter;
                        counter -= 1;
                        res
                    });
                    let (z1_output, z1_feedback) = child.add_feedback_with_export(Z1::new(1));
                    let mul = countdown.apply2(&z1_output.local, |n1: &usize, n2: &usize| n1 * n2);
                    z1_feedback.connect(&mul);
                    Ok((countdown.condition(|n| *n <= 1), z1_output.export))
                })
                .unwrap();
            fact.inspect(move |n| actual_output_clone.borrow_mut().push(*n));
            Ok(())
        })
        .unwrap()
        .0;

        for _ in 1..10 {
            circuit.step().unwrap();
        }

        let mut expected_output: Vec<usize> = Vec::with_capacity(10);
        for i in 1..10 {
            expected_output.push(my_factorial(i));
        }
        assert_eq!(&expected_output, actual_output.borrow().deref());
    }

    fn my_factorial(n: usize) -> usize {
        if n == 1 {
            1
        } else {
            n * my_factorial(n - 1)
        }
    }

    #[test]
    fn init_circuit_constructor_error() {
        match RootCircuit::build(|_circuit| Err::<(), _>(anyhow!("constructor failed"))) {
            Err(DBSPError::Constructor(msg)) => assert_eq!(msg.to_string(), "constructor failed"),
            _ => panic!(),
        }
    }
}
