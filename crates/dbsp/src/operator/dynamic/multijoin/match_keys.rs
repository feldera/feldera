use crate::{
    Circuit, DBData, Position, SchedulerError, Scope, Stream, Timestamp, ZWeight,
    algebra::{IndexedZSet, PartialOrder, ZBatch},
    circuit::{
        GlobalNodeId, NodeId, OwnershipPreference, WithClock,
        circuit_builder::{Node, StreamMetadata},
        metadata::{
            BatchSizeStats, COMPUTED_OUTPUT_RECORDS_COUNT, INPUT_INTEGRAL_RECORDS_COUNT,
            MEMORY_ALLOCATIONS_COUNT, MetaItem, MetricReading, OUTPUT_BATCHES_STATS,
            OUTPUT_REDUNDANCY_PERCENT, OperatorMeta, PREFIX_BATCHES_STATS, SHARED_MEMORY_BYTES,
            STATE_RECORDS_COUNT, USED_MEMORY_BYTES,
        },
        splitter_output_chunk_size,
    },
    dynamic::{
        ClonableTrait, Data, DynDataTyped, DynPair, DynPairs, Erase, Factory, LeanVec, WithFactory,
    },
    trace::{
        BatchFactories, BatchReader, BatchReaderFactories, Batcher, Cursor, Spine, SpineSnapshot,
        Trace, WeightedItem, WithSnapshot, cursor::SaturatingCursor,
    },
    utils::Tup2,
};
use async_stream::stream;
use feldera_storage::{FileCommitter, StoragePath};
use futures::{Stream as AsyncStream, StreamExt};
use size_of::{Context, SizeOf};
use std::{
    cell::RefCell,
    collections::{BTreeMap, HashMap},
    marker::PhantomData,
    ops::Deref,
    pin::Pin,
    rc::Rc,
    sync::Arc,
};

pub struct MatchFactories<I, T, O>
where
    I: IndexedZSet,
    O: IndexedZSet,
    T: Timestamp,
{
    pub prefix_factories: I::Factories,
    pub output_factories: O::Factories,
    pub timed_item_factory:
        &'static dyn Factory<DynPair<DynDataTyped<T>, WeightedItem<O::Key, O::Val, O::R>>>,
    pub timed_items_factory:
        &'static dyn Factory<DynPairs<DynDataTyped<T>, WeightedItem<O::Key, O::Val, O::R>>>,
}

impl<I, T, O> MatchFactories<I, T, O>
where
    I: IndexedZSet,
    O: IndexedZSet,
    T: Timestamp,
{
    pub fn new<KType, VType, OKType, OVType>() -> Self
    where
        KType: DBData + Erase<I::Key>,
        VType: DBData + Erase<I::Val>,
        OKType: DBData + Erase<O::Key>,
        OVType: DBData + Erase<O::Val>,
    {
        Self {
            prefix_factories: BatchReaderFactories::new::<KType, VType, ZWeight>(),
            output_factories: BatchReaderFactories::new::<OKType, OVType, ZWeight>(),
            timed_item_factory:
                WithFactory::<Tup2<T, Tup2<Tup2<OKType, OVType>, ZWeight>>>::FACTORY,
            timed_items_factory:
                WithFactory::<LeanVec<Tup2<T, Tup2<Tup2<OKType, OVType>, ZWeight>>>>::FACTORY,
        }
    }
}

impl<I, T, O> Clone for MatchFactories<I, T, O>
where
    I: IndexedZSet,
    O: IndexedZSet,
    T: Timestamp,
{
    fn clone(&self) -> Self {
        Self {
            prefix_factories: self.prefix_factories.clone(),
            output_factories: self.output_factories.clone(),
            timed_item_factory: self.timed_item_factory,
            timed_items_factory: self.timed_items_factory,
        }
    }
}

/// Computation performed by the Match operator for each key.
///
/// The Match operator iterates over common keys of a set of input collections
/// and constructs the output by performing a computation described by `MatchFunc` for each key.
///
/// For example, to implement the StarJoin operator, `MatchFunc` applies the join function
/// to all combinations of values under the input cursors for the current key.
///
/// The `MatchFunc` trait is invoked at every step to create a new generator object, which is
/// then invoked for every key to return another generator that yields the output for that key.
pub trait MatchFunc<C: WithClock, I: IndexedZSet, OK: ?Sized + 'static, OV: ?Sized + 'static>:
    'static
{
    type Generator: MatchGenerator<C, I, OK, OV>;

    /// Create a generator for the current step.
    fn new_generator(&self, current_time: C::Time) -> Self::Generator;
}

/// Generator for the output of the Match operator for one step of the circuit.
///
/// Can hold any memory allocations made once per step, e.g., the array of (weight, time)
/// tuples for each trace cursor used to evaluate the join function.
pub trait MatchGenerator<C: WithClock, I: IndexedZSet, OK: ?Sized + 'static, OV: ?Sized + 'static> {
    type Generator<'a, 'b>: MatchKeyGenerator<C, I, OK, OV>
    where
        Self: 'a,
        'b: 'a;

    /// Create a generator for the current key.
    ///
    /// Assumes that all cursors are valid and positioned at the same key.
    fn new_generator_for_key<'a, 'b>(
        &'a mut self,
        prefix_cursor: &'a mut <SpineSnapshot<I> as BatchReader>::Cursor<'b>,
        trace_cursors: &'a mut [SaturatingCursor<'b, I::Key, I::Val, C::Time>],
    ) -> Self::Generator<'a, 'b>
    where
        'b: 'a;
}

/// Generator for the output of the Match operator for one key.
pub trait MatchKeyGenerator<
    C: WithClock,
    I: IndexedZSet,
    OK: ?Sized + 'static,
    OV: ?Sized + 'static,
>
{
    /// Yield zero or more output tuples for the current key by invoking the callback
    /// for each output tuple.
    ///
    /// Returns true if there are more output tuples to yield, false otherwise.
    ///
    /// The method can invoke the callback several times, but it is not expected
    /// to do so too many times, since the Match operator only gets a chance to yield
    /// current outputs in between calls to `next`.
    fn next(&mut self, cb: impl FnMut(&mut OK, &mut OV, C::Time, ZWeight)) -> bool;
}

/// Builder for the Match operator.
///
/// The Match operator takes a "prefix" stream of untimed batches of type `I`
/// and multiple streams of timed batches with the same key and value types
/// as `I`. The trace streams can be a mix of `SpineSnapshot` streams (for delayed
/// integrals) and `Spine` streams (for current integrals).
pub struct MatchBuilder<C, I, O, F>
where
    C: Circuit,
    I: IndexedZSet,
    O: IndexedZSet,
    F: MatchFunc<C, I, O::Key, O::Val>,
{
    factories: MatchFactories<I, C::Time, O>,
    circuit: C,
    global_id: GlobalNodeId,

    prefix_stream: Box<dyn StreamMetadata>,

    /// Function to apply to elements of the prefix stream to get a `SpineSnapshot` of `I`.
    preprocess_prefix: Box<dyn Fn() -> Option<SpineSnapshot<I>>>,

    streams: Vec<(Box<dyn StreamMetadata>, &'static dyn Factory<I::Val>, bool)>,

    /// Functions to apply to elements of the trace streams to get a `SpineSnapshot` of `I`.
    preprocess:
        Vec<Box<dyn Fn() -> SpineSnapshot<<<C as WithClock>::Time as Timestamp>::TimedBatch<I>>>>,
    join_func: F,
}

impl<C, I, O, F> MatchBuilder<C, I, O, F>
where
    C: Circuit,
    I: IndexedZSet,
    O: IndexedZSet,
    F: MatchFunc<C, I, O::Key, O::Val>,
{
    pub fn new<B>(
        factories: &MatchFactories<I, C::Time, O>,
        global_id: GlobalNodeId,
        circuit: C,
        prefix_stream: Stream<C, Option<B>>,
        join_func: F,
    ) -> Self
    where
        B: Clone + WithSnapshot<Batch = I> + 'static,
    {
        prefix_stream.circuit().connect_stream(
            &prefix_stream,
            global_id.local_node_id().unwrap(),
            OwnershipPreference::INDIFFERENT,
        );

        Self {
            factories: factories.clone(),
            global_id,
            circuit,
            prefix_stream: Box::new(prefix_stream.clone()),
            preprocess_prefix: Box::new(move || {
                prefix_stream.map_value(|x| x.as_ref().map(|x| x.ro_snapshot()))
            }),
            streams: vec![],
            preprocess: vec![],
            join_func,
        }
    }

    pub fn add_input<B>(
        &mut self,
        stream: Stream<C, B>,
        val_factory: &'static dyn Factory<I::Val>,
        saturate: bool,
    ) where
        B: Clone
            + WithSnapshot<Batch = <<C as WithClock>::Time as Timestamp>::TimedBatch<I>>
            + 'static,
    {
        self.streams
            .push((Box::new(stream.clone()), val_factory, saturate));

        stream.circuit().connect_stream(
            &stream,
            self.global_id.local_node_id().unwrap(),
            OwnershipPreference::INDIFFERENT,
        );

        let preprocess = move || stream.map_value(|x| x.ro_snapshot());
        self.preprocess.push(Box::new(preprocess));
    }

    pub fn build(self) -> Match<C, I, O, F> {
        let id = self.global_id.local_node_id().unwrap();
        Match {
            global_id: self.global_id,
            labels: BTreeMap::new(),
            prefix: None,
            prefix_stream: self.prefix_stream,
            preprocess_prefix: self.preprocess_prefix,
            streams: self.streams,
            preprocess: self.preprocess,
            flush: RefCell::new(false),
            async_stream: None,
            inner: Rc::new(MatchInternal::new(
                id,
                self.factories,
                self.join_func,
                self.circuit,
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
struct MatchStats {
    prefix_batch_stats: BatchSizeStats,
    trace_sizes: Vec<usize>,
    output_tuples: usize,
    output_batch_stats: BatchSizeStats,
}

impl MatchStats {
    pub const fn new() -> Self {
        Self {
            prefix_batch_stats: BatchSizeStats::new(),
            trace_sizes: vec![],
            output_tuples: 0,
            output_batch_stats: BatchSizeStats::new(),
        }
    }

    pub fn add_output_batch<Z: ZBatch>(&mut self, batch: &Z) {
        self.output_batch_stats.add_batch(batch.len())
    }

    pub fn add_prefix_batch<Z: BatchReader>(&mut self, batch: &Z) {
        self.prefix_batch_stats.add_batch(batch.len())
    }
}

/// Iterate over the common keys of `prefix_cursor` and all `trace_cursors`.
struct JointKeyCursor<'a, C, I>
where
    C: Circuit,
    I: IndexedZSet,
{
    prefix_cursor: <SpineSnapshot<I> as BatchReader>::Cursor<'a>,

    /// Index of the trace cursor (in `trace_cursors`) with the smallest key count.
    ///
    /// The key count for saturated cursors is considered to be `usize::MAX`.
    smallest_trace_cursor: Option<usize>,

    trace_cursors: Vec<SaturatingCursor<'a, I::Key, I::Val, C::Time>>,
}

impl<'a, C, I> JointKeyCursor<'a, C, I>
where
    C: Circuit,
    I: IndexedZSet,
{
    /// Create a new cursor to iterate over the common keys of `prefix_cursor` and all `trace_cursors`.
    ///
    /// `snapshots` is a vector of tuples of `SpineSnapshot` and a boolean indicating whether the
    /// snapshot is saturated and should be treated as having a key count of `usize::MAX`.
    pub fn new(
        key_factory: &'static dyn Factory<I::Key>,
        prefix: SpineSnapshot<I>,
        snapshots: &[(
            SpineSnapshot<<<C as WithClock>::Time as Timestamp>::TimedBatch<I>>,
            &'static dyn Factory<I::Val>,
            bool,
        )],
    ) -> Self {
        // Choose the cursor with the smallest key count.
        // We will iterate over this cursor and seek to the same key in all other cursors.

        let prefix_cursor = prefix.cursor();

        // Find the index of the snapshot with the smallest key count.
        let smallest_snapshot_index = snapshots
            .iter()
            .enumerate()
            .min_by_key(
                |(_, (s, _factory, saturate))| if *saturate { usize::MAX } else { s.key_count() },
            )
            .unwrap()
            .0;

        let trace_cursors = snapshots
            .iter()
            .map(|(s, val_factory, saturate)| {
                SaturatingCursor::new(*saturate, Box::new(s.cursor()), key_factory, *val_factory)
            })
            .collect::<Vec<_>>();

        let smallest_trace_cursor = if !snapshots[smallest_snapshot_index].2
            && snapshots[smallest_snapshot_index].0.key_count() < prefix.key_count()
        {
            Some(smallest_snapshot_index)
        } else {
            None
        };

        Self {
            prefix_cursor,
            smallest_trace_cursor,
            trace_cursors,
        }
    }

    /// Advance key until finding a key that is present in all cursors.
    ///
    /// Returns true if a common key was found, false otherwise.
    pub fn next(&mut self) -> bool {
        if let Some(smallest_trace_cursor) = &self.smallest_trace_cursor {
            while self.trace_cursors[*smallest_trace_cursor].key_valid() {
                let (left, right) = self.trace_cursors.split_at_mut(*smallest_trace_cursor);
                let (cursor, right) = right.split_first_mut().unwrap();
                let key = cursor.key();
                let hash = key.default_hash();

                if self.prefix_cursor.seek_key_exact(key, Some(hash))
                    && left
                        .iter_mut()
                        .chain(right.iter_mut())
                        .all(|cursor| cursor.seek_key_exact(key, Some(hash)))
                {
                    return true;
                }

                cursor.step_key();
            }
        } else {
            while self.prefix_cursor.key_valid() {
                let key = self.prefix_cursor.key();
                let hash = key.default_hash();
                if self
                    .trace_cursors
                    .iter_mut()
                    .all(|cursor| cursor.seek_key_exact(key, Some(hash)))
                {
                    return true;
                }

                self.prefix_cursor.step_key();
            }
        }
        false
    }

    pub fn step_key(&mut self) {
        if let Some(smallest_trace_cursor) = &mut self.smallest_trace_cursor {
            self.trace_cursors[*smallest_trace_cursor].step_key();
        } else {
            self.prefix_cursor.step_key();
        }
    }

    pub fn position(&self) -> Option<Position> {
        if let Some(smallest_trace_cursor) = &self.smallest_trace_cursor {
            self.trace_cursors[*smallest_trace_cursor].position()
        } else {
            self.prefix_cursor.position()
        }
    }
}

/// Internals of the Match operator.
struct MatchInternal<C, I, O, F>
where
    C: Circuit,
    I: IndexedZSet,
    O: IndexedZSet,
    F: MatchFunc<C, I, O::Key, O::Val>,
{
    // Used in debug prints.
    #[allow(dead_code)]
    id: NodeId,
    key_factory: &'static dyn Factory<I::Key>,
    output_factories: O::Factories,
    timed_item_factory:
        &'static dyn Factory<DynPair<DynDataTyped<C::Time>, WeightedItem<O::Key, O::Val, O::R>>>,
    timed_items_factory:
        &'static dyn Factory<DynPairs<DynDataTyped<C::Time>, WeightedItem<O::Key, O::Val, O::R>>>,
    join_func: F,
    circuit: C,

    empty_input: RefCell<bool>,
    empty_output: RefCell<bool>,
    stats: RefCell<MatchStats>,

    // Future updates computed ahead of time, indexed by time
    // when each set of updates should be output.
    future_outputs: RefCell<HashMap<C::Time, Spine<O>>>,
    output_stream: Stream<C, O>,

    phantom: PhantomData<I>,
}

impl<C, I, O, F> MatchInternal<C, I, O, F>
where
    C: Circuit,
    I: IndexedZSet,
    O: IndexedZSet,
    F: MatchFunc<C, I, O::Key, O::Val>,
{
    fn new(id: NodeId, factories: MatchFactories<I, C::Time, O>, join_func: F, circuit: C) -> Self {
        Self {
            id,
            key_factory: factories.prefix_factories.key_factory(),
            output_factories: factories.output_factories,
            timed_item_factory: factories.timed_item_factory,
            timed_items_factory: factories.timed_items_factory,
            join_func,
            empty_input: RefCell::new(false),
            empty_output: RefCell::new(true),
            future_outputs: RefCell::new(HashMap::new()),
            stats: RefCell::new(MatchStats::new()),
            circuit: circuit.clone(),
            output_stream: Stream::new(circuit, id),
            phantom: PhantomData,
        }
    }

    fn async_eval(
        self: Rc<Self>,
        prefix: SpineSnapshot<I>,
        snapshots: Vec<(
            SpineSnapshot<<<C as WithClock>::Time as Timestamp>::TimedBatch<I>>,
            &'static dyn Factory<I::Val>,
            bool,
        )>,
    ) -> impl AsyncStream<Item = (O, bool, Option<Position>)> {
        let chunk_size = splitter_output_chunk_size();

        stream! {
            self.stats.borrow_mut().add_prefix_batch(&prefix);
            self.stats.borrow_mut().trace_sizes = snapshots.iter().map(|(s, _factories, _saturate)| s.len()).collect();

            *self.empty_input.borrow_mut() = prefix.is_empty();
            *self.empty_output.borrow_mut() = true;

            let mut generator = self.join_func.new_generator(self.circuit.time());

            let mut joint_cursor =
                JointKeyCursor::<'_, C, I>::new(self.key_factory, prefix, &snapshots);

            let batch = if size_of::<C::Time>() != 0 {
                // Evaluate join in the nested scope, where we need to worry about preparing future outputs.
                // TODO: We don't chunk outputs in the recursive scope.

                let time = self.circuit.time();

                let mut output_tuples = self.timed_items_factory.default_box();
                output_tuples.reserve(chunk_size);

                let mut timed_item = self.timed_item_factory.default_box();

                while joint_cursor.next() {
                    // println!("{} async_eval: key: {:?}", Runtime::worker_index(), joint_cursor.prefix_cursor.key());
                    let mut iter = generator.new_generator_for_key(&mut joint_cursor.prefix_cursor, &mut joint_cursor.trace_cursors);

                    while iter.next(|k, v, time, weight| {
                        let (time_ref, item) = timed_item.split_mut();
                        let (kv, w) = item.split_mut();
                        let (key, val) = kv.split_mut();

                        **w = weight;
                        k.clone_to(key);
                        v.clone_to(val);
                        **time_ref = time;
                        output_tuples.push_val(timed_item.as_mut());
                    }) {}

                    drop(iter);

                    joint_cursor.step_key();
                }

                self.stats.borrow_mut().output_tuples += output_tuples.len();

                // Sort `output_tuples` by timestamp and push all tuples for each unique
                // timestamp to the appropriate spine.
                output_tuples.sort_by_key();

                let mut batch = self.output_factories.weighted_items_factory().default_box();
                let mut start: usize = 0;

                while start < output_tuples.len() {
                    let batch_time = output_tuples[start].fst().deref().clone();

                    let run_length =
                        output_tuples.advance_while(start, output_tuples.len(), &|tuple| {
                            tuple.fst().deref() == &batch_time
                        });
                    batch.reserve(run_length);

                    for i in start..start + run_length {
                        batch.push_val(unsafe { output_tuples.index_mut_unchecked(i) }.snd_mut());
                    }

                    start += run_length;

                    self.future_outputs.borrow_mut().entry(batch_time).or_insert_with(|| {
                        let mut spine = <Spine<O> as Trace>::new(&self.output_factories);
                        spine.insert(O::dyn_from_tuples(&self.output_factories, (), &mut batch));
                        spine
                    });
                    batch.clear();
                }

                // Consolidate the spine for the current timestamp and return it.
                self.future_outputs.borrow_mut()
                    .remove(&time)
                    .and_then(|spine| spine.consolidate())
                    .unwrap_or_else(|| O::dyn_empty(&self.output_factories))
            } else {
                // Evaluate join in the outer scope, where we don't need to worry about preparing future outputs.

                let mut output_tuples = self.output_factories.weighted_items_factory().default_box();
                output_tuples.reserve(chunk_size);
                let mut batcher = O::Batcher::new_batcher(&self.output_factories, ());

                let mut output_tuple = self.output_factories.weighted_item_factory().default_box();

                while joint_cursor.next() {
                    //println!("{}:{} async_eval: key: {:?}", Runtime::worker_index(), self.id, joint_cursor.prefix_cursor.key());

                    let position = joint_cursor.position();
                    let mut iter = generator.new_generator_for_key(&mut joint_cursor.prefix_cursor, &mut joint_cursor.trace_cursors);

                    while iter.next(|k, v, _time, weight| {
                        let (kv, w) = output_tuple.split_mut();
                        let (key, val) = kv.split_mut();
                        k.clone_to(key);
                        v.clone_to(val);
                        **w = weight;
                        output_tuples.push_val(output_tuple.as_mut())
                    }) {
                        // Push a sufficiently large chunk of updates to the batcher. The batcher
                        // will consolidate the updates and possibly merge them with previous updates.
                        // Yield if the batcher has accumulated enough tuples. The divisor of 3 guarantees that
                        // the output batch won't exceed `chunk_size` by more than 33%. Alternatively we could
                        // push every individual output tuple to the batcher, but that's probably inefficient.
                        if output_tuples.len() >= chunk_size / 3 {
                            self.stats.borrow_mut().output_tuples += output_tuples.len();
                            batcher.push_batch(&mut output_tuples);

                            if batcher.tuples() >= chunk_size {
                                *self.empty_output.borrow_mut() = false;
                                let batch = batcher.seal();
                                self.stats.borrow_mut().add_output_batch(&batch);

                                // println!("{}:{} async_eval: yield batch: {:?}", Runtime::worker_index(), self.id, batch);
                                yield (batch, false, position.clone());
                                batcher = O::Batcher::new_batcher(&self.output_factories, ());
                            }
                        }
                    }

                    drop(iter);
                    joint_cursor.step_key();
                }

                self.stats.borrow_mut().output_tuples += output_tuples.len();
                batcher.push_batch(&mut output_tuples);
                batcher.seal()
            };

            // println!(
            //     "{}: join produces {} outputs (final = {:?}):{:?}",
            //     Runtime::worker_index(),
            //     batch.len(),
            //     self.state.is_none(),
            //     batch
            // );

            self.stats.borrow_mut().add_output_batch(&batch);

            if !batch.is_empty() {
                *self.empty_output.borrow_mut() = false;
            }

            //println!("{}:{} async_eval: yield final batch: {:?}", Runtime::worker_index(), self.id, batch);
            yield (batch, true, joint_cursor.position())
        }
    }
}

/// An operator that iterates over the common keys of multiple input streams and invokes a user-provided function for each key.
///
/// This is a low-level operator that can be used to implement different building blocks of multiway join algorithms.
pub struct Match<C, I, O, F>
where
    C: Circuit,
    I: IndexedZSet,
    O: IndexedZSet,
    F: MatchFunc<C, I, O::Key, O::Val>,
{
    global_id: GlobalNodeId,
    labels: BTreeMap<String, String>,

    prefix: Option<SpineSnapshot<I>>,
    prefix_stream: Box<dyn StreamMetadata>,
    preprocess_prefix: Box<dyn Fn() -> Option<SpineSnapshot<I>>>,

    streams: Vec<(Box<dyn StreamMetadata>, &'static dyn Factory<I::Val>, bool)>,
    preprocess:
        Vec<Box<dyn Fn() -> SpineSnapshot<<<C as WithClock>::Time as Timestamp>::TimedBatch<I>>>>,

    flush: RefCell<bool>,

    async_stream: Option<Pin<Box<dyn AsyncStream<Item = (O, bool, Option<Position>)>>>>,
    inner: Rc<MatchInternal<C, I, O, F>>,
}

impl<C, I, O, F> Match<C, I, O, F>
where
    C: Circuit,
    I: IndexedZSet,
    O: IndexedZSet,
    F: MatchFunc<C, I, O::Key, O::Val>,
{
    pub fn output_stream(&self) -> Stream<C, O> {
        self.inner.output_stream.clone()
    }
}

impl<C, I, O, F> Node for Match<C, I, O, F>
where
    C: Circuit,
    I: IndexedZSet,
    O: IndexedZSet,
    F: MatchFunc<C, I, O::Key, O::Val> + 'static,
{
    fn local_id(&self) -> NodeId {
        self.global_id.local_node_id().unwrap()
    }

    fn global_id(&self) -> &GlobalNodeId {
        &self.global_id
    }

    fn name(&self) -> std::borrow::Cow<'static, str> {
        "Match".into()
    }

    fn is_input(&self) -> bool {
        false
    }

    fn is_async(&self) -> bool {
        false
    }

    fn ready(&self) -> bool {
        true
    }

    fn start_transaction(&mut self) {}

    fn flush(&mut self) {
        *self.flush.borrow_mut() = true;
    }

    fn is_flush_complete(&self) -> bool {
        self.async_stream.is_none()
    }

    fn clock_start(&mut self, scope: Scope) {
        if scope == 0 {
            *self.inner.empty_input.borrow_mut() = false;
            *self.inner.empty_output.borrow_mut() = false;
        }
    }

    fn clock_end(&mut self, _scope: Scope) {
        debug_assert!(
            self.inner
                .future_outputs
                .borrow()
                .keys()
                .all(|time| !time.less_equal(&self.inner.circuit.time()))
        );
    }

    fn metadata(&self, meta: &mut OperatorMeta) {
        let stats = self.inner.stats.borrow();
        let total_size: usize = self
            .inner
            .future_outputs
            .borrow()
            .values()
            .map(|spine| spine.len())
            .sum();

        // let batch_sizes = MetaItem::Array(
        //     self.inner
        //         .future_outputs
        //         .borrow()
        //         .values()
        //         .map(|batcher| {
        //             let size = batcher.size_of();

        //             MetaItem::Map(
        //                 metadata! {
        //                     "allocated" => MetaItem::bytes(size.total_bytes()),
        //                     "used" => MetaItem::bytes(size.used_bytes()),
        //                 }
        //                 .into(),
        //             )
        //         })
        //         .collect(),
        // );

        let bytes = {
            let mut context = Context::new();
            for batcher in self.inner.future_outputs.borrow().values() {
                batcher.size_of_with_context(&mut context);
            }

            context.size_of()
        };

        // Find the percentage of consolidated outputs
        let output_redundancy = MetaItem::Percent {
            numerator: (stats.output_tuples - stats.output_batch_stats.total_size()) as u64,
            denominator: stats.output_tuples as u64,
        };

        meta.extend(metadata! {
            STATE_RECORDS_COUNT => MetaItem::Count(total_size),
            //"batch sizes" => batch_sizes,
            USED_MEMORY_BYTES => MetaItem::bytes(bytes.used_bytes()),
            MEMORY_ALLOCATIONS_COUNT => MetaItem::Count(bytes.distinct_allocations()),
            SHARED_MEMORY_BYTES => MetaItem::bytes(bytes.shared_bytes()),
            PREFIX_BATCHES_STATS => stats.prefix_batch_stats.metadata(),
            COMPUTED_OUTPUT_RECORDS_COUNT => stats.output_tuples,
            OUTPUT_BATCHES_STATS => stats.output_batch_stats.metadata(),
            OUTPUT_REDUNDANCY_PERCENT => output_redundancy,
        });

        for (i, size) in stats.trace_sizes.iter().enumerate() {
            meta.extend([MetricReading::new(
                INPUT_INTEGRAL_RECORDS_COUNT,
                vec![("integral".into(), i.to_string().into())],
                MetaItem::Count(*size),
            )])
        }
    }

    fn fixedpoint(&self, scope: Scope) -> bool {
        let epoch_end = self.inner.circuit.time().epoch_end(scope);
        // We're in a stable state if input and output at the current clock cycle are
        // both empty, and there are no precomputed outputs before the end of the
        // clock epoch.
        *self.inner.empty_input.borrow()
            && *self.inner.empty_output.borrow()
            && self
                .inner
                .future_outputs
                .borrow()
                .keys()
                .all(|time| !time.less_equal(&epoch_end))
    }

    fn clear_state(&mut self) -> Result<(), crate::Error> {
        Ok(())
    }

    fn start_replay(&mut self) -> Result<(), crate::Error> {
        panic!("Match: start_replay() is not implemented for this operator")
    }

    fn is_replay_complete(&self) -> bool {
        panic!("Match: is_replay_complete() is not implemented for this operator")
    }

    fn end_replay(&mut self) -> Result<(), crate::Error> {
        panic!("Match: end_replay() is not implemented for this operator")
    }

    fn set_label(&mut self, key: &str, value: &str) {
        self.labels.insert(key.to_string(), value.to_string());
    }

    fn get_label(&self, key: &str) -> Option<&str> {
        self.labels.get(key).map(|s| s.as_str())
    }

    fn labels(&self) -> &BTreeMap<String, String> {
        &self.labels
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn checkpoint(
        &mut self,
        _base: &StoragePath,
        _files: &mut Vec<Arc<dyn FileCommitter>>,
    ) -> Result<(), crate::Error> {
        Ok(())
    }

    fn restore(&mut self, _base: &StoragePath) -> Result<(), crate::Error> {
        Ok(())
    }

    fn eval<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Position>, SchedulerError>> + 'a>> {
        Box::pin(async {
            // println!("Match::eval: {} traces", self.streams.len());

            // Save prefix if Some().
            if let Some(prefix) = (self.preprocess_prefix)() {
                self.prefix = Some(prefix);
            }

            let mut snapshots = Vec::with_capacity(self.streams.len());

            if *self.flush.borrow() {
                // Collect all input snapshots.
                self.preprocess
                    .iter()
                    .enumerate()
                    .for_each(|(i, preprocess)| {
                        snapshots.push((preprocess(), self.streams[i].1, self.streams[i].2));
                    });
            }

            // Consume all input tokens.
            self.streams.iter().for_each(|stream| {
                stream.0.consume_token();
            });
            self.prefix_stream.consume_token();

            if *self.flush.borrow() {
                assert!(self.async_stream.is_none());
                let prefix = self.prefix.take().unwrap();

                self.async_stream =
                    Some(Box::pin(self.inner.clone().async_eval(prefix, snapshots))
                        as Pin<
                            Box<dyn AsyncStream<Item = (O, bool, Option<Position>)>>,
                        >);

                *self.flush.borrow_mut() = false;
            }

            if let Some(async_stream) = self.async_stream.as_mut() {
                let Some((output_batch, complete, progress)) = async_stream.next().await else {
                    panic!("MultiJoin::eval unexpectedly reached end of stream");
                };

                self.output_stream().put(output_batch);

                if complete {
                    self.async_stream = None;
                }
                Ok(progress)
            } else {
                self.output_stream()
                    .put(O::dyn_empty(&self.inner.output_factories));
                Ok(None)
            }
        })
    }
}
