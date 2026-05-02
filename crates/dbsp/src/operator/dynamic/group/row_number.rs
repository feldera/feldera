use crate::{
    Circuit, DBData, DynZWeight, Position, RootCircuit, Scope, Stream, ZWeight,
    algebra::{OrdIndexedZSet, OrdIndexedZSetFactories},
    circuit::{
        OwnershipPreference,
        circuit_builder::register_replay_stream,
        metadata::{BatchSizeStats, INPUT_BATCHES_STATS, OUTPUT_BATCHES_STATS, OperatorMeta},
        operator_traits::Operator,
        splitter_output_chunk_size,
    },
    dynamic::{ClonableTrait, DataTrait, DowncastTrait, DynData, DynPair, Erase},
    operator::{
        async_stream_operators::{StreamingBinaryOperator, StreamingBinaryWrapper},
        dynamic::{
            MonoIndexedZSet,
            accumulate_trace::{AccumulateTraceAppend, AccumulateTraceId, AccumulateZ1Trace},
            group::rank::{RankType, RankedBatch, RankedSpineFactories},
            trace::{BoundsId, DelayedTraceId, TraceBounds},
        },
    },
    trace::{Batch, BatchReader, BatchReaderFactories, Builder, Cursor, Spine, WithSnapshot},
    utils::Tup2,
};
use async_stream::stream;
use futures::Stream as AsyncStream;
use std::{borrow::Cow, cell::RefCell, cmp::Ordering, marker::PhantomData};

pub struct RowNumberCustomOrdFactories<K, V, OV>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    OV: DataTrait + ?Sized,
{
    inner_factories: OrdIndexedZSetFactories<K, V>,
    ranked_factories: RankedSpineFactories<K, V>,
    output_factories: OrdIndexedZSetFactories<K, OV>,
}

impl<K, V, OV> RowNumberCustomOrdFactories<K, V, OV>
where
    K: DataTrait + ?Sized,
    OV: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
{
    pub fn new<KType, VType, OVType>() -> Self
    where
        KType: DBData + Erase<K>,
        VType: DBData + Erase<V>,
        OVType: DBData + Erase<OV>,
    {
        Self {
            inner_factories: BatchReaderFactories::new::<KType, VType, ZWeight>(),
            ranked_factories: BatchReaderFactories::new::<
                KType,
                Tup2<VType, (RankType, RankType)>,
                ZWeight,
            >(),
            output_factories: BatchReaderFactories::new::<KType, OVType, ZWeight>(),
        }
    }
}

impl Stream<RootCircuit, MonoIndexedZSet> {
    pub fn dyn_row_number_custom_order_mono(
        &self,
        persistent_id: Option<&str>,
        factories: &RowNumberCustomOrdFactories<DynData, DynData, DynData>,
        encode: Box<dyn Fn(&DynData, &mut DynData)>,
        output_func: Box<dyn Fn(RankType, &DynData, &mut DynData)>,
    ) -> Stream<RootCircuit, MonoIndexedZSet> {
        self.dyn_row_number_custom_order(persistent_id, factories, encode, output_func)
    }
}

impl<K, V> Stream<RootCircuit, OrdIndexedZSet<K, V>>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
{
    /// See [`Stream::row_number_custom_order`].
    pub fn dyn_row_number_custom_order<V2, OV>(
        &self,
        persistent_id: Option<&str>,
        factories: &RowNumberCustomOrdFactories<K, V2, OV>,
        encode: Box<dyn Fn(&V, &mut V2)>,
        output_func: Box<dyn Fn(RankType, &V2, &mut OV)>,
    ) -> Stream<RootCircuit, OrdIndexedZSet<K, OV>>
    where
        V2: DataTrait + ?Sized,
        OV: DataTrait + ?Sized,
    {
        self.circuit().region("row_number", || {
            // The RowNumber operator outputs tuples of the form (key, (value, (row_number, num_rows))) with weight +1 or -1.
            // flat_map expands each such tuple into `num_rows` tuples:
            // `(key, (value, row_number)), (key, (value, row_number + 1)), ..., (key, (value, row_number + num_rows - 1))`.
            //
            //                                                          ┌─────────────────────┐
            //                                                   ┌─────►│flat_map(output_func)├────────►
            //                                                   │      └─────────────────────┘
            //                                             delta │          ^
            //                                                   │          . replay_stream
            //          ┌──────┐         ┌────────────────┐    ┌─┴───────┐  .  ┌─────────┐
            // ────────►│encode├────────►│shard_accumulate├───►│RowNumber├────►│integrate│
            //          └──────┘         └────────────────┘    └─────────┘     └────┬────┘
            //                                                     ▲                 │
            //                                                     │                 │
            //                                                     └─────────────────┘
            //
            let circuit = self.circuit();
            let bounds = <TraceBounds<K, DynPair<V2, DynData>>>::unbounded();

            let stream = self.try_sharded_version().dyn_map_index(
                &factories.inner_factories,
                Box::new(move |(k, v), kv| {
                    let (out_k, out_v) = kv.split_mut();
                    k.clone_to(out_k);
                    encode(v, out_v);
                }),
            );

            // The above map_index preserves sharding.
            stream.mark_sharded_if(self);

            let accumulated = stream.dyn_shard_accumulate(&factories.inner_factories);

            let (delayed_trace, z1feedback) = circuit.add_feedback_persistent(
                persistent_id
                    .map(|name| format!("{name}.numbered"))
                    .as_deref(),
                AccumulateZ1Trace::new(
                    &factories.ranked_factories,
                    &factories.ranked_factories,
                    false,
                    0,
                    bounds.clone(),
                ),
            );
            delayed_trace.mark_sharded();

            let delta = circuit.add_binary_operator(
                StreamingBinaryWrapper::new(RowNumber::new(&factories.ranked_factories)),
                &accumulated,
                &delayed_trace,
            );
            delta.mark_sharded();
            let replay_stream = z1feedback.operator_mut().prepare_replay_stream(&delta);

            let trace =
                circuit.add_binary_operator_with_preference(
                    <AccumulateTraceAppend<
                        Spine<RankedBatch<K, V2>>,
                        RankedBatch<K, V2>,
                        RootCircuit,
                    >>::new(&factories.ranked_factories, circuit.clone()),
                    (&delayed_trace, OwnershipPreference::STRONGLY_PREFER_OWNED),
                    (
                        &delta.dyn_accumulate(&factories.ranked_factories),
                        OwnershipPreference::PREFER_OWNED,
                    ),
                );
            trace.mark_sharded();

            z1feedback.connect_with_preference(&trace, OwnershipPreference::STRONGLY_PREFER_OWNED);

            register_replay_stream(circuit, &delta, &replay_stream);

            circuit.cache_insert(DelayedTraceId::new(trace.stream_id()), delayed_trace);
            circuit.cache_insert(AccumulateTraceId::new(delta.stream_id()), trace);
            circuit.cache_insert(
                BoundsId::<RankedBatch<K, V2>>::new(delta.stream_id()),
                bounds,
            );

            let mut out_k = factories.output_factories.key_factory().default_box();
            let mut out_v = factories.output_factories.val_factory().default_box();

            delta.dyn_flat_map_index(
                &factories.output_factories,
                Box::new(move |(k, v_rn), f| {
                    let (v, rn) = v_rn.split();
                    let (row_number, num_rows) = unsafe { rn.downcast::<(RankType, RankType)>() };
                    let mut row_number = *row_number;

                    for _ in 0..*num_rows {
                        k.clone_to(&mut out_k);
                        output_func(row_number, v, &mut out_v);
                        row_number += 1;
                        f(&mut out_k, &mut out_v);
                    }
                }),
            )
        })
    }
}

/// Iterate over values under `delta` and `trace` cursors jointly.
///
/// Tuples under the delta cursor have the standard form `(value, weight)`.
/// Tuples under the trace cursor have the form `((value, (row_number, num_rows)), +1)`.
/// Where `row_number` is the row number assigned to the first instance of `value` in the trace;
/// `num_rows` is the number of rows with the same value.
///
/// For each value present in at least one of the cursors, yield a tuple:
///
/// `(value, new_num_rows, Option<(old_row_number, old_num_rows)>)`
///
/// that can be used to retract any previous (value, (old_row_number, old_num_rows), +1) tuples
/// under the cursor and insert the new value instead.
///
/// - `new_num_rows` is the sum of the value's weight under delta_cursor and the row_number value
///   under the trace cursor (the sum can be 0).
/// - `old_row_number` - the row_number of the value in the trace cursor
/// - `old_num_rows` - the num_rows of the value in the trace cursor
struct JointRowNumCursor<'a, K, V, C1, C2>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    C1: Cursor<K, V, (), DynZWeight>,
    C2: Cursor<K, DynPair<V, DynData>, (), DynZWeight>,
{
    delta_cursor: &'a mut C1,

    /// Whether the delta cursor is valid.
    delta_valid: bool,

    /// Cursor over the trace of the numbered stream from the previous step.
    trace_cursor: &'a mut C2,

    /// Whether the trace cursor is valid.
    trace_valid: bool,

    /// Relative ordering of the values under the delta and trace cursors
    /// when both cursor are valid.
    ord: Ordering,

    phantom: PhantomData<fn(&K, &V)>,
}

impl<'a, K, V, C1, C2> JointRowNumCursor<'a, K, V, C1, C2>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    C1: Cursor<K, V, (), DynZWeight>,
    C2: Cursor<K, DynPair<V, DynData>, (), DynZWeight>,
{
    fn new(delta_cursor: &'a mut C1, trace_cursor: &'a mut C2) -> Self {
        let delta_valid = delta_cursor.val_valid();
        let trace_valid = trace_cursor.val_valid();

        let ord = if delta_valid && trace_valid {
            delta_cursor.val().cmp(trace_cursor.val().fst())
        } else {
            Ordering::Equal
        };

        Self {
            delta_cursor,
            delta_valid,
            trace_cursor,
            trace_valid,
            ord,
            phantom: PhantomData,
        }
    }

    fn step_val(&mut self) {
        if self.delta_valid && self.trace_valid {
            match self.ord {
                Ordering::Less => {
                    self.delta_cursor.step_val();
                    self.delta_valid = self.delta_cursor.val_valid();
                }
                Ordering::Equal => {
                    self.trace_cursor.step_val();
                    self.trace_valid = self.trace_cursor.val_valid();
                    self.delta_cursor.step_val();
                    self.delta_valid = self.delta_cursor.val_valid();
                }
                Ordering::Greater => {
                    self.trace_cursor.step_val();
                    self.trace_valid = self.trace_cursor.val_valid();
                }
            }
            if self.delta_valid && self.trace_valid {
                self.ord = self.delta_cursor.val().cmp(self.trace_cursor.val().fst());
            }
        } else if self.delta_valid {
            self.delta_cursor.step_val();
            self.delta_valid = self.delta_cursor.val_valid();
        } else if self.trace_valid {
            self.trace_cursor.step_val();
            self.trace_valid = self.trace_cursor.val_valid();
        } else {
            panic!("Both cursors are invalid");
        }
    }

    fn get(&mut self) -> Option<(&V, ZWeight, Option<(RankType, RankType)>)> {
        if self.delta_valid && self.trace_valid {
            match self.ord {
                // Value is only present in the delta cursor.
                Ordering::Less => {
                    let weight = **self.delta_cursor.weight();
                    Some((self.delta_cursor.val(), weight, None))
                }
                // Value is present in both cursors.
                Ordering::Equal => {
                    let delta_weight = **self.delta_cursor.weight();
                    debug_assert_eq!(**self.trace_cursor.weight(), 1);
                    let (val, rn) = self.trace_cursor.val().split();
                    let (row_number, num_rows) = *unsafe { rn.downcast::<(RankType, RankType)>() };
                    Some((val, delta_weight + num_rows, Some((row_number, num_rows))))
                }
                // Value is only present in the trace cursor.
                Ordering::Greater => {
                    debug_assert_eq!(**self.trace_cursor.weight(), 1);
                    let (val, rn) = self.trace_cursor.val().split();
                    let (row_number, num_rows) = *unsafe { rn.downcast::<(RankType, RankType)>() };
                    Some((val, num_rows, Some((row_number, num_rows))))
                }
            }
        } else if self.delta_valid {
            let weight = **self.delta_cursor.weight();
            Some((self.delta_cursor.val(), weight, None))
        } else if self.trace_valid {
            debug_assert_eq!(**self.trace_cursor.weight(), 1);
            let (val, rn) = self.trace_cursor.val().split();
            let (row_number, num_rows) = *unsafe { rn.downcast::<(RankType, RankType)>() };
            Some((val, num_rows, Some((row_number, num_rows))))
        } else {
            None
        }
    }

    fn key(&self) -> &K {
        self.delta_cursor.key()
    }

    fn position(&self) -> Option<Position> {
        self.delta_cursor.position()
    }
}

struct RowNumber<K: DataTrait + ?Sized, V: DataTrait + ?Sized> {
    batch_factories: RankedSpineFactories<K, V>,

    // Input batch sizes.
    input_batch_stats: RefCell<BatchSizeStats>,

    // Output batch sizes.
    output_batch_stats: RefCell<BatchSizeStats>,

    _phantom: PhantomData<fn(&K, &V)>,
}

impl<K, V> RowNumber<K, V>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
{
    fn new(batch_factories: &RankedSpineFactories<K, V>) -> Self {
        Self {
            batch_factories: batch_factories.clone(),
            input_batch_stats: RefCell::new(BatchSizeStats::new()),
            output_batch_stats: RefCell::new(BatchSizeStats::new()),
            _phantom: PhantomData,
        }
    }

    fn push_val_diff(
        &self,
        builder: &mut <RankedBatch<K, V> as Batch>::Builder,
        val: &V,
        rn: (RankType, RankType),
        weight: ZWeight,
        out_val: &mut Box<DynPair<V, DynData>>,
    ) {
        let (out_v, out_rn) = out_val.split_mut();
        val.clone_to(out_v);
        Erase::<DynData>::erase(&rn).clone_to(out_rn);
        builder.push_val_diff(out_val.as_ref(), weight.erase());
    }
}

impl<K, V> Operator for RowNumber<K, V>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("row_number")
    }

    fn metadata(&self, meta: &mut OperatorMeta) {
        meta.extend(metadata! {
            INPUT_BATCHES_STATS => self.input_batch_stats.borrow().metadata(),
            OUTPUT_BATCHES_STATS => self.output_batch_stats.borrow().metadata(),
        });
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<K, V>
    StreamingBinaryOperator<
        Option<Spine<OrdIndexedZSet<K, V>>>,
        Spine<RankedBatch<K, V>>,
        RankedBatch<K, V>,
    > for RowNumber<K, V>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
{
    fn eval(
        self: std::rc::Rc<Self>,
        delta: &Option<Spine<OrdIndexedZSet<K, V>>>,
        delayed_trace: &Spine<RankedBatch<K, V>>,
    ) -> impl AsyncStream<Item = (RankedBatch<K, V>, bool, Option<Position>)> + 'static {
        let chunk_size = splitter_output_chunk_size();

        // println!(
        //     "{}: AggregateIncremental::eval({delta:?})",
        //     Runtime::worker_index()
        // );
        let delta = delta.as_ref().map(|b| b.ro_snapshot());

        // We assume that delta.is_some() implies that the operator is being flushed,
        // since the integral is always flushed in the same step as delta.
        let input_trace = if delta.is_some() {
            Some(delayed_trace.ro_snapshot())
        } else {
            None
        };

        stream! {
            let Some(delta) = delta else {
                // println!("yield empty");
                yield (RankedBatch::dyn_empty(&self.batch_factories), true, None);
                return;
            };

            self.input_batch_stats.borrow_mut().add_batch(delta.len());

            // println!("delta");
            // for (k, v, w) in delta.iter() {
            //     println!(" ({:?}, {:?}, {:?})", k, v, w);
            // }
            // println!("input_trace:");
            // for (k, v, w) in input_trace.as_ref().unwrap().iter() {
            //     println!(" ({:?}, {:?}, {:?})", k, v, w);
            // }

            let mut delta_cursor = delta.cursor();
            let mut input_trace_cursor = input_trace.unwrap().cursor();
            let mut builder = <RankedBatch::<K, V> as Batch>::Builder::with_capacity(&self.batch_factories, chunk_size + 1, chunk_size + 1);

            let mut out_val = self.batch_factories.val_factory().default_box();

            while delta_cursor.key_valid() {
                //println!("Processing key: {:?}", delta_cursor.key());
                debug_assert!(delta_cursor.val_valid());

                if input_trace_cursor.seek_key_exact(delta_cursor.key(), None) {
                    // Value is present in the trace cursor:
                    // 1. Find the first value in the trace just below the value in the delta cursor (call its row number R0), use it to
                    //    compute the first row number R1 to start counting from.
                    // 2. Scan the joint delta + trace cursor from the current position onward. Retract all values currently in the trace
                    //    and reinsert them with the new row numbers.

                    let mut has_values = false;

                    let first_delta_val = delta_cursor.val();
                    //println!("First delta value: {:?}", val);

                    // Step 1: Compute R0 and R1.

                    // Search backward to the first value smaller than the current delta value. This is the value with row number R0,
                    // i.e., the largest existing value whose rank is not affected by the change.
                    input_trace_cursor.fast_forward_vals();
                    input_trace_cursor.seek_val_with_reverse(&|v| v.fst() < first_delta_val);

                    let mut current_row_number: RankType = if input_trace_cursor.val_valid() {
                        debug_assert_eq!(**input_trace_cursor.weight(), 1);
                        let (_v, rn) = input_trace_cursor.val().split();

                        let (row_number0, num_rows) = *unsafe { rn.downcast::<(RankType, RankType)>() };

                        input_trace_cursor.rewind_vals();
                        input_trace_cursor.seek_val_with(&|v| v.fst() >= first_delta_val);

                        row_number0 + num_rows
                    } else {
                        input_trace_cursor.rewind_vals();
                        1
                    };

                    // Step 2: Recompute all row numbers from the current position onward.
                    let mut joint_cursor = JointRowNumCursor::new(&mut delta_cursor, &mut input_trace_cursor);

                    while let Some((val, new_num_rows, old)) = joint_cursor.get()  {

                        // If the value is present in the joint cursor with a non-zero weight, prepare an insertion with the new rank and weight.
                        let insertion: Option<(&V, (RankType, RankType))> = if new_num_rows != 0 {
                            Some((val, (current_row_number, new_num_rows)))
                        } else {
                            None
                        };

                        // This only matters if the input stream has negative weights.
                        // The flat_map operator will drop output records with negative row_numbers.
                        // Here we make sure we don't introduce negative or duplicate numbers by
                        // moving the counter backward.
                        if new_num_rows > 0 {
                            current_row_number += new_num_rows;
                        }

                        // Retract any old (value, row_number, weight) tuple.
                        let retraction: Option<(&V, (RankType, RankType))> = if let Some((old_rank, old_num_rows)) = old {
                            debug_assert_ne!(old_num_rows, 0);
                            //println!("Retraction: {:?}", (val, old_rank, -old_weight));
                            Some((val, (old_rank, old_num_rows)))
                        } else {
                            None
                        };

                        // We are using a Builder, which expects ordered and consolidated inputs.
                        // We have up to two updates we want to push (insertion and retraction), so we need to
                        // sort and consolidate them.
                        match (insertion, retraction) {
                            (Some((v, (rank, num_rows))), None) => {
                                debug_assert_ne!(num_rows, 0);
                                self.push_val_diff(&mut builder, v, (rank, num_rows), 1, &mut out_val);
                                has_values = true;
                            }
                            (None, Some((v, (retract_rank, retract_num_rows)))) => {
                                debug_assert_ne!(retract_num_rows, 0);

                                self.push_val_diff(&mut builder, v, (retract_rank, retract_num_rows), -1, &mut out_val);
                                has_values = true;
                            }
                            (Some((new_v, (new_rank, new_num_rows))), Some((retract_v, (retract_rank, retract_num_rows)))) => {
                                debug_assert_ne!(new_num_rows, 0);
                                debug_assert_ne!(retract_num_rows, 0);
                                debug_assert_eq!(new_v, retract_v);

                                // The new and old values are identical: break the tie based on the ranks.
                                // If ranks are the same, consolidate the weights.
                                match (new_rank, new_num_rows).cmp(&(retract_rank, retract_num_rows)) {
                                    Ordering::Less => {
                                        // Insert, then retract
                                        //println!("Insert, then retract: {:?}", (new_v, new_rank, new_weight, retract_v, retract_rank, retract_weight));
                                        self.push_val_diff(&mut builder, new_v, (new_rank, new_num_rows), 1, &mut out_val);
                                        self.push_val_diff(&mut builder, retract_v, (retract_rank, retract_num_rows), -1, &mut out_val);
                                        has_values = true;
                                    }
                                    Ordering::Equal => {}
                                    Ordering::Greater => {
                                        //println!("Retract, then insert: {:?}", (retract_v, retract_rank, retract_weight, new_v, new_rank, new_weight));
                                        // Retract, then insert
                                        self.push_val_diff(&mut builder, retract_v, (retract_rank, retract_num_rows), -1, &mut out_val);
                                        self.push_val_diff(&mut builder, new_v, (new_rank, new_num_rows), 1, &mut out_val);
                                        has_values = true;
                                    }
                                }
                            }
                            (None, None) => {
                            }
                        }

                        joint_cursor.step_val();

                        if builder.num_tuples() >= chunk_size {
                            if has_values {
                                builder.push_key(joint_cursor.key());
                                has_values = false;
                            }
                            let result = builder.done();
                            self.output_batch_stats.borrow_mut().add_batch(result.len());
                            yield (result, false, joint_cursor.position());
                            builder = <RankedBatch::<K, V> as Batch>::Builder::with_capacity(&self.batch_factories, chunk_size + 1, chunk_size + 1);
                        }
                    }

                    if has_values {
                        builder.push_key(delta_cursor.key());
                    }

                } else {
                    // New key: compute row numbers from scratch for values in delta_cursor.
                    let mut current_row_number: RankType = 1;
                    let mut has_values = false;

                    while delta_cursor.val_valid() {
                        let weight = **delta_cursor.weight();

                        self.push_val_diff(&mut builder, delta_cursor.val(), (current_row_number, weight), 1, &mut out_val);
                        has_values = true;
                        current_row_number += weight;

                        if builder.num_tuples() >= chunk_size {
                            builder.push_key(delta_cursor.key());
                            has_values = false;
                            let result = builder.done();
                            self.output_batch_stats.borrow_mut().add_batch(result.len());
                            yield (result, false, delta_cursor.position());
                            builder = <RankedBatch::<K, V> as Batch>::Builder::with_capacity(&self.batch_factories, chunk_size + 1, chunk_size + 1);
                        }

                        delta_cursor.step_val();
                    }
                    if has_values {
                        builder.push_key(delta_cursor.key());
                    }

                }
                delta_cursor.step_key();
            }

            let result = builder.done();
            self.output_batch_stats.borrow_mut().add_batch(result.len());
            yield (result, true, None);
        }
    }
}
