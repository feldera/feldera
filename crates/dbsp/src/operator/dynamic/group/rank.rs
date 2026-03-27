use crate::{
    Circuit, DBData, DynZWeight, Position, RootCircuit, Scope, Stream, ZWeight,
    algebra::{OrdIndexedZSet, OrdIndexedZSetFactories},
    circuit::{
        OwnershipPreference, circuit_builder::register_replay_stream, metadata::OperatorMeta,
        operator_traits::Operator, splitter_output_chunk_size,
    },
    dynamic::{
        ClonableTrait, DataTrait, DowncastTrait, DynData, DynPair, Erase, Factory, WithFactory,
    },
    operator::{
        async_stream_operators::{StreamingBinaryOperator, StreamingBinaryWrapper},
        dynamic::{
            MonoIndexedZSet,
            accumulate_trace::{AccumulateTraceAppend, AccumulateTraceId, AccumulateZ1Trace},
            trace::{BoundsId, DelayedTraceId, TraceBounds},
        },
    },
    trace::{Batch, BatchReader, BatchReaderFactories, Builder, Cursor, Spine, WithSnapshot},
    utils::Tup2,
};
use async_stream::stream;
use futures::Stream as AsyncStream;
use std::{borrow::Cow, cmp::Ordering, marker::PhantomData};

/// A ranked batch pairs a value with its rank.
type RankedBatch<K, V> = OrdIndexedZSet<K, DynPair<V, DynData>>;
type RankedSpineFactories<K, V> = OrdIndexedZSetFactories<K, DynPair<V, DynData>>;

/// SQL ranks are bigints.
type RankType = i64;

/// Function that updates the rank of a record based on the total weight of records with the previous rank.
///
/// Generalizes RANK and DENSE_RANK behavior.
pub trait RankUpdateFunc: Send + Sync + 'static {
    fn update_rank(base_rank: RankType, weight: ZWeight) -> RankType;
}

/// Conventional (non-dense) rank increases by the total weight of records with the previous rank.
struct UpdateRank;

impl RankUpdateFunc for UpdateRank {
    fn update_rank(base_rank: RankType, weight: ZWeight) -> RankType {
        base_rank.checked_add(weight).expect("rank overflow")
    }
}

/// Dense rank always increases by 1.
struct UpdateDenseRank;
impl RankUpdateFunc for UpdateDenseRank {
    fn update_rank(base_rank: RankType, _weight: ZWeight) -> RankType {
        base_rank + 1
    }
}

pub struct RankCustomOrdFactories<K, V, RV, OV>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    RV: DataTrait + ?Sized,
    OV: DataTrait + ?Sized,
{
    val_factory: &'static dyn Factory<RV>,
    inner_factories: OrdIndexedZSetFactories<K, V>,
    ranked_factories: RankedSpineFactories<K, V>,
    output_factories: OrdIndexedZSetFactories<K, OV>,
}

impl<K, V, RV, OV> RankCustomOrdFactories<K, V, RV, OV>
where
    K: DataTrait + ?Sized,
    RV: DataTrait + ?Sized,
    OV: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
{
    pub fn new<KType, VType, RVType, OVType>() -> Self
    where
        KType: DBData + Erase<K>,
        VType: DBData + Erase<V>,
        RVType: DBData + Erase<RV>,
        OVType: DBData + Erase<OV>,
    {
        Self {
            val_factory: WithFactory::<RVType>::FACTORY,
            inner_factories: BatchReaderFactories::new::<KType, VType, ZWeight>(),
            ranked_factories: BatchReaderFactories::new::<KType, Tup2<VType, RankType>, ZWeight>(),
            output_factories: BatchReaderFactories::new::<KType, OVType, ZWeight>(),
        }
    }
}

impl Stream<RootCircuit, MonoIndexedZSet> {
    pub fn dyn_rank_custom_order_mono(
        &self,
        persistent_id: Option<&str>,
        factories: &RankCustomOrdFactories<DynData, DynData, DynData, DynData>,
        encode: Box<dyn Fn(&DynData, &mut DynData)>,
        projection_func: Box<dyn Fn(&DynData, &mut DynData)>,
        rank_cmp_func: Box<dyn Fn(&DynData, &DynData) -> Ordering>,
        output_func: Box<dyn Fn(RankType, &DynData, &mut DynData)>,
    ) -> Stream<RootCircuit, MonoIndexedZSet> {
        self.dyn_rank_custom_order::<_, _, _, UpdateRank>(
            persistent_id,
            factories,
            encode,
            projection_func,
            rank_cmp_func,
            output_func,
        )
    }

    pub fn dyn_dense_rank_custom_order_mono(
        &self,
        persistent_id: Option<&str>,
        factories: &RankCustomOrdFactories<DynData, DynData, DynData, DynData>,
        encode: Box<dyn Fn(&DynData, &mut DynData)>,
        projection_func: Box<dyn Fn(&DynData, &mut DynData)>,
        rank_cmp_func: Box<dyn Fn(&DynData, &DynData) -> Ordering>,
        output_func: Box<dyn Fn(RankType, &DynData, &mut DynData)>,
    ) -> Stream<RootCircuit, MonoIndexedZSet> {
        self.dyn_rank_custom_order::<_, _, _, UpdateDenseRank>(
            persistent_id,
            factories,
            encode,
            projection_func,
            rank_cmp_func,
            output_func,
        )
    }

    // pub fn dyn_row_number_custom_order_mono(
    //     &self,
    //     persistent_id: Option<&str>,
    //     factories: &RankCustomOrdFactories<DynData, DynData, DynData>,
    //     encode: Box<dyn Fn(&DynData, &mut DynData)>,
    //     output_func: Box<dyn Fn(i64, &DynData, &mut DynData)>,
    // ) -> Stream<RootCircuit, MonoIndexedZSet> {
    //     self.dyn_row_number_custom_order(persistent_id, factories, encode, output_func)
    // }
}

impl<K, V> Stream<RootCircuit, OrdIndexedZSet<K, V>>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
{
    /// See [`Stream::rank_custom_order`].
    pub fn dyn_rank_custom_order<V2, RV, OV, RF>(
        &self,
        persistent_id: Option<&str>,
        factories: &RankCustomOrdFactories<K, V2, RV, OV>,
        encode: Box<dyn Fn(&V, &mut V2)>,
        projection_func: Box<dyn Fn(&V2, &mut RV)>,
        rank_cmp_func: Box<dyn Fn(&V2, &RV) -> Ordering>,
        output_func: Box<dyn Fn(RankType, &V2, &mut OV)>,
    ) -> Stream<RootCircuit, OrdIndexedZSet<K, OV>>
    where
        RV: DataTrait + ?Sized,
        RF: RankUpdateFunc,
        V2: DataTrait + ?Sized,
        OV: DataTrait + ?Sized,
    {
        self.circuit().region("rank", || {
            // The `rank` operator preserves the input stream, only adding a new rank column to it; therefore
            // it only requires maintaining the output integral:
            //                                                          ┌────────────────┐
            //                                                   ┌─────►│map(output_func)├────────►
            //                                                   │    .►└────────────────┘
            //                                             delta │    .
            //                                                   │    . replay_stream
            //          ┌──────┐         ┌────────────────┐    ┌─┴──┐ .   ┌─────────┐
            // ────────►│encode├────────►│shard_accumulate├───►│rank├────►│integrate│
            //          └──────┘         └────────────────┘    └────┘     └────┬────┘
            //                                                   ▲             │
            //                                                   │             │
            //                                                   └─────────────┘
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

            let stream = stream.dyn_shard(&factories.inner_factories);

            let accumulated = stream.dyn_accumulate(&factories.inner_factories);

            let (delayed_trace, z1feedback) = circuit.add_feedback_persistent(
                persistent_id
                    .map(|name| format!("{name}.ranked"))
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
                StreamingBinaryWrapper::new(Rank::<_, _, _, _, _, RF>::new(
                    factories.val_factory,
                    &factories.ranked_factories,
                    projection_func,
                    rank_cmp_func,
                )),
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

            delta.dyn_map_index(
                &factories.output_factories,
                Box::new(move |(k, v), kv| {
                    let (out_k, out_v) = kv.split_mut();
                    let (v2, rank) = v.split();
                    k.clone_to(out_k);
                    output_func(*unsafe { rank.downcast::<RankType>() }, v2, out_v);
                }),
            )
        })
    }

    // /// See [`Stream::row_number_custom_order`].
    // pub fn dyn_row_number_custom_order<V2, OV>(
    //     &self,
    //     persistent_id: Option<&str>,
    //     factories: &RankCustomOrdFactories<K, V2, OV>,
    //     encode: Box<dyn Fn(&V, &mut V2)>,
    //     output_func: Box<dyn Fn(i64, &V2, &mut OV)>,
    // ) -> Stream<RootCircuit, OrdIndexedZSet<K, OV>>
    // where
    //     V2: DataTrait + ?Sized,
    //     OV: DataTrait + ?Sized,
    // {
    //     todo!()
    //     // self.circuit().region(&format!("topk_row_number_{k}"), || {
    //     //     self.dyn_map_index(
    //     //         &factories.inner_factories,
    //     //         Box::new(move |(k, v), kv| {
    //     //             let (out_k, out_v) = kv.split_mut();
    //     //             k.clone_to(out_k);
    //     //             encode(v, out_v);
    //     //         }),
    //     //     )
    //     //     .set_persistent_id(
    //     //         persistent_id
    //     //             .map(|name| format!("{name}-ordered"))
    //     //             .as_deref(),
    //     //     )
    //     //     .dyn_group_transform(
    //     //         persistent_id,
    //     //         &factories.inner_factories,
    //     //         &factories.output_factories,
    //     //         Box::new(DiffGroupTransformer::new(
    //     //             factories.output_factories.val_factory(),
    //     //             TopKRowNumber::new(factories.output_factories.val_factory(), k, output_func),
    //     //         )),
    //     //     )
    //     // })
    // }
}

/// Implements both `rank` and `dense_rank` operators.
struct Rank<
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    RV: DataTrait + ?Sized,
    PF,
    KCF,
    RF: RankUpdateFunc,
> {
    /// Function that projects the value to a value that is used for the ranking
    /// (typically the columns listed in the ORDER BY clause).
    projection_func: PF,

    /// Value comparison function.
    key_cmp: KCF,

    val_factory: &'static dyn Factory<RV>,
    batch_factories: RankedSpineFactories<K, V>,
    _phantom: PhantomData<fn(&K, &V, &RV, &PF, &RF)>,
}

impl<K, V, RV, PF, KCF, RF> Rank<K, V, RV, PF, KCF, RF>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    RV: DataTrait + ?Sized,
    PF: Fn(&V, &mut RV) + 'static,
    KCF: Fn(&V, &RV) -> Ordering + 'static,
    RF: RankUpdateFunc,
{
    fn new(
        val_factory: &'static dyn Factory<RV>,
        batch_factories: &RankedSpineFactories<K, V>,
        projection_func: PF,
        key_cmp: KCF,
    ) -> Self {
        Self {
            projection_func,
            key_cmp,
            val_factory,
            batch_factories: batch_factories.clone(),
            _phantom: PhantomData,
        }
    }

    fn push_val_diff(
        &self,
        builder: &mut <RankedBatch<K, V> as Batch>::Builder,
        val: &V,
        rank: RankType,
        weight: ZWeight,
        out_val: &mut Box<DynPair<V, DynData>>,
    ) {
        let (out_v, out_rank) = out_val.split_mut();
        val.clone_to(out_v);
        Erase::<DynData>::erase(&rank).clone_to(out_rank);
        builder.push_val_diff(out_val.as_ref(), weight.erase());
    }
}

impl<K, V, RV, PF, KCF, RF> Operator for Rank<K, V, RV, PF, KCF, RF>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    RV: DataTrait + ?Sized,
    PF: Fn(&V, &mut RV) + 'static,
    KCF: Fn(&V, &RV) -> Ordering + 'static,
    RF: RankUpdateFunc,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("rank")
    }

    fn metadata(&self, _meta: &mut OperatorMeta) {}

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

/// Iterate over values under `delta` and `trace` cursors jointly.
///
/// For each value present in at least one of the cursors, yield a tuple:
///
/// `(value, new_weight, Option<(old_rank, old_weight)>)`
///
/// that can be used to retract any previous (value, rank, weight) triple
/// under the cursor and insert the new value instead.
///
/// - `new_weight` is the sum of the value's weights in both tuples
///   (which can be 0).
/// - `old_rank` - the rank of the value in the trace cursor
/// - `old_weight` - the weight of the value in the trace cursor
///
struct JointCursor<'a, K, V, C1, C2>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    C1: Cursor<K, V, (), DynZWeight>,
    C2: Cursor<K, DynPair<V, DynData>, (), DynZWeight>,
{
    delta_cursor: &'a mut C1,

    /// Whether the delta cursor is valid.
    delta_valid: bool,

    /// Cursor over the trace of the ranked stream from the previous step.
    trace_cursor: &'a mut C2,

    /// Whether the trace cursor is valid.
    trace_valid: bool,

    /// Relative ordering of the values under the delta and trace cursors
    /// when both cursor are valid.
    ord: Ordering,

    phantom: PhantomData<fn(&K, &V)>,
}

impl<'a, K, V, C1, C2> JointCursor<'a, K, V, C1, C2>
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

    fn get(&mut self) -> Option<(&V, ZWeight, Option<(RankType, ZWeight)>)> {
        if self.delta_valid && self.trace_valid {
            match self.ord {
                // Value is only present in the delta cursor.
                Ordering::Less => {
                    let weight = **self.delta_cursor.weight();
                    Some((self.delta_cursor.val(), weight, None))
                }
                // Value is present in both cursors.
                Ordering::Equal => {
                    let weight1 = **self.delta_cursor.weight();
                    let weight2 = **self.trace_cursor.weight();
                    let weight = weight1 + weight2;
                    let (val, rank) = self.trace_cursor.val().split();
                    let rank = *unsafe { rank.downcast::<RankType>() };
                    Some((val, weight, Some((rank, weight2))))
                }
                // Value is only present in the trace cursor.
                Ordering::Greater => {
                    let weight = **self.trace_cursor.weight();
                    let (val, rank) = self.trace_cursor.val().split();
                    let rank = *unsafe { rank.downcast::<RankType>() };
                    Some((val, weight, Some((rank, weight))))
                }
            }
        } else if self.delta_valid {
            let weight = **self.delta_cursor.weight();
            Some((self.delta_cursor.val(), weight, None))
        } else if self.trace_valid {
            let weight = **self.trace_cursor.weight();
            let (val, rank) = self.trace_cursor.val().split();
            let rank = *unsafe { rank.downcast::<RankType>() };
            Some((val, weight, Some((rank, weight))))
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

impl<K, V, RV, PF, KCF, RF>
    StreamingBinaryOperator<
        Option<Spine<OrdIndexedZSet<K, V>>>,
        Spine<RankedBatch<K, V>>,
        RankedBatch<K, V>,
    > for Rank<K, V, RV, PF, KCF, RF>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    RV: DataTrait + ?Sized,
    PF: Fn(&V, &mut RV) + 'static,
    KCF: Fn(&V, &RV) -> Ordering + 'static,
    RF: RankUpdateFunc,
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
            let mut current_val = self.val_factory.default_box();

            while delta_cursor.key_valid() {
                //println!("Processing key: {:?}", delta_cursor.key());
                debug_assert!(delta_cursor.val_valid());

                let mut current_val_valid = false;

                if input_trace_cursor.seek_key_exact(delta_cursor.key(), None) {
                    // Value is present in the trace cursor:
                    // 1. Find the first values in the trace just below the value in the delta cursor (call its rank R0).
                    // 2. Scan all values in the trace with rank R0 that are smaller than the current value under the delta cursor to
                    //    compute their total weight, which will then be used to compute the rank for the first value in the delta cursor.
                    // 3. Scan the joint delta + trace cursor from the current position onward. Retract all values currently in the trace
                    //    and reinsert them with the new rank and weight (unless the rank and weight don't change).

                    let mut has_values = false;

                    let first_delta_val = delta_cursor.val();
                    //println!("First delta value: {:?}", val);

                    // Step 1: Compute R0 and R1.

                    // Search backward to the first value smaller than the current delta value. This is the value with rank R0,
                    // i.e., the largest existing value whose rank is not affected by the change.
                    input_trace_cursor.fast_forward_vals();
                    input_trace_cursor.seek_val_with_reverse(&|v| v.fst() < first_delta_val);

                    // Total weight of values with the current rank.
                    let mut weight: ZWeight = 0;

                    // Step 2: Find the first value with rank R0 and step forward from it to compute
                    // the total weight of values with the same rank that are smaller than the current delta value.
                    let mut current_rank: RankType = if input_trace_cursor.val_valid() {
                        let (v, rank) = input_trace_cursor.val().split();

                        let rank0 = *unsafe { rank.downcast::<RankType>() };

                        (self.projection_func)(v, &mut current_val);
                        current_val_valid = true;
                        input_trace_cursor.rewind_vals();
                        input_trace_cursor.seek_val_with(&|v| (self.key_cmp)(v.fst(), &*current_val) != Ordering::Less);
                        debug_assert!(input_trace_cursor.val_valid());
                        debug_assert_eq!((self.key_cmp)(input_trace_cursor.val().fst(), &*current_val), Ordering::Equal);

                        loop {
                            weight += **input_trace_cursor.weight();

                            input_trace_cursor.step_val();
                            if let Some(v) = input_trace_cursor.get_val() {
                                if v.fst() >= first_delta_val {
                                    break;
                                }
                            } else {
                                break;
                            }
                        }

                        rank0

                    } else {
                        input_trace_cursor.rewind_vals();
                        1
                    };

                    // Step 3: Recompute all ranks from the current position onward.
                    let mut joint_cursor = JointCursor::new(&mut delta_cursor, &mut input_trace_cursor);

                    while let Some((val, new_weight, old)) = joint_cursor.get()  {

                        // If the value is present in the joint cursor with a non-zero weight, prepare an insertion with the new rank and weight.
                        let insertion: Option<(&V, RankType, ZWeight)> = if new_weight != 0 {
                            if !current_val_valid {
                                (self.projection_func)(val, &mut current_val);
                                current_val_valid = true;
                            }

                            // Check if the current value should have the same rank as the previous value. If not, advance current_rank.
                            //
                            // At this point:
                            // - `weight` is the total weight of values with the current rank.
                            // - `current_val` contains the projection of the previous value on the columns used for ranking.
                            // - `current_rank` is the rank of the previous value.
                            // We use these values to compute the rank of the new value.
                            if weight > 0 && (self.key_cmp)(val, &*current_val) != Ordering::Equal {
                                //println!("rank change: prev_val: {:?}, val: {:?}", &*prev_val, val);
                                debug_assert_eq!((self.key_cmp)(val, &*current_val), Ordering::Greater);
                                current_rank = RF::update_rank(current_rank, weight);
                                (self.projection_func)(val, &mut current_val);
                                weight = 0;
                            }

                            weight += new_weight;

                            Some((val, current_rank, new_weight))
                        } else {
                            None
                        };

                        // Retract any old (value, rank, weight) tuple.
                        let retraction: Option<(&V, RankType, ZWeight)> = if let Some((old_rank, old_weight)) = old {
                            debug_assert_ne!(old_weight, 0);
                            //println!("Retraction: {:?}", (val, old_rank, -old_weight));
                            Some((val, old_rank, -old_weight))
                        } else {
                            None
                        };

                        // We are using a Builder, which expects ordered and consolidated inputs.
                        // We have up to two updates we want to push (insertion and retraction), so we need to
                        // sort and consolidate them.
                        match (insertion, retraction) {
                            (Some((v, rank, weight)), None) => {
                                debug_assert_ne!(weight, 0);
                                self.push_val_diff(&mut builder, v, rank, weight, &mut out_val);
                                has_values = true;
                            }
                            (None, Some((v, rank, weight))) => {
                                debug_assert_ne!(weight, 0);

                                self.push_val_diff(&mut builder, v, rank, weight, &mut out_val);
                                has_values = true;
                            }
                            (Some((new_v, new_rank, new_weight)), Some((retract_v, retract_rank, retract_weight))) => {
                                debug_assert_ne!(new_weight, 0);
                                debug_assert_ne!(retract_weight, 0);
                                debug_assert_eq!(new_v, retract_v);

                                // The new and old values are identical: break the tie based on the ranks.
                                // If ranks are the same, consolidate the weights.
                                match new_rank.cmp(&retract_rank) {
                                    Ordering::Less => {
                                        // Insert, then retract
                                        //println!("Insert, then retract: {:?}", (new_v, new_rank, new_weight, retract_v, retract_rank, retract_weight));
                                        self.push_val_diff(&mut builder, new_v, new_rank, new_weight, &mut out_val);
                                        self.push_val_diff(&mut builder, retract_v, retract_rank, retract_weight, &mut out_val);
                                        has_values = true;
                                    }
                                    Ordering::Equal => {
                                        let weight = new_weight + retract_weight;
                                        if weight != 0 {
                                            // insert with weight
                                            self.push_val_diff(&mut builder, new_v, new_rank, weight, &mut out_val);
                                            has_values = true;
                                        }
                                    }
                                    Ordering::Greater => {
                                        //println!("Retract, then insert: {:?}", (retract_v, retract_rank, retract_weight, new_v, new_rank, new_weight));
                                        // Retract, then insert
                                        self.push_val_diff(&mut builder, retract_v, retract_rank, retract_weight, &mut out_val);
                                        self.push_val_diff(&mut builder, new_v, new_rank, new_weight, &mut out_val);
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
                            yield (result, false, joint_cursor.position());
                            builder = <RankedBatch::<K, V> as Batch>::Builder::with_capacity(&self.batch_factories, chunk_size + 1, chunk_size + 1);
                        }
                    }

                    if has_values {
                        builder.push_key(delta_cursor.key());
                    }

                } else {
                    // New key: compute rank from scratch for values in delta_cursor.
                    let mut current_rank: RankType = 1;
                    let mut weight: ZWeight = 0;

                    if delta_cursor.val_valid() {
                        let mut new_weight = **delta_cursor.weight();

                        let mut val = delta_cursor.val();
                        (self.projection_func)(val, &mut current_val);

                        let mut has_values;

                        loop {
                            weight += new_weight;
                            self.push_val_diff(&mut builder, val, current_rank, new_weight, &mut out_val);
                            has_values = true;
                            delta_cursor.step_val();

                            if builder.num_tuples() >= chunk_size {
                                builder.push_key(delta_cursor.key());
                                has_values = false;
                                let result = builder.done();
                                yield (result, false, delta_cursor.position());
                                builder = <RankedBatch::<K, V> as Batch>::Builder::with_capacity(&self.batch_factories, chunk_size + 1, chunk_size + 1);
                            }

                            if !delta_cursor.val_valid() {
                                break;
                            }
                            new_weight = **delta_cursor.weight();

                            val = delta_cursor.val();
                            if (self.key_cmp)(val, &*current_val) != Ordering::Equal {
                                debug_assert_eq!((self.key_cmp)(val, &*current_val), Ordering::Greater);
                                current_rank = RF::update_rank(current_rank, weight);
                                (self.projection_func)(val, &mut current_val);
                                weight = 0;
                            }
                        }
                        if has_values {
                            builder.push_key(delta_cursor.key());
                        }
                    }
                }
                delta_cursor.step_key();
            }

            let result = builder.done();
            yield (result, true, None);
        }
    }
}

#[cfg(test)]
mod test {
    use std::{
        cmp::Ordering,
        collections::{BTreeMap, HashMap},
    };

    use anyhow::Result as AnyResult;
    use proptest::prelude::*;
    use rand::{Rng, SeedableRng, rngs::StdRng};

    use tempfile::tempdir;

    use crate::{
        DBData, OrdIndexedZSet, RootCircuit, Runtime, ZWeight,
        algebra::HasZero,
        circuit::{
            CircuitConfig, CircuitStorageConfig, StorageCacheConfig, StorageConfig, StorageOptions,
        },
        indexed_zset,
        operator::{CmpFunc, IndexedZSetHandle, OutputHandle},
        trace::test::test_batch::assert_typed_batch_eq,
        typed_batch::{IndexedZSetReader, SpineSnapshot},
        utils::{Tup2, Tup3},
    };

    use super::RankType;

    const P: u64 = 0;

    /// Total order: sort by `Tup2::0`, then `Tup2::1` (tie-break). `rank_eq` uses only `::0`.
    struct OrderByFirstTupleField;

    impl CmpFunc<Tup2<u64, u64>> for OrderByFirstTupleField {
        fn cmp(left: &Tup2<u64, u64>, right: &Tup2<u64, u64>) -> Ordering {
            left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1))
        }
    }

    /// Reference non-incremental implementation of SQL `RANK()` per partition:
    /// sort values by `CF`, group adjacent rows that tie on `rank_eq_func` (`Ordering::Equal`),
    /// assign one rank per group, then advance the next rank by the group's total weight.
    pub fn reference_rank_custom_order<K, V, CF, EF, OF, OV>(
        input: &OrdIndexedZSet<K, V>,
        rank_eq_func: EF,
        output_func: OF,
    ) -> OrdIndexedZSet<K, OV>
    where
        K: DBData + Clone,
        V: DBData + Ord,
        CF: CmpFunc<V>,
        OV: DBData,
        EF: Fn(&V, &V) -> Ordering,
        OF: Fn(i64, &V) -> OV,
    {
        let mut per_key: HashMap<K, BTreeMap<V, ZWeight>> = HashMap::new();
        for (k, v, w) in input.iter() {
            *per_key.entry(k).or_default().entry(v).or_insert(0) += w;
        }

        let mut tuples: Vec<Tup2<Tup2<K, OV>, ZWeight>> = Vec::new();

        for (k, vals) in per_key {
            let mut pairs: Vec<(V, ZWeight)> = vals.into_iter().collect();

            pairs.sort_by(|(a, _), (b, _)| CF::cmp(a, b));

            let mut i = 0;
            let mut rank: RankType = 1;

            while i < pairs.len() {
                let mut j = i + 1;
                while j < pairs.len()
                    && rank_eq_func(&pairs[j - 1].0, &pairs[j].0) == Ordering::Equal
                {
                    j += 1;
                }
                let group_weight: ZWeight = pairs[i..j].iter().map(|(_, w)| *w).sum();
                for (v, w) in &pairs[i..j] {
                    let ov = output_func(rank, v);
                    tuples.push(Tup2(Tup2(k.clone(), ov), *w));
                }
                rank += group_weight;
                i = j;
            }
        }

        OrdIndexedZSet::from_tuples((), tuples)
    }

    /// Reference non-incremental implementation of SQL `DENSE_RANK()` per partition:
    /// sort values by `CF`, group adjacent rows that tie on `rank_eq_func` (`Ordering::Equal`),
    /// assign one rank per group, then advance the next rank by the group's total weight.
    pub fn reference_dense_rank_custom_order<K, V, CF, EF, OF, OV>(
        input: &OrdIndexedZSet<K, V>,
        rank_eq_func: EF,
        output_func: OF,
    ) -> OrdIndexedZSet<K, OV>
    where
        K: DBData + Clone,
        V: DBData + Ord,
        CF: CmpFunc<V>,
        OV: DBData,
        EF: Fn(&V, &V) -> Ordering,
        OF: Fn(i64, &V) -> OV,
    {
        let mut per_key: HashMap<K, BTreeMap<V, ZWeight>> = HashMap::new();
        for (k, v, w) in input.iter() {
            *per_key.entry(k).or_default().entry(v).or_insert(0) += w;
        }

        let mut tuples: Vec<Tup2<Tup2<K, OV>, ZWeight>> = Vec::new();

        for (k, vals) in per_key {
            let mut pairs: Vec<(V, ZWeight)> = vals.into_iter().collect();

            pairs.sort_by(|(a, _), (b, _)| CF::cmp(a, b));

            let mut i = 0;
            let mut rank: RankType = 1;

            while i < pairs.len() {
                let mut j = i + 1;
                while j < pairs.len()
                    && rank_eq_func(&pairs[j - 1].0, &pairs[j].0) == Ordering::Equal
                {
                    j += 1;
                }
                for (v, w) in &pairs[i..j] {
                    let ov = output_func(rank, v);
                    tuples.push(Tup2(Tup2(k.clone(), ov), *w));
                }
                rank += 1;
                i = j;
            }
        }

        OrdIndexedZSet::from_tuples((), tuples)
    }

    #[test]
    fn reference_rank_custom_order_matches_sql_rank_on_weights() {
        let input = indexed_zset! {
            P => {
                Tup2(110u64, 1u64) => 1,
                Tup2(120u64, 2u64) => 2,
                Tup2(130u64, 3u64) => 1
            }
        };
        let got = reference_rank_custom_order::<_, _, OrderByFirstTupleField, _, _, _>(
            &input,
            |a, b| a.0.cmp(&b.0),
            |rank, t| Tup3(t.0, t.1, rank),
        );
        let want = indexed_zset! {
            P => {
                Tup3(110u64, 1u64, 1) => 1,
                Tup3(120u64, 2u64, 2) => 2,
                Tup3(130u64, 3u64, 4) => 1
            }
        };
        assert_typed_batch_eq(&got, &want);
    }

    #[test]
    fn reference_dense_rank_custom_order_matches_sql_dense_rank_on_weights() {
        let input = indexed_zset! {
            P => {
                Tup2(110u64, 1u64) => 1,
                Tup2(120u64, 2u64) => 2,
                Tup2(130u64, 3u64) => 1
            }
        };
        let got = reference_dense_rank_custom_order::<_, _, OrderByFirstTupleField, _, _, _>(
            &input,
            |a, b| a.0.cmp(&b.0),
            |rank, t| Tup3(t.0, t.1, rank),
        );
        let want = indexed_zset! {
            P => {
                Tup3(110u64, 1u64, 1) => 1,
                Tup3(120u64, 2u64, 2) => 2,
                Tup3(130u64, 3u64, 3) => 1
            }
        };
        assert_typed_batch_eq(&got, &want);
    }

    /// Input indexed zset keyed by `u64` with `Tup2(first, second)` values; ranks by the first
    /// field (SQL-style ties on that field). Integrated output is
    /// `OrdIndexedZSet<u64, Tup3<first, second, rank>>`.
    pub(super) fn rank_custom_order_tup2_test_circuit(
        circuit: &mut RootCircuit,
    ) -> AnyResult<(
        IndexedZSetHandle<u64, Tup2<u64, u64>>,
        OutputHandle<SpineSnapshot<OrdIndexedZSet<u64, Tup3<u64, u64, RankType>>>>,
        OutputHandle<SpineSnapshot<OrdIndexedZSet<u64, Tup3<u64, u64, RankType>>>>,
    )> {
        let (input_stream, input_handle) = circuit.add_input_indexed_zset::<u64, Tup2<u64, u64>>();

        let (ranked_handle, _) = input_stream
            .rank_custom_order_persistent::<OrderByFirstTupleField, u64, _, _, _, _>(
                Some("ranked"),
                |v, rv| *rv = v.0,
                |a, b| a.0.cmp(b),
                |rank, t| Tup3(t.0, t.1, rank),
            )
            .set_persistent_id(Some("ranked_stream"))
            .accumulate_integrate_trace()
            .apply(|t| t.ro_snapshot())
            .output_persistent_with_gid(Some("ranked_output"));

        let (dense_ranked_handle, _) = input_stream
            .dense_rank_custom_order_persistent::<OrderByFirstTupleField, u64, _, _, _, _>(
                Some("dense_ranked"),
                |v| v.0,
                |a, b| a.0.cmp(b),
                |rank, t| Tup3(t.0, t.1, rank),
            )
            .set_persistent_id(Some("dense_ranked_stream"))
            .accumulate_integrate_trace()
            .apply(|t| t.ro_snapshot())
            .output_persistent_with_gid(Some("dense_ranked_output"));

        Ok((input_handle, ranked_handle, dense_ranked_handle))
    }

    #[test]
    fn rank_custom_order_tup2_circuit_smoke() {
        let (mut dbsp, (input, ranked, dense_ranked)) = Runtime::init_circuit(
            CircuitConfig::with_workers(2).with_splitter_chunk_size_records(2),
            rank_custom_order_tup2_test_circuit,
        )
        .unwrap();

        let empty = <OrdIndexedZSet<u64, Tup3<u64, u64, RankType>> as HasZero>::zero();

        // Expected integral after each transaction; steps below push a batch of `(key, val, w)`
        // tuples per transaction (first batch inserts five rows at once).
        let forward_expected = vec![
            indexed_zset! {
                P => {
                    Tup3(110u64, 1u64, 1) => 1,
                    Tup3(120u64, 2u64, 2) => 1,
                    Tup3(130u64, 3u64, 3) => 1,
                    Tup3(140u64, 4u64, 4) => 1,
                    Tup3(150u64, 5u64, 5) => 1
                }
            },
            indexed_zset! {
                P => {
                    Tup3(110u64, 1u64, 1) => 1,
                    Tup3(120u64, 2u64, 2) => 1,
                    Tup3(130u64, 3u64, 3) => 1,
                    Tup3(140u64, 4u64, 4) => 1,
                    Tup3(150u64, 5u64, 5) => 1,
                    Tup3(160u64, 6u64, 6) => 1
                }
            },
            indexed_zset! {
                P => {
                    Tup3(100u64, 7u64, 1) => 1,
                    Tup3(110u64, 1u64, 2) => 1,
                    Tup3(120u64, 2u64, 3) => 1,
                    Tup3(130u64, 3u64, 4) => 1,
                    Tup3(140u64, 4u64, 5) => 1,
                    Tup3(150u64, 5u64, 6) => 1,
                    Tup3(160u64, 6u64, 7) => 1
                }
            },
            indexed_zset! {
                P => {
                    Tup3(100u64, 7u64, 1) => 1,
                    Tup3(110u64, 1u64, 2) => 1,
                    Tup3(120u64, 2u64, 3) => 1,
                    Tup3(130u64, 3u64, 4) => 1,
                    Tup3(135u64, 8u64, 5) => 1,
                    Tup3(140u64, 4u64, 6) => 1,
                    Tup3(150u64, 5u64, 7) => 1,
                    Tup3(160u64, 6u64, 8) => 1
                }
            },
            indexed_zset! {
                P => {
                    Tup3(100u64, 7u64, 1) => 1,
                    Tup3(110u64, 1u64, 2) => 1,
                    Tup3(120u64, 2u64, 3) => 1,
                    Tup3(130u64, 3u64, 4) => 1,
                    Tup3(135u64, 8u64, 5) => 2,
                    Tup3(140u64, 4u64, 7) => 1,
                    Tup3(150u64, 5u64, 8) => 1,
                    Tup3(160u64, 6u64, 9) => 1
                }
            },
            indexed_zset! {
                P => {
                    Tup3(100u64, 7u64, 1) => 1,
                    Tup3(110u64, 1u64, 2) => 1,
                    Tup3(120u64, 2u64, 3) => 1,
                    Tup3(130u64, 3u64, 4) => 1,
                    Tup3(135u64, 8u64, 5) => 3,
                    Tup3(140u64, 4u64, 8) => 1,
                    Tup3(150u64, 5u64, 9) => 1,
                    Tup3(160u64, 6u64, 10) => 1
                }
            },
            indexed_zset! {
                P => {
                    Tup3(100u64, 7u64, 1) => 1,
                    Tup3(110u64, 1u64, 2) => 1,
                    Tup3(120u64, 2u64, 3) => 1,
                    Tup3(130u64, 3u64, 4) => 1,
                    Tup3(135u64, 8u64, 5) => 3,
                    Tup3(135u64, 9u64, 5) => 1,
                    Tup3(140u64, 4u64, 9) => 1,
                    Tup3(150u64, 5u64, 10) => 1,
                    Tup3(160u64, 6u64, 11) => 1
                }
            },
            indexed_zset! {
                P => {
                    Tup3(50u64, 0u64, 1) => 1,
                    Tup3(100u64, 7u64, 2) => 1,
                    Tup3(105u64, 0u64, 3) => 1,
                    Tup3(110u64, 1u64, 4) => 1,
                    Tup3(115u64, 0u64, 5) => 1,
                    Tup3(120u64, 2u64, 6) => 1,
                    Tup3(125u64, 0u64, 7) => 1,
                    Tup3(130u64, 3u64, 8) => 1,
                    Tup3(135u64, 0u64, 9) => 1,
                    Tup3(135u64, 8u64, 9) => 3,
                    Tup3(135u64, 9u64, 9) => 1,
                    Tup3(140u64, 4u64, 14) => 1,
                    Tup3(145u64, 0u64, 15) => 1,
                    Tup3(150u64, 5u64, 16) => 1,
                    Tup3(155u64, 0u64, 17) => 1,
                    Tup3(160u64, 6u64, 18) => 1,
                    Tup3(165u64, 0u64, 19) => 1
                }
            },
        ];

        let forward_expected_dense = vec![
            indexed_zset! {
                P => {
                    Tup3(110u64, 1u64, 1) => 1,
                    Tup3(120u64, 2u64, 2) => 1,
                    Tup3(130u64, 3u64, 3) => 1,
                    Tup3(140u64, 4u64, 4) => 1,
                    Tup3(150u64, 5u64, 5) => 1
                }
            },
            indexed_zset! {
                P => {
                    Tup3(110u64, 1u64, 1) => 1,
                    Tup3(120u64, 2u64, 2) => 1,
                    Tup3(130u64, 3u64, 3) => 1,
                    Tup3(140u64, 4u64, 4) => 1,
                    Tup3(150u64, 5u64, 5) => 1,
                    Tup3(160u64, 6u64, 6) => 1
                }
            },
            indexed_zset! {
                P => {
                    Tup3(100u64, 7u64, 1) => 1,
                    Tup3(110u64, 1u64, 2) => 1,
                    Tup3(120u64, 2u64, 3) => 1,
                    Tup3(130u64, 3u64, 4) => 1,
                    Tup3(140u64, 4u64, 5) => 1,
                    Tup3(150u64, 5u64, 6) => 1,
                    Tup3(160u64, 6u64, 7) => 1
                }
            },
            indexed_zset! {
                P => {
                    Tup3(100u64, 7u64, 1) => 1,
                    Tup3(110u64, 1u64, 2) => 1,
                    Tup3(120u64, 2u64, 3) => 1,
                    Tup3(130u64, 3u64, 4) => 1,
                    Tup3(135u64, 8u64, 5) => 1,
                    Tup3(140u64, 4u64, 6) => 1,
                    Tup3(150u64, 5u64, 7) => 1,
                    Tup3(160u64, 6u64, 8) => 1
                }
            },
            indexed_zset! {
                P => {
                    Tup3(100u64, 7u64, 1) => 1,
                    Tup3(110u64, 1u64, 2) => 1,
                    Tup3(120u64, 2u64, 3) => 1,
                    Tup3(130u64, 3u64, 4) => 1,
                    Tup3(135u64, 8u64, 5) => 2,
                    Tup3(140u64, 4u64, 6) => 1,
                    Tup3(150u64, 5u64, 7) => 1,
                    Tup3(160u64, 6u64, 8) => 1
                }
            },
            indexed_zset! {
                P => {
                    Tup3(100u64, 7u64, 1) => 1,
                    Tup3(110u64, 1u64, 2) => 1,
                    Tup3(120u64, 2u64, 3) => 1,
                    Tup3(130u64, 3u64, 4) => 1,
                    Tup3(135u64, 8u64, 5) => 3,
                    Tup3(140u64, 4u64, 6) => 1,
                    Tup3(150u64, 5u64, 7) => 1,
                    Tup3(160u64, 6u64, 8) => 1
                }
            },
            indexed_zset! {
                P => {
                    Tup3(100u64, 7u64, 1) => 1,
                    Tup3(110u64, 1u64, 2) => 1,
                    Tup3(120u64, 2u64, 3) => 1,
                    Tup3(130u64, 3u64, 4) => 1,
                    Tup3(135u64, 8u64, 5) => 3,
                    Tup3(135u64, 9u64, 5) => 1,
                    Tup3(140u64, 4u64, 6) => 1,
                    Tup3(150u64, 5u64, 7) => 1,
                    Tup3(160u64, 6u64, 8) => 1
                }
            },
            indexed_zset! {
                P => {
                    Tup3(50u64, 0u64, 1) => 1,
                    Tup3(100u64, 7u64, 2) => 1,
                    Tup3(105u64, 0u64, 3) => 1,
                    Tup3(110u64, 1u64, 4) => 1,
                    Tup3(115u64, 0u64, 5) => 1,
                    Tup3(120u64, 2u64, 6) => 1,
                    Tup3(125u64, 0u64, 7) => 1,
                    Tup3(130u64, 3u64, 8) => 1,
                    Tup3(135u64, 0u64, 9) => 1,
                    Tup3(135u64, 8u64, 9) => 3,
                    Tup3(135u64, 9u64, 9) => 1,
                    Tup3(140u64, 4u64, 10) => 1,
                    Tup3(145u64, 0u64, 11) => 1,
                    Tup3(150u64, 5u64, 12) => 1,
                    Tup3(155u64, 0u64, 13) => 1,
                    Tup3(160u64, 6u64, 14) => 1,
                    Tup3(165u64, 0u64, 15) => 1
                }
            },
        ];

        let forward_steps: Vec<Vec<(u64, Tup2<u64, u64>, ZWeight)>> = vec![
            vec![
                (P, Tup2(110, 1), 1),
                (P, Tup2(120, 2), 1),
                (P, Tup2(130, 3), 1),
                (P, Tup2(140, 4), 1),
                (P, Tup2(150, 5), 1),
            ],
            vec![(P, Tup2(160, 6), 1)],
            vec![(P, Tup2(100, 7), 1)],
            vec![(P, Tup2(135, 8), 1)],
            vec![(P, Tup2(135, 8), 1)],
            vec![(P, Tup2(135, 8), 1)],
            vec![(P, Tup2(135, 9), 1)],
            vec![
                (P, Tup2(50, 0), 1),
                (P, Tup2(105, 0), 1),
                (P, Tup2(115, 0), 1),
                (P, Tup2(125, 0), 1),
                (P, Tup2(135, 0), 1),
                (P, Tup2(145, 0), 1),
                (P, Tup2(155, 0), 1),
                (P, Tup2(165, 0), 1),
            ],
        ];

        assert_eq!(forward_expected.len(), forward_steps.len());
        assert_eq!(forward_expected_dense.len(), forward_steps.len());

        for (batch, (expected, expected_dense)) in forward_steps
            .iter()
            .zip(forward_expected.iter().zip(forward_expected_dense.iter()))
        {
            for (k, v, w) in batch {
                input.push(*k, (*v, *w));
            }
            dbsp.transaction().unwrap();
            assert_typed_batch_eq(&ranked.concat().consolidate(), expected);
            assert_typed_batch_eq(&dense_ranked.concat().consolidate(), expected_dense);
        }

        let reverse_steps: Vec<Vec<(u64, Tup2<u64, u64>, ZWeight)>> = vec![
            vec![
                (P, Tup2(50, 0), -1),
                (P, Tup2(105, 0), -1),
                (P, Tup2(115, 0), -1),
                (P, Tup2(125, 0), -1),
                (P, Tup2(135, 0), -1),
                (P, Tup2(145, 0), -1),
                (P, Tup2(155, 0), -1),
                (P, Tup2(165, 0), -1),
            ],
            vec![(P, Tup2(135, 9), -1)],
            vec![(P, Tup2(135, 8), -1)],
            vec![(P, Tup2(135, 8), -1)],
            vec![(P, Tup2(135, 8), -1)],
            vec![(P, Tup2(100, 7), -1)],
            vec![(P, Tup2(160, 6), -1)],
            vec![
                (P, Tup2(150, 5), -1),
                (P, Tup2(140, 4), -1),
                (P, Tup2(130, 3), -1),
                (P, Tup2(120, 2), -1),
                (P, Tup2(110, 1), -1),
            ],
        ];

        assert_eq!(reverse_steps.len(), forward_steps.len());

        for (i, batch) in reverse_steps.iter().enumerate() {
            for (k, v, w) in batch {
                input.push(*k, (*v, *w));
            }
            dbsp.transaction().unwrap();
            let exp = if i == reverse_steps.len() - 1 {
                &empty
            } else {
                &forward_expected[forward_expected.len() - 2 - i]
            };
            assert_typed_batch_eq(&ranked.concat().consolidate(), exp);

            let exp_dense = if i == reverse_steps.len() - 1 {
                &empty
            } else {
                &forward_expected_dense[forward_expected_dense.len() - 2 - i]
            };
            assert_typed_batch_eq(&dense_ranked.concat().consolidate(), exp_dense);
        }
    }

    fn multiset_to_indexed_zset(
        counts: &HashMap<(u64, Tup2<u64, u64>), ZWeight>,
    ) -> OrdIndexedZSet<u64, Tup2<u64, u64>> {
        let tuples: Vec<Tup2<Tup2<u64, Tup2<u64, u64>>, ZWeight>> = counts
            .iter()
            .filter(|(_, w)| **w != 0)
            .map(|((k, v), w)| Tup2(Tup2(*k, *v), *w))
            .collect();
        OrdIndexedZSet::from_tuples((), tuples)
    }

    #[derive(Clone, Copy)]
    enum BatchOp {
        Ins { k: u64, v0: u64, v1: u64 },
        Del { k: u64, v0: u64, v1: u64 },
    }

    impl BatchOp {
        fn sort_key(self) -> (u64, u64, u64) {
            match self {
                BatchOp::Ins { k, v0, v1 } | BatchOp::Del { k, v0, v1 } => (k, v0, v1),
            }
        }
    }

    /// Simulates sorted pushes from the batch-start multiset; returns false if a delete runs
    /// before that `(key, value)` exists (should not happen for our generator + sort).
    fn sorted_batch_is_replayable(
        start: &HashMap<(u64, Tup2<u64, u64>), ZWeight>,
        sorted: &[BatchOp],
    ) -> bool {
        let mut sim: HashMap<(u64, Tup2<u64, u64>), ZWeight> = start.clone();
        for op in sorted {
            match *op {
                BatchOp::Ins { k, v0, v1 } => {
                    let t = Tup2(v0, v1);
                    *sim.entry((k, t)).or_insert(0) += 1;
                }
                BatchOp::Del { k, v0, v1 } => {
                    let t = Tup2(v0, v1);
                    let Some(w) = sim.get_mut(&(k, t)) else {
                        return false;
                    };
                    if *w <= 0 {
                        return false;
                    }
                    *w -= 1;
                    if *w == 0 {
                        sim.remove(&(k, t));
                    }
                }
            }
        }
        true
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 50,
            .. ProptestConfig::default()
        })]

        /// Drives [`rank_custom_order_tup2_test_circuit`] against [`reference_rank_custom_order`].
        #[test]
        fn rank_custom_order_matches_reference_random_batches(
            seed in any::<u64>(),
            num_batches in 1usize..=50usize,
        ) {
            let _storage_dir = tempdir().expect("temp dir for circuit storage");
            let storage = CircuitStorageConfig::for_config(
                StorageConfig {
                    path: _storage_dir.path().to_string_lossy().into_owned(),
                    cache: StorageCacheConfig::default(),
                },
                StorageOptions::default(),
            )
            .expect("storage backend");
            let config = CircuitConfig::from(2)
                .with_splitter_chunk_size_records(2)
                .with_storage(storage);

            let mut rng = StdRng::seed_from_u64(seed);
            let mut multiset: HashMap<(u64, Tup2<u64, u64>), ZWeight> = HashMap::new();

            let (mut dbsp, (mut input, mut ranked, mut dense_ranked)) =
                Runtime::init_circuit(config.clone(), rank_custom_order_tup2_test_circuit).unwrap();

            for i in 0..num_batches {
                let mut tmp = multiset.clone();
                let mut ops: Vec<BatchOp> = Vec::with_capacity(10);

                for _ in 0..10 {
                    let can_delete = tmp.iter().any(|(_, w)| *w > 0);
                    let delete = can_delete && rng.gen_bool(0.45);

                    if delete {
                        let candidates: Vec<(u64, Tup2<u64, u64>)> = tmp
                            .iter()
                            .filter(|(_, w)| **w > 0)
                            .map(|(kv, _)| *kv)
                            .collect();
                        let (k, t) = candidates[rng.gen_range(0..candidates.len())];
                        ops.push(BatchOp::Del {
                            k,
                            v0: t.0,
                            v1: t.1,
                        });
                        let e = tmp.get_mut(&(k, t)).unwrap();
                        *e -= 1;
                        if *e == 0 {
                            tmp.remove(&(k, t));
                        }
                    } else {
                        let k = rng.gen_range(0..5u64);
                        let v0 = rng.gen_range(0..10u64);
                        let v1 = rng.gen_range(0..10u64);
                        ops.push(BatchOp::Ins { k, v0, v1 });
                        let t = Tup2(v0, v1);
                        *tmp.entry((k, t)).or_insert(0) += 1;
                    }
                }

                let mut tagged: Vec<(usize, BatchOp)> = ops.iter().cloned().enumerate().collect();
                tagged.sort_by(|(ia, a), (ib, b)| {
                    a.sort_key()
                        .cmp(&b.sort_key())
                        .then_with(|| ia.cmp(ib))
                });
                let sorted: Vec<BatchOp> = tagged.into_iter().map(|(_, o)| o).collect();

                // Deletes are only sampled from positive `tmp`; same-key ops are ordered by
                // generation index, so replay from the batch-start multiset cannot underflow.
                assert!(
                    sorted_batch_is_replayable(&multiset, &sorted),
                    "sorted_batch_is_replayable: invariant broken (sort key or generator bug)"
                );

                for op in sorted {
                    match op {
                        BatchOp::Ins { k, v0, v1 } => {
                            input.push(k, (Tup2(v0, v1), 1));
                        }
                        BatchOp::Del { k, v0, v1 } => {
                            input.push(k, (Tup2(v0, v1), -1));
                        }
                    }
                }

                multiset = tmp;
                dbsp.transaction().unwrap();

                let current = multiset_to_indexed_zset(&multiset);
                let expected =
                    reference_rank_custom_order::<_, _, OrderByFirstTupleField, _, _, _>(
                        &current,
                        |a, b| a.0.cmp(&b.0),
                        |rank, v| Tup3(v.0, v.1, rank),
                    );
                assert_typed_batch_eq(&ranked.concat().consolidate(), &expected);

                let expected_dense =
                    reference_dense_rank_custom_order::<_, _, OrderByFirstTupleField, _, _, _>(
                        &current,
                        |a, b| a.0.cmp(&b.0),
                        |rank, v| Tup3(v.0, v.1, rank),
                    );

                assert_typed_batch_eq(&dense_ranked.concat().consolidate(), &expected_dense);

                if i % 20 == 0 {
                    println!("Checkpoint at batch {}", i);
                    (dbsp, (input, ranked, dense_ranked)) = {
                        let checkpoint = dbsp.checkpoint().run().unwrap();
                        dbsp.kill().unwrap();

                        let mut config = config.clone();
                        config.storage.as_mut().unwrap().init_checkpoint = Some(checkpoint.uuid);

                        let (dbsp, (input, ranked, dense_ranked)) = Runtime::init_circuit(config, rank_custom_order_tup2_test_circuit).unwrap();
                        assert!(!dbsp.bootstrap_in_progress());
                        (dbsp, (input, ranked, dense_ranked))
                    }
                }
            }
        }
    }
}
