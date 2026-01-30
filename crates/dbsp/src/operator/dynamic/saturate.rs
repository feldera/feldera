use crate::{
    Circuit, Position, RootCircuit, Scope, Stream,
    algebra::{OrdIndexedZSet, OrdIndexedZSetFactories},
    circuit::{
        circuit_builder::StreamId,
        metadata::{BatchSizeStats, INPUT_BATCHES_STATS, OUTPUT_BATCHES_STATS, OperatorMeta},
        operator_traits::Operator,
        splitter_output_chunk_size,
    },
    circuit_cache_key,
    dynamic::{DataTrait, Erase},
    operator::{
        accumulator::AccumulateApply2,
        async_stream_operators::{StreamingBinaryOperator, StreamingBinaryWrapper},
    },
    trace::{
        Batch, BatchReader, BatchReaderFactories, Builder, Cursor, Spine, SpineSnapshot,
        WithSnapshot, spine_async::SpineCursor,
    },
};
use async_stream::stream;
use futures::Stream as AsyncStream;
use std::{
    borrow::Cow, cell::RefCell, cmp::Ordering, marker::PhantomData, panic::Location, rc::Rc,
};

circuit_cache_key!(SaturateId<C, B: Batch>(StreamId => Stream<C, Option<SpineSnapshot<B>>>));
circuit_cache_key!(BalancedSaturateId<C, B: Batch>(StreamId => Stream<C, Option<SpineSnapshot<B>>>));

impl<K, V> Stream<RootCircuit, OrdIndexedZSet<K, V>>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
{
    /// Saturate the input stream by adding a ghost (k, None) tuple for each key
    /// not present in the trace of the input stream.
    ///
    /// This is an auxiliary operator used to implement incremental
    /// outer joins.  The idea is to convert, e.g., a left join into
    /// an inner join by simulating that every key in the right side
    /// of the join is always present. We do this without
    /// materializing the entire universe of keys by:
    ///
    /// 1. Providing a modified cursor over the integral of the
    ///    right side, which returns missing keys on demand (see `SaturatingCursor`).
    /// 2. Augmenting the change stream with the missing records (this operator), specifically:
    ///    - When the first value for a key is added to the collection, we inject
    ///      a retraction of the ghost tuple ((k, None), -1).
    ///    - When the last value for a key is removed from the collection, we inject
    ///      an addition of the ghost tuple ((k, None), +1).
    ///
    /// **Caveat:** In order to faithfully implement the saturated stream, we'd have to
    /// output ghost tuples for all missing keys during the first step. We don't do this,
    /// relying on the fact that the left join essentially ignores the first delta in the
    /// right stream (by joining with the empty delayed integral of the left side, which is
    /// always empty in the first step, since it's the output of a delay operator).
    ///
    pub fn dyn_saturate(
        &self,
        factories: &<OrdIndexedZSet<K, V> as BatchReader>::Factories,
    ) -> Stream<RootCircuit, Option<SpineSnapshot<OrdIndexedZSet<K, V>>>> {
        // We use the Saturate operator to compute ghost tuples and concatenate
        // its output with the original stream to obtain the complete saturated stream.
        //
        // ```text
        //                        ┌───────────────────────────────┐
        //                        │                               │
        //                        │                               │
        //                        │                               ▼      ghost
        //  stream ┌──────────┐   │   ┌─────┐  delayed trace  ┌────────┐ tuples    ┌──────────┐
        // ───────►│accumulate├───┴──►│trace├────────────────►│Saturate├──────────►│accumulate│
        //         └────┬─────┘       └─────┘                 └────────┘           └─────┬────┘
        //              │                                                                │
        //              │                                                                ▼
        //              │                                                            ┌──────┐
        //              └───────────────────────────────────────────────────────────►│  +   ├──────►
        //                                                                           └──────┘
        // ```

        self.circuit()
            .cache_get_or_insert_with(SaturateId::new(self.stream_id()), || {
                self.circuit()
                    .region("saturate", || {
                        let stream = self.dyn_shard(factories);

                        let delayed_trace = stream
                            .dyn_accumulate_trace(factories, factories)
                            .accumulate_delay_trace();

                        let ghost = self.circuit().add_binary_operator(
                            StreamingBinaryWrapper::new(Saturate::new(factories)),
                            &stream.dyn_accumulate(factories),
                            &delayed_trace,
                        );

                        ghost.mark_sharded();

                        let output_factories = factories.clone();

                        // Plus
                        let result = stream.circuit().add_binary_operator(
                            AccumulateApply2::new(
                                move |stream, saturation| {
                                    SpineSnapshot::concat(
                                        output_factories.clone(),
                                        vec![&stream, &saturation],
                                    )
                                },
                                Location::caller(),
                            ),
                            &stream.dyn_accumulate(factories),
                            &ghost.dyn_accumulate(factories),
                        );

                        // `result` is also the saturated version of the sharded stream.
                        self.circuit()
                            .cache_insert(SaturateId::new(stream.stream_id()), result.clone());
                        result.mark_sharded();
                        result
                    })
                    .clone()
            })
            .clone()
    }

    pub fn dyn_saturate_balanced(
        &self,
        factories: &<OrdIndexedZSet<K, V> as BatchReader>::Factories,
    ) -> Stream<RootCircuit, Option<SpineSnapshot<OrdIndexedZSet<K, V>>>> {
        self.circuit()
            .cache_get_or_insert_with(BalancedSaturateId::new(self.stream_id()), || {
                self.circuit()
                    .region("saturate_balanced", || {
                        // let stream = self.dyn_shard(&factories.batch_factories);

                        // let delayed_trace = stream
                        //     .dyn_accumulate_trace(
                        //         &factories.trace_factories,
                        //         &factories.batch_factories,
                        //     )
                        //     .accumulate_delay_trace();

                        let (accumulator, trace) =
                            self.dyn_accumulate_trace_balanced(factories, factories);

                        let delayed_trace = trace.accumulate_delay_trace();

                        let ghost = self.circuit().add_binary_operator(
                            StreamingBinaryWrapper::new(Saturate::new(factories)),
                            &accumulator,
                            &delayed_trace,
                        );

                        let output_factories = factories.clone();

                        // Plus
                        self.circuit().add_binary_operator(
                            AccumulateApply2::new(
                                move |stream, saturation| {
                                    SpineSnapshot::concat(
                                        output_factories.clone(),
                                        vec![&stream, &saturation],
                                    )
                                },
                                Location::caller(),
                            ),
                            &accumulator,
                            &ghost.dyn_accumulate(factories),
                        )
                    })
                    .clone()
            })
            .clone()
    }
}

/// This operator computes the ghost tuples. Concatenate its output with the original stream
/// to obtain the complete saturated stream.
struct Saturate<K, V>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
{
    factories: OrdIndexedZSetFactories<K, V>,
    input_batch_stats: BatchSizeStats,
    output_batch_stats: RefCell<BatchSizeStats>,
    phantom: PhantomData<fn(&K, &V)>,
}

impl<K, V> Saturate<K, V>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
{
    pub fn new(factories: &OrdIndexedZSetFactories<K, V>) -> Self {
        Self {
            factories: factories.clone(),
            input_batch_stats: BatchSizeStats::new(),
            output_batch_stats: RefCell::new(BatchSizeStats::new()),
            phantom: PhantomData,
        }
    }

    /// True if there's at least one value with a non-zero weight in
    /// the union of the two streams.
    fn combined_key_valid(
        delta_cursor: &mut SpineCursor<OrdIndexedZSet<K, V>>,
        trace_cursor: &mut SpineCursor<OrdIndexedZSet<K, V>>,
    ) -> bool {
        while delta_cursor.val_valid() && trace_cursor.val_valid() {
            match delta_cursor.val().cmp(trace_cursor.val()) {
                Ordering::Less | Ordering::Greater => {
                    return true;
                }
                Ordering::Equal => {
                    if **delta_cursor.weight() + **trace_cursor.weight() != 0 {
                        return true;
                    }
                    delta_cursor.step_val();
                    trace_cursor.step_val();
                }
            }
        }

        if delta_cursor.val_valid() || trace_cursor.val_valid() {
            return true;
        }

        false
    }
}

impl<K, V> Operator for Saturate<K, V>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
{
    fn name(&self) -> Cow<'static, str> {
        "Saturate".into()
    }

    fn metadata(&self, meta: &mut OperatorMeta) {
        meta.extend(metadata! {
            INPUT_BATCHES_STATS => self.input_batch_stats.metadata(),
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
        SpineSnapshot<OrdIndexedZSet<K, V>>,
        OrdIndexedZSet<K, V>,
    > for Saturate<K, V>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
{
    fn eval(
        self: Rc<Self>,
        delta: &Option<Spine<OrdIndexedZSet<K, V>>>,
        delayed_trace: &SpineSnapshot<OrdIndexedZSet<K, V>>,
    ) -> impl AsyncStream<Item = (OrdIndexedZSet<K, V>, bool, Option<Position>)> + 'static {
        //println!("Saturate: eval: delta: {:?}, trace: {:?}", delta, trace);
        let chunk_size = splitter_output_chunk_size();

        let delta = delta.as_ref().map(|b| b.ro_snapshot());

        // We assume that delta.is_some() implies that the operator is being flushed,
        // since the integral is always flushed in same step as delta.
        let trace = if delta.is_some() {
            Some(delayed_trace.ro_snapshot())
        } else {
            None
        };

        stream! {
            let none_val = self.factories.val_factory().default_box();

            let Some(delta) = delta else {
                // println!("yield empty");
                yield (OrdIndexedZSet::dyn_empty(&self.factories), true, None);
                return;
            };

            let mut builder = <OrdIndexedZSet<K, V> as Batch>::Builder::with_capacity(&self.factories, chunk_size, chunk_size);

            let mut delta_cursor = delta.cursor();
            let mut trace_cursor = trace.unwrap().cursor();

            while delta_cursor.key_valid() {
                //println!("Saturate: key: {:?}", delta_cursor.key());
                if trace_cursor.seek_key_exact(delta_cursor.key(), None) {
                    // Key presents in the trace. Check if it's still present in the combined stream.
                    // If not, add a ghost tuple.

                    //println!("Saturate: key found in trace: {:?}", delta_cursor.key());
                    if !Self::combined_key_valid(&mut delta_cursor, &mut trace_cursor) {
                        builder.push_val_diff(&*none_val, 1.erase());
                        builder.push_key(delta_cursor.key());
                    }
                } else {
                    // Key not found in trace -- retract the ghost value.
                    //println!("Saturate: key not found in trace: {:?}", delta_cursor.key());
                    builder.push_val_diff(&*none_val, (-1).erase());
                    builder.push_key(delta_cursor.key());
                }

                if builder.num_tuples() >= chunk_size {
                    let builder = std::mem::replace(
                        &mut builder,
                        <OrdIndexedZSet<K, V> as Batch>::Builder::with_capacity(
                            &self.factories,
                            chunk_size,
                            chunk_size,
                        ),
                    );

                    let result = builder.done();
                    self.output_batch_stats.borrow_mut().add_batch(result.len());

                    yield (result, false, delta_cursor.position())
                }

                delta_cursor.step_key();
            }

            let result = builder.done();
            self.output_batch_stats.borrow_mut().add_batch(result.len());

            yield (result, true, delta_cursor.position())
        }
    }
}
