//! Group transformer operators that map multiple input records
//! into multiple output records.

use crate::{
    algebra::{HasZero, IndexedZSet, OrdIndexedZSet, ZCursor, ZTrace},
    circuit::{
        operator_traits::{Operator, TernaryOperator},
        Scope,
    },
    dynamic::{DataTrait, DynPair, DynUnit, DynWeightedPairs, Factory},
    operator::dynamic::trace::{TraceBounds, TraceFeedback},
    trace::{
        cursor::{CursorEmpty, CursorGroup, CursorPair},
        BatchFactories, BatchReaderFactories, Builder, Cursor, OrdIndexedWSetFactories, Spine,
    },
    Circuit, DynZWeight, RootCircuit, Stream,
};
use std::{borrow::Cow, marker::PhantomData, ops::Neg};

mod lag;
mod topk;

#[cfg(test)]
mod test;

use dyn_clone::clone_box;

use crate::dynamic::{ClonableTrait, Erase};
pub use lag::LagFactories;
pub use topk::{TopKCustomOrdFactories, TopKFactories, TopKRankCustomOrdFactories};

/// Specifies the order in which a group transformer produces output tuples.
#[derive(PartialEq, Eq)]
pub enum Monotonicity {
    /// Transformer produces outputs in ascending order.  Output tuples
    /// can be pushed directly to a `Builder`.
    Ascending,
    /// Transformer produces outputs in descending order.  Once all outputs
    /// have been produced, they can be pushed to a `Builder` in reverse
    /// order.
    Descending,
    /// Transformer does not guarantee an particular order of output tuples.
    /// Outputs must be sorted before pushing them to a `Builder`.
    #[allow(dead_code)]
    Unordered,
}

/// Defines an incremental transformation of multiple input records
/// into multiple output records.
///
/// Group transformers are a generalization of aggregators: while an
/// aggregator maps a group of values into a single aggregate value,
/// a group transformer maps multiple input values into multiple output
/// values. Examples are the `top-k` transformer that returns
/// `k` largest values in the group and the `row-number` transformer
/// that attaches index to each input value according to ascdending or
/// descending order.
pub trait GroupTransformer<I: ?Sized, O: ?Sized>: 'static {
    /// Transformer name.
    fn name(&self) -> &str;

    /// Output ordering guaranteed by this transformer.
    fn monotonicity(&self) -> Monotonicity;

    /// Compute changes to the output group given changes to the input group.
    /// Produces changes to the output group by invoking `output_cb` for each
    /// output update in the order consistent with `self.monotonicity()`.
    ///
    /// # Arguments
    ///
    /// * `input_delta` - cursor over changes to the input group.
    /// * `input_trace` - cursor over the entire contents of the input group
    ///   after the previous clock tick.
    /// * `output_trace` - cursor over the entire contents of the output group
    ///   after the previous clock tick.
    /// * `output_cb` - callback invoked for each output update.
    fn transform(
        &mut self,
        input_delta: &mut dyn ZCursor<I, DynUnit, ()>,
        input_trace: &mut dyn ZCursor<I, DynUnit, ()>,
        output_trace: &mut dyn ZCursor<O, DynUnit, ()>,
        output_cb: &mut dyn FnMut(&mut O, &mut DynZWeight),
    );
}

/// Non-incremental group transformer.
///
/// This version of the group transformer trait computes the
/// complete contents of the output group at each clock tick.
/// It is easier to implement than [`GroupTransformer`], which
/// constructs the output group incrementally.  It is generally
/// less efficient than an optimized incremental implementation
/// as it requires scanning the entire input group.  One notable
/// exception is `top-k` with a small value of `k`, which only
/// requires scanning `k` top elements.
///
/// A transformer that implements this trait can be used to build
/// an incremental group transformer by wrapping it in
/// [`DiffGroupTransformer`].
pub trait NonIncrementalGroupTransformer<I: ?Sized, O: ?Sized>: 'static {
    /// Transformer name.
    fn name(&self) -> &str;

    /// Output ordering guaranteed by this transformer.
    fn monotonicity(&self) -> Monotonicity;

    /// Compute the complete contents of the output group given the complete
    /// contents of the input group.
    /// Produces changes to the output group by invoking `output_cb` for each
    /// output update in the order consistent with `self.monotonicity()`.
    ///
    /// # Arguments
    ///
    /// * `cursor` - cursor over the contents of the input group.
    /// * `output_cb` - callback invoked for each output record.
    fn transform<C, CB>(&mut self, cursor: &mut C, output_cb: CB)
    where
        C: Cursor<I, DynUnit, (), DynZWeight>,
        CB: FnMut(&mut O, &mut DynZWeight);
}

/// Incremental group transformer that wraps a non-incremental transformer.
///
/// This object implements the incremental group transformer API
/// ([`GroupTransformer`]) on top of a non-incremental transformer
/// ([`NonIncrementalGroupTransformer`]).  It works by using the underlying
/// non-incremental transformer to compute the complete output group on
/// each clock tick and subtracting the previous contents of the output
/// group.
// TODO: This implementation maintains the trace of both input and output
// collections.  An alternative implementation could trade memory for CPU
// by maintaining only the input trace and computing both current and
// previous outputs every time.
pub struct DiffGroupTransformer<I: DataTrait + ?Sized, O: DataTrait + ?Sized, T> {
    output_factory: &'static dyn Factory<O>,
    transformer: T,
    _phantom: PhantomData<fn(&I, &O)>,
}

impl<I: DataTrait + ?Sized, O: DataTrait + ?Sized, T> DiffGroupTransformer<I, O, T> {
    fn new(output_factory: &'static dyn Factory<O>, transformer: T) -> Self {
        Self {
            output_factory,
            transformer,
            _phantom: PhantomData,
        }
    }
}

impl<I, O, T> GroupTransformer<I, O> for DiffGroupTransformer<I, O, T>
where
    I: DataTrait + ?Sized,
    O: DataTrait + ?Sized,
    T: NonIncrementalGroupTransformer<I, O>,
{
    fn name(&self) -> &str {
        self.transformer.name()
    }

    fn monotonicity(&self) -> Monotonicity {
        self.transformer.monotonicity()
    }

    fn transform(
        &mut self,
        input_delta: &mut dyn ZCursor<I, DynUnit, ()>,
        input_trace: &mut dyn ZCursor<I, DynUnit, ()>,
        output_trace: &mut dyn ZCursor<O, DynUnit, ()>,
        output_cb: &mut dyn FnMut(&mut O, &mut DynZWeight),
    ) {
        let mut key = self.output_factory.default_box();

        match self.transformer.monotonicity() {
            Monotonicity::Ascending => {
                // Transformer produces outputs in ascending order.  Interleave them
                // with retractions from the output trace to maintain ascending order
                // across insertions and retractions.
                self.transformer.transform(
                    &mut CursorPair::new(input_delta, input_trace),
                    |v, w| {
                        while output_trace.key_valid() && output_trace.key() < v {
                            let ow = **output_trace.weight();
                            if !ow.is_zero() {
                                output_trace.key().clone_to(&mut key);
                                output_cb(&mut key, ow.neg().erase_mut());
                            }
                            output_trace.step_key();
                        }
                        if output_trace.key_valid() && output_trace.key() == v {
                            let mut w = **w + output_trace.weight().neg();
                            if !w.is_zero() {
                                output_trace.key().clone_to(&mut key);
                                output_cb(&mut key, w.erase_mut());
                            }
                            output_trace.step_key();
                        } else {
                            output_cb(v, w);
                        }
                    },
                );

                // Output remaining retractions in the output trace.
                while output_trace.key_valid() {
                    let w = **output_trace.weight();
                    if !w.is_zero() {
                        output_trace.key().clone_to(&mut key);
                        output_cb(&mut key, w.neg().erase_mut());
                    }
                    output_trace.step_key();
                }
            }

            Monotonicity::Descending => {
                // Transformer produces outputs in descending order.  Interleave them
                // with retractions from the output trace to maintain descending order
                // across insertions and retractions.
                output_trace.fast_forward_keys();
                self.transformer.transform(
                    &mut CursorPair::new(input_delta, input_trace),
                    |v, w| {
                        while output_trace.key_valid() && output_trace.key() > v {
                            let ow = **output_trace.weight();
                            if !ow.is_zero() {
                                output_trace.key().clone_to(&mut key);
                                output_cb(&mut key, ow.neg().erase_mut());
                            }
                            output_trace.step_key_reverse();
                        }
                        if output_trace.key_valid() && output_trace.key() == v {
                            let mut w = **w + output_trace.weight().neg();
                            if !w.is_zero() {
                                output_trace.key().clone_to(&mut key);
                                output_cb(&mut key, w.erase_mut());
                            }
                            output_trace.step_key_reverse();
                        } else {
                            output_cb(v, w);
                        }
                    },
                );

                // Output remaining retractions in the output trace.
                while output_trace.key_valid() {
                    let w = **output_trace.weight();
                    if !w.is_zero() {
                        output_trace.key().clone_to(&mut key);
                        output_cb(&mut key, w.neg().erase_mut());
                    }
                    output_trace.step_key_reverse();
                }
            }

            Monotonicity::Unordered => {
                // Transformer produces unordered outputs.
                self.transformer
                    .transform(&mut CursorPair::new(input_delta, input_trace), |v, w| {
                        output_cb(v, w)
                    });

                // Output retractions in output trace.
                while output_trace.key_valid() {
                    let w = **output_trace.weight();
                    if !w.is_zero() {
                        output_trace.key().clone_to(&mut key);
                        output_cb(&mut key, w.neg().erase_mut());
                    }
                    output_trace.step_key();
                }
            }
        }
    }
}

impl<B> Stream<RootCircuit, B>
where
    B: IndexedZSet + Send,
{
    /// Apply group `transformer` to each partition in the input stream.
    ///
    /// Applies group transformer `transformer` to values associated with
    /// each key in the input stream.
    fn dyn_group_transform<OV>(
        &self,
        input_factories: &B::Factories,
        output_factories: &OrdIndexedWSetFactories<B::Key, OV, DynZWeight>,
        transformer: Box<dyn GroupTransformer<B::Val, OV>>,
    ) -> Stream<RootCircuit, OrdIndexedZSet<B::Key, OV>>
    where
        OV: DataTrait + ?Sized,
    {
        self.dyn_group_transform_generic(input_factories, output_factories, transformer)
    }

    /// Like [`group_transform`](`Self::group_transform`), but can output any
    /// indexed Z-set, not just [`OrdIndexedZSet`]
    fn dyn_group_transform_generic<OB>(
        &self,
        input_factories: &B::Factories,
        output_factories: &OB::Factories,
        transform: Box<dyn GroupTransformer<B::Val, OB::Val>>,
    ) -> Stream<RootCircuit, OB>
    where
        OB: IndexedZSet<Key = B::Key>,
    {
        let circuit = self.circuit();
        let stream = self.dyn_shard(input_factories);

        // ```
        //       ┌────────────────────────────────────────────┐
        //       │                                            │
        //       │                                            ▼
        // stream│  ┌─────────────────────────┐        ┌──────────────┐  output    ┌──────────────────┐
        // ──────┴─►│integrate().delay_trace()├───────►│GroupTransform├────────────┤UntimedTraceAppend├───┐
        //          └─────────────────────────┘        └──────────────┘            └──────────────────┘   │
        //                                                    ▲                          ▲                │
        //                                                    │                          │                │
        //                                                    │        delayed_trace   ┌─┴──┐             │
        //                                                    └────────────────────────┤Z^-1│◄────────────┘
        //                                                                             └────┘
        // ```
        let bounds = TraceBounds::unbounded();
        let feedback = circuit.add_integrate_trace_feedback::<Spine<OB>>(output_factories, bounds);

        let output = circuit
            .add_ternary_operator(
                GroupTransform::new(output_factories, transform),
                &stream,
                &stream.dyn_integrate_trace(input_factories).delay_trace(),
                &feedback.delayed_trace,
            )
            .mark_sharded();

        feedback.connect(&output);

        output
    }
}

struct GroupTransform<B, OB, T, OT>
where
    B: IndexedZSet,
    OB: IndexedZSet,
{
    output_factories: OB::Factories,
    transformer: Box<dyn GroupTransformer<B::Val, OB::Val>>,
    buffer: Box<DynWeightedPairs<DynPair<OB::Key, OB::Val>, OB::R>>,
    _phantom: PhantomData<(B, OB, T, OT)>,
}

impl<B, OB, T, OT> GroupTransform<B, OB, T, OT>
where
    B: IndexedZSet,
    OB: IndexedZSet,
{
    fn new(
        output_factories: &OB::Factories,
        transformer: Box<dyn GroupTransformer<B::Val, OB::Val>>,
    ) -> Self {
        Self {
            output_factories: output_factories.clone(),
            transformer,
            buffer: output_factories.weighted_items_factory().default_box(),
            _phantom: PhantomData,
        }
    }
}

impl<B, OB, T, OT> Operator for GroupTransform<B, OB, T, OT>
where
    B: IndexedZSet + 'static,
    OB: IndexedZSet + 'static,
    T: 'static,
    OT: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from(format!("GroupTransform({})", self.transformer.name()))
    }
    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<B, OB, T, OT> TernaryOperator<B, T, OT, OB> for GroupTransform<B, OB, T, OT>
where
    B: IndexedZSet,
    T: ZTrace<Key = B::Key, Val = B::Val, Time = ()> + Clone,
    OB: IndexedZSet<Key = B::Key>,
    OT: ZTrace<Key = B::Key, Val = OB::Val, Time = ()> + Clone,
{
    fn eval<'a>(
        &mut self,
        delta: Cow<'a, B>,
        input_trace: Cow<'a, T>,
        output_trace: Cow<'a, OT>,
    ) -> OB {
        let mut delta_cursor = delta.cursor();
        let mut input_trace_cursor = input_trace.cursor();
        let mut output_trace_cursor = output_trace.cursor();

        let mut builder = OB::Builder::with_capacity(&self.output_factories, (), delta.len());

        while delta_cursor.key_valid() {
            let key = clone_box(delta_cursor.key());
            let mut key2 = clone_box(key.as_ref());
            let mut tuple = self.output_factories.weighted_item_factory().default_box();

            // Output callback that pushes directly to builder.
            let mut cb_asc = |val: &mut OB::Val, w: &mut B::R| {
                key.clone_to(&mut key2);
                // println!("val: {val:?}, w: {w:?}");
                builder.push_vals(&mut key2, val, w);
            };
            // Output callaback that pushes to an intermediate buffer.
            let mut cb_desc = |val: &mut OB::Val, w: &mut B::R| {
                //println!("val: {val:?}, w: {w:?}");
                let (kv, weight) = tuple.split_mut();
                let (k, v) = kv.split_mut();
                key.clone_to(k);
                val.move_to(v);
                w.move_to(weight);
                self.buffer.push_val(&mut *tuple);
            };

            let cb = if self.transformer.monotonicity() == Monotonicity::Ascending {
                // Ascending transformer: push directly to builder.
                &mut cb_asc as &mut dyn FnMut(&mut OB::Val, &mut B::R)
            } else {
                // Descending or unordered transformer: push to buffer.
                &mut cb_desc as &mut dyn FnMut(&mut OB::Val, &mut B::R)
            };

            input_trace_cursor.seek_key(&key);

            let mut delta_group_cursor = CursorGroup::new(&mut delta_cursor, ());

            // I was not able to avoid 4-way code duplication below.  Depending on
            // whether `key` is found in the input and output trace, we must invoke
            // `transformer.transform` with four different combinations of
            // empty/non-empty cursors.  Since the cursors have different types
            // (`CursorEmpty` and `CursorGroup`), we can't bind them to the same
            // variable.
            if input_trace_cursor.key_valid() && input_trace_cursor.key() == key.as_ref() {
                let mut input_group_cursor = CursorGroup::new(&mut input_trace_cursor, ());

                output_trace_cursor.seek_key(&key);

                if output_trace_cursor.key_valid() && output_trace_cursor.key() == key.as_ref() {
                    let mut output_group_cursor = CursorGroup::new(&mut output_trace_cursor, ());

                    self.transformer.transform(
                        &mut delta_group_cursor,
                        &mut input_group_cursor,
                        &mut output_group_cursor,
                        cb,
                    );
                } else {
                    let mut output_group_cursor =
                        CursorEmpty::new(self.output_factories.weight_factory());

                    self.transformer.transform(
                        &mut delta_group_cursor,
                        &mut input_group_cursor,
                        &mut output_group_cursor,
                        cb,
                    );
                };
            } else {
                let mut input_group_cursor =
                    CursorEmpty::new(self.output_factories.weight_factory());

                output_trace_cursor.seek_key(&key);

                if output_trace_cursor.key_valid() && output_trace_cursor.key() == key.as_ref() {
                    let mut output_group_cursor = CursorGroup::new(&mut output_trace_cursor, ());

                    self.transformer.transform(
                        &mut delta_group_cursor,
                        &mut input_group_cursor,
                        &mut output_group_cursor,
                        cb,
                    );
                } else {
                    let mut output_group_cursor =
                        CursorEmpty::new(self.output_factories.weight_factory());

                    self.transformer.transform(
                        &mut CursorGroup::new(&mut delta_cursor, ()),
                        &mut input_group_cursor,
                        &mut output_group_cursor,
                        cb,
                    );
                };
            };
            match self.transformer.monotonicity() {
                // Descending transformer: push tuples from `buffer` to builder
                // in reverse order.
                Monotonicity::Descending => {
                    for tuple in self.buffer.dyn_iter_mut().rev() {
                        builder.push(tuple)
                    }
                }
                // Unordered transformer: sort the buffer before pushing tuples
                // to `builder`.
                Monotonicity::Unordered => {
                    self.buffer.sort();
                    for tuple in self.buffer.dyn_iter_mut() {
                        builder.push(tuple)
                    }
                }
                // Ascending transformer: all updates already pushed to builder.
                _ => {}
            }
            self.buffer.clear();

            delta_cursor.step_key();
        }

        builder.done()
    }
}
