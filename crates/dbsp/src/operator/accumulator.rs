use std::{
    borrow::Cow,
    marker::PhantomData,
    panic::Location,
    sync::{atomic::AtomicUsize, Arc},
};

use crate::{
    circuit::{
        metadata::OperatorLocation,
        operator_traits::{BinaryOperator, Operator},
    },
    trace::{
        Batch as DynBatch, BatchReaderFactories, Spine as DynSpine, SpineSnapshot, WithSnapshot,
    },
    typed_batch::{Spine, TypedBatch},
    Batch, BatchReader, Circuit, Scope, Stream,
};

impl<C, B> Stream<C, B>
where
    C: Circuit,
    B: Batch,
{
    /// Accumulate changes within a clock cycle in a spine.
    ///
    /// Outputs a spine containing all input changes accumulated since the previous flush once per clock cycle,
    /// during flush, and `None` otherwise.
    ///
    /// This operator is a key part of efficient processing of long transactions.  It is used in conjunction with
    /// stateful operators like join, aggregate, distinct, etc., to supply all inputs comprising a transaction at
    /// once, avoiding computing mutually canceling changes.
    ///
    /// Using `Spine` to accumulate changes ensures that during a long transaction changes
    /// are pushed to storage and get compacted by background workers.
    #[track_caller]
    pub fn accumulate(&self) -> Stream<C, Option<Spine<B>>> {
        let factories = BatchReaderFactories::new::<B::Key, B::Val, B::R>();

        let result = self.inner().dyn_accumulate(&factories);

        unsafe { result.transmute_payload() }
    }

    /// Like [`Self::accumulate`], but also returns a reference to the enable count of the accumulator.
    ///
    /// Used to instantiate accumulators for output connectors. See `Accumulator::enable_count` documentation.
    #[track_caller]
    pub fn accumulate_with_enable_count(&self) -> (Stream<C, Option<Spine<B>>>, Arc<AtomicUsize>) {
        let factories = BatchReaderFactories::new::<B::Key, B::Val, B::R>();

        let (result, enable_count) = self.inner().dyn_accumulate_with_enable_count(&factories);

        (unsafe { result.transmute_payload() }, enable_count)
    }

    #[track_caller]
    pub fn accumulate_apply2<B2, F, T>(
        &self,
        other: &Stream<C, B2>,
        func: F,
    ) -> Stream<C, Option<T>>
    where
        B2: Batch,
        F: Fn(
                TypedBatch<B::Key, B::Val, B::R, SpineSnapshot<B::Inner>>,
                TypedBatch<B2::Key, B2::Val, B2::R, SpineSnapshot<B2::Inner>>,
            ) -> T
            + 'static,
        T: Clone + 'static,
    {
        let factories1 = BatchReaderFactories::new::<B::Key, B::Val, B::R>();
        let factories2 = BatchReaderFactories::new::<B2::Key, B2::Val, B2::R>();

        let stream1 = self.inner().dyn_accumulate(&factories1);
        let stream2 = other.inner().dyn_accumulate(&factories2);

        stream1.circuit().add_binary_operator(
            AccumulateApply2::<B::Inner, B2::Inner, _>::new(
                move |b1, b2| func(TypedBatch::from_inner(b1), TypedBatch::from_inner(b2)),
                Location::caller(),
            ),
            &stream1,
            &stream2,
        )
    }
}

/// Applies a user-provided binary function to its inputs at each timestamp.
pub struct AccumulateApply2<B1, B2, F>
where
    B1: DynBatch,
    B2: DynBatch,
{
    func: F,
    location: &'static Location<'static>,
    input1: Option<SpineSnapshot<B1>>,
    input2: Option<SpineSnapshot<B2>>,
    flush: bool,
    phantom: PhantomData<fn(&B1, &B2)>,
}

impl<B1, B2, F> AccumulateApply2<B1, B2, F>
where
    B1: DynBatch,
    B2: DynBatch,
{
    pub const fn new(func: F, location: &'static Location<'static>) -> Self
    where
        F: 'static,
    {
        Self {
            func,
            location,
            input1: None,
            input2: None,
            flush: false,
            phantom: PhantomData,
        }
    }
}

impl<B1, B2, F, T> Operator for AccumulateApply2<B1, B2, F>
where
    F: 'static,
    B1: DynBatch,
    B2: DynBatch,
    F: Fn(SpineSnapshot<B1>, SpineSnapshot<B2>) -> T + 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("AccumulateApply2")
    }

    fn location(&self) -> OperatorLocation {
        Some(self.location)
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        // TODO: either change `F` type to `Fn` from `FnMut` or
        // parameterize the operator with custom fixed point check.
        unimplemented!();
    }

    fn flush(&mut self) {
        self.flush = true;
    }
}

impl<B1, B2, T, F> BinaryOperator<Option<DynSpine<B1>>, Option<DynSpine<B2>>, Option<T>>
    for AccumulateApply2<B1, B2, F>
where
    B1: DynBatch,
    B2: DynBatch,
    F: Fn(SpineSnapshot<B1>, SpineSnapshot<B2>) -> T + 'static,
{
    async fn eval(&mut self, i1: &Option<DynSpine<B1>>, i2: &Option<DynSpine<B2>>) -> Option<T> {
        if let Some(i1) = i1 {
            self.input1 = Some(i1.ro_snapshot());
        }

        if let Some(i2) = i2 {
            self.input2 = Some(i2.ro_snapshot());
        }

        if self.flush {
            self.flush = false;
            Some((self.func)(
                self.input1.take().unwrap(),
                self.input2.take().unwrap(),
            ))
        } else {
            None
        }
    }
}
