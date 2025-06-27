use std::{borrow::Cow, marker::PhantomData, panic::Location};

use crate::{
    circuit::{
        metadata::OperatorLocation,
        operator_traits::{BinaryOperator, Operator},
    },
    trace::{spine_async::WithSnapshot, BatchReaderFactories, SpineSnapshot},
    typed_batch::{Spine, TypedBatch},
    Batch, Circuit, Scope, Stream,
};

impl<C, B> Stream<C, B>
where
    C: Circuit,
    B: Batch,
{
    #[track_caller]
    pub fn accumulate(&self) -> Stream<C, Option<Spine<B>>> {
        let factories = BatchReaderFactories::new::<B::Key, B::Val, B::R>();

        let result = self.inner().dyn_accumulate(&factories);

        unsafe { result.transmute_payload() }
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
                &TypedBatch<B::Key, B::Val, B::R, SpineSnapshot<B::Inner>>,
                &TypedBatch<B2::Key, B2::Val, B2::R, SpineSnapshot<B2::Inner>>,
            ) -> T
            + 'static,
        T: Clone + 'static,
    {
        let stream1 = self.accumulate();
        let stream2 = other.accumulate();

        stream1.circuit().add_binary_operator(
            AccumulateApply2::<B, B2, _>::new(func, Location::caller()),
            &stream1,
            &stream2,
        )
    }
}

/// Applies a user-provided binary function to its inputs at each timestamp.
pub struct AccumulateApply2<B1, B2, F>
where
    B1: Batch,
    B2: Batch,
{
    func: F,
    location: &'static Location<'static>,
    input1: Option<TypedBatch<B1::Key, B1::Val, B1::R, SpineSnapshot<B1::Inner>>>,
    input2: Option<TypedBatch<B2::Key, B2::Val, B2::R, SpineSnapshot<B2::Inner>>>,
    flush: bool,
    phantom: PhantomData<fn(&B1, B2)>,
}

impl<B1, B2, F> AccumulateApply2<B1, B2, F>
where
    B1: Batch,
    B2: Batch,
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
    B1: Batch,
    B2: Batch,
    F: Fn(
            &TypedBatch<B1::Key, B1::Val, B1::R, SpineSnapshot<B1::Inner>>,
            &TypedBatch<B2::Key, B2::Val, B2::R, SpineSnapshot<B2::Inner>>,
        ) -> T
        + 'static,
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

impl<B1, B2, T, F> BinaryOperator<Option<Spine<B1>>, Option<Spine<B2>>, Option<T>>
    for AccumulateApply2<B1, B2, F>
where
    B1: Batch,
    B2: Batch,
    F: Fn(
            &TypedBatch<B1::Key, B1::Val, B1::R, SpineSnapshot<B1::Inner>>,
            &TypedBatch<B2::Key, B2::Val, B2::R, SpineSnapshot<B2::Inner>>,
        ) -> T
        + 'static,
{
    async fn eval(&mut self, i1: &Option<Spine<B1>>, i2: &Option<Spine<B2>>) -> Option<T> {
        if let Some(i1) = i1 {
            self.input1 = Some(TypedBatch::new(i1.ro_snapshot()));
        }

        if let Some(i2) = i2 {
            self.input2 = Some(TypedBatch::new(i2.ro_snapshot()));
        }

        if self.flush {
            self.flush = false;
            Some((self.func)(
                &self.input1.take().unwrap(),
                &self.input2.take().unwrap(),
            ))
        } else {
            None
        }
    }
}
