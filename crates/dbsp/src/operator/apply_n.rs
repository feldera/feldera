use std::{borrow::Cow, marker::PhantomData};

use crate::{
    circuit::operator_traits::{NaryOperator, Operator},
    ChildCircuit, Circuit, Stream, Timestamp,
};

impl<P, TS> ChildCircuit<P, TS>
where
    P: Clone + 'static,
    TS: Timestamp,
{
    #[track_caller]
    pub fn apply_n<'a, T, T2, I, F>(&'a self, streams: I, func: F) -> Stream<Self, T2>
    where
        I: Iterator<Item = &'a Stream<Self, T>>,
        T: Clone + 'static,
        F: FnMut(&dyn Iterator<Item = Cow<T>>) -> T2 + 'static,
        T2: Clone + 'static,
    {
        self.add_nary_operator(
            ApplyN::<T, T2, F>::new(func, Cow::Borrowed("ApplyN")),
            streams,
        )
    }
}

struct ApplyN<T, T2, F> {
    name: Cow<'static, str>,
    func: F,
    phantom: PhantomData<dyn Fn(&T, &T2)>,
}

impl<T, T2, F> ApplyN<T, T2, F> {
    fn new(func: F, name: Cow<'static, str>) -> Self {
        Self {
            func,
            name,
            phantom: PhantomData,
        }
    }
}

impl<T, T2, F> Operator for ApplyN<T, T2, F>
where
    T: 'static,
    T2: 'static,
    F: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        self.name.clone()
    }

    fn fixedpoint(&self, _scope: crate::circuit::Scope) -> bool {
        // TODO: either change `F` type to `Fn` from `FnMut` or
        // parameterize the operator with custom fixed point check.
        unimplemented!();
    }
}

impl<T, T2, F> NaryOperator<T, T2> for ApplyN<T, T2, F>
where
    T: Clone + 'static,
    T2: 'static,
    F: FnMut(&dyn Iterator<Item = Cow<T>>) -> T2 + 'static,
{
    async fn eval<'a, Iter>(&'a mut self, inputs: Iter) -> T2
    where
        Iter: Iterator<Item = Cow<'a, T>>,
    {
        (self.func)(&inputs)
    }
}
