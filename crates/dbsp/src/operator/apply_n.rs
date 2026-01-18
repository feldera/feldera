use std::{borrow::Cow, marker::PhantomData};

use crate::{
    Circuit, Stream,
    circuit::operator_traits::{NaryOperator, Operator},
};

#[track_caller]
pub fn apply_n<'a, C, T, T2, I, F>(circuit: &'a C, streams: I, func: F) -> Stream<C, T2>
where
    C: Circuit,
    I: Iterator<Item = &'a Stream<C, T>>,
    T: Clone + 'static,
    F: Fn(&mut dyn Iterator<Item = Cow<T>>) -> T2 + 'static,
    T2: Clone + 'static,
{
    circuit.add_nary_operator(
        ApplyN::<T, T2, F>::new(func, Cow::Borrowed("ApplyN")),
        streams,
    )
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
        true
    }
}

impl<T, T2, F> NaryOperator<T, T2> for ApplyN<T, T2, F>
where
    T: Clone + 'static,
    T2: 'static,
    F: Fn(&mut dyn Iterator<Item = Cow<T>>) -> T2 + 'static,
{
    async fn eval<'a, Iter>(&'a mut self, mut inputs: Iter) -> T2
    where
        Iter: Iterator<Item = Cow<'a, T>>,
    {
        (self.func)(&mut inputs)
    }
}
