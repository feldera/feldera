//! Ternary operator that applies an arbitrary ternary function to its inputs.

use crate::circuit::{
    metadata::OperatorLocation,
    operator_traits::{Operator, TernaryOperator},
    Circuit, OwnershipPreference, Scope, Stream,
};
use std::{borrow::Cow, panic::Location};

impl<C, T1> Stream<C, T1>
where
    C: Circuit,
    T1: Clone + 'static,
{
    /// Apply a user-provided ternary function to inputs from three streams at
    /// each timestamp.
    #[track_caller]
    pub fn apply3<F, T2, T3, T4>(
        &self,
        other1: &Stream<C, T2>,
        other2: &Stream<C, T3>,
        func: F,
    ) -> Stream<C, T4>
    where
        T2: Clone + 'static,
        T3: Clone + 'static,
        T4: Clone + 'static,
        F: Fn(Cow<'_, T1>, Cow<'_, T2>, Cow<'_, T3>) -> T4 + 'static,
    {
        self.apply3_with_preference(
            OwnershipPreference::INDIFFERENT,
            (other1, OwnershipPreference::INDIFFERENT),
            (other2, OwnershipPreference::INDIFFERENT),
            func,
        )
    }

    /// Apply a user-provided ternary function to inputs at each
    /// timestamp.
    ///
    /// Allows the caller to specify the ownership preference for
    /// each input stream.
    #[track_caller]
    pub fn apply3_with_preference<F, T2, T3, T4>(
        &self,
        self_preference: OwnershipPreference,
        other1: (&Stream<C, T2>, OwnershipPreference),
        other2: (&Stream<C, T3>, OwnershipPreference),
        func: F,
    ) -> Stream<C, T4>
    where
        T2: Clone + 'static,
        T3: Clone + 'static,
        T4: Clone + 'static,
        F: Fn(Cow<'_, T1>, Cow<'_, T2>, Cow<'_, T3>) -> T4 + 'static,
    {
        self.circuit().add_ternary_operator_with_preference(
            Apply3::new(func, Location::caller()),
            (self, self_preference),
            other1,
            other2,
        )
    }
}

/// Applies a user-provided ternary function to its inputs at each timestamp.
pub struct Apply3<F> {
    func: F,
    location: &'static Location<'static>,
}

impl<F> Apply3<F> {
    pub const fn new(func: F, location: &'static Location<'static>) -> Self
    where
        F: 'static,
    {
        Self { func, location }
    }
}

impl<F> Operator for Apply3<F>
where
    F: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("Apply3")
    }

    fn location(&self) -> OperatorLocation {
        Some(self.location)
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        // TODO: either change `F` type to `Fn` from `FnMut` or
        // parameterize the operator with custom fixed point check.
        unimplemented!();
    }
}

impl<T1, T2, T3, T4, F> TernaryOperator<T1, T2, T3, T4> for Apply3<F>
where
    F: Fn(Cow<'_, T1>, Cow<'_, T2>, Cow<'_, T3>) -> T4 + 'static,
    T1: Clone + 'static,
    T2: Clone + 'static,
    T3: Clone + 'static,
{
    fn eval<'a>(&mut self, i1: Cow<'a, T1>, i2: Cow<'a, T2>, i3: Cow<'a, T3>) -> T4 {
        (self.func)(i1, i2, i3)
    }
}

#[cfg(test)]
mod test {
    use crate::{operator::Generator, Circuit, RootCircuit};
    use std::vec;

    #[test]
    fn apply3_test() {
        let circuit: crate::CircuitHandle = RootCircuit::build(move |circuit| {
            let mut inputs1 = vec![2, 4, 6].into_iter();
            let mut inputs2 = vec![-1, -2, -3].into_iter();
            let mut inputs3 = vec![-1, -2, -3].into_iter();

            let source1 = circuit.add_source(Generator::new(move || inputs1.next().unwrap()));
            let source2 = circuit.add_source(Generator::new(move || inputs2.next().unwrap()));
            let source3 = circuit.add_source(Generator::new(move || inputs3.next().unwrap()));

            source1
                .apply3(&source2, &source3, |x, y, z| *x + *y + *z)
                .inspect(|z| assert_eq!(*z, 0));
            Ok(())
        })
        .unwrap()
        .0;

        for _ in 0..3 {
            circuit.step().unwrap();
        }
    }
}
