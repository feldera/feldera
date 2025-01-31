//! Defines a sink operator that inspects every element of its input stream by
//! applying a user-provided callback to it.

use crate::circuit::{
    operator_traits::{Operator, UnaryOperator},
    Circuit, Scope, Stream,
};
use std::{borrow::Cow, marker::PhantomData};

impl<C, D> Stream<C, D>
where
    D: Clone + 'static,
    C: Circuit,
{
    /// Apply [`Inspect`] operator to `self`.
    ///
    /// # Examples
    ///
    /// ```
    /// # use dbsp::{
    /// #     operator::Generator,
    /// #     Circuit, RootCircuit,
    /// # };
    /// let circuit = RootCircuit::build(move |circuit| {
    ///     let mut n = 1;
    ///     let stream = circuit.add_source(Generator::new(move || {
    ///         let res = n;
    ///         n += 1;
    ///         res
    ///     }));
    ///     // Print all values in `stream`.
    ///     stream.inspect(|n| println!("inspect: {}", n));
    ///     Ok(())
    /// })
    /// .unwrap();
    /// ```
    #[track_caller]
    pub fn inspect<F>(&self, callback: F) -> Self
    where
        F: FnMut(&D) + 'static,
    {
        let inspected = self
            .circuit()
            .add_unary_operator(Inspect::new(callback), &self.try_sharded_version());
        inspected.mark_sharded_if(self);
        inspected
    }
}

/// Sink operator that consumes a stream of values of type `T` and
/// applies a user-provided callback to each input.
pub struct Inspect<T, F> {
    callback: F,
    phantom: PhantomData<T>,
}

impl<T, F> Inspect<T, F>
where
    F: FnMut(&T),
{
    /// Create a new instance of the `Inspect` operator that will apply
    /// `callback` to each value in the input stream.
    pub fn new(callback: F) -> Self {
        Self {
            callback,
            phantom: PhantomData,
        }
    }
}

impl<T, F> Operator for Inspect<T, F>
where
    T: 'static,
    F: FnMut(&T) + 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("Inspect")
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<T, F> UnaryOperator<T, T> for Inspect<T, F>
where
    T: Clone + 'static,
    F: FnMut(&T) + 'static,
{
    async fn eval(&mut self, i: &T) -> T {
        (self.callback)(i);
        i.clone()
    }
    async fn eval_owned(&mut self, i: T) -> T {
        (self.callback)(&i);
        i
    }
}
