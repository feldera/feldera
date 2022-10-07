//! Operator that applies an arbitrary function to its input.

use crate::circuit::{
    metadata::OperatorLocation,
    operator_traits::{Operator, UnaryOperator},
    Circuit, OwnershipPreference, Scope, Stream,
};
use std::{borrow::Cow, panic::Location};

impl<P, T1> Stream<Circuit<P>, T1>
where
    P: Clone + 'static,
    T1: Clone + 'static,
{
    /// Apply  the `Apply` operator to `self`.
    #[track_caller]
    pub fn apply<F, T2>(&self, func: F) -> Stream<Circuit<P>, T2>
    where
        F: FnMut(&T1) -> T2 + 'static,
        T2: Clone + 'static,
    {
        self.circuit()
            .add_unary_operator(Apply::new(func, Location::caller()), self)
    }

    /// Apply the `ApplyOwned` operator to `self`
    #[track_caller]
    pub fn apply_owned<F, T2>(&self, func: F) -> Stream<Circuit<P>, T2>
    where
        F: FnMut(T1) -> T2 + 'static,
        T2: Clone + 'static,
    {
        self.circuit().add_unary_operator(
            ApplyOwned::new(func, Cow::Borrowed("ApplyOwned"), Location::caller()),
            self,
        )
    }

    /// Apply the `ApplyOwned` operator to `self` with a custom name
    #[track_caller]
    pub fn apply_owned_named<N, F, T2>(&self, name: N, func: F) -> Stream<Circuit<P>, T2>
    where
        N: Into<Cow<'static, str>>,
        F: FnMut(T1) -> T2 + 'static,
        T2: Clone + 'static,
    {
        self.circuit()
            .add_unary_operator(ApplyOwned::new(func, name.into(), Location::caller()), self)
    }
}

/// Operator that applies a user provided function to its input at each
/// timestamp.
pub struct Apply<F> {
    func: F,
    location: &'static Location<'static>,
}

impl<F> Apply<F> {
    pub const fn new(func: F, location: &'static Location<'static>) -> Self
    where
        F: 'static,
    {
        Self { func, location }
    }
}

impl<F> Operator for Apply<F>
where
    F: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("Apply")
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

impl<T1, T2, F> UnaryOperator<T1, T2> for Apply<F>
where
    F: FnMut(&T1) -> T2 + 'static,
{
    fn eval(&mut self, i1: &T1) -> T2 {
        (self.func)(i1)
    }
}

pub struct ApplyOwned<F> {
    apply: F,
    name: Cow<'static, str>,
    location: &'static Location<'static>,
}

impl<F> ApplyOwned<F> {
    pub const fn new(
        apply: F,
        name: Cow<'static, str>,
        location: &'static Location<'static>,
    ) -> Self
    where
        F: 'static,
    {
        Self {
            apply,
            name,
            location,
        }
    }
}

impl<F> Operator for ApplyOwned<F>
where
    F: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        self.name.clone()
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

impl<T1, T2, F> UnaryOperator<T1, T2> for ApplyOwned<F>
where
    F: FnMut(T1) -> T2 + 'static,
{
    fn eval(&mut self, _input: &T1) -> T2 {
        unreachable!("cannot use ApplyOwned with reference inputs")
    }

    #[inline]
    fn eval_owned(&mut self, input: T1) -> T2 {
        (self.apply)(input)
    }

    #[inline]
    fn input_preference(&self) -> OwnershipPreference {
        OwnershipPreference::STRONGLY_PREFER_OWNED
    }
}
