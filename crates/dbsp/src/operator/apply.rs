//! Operator that applies an arbitrary function to its input.

use crate::circuit::{
    metadata::OperatorLocation,
    operator_traits::{Data, Operator, UnaryOperator},
    Circuit, OwnershipPreference, Scope, Stream,
};
use std::{borrow::Cow, panic::Location};

impl<C, T1> Stream<C, T1>
where
    C: Circuit,
    T1: Clone + 'static,
{
    /// Returns a stream that contains `func(&x)` for each `x` in `self`.
    /// `func` cannot mutate captured state.
    ///
    /// The operator will have a generic name for debugging and profiling
    /// purposes.  Use [`Stream::apply_named`], instead, to give it a
    /// specific name.
    #[track_caller]
    pub fn apply<F, T2>(&self, func: F) -> Stream<C, T2>
    where
        F: Fn(&T1) -> T2 + 'static,
        T2: Clone + 'static,
    {
        self.circuit().add_unary_operator(
            Apply::<_, true>::new(func, Cow::Borrowed("Apply"), Location::caller()),
            self,
        )
    }

    /// Returns a stream that contains `func(&x)` for each `x` in `self`.
    /// `func` can access and mutate captured state.
    ///
    /// The operator will have a generic name for debugging and profiling
    /// purposes.  Use [`Stream::apply_mut_named`], instead, to give it a
    /// specific name.
    #[track_caller]
    pub fn apply_mut<F, T2>(&self, func: F) -> Stream<C, T2>
    where
        F: FnMut(&T1) -> T2 + 'static,
        T2: Clone + 'static,
    {
        self.circuit().add_unary_operator(
            Apply::<_, false>::new(func, Cow::Borrowed("ApplyMut"), Location::caller()),
            self,
        )
    }

    /// Returns a stream that contains `func(&x)` for each `x` in `self`, giving
    /// the operator the given `name` for debugging and profiling purposes.
    /// `func` cannot mutate captured state.
    #[track_caller]
    pub fn apply_named<N, F, T2>(&self, name: N, func: F) -> Stream<C, T2>
    where
        N: Into<Cow<'static, str>>,
        F: Fn(&T1) -> T2 + 'static,
        T2: Clone + 'static,
    {
        self.circuit().add_unary_operator(
            Apply::<_, true>::new(func, name.into(), Location::caller()),
            self,
        )
    }

    /// Returns a stream that contains `func(&x)` for each `x` in `self`, giving
    /// the operator the given `name` for debugging and profiling purposes.
    /// `func` can access and mutate captured state.
    #[track_caller]
    pub fn apply_mut_named<N, F, T2>(&self, name: N, func: F) -> Stream<C, T2>
    where
        N: Into<Cow<'static, str>>,
        F: Fn(&T1) -> T2 + 'static,
        T2: Clone + 'static,
    {
        self.circuit().add_unary_operator(
            Apply::<_, false>::new(func, name.into(), Location::caller()),
            self,
        )
    }

    /// Returns a stream that contains `func(x)` for each `x` in `self`.
    /// `func` cannot mutate captured state.
    ///
    /// The operator will have a generic name for debugging and profiling
    /// purposes.  Use [`Stream::apply_owned_named`], instead, to give it a
    /// specific name.
    #[track_caller]
    pub fn apply_owned<F, T2>(&self, func: F) -> Stream<C, T2>
    where
        F: Fn(T1) -> T2 + 'static,
        T2: Clone + 'static,
    {
        self.circuit().add_unary_operator(
            ApplyOwned::<_, true>::new(func, Cow::Borrowed("ApplyOwned"), Location::caller()),
            self,
        )
    }

    /// Returns a stream that contains `func(x)` for each `x` in `self`.
    /// `func` can access and mutate captured state.
    ///
    /// The operator will have a generic name for debugging and profiling
    /// purposes.  Use [`Stream::apply_mut_owned_named`], instead, to give it a
    /// specific name.
    #[track_caller]
    pub fn apply_mut_owned<F, T2>(&self, func: F) -> Stream<C, T2>
    where
        F: FnMut(T1) -> T2 + 'static,
        T2: Clone + 'static,
    {
        self.circuit().add_unary_operator(
            ApplyOwned::<_, false>::new(func, Cow::Borrowed("ApplyOwned"), Location::caller()),
            self,
        )
    }

    /// Returns a stream that contains `func(x)` for each `x` in `self`, giving
    /// the operator the given `name` for debugging and profiling purposes.
    /// `func` cannot mutate captured state.
    #[track_caller]
    pub fn apply_owned_named<N, F, T2>(&self, name: N, func: F) -> Stream<C, T2>
    where
        N: Into<Cow<'static, str>>,
        F: Fn(T1) -> T2 + 'static,
        T2: Data,
    {
        self.circuit().add_unary_operator(
            ApplyOwned::<_, true>::new(func, name.into(), Location::caller()),
            self,
        )
    }

    /// Returns a stream that contains `func(x)` for each `x` in `self`, giving
    /// the operator the given `name` for debugging and profiling purposes.
    /// `func` can access and mutate captured state.
    #[track_caller]
    pub fn apply_mut_owned_named<N, F, T2>(&self, name: N, func: F) -> Stream<C, T2>
    where
        N: Into<Cow<'static, str>>,
        F: FnMut(T1) -> T2 + 'static,
        T2: Data,
    {
        self.circuit().add_unary_operator(
            ApplyOwned::<_, false>::new(func, name.into(), Location::caller()),
            self,
        )
    }

    /// Apply the `ApplyCore` operator to `self` with a custom name
    #[track_caller]
    pub fn apply_core<N, T2, O, B, F>(
        &self,
        name: N,
        owned: O,
        borrowed: B,
        fixpoint: F,
    ) -> Stream<C, T2>
    where
        N: Into<Cow<'static, str>>,
        T2: Data,
        O: FnMut(T1) -> T2 + 'static,
        B: FnMut(&T1) -> T2 + 'static,
        F: Fn(Scope) -> bool + 'static,
    {
        self.circuit().add_unary_operator(
            ApplyCore::new(owned, borrowed, fixpoint, name.into(), Location::caller()),
            self,
        )
    }
}

/// Operator that applies a user provided function to its input at each
/// timestamp.
pub struct Apply<F, const FP: bool> {
    func: F,
    name: Cow<'static, str>,
    location: &'static Location<'static>,
}

impl<F, const FP: bool> Apply<F, FP> {
    pub const fn new(
        func: F,
        name: Cow<'static, str>,
        location: &'static Location<'static>,
    ) -> Self {
        Self {
            func,
            name,
            location,
        }
    }
}

impl<F, const FP: bool> Operator for Apply<F, FP>
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
        assert!(FP);
        true
    }
}

impl<T1, T2, F, const FP: bool> UnaryOperator<T1, T2> for Apply<F, FP>
where
    F: FnMut(&T1) -> T2 + 'static,
{
    fn eval(&mut self, i1: &T1) -> T2 {
        (self.func)(i1)
    }
}

pub struct ApplyOwned<F, const FP: bool> {
    apply: F,
    name: Cow<'static, str>,
    location: &'static Location<'static>,
}

impl<F, const FP: bool> ApplyOwned<F, FP> {
    pub const fn new(
        apply: F,
        name: Cow<'static, str>,
        location: &'static Location<'static>,
    ) -> Self {
        Self {
            apply,
            name,
            location,
        }
    }
}

impl<F, const FP: bool> Operator for ApplyOwned<F, FP>
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
        assert!(FP);
        true
    }
}

impl<T1, T2, F, const FP: bool> UnaryOperator<T1, T2> for ApplyOwned<F, FP>
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

pub struct ApplyCore<O, B, F> {
    owned: O,
    borrowed: B,
    fixpoint: F,
    name: Cow<'static, str>,
    location: &'static Location<'static>,
}

impl<O, B, F> ApplyCore<O, B, F> {
    pub const fn new(
        owned: O,
        borrowed: B,
        fixpoint: F,
        name: Cow<'static, str>,
        location: &'static Location<'static>,
    ) -> Self {
        Self {
            owned,
            borrowed,
            fixpoint,
            name,
            location,
        }
    }
}

impl<O, B, F> Operator for ApplyCore<O, B, F>
where
    O: 'static,
    B: 'static,
    F: Fn(Scope) -> bool + 'static,
{
    fn name(&self) -> Cow<'static, str> {
        self.name.clone()
    }

    fn location(&self) -> OperatorLocation {
        Some(self.location)
    }

    fn fixedpoint(&self, scope: Scope) -> bool {
        (self.fixpoint)(scope)
    }
}

impl<O, B, F, T1, T2> UnaryOperator<T1, T2> for ApplyCore<O, B, F>
where
    O: FnMut(T1) -> T2 + 'static,
    B: FnMut(&T1) -> T2 + 'static,
    F: Fn(Scope) -> bool + 'static,
{
    fn eval(&mut self, input: &T1) -> T2 {
        (self.borrowed)(input)
    }

    #[inline]
    fn eval_owned(&mut self, input: T1) -> T2 {
        (self.owned)(input)
    }

    #[inline]
    fn input_preference(&self) -> OwnershipPreference {
        OwnershipPreference::STRONGLY_PREFER_OWNED
    }
}
