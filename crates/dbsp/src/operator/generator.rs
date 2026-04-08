//! Defines an operator that generates an infinite output stream from a single
//! seed value.

use crate::circuit::{
    Runtime, Scope,
    operator_traits::{Data, Operator, SourceOperator},
};
use std::{borrow::Cow, marker::PhantomData};

/// A source operator that yields an infinite output stream
/// from a generator function.
pub struct Generator<T, F> {
    generator: F,
    _t: PhantomData<T>,
}

impl<T, F> Generator<T, F> {
    /// Creates a generator
    pub fn new(g: F) -> Self {
        Self {
            generator: g,
            _t: Default::default(),
        }
    }
}

impl<T, F> Operator for Generator<T, F>
where
    T: 'static,
    F: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("Generator")
    }
    fn fixedpoint(&self, _scope: Scope) -> bool {
        false
    }
    fn is_input(&self) -> bool {
        true
    }
}

impl<T, F> SourceOperator<T> for Generator<T, F>
where
    F: FnMut() -> T + 'static,
    T: 'static,
{
    async fn eval(&mut self) -> T {
        (self.generator)()
    }
}

/// A source operator for testing transactions.
///
/// The generator function is called once per transaction at commit time.  It
/// returns an iterator that returns the values to output in successive steps.
/// The iterator must return at least one value because the scheduler always
/// invokes the operator at least once.  When the iterator ends, the generator
/// signals that flush is complete.
///
/// The value type must have a `Default` implementation so the operator has
/// something to output when it is evaluated before it is notified about
/// upstream flush.
#[cfg(test)]
pub struct IteratorGenerator<G, I, T> {
    generator: G,
    next: Option<T>,
    iterator: Option<I>,
}

#[cfg(test)]
impl<G, I, T> IteratorGenerator<G, I, T> {
    pub fn new(generator: G) -> Self {
        Self {
            generator,
            next: None,
            iterator: None,
        }
    }
}

#[cfg(test)]
impl<G, I, T> Operator for IteratorGenerator<G, I, T>
where
    G: FnMut() -> I + 'static,
    I: Iterator<Item = T> + 'static,
    T: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("IteratorGenerator")
    }
    fn fixedpoint(&self, _scope: Scope) -> bool {
        false
    }
    fn flush(&mut self) {
        let mut iterator = (self.generator)();
        self.next = Some(
            iterator
                .next()
                .expect("operator must produce at least one output"),
        );
        self.iterator = Some(iterator);
    }
    fn is_flush_complete(&self) -> bool {
        self.next.is_none()
    }
}

#[cfg(test)]
impl<G, I, T> SourceOperator<T> for IteratorGenerator<G, I, T>
where
    G: FnMut() -> I + 'static,
    I: Iterator<Item = T> + 'static,
    T: Default + 'static,
{
    async fn eval(&mut self) -> T {
        let result = self.next.take();
        if let Some(iterator) = &mut self.iterator {
            self.next = iterator.next();
            if self.next.is_none() {
                self.iterator = None;
            }
        };
        result.unwrap_or_default()
    }
}

/// A version of Generator that passes a flag to the generator function when `flush` has been called, giving it a chance
/// to produce one output per transaction.
pub struct TransactionGenerator<T, F> {
    generator: F,
    flush: bool,
    _t: PhantomData<T>,
}

impl<T, F> TransactionGenerator<T, F> {
    /// Creates a generator
    pub fn new(g: F) -> Self {
        Self {
            generator: g,
            flush: false,
            _t: Default::default(),
        }
    }
}

impl<T, F> Operator for TransactionGenerator<T, F>
where
    T: 'static,
    F: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("Generator")
    }
    fn flush(&mut self) {
        self.flush = true;
    }
    fn fixedpoint(&self, _scope: Scope) -> bool {
        false
    }
    fn is_input(&self) -> bool {
        true
    }
}

impl<T, F> SourceOperator<T> for TransactionGenerator<T, F>
where
    F: FnMut(bool) -> T + 'static,
    T: 'static,
{
    async fn eval(&mut self) -> T {
        let result = (self.generator)(self.flush);
        self.flush = false;
        result
    }
}

/// Generator operator for nested circuits.
///
/// At each parent clock tick, invokes a user-provided reset closure, which
/// returns a generator closure, that yields a nested stream.
pub struct GeneratorNested<T> {
    reset: Box<dyn FnMut() -> Box<dyn FnMut() -> T>>,
    generator: Option<Box<dyn FnMut() -> T>>,
    flush: bool,
    empty: T,
}

impl<T> GeneratorNested<T>
where
    T: Clone,
{
    /// Creates a nested generator with specified `reset` closure.
    pub fn new(reset: Box<dyn FnMut() -> Box<dyn FnMut() -> T>>, empty: T) -> Self {
        Self {
            reset,
            generator: None,
            flush: false,
            empty,
        }
    }
}

impl<T> Operator for GeneratorNested<T>
where
    T: Data,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("GeneratorNested")
    }
    fn clock_start(&mut self, scope: Scope) {
        if scope == 0 {
            self.generator = Some((self.reset)());
        }
    }

    fn is_input(&self) -> bool {
        true
    }

    fn fixedpoint(&self, _scope: Scope) -> bool {
        // TODO: do we want a version of `GeneratorNested` that
        // can inform the circuit that it's reached a fixedpoint?
        false
    }

    fn flush(&mut self) {
        self.flush = true;
    }
}

impl<T> SourceOperator<T> for GeneratorNested<T>
where
    T: Data,
{
    async fn eval(&mut self) -> T {
        if self.flush {
            self.flush = false;
            (self.generator.as_mut().unwrap())()
        } else {
            self.empty.clone()
        }
    }
}

/// A source operator that yields an infinite output stream
/// from a constant value.  Note: only worker 0 generates the data;
/// the other workers generate empty streams.
pub struct ConstantGenerator<T> {
    generator: T,
}

impl<T> ConstantGenerator<T>
where
    T: Clone,
{
    /// Creates a constant generator
    pub fn new(g: T) -> Self {
        Self { generator: g }
    }
}

impl<T> Operator for ConstantGenerator<T>
where
    T: Data + 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("ConstantGenerator")
    }
    fn fixedpoint(&self, _scope: Scope) -> bool {
        // Always a fixed-point
        true
    }
    fn is_input(&self) -> bool {
        true
    }
}

impl<T> SourceOperator<T> for ConstantGenerator<T>
where
    T: Data + 'static + Clone + Default,
{
    async fn eval(&mut self) -> T {
        if Runtime::worker_index() == 0 {
            self.generator.clone()
        } else {
            T::default()
        }
    }
}
