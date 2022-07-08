//! Filtering operators.

use crate::{
    circuit::{
        operator_traits::{Operator, UnaryOperator},
        Circuit, Scope, Stream,
    },
    trace::{Batch, BatchReader, Builder, Cursor},
    OrdIndexedZSet, OrdZSet,
};
use std::{borrow::Cow, marker::PhantomData};

impl<P, CI> Stream<Circuit<P>, CI>
where
    CI: BatchReader<Time = (), Val = ()> + Clone + 'static,
    CI::Key: Clone,
    P: Clone + 'static,
{
    /// Filter input batches using a predicate.
    ///
    /// The operator applies `predicate` to each key in the input
    /// batch and builds an output batch containing only the elements
    /// that satisfy the predicate.
    ///
    /// This operator returns output batches of type [`OrdZSet`].
    /// Use [`Self::filter_keys_generic`] to create other types
    /// of output batches.
    ///
    /// # Type arguments
    ///
    /// * `CI` - input collection type.
    /// * `F` - predicate function type.
    pub fn filter<F>(&self, predicate: F) -> Stream<Circuit<P>, OrdZSet<CI::Key, CI::R>>
    where
        CI::Key: Ord,
        F: Fn(&CI::Key) -> bool + 'static,
    {
        self.filter_keys_generic(predicate)
    }
}

impl<P, CI> Stream<Circuit<P>, CI>
where
    CI: BatchReader<Time = ()> + Clone + 'static,
    CI::Key: Clone,
    CI::Val: Clone,
    P: Clone + 'static,
{
    /// Filter input batches using a predicate over keys.
    ///
    /// The operator applies `predicate` to each key in the input
    /// batch and builds an output batch containing only the elements
    /// that satisfy the predicate.
    ///
    /// This operator returns output batches of type [`OrdIndexedZSet`].
    /// Use [`Self::filter_keys_generic`] to create other types
    /// of output batches.
    ///
    /// # Type arguments
    ///
    /// * `CI` - input collection type.
    /// * `F` - predicate function type.
    #[allow(clippy::type_complexity)]
    pub fn filter_keys<F>(
        &self,
        predicate: F,
    ) -> Stream<Circuit<P>, OrdIndexedZSet<CI::Key, CI::Val, CI::R>>
    where
        F: Fn(&CI::Key) -> bool + 'static,
        CI::Key: Ord,
        CI::Val: Ord,
    {
        self.filter_keys_generic(predicate)
    }

    /// Like [`filter_keys`](`Self::filter_keys`), but can return any batch
    /// type, not just `OrdZSet`.
    pub fn filter_keys_generic<CO, F>(&self, predicate: F) -> Stream<Circuit<P>, CO>
    where
        CO: Batch<Key = CI::Key, Val = CI::Val, Time = (), R = CI::R> + Clone + 'static,
        F: Fn(&CI::Key) -> bool + 'static,
    {
        self.circuit()
            .add_unary_operator(FilterKeys::new(predicate), self)
    }

    /// Filter input batches using a predicate over both key and values.
    ///
    /// The operator applies `predicate` to each `(key, value)` pair in the
    /// input batch and builds an output batch containing only the elements
    /// that satisfy the predicate.
    ///
    /// This operator returns output batches of type [`OrdIndexedZSet`].
    /// Use [`Self::filter_vals_generic`] to create other types
    /// of output batches.
    ///
    /// # Type arguments
    ///
    /// * `CI` - input collection type.
    /// * `F` - predicate function type.
    #[allow(clippy::type_complexity)]
    pub fn filter_vals<F>(
        &self,
        predicate: F,
    ) -> Stream<Circuit<P>, OrdIndexedZSet<CI::Key, CI::Val, CI::R>>
    where
        F: Fn(&CI::Key, &CI::Val) -> bool + 'static,
        CI::Key: Ord,
        CI::Val: Ord,
    {
        self.filter_vals_generic(predicate)
    }

    /// Like [`filter_vals`](`Self::filter_vals`), but can return any batch
    /// type, not just `OrdIndexedZSet`.
    pub fn filter_vals_generic<CO, F>(&self, predicate: F) -> Stream<Circuit<P>, CO>
    where
        CO: Batch<Key = CI::Key, Val = CI::Val, Time = (), R = CI::R> + Clone + 'static,
        F: Fn(&CI::Key, &CI::Val) -> bool + 'static,
    {
        self.circuit()
            .add_unary_operator(FilterVals::new(predicate), self)
    }
}

/// Internal implementation of the
/// [`filter_keys`](`crate::circuit::Stream::filter_keys`) operator.
pub struct FilterKeys<CI, CO, F>
where
    F: 'static,
{
    filter: F,
    _type: PhantomData<(CI, CO)>,
}

impl<CI, CO, F> FilterKeys<CI, CO, F>
where
    F: 'static,
{
    pub fn new(filter: F) -> Self {
        Self {
            filter,
            _type: PhantomData,
        }
    }
}

impl<CI, CO, F> Operator for FilterKeys<CI, CO, F>
where
    CI: 'static,
    CO: 'static,
    F: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("FilterKeys")
    }
    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<CI, CO, F> UnaryOperator<CI, CO> for FilterKeys<CI, CO, F>
where
    CI: BatchReader<Time = ()> + 'static,
    CI::Key: Clone,
    CI::Val: Clone,
    CO: Batch<Key = CI::Key, Val = CI::Val, Time = (), R = CI::R> + 'static,
    F: Fn(&CI::Key) -> bool + 'static,
{
    fn eval(&mut self, i: &CI) -> CO {
        let mut cursor = i.cursor();

        // We can use Builder because cursor yields ordered values.  This
        // is a nice property of the filter operation.

        // This will create waste if most tuples get filtered out, since
        // the buffers allocated here can make it all the way to the output batch.
        // This is probably ok, because the batch will either get freed at the end
        // of the current clock tick or get added to the trace, where it will likely
        // get merged with other batches soon, at which point the waste is gone.
        let mut builder = CO::Builder::with_capacity((), i.len());

        while cursor.key_valid() {
            if (self.filter)(cursor.key()) {
                while cursor.val_valid() {
                    let val = cursor.val().clone();
                    let w = cursor.weight();
                    builder.push((cursor.key().clone(), val, w.clone()));
                    cursor.step_val();
                }
            }
            cursor.step_key();
        }
        builder.done()
    }

    fn eval_owned(&mut self, i: CI) -> CO {
        // TODO: owned implementation
        self.eval(&i)
    }
}

/// Internal implementation of the
/// [`filter_vals`](`crate::circuit::Stream::filter_vals`) operator.
pub struct FilterVals<CI, CO, F>
where
    F: 'static,
{
    filter: F,
    _type: PhantomData<(CI, CO)>,
}

impl<CI, CO, F> FilterVals<CI, CO, F>
where
    F: 'static,
{
    pub fn new(filter: F) -> Self {
        Self {
            filter,
            _type: PhantomData,
        }
    }
}

impl<CI, CO, F> Operator for FilterVals<CI, CO, F>
where
    CI: 'static,
    CO: 'static,
    F: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("FilterVals")
    }
    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<CI, CO, F> UnaryOperator<CI, CO> for FilterVals<CI, CO, F>
where
    CI: BatchReader<Time = ()> + 'static,
    CI::Key: Clone,
    CI::Val: Clone,
    CO: Batch<Key = CI::Key, Val = CI::Val, Time = (), R = CI::R> + 'static,
    F: Fn(&CI::Key, &CI::Val) -> bool + 'static,
{
    fn eval(&mut self, i: &CI) -> CO {
        let mut cursor = i.cursor();

        // We can use Builder because cursor yields ordered values.  This
        // is a nice property of the filter operation.

        // This will create waste if most tuples get filtered out, since
        // the buffers allocated here can make it all the way to the output batch.
        // This is probably ok, because the batch will either get freed at the end
        // of the current clock tick or get added to the trace, where it will likely
        // get merged with other batches soon, at which point the waste is gone.
        let mut builder = CO::Builder::with_capacity((), i.len());

        while cursor.key_valid() {
            while cursor.val_valid() {
                if (self.filter)(cursor.key(), cursor.val()) {
                    let val = cursor.val().clone();
                    let w = cursor.weight();
                    builder.push((cursor.key().clone(), val, w.clone()));
                }
                cursor.step_val();
            }
            cursor.step_key();
        }
        builder.done()
    }

    fn eval_owned(&mut self, i: CI) -> CO {
        // TODO: owned implementation
        self.eval(&i)
    }
}

#[cfg(test)]
mod test {
    use crate::{circuit::Root, indexed_zset, operator::Generator, zset};
    use std::vec;

    #[test]
    fn filter_test() {
        let root = Root::build(move |circuit| {
            let mut inputs = vec![
                zset! { 1 => 1, 2 => 1, 3 => -1 },
                zset! { -1 => 1, -2 => 1, -3 => -1 },
                zset! { -1 => 1, 2 => 1, 3 => -1 },
            ]
            .into_iter();
            let mut expected_outputs = vec![
                zset! { 1 => 1, 2 => 1, 3 => -1 },
                zset! {},
                zset! { 2 => 1, 3 => -1 },
            ]
            .into_iter();

            let output_stream = circuit
                .add_source(Generator::new(move || inputs.next().unwrap()))
                .filter(|x| *x > 0);

            output_stream.inspect(move |zs| {
                assert_eq!(*zs, expected_outputs.next().unwrap());
            });
        })
        .unwrap();

        for _ in 0..3 {
            root.step().unwrap();
        }
    }

    #[test]
    fn filter_keyval_test() {
        let root = Root::build(move |circuit| {
            let mut inputs = vec![
                indexed_zset! { 1 => {"1" => 1, "foo" => 1}, 2 => {"2" => 1}, 3 => {"3" => -1} },
                indexed_zset! { -1 => {"-1" => 1}, -2 => {"-2" => 1}, -3 => {"-3" => -1, "foo" => 1} },
                indexed_zset! { -1 => {"-1" => 1}, 2 => {"2" => 1, "foo" => 1}, 3 => {"3" => -1} },
            ]
            .into_iter();

            let mut expected_outputs1 = vec![
                indexed_zset! { 1 => {"1" => 1, "foo" => 1}, 2 => {"2" => 1}, 3 => {"3" => -1} },
                indexed_zset! {},
                indexed_zset! { 2 => {"2" => 1, "foo" => 1}, 3 => {"3" => -1} },
            ]
            .into_iter();

            let mut expected_outputs2 = vec![
                indexed_zset! { 1 => {"1" => 1}, 2 => {"2" => 1}, 3 => {"3" => -1} },
                indexed_zset! {},
                indexed_zset! { 2 => {"2" => 1}, 3 => {"3" => -1} },
            ]
            .into_iter();

            let source = circuit.add_source(Generator::new(move || inputs.next().unwrap()));

            let output_stream1 = source.filter_keys(|x| *x > 0);
            let output_stream2 = source.filter_vals(|k, v| *k > 0 && *v != "foo");

            output_stream1.inspect(move |zs| {
                assert_eq!(*zs, expected_outputs1.next().unwrap());
            });
            output_stream2.inspect(move |zs| {
                assert_eq!(*zs, expected_outputs2.next().unwrap());
            });

        })
        .unwrap();

        for _ in 0..3 {
            root.step().unwrap();
        }
    }
}
