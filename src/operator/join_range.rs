//! Range-join operators.
//!
//! Range-join is a form of non-equi join where each key in the left operand
//! matches a contiguous range of keys in the right operand.
//!
//! Consider two indexed Z-sets `z1` and `z2`, a function `range_func` that
//! maps a key in `z1` to a half-closed interval of keys `[lower, upper)` in
//! `z2` and another function `join_func` that, given `(k1, v1)` in `z1` and
//! `(k2, v2)` in `z2`, returns an iterable collection `c` of output values.
//! The range-join operator works as follows:
//! * For each `k1` in `z1`, find all keys in `z2` within the half-closed
//!   interval `join_range(k1)`.
//! * For each `((k1, v1), w1)` in `z1` and `((k2, v2), w2)` in `z2` where `k2 ∈
//!   join_range(k1)`, add all values in `join_func(k1,v1,k2,v2)` to the output
//!   batch with weight `w1 * w2`.

use crate::{
    algebra::MulByRef,
    circuit::{
        operator_traits::{BinaryOperator, Operator},
        Circuit, Scope, Stream,
    },
    trace::{cursor::Cursor, Batch, BatchReader},
    OrdIndexedZSet, OrdZSet,
};
use std::{borrow::Cow, marker::PhantomData};

impl<P, I1> Stream<Circuit<P>, I1>
where
    P: Clone + 'static,
{
    /// Range-join two streams into an `OrdZSet`.
    ///
    /// See module documentation for the definition of the range-join operator
    /// and its arguments.
    ///
    /// This operator is non-incremental, i.e., it joins the pair of batches it
    /// receives at each timestamp ignoring previous inputs.
    pub fn stream_join_range<RF, JF, It, I2>(
        &self,
        other: &Stream<Circuit<P>, I2>,
        range_func: RF,
        join_func: JF,
    ) -> Stream<Circuit<P>, OrdZSet<It::Item, I1::R>>
    where
        I1: BatchReader<Time = ()> + Clone + 'static,
        I1::R: MulByRef<Output = I1::R>,
        I2: BatchReader<Time = (), R = I1::R> + Clone + 'static,
        I2::Key: Ord,
        RF: Fn(&I1::Key) -> (I2::Key, I2::Key) + 'static,
        JF: Fn(&I1::Key, &I1::Val, &I2::Key, &I2::Val) -> It + 'static,
        It: IntoIterator + 'static,
        It::Item: Clone + Ord + 'static,
    {
        self.stream_join_range_generic(other, range_func, move |k1, v1, k2, v2| {
            join_func(k1, v1, k2, v2).into_iter().map(|k| (k, ()))
        })
    }

    /// Range-join two streams into an `OrdIndexedZSet`.
    ///
    /// See module documentation for the definition of the range-join operator
    /// and its arguments.
    ///
    /// In this version of the operator, the `join_func` closure returns
    /// an iterator over `(key, value)` pairs used to assemble the output
    /// indexed Z-set.
    ///
    /// This operator is non-incremental, i.e., it joins the pair of batches it
    /// receives at each timestamp ignoring previous inputs.
    pub fn stream_join_range_index<RF, JF, It, K, V, I2>(
        &self,
        other: &Stream<Circuit<P>, I2>,
        range_func: RF,
        join_func: JF,
    ) -> Stream<Circuit<P>, OrdIndexedZSet<K, V, I1::R>>
    where
        I1: BatchReader<Time = ()> + Clone + 'static,
        I1::R: MulByRef<Output = I1::R>,
        I2: BatchReader<Time = (), R = I1::R> + Clone + 'static,
        I2::Key: Ord,
        RF: Fn(&I1::Key) -> (I2::Key, I2::Key) + 'static,
        JF: Fn(&I1::Key, &I1::Val, &I2::Key, &I2::Val) -> It + 'static,
        K: Clone + Ord + 'static,
        V: Clone + Ord + 'static,
        It: IntoIterator<Item = (K, V)> + 'static,
    {
        self.stream_join_range_generic(other, range_func, join_func)
    }

    /// Like [`Self::stream_join_range`], but can return any indexed Z-set type.
    pub fn stream_join_range_generic<RF, JF, It, I2, O>(
        &self,
        other: &Stream<Circuit<P>, I2>,
        range_func: RF,
        join_func: JF,
    ) -> Stream<Circuit<P>, O>
    where
        I1: BatchReader<Time = (), R = O::R> + Clone + 'static,
        I2: BatchReader<Time = (), R = O::R> + Clone + 'static,
        I2::Key: Ord,
        O: Batch<Time = ()> + Clone + 'static,
        O::R: MulByRef<Output = O::R>,
        RF: Fn(&I1::Key) -> (I2::Key, I2::Key) + 'static,
        JF: Fn(&I1::Key, &I1::Val, &I2::Key, &I2::Val) -> It + 'static,
        It: IntoIterator<Item = (O::Key, O::Val)> + 'static,
    {
        self.circuit()
            .add_binary_operator(StreamJoinRange::new(range_func, join_func), self, other)
    }
}

pub struct StreamJoinRange<RF, JF, It, I1, I2, O> {
    range_func: RF,
    join_func: JF,
    _types: PhantomData<(It, I1, I2, O)>,
}

impl<RF, JF, It, I1, I2, O> StreamJoinRange<RF, JF, It, I1, I2, O> {
    pub fn new(range_func: RF, join_func: JF) -> Self {
        Self {
            range_func,
            join_func,
            _types: PhantomData,
        }
    }
}

impl<RF, JF, It, I1, I2, O> Operator for StreamJoinRange<RF, JF, It, I1, I2, O>
where
    I1: 'static,
    I2: 'static,
    It: 'static,
    RF: 'static,
    JF: 'static,
    O: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("StreamJoinRange")
    }
    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<RF, JF, It, I1, I2, O> BinaryOperator<I1, I2, O> for StreamJoinRange<RF, JF, It, I1, I2, O>
where
    I1: BatchReader<Time = (), R = O::R> + Clone + 'static,
    I2: BatchReader<Time = (), R = O::R> + Clone + 'static,
    I2::Key: Ord,
    O: Batch<Time = ()> + 'static,
    O::R: MulByRef<Output = O::R>,
    RF: Fn(&I1::Key) -> (I2::Key, I2::Key) + 'static,
    JF: Fn(&I1::Key, &I1::Val, &I2::Key, &I2::Val) -> It + 'static,
    It: IntoIterator<Item = (O::Key, O::Val)> + 'static,
{
    fn eval(&mut self, i1: &I1, i2: &I2) -> O {
        let mut tuples = Vec::new();
        let mut i1_cursor = i1.cursor();
        let mut i2_cursor = i2.cursor();

        // For each key in `i1`.
        while i1_cursor.key_valid() {
            // Compute the range of matching keys in `i2`.
            let (lower, upper) = (self.range_func)(i1_cursor.key());

            // Assuming that `lower` grows monotonically, we wouldn't need to rewind every
            // time.
            i2_cursor.rewind_keys();
            i2_cursor.seek_key(&lower);

            // Iterate over the `[lower, upper)` interval.
            while i2_cursor.key_valid() && i2_cursor.key() < &upper {
                // Iterate over all pairs of values in i1 and i2.
                i1_cursor.rewind_vals();
                while i1_cursor.val_valid() {
                    let w1 = i1_cursor.weight();
                    let k1 = i1_cursor.key();
                    let v1 = i1_cursor.val();
                    i2_cursor.rewind_vals();

                    while i2_cursor.val_valid() {
                        let w2 = i2_cursor.weight();
                        let w = w1.mul_by_ref(&w2);

                        // Add all `(k,v)` tuples output by `join_func` to the output batch.
                        for (k, v) in (self.join_func)(k1, v1, i2_cursor.key(), i2_cursor.val()) {
                            tuples.push((O::item_from(k, v), w.clone()));
                        }
                        i2_cursor.step_val();
                    }
                    i1_cursor.step_val();
                }
                i2_cursor.step_key();
            }
            i1_cursor.step_key();
        }

        O::from_tuples((), tuples)
    }
}

#[cfg(test)]
mod test {
    use crate::{operator::Generator, zset, Circuit};

    #[test]
    fn stream_join_range_test() {
        let circuit = Circuit::build(move |circuit| {
            let mut input1 = vec![
                zset! {
                    (1, "a") => 1,
                    (1, "b") => 2,
                    (2, "c") => 3,
                    (2, "d") => 4,
                    (3, "e") => 5,
                    (3, "f") => -2,
                },
                zset! {(1, "a") => 1},
                zset! {(1, "a") => 1},
                zset! {(4, "n") => 2},
                zset! {(1, "a") => 0},
            ]
            .into_iter();
            let mut input2 = vec![
                zset! {
                    (2, "g") => 3,
                    (2, "h") => 4,
                    (3, "i") => 5,
                    (3, "j") => -2,
                    (4, "k") => 5,
                    (4, "l") => -2,
                },
                zset! {(1, "b") => 1},
                zset! {(4, "m") => 1},
                zset! {},
                zset! {},
            ]
            .into_iter();
            let mut outputs = vec![
                zset! {
                    ((1, "a"), (2, "g")) => 3,
                    ((1, "a"), (2, "h")) => 4,
                    ((1, "b"), (2, "g")) => 6,
                    ((1, "b"), (2, "h")) => 8,
                    ((2, "c"), (2, "g")) => 9,
                    ((2, "c"), (2, "h")) => 12,
                    ((2, "c"), (3, "i")) => 15,
                    ((2, "c"), (3, "j")) => -6,
                    ((2, "d"), (2, "g")) => 12,
                    ((2, "d"), (2, "h")) => 16,
                    ((2, "d"), (3, "i")) => 20,
                    ((2, "d"), (3, "j")) => -8,
                    ((3, "e"), (2, "g")) => 15,
                    ((3, "e"), (2, "h")) => 20,
                    ((3, "e"), (3, "i")) => 25,
                    ((3, "e"), (3, "j")) => -10,
                    ((3, "e"), (4, "k")) => 25,
                    ((3, "e"), (4, "l")) => -10,
                    ((3, "f"), (2, "g")) => -6,
                    ((3, "f"), (2, "h")) => -8,
                    ((3, "f"), (3, "i")) => -10,
                    ((3, "f"), (3, "j")) => 4,
                    ((3, "f"), (4, "k")) => -10,
                    ((3, "f"), (4, "l")) => 4,
                },
                zset! {
                    ((1, "a"), (1, "b")) => 1,
                },
                zset! {},
                zset! {},
                zset! {},
            ]
            .into_iter();

            let index1 = circuit
                .add_source(Generator::new(move || input1.next().unwrap()))
                .index();
            let index2 = circuit
                .add_source(Generator::new(move || input2.next().unwrap()))
                .index();
            let output1 = index1.stream_join_range(
                &index2,
                |&k| (k - 1, k + 2),
                |&k1, &v1, &k2, &v2| Some(((k1, v1), (k2, v2))),
            );
            output1.inspect(move |fm| assert_eq!(fm, &outputs.next().unwrap()));
            let output2 = index1.stream_join_range_index(
                &index2,
                |&k| (k - 1, k + 2),
                |&k1, &v1, &k2, &v2| Some(((k1, v1), (k2, v2))),
            );
            output1
                .index()
                .apply2(&output2, |o1, o2| assert_eq!(o1, o2));
        })
        .unwrap()
        .0;

        for _ in 0..5 {
            circuit.step().unwrap();
        }
    }
}
