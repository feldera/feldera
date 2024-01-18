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
    algebra::{MulByRef, ZRingValue},
    circuit::{
        operator_traits::{BinaryOperator, Operator},
        Circuit, Scope, Stream,
    },
    trace::{cursor::Cursor, Batch, BatchReader},
    DBData, OrdIndexedZSet, OrdZSet,
};
use std::{borrow::Cow, marker::PhantomData};

impl<C, I1> Stream<C, I1>
where
    C: Circuit,
{
    /// Range-join two streams into an `OrdZSet` according to the definition of
    /// the [range-join operator](crate::operator::join_range).
    ///
    /// This operator is non-incremental, i.e., it joins the pair of batches it
    /// receives at each timestamp ignoring previous inputs.
    pub fn stream_join_range<RF, JF, It, I2>(
        &self,
        other: &Stream<C, I2>,
        range_func: RF,
        join_func: JF,
    ) -> Stream<C, OrdZSet<It::Item, I1::R>>
    where
        I1: BatchReader<Time = ()> + Clone,
        I1::R: ZRingValue,
        I2: BatchReader<Time = (), R = I1::R> + Clone,
        RF: Fn(&I1::Key) -> (I2::Key, I2::Key) + 'static,
        JF: Fn(&I1::Key, &I1::Val, &I2::Key, &I2::Val) -> It + 'static,
        It: IntoIterator + 'static,
        It::Item: DBData,
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
        other: &Stream<C, I2>,
        range_func: RF,
        join_func: JF,
    ) -> Stream<C, OrdIndexedZSet<K, V, I1::R>>
    where
        I1: BatchReader<Time = ()> + Clone,
        I1::R: ZRingValue,
        I2: BatchReader<Time = (), R = I1::R> + Clone,
        RF: Fn(&I1::Key) -> (I2::Key, I2::Key) + 'static,
        JF: Fn(&I1::Key, &I1::Val, &I2::Key, &I2::Val) -> It + 'static,
        K: DBData,
        V: DBData,
        It: IntoIterator<Item = (K, V)> + 'static,
    {
        self.stream_join_range_generic(other, range_func, join_func)
    }

    /// Like [`Self::stream_join_range`], but can return any indexed Z-set type.
    pub fn stream_join_range_generic<RF, JF, It, I2, O>(
        &self,
        other: &Stream<C, I2>,
        range_func: RF,
        join_func: JF,
    ) -> Stream<C, O>
    where
        I1: BatchReader<Time = (), R = O::R> + Clone,
        I2: BatchReader<Time = (), R = O::R> + Clone,
        O: Batch<Time = ()>,
        O::R: ZRingValue,
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
    I1: BatchReader<Time = (), R = O::R> + Clone,
    I2: BatchReader<Time = (), R = O::R> + Clone,
    O: Batch<Time = ()>,
    O::R: ZRingValue,
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
    use crate::utils::Tup2;
    use crate::{operator::Generator, zset, Circuit, RootCircuit};

    #[test]
    fn stream_join_range_test() {
        let circuit = RootCircuit::build(move |circuit| {
            let mut input1 = vec![
                zset! {
                    Tup2(1, 'a') => 1,
                    Tup2(1, 'b') => 2,
                    Tup2(2, 'c') => 3,
                    Tup2(2, 'd') => 4,
                    Tup2(3, 'e') => 5,
                    Tup2(3, 'f') => -2,
                },
                zset! {Tup2(1, 'a') => 1},
                zset! {Tup2(1, 'a') => 1},
                zset! {Tup2(4, 'n') => 2},
                zset! {Tup2(1, 'a') => 0},
            ]
            .into_iter();
            let mut input2 = vec![
                zset! {
                    Tup2(2, 'g') => 3,
                    Tup2(2, 'h') => 4,
                    Tup2(3, 'i') => 5,
                    Tup2(3, 'j') => -2,
                    Tup2(4, 'k') => 5,
                    Tup2(4, 'l') => -2,
                },
                zset! {Tup2(1, 'b') => 1},
                zset! {Tup2(4, 'm') => 1},
                zset! {},
                zset! {},
            ]
            .into_iter();
            let mut outputs = vec![
                zset! {
                    Tup2(Tup2(1, 'a'), Tup2(2, 'g')) => 3,
                    Tup2(Tup2(1, 'a'), Tup2(2, 'h')) => 4,
                    Tup2(Tup2(1, 'b'), Tup2(2, 'g')) => 6,
                    Tup2(Tup2(1, 'b'), Tup2(2, 'h')) => 8,
                    Tup2(Tup2(2, 'c'), Tup2(2, 'g')) => 9,
                    Tup2(Tup2(2, 'c'), Tup2(2, 'h')) => 12,
                    Tup2(Tup2(2, 'c'), Tup2(3, 'i')) => 15,
                    Tup2(Tup2(2, 'c'), Tup2(3, 'j')) => -6,
                    Tup2(Tup2(2, 'd'), Tup2(2, 'g')) => 12,
                    Tup2(Tup2(2, 'd'), Tup2(2, 'h')) => 16,
                    Tup2(Tup2(2, 'd'), Tup2(3, 'i')) => 20,
                    Tup2(Tup2(2, 'd'), Tup2(3, 'j')) => -8,
                    Tup2(Tup2(3, 'e'), Tup2(2, 'g')) => 15,
                    Tup2(Tup2(3, 'e'), Tup2(2, 'h')) => 20,
                    Tup2(Tup2(3, 'e'), Tup2(3, 'i')) => 25,
                    Tup2(Tup2(3, 'e'), Tup2(3, 'j')) => -10,
                    Tup2(Tup2(3, 'e'), Tup2(4, 'k')) => 25,
                    Tup2(Tup2(3, 'e'), Tup2(4, 'l')) => -10,
                    Tup2(Tup2(3, 'f'), Tup2(2, 'g')) => -6,
                    Tup2(Tup2(3, 'f'), Tup2(2, 'h')) => -8,
                    Tup2(Tup2(3, 'f'), Tup2(3, 'i')) => -10,
                    Tup2(Tup2(3, 'f'), Tup2(3, 'j')) => 4,
                    Tup2(Tup2(3, 'f'), Tup2(4, 'k')) => -10,
                    Tup2(Tup2(3, 'f'), Tup2(4, 'l')) => 4,
                },
                zset! {
                    Tup2(Tup2(1, 'a'), Tup2(1, 'b')) => 1,
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
                |&k1, &v1, &k2, &v2| Some(Tup2(Tup2(k1, v1), Tup2(k2, v2))),
            );
            output1.inspect(move |fm| assert_eq!(fm, &outputs.next().unwrap()));
            let output2 = index1.stream_join_range_index(
                &index2,
                |&k| (k - 1, k + 2),
                |&k1, &v1, &k2, &v2| Some((Tup2(k1, v1), Tup2(k2, v2))),
            );
            output1
                .index()
                .apply2(&output2, |o1, o2| assert_eq!(o1, o2));
            Ok(())
        })
        .unwrap()
        .0;

        for _ in 0..5 {
            circuit.step().unwrap();
        }
    }
}
