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
    algebra::{IndexedZSet, IndexedZSetReader, MulByRef, OrdIndexedZSet, OrdZSet},
    circuit::{
        operator_traits::{BinaryOperator, Operator},
        Circuit, Scope, Stream,
    },
    dynamic::{DataTrait, DynUnit, Erase},
    trace::{BatchFactories, BatchReaderFactories, Cursor},
    DBData, ZWeight,
};
use std::{borrow::Cow, marker::PhantomData};

pub struct StreamJoinRangeFactories<I, O>
where
    I: IndexedZSetReader,
    O: IndexedZSet,
{
    input2_factories: I::Factories,
    output_factories: O::Factories,
}

impl<I: IndexedZSetReader, O: IndexedZSet> Clone for StreamJoinRangeFactories<I, O> {
    fn clone(&self) -> Self {
        Self {
            input2_factories: self.input2_factories.clone(),
            output_factories: self.output_factories.clone(),
        }
    }
}

impl<I, O> StreamJoinRangeFactories<I, O>
where
    I: IndexedZSetReader,
    O: IndexedZSet,
{
    pub fn new<IKType, IVType, OKType, OVType>() -> Self
    where
        IKType: DBData + Erase<I::Key>,
        IVType: DBData + Erase<I::Val>,
        OKType: DBData + Erase<O::Key>,
        OVType: DBData + Erase<O::Val>,
    {
        Self {
            input2_factories: BatchReaderFactories::new::<IKType, IVType, ZWeight>(),
            output_factories: BatchReaderFactories::new::<OKType, OVType, ZWeight>(),
        }
    }
}

impl<C, I1> Stream<C, I1>
where
    C: Circuit,
{
    /// See [`Stream::stream_join_range`].
    pub fn dyn_stream_join_range<I2, V>(
        &self,
        factories: &StreamJoinRangeFactories<I2, OrdZSet<V>>,
        other: &Stream<C, I2>,
        range_func: Box<dyn Fn(&I1::Key, &mut I2::Key, &mut I2::Key)>,
        join_func: Box<
            dyn Fn(&I1::Key, &I1::Val, &I2::Key, &I2::Val, &mut dyn FnMut(&mut V, &mut DynUnit)),
        >,
    ) -> Stream<C, OrdZSet<V>>
    where
        I1: IndexedZSetReader + Clone,
        I2: IndexedZSetReader + Clone,
        V: DataTrait + ?Sized,
    {
        self.dyn_stream_join_range_generic(factories, other, range_func, join_func)
    }

    /// See [`Stream::stream_join_range_index`].
    pub fn dyn_stream_join_range_index<K, V, I2>(
        &self,
        factories: &StreamJoinRangeFactories<I2, OrdIndexedZSet<K, V>>,
        other: &Stream<C, I2>,
        range_func: Box<dyn Fn(&I1::Key, &mut I2::Key, &mut I2::Key)>,
        join_func: Box<
            dyn Fn(&I1::Key, &I1::Val, &I2::Key, &I2::Val, &mut dyn FnMut(&mut K, &mut V)),
        >,
    ) -> Stream<C, OrdIndexedZSet<K, V>>
    where
        I1: IndexedZSetReader + Clone,
        I2: IndexedZSetReader + Clone,
        K: DataTrait + ?Sized,
        V: DataTrait + ?Sized,
    {
        self.dyn_stream_join_range_generic(factories, other, range_func, join_func)
    }

    /// Like [`Self::dyn_stream_join_range`], but can return any indexed Z-set
    /// type.
    pub fn dyn_stream_join_range_generic<I2, O>(
        &self,
        factories: &StreamJoinRangeFactories<I2, O>,
        other: &Stream<C, I2>,
        range_func: Box<dyn Fn(&I1::Key, &mut I2::Key, &mut I2::Key)>,
        join_func: Box<
            dyn Fn(
                &I1::Key,
                &I1::Val,
                &I2::Key,
                &I2::Val,
                &mut dyn FnMut(&mut O::Key, &mut O::Val),
            ),
        >,
    ) -> Stream<C, O>
    where
        I1: IndexedZSetReader + Clone,
        I2: IndexedZSetReader + Clone,
        O: IndexedZSet,
    {
        self.circuit().add_binary_operator(
            StreamJoinRange::new(factories, range_func, join_func),
            self,
            other,
        )
    }
}

pub struct StreamJoinRange<I1, I2, O>
where
    I1: IndexedZSetReader,
    I2: IndexedZSetReader,
    O: IndexedZSet,
{
    factories: StreamJoinRangeFactories<I2, O>,
    range_func: Box<dyn Fn(&I1::Key, &mut I2::Key, &mut I2::Key)>,
    join_func: Box<
        dyn Fn(&I1::Key, &I1::Val, &I2::Key, &I2::Val, &mut dyn FnMut(&mut O::Key, &mut O::Val)),
    >,
    _types: PhantomData<(I1, I2, O)>,
}

impl<I1, I2, O> StreamJoinRange<I1, I2, O>
where
    I1: IndexedZSetReader,
    I2: IndexedZSetReader,
    O: IndexedZSet,
{
    pub fn new(
        factories: &StreamJoinRangeFactories<I2, O>,
        range_func: Box<dyn Fn(&I1::Key, &mut I2::Key, &mut I2::Key)>,
        join_func: Box<
            dyn Fn(
                &I1::Key,
                &I1::Val,
                &I2::Key,
                &I2::Val,
                &mut dyn FnMut(&mut O::Key, &mut O::Val),
            ),
        >,
    ) -> Self {
        Self {
            factories: factories.clone(),
            range_func,
            join_func,
            _types: PhantomData,
        }
    }
}

impl<I1, I2, O> Operator for StreamJoinRange<I1, I2, O>
where
    I1: IndexedZSetReader,
    I2: IndexedZSetReader,
    O: IndexedZSet,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("StreamJoinRange")
    }
    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<I1, I2, O> BinaryOperator<I1, I2, O> for StreamJoinRange<I1, I2, O>
where
    I1: IndexedZSetReader + Clone,
    I2: IndexedZSetReader + Clone,
    O: IndexedZSet,
{
    async fn eval(&mut self, i1: &I1, i2: &I2) -> O {
        let mut tuples = self
            .factories
            .output_factories
            .weighted_items_factory()
            .default_box();

        let mut item = self
            .factories
            .output_factories
            .weighted_item_factory()
            .default_box();

        let mut i1_cursor = i1.cursor();
        let mut i2_cursor = i2.cursor();

        let mut lower = self.factories.input2_factories.key_factory().default_box();
        let mut upper = self.factories.input2_factories.key_factory().default_box();

        // For each key in `i1`.
        while i1_cursor.key_valid() {
            // Compute the range of matching keys in `i2`.
            (self.range_func)(i1_cursor.key(), lower.as_mut(), upper.as_mut());

            // Assuming that `lower` grows monotonically, we wouldn't need to rewind every
            // time.
            i2_cursor.rewind_keys();
            i2_cursor.seek_key(&lower);

            // Iterate over the `[lower, upper)` interval.
            while i2_cursor.key_valid() && i2_cursor.key() < &upper {
                // Iterate over all pairs of values in i1 and i2.
                i1_cursor.rewind_vals();
                while i1_cursor.val_valid() {
                    let w1 = **i1_cursor.weight();
                    let k1 = i1_cursor.key();
                    let v1 = i1_cursor.val();
                    i2_cursor.rewind_vals();

                    while i2_cursor.val_valid() {
                        let w2 = **i2_cursor.weight();
                        let w = w1.mul_by_ref(&w2);

                        // Add all `(k,v)` tuples output by `join_func` to the output batch.
                        (self.join_func)(k1, v1, i2_cursor.key(), i2_cursor.val(), &mut |k, v| {
                            let (kv, weight) = item.split_mut();
                            kv.from_vals(k, v);
                            **weight = w;
                            tuples.push_val(item.as_mut());
                        });
                        i2_cursor.step_val();
                    }
                    i1_cursor.step_val();
                }
                i2_cursor.step_key();
            }
            i1_cursor.step_key();
        }

        O::dyn_from_tuples(&self.factories.output_factories, (), &mut tuples)
    }
}

#[cfg(test)]
mod test {
    use crate::{operator::Generator, utils::Tup2, zset, Circuit, RootCircuit};

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
                .map_index(|Tup2(k, v)| (*k, *v));
            let index2 = circuit
                .add_source(Generator::new(move || input2.next().unwrap()))
                .map_index(|Tup2(k, v)| (*k, *v));
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
                .map_index(|Tup2(k, v)| (*k, *v))
                .apply2(&output2, |o1, o2| assert_eq!(o1, o2));
            Ok(())
        })
        .unwrap()
        .0;

        for _ in 0..5 {
            circuit.transaction().unwrap();
        }
    }
}
