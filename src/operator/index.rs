//! Index operator.

use crate::{
    algebra::{finite_map::KeyProperties, IndexedZSet, MapBuilder, ZRingValue},
    circuit::{
        operator_traits::{Operator, UnaryOperator},
        Circuit, NodeId, Scope, Stream,
    },
    circuit_cache_key, RefPair,
};
use std::{borrow::Cow, marker::PhantomData};

circuit_cache_key!(IndexId<C, D>(NodeId => Stream<C, D>));

impl<P, CI> Stream<Circuit<P>, CI>
where
    CI: Clone,
    P: Clone + 'static,
{
    /// Apply [`Index`] operator to `self`.
    pub fn index<K, V, W, CO>(&self) -> Stream<Circuit<P>, CO>
    where
        K: KeyProperties,
        V: KeyProperties,
        W: ZRingValue,
        CI: IntoIterator<Item = ((K, V), W)> + 'static,
        for<'a> &'a CI: IntoIterator,
        for<'a> <&'a CI as IntoIterator>::Item: RefPair<'a, (K, V), W>,
        CO: IndexedZSet<K, V, W>,
    {
        self.circuit()
            .cache_get_or_insert_with(IndexId::new(self.local_node_id()), || {
                self.circuit().add_unary_operator(Index::new(), self)
            })
            .clone()
    }
}

/// Operator that generates an indexed representation of a Z-set.
///
/// The input of the operator is a Z-set where the value type is
/// a key/value pair.  The output is an indexed representation of
/// the Z-set, i.e., a finite map from keys to Z-sets of
/// values associated with each key.
///
/// The input Z-set can be represented by any type that can be
/// converted into a sequence of `((key, value), weight)` pairs.
///
/// # Type arguments
///
/// * `K` - key type.
/// * `V` - value type.
/// * `W` - weight type.
/// * `CI` - input collection type.
/// * `CO` - output collection type, a finite map from keys to a Z-set of
///   values.
pub struct Index<K, V, W, CI, CO> {
    _type: PhantomData<(K, V, W, CI, CO)>,
}

impl<K, V, W, CI, CO> Index<K, V, W, CI, CO> {
    pub fn new() -> Self {
        Self { _type: PhantomData }
    }
}

impl<K, V, W, CI, CO> Default for Index<K, V, W, CI, CO> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V, W, CI, CO> Operator for Index<K, V, W, CI, CO>
where
    K: 'static,
    V: 'static,
    W: 'static,
    CI: 'static,
    CO: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("Index")
    }
    fn clock_start(&mut self, _scope: Scope) {}
    fn clock_end(&mut self, _scope: Scope) {}
}

impl<K, V, W, CI, CO> UnaryOperator<CI, CO> for Index<K, V, W, CI, CO>
where
    K: KeyProperties,
    V: KeyProperties,
    W: ZRingValue,
    CI: IntoIterator<Item = ((K, V), W)> + 'static,
    for<'a> &'a CI: IntoIterator,
    for<'a> <&'a CI as IntoIterator>::Item: RefPair<'a, (K, V), W>,
    CO: IndexedZSet<K, V, W>,
{
    fn eval(&mut self, i: &CI) -> CO {
        let mut res = CO::empty();
        for pair in i.into_iter() {
            let ((k, v), w) = pair.into_refs();
            res.update(k, |val| val.increment(v, w.clone()));
        }
        res
    }

    fn eval_owned(&mut self, i: CI) -> CO {
        let mut res = CO::empty();
        for ((k, v), w) in i.into_iter() {
            res.update_owned(k, |val| val.increment_owned(v, w.clone()));
        }
        res
    }
}

#[cfg(test)]
mod test {
    use crate::{algebra::FiniteHashMap, circuit::Root, finite_map, operator::Generator};

    #[test]
    fn index_sequence() {
        let root = Root::build(move |circuit| {
            let mut inputs = vec![
                vec![ ((1, "a"), 1)
                    , ((1, "b"), 1)
                    , ((2, "a"), 1)
                    , ((2, "c"), 1)
                    , ((1, "a"), 2)
                    , ((1, "b"), -1)
                ],
                vec![ ((1, "d"), 1)
                    , ((1, "e"), 1)
                    , ((2, "a"), -1)
                    , ((3, "a"), 2)
                ],
            ].into_iter();
            let mut outputs = vec![
                finite_map!{ 1 => finite_map!{"a" => 3}, 2 => finite_map!{"a" => 1, "c" => 1}},
                finite_map!{ 1 => finite_map!{"a" => 3, "d" => 1, "e" => 1}, 2 => finite_map!{"c" => 1}, 3 => finite_map!{"a" => 2}},
            ].into_iter();
            circuit.add_source(Generator::new(move || inputs.next().unwrap() ))
                   .index::<_, _, _, FiniteHashMap<_, _>>()
                   .integrate()
                   .inspect(move |fm: &FiniteHashMap<_, _>| assert_eq!(fm, &outputs.next().unwrap()));
        })
        .unwrap();

        for _ in 0..2 {
            root.step().unwrap();
        }
    }

    #[test]
    fn index_zset() {
        let root = Root::build(move |circuit| {
            let mut inputs = vec![
                finite_map!{
                      (1, "a") => 1
                    , (1, "b") => 1
                    , (2, "a") => 1
                    , (2, "c") => 1
                    , (1, "a") => 2
                    , (1, "b") => -1
                },
                finite_map!{
                      (1, "d") => 1
                    , (1, "e") => 1
                    , (2, "a") => -1
                    , (3, "a") => 2
                },
            ].into_iter();
            let mut outputs = vec![
                finite_map!{ 1 => finite_map!{"a" => 3}, 2 => finite_map!{"a" => 1, "c" => 1}},
                finite_map!{ 1 => finite_map!{"a" => 3, "d" => 1, "e" => 1}, 2 => finite_map!{"c" => 1}, 3 => finite_map!{"a" => 2}},
            ].into_iter();
            circuit.add_source(Generator::new(move || inputs.next().unwrap() ))
                   .index::<_, _, _, FiniteHashMap<_, _>>()
                   .integrate()
                   .inspect(move |fm: &FiniteHashMap<_, _>| assert_eq!(fm, &outputs.next().unwrap()));
        })
        .unwrap();

        for _ in 0..2 {
            root.step().unwrap();
        }
    }
}
