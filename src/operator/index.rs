//! Operators to convert Z-sets into indexed Z-sets.

use crate::{
    circuit::{
        operator_traits::{Operator, UnaryOperator},
        Circuit, NodeId, Scope, Stream,
    },
    circuit_cache_key,
    trace::{cursor::Cursor, ord::OrdIndexedZSet, Batch, BatchReader, Builder},
};
use std::{borrow::Cow, marker::PhantomData};

circuit_cache_key!(IndexId<C, D>(NodeId => Stream<C, D>));

impl<P, CI> Stream<Circuit<P>, CI>
where
    CI: Clone + 'static,
    P: Clone + 'static,
{
    /// Convert input batches to an indexed representation.
    ///
    /// Converts input batches whose key type is a `(key,value)` tuple into an
    /// indexed Z-set using the first element of each tuple as a key and the
    /// second element as the value. The indexed Z-set representation is
    /// used as input to various join and aggregation operators.
    pub fn index<K, V>(&self) -> Stream<Circuit<P>, OrdIndexedZSet<K, V, CI::R>>
    where
        K: Ord + Clone + 'static,
        V: Ord + Clone + 'static,
        CI: BatchReader<Key = (K, V), Val = (), Time = ()>,
    {
        self.index_generic()
    }

    /// Like [`index`](`Self::index`), but can return any indexed Z-set type,
    /// not just `OrdIndexedZSet`.
    pub fn index_generic<CO>(&self) -> Stream<Circuit<P>, CO>
    where
        CI: BatchReader<Key = (CO::Key, CO::Val), Val = (), Time = (), R = CO::R> + 'static,
        CO: Batch<Time = ()> + Clone + 'static,
        CO::Key: Clone,
        CO::Val: Clone,
    {
        self.circuit()
            .cache_get_or_insert_with(IndexId::new(self.local_node_id()), || {
                self.circuit().add_unary_operator(Index::new(), self)
            })
            .clone()
    }

    /// Convert input batches to an indexed representation with the help of a
    /// user-provided function that maps a key in the input Z-set into an
    /// output `(key, value)` pair.
    ///
    /// Converts input batches into an indexed Z-set by applying `index_func` to
    /// each key in the input batch and using the first element of the
    /// resulting tuple as a key and the second element as the value.  The
    /// indexed Z-set representation is used as input to join and
    /// aggregation operators.
    pub fn index_with<K, V, F>(
        &self,
        index_func: F,
    ) -> Stream<Circuit<P>, OrdIndexedZSet<K, V, CI::R>>
    where
        CI: BatchReader<Time = (), Val = ()> + 'static,
        F: Fn(&CI::Key) -> (K, V) + Clone + 'static,
        K: Ord + Clone + 'static,
        V: Ord + Clone + 'static,
    {
        self.index_with_generic(index_func)
    }

    /// Like [`index_with`](`Self::index_with`), but can return any indexed
    /// Z-set type, not just `OrdIndexedZSet`.
    pub fn index_with_generic<CO, F>(&self, index_func: F) -> Stream<Circuit<P>, CO>
    where
        CI: BatchReader<Time = (), Val = ()> + 'static,
        CO: Batch<Time = (), R = CI::R> + Clone + 'static,
        CO::Key: Clone + Ord,
        CO::Val: Clone + Ord,
        F: Fn(&CI::Key) -> (CO::Key, CO::Val) + Clone + 'static,
    {
        self.circuit()
            .add_unary_operator(IndexWith::new(index_func), self)
    }
}

/// Operator that generates an indexed representation of a Z-set.
///
/// The input of the operator is a Z-set where the value type is
/// a key/value pair.  The output is an indexed representation of
/// the Z-set.
///
/// # Type arguments
///
/// * `CI` - input batch type.
/// * `CO` - output batch type.
pub struct Index<CI, CO> {
    _type: PhantomData<(CI, CO)>,
}

impl<CI, CO> Index<CI, CO> {
    pub fn new() -> Self {
        Self { _type: PhantomData }
    }
}

impl<CI, CO> Default for Index<CI, CO> {
    fn default() -> Self {
        Self::new()
    }
}

impl<CI, CO> Operator for Index<CI, CO>
where
    CI: 'static,
    CO: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("Index")
    }
    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<CI, CO> UnaryOperator<CI, CO> for Index<CI, CO>
where
    CO: Batch<Time = ()> + Clone + 'static,
    CI: BatchReader<Key = (CO::Key, CO::Val), Val = (), Time = (), R = CO::R> + 'static,
    CO::Key: Clone,
    CO::Val: Clone,
{
    fn eval(&mut self, i: &CI) -> CO {
        let mut builder = <CO as Batch>::Builder::with_capacity((), i.len());

        let mut cursor = i.cursor();
        while cursor.key_valid() {
            let (k, v) = cursor.key().clone();
            // TODO: pass key (and value?) by reference
            let w = cursor.weight();
            builder.push((k, v, w));
            cursor.step_key();
        }
        builder.done()
    }

    fn eval_owned(&mut self, i: CI) -> CO {
        // TODO: owned implementation.
        self.eval(&i)
    }
}

/// Operator that generates an indexed representation of a Z-set
/// using a function that maps each key in the input Z-set into an output
/// (key, value) pair.
///
/// The input of the operator is a Z-set where the value type is
/// a key/value pair.  The output is an indexed representation of
/// the Z-set.
///
/// # Type arguments
///
/// * `CI` - input batch type.
/// * `CO` - output batch type.
/// * `F` - function that maps each key in the input batch into an output (key,
///   value) pair.
pub struct IndexWith<CI, CO, F> {
    index_func: F,
    _type: PhantomData<(CI, CO)>,
}

impl<CI, CO, F> IndexWith<CI, CO, F> {
    pub fn new(index_func: F) -> Self {
        Self {
            index_func,
            _type: PhantomData,
        }
    }
}

impl<CI, CO, F> Operator for IndexWith<CI, CO, F>
where
    CI: 'static,
    CO: 'static,
    F: 'static,
{
    fn name(&self) -> Cow<'static, str> {
        Cow::from("IndexWith")
    }
    fn fixedpoint(&self, _scope: Scope) -> bool {
        true
    }
}

impl<CI, CO, F> UnaryOperator<CI, CO> for IndexWith<CI, CO, F>
where
    CO: Batch<Time = ()> + Clone + 'static,
    CI: BatchReader<Val = (), Time = (), R = CO::R> + 'static,
    F: Fn(&CI::Key) -> (CO::Key, CO::Val) + 'static,
    CO::Key: Clone,
    CO::Val: Clone,
{
    fn eval(&mut self, i: &CI) -> CO {
        let mut tuples = Vec::with_capacity(i.len());

        let mut cursor = i.cursor();
        while cursor.key_valid() {
            let (k, v) = (self.index_func)(cursor.key());
            // TODO: pass key (and value?) by reference
            let w = cursor.weight();
            tuples.push(((k, v), w));
            cursor.step_key();
        }

        CO::from_tuples((), tuples)
    }

    fn eval_owned(&mut self, i: CI) -> CO {
        // TODO: owned implementation.
        self.eval(&i)
    }
}
#[cfg(test)]
mod test {
    use crate::{
        circuit::Root, indexed_zset, operator::Generator, trace::ord::OrdIndexedZSet, zset,
    };

    #[test]
    fn index_test() {
        let root = Root::build(move |circuit| {
            let mut inputs = vec![
                zset!{ (1, "a") => 1
                     , (1, "b") => 1
                     , (2, "a") => 1
                     , (2, "c") => 1
                     , (1, "a") => 2
                     , (1, "b") => -1
                },
                zset!{ (1, "d") => 1
                     , (1, "e") => 1
                     , (2, "a") => -1
                     , (3, "a") => 2
                },
            ].into_iter();
            let mut outputs = vec![
                indexed_zset!{ 1 => {"a" => 3}, 2 => {"a" => 1, "c" => 1}},
                indexed_zset!{ 1 => {"a" => 3, "d" => 1, "e" => 1}, 2 => {"c" => 1}, 3 => {"a" => 2}},
            ].into_iter();
            circuit.add_source(Generator::new(move || inputs.next().unwrap() ))
                   .index()
                   .integrate()
                   .inspect(move |fm: &OrdIndexedZSet<_, _, _>| assert_eq!(fm, &outputs.next().unwrap()));
        })
        .unwrap();

        for _ in 0..2 {
            root.step().unwrap();
        }
    }

    #[test]
    fn index_with_test() {
        let root = Root::build(move |circuit| {
            let mut inputs = vec![
                zset!{ (1, "a") => 1
                     , (1, "b") => 1
                     , (2, "a") => 1
                     , (2, "c") => 1
                     , (1, "a") => 2
                     , (1, "b") => -1
                },
                zset!{ (1, "d") => 1
                     , (1, "e") => 1
                     , (2, "a") => -1
                     , (3, "a") => 2
                },
            ].into_iter();
            let mut outputs = vec![
                indexed_zset!{ 1 => {"a" => 3}, 2 => {"a" => 1, "c" => 1}},
                indexed_zset!{ 1 => {"a" => 3, "d" => 1, "e" => 1}, 2 => {"c" => 1}, 3 => {"a" => 2}},
            ].into_iter();
            circuit.add_source(Generator::new(move || inputs.next().unwrap() ))
                   .index_with(|(k, v)| (k.clone(), v.clone()))
                   .integrate()
                   .inspect(move |fm: &OrdIndexedZSet<_, _, _>| assert_eq!(fm, &outputs.next().unwrap()));
        })
        .unwrap();

        for _ in 0..2 {
            root.step().unwrap();
        }
    }
}
