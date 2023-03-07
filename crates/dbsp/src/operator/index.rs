//! Operators to convert Z-sets into indexed Z-sets.

use crate::{
    circuit::{
        operator_traits::{Operator, UnaryOperator},
        Circuit, GlobalNodeId, OwnershipPreference, Scope, Stream,
    },
    circuit_cache_key,
    trace::{
        cursor::Cursor, ord::OrdIndexedZSet, Batch, BatchReader, Builder, Consumer, ValueConsumer,
    },
    DBData,
};
use std::{borrow::Cow, marker::PhantomData};

circuit_cache_key!(IndexId<C, D>(GlobalNodeId => Stream<C, D>));

impl<C, CI> Stream<C, CI>
where
    CI: Clone + 'static,
    C: Circuit,
{
    /// Convert input batches to an indexed representation.
    ///
    /// Converts input batches whose key type is a `(key,value)` tuple into an
    /// indexed Z-set using the first element of each tuple as a key and the
    /// second element as the value. The indexed Z-set representation is
    /// used as input to various join and aggregation operators.
    pub fn index<K, V>(&self) -> Stream<C, OrdIndexedZSet<K, V, CI::R>>
    where
        K: DBData,
        V: DBData,
        CI: BatchReader<Key = (K, V), Val = (), Time = ()>,
    {
        self.index_generic()
    }

    /// Like [`index`](`Self::index`), but can return any indexed Z-set type,
    /// not just `OrdIndexedZSet`.
    pub fn index_generic<CO>(&self) -> Stream<C, CO>
    where
        CI: BatchReader<Key = (CO::Key, CO::Val), Val = (), Time = (), R = CO::R>,
        CO: Batch<Time = ()>,
    {
        self.circuit()
            .cache_get_or_insert_with(IndexId::new(self.origin_node_id().clone()), || {
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
    pub fn index_with<K, V, F>(&self, index_func: F) -> Stream<C, OrdIndexedZSet<K, V, CI::R>>
    where
        CI: BatchReader<Time = (), Val = ()>,
        F: Fn(&CI::Key) -> (K, V) + Clone + 'static,
        K: DBData,
        V: DBData,
    {
        self.index_with_generic(index_func)
    }

    /// Like [`index_with`](`Self::index_with`), but can return any indexed
    /// Z-set type, not just `OrdIndexedZSet`.
    pub fn index_with_generic<CO, F>(&self, index_func: F) -> Stream<C, CO>
    where
        CI: BatchReader<Time = (), Val = ()>,
        CO: Batch<Time = (), R = CI::R>,
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
    CO: Batch<Time = ()>,
    CI: BatchReader<Key = (CO::Key, CO::Val), Val = (), Time = (), R = CO::R>,
{
    fn eval(&mut self, input: &CI) -> CO {
        let mut builder = <CO as Batch>::Builder::with_capacity((), input.len());

        let mut cursor = input.cursor();
        while cursor.key_valid() {
            let (k, v) = cursor.key().clone();
            // TODO: pass key (and value?) by reference
            let w = cursor.weight();
            builder.push((CO::item_from(k, v), w));
            cursor.step_key();
        }

        builder.done()
    }

    fn eval_owned(&mut self, input: CI) -> CO {
        let mut builder = <CO as Batch>::Builder::with_capacity((), input.len());

        let mut consumer = input.consumer();
        while consumer.key_valid() {
            let ((key, value), mut values) = consumer.next_key();

            debug_assert!(values.value_valid(), "found zst value with no weight");
            let ((), weight, ()) = values.next_value();

            builder.push((CO::item_from(key, value), weight));
        }

        builder.done()
    }

    fn input_preference(&self) -> OwnershipPreference {
        OwnershipPreference::PREFER_OWNED
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
    CO: Batch<Time = ()>,
    CI: BatchReader<Val = (), Time = (), R = CO::R>,
    F: Fn(&CI::Key) -> (CO::Key, CO::Val) + 'static,
{
    fn eval(&mut self, i: &CI) -> CO {
        let mut tuples = Vec::with_capacity(i.len());

        let mut cursor = i.cursor();
        while cursor.key_valid() {
            let (k, v) = (self.index_func)(cursor.key());
            // TODO: pass key (and value?) by reference
            let w = cursor.weight();
            tuples.push((CO::item_from(k, v), w));
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
        indexed_zset, operator::Generator, trace::ord::OrdIndexedZSet, zset, Circuit, RootCircuit,
    };

    #[test]
    fn index_test() {
        let circuit = RootCircuit::build(move |circuit| {
            let mut inputs = vec![
                zset!{ (1, 'a') => 1
                     , (1, 'b') => 1
                     , (2, 'a') => 1
                     , (2, 'c') => 1
                     , (1, 'a') => 2
                     , (1, 'b') => -1
                },
                zset!{ (1, 'd') => 1
                     , (1, 'e') => 1
                     , (2, 'a') => -1
                     , (3, 'a') => 2
                },
            ].into_iter();
            let mut outputs = vec![
                indexed_zset!{ 1 => {'a' => 3}, 2 => {'a' => 1, 'c' => 1}},
                indexed_zset!{ 1 => {'a' => 3, 'd' => 1, 'e' => 1}, 2 => {'c' => 1}, 3 => {'a' => 2}},
            ].into_iter();
            circuit.add_source(Generator::new(move || inputs.next().unwrap() ))
                   .index()
                   .integrate()
                   .inspect(move |fm: &OrdIndexedZSet<_, _, _>| assert_eq!(fm, &outputs.next().unwrap()));
        })
        .unwrap().0;

        for _ in 0..2 {
            circuit.step().unwrap();
        }
    }

    #[test]
    fn index_with_test() {
        let circuit = RootCircuit::build(move |circuit| {
            let mut inputs = vec![
                zset!{ (1, 'a') => 1
                     , (1, 'b') => 1
                     , (2, 'a') => 1
                     , (2, 'c') => 1
                     , (1, 'a') => 2
                     , (1, 'b') => -1
                },
                zset!{ (1, 'd') => 1
                     , (1, 'e') => 1
                     , (2, 'a') => -1
                     , (3, 'a') => 2
                },
            ].into_iter();
            let mut outputs = vec![
                indexed_zset!{ 1 => {'a' => 3}, 2 => {'a' => 1, 'c' => 1}},
                indexed_zset!{ 1 => {'a' => 3, 'd' => 1, 'e' => 1}, 2 => {'c' => 1}, 3 => {'a' => 2}},
            ].into_iter();
            circuit.add_source(Generator::new(move || inputs.next().unwrap() ))
                   .index_with(|&(k, v)| (k, v))
                   .integrate()
                   .inspect(move |fm: &OrdIndexedZSet<_, _, _>| assert_eq!(fm, &outputs.next().unwrap()));
        })
        .unwrap().0;

        for _ in 0..2 {
            circuit.step().unwrap();
        }
    }
}
