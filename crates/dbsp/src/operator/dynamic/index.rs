//! Operators to convert Z-sets into indexed Z-sets.

use crate::{
    circuit::{
        circuit_builder::StreamId,
        operator_traits::{Operator, UnaryOperator},
        Circuit, OwnershipPreference, Scope, Stream,
    },
    circuit_cache_key,
    dynamic::{ClonableTrait, DataTrait, DynPair, DynUnit},
    trace::{
        Batch, BatchFactories, BatchReader, BatchReaderFactories, Builder, Cursor, OrdIndexedWSet,
    },
};
use std::{borrow::Cow, marker::PhantomData, ops::DerefMut};

circuit_cache_key!(IndexId<C, D>(StreamId => Stream<C, D>));

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
    pub fn index<K, V>(
        &self,
        output_factories: &<OrdIndexedWSet<K, V, CI::R> as BatchReader>::Factories,
    ) -> Stream<C, OrdIndexedWSet<K, V, CI::R>>
    where
        K: DataTrait + ?Sized,
        V: DataTrait + ?Sized,
        CI: BatchReader<Key = DynPair<K, V>, Val = DynUnit, Time = ()>,
    {
        self.index_generic(output_factories)
    }

    /// Like [`index`](`Self::index`), but can return any indexed Z-set type,
    /// not just `OrdIndexedZSet`.
    pub fn index_generic<CO>(&self, output_factories: &CO::Factories) -> Stream<C, CO>
    where
        CI: BatchReader<Key = DynPair<CO::Key, CO::Val>, Val = DynUnit, Time = (), R = CO::R>,
        CO: Batch<Time = ()>,
    {
        self.circuit()
            .cache_get_or_insert_with(IndexId::new(self.stream_id()), || {
                self.circuit()
                    .add_unary_operator(Index::new(output_factories), self)
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
        output_factories: &<OrdIndexedWSet<K, V, CI::R> as BatchReader>::Factories,
        index_func: F,
    ) -> Stream<C, OrdIndexedWSet<K, V, CI::R>>
    where
        CI: BatchReader<Time = (), Val = DynUnit>,
        F: Fn(&CI::Key, &mut DynPair<K, V>) + Clone + 'static,
        K: DataTrait + ?Sized,
        V: DataTrait + ?Sized,
    {
        self.index_with_generic(index_func, output_factories)
    }

    /// Like [`index_with`](`Self::index_with`), but can return any indexed
    /// Z-set type, not just `OrdIndexedZSet`.
    pub fn index_with_generic<CO, F>(
        &self,
        index_func: F,
        output_factories: &CO::Factories,
    ) -> Stream<C, CO>
    where
        CI: BatchReader<Time = (), Val = DynUnit>,
        CO: Batch<Time = (), R = CI::R>,
        F: Fn(&CI::Key, &mut DynPair<CO::Key, CO::Val>) + Clone + 'static,
    {
        self.circuit()
            .add_unary_operator(IndexWith::new(index_func, output_factories), self)
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
pub struct Index<CI, CO: BatchReader> {
    factories: CO::Factories,
    _type: PhantomData<(CI, CO)>,
}

impl<CI, CO: BatchReader> Index<CI, CO> {
    pub fn new(factories: &CO::Factories) -> Self {
        Self {
            factories: factories.clone(),
            _type: PhantomData,
        }
    }
}

impl<CI, CO> Operator for Index<CI, CO>
where
    CI: 'static,
    CO: BatchReader + 'static,
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
    CI: BatchReader<Key = DynPair<CO::Key, CO::Val>, Val = DynUnit, Time = (), R = CO::R>,
{
    async fn eval(&mut self, input: &CI) -> CO {
        let mut builder =
            <CO as Batch>::Builder::with_capacity(&self.factories, input.len(), input.len());

        let mut cursor = input.cursor();
        let mut prev_key = self.factories.key_factory().default_box();
        let mut has_prev_key = false;
        while cursor.key_valid() {
            builder.push_diff(cursor.weight());
            let (k, v) = cursor.key().split();
            if has_prev_key {
                if k != &*prev_key {
                    builder.push_key_mut(&mut prev_key);
                    k.clone_to(&mut prev_key);
                }
            } else {
                k.clone_to(&mut prev_key);
                has_prev_key = true;
            }
            builder.push_val(v);

            cursor.step_key();
        }
        if has_prev_key {
            builder.push_key_mut(&mut prev_key);
        }

        builder.done()
    }

    // TODO: implement consumers
    /*fn eval_owned(&mut self, input: CI) -> CO {
        let mut builder = <CO as Batch>::Builder::with_capacity((), input.len());

        let mut consumer = input.consumer();
        while consumer.key_valid() {
            let (Tup2(key, value), mut values) = consumer.next_key();

            debug_assert!(values.value_valid(), "found zst value with no weight");
            let ((), weight, ()) = values.next_value();

            builder.push((CO::item_from(key, value), weight));
        }

        builder.done()
    }*/

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
pub struct IndexWith<CI, CO: BatchReader, F> {
    factories: CO::Factories,
    index_func: F,
    _type: PhantomData<(CI, CO)>,
}

impl<CI, CO: BatchReader, F> IndexWith<CI, CO, F> {
    pub fn new(index_func: F, factories: &CO::Factories) -> Self {
        Self {
            factories: factories.clone(),
            index_func,
            _type: PhantomData,
        }
    }
}

impl<CI, CO, F> Operator for IndexWith<CI, CO, F>
where
    CI: 'static,
    CO: BatchReader + 'static,
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
    CI: BatchReader<Val = DynUnit, Time = (), R = CO::R>,
    F: Fn(&CI::Key, &mut DynPair<CO::Key, CO::Val>) + 'static,
{
    async fn eval(&mut self, i: &CI) -> CO {
        let mut tuples = self.factories.weighted_items_factory().default_box();
        tuples.reserve(i.len());

        let mut item = self.factories.weighted_item_factory().default_box();

        let mut cursor = i.cursor();
        while cursor.key_valid() {
            let (kv, weight) = item.split_mut();
            (self.index_func)(cursor.key(), kv);
            cursor.weight().clone_to(weight);
            tuples.push_val(item.deref_mut());
            cursor.step_key();
        }

        CO::dyn_from_tuples(&self.factories, (), &mut tuples)
    }

    async fn eval_owned(&mut self, i: CI) -> CO {
        // TODO: owned implementation.
        self.eval(&i).await
    }
}

#[cfg(test)]
mod test {
    use crate::{
        dynamic::{ClonableTrait, DynData, DynPair, Erase, LeanVec},
        indexed_zset,
        operator::Generator,
        trace::{BatchReaderFactories, Batcher},
        typed_batch::{DynBatch, DynOrdZSet, OrdIndexedZSet},
        utils::Tup2,
        Circuit, RootCircuit, ZWeight,
    };

    #[test]
    fn index_test() {
        let circuit = RootCircuit::build(move |circuit| {
            let mut inputs = vec![
                vec![
                    (Tup2(1, 'a'), 1i64),
                    (Tup2(1, 'b'), 1),
                    (Tup2(2, 'a'), 1),
                    (Tup2(2, 'c'), 1),
                    (Tup2(1, 'a'), 2),
                    (Tup2(1, 'b'), -1),
                ],
                vec![
                    (Tup2(1, 'd'), 1),
                    (Tup2(1, 'e'), 1),
                    (Tup2(2, 'a'), -1),
                    (Tup2(3, 'a'), 2),
                ],
            ]
            .into_iter()
            .map(|tuples| {
                let tuples = tuples
                    .into_iter()
                    .map(|(k, v)| Tup2(Tup2(k, ()), v))
                    .collect::<Vec<_>>();
                let mut batcher =
                    <DynOrdZSet<DynPair<DynData, DynData>> as DynBatch>::Batcher::new_batcher(
                        &BatchReaderFactories::new::<Tup2<i32, char>, (), ZWeight>(),
                        (),
                    );
                batcher.push_batch(&mut Box::new(LeanVec::from(tuples)).erase_box());
                batcher.seal()
            });
            let mut outputs = vec![
                indexed_zset! { 1 => {'a' => 3}, 2 => {'a' => 1, 'c' => 1}},
                indexed_zset! { 1 => {'e' => 1, 'd' => 1}, 2 => {'a' => -1}, 3 => {'a' => 2}},
            ]
            .into_iter();
            circuit
                .add_source(Generator::new(move || inputs.next().unwrap()))
                .index(&BatchReaderFactories::new::<i32, char, ZWeight>())
                .typed()
                //.integrate()
                .inspect(move |fm: &OrdIndexedZSet<_, _>| assert_eq!(fm, &outputs.next().unwrap()));
            Ok(())
        })
        .unwrap()
        .0;

        for _ in 0..2 {
            circuit.transaction().unwrap();
        }
    }

    #[test]
    fn index_with_test() {
        let circuit = RootCircuit::build(move |circuit| {
            let mut inputs = vec![
                vec![
                    (Tup2(1, 'a'), 1),
                    (Tup2(1, 'b'), 1),
                    (Tup2(2, 'a'), 1),
                    (Tup2(2, 'c'), 1),
                    (Tup2(1, 'a'), 2),
                    (Tup2(1, 'b'), -1),
                ],
                vec![
                    (Tup2(1, 'd'), 1),
                    (Tup2(1, 'e'), 1),
                    (Tup2(2, 'a'), -1),
                    (Tup2(3, 'a'), 2),
                ],
            ]
            .into_iter()
            .map(|tuples| {
                let tuples = tuples
                    .into_iter()
                    .map(|(k, v)| Tup2(Tup2(k, ()), v))
                    .collect::<Vec<_>>();
                let mut batcher =
                    <DynOrdZSet<DynPair<DynData, DynData>> as DynBatch>::Batcher::new_batcher(
                        &BatchReaderFactories::new::<Tup2<i32, char>, (), ZWeight>(),
                        (),
                    );
                batcher.push_batch(&mut Box::new(LeanVec::from(tuples)).erase_box());
                batcher.seal()
            });

            let mut outputs = vec![
                indexed_zset! { 1 => {'a' => 3}, 2 => {'a' => 1, 'c' => 1}},
                indexed_zset! { 1 => {'e' => 1, 'd' => 1}, 2 => {'a' => -1}, 3 => {'a' => 2}},
            ]
            .into_iter();

            circuit
                .add_source(Generator::new(move || inputs.next().unwrap()))
                .index_with(
                    &BatchReaderFactories::new::<i32, char, ZWeight>(),
                    |kv: &DynPair<DynData /* <i32> */, DynData /* <char> */>, result| {
                        kv.clone_to(result)
                    },
                )
                .typed()
                .inspect(move |fm: &OrdIndexedZSet<_, _>| assert_eq!(fm, &outputs.next().unwrap()));
            Ok(())
        })
        .unwrap()
        .0;

        for _ in 0..2 {
            circuit.transaction().unwrap();
        }
    }
}
