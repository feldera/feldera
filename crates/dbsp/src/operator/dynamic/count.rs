//! Count operators.

use crate::{
    algebra::{IndexedZSet, OrdIndexedZSet},
    circuit::{Circuit, Stream},
    dynamic::{ClonableTrait, DataTrait, Erase},
    operator::dynamic::{
        aggregate::{
            IncAggregateLinearFactories, StreamLinearAggregateFactories, WeightedCountOutFunc,
        },
        distinct::DistinctFactories,
    },
    trace::{BatchReaderFactories, Deserializable},
    DBData, Timestamp, ZWeight,
};

pub struct DistinctCountFactories<Z, O, T>
where
    Z: IndexedZSet,
    O: IndexedZSet<Key = Z::Key>,
    O::Val: DataTrait,
    T: Timestamp,
{
    distinct_factories: DistinctFactories<Z, T>,
    aggregate_factories: IncAggregateLinearFactories<Z, Z::R, O, T>,
}

impl<Z, O, T> DistinctCountFactories<Z, O, T>
where
    Z: IndexedZSet,
    O: IndexedZSet<Key = Z::Key>,
    T: Timestamp,
{
    pub fn new<KType, VType, OType>() -> Self
    where
        KType: DBData + Erase<Z::Key>,
        <KType as Deserializable>::ArchivedDeser: Ord,
        VType: DBData + Erase<Z::Val>,
        OType: DBData + Erase<O::Val>,
    {
        Self {
            distinct_factories: DistinctFactories::new::<KType, VType>(),
            aggregate_factories: IncAggregateLinearFactories::new::<KType, ZWeight, OType>(),
        }
    }
}

pub struct StreamDistinctCountFactories<Z, O>
where
    Z: IndexedZSet,
    O: IndexedZSet<Key = Z::Key>,
{
    input_factories: Z::Factories,
    aggregate_factories: StreamLinearAggregateFactories<Z, Z::R, O>,
}

impl<Z, O> StreamDistinctCountFactories<Z, O>
where
    Z: IndexedZSet,
    O: IndexedZSet<Key = Z::Key>,
{
    pub fn new<KType, VType, OType>() -> Self
    where
        KType: DBData + Erase<Z::Key>,
        <KType as Deserializable>::ArchivedDeser: Ord,
        VType: DBData + Erase<Z::Val>,
        OType: DBData + Erase<O::Val>,
    {
        Self {
            input_factories: BatchReaderFactories::new::<KType, VType, ZWeight>(),
            aggregate_factories: StreamLinearAggregateFactories::new::<KType, VType, ZWeight, OType>(
            ),
        }
    }
}

impl<C, Z> Stream<C, Z>
where
    C: Circuit,
    Z: IndexedZSet,
{
    /// See [`Stream::weighted_count`].
    #[allow(clippy::type_complexity)]
    pub fn dyn_weighted_count(
        &self,
        persistent_id: Option<&str>,
        factories: &IncAggregateLinearFactories<Z, Z::R, OrdIndexedZSet<Z::Key, Z::R>, C::Time>,
    ) -> Stream<C, OrdIndexedZSet<Z::Key, Z::R>> {
        self.dyn_weighted_count_generic(persistent_id, factories, Box::new(|w, out| w.move_to(out)))
    }

    /// Like [`Self::dyn_weighted_count`], but can return any batch type.
    pub fn dyn_weighted_count_generic<A, O>(
        &self,
        persistent_id: Option<&str>,
        factories: &IncAggregateLinearFactories<Z, Z::R, O, C::Time>,
        out_func: Box<dyn WeightedCountOutFunc<Z::R, A>>,
    ) -> Stream<C, O>
    where
        O: IndexedZSet<Key = Z::Key, Val = A>,
        A: DataTrait + ?Sized,
    {
        self.dyn_aggregate_linear_generic(
            persistent_id,
            factories,
            Box::new(|_k, _v, w, res| w.clone_to(res)),
            out_func,
        )
    }

    /// See [`Stream::distinct_count`].
    #[allow(clippy::type_complexity)]
    pub fn dyn_distinct_count(
        &self,
        persistent_id: Option<&str>,
        factories: &DistinctCountFactories<Z, OrdIndexedZSet<Z::Key, Z::R>, C::Time>,
    ) -> Stream<C, OrdIndexedZSet<Z::Key, Z::R>>
    where
        Z: Send,
    {
        self.dyn_distinct_count_generic(persistent_id, factories, Box::new(|w, out| w.move_to(out)))
    }

    /// Like [`Self::dyn_distinct_count`], but can return any batch type.
    pub fn dyn_distinct_count_generic<A, O>(
        &self,
        persistent_id: Option<&str>,
        factories: &DistinctCountFactories<Z, O, C::Time>,
        out_func: Box<dyn WeightedCountOutFunc<Z::R, A>>,
    ) -> Stream<C, O>
    where
        A: DataTrait + ?Sized,
        O: IndexedZSet<Key = Z::Key, Val = A>,
        Z: Send,
    {
        self.dyn_distinct(&factories.distinct_factories)
            .dyn_weighted_count_generic(persistent_id, &factories.aggregate_factories, out_func)
    }

    /// See [`Stream::stream_weighted_count`].
    #[allow(clippy::type_complexity)]
    pub fn dyn_stream_weighted_count(
        &self,
        factories: &StreamLinearAggregateFactories<Z, Z::R, OrdIndexedZSet<Z::Key, Z::R>>,
    ) -> Stream<C, OrdIndexedZSet<Z::Key, Z::R>> {
        self.dyn_stream_weighted_count_generic(factories, Box::new(|w, out| w.move_to(out)))
    }

    /// Like [`Self::dyn_stream_weighted_count`], but can return any batch type.
    pub fn dyn_stream_weighted_count_generic<A, O>(
        &self,
        factories: &StreamLinearAggregateFactories<Z, Z::R, O>,
        out_func: Box<dyn WeightedCountOutFunc<Z::R, A>>,
    ) -> Stream<C, O>
    where
        A: DataTrait + ?Sized,
        O: IndexedZSet<Key = Z::Key, Val = A>,
    {
        self.dyn_stream_aggregate_linear_generic(
            factories,
            Box::new(|_k, _v, w, res| w.clone_to(res)),
            out_func,
        )
    }

    /// See [`Stream::stream_distinct_count`].
    #[allow(clippy::type_complexity)]
    pub fn dyn_stream_distinct_count(
        &self,
        factories: &StreamDistinctCountFactories<Z, OrdIndexedZSet<Z::Key, Z::R>>,
    ) -> Stream<C, OrdIndexedZSet<Z::Key, Z::R>>
    where
        Z: Send,
    {
        self.dyn_stream_distinct_count_generic(factories, Box::new(|w, out| w.move_to(out)))
    }

    /// Like [`Self::dyn_distinct_count`], but can return any batch type.
    pub fn dyn_stream_distinct_count_generic<A, O>(
        &self,
        factories: &StreamDistinctCountFactories<Z, O>,
        out_func: Box<dyn WeightedCountOutFunc<Z::R, A>>,
    ) -> Stream<C, O>
    where
        A: DataTrait + ?Sized,
        O: IndexedZSet<Key = Z::Key, Val = A>,
        Z: Send,
    {
        self.dyn_stream_distinct(&factories.input_factories)
            .dyn_stream_weighted_count_generic(&factories.aggregate_factories, out_func)
    }
}

#[cfg(test)]
mod test {
    use crate::{
        indexed_zset,
        typed_batch::{OrdIndexedZSet, SpineSnapshot},
        utils::Tup2,
        Runtime,
    };
    use core::ops::Range;
    use rand::{rngs::StdRng, seq::SliceRandom, Rng, SeedableRng};

    #[test]
    fn weighted_count_test() {
        let (mut circuit, (input_handle, counts, stream_counts)) =
            Runtime::init_circuit(1, move |circuit| {
                let (inputs, input_handle) = circuit.add_input_zset::<i64>();

                let counts = inputs.weighted_count().accumulate_integrate();
                let stream_counts = circuit
                    .non_incremental(&inputs, |_child, inputs| {
                        Ok(inputs.integrate().stream_weighted_count())
                    })
                    .unwrap();

                Ok((
                    input_handle,
                    counts.accumulate_output(),
                    stream_counts.accumulate_output(),
                ))
            })
            .unwrap();

        // Generate expected values in `counts` by another means, using the formula for
        // A077925 (https://oeis.org/A077925).
        fn a077925(n: i64) -> i64 {
            let mut x = 2 << n;
            if (n & 1) == 0 {
                x = -x;
            }
            (1 - x) / 3
        }

        let mut next = 0;
        let mut term = 0;
        let mut ones_count = 0;

        for _ in 0..10 {
            // Generate sequence with key 1 and weights 1, -2, 4, -8, 16, -32, ...
            // Generate sequence with key 2 and delayed weights.
            input_handle.push(2, next);
            next = if next == 0 { 1 } else { next * (-2) };
            input_handle.push(1, next);

            circuit.transaction().unwrap();
            let counts = counts.concat().consolidate();
            let stream_counts = stream_counts.concat().consolidate();
            // println!("counts={}", counts);
            // println!("stream_counts={}", stream_counts);
            // println!("expected={}", expected_counts);

            term += 1;

            let twos_count = ones_count;
            ones_count = a077925(term - 1);

            let expected_counts = if twos_count == 0 {
                indexed_zset! { 1 => {ones_count => 1 } }
            } else {
                indexed_zset! { 1 => {ones_count => 1 }, 2 => {twos_count => 1} }
            };

            assert_eq!(counts, expected_counts);
            assert_eq!(stream_counts, expected_counts);
        }
    }

    #[test]
    fn distinct_count_test() {
        // Number of steps to test.
        const N: usize = 50;

        // Generate `input` as a vector of `N` Z-sets with keys in range `K`, values in
        // range `V`, and weights in range `W`, and `expected` as a vector that
        // for each element in `input` contains a Z-set that maps from each key
        // to the number of values with positive weight.
        const K: Range<u64> = 0..10; // Range of keys in Z-set.
        const V: Range<u64> = 0..10; // Range of values in Z-set.
        const W: Range<i64> = -10..10; // Range of weights in Z-set.
        let mut rng = StdRng::seed_from_u64(0); // Make the test reproducible.
        let mut input: Vec<Vec<Tup2<u64, Tup2<i64, i64>>>> = Vec::new();
        let mut expected: Vec<Vec<(u64, i64, i64)>> = Vec::new();
        for _ in 0..N {
            let mut input_tuples = Vec::new();
            let mut expected_tuples = Vec::new();
            for k in K {
                let mut v: Vec<u64> = V.collect();
                let n = rng.gen_range(V);
                v.partial_shuffle(&mut rng, n as usize);

                let mut distinct_count = 0;
                for &v in &v[0..n as usize] {
                    let w = rng.gen_range(W);
                    input_tuples.push(Tup2(k, Tup2(v as i64, w)));
                    if w > 0 {
                        distinct_count += 1;
                    }
                }
                if distinct_count > 0 {
                    expected_tuples.push((k, distinct_count, 1i64));
                }
            }
            input.push(input_tuples);
            expected.push(expected_tuples);
        }
        let input_copy = input.clone();

        let (mut circuit, (source_handle, counts, _stream_counts)) =
            Runtime::init_circuit(1, move |circuit| {
                let (source, source_handle) = circuit.add_input_indexed_zset::<u64, i64>();
                let counts = source
                    .accumulate_differentiate()
                    .distinct_count()
                    .accumulate_integrate();
                let stream_counts = source.stream_distinct_count();
                Ok((
                    source_handle,
                    counts.accumulate_output(),
                    stream_counts.accumulate_output(),
                ))
            })
            .unwrap();

        for (mut input, expected_counts) in input_copy.into_iter().zip(expected.into_iter()) {
            println!("step");
            source_handle.append(&mut input);
            circuit.transaction().unwrap();

            let counts = SpineSnapshot::<OrdIndexedZSet<u64, i64>>::concat(&counts.take_from_all())
                .iter()
                .collect::<Vec<_>>();

            // let stream_counts = stream_counts.consolidate();
            // println!("input={}", _input);
            // println!("counts={}", counts);
            // println!("stream_counts={}", stream_counts);
            // println!("expected={}", expected_counts);

            assert_eq!(counts, expected_counts.to_vec());

            // TODO
            //assert_eq!(stream_counts, expected_counts);
        }
    }
}
