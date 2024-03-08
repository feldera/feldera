#![cfg(test)]

use crate::{
    dynamic::{
        DataTrait, DynData, DynPair, DynWeight, DynWeightedPairs, Erase, LeanVec, WeightTrait,
        WithFactory,
    },
    lean_vec,
    trace::{BatchReaderFactories, Batcher, OrdValBatch},
    utils::Tup2,
    DBData, DBWeight,
};
use std::{marker::PhantomData, mem::size_of};

use super::{MergeBatcher, MergeSorter};

macro_rules! pairs_vec {
    () => (
        Box::new(lean_vec!()).erase_box()
    );
    ($elem:expr; $n:expr) => (
        Box::new(lean_vec!($elem; $n)).erase_box()
    );
    ($($x:expr),+ $(,)?) => (
        Box::new(lean_vec![$($x),+]).erase_box()
    );
}

fn preallocated_stashes<K, V, KType, VType>(stashes: usize) -> Vec<Box<DynWeightedPairs<K, V>>>
where
    K: DataTrait + ?Sized,
    V: WeightTrait + ?Sized,
    KType: DBData + Erase<K>,
    VType: DBWeight + Erase<V>,
{
    (0..stashes)
        .map(|_| {
            let vec = LeanVec::<Tup2<KType, VType>>::with_capacity(
                MergeSorter::<K, V>::buffer_elements(size_of::<Tup2<KType, VType>>()),
            );
            Box::new(vec).erase_box()
        })
        .collect()
}

// Merging empty lists should produce an empty list
#[test]
fn merge_empty_inputs() {
    let mut merger: MergeSorter<DynData /* <u64> */, DynWeight /* <i64> */> = MergeSorter::new(
        WithFactory::<Tup2<u64, i64>>::FACTORY,
        WithFactory::<LeanVec<Tup2<u64, i64>>>::FACTORY,
    );
    let merged = merger.merge_by(Vec::new(), Vec::new());
    assert!(merged.is_empty());
}

#[test]
fn small_push() {
    let mut merger: MergeSorter<
        DynPair<DynData /* <u64> */, DynData /* <u64> */>,
        DynWeight, /* <i64> */
    > = MergeSorter::new(
        WithFactory::<Tup2<Tup2<u64, u64>, i64>>::FACTORY,
        WithFactory::<LeanVec<Tup2<Tup2<u64, u64>, i64>>>::FACTORY,
    );

    merger.queue = vec![vec![
        pairs_vec![Tup2(Tup2(0u64, 0u64), 1i64)],
        pairs_vec![Tup2(Tup2(45u64, 0u64), -1i64)],
    ]];

    let mut batch = pairs_vec![Tup2(Tup2(45u64, 1u64), 1i64)];
    merger.push_batch(&mut batch);

    let mut output = Vec::new();
    merger.finish_into(&mut output);

    assert_eq!(
        output,
        vec![
            pairs_vec![Tup2(Tup2(0u64, 0u64), 1i64), Tup2(Tup2(45, 0), -1)],
            pairs_vec![Tup2(Tup2(45u64, 1u64), 1i64)]
        ],
    );
}

#[test]
fn merge_by() {
    let mut merger: MergeSorter<DynData /* <u64> */, DynWeight /* <i64> */> = MergeSorter::new(
        WithFactory::<Tup2<u64, i64>>::FACTORY,
        WithFactory::<LeanVec<Tup2<u64, i64>>>::FACTORY,
    );

    let left = vec![pairs_vec![
        Tup2(0u64, 1i64),
        Tup2(1, 6),
        Tup2(24, 5),
        Tup2(54, -23)
    ]];
    let right = vec![pairs_vec![
        Tup2(0u64, 7i64),
        Tup2(1, 6),
        Tup2(24, -5),
        Tup2(25, 12),
        Tup2(89, 1)
    ]];
    let merged = merger.merge_by(left, right);

    // TODO: Not entirely sure if this is an optimal result, we always leave
    // one element trailing
    let expected = vec![
        pairs_vec![Tup2(0u64, 8i64), Tup2(1, 12), Tup2(25, 12), Tup2(54, -23)],
        pairs_vec![Tup2(89u64, 1i64)],
    ];
    assert_eq!(merged, expected);
}

#[test]
fn push_with_excess_stashes() {
    let mut merger: MergeSorter<DynData /* <u64> */, DynWeight /* <i64> */> = MergeSorter::new(
        WithFactory::<Tup2<u64, i64>>::FACTORY,
        WithFactory::<LeanVec<Tup2<u64, i64>>>::FACTORY,
    );
    merger.stash = preallocated_stashes::<_, _, u64, i64>(5);

    merger.push_batch(&mut pairs_vec![
        Tup2(0u64, 1i64),
        Tup2(1, 6),
        Tup2(24, 5),
        Tup2(54, -23)
    ]);

    let mut output = Vec::new();
    merger.finish_into(&mut output);

    assert_eq!(
        output,
        vec![pairs_vec![
            Tup2(0u64, 1i64),
            Tup2(1, 6),
            Tup2(24, 5),
            Tup2(54, -23)
        ]]
    );
}

#[test]
fn force_finish_merge() {
    let mut merger: MergeSorter<DynData /* <u64> */, DynWeight /* <i64> */> = MergeSorter::new(
        WithFactory::<Tup2<u64, i64>>::FACTORY,
        WithFactory::<LeanVec<Tup2<u64, i64>>>::FACTORY,
    );

    merger.queue = vec![
        vec![pairs_vec![
            Tup2(0u64, 1i64),
            Tup2(1, 6),
            Tup2(24, 5),
            Tup2(54, -23)
        ]],
        vec![pairs_vec![Tup2(89u64, 1i64)]],
        vec![pairs_vec![
            Tup2(0u64, 8i64),
            Tup2(1, 12),
            Tup2(25, 12),
            Tup2(54, -23)
        ]],
        vec![pairs_vec![Tup2(23u64, 54i64), Tup2(97, -102)]],
    ];
    merger.stash = preallocated_stashes::<_, _, u64, i64>(5);

    let mut output = Vec::new();
    merger.finish_into(&mut output);

    let expected = vec![
        pairs_vec![
            Tup2(0u64, 9i64),
            Tup2(1, 18),
            Tup2(23, 54),
            Tup2(24, 5),
            Tup2(25, 12),
            Tup2(54, -46)
        ],
        pairs_vec![Tup2(89u64, 1i64)],
        pairs_vec![Tup2(97u64, -102i64)],
    ];
    assert_eq!(output, expected);
}

#[test]
fn force_merge_on_push() {
    let mut merger: MergeSorter<DynData /* <u64> */, DynWeight /* <i64> */> = MergeSorter::new(
        WithFactory::<Tup2<u64, i64>>::FACTORY,
        WithFactory::<LeanVec<Tup2<u64, i64>>>::FACTORY,
    );

    merger.queue = vec![
        vec![pairs_vec![
            Tup2(0u64, 1i64),
            Tup2(1, 6),
            Tup2(24, 5),
            Tup2(54, -23)
        ]],
        vec![pairs_vec![Tup2(89u64, 1i64)]],
        vec![pairs_vec![
            Tup2(0u64, 8i64),
            Tup2(1, 12),
            Tup2(25, 12),
            Tup2(54, -23)
        ]],
    ];
    merger.stash = preallocated_stashes::<_, _, u64, i64>(5);

    merger.push_batch(&mut pairs_vec![Tup2(23u64, 54i64), Tup2(97, -102)]);

    let mut output = Vec::new();
    merger.finish_into(&mut output);

    let expected = vec![
        pairs_vec![
            Tup2(0u64, 9i64),
            Tup2(1, 18),
            Tup2(23, 54),
            Tup2(24, 5),
            Tup2(25, 12),
            Tup2(54, -46)
        ],
        pairs_vec![Tup2(89u64, 1i64)],
        pairs_vec![Tup2(97u64, -102i64)],
    ];
    assert_eq!(output, expected);
}

#[test]
fn count_tuples() {
    let empty: MergeSorter<DynData /* <u64> */, DynWeight /* <i64> */> = MergeSorter::new(
        WithFactory::<Tup2<u64, i64>>::FACTORY,
        WithFactory::<LeanVec<Tup2<u64, i64>>>::FACTORY,
    );

    assert_eq!(empty.len(), 0);

    let mut still_empty: MergeSorter<DynData /* <u64> */, DynWeight /* <i64> */> = MergeSorter::new(
        WithFactory::<Tup2<u64, i64>>::FACTORY,
        WithFactory::<LeanVec<Tup2<u64, i64>>>::FACTORY,
    );

    still_empty.stash = preallocated_stashes::<_, _, u64, i64>(100);

    assert_eq!(still_empty.len(), 0);

    let mut has_tuples: MergeSorter<
        DynPair<DynData /* <u64> */, DynData /* <u64> */>,
        DynWeight, /* <i64> */
    > = MergeSorter::new(
        WithFactory::<Tup2<Tup2<u64, u64>, i64>>::FACTORY,
        WithFactory::<LeanVec<Tup2<Tup2<u64, u64>, i64>>>::FACTORY,
    );

    has_tuples.queue = vec![
        vec![
            Box::new(LeanVec::<Tup2<Tup2<u64, u64>, i64>>::new()).erase_box(),
            pairs_vec![Tup2(Tup2(0u64, 0u64), 0i64); 1000],
            pairs_vec![Tup2(Tup2(1, 1), 1)],
        ],
        Vec::new(),
        vec![pairs_vec![Tup2(Tup2(10, 10), 10); 256]],
    ];
    has_tuples.stash = preallocated_stashes::<_, _, Tup2<u64, u64>, i64>(100);

    assert_eq!(has_tuples.len(), 1257);

    #[allow(clippy::type_complexity)]
    let batcher: MergeBatcher<
        OrdValBatch<
            DynData, /* <u64> */
            DynData, /* <u64> */
            u32,
            DynWeight, /* <i64> */
            u64,
        >,
    > = MergeBatcher {
        batch_factories: BatchReaderFactories::new::<u64, (), i64>(),
        sorter: has_tuples,
        time: 0,
        phantom: PhantomData,
    };
    assert_eq!(batcher.tuples(), 1257);
}

// These tests will utterly destroy miri's performance
#[cfg_attr(miri, ignore)]
mod proptests {
    use super::{preallocated_stashes, MergeSorter};
    use crate::{
        dynamic::{
            DowncastTrait, DynData, DynPair, DynWeight, DynWeightedPairs, Erase, LeanVec,
            WithFactory,
        },
        utils::{consolidate, Tup2},
    };
    use proptest::{collection::vec, prelude::*};
    use std::collections::BTreeMap;

    type AggregatedData = BTreeMap<Tup2<u64, u64>, i64>;

    prop_compose! {
        /// Create a batch data tuple
        fn tuple()(
            key in 0..1000u64,
            value in 0..1000u64,
            diff in -1000..=1000i64,
        ) -> Tup2<Tup2<u64, u64>, i64> {
            Tup2(Tup2(key, value), diff)
        }
    }

    prop_compose! {
        /// Generate a random batch of data
        fn batch()
            (length in 0..500usize)
            (batch in vec(tuple(), 0..=length))
        -> LeanVec<Tup2<Tup2<u64, u64>, i64>> {
            LeanVec::from(batch)
        }
    }

    /// Generate a random, consolidated batch of data
    fn consolidated_batch() -> impl Strategy<Value = LeanVec<Tup2<Tup2<u64, u64>, i64>>> {
        batch().prop_map(|mut batch| {
            consolidate(&mut batch);
            batch
        })
    }

    prop_compose! {
        /// Generate multiple random batches of data
        fn batches()
            (length in 0..100usize)
            (batches in vec(batch(), 0..=length))
        -> Vec<LeanVec<Tup2<Tup2<u64, u64>, i64>>> {
            batches
        }
    }

    prop_compose! {
        /// Generate multiple random consolidated batches of data
        fn consolidated_batches()
            (length in 0..150usize)
            (batches in vec(consolidated_batch(), 0..=length))
        -> Vec<Box<DynWeightedPairs<DynPair<DynData, DynData>, DynWeight>>> {
            batches.into_iter().map(|batch| Box::new(batch).erase_box()).collect()
        }
    }

    prop_compose! {
        // Create an initialized merge sorter with some stashes and
        //already queued data
        fn merge_sorter()
            (queue_len in 0..10usize)
            (
                stashes in 0..5usize,
                // Each batch within the merge sorter's queue must
                //already be consolidated
                queue in vec(consolidated_batches(), 0..=queue_len),
            )
        -> MergeSorter<DynPair<DynData, DynData>, DynWeight> {
            let mut merger: MergeSorter<DynPair<DynData, DynData>, DynWeight> =
                MergeSorter::new(WithFactory::<Tup2<Tup2<u64, u64>, i64>>::FACTORY, WithFactory::<LeanVec<Tup2<Tup2<u64, u64>, i64>>>::FACTORY);

            merger.queue = queue;
            merger.stash = preallocated_stashes::<_,_,Tup2<u64, u64>, i64>(stashes);
            merger
        }
    }

    fn empty_merge_sorter() -> MergeSorter<DynPair<DynData, DynData>, DynWeight> {
        let mut merger: MergeSorter<DynPair<DynData, DynData>, DynWeight> = MergeSorter::new(
            WithFactory::<Tup2<Tup2<u64, u64>, i64>>::FACTORY,
            WithFactory::<LeanVec<Tup2<Tup2<u64, u64>, i64>>>::FACTORY,
        );

        merger.queue = Vec::new();
        merger.stash = Vec::new();
        merger
    }

    fn expected_data(
        merger: &MergeSorter<DynPair<DynData, DynData>, DynWeight>,
        batch: &[Tup2<Tup2<u64, u64>, i64>],
    ) -> AggregatedData {
        for stash in &merger.stash {
            assert!(stash.is_empty());
        }

        // Collect all previously queued values
        let mut values = BTreeMap::new();
        for queued in &merger.queue {
            for batch in queued {
                for &Tup2(tuple, diff) in batch
                    .downcast_checked::<LeanVec<Tup2<Tup2<u64, u64>, i64>>>()
                    .as_slice()
                    .iter()
                {
                    values
                        .entry(tuple)
                        .and_modify(|acc| *acc += diff)
                        .or_insert(diff);
                }
            }
        }

        // Collect all tuples within the batch
        for &Tup2(tuple, diff) in batch {
            values
                .entry(tuple)
                .and_modify(|acc| *acc += diff)
                .or_insert(diff);
        }

        // Elements with a value of zero are removed in consolidation
        values.retain(|_, &mut diff| diff != 0);
        values
    }

    fn batches_data(batches: &[LeanVec<Tup2<Tup2<u64, u64>, i64>>]) -> AggregatedData {
        let mut values = BTreeMap::new();
        for batch in batches {
            for &Tup2(tuple, diff) in batch.as_slice() {
                values
                    .entry(tuple)
                    .and_modify(|acc| *acc += diff)
                    .or_insert(diff);
            }
        }

        // Elements with a value of zero are removed in consolidation
        values.retain(|_, &mut diff| diff != 0);
        values
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        #[ignore = "Currently broken, merge_sorter() doesn't ensure that the queue is sorted"]
        fn push_multiple_batches(mut merger in merge_sorter(), batch in batch()) {
            let input = expected_data(&merger, batch.as_slice());

            // Push the new batch
            merger.push_batch(&mut Box::new(batch).erase_box());

            let mut output = Vec::new();
            merger.finish_into(&mut output);

            // Ensure all output batches are sorted
            for batch in &output {
                prop_assert!(
                    batch.is_sorted_by(&|a, b| a.fst().cmp(b.fst())),
                    "unsorted batch: {batch:?}",
                );
            }

            let merged = batches_data(&output.iter().map(|batch| batch.downcast_checked::<LeanVec<Tup2<Tup2<u64, u64>, i64>>>().clone()).collect::<Vec<_>>());
            prop_assert_eq!(input, merged);
        }

        #[test]
        fn push_batches_into_empty(batches in batches()) {
            let input = batches_data(&batches);
            let mut merger = empty_merge_sorter();

            // Push all batches
            for batch in batches {
                merger.push_batch(&mut Box::new(batch).erase_box());
            }

            let mut output = Vec::new();
            merger.finish_into(&mut output);

            // Ensure all output batches are sorted
            for batch in &output {
                prop_assert!(batch.is_sorted_by(&|a, b| a.fst().cmp(b.fst())));
            }

            let mut result = None;
            for batch in &output {
                match &mut result {
                    None => result = Some(batch.clone()),
                    Some(b) => b.append(batch.clone().as_mut().as_vec_mut()),
                }
            }

            if let Some(result) = &result {
                prop_assert!(result.is_sorted_by(&|a, b| a.fst().cmp(b.fst())));
            }

            let merged = batches_data(&output.iter().map(|batch| batch.downcast_checked::<LeanVec<Tup2<Tup2<u64, u64>, i64>>>().clone()).collect::<Vec<_>>());
            prop_assert_eq!(input, merged);
        }
    }
}
