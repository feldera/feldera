use crate::algebra::{OrdZSet, OrdZSetFactories};
use crate::dynamic::DynData;
use crate::trace::ord::vec::VecWSet;
use crate::trace::{test::zset_tuples, Batch, BatchReaderFactories, Trace};
use crate::utils::Tup2;
use crate::{DynZWeight, ZWeight};

use super::Spine;

type DynI32 = DynData;

/// We insert a small and a larger batch, and we make sure they are placed in the right `levels` bin.
#[test]
fn batch_ends_up_in_correct_bin() {
    let factories = <OrdZSetFactories<DynI32>>::new::<i32, (), ZWeight>();
    let mut trace = Spine::new(&factories);
    let small_batch = vec![Tup2(0, 1)];
    let large_batch = (0..100_000).map(|x| Tup2(x, x as i64)).collect();
    let batches: Vec<Vec<Tup2<i32, ZWeight>>> = vec![small_batch, large_batch];

    for tuples in batches.into_iter() {
        let erased_tuples = zset_tuples(tuples.clone());
        let batch = OrdZSet::<DynI32>::dyn_from_tuples(&factories, (), &mut erased_tuples.clone());
        trace.insert(batch);
    }

    assert_eq!(trace.levels[0].len(), 1);
    assert_eq!(trace.levels[1].len(), 1);
    assert_eq!(trace.levels.iter().map(|l| l.len()).sum::<usize>(), 2);
}

#[test]
fn merger_can_merge_stuff() {
    let factories = <OrdZSetFactories<DynI32>>::new::<i32, (), ZWeight>();
    let mut trace = Spine::new(&factories);

    let large_batch1: Vec<Tup2<i32, ZWeight>> = (1..=100_000).map(|x| Tup2(x, x as i64)).collect();
    let large_batch2: Vec<Tup2<i32, ZWeight>> = (1..=100_000).map(|x| Tup2(x, x as i64)).collect();
    let len_after_merge = large_batch2.len() + large_batch2.len();
    let batches: Vec<Vec<Tup2<i32, ZWeight>>> = vec![large_batch1, large_batch2];

    for tuples in batches.into_iter() {
        let erased_tuples = zset_tuples(tuples.clone());
        let batch = OrdZSet::<DynI32>::dyn_from_tuples(&factories, (), &mut erased_tuples.clone());
        trace.insert(batch);
    }

    let level = Spine::<VecWSet<DynI32, DynZWeight>>::size_to_level(len_after_merge);
    assert_eq!(trace.levels[level].len(), 1);
    assert_eq!(
        trace.levels[level].first_entry().unwrap().get().len(),
        len_after_merge
    );
}
