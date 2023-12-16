#![cfg(test)]

use crate::{
    algebra::{AddAssignByRef, HasZero},
    trace::{
        layers::{
            column_layer::{ColumnLayerBuilder, ColumnLayerConsumer},
            Builder, TupleBuilder,
        },
        Consumer, ValueConsumer,
    },
    DBWeight,
};
use rkyv::{with::Skip, Archive, Deserialize, Serialize};
use size_of::SizeOf;
use std::ops::Add;
use std::sync::{Arc, Mutex};
use std::{cmp::Ordering, hash::Hash, ops::AddAssign};

const TOTAL_TUPLES: usize = 100;
const EXPECTED_DROPS: usize = TOTAL_TUPLES * 2;

#[derive(Clone, Debug, Default)]
struct Canary {
    /// The total number of drops done within the entire system
    total: Arc<Mutex<usize>>,
}

impl Canary {
    fn new() -> Self {
        Self {
            total: Arc::new(Mutex::new(0)),
        }
    }
}

impl Drop for Canary {
    fn drop(&mut self) {
        let mut ttl = self.total.lock().unwrap();
        *ttl += 1;
    }
}

#[derive(Clone, Debug, Archive, Serialize, Deserialize, SizeOf)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
#[archive(bound(archive = "T: Archive, <T as Archive>::Archived: Ord"))]
#[archive(compare(PartialEq, PartialOrd))]
struct Item<T>
where
    T: DBWeight,
{
    value: T,
    #[size_of(skip)]
    #[with(Skip)]
    _canary: Canary,
}

impl PartialEq<Canary> for () {
    fn eq(&self, _other: &Canary) -> bool {
        todo!()
    }
}
impl PartialOrd<Canary> for () {
    fn partial_cmp(&self, _other: &Canary) -> Option<Ordering> {
        todo!()
    }
}

impl<T> Hash for Item<T>
where
    T: DBWeight,
{
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.value.hash(state);
    }
}

impl<T> Item<T>
where
    T: DBWeight,
{
    fn new(value: T, canary: Canary) -> Self {
        Self {
            _canary: canary,
            value,
        }
    }
}

impl<T> PartialEq for Item<T>
where
    T: DBWeight + PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl<T> Eq for Item<T> where T: DBWeight + Eq {}

impl<T> PartialOrd for Item<T>
where
    T: DBWeight + PartialOrd,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.value.cmp(&other.value))
    }
}

impl<T> Ord for Item<T>
where
    T: DBWeight + Ord,
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.value.cmp(&other.value)
    }
}

impl<T> AddAssign for Item<T>
where
    T: DBWeight + AddAssign,
{
    fn add_assign(&mut self, other: Self) {
        self.value.add_assign(other.value);
    }
}

impl<T> Add for Item<T>
where
    T: DBWeight + AddAssign,
{
    type Output = Self;

    fn add(mut self, other: Self) -> Self::Output {
        self += other;
        self
    }
}

impl<'a, T> Add for &'a Item<T>
where
    T: DBWeight + AddAssign,
{
    type Output = Item<T> where
        T: DBWeight + AddAssign;

    fn add(self, _other: &'a Item<T>) -> Item<T> {
        unimplemented!()
    }
}

impl<T> AddAssignByRef for Item<T>
where
    T: DBWeight + AddAssignByRef,
{
    fn add_assign_by_ref(&mut self, other: &Self) {
        self.value.add_assign_by_ref(&other.value);
    }
}

impl<T> HasZero for Item<T>
where
    T: DBWeight + HasZero,
{
    fn is_zero(&self) -> bool {
        self.value.is_zero()
    }

    fn zero() -> Self {
        Self {
            value: T::zero(),
            _canary: Canary::new(),
        }
    }
}

fn standard_consumer(canary: &Canary) -> ColumnLayerConsumer<Item<u64>, Item<i32>> {
    let mut batcher = ColumnLayerBuilder::new();
    for idx in 0..TOTAL_TUPLES {
        batcher.push_tuple((
            Item::new(idx as u64, canary.clone()),
            Item::new(1, canary.clone()),
        ));
    }

    ColumnLayerConsumer::from(batcher.done())
}

#[test]
fn no_double_drops_during_consumption() {
    let canary = Canary::new();
    {
        let mut consumer = standard_consumer(&canary);
        while consumer.key_valid() {
            let (_key, mut values) = consumer.next_key();
            while values.value_valid() {
                let ((), _diff, ()) = values.next_value();
            }
        }
    }

    assert_eq!(*canary.total.lock().unwrap(), EXPECTED_DROPS);
}

#[test]
fn no_double_drops_during_abandonment() {
    let canary = Canary::new();
    {
        let _consumer = standard_consumer(&canary);
    }

    assert_eq!(*canary.total.lock().unwrap(), EXPECTED_DROPS);
}

#[test]
fn no_double_drops_during_value_abandonment() {
    let canary = Canary::new();
    {
        let mut consumer = standard_consumer(&canary);
        while consumer.key_valid() {
            let (_key, _values) = consumer.next_key();
        }
    }

    assert_eq!(*canary.total.lock().unwrap(), EXPECTED_DROPS);
}

#[test]
fn no_double_drops_during_partial_abandonment() {
    let canary = Canary::new();
    {
        let mut consumer = standard_consumer(&canary);
        let mut counter = 0;
        while consumer.key_valid() && counter < TOTAL_TUPLES / 2 {
            let (_key, _values) = consumer.next_key();
            counter += 1;
        }
    }

    assert_eq!(*canary.total.lock().unwrap(), EXPECTED_DROPS);
}

/*
#[cfg_attr(miri, ignore)]
mod proptests {
    use crate::{
        trace::{
            layers::{
                column_layer::{ColumnLayerBuilder, ColumnLayerConsumer},
                Builder, TupleBuilder,
            },
            Consumer,
        },
        utils::tests::{orderings, ArtificialPanic, LimitedDrops, RandomlyOrdered},
    };
    use proptest::prelude::*;
    use std::sync::{Arc, Mutex};

    proptest! {
        #[test]
        fn seek_comparison_safety(orderings in orderings(100), leaf_length in 0..=100usize) {
            let orderings = RandomlyOrdered::new(orderings);

            // Build the source column leaf, we use the builder api so that no comparisons occur here
            let mut consumer = {
                let mut builder = ColumnLayerBuilder::with_capacity(leaf_length);
                for _ in 0..leaf_length {
                    builder.push_tuple((Box::new(orderings.clone()), 1));
                }

                ColumnLayerConsumer::from(builder.done())
            };

            // Seek repeatedly to incur a panic within the comparison function
            let needle = Box::new(orderings);
            for _ in 0..100 {
                ArtificialPanic::catch(|| consumer.seek_key(&needle));
            }
        }


        #[test]
        fn seek_panic_safety(needle in 0..101usize, leaf_length in 0..100usize, allowed_drops in 0..100usize) {
            let allowed_drops = Arc::new(Mutex::new(allowed_drops));

            // Build the source column leaf
            let mut consumer = {
                let mut builder = ColumnLayerBuilder::with_capacity(leaf_length);
                for key in 0..leaf_length {
                    builder.push_tuple((LimitedDrops::new(Box::new(key), Some(allowed_drops.clone())), 1));
                }

                ColumnLayerConsumer::from(builder.done())
            };

            // We seek for a random key which should drop some elements and then we drop the
            // consumer which should drop all remaining elements
            ArtificialPanic::catch(||  {
                consumer.seek_key(&LimitedDrops::new(Box::new(needle), None));
                drop(consumer);
            });
        }
    }
}
*/
