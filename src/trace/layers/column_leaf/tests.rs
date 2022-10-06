#![cfg(test)]

use crate::{
    algebra::{AddAssignByRef, HasZero},
    trace::{
        layers::{
            column_leaf::{ColumnLeafConsumer, UnorderedColumnLeafBuilder},
            Builder, TupleBuilder,
        },
        Consumer, ValueConsumer,
    },
};
use std::{cell::Cell, cmp::Ordering, ops::AddAssign, rc::Rc};

const TOTAL_TUPLES: usize = 100;
const EXPECTED_DROPS: usize = TOTAL_TUPLES * 2;

#[derive(Clone)]
struct Canary {
    /// The total number of drops done within the entire system
    total: Rc<Cell<usize>>,
}

impl Canary {
    fn new() -> Self {
        Self {
            total: Rc::new(Cell::new(0)),
        }
    }
}

impl Drop for Canary {
    fn drop(&mut self) {
        self.total.set(self.total.get() + 1);
    }
}

#[derive(Clone)]
struct Item<T> {
    value: T,
    _canary: Canary,
}

impl<T> Item<T> {
    fn new(value: T, canary: Canary) -> Self {
        Self {
            _canary: canary,
            value,
        }
    }
}

impl<T> PartialEq for Item<T>
where
    T: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl<T> Eq for Item<T> where T: Eq {}

impl<T> PartialOrd for Item<T>
where
    T: PartialOrd,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.value.partial_cmp(&other.value)
    }
}

impl<T> Ord for Item<T>
where
    T: Ord,
{
    fn cmp(&self, other: &Self) -> Ordering {
        self.value.cmp(&other.value)
    }
}

impl<T> AddAssign for Item<T>
where
    T: AddAssign,
{
    fn add_assign(&mut self, other: Self) {
        self.value.add_assign(other.value);
    }
}

impl<T> AddAssignByRef for Item<T>
where
    T: AddAssignByRef,
{
    fn add_assign_by_ref(&mut self, other: &Self) {
        self.value.add_assign_by_ref(&other.value);
    }
}

impl<T> HasZero for Item<T>
where
    T: HasZero,
{
    fn zero() -> Self {
        Self {
            value: T::zero(),
            _canary: Canary::new(),
        }
    }

    fn is_zero(&self) -> bool {
        self.value.is_zero()
    }
}

fn standard_consumer(canary: &Canary) -> ColumnLeafConsumer<Item<usize>, Item<i32>> {
    let mut batcher = UnorderedColumnLeafBuilder::new();
    for idx in 0..TOTAL_TUPLES {
        batcher.push_tuple((Item::new(idx, canary.clone()), Item::new(1, canary.clone())));
    }

    ColumnLeafConsumer::from(batcher.done())
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

    assert_eq!(canary.total.get(), EXPECTED_DROPS);
}

#[test]
fn no_double_drops_during_abandonment() {
    let canary = Canary::new();
    {
        let _consumer = standard_consumer(&canary);
    }

    assert_eq!(canary.total.get(), EXPECTED_DROPS);
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

    assert_eq!(canary.total.get(), EXPECTED_DROPS);
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

    assert_eq!(canary.total.get(), EXPECTED_DROPS);
}

#[cfg_attr(miri, ignore)]
mod proptests {
    use crate::trace::{
        layers::{
            column_leaf::{ColumnLeafConsumer, OrderedColumnLeaf, OrderedColumnLeafBuilder},
            Builder, TupleBuilder,
        },
        Consumer,
    };
    use proptest::{collection::vec, prelude::*};
    use std::{
        cell::{Cell, RefCell},
        cmp::Ordering,
        panic::{self, AssertUnwindSafe},
        rc::Rc,
        thread,
    };

    prop_compose! {
        /// Generate a random vec of orderings
        fn orderings(max_length: usize)
            (orderings in vec(any::<Ordering>(), 0..=max_length))
        -> Vec<Ordering> {
            orderings
        }
    }

    prop_compose! {
        /// Generate a OrderedColumnLeaf
        fn column_leaf(max_length: usize)
            (length in 0..=max_length)
            (length in length..=length, diffs in vec(any::<isize>(), length))
        -> OrderedColumnLeaf<usize, isize> {
            let mut builder = OrderedColumnLeafBuilder::with_capacity(length);
            for (idx, diff) in diffs.into_iter().enumerate() {
                builder.push_tuple((idx, diff));
            }

            builder.done()
        }
    }

    #[derive(Debug)]
    struct ArtificialPanic;

    #[derive(Debug, Clone)]
    struct RandomlyOrdered {
        orderings: Rc<RefCell<Vec<Ordering>>>,
    }

    impl RandomlyOrdered {
        pub fn new(orderings: Vec<Ordering>) -> Self {
            Self {
                orderings: Rc::new(RefCell::new(orderings)),
            }
        }
    }

    impl PartialEq for RandomlyOrdered {
        fn eq(&self, _other: &Self) -> bool {
            false
        }
    }

    impl Eq for RandomlyOrdered {}

    impl PartialOrd for RandomlyOrdered {
        fn partial_cmp(&self, _other: &Self) -> Option<Ordering> {
            match self.orderings.borrow_mut().pop() {
                order @ Some(_) => order,
                None => panic::panic_any(ArtificialPanic),
            }
        }
    }

    impl Ord for RandomlyOrdered {
        fn cmp(&self, _other: &Self) -> Ordering {
            match self.orderings.borrow_mut().pop() {
                Some(order) => order,
                None => panic::panic_any(ArtificialPanic),
            }
        }
    }

    #[derive(Clone)]
    struct LimitedDrops<T> {
        inner: T,
        allowed_drops: Option<Rc<Cell<usize>>>,
    }

    impl<T> LimitedDrops<T> {
        fn new(inner: T, allowed_drops: Option<Rc<Cell<usize>>>) -> Self {
            Self {
                inner,
                allowed_drops,
            }
        }
    }

    impl<T> PartialEq for LimitedDrops<T>
    where
        T: PartialEq,
    {
        fn eq(&self, other: &Self) -> bool {
            self.inner.eq(&other.inner)
        }
    }

    impl<T: Eq> Eq for LimitedDrops<T> {}

    impl<T> PartialOrd for LimitedDrops<T>
    where
        T: PartialOrd,
    {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            self.inner.partial_cmp(&other.inner)
        }
    }

    impl<T> Ord for LimitedDrops<T>
    where
        T: Ord,
    {
        fn cmp(&self, other: &Self) -> Ordering {
            self.inner.cmp(&other.inner)
        }
    }

    impl<T> Drop for LimitedDrops<T> {
        fn drop(&mut self) {
            if let Some(allowed_drops) =
                self.allowed_drops.as_ref().filter(|_| !thread::panicking())
            {
                let remaining_drops = allowed_drops.get();
                if remaining_drops == 0 {
                    panic::panic_any(ArtificialPanic);
                } else {
                    allowed_drops.set(remaining_drops - 1);
                }
            }
        }
    }

    proptest! {
        #[test]
        fn seek_comparison_safety(orderings in orderings(100), leaf_length in 0..=100usize) {
            let orderings = RandomlyOrdered::new(orderings);

            // Build the source column leaf, we use the builder api so that no comparisons occur here
            let mut consumer = {
                let mut builder = OrderedColumnLeafBuilder::with_capacity(leaf_length);
                for _ in 0..leaf_length {
                    builder.push_tuple((Box::new(orderings.clone()), 1));
                }

                ColumnLeafConsumer::from(builder.done())
            };

            // Seek repeatedly to incur a panic within the comparison function
            let needle = Box::new(orderings);
            for _ in 0..100 {
                let result = panic::catch_unwind(AssertUnwindSafe(|| consumer.seek_key(&needle)));

                // Catch any of our artificially induced panics
                if let Err(payload) = result {
                    if !payload.is::<ArtificialPanic>() {
                        panic::resume_unwind(payload);
                    }
                }
            }
        }


        #[test]
        fn seek_panic_safety(needle in 0..101usize, leaf_length in 0..100usize, allowed_drops in 0..100usize) {
            let allowed_drops = Rc::new(Cell::new(allowed_drops));

            // Build the source column leaf
            let mut consumer = {
                let mut builder = OrderedColumnLeafBuilder::with_capacity(leaf_length);
                for key in 0..leaf_length {
                    builder.push_tuple((LimitedDrops::new(Box::new(key), Some(allowed_drops.clone())), 1));
                }

                ColumnLeafConsumer::from(builder.done())
            };

            // We seek for a random key which should drop some elements and then we drop the
            // consumer which should drop all remaining elements
            let result = panic::catch_unwind(AssertUnwindSafe(|| {
                consumer.seek_key(&LimitedDrops::new(Box::new(needle), None));
                drop(consumer);
            }));

            // Catch any of our artificially induced panics
            if let Err(payload) = result {
                if !payload.is::<ArtificialPanic>() {
                    panic::resume_unwind(payload);
                }
            }
        }
    }
}
