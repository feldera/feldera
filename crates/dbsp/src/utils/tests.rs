//! Test utilities
#![cfg(test)]

use crate::{
    trace::layers::{
        column_layer::{ColumnLayer, ColumnLayerBuilder},
        Builder, TupleBuilder,
    },
    utils::cast_uninit_vec,
};
use proptest::{collection::vec, prelude::*};
use rkyv::{with::Skip, Archive, Deserialize, Serialize};
use size_of::SizeOf;
use std::sync::{Arc, Mutex};
use std::{
    cmp::Ordering,
    hash::{Hash, Hasher},
    panic::{self, AssertUnwindSafe},
};

#[test]
fn cast_uninit_vecs() {
    let vec = vec![0u8, 1, 2, 3, 4];
    assert_eq!(vec.len(), 5);
    assert_eq!(vec.capacity(), 5);

    let uninit = cast_uninit_vec(vec);
    assert_eq!(uninit.len(), 5);
    assert_eq!(uninit.capacity(), 5);

    let empty = Vec::<u8>::with_capacity(4096);
    assert_eq!(empty.len(), 0);
    assert_eq!(empty.capacity(), 4096);

    let uninit = cast_uninit_vec(empty);
    assert_eq!(uninit.len(), 0);
    assert_eq!(uninit.capacity(), 4096);
}

#[derive(Debug)]
pub(crate) struct ArtificialPanic;

impl ArtificialPanic {
    pub(crate) fn catch<F>(closure: F)
    where
        F: FnOnce(),
    {
        match panic::catch_unwind(AssertUnwindSafe(closure)) {
            Ok(()) => {}
            // Catch any of our artificially induced panics
            Err(payload) if payload.is::<ArtificialPanic>() => {}
            // For any panic we didn't cause, resume its unwinding
            Err(payload) => panic::resume_unwind(payload),
        }
    }
}

#[derive(Debug, Clone, SizeOf, Archive, Serialize, Deserialize)]
pub(crate) struct RandomlyOrdered {
    #[size_of(skip)]
    #[with(Skip)]
    orderings: Arc<Mutex<Vec<Ordering>>>,
}

impl Hash for RandomlyOrdered {
    fn hash<H: Hasher>(&self, _state: &mut H) {
        todo!()
    }
}

impl RandomlyOrdered {
    pub(crate) fn new(orderings: Vec<Ordering>) -> Self {
        Self {
            orderings: Arc::new(Mutex::new(orderings)),
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
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RandomlyOrdered {
    fn cmp(&self, _other: &Self) -> Ordering {
        match self.orderings.lock() {
            Ok(mut orderings) => match orderings.pop() {
                Some(order) => order,
                None => panic::panic_any(ArtificialPanic),
            },
            Err(_) => panic::panic_any(ArtificialPanic),
        }
    }
}

#[derive(Debug, Clone, SizeOf, Archive, Serialize, Deserialize)]
pub(crate) struct LimitedDrops<T> {
    #[size_of(skip)]
    #[with(Skip)]
    inner: T,
    #[size_of(skip)]
    #[with(Skip)]
    allowed_drops: Option<Arc<Mutex<usize>>>,
}

impl<T> Hash for LimitedDrops<T> {
    fn hash<H: Hasher>(&self, _state: &mut H) {
        unimplemented!()
    }
}

impl<T> LimitedDrops<T> {
    pub(crate) fn new(inner: T, allowed_drops: Option<Arc<Mutex<usize>>>) -> Self {
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
        if let Some(allowed_drops) = self.allowed_drops.as_ref() {
            match allowed_drops.lock() {
                Ok(mut remaining_drops) => {
                    if *remaining_drops == 0 {
                        panic::panic_any(ArtificialPanic);
                    } else {
                        *remaining_drops -= 1;
                    }
                }
                Err(_) => {
                    // don't do anything here?
                }
            }
        }
    }
}

prop_compose! {
    /// Generate a random vec of orderings
    pub(crate) fn orderings(max_length: usize)
        (orderings in vec(any::<Ordering>(), 0..=max_length))
    -> Vec<Ordering> {
        orderings
    }
}

prop_compose! {
    /// Generate a `ColumnLayer`
    pub(crate) fn column_leaf(max_length: usize)
        (length in 0..=max_length)
        (length in length..=length, diffs in vec(any::<isize>(), length))
    -> ColumnLayer<usize, isize> {
        let mut builder = ColumnLayerBuilder::with_capacity(length);
        for (idx, diff) in diffs.into_iter().enumerate() {
            builder.push_tuple((idx, diff));
        }

        builder.done()
    }
}
