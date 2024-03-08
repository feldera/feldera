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
use std::{
    cell::{Cell, RefCell},
    cmp::Ordering,
    panic::{self, AssertUnwindSafe},
    rc::Rc,
    thread,
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
    #[allow(unused)]
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

#[derive(Debug, Clone)]
pub(crate) struct RandomlyOrdered {
    orderings: Rc<RefCell<Vec<Ordering>>>,
}

impl RandomlyOrdered {
    pub(crate) fn new(orderings: Vec<Ordering>) -> Self {
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
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
#[archive(bound(archive = "T: Archive, <T as Archive>::Archived: Ord"))]
//#[archive(compare(PartialEq, PartialOrd))]
pub(crate) struct LimitedDrops<T> {
    inner: T,
    allowed_drops: Option<Rc<Cell<usize>>>,
}

impl<T> LimitedDrops<T> {
    pub(crate) fn new(inner: T, allowed_drops: Option<Rc<Cell<usize>>>) -> Self {
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
        if let Some(allowed_drops) = self.allowed_drops.as_ref().filter(|_| !thread::panicking()) {
            let remaining_drops = allowed_drops.get();
            if remaining_drops == 0 {
                panic::panic_any(ArtificialPanic);
            } else {
                allowed_drops.set(remaining_drops - 1);
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
        (length in length..=length, diffs in vec(any::<i64>(), length as usize))
    -> ColumnLayer<u64, i64> {
        let mut builder = ColumnLayerBuilder::with_capacity(length as usize);
        for (idx, diff) in diffs.into_iter().enumerate() {
            builder.push_tuple((idx as u64, diff));
        }

        builder.done()
    }
}
