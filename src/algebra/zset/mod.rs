#[macro_use]
mod zset_macro;

use crate::{
    algebra::{GroupValue, HasOne, HasZero, ZRingValue},
    trace::{cursor::Cursor, Batch, Builder},
    NumEntries, SharedRef,
};

// TODO: allow arbitrary `Time` types?
/// An indexed Z-set maps arbitrary keys to Z-set values.
pub trait IndexedZSet:
    Batch<Time = ()> + GroupValue + NumEntries + SharedRef<Target = Self>
{
}

impl<Z> IndexedZSet for Z where
    Z: Batch<Time = ()> + GroupValue + NumEntries + SharedRef<Target = Self>
{
}

/// The Z-set trait.
///
/// A Z-set is a set where each element has a weight.
/// Weights belong to some ring.
pub trait ZSet: IndexedZSet<Val = ()> {
    /// Returns a Z-set that contains all elements with positive weights from
    /// `self` with weights set to 1.
    fn distinct(&self) -> Self;

    /// Like `distinct` but optimized to operate on an owned value.
    fn distinct_owned(self) -> Self;

    /// Count of elements, where each count is multiplied by its weight.
    /// Notice that the result could be 0 for non-empty sets.
    fn weighted_count(&self) -> Self::R;
}

impl<Z> ZSet for Z
where
    Z: IndexedZSet<Val = ()>,
    Z::Key: Clone,
    Z::R: ZRingValue,
{
    fn distinct(&self) -> Self {
        let mut builder = Self::Builder::with_capacity((), self.len());
        let mut cursor = self.cursor();

        while cursor.key_valid() {
            let w = cursor.weight();
            if w.ge0() {
                builder.push((cursor.key().clone(), (), HasOne::one()));
            }
            cursor.step_key();
        }

        builder.done()
    }

    // TODO: optimized implementation for owned values
    fn distinct_owned(self) -> Self {
        self.distinct()
    }

    fn weighted_count(&self) -> Self::R {
        let mut sum = Self::R::zero();
        let mut cursor = self.cursor();
        while cursor.key_valid() {
            sum += cursor.weight();
            cursor.step_key();
        }
        sum
    }
}
