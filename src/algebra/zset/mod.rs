#[macro_use]
mod zset_macro;

use crate::{
    algebra::{GroupValue, HasOne, HasZero, ZRingValue},
    trace::{cursor::Cursor, Batch, Builder},
    NumEntries,
};

/// An indexed Z-set maps arbitrary keys to Z-set values.
pub trait IndexedZSet: Batch<Time = ()> + GroupValue + NumEntries {
    /// Returns an indexed Z-set that contains all elements with positive
    /// weights from `self` with weights set to 1.
    fn distinct(&self) -> Self
    where
        Self::R: ZRingValue,
        Self::Key: Clone,
        Self::Val: Clone,
    {
        let mut builder = Self::Builder::with_capacity((), self.key_count());
        let mut cursor = self.cursor();

        while cursor.key_valid() {
            while cursor.val_valid() {
                let w = cursor.weight();
                if w.ge0() {
                    builder.push((
                        Self::item_from(cursor.key().clone(), cursor.val().clone()),
                        HasOne::one(),
                    ));
                }
                cursor.step_val();
            }
            cursor.step_key();
        }

        builder.done()
    }

    // TODO: optimized implementation for owned values
    /// Like `distinct` but optimized to operate on an owned value.
    fn distinct_owned(self) -> Self
    where
        Self::R: ZRingValue,
        Self::Key: Clone,
        Self::Val: Clone,
    {
        self.distinct()
    }
}

impl<Z> IndexedZSet for Z where Z: Batch<Time = ()> + GroupValue + NumEntries {}

/// The Z-set trait.
///
/// A Z-set is a set where each element has a weight.
/// Weights belong to some ring.
pub trait ZSet: IndexedZSet<Val = ()> {
    /// Count of elements, where each count is multiplied by its weight.
    /// Notice that the result could be 0 for non-empty sets.
    fn weighted_count(&self) -> Self::R;
}

impl<Z, K> ZSet for Z
where
    Z: IndexedZSet<Item = K, Key = K, Val = ()>,
{
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
