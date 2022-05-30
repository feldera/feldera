#[macro_use]
mod zset_macro;

use crate::{algebra::GroupValue, trace::Batch, NumEntries, SharedRef};

// TODO: allow arbitrary `Time` types?
/// An indexed Z-set maps arbitrary keys to Z-set values.
pub trait IndexedZSet:
    Batch<Time = ()> + GroupValue + NumEntries + SharedRef<Target = Self>
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
}
