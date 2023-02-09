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

    /// Returns an iterator over updates in the indexed Z-set.
    fn iter(&self) -> IndexedZSetIterator<Self> {
        IndexedZSetIterator::new(self.cursor())
    }
}

impl<Z> IndexedZSet for Z where Z: Batch<Time = ()> + GroupValue + NumEntries {}

/// Iterator over `(key, value, weight)` tuples of an indexed Z-set.
pub struct IndexedZSetIterator<'a, Z>
where
    Z: IndexedZSet,
{
    cursor: Z::Cursor<'a>,
}

impl<'a, Z> IndexedZSetIterator<'a, Z>
where
    Z: IndexedZSet,
{
    /// Returns an iterator of `(key, value, weight)` over the items that
    /// `cursor` visits.
    fn new(cursor: Z::Cursor<'a>) -> Self {
        Self { cursor }
    }
}

impl<'a, Z> Iterator for IndexedZSetIterator<'a, Z>
where
    Z: IndexedZSet,
{
    type Item = (Z::Key, Z::Val, Z::R);

    fn next(&mut self) -> Option<Self::Item> {
        while self.cursor.key_valid() {
            if self.cursor.val_valid() {
                let retval = (
                    self.cursor.key().clone(),
                    self.cursor.val().clone(),
                    self.cursor.weight(),
                );
                self.cursor.step_val();
                return Some(retval);
            }
            self.cursor.step_key();
        }
        None
    }
}

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

#[cfg(test)]
mod test {
    use crate::trace::Batch;
    use crate::{IndexedZSet, OrdIndexedZSet};

    #[test]
    fn test_indexed_zset_iterator() {
        let tuples: Vec<((usize, String), i32)> = vec![
            ((1, "a".to_string()), 1),
            ((1, "b".to_string()), 2),
            ((1, "c".to_string()), -1),
            ((2, "d".to_string()), 1),
        ];

        let indexed_zset = <OrdIndexedZSet<usize, String, i32>>::from_tuples((), tuples.clone());

        assert_eq!(
            indexed_zset
                .iter()
                .map(|(k, v, w)| ((k, v), w))
                .collect::<Vec<_>>(),
            tuples
        );

        let indexed_zset = <OrdIndexedZSet<usize, String, i32>>::from_tuples((), Vec::new());

        assert_eq!(
            indexed_zset
                .iter()
                .map(|(k, v, w)| ((k, v), w))
                .collect::<Vec<_>>(),
            Vec::new()
        );
    }
}
