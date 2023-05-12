#[macro_use]
mod zset_macro;

#[cfg(doc)]
use crate::trace::{ord::OrdIndexedZSet, ord::OrdZSet, BatchReader};
use crate::{
    algebra::{GroupValue, HasOne, HasZero, ZRingValue},
    trace::{cursor::Cursor, Batch, Builder},
    NumEntries,
};

/// A set of weighted key-value pairs.
///
/// An indexed Z-set is a set of `(key, value)` pairs.  **Pairs must
/// be unique**, but keys need not be.
///
/// Each pair has a weight drawn from a ring, ordinarily the ring of
/// integers ℤ (hence the name "Z-set").  Weights are often
/// interpreted as the number of times that the pair appears in the
/// set, and for this reason **a weight of 0 is disallowed** and must
/// not appear in an indexed Z-set.  Negative weights are allowed,
/// however, because of an important secondary interpretation as an
/// "update" or "delta" to be added to some other Z-set: for this use,
/// a positive weight represents adding copies of a pair and a
/// negative weight represents removing them.
///
/// `IndexedZSet` has supertrait [`Batch`], which has supertrait
/// [`BatchReader`].  These supertraits have all of the interesting
/// related type definitions:
///
///  * The `Key` associated type, which is the type of `key`.
///
///  * The `Val` associated type, which is the type of `value`.
///
///  * The `R` associated type, which is the type of `weight`.  The client
///    specifies this type.  `i32` and `i64` are common choices.
///
/// The "index" in `IndexedZSet` refers to how it contains key-value
/// pairs: an `IndexedZSet` is often regarded as mapping from keys to
/// values.  For a simpler Z-set, without the index, use `()` for
/// `Val`, and indeed the [`ZSet`] trait simply contrains
/// `IndexedZSet` with `Val = ()`.
///
/// `IndexedZSet` has no requirements for implementors beyond its
/// supertraits, so it has no substantive implementations, only an
/// empty blanket implementation for all types that satisfy its
/// supertraits.  If DBSP client code needs to create its own
/// `IndexedZSet`s, the [`OrdIndexedZSet`] and [`OrdZSet`] types are
/// likely suitable.  But it is somewhat unusual for a client to need
/// to do this outside of test code, in which the [`indexed_zset!`],
/// [`zset!`], and [`zset_set!`] macros may be useful for creating
/// (indexed) Z-sets with specified elements.
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
/// A Z-set is a set of unique keys, each associated with a nonzero weight.
/// A `ZSet` is merely an `IndexedZSet` with its value type set to `()`.
pub trait ZSet: IndexedZSet<Val = ()> {
    /// Sum of the weights of the elements in the Z-set.  Weights can
    /// be negative, so the result can be zero even if the Z-set is
    /// nonempty.
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
