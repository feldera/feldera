//! Z-sets and indexed Z-sets.
//!
//! An [`IndexedZSet`] is conceptually a set of `(key, value, weight)` tuples.
//! Indexed Z-sets have a specialization called a “non-indexed Z-set” ([`ZSet`])
//! that contains key and weight only.
//!
//! Indexed and non-indexed Z-sets are both subtraits of a higher-level
//! [`Batch`] trait.
//!
//! [`Stream`] values are often indexed or non-indexed Z-sets, which can
//! represent both data and deltas (see [Data streams versus delta streams]).
//!
//! [`Stream`]: crate::Stream
//! [`Batch`]: crate::Batch
//! [Data streams versus delta streams]: crate::Stream#data-streams-versus-delta-streams
mod zset_macro;

use crate::{
    dynamic::{DataTrait, DynUnit, DynWeightTyped, Erase, WeightTrait},
    trace::{
        ord::vec::{VecIndexedWSet, VecIndexedWSetFactories, VecWSet, VecWSetFactories},
        Batch, BatchReader, Builder, Cursor, OrdIndexedWSet, OrdIndexedWSetFactories, OrdWSet,
        OrdWSetFactories, Trace,
    },
};
use dyn_clone::clone_box;
use std::ops::Neg;

use super::{AddAssignByRef, AddByRef, HasOne, NegByRef, ZRingValue};

/// The default integer weight type.
///
/// Z-sets are built out of values with weights.  Weights are typically integers,
/// (hence "Z", for the ring of integers); however they can also be elements
/// of an arbitrary group.  To allow the latter, all batch types are parameterized
/// with the `R` type argument.  In practice, most Z-sets have integer weights.
/// We use `ZWeight` as the standard integer weight type.
pub type ZWeight = i64;

/// A dynamically typed wrapper around [`ZWeight`].
pub type DynZWeight = DynWeightTyped<ZWeight>;

/// A Z-set with integer weights.
pub type OrdZSet<K> = OrdWSet<K, DynZWeight>;

pub type OrdZSetFactories<K> = OrdWSetFactories<K, DynZWeight>;

/// A in-memory Z-set with integer weights.
pub type VecZSet<K> = VecWSet<K, DynZWeight>;

pub type VecZSetFactories<K> = VecWSetFactories<K, DynZWeight>;

/// An indexed Z-set with integer weights.
pub type OrdIndexedZSet<K, V> = OrdIndexedWSet<K, V, DynZWeight>;
pub type OrdIndexedZSetFactories<K, V> = OrdIndexedWSetFactories<K, V, DynZWeight>;

/// An in-memory indexed Z-set with integer weights.
pub type VecIndexedZSet<K, V> = VecIndexedWSet<K, V, DynZWeight>;
pub type VecIndexedZSetFactories<K, V> = VecIndexedWSetFactories<K, V, DynZWeight>;

// #[cfg(doc)]
//use crate::trace::{ord::OrdIndexedZSet, ord::OrdZSet, BatchReader, DBWeight,
// Trace};

/// Cursor over a batch with integer weights.
pub trait ZCursor<K: ?Sized, V: ?Sized, T>: Cursor<K, V, T, DynZWeight> {}

impl<K: ?Sized, V: ?Sized, T, C> ZCursor<K, V, T> for C where C: Cursor<K, V, T, DynZWeight> {}

/// [`BatchReader`] with integer weights.
pub trait ZBatchReader: BatchReader<R = DynZWeight> {}
impl<B> ZBatchReader for B where B: BatchReader<R = DynZWeight> {}

/// [`Batch`] with integer weights.
pub trait ZBatch: Batch<R = DynZWeight> {}
impl<B> ZBatch for B where B: Batch<R = DynZWeight> {}

/// [`Trace`] consisting of batches with integer weights.
pub trait ZTrace: Trace<R = DynZWeight> {}
impl<T> ZTrace for T where T: Trace<R = DynZWeight> {}

/// A set of weighted key-value pairs.
///
/// An indexed Z-set is a set of `(key, value)` pairs.  **Pairs must
/// be unique**, but keys need not be.
///
/// Each pair has a weight drawn from the ring of
/// integers ℤ (hence the name "Z-set").  Weights are often
/// interpreted as the number of times that the pair appears in the
/// set.  Negative weights are allowed, because of an important secondary
/// interpretation as an "update" or "delta" to be added to some other Z-set:
/// for this use, a positive weight represents adding copies of a pair and a
/// negative weight represents removing them.
///
/// A weight of zero should ideally not appear in a Z-set (or batch), although
/// in another sense every pair not explicitly present is implicitly present
/// with a zero weight.  However, a [`Trace`], which also implements
/// `IndexedZSet`, can have multiple entries for a pair that add up to zero.
/// Thus, code that processes an arbitrary `IndexedZSet` must not assume nonzero
/// weights.
///
/// `IndexedZSet` has supertrait [`Batch`], which has supertrait
/// [`BatchReader`].  These supertraits have all of the interesting
/// related type definitions:
///
///  * The `Key` associated type, which is the type of `key`.
///
///  * The `Val` associated type, which is the type of `value`.
///
/// The "index" in `IndexedZSet` refers to how it contains key-value
/// pairs: an `IndexedZSet` is often regarded as mapping from keys to
/// values.  For a simpler Z-set, without the index, use `()` for
/// `Val`, and indeed the [`ZSet`] trait simply constrains
/// `IndexedZSet` with `Val = ()`.
///
/// `IndexedZSet` has no requirements for implementors beyond its
/// supertraits, so it has no substantive implementations, only an
/// empty blanket implementation for all types that satisfy its
/// supertraits.  If DBSP client code needs to create its own
/// `IndexedZSet`s, the [`OrdIndexedZSet`] and [`OrdZSet`] types are
/// likely suitable.  But it is somewhat unusual for a client to need
/// to do this outside of test code, in which the
/// [`indexed_zset!`](`crate::indexed_zset!`),
/// [`zset!`](`crate::zset!`), and
/// [`zset_set!`](`crate::zset_set!`) macros may be useful for creating
/// (indexed) Z-sets with specified elements.
pub trait IndexedZSetReader: BatchReader<Time = (), R = DynZWeight> {}

impl<Z> IndexedZSetReader for Z where Z: BatchReader<Time = (), R = DynZWeight> {}

/// An indexed Z-set.
///
/// An [`IndexedZSet`] is conceptually a set of `(key, value, weight)` tuples.
/// Indexed Z-sets have a specialization called a “non-indexed Z-set” ([`ZSet`])
/// that contains key and weight only.
///
/// For more information, see [`IndexedZSetReader`].
pub trait IndexedZSet:
    Batch<Time = (), R = DynZWeight> + AddByRef + AddAssignByRef + Neg<Output = Self> + NegByRef + Eq
{
    /// Returns an indexed Z-set that contains all elements with positive
    /// weights from `self` with weights set to 1.
    fn distinct(&self) -> Self;

    // TODO: optimized implementation for owned values
    /// Like `distinct` but optimized to operate on an owned value.
    fn distinct_owned(self) -> Self {
        self.distinct()
    }

    // /// Returns an iterator over updates in the indexed Z-set.
    // fn iter(&self) -> IndexedZSetIterator<Self> {
    //     IndexedZSetIterator::new(self.cursor())
    // }

    fn iter(&self) -> IndexedZSetIterator<Self> {
        IndexedZSetIterator::new(self.cursor())
    }
}

impl<Z> IndexedZSet for Z
where
    Z: Batch<Time = (), R = DynZWeight>
        + AddByRef
        + AddAssignByRef
        + Neg<Output = Z>
        + NegByRef
        + Eq, /* + GroupValue + NumEntries */
{
    fn distinct(&self) -> Self {
        let factories = self.factories();
        let mut builder = Self::Builder::with_capacity(&factories, (), self.key_count());
        let mut cursor = self.cursor();

        while cursor.key_valid() {
            while cursor.val_valid() {
                let weight = cursor.weight();
                if weight.ge0() {
                    builder.push_refs(cursor.key(), cursor.val(), ZWeight::one().erase());
                }
                cursor.step_val();
            }
            cursor.step_key();
        }

        builder.done()
    }
}

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
    type Item = (Box<Z::Key>, Box<Z::Val>, ZWeight);

    fn next(&mut self) -> Option<Self::Item> {
        while self.cursor.key_valid() {
            if self.cursor.val_valid() {
                let weight = **self.cursor.weight();

                let retval = (
                    clone_box(self.cursor.key()),
                    clone_box(self.cursor.val()),
                    weight,
                );
                self.cursor.step_val();
                return Some(retval);
            }
            self.cursor.step_key();
        }
        None
    }
}

/// A Z-set reader.
///
/// This is just a specialization of [`IndexedZSetReader`] with [`DynUnit`]
/// (essentially `()`) as the value type.
pub trait ZSetReader: IndexedZSetReader<Val = DynUnit> {}
impl<Z> ZSetReader for Z where Z: IndexedZSetReader<Val = DynUnit> {}

/// A Z-set.
///
/// A Z-set is a set of unique keys, each associated with a weight.  A `ZSet` is
/// merely an [`IndexedZSet`] with its value type set to [`DynUnit`], which is
/// essentially `()`.
pub trait ZSet: IndexedZSet<Val = DynUnit> {
    /// Sum of the weights of the elements in the Z-set.  Weights can be
    /// negative, so the result can be zero even if the Z-set contains nonzero
    /// weights.
    fn weighted_count(&self, sum: &mut Self::R);
}

impl<Z, K> ZSet for Z
where
    Z: IndexedZSet<Key = K, Val = DynUnit>,
    K: DataTrait + ?Sized,
{
    fn weighted_count(&self, sum: &mut Self::R) {
        sum.set_zero();

        let mut cursor = self.cursor();
        while cursor.key_valid() {
            WeightTrait::add_assign(sum, cursor.weight());
            cursor.step_key();
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{typed_batch::OrdIndexedZSet, utils::Tup2, ZWeight};

    #[test]
    fn test_indexed_zset_iterator() {
        let tuples: Vec<Tup2<Tup2<i64, String>, ZWeight>> = vec![
            Tup2(Tup2(1, "a".to_string()), 1),
            Tup2(Tup2(1, "b".to_string()), 2),
            Tup2(Tup2(1, "c".to_string()), -1),
            Tup2(Tup2(2, "d".to_string()), 1),
        ]
        .into_iter()
        .collect();

        let indexed_zset = <OrdIndexedZSet<i64, String>>::from_tuples((), tuples.clone());

        assert_eq!(
            indexed_zset
                .iter()
                .map(|(k, v, w)| Tup2(Tup2(k, v), w))
                .collect::<Vec<_>>(),
            tuples
        );

        let indexed_zset = <OrdIndexedZSet<i64, String>>::from_tuples((), vec![]);

        assert_eq!(
            indexed_zset
                .iter()
                .map(|(k, v, w)| ((k, v), w))
                .collect::<Vec<_>>(),
            Vec::new()
        );
    }
}
