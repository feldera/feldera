//! Ordering an archived value against an unarchived one.
//!
//! Storage keeps batches in rkyv's archived form and searches them with keys
//! it holds in memory. Deserializing a probed key costs an allocation for
//! each heap-backed field, so a search would rather compare the archived
//! form directly with the unarchived key. [`OrdRepr`] is that comparison.
//!
//! It is a local trait rather than `PartialOrd<T>` because the orphan rules
//! stop this crate from implementing a foreign trait for a foreign type such
//! as `rkyv::ArchivedOption<U>`, and because a local trait can also cover
//! types that a foreign `PartialOrd` never could, such as `usize` (archived
//! as `u64`) or the standard tuples.

use ordered_float::OrderedFloat;
use rkyv::{
    ArchivePointee, FixedIsize, FixedUsize,
    boxed::ArchivedBox,
    collections::{ArchivedBTreeMap, btree_set::ArchivedBTreeSet},
    option::ArchivedOption,
    rc::ArchivedRc,
    string::ArchivedString,
    vec::ArchivedVec,
};
use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    marker::PhantomData,
    sync::Arc,
};

/// Compares an archived value with an unarchived value of type `T`.
///
/// # Contract
///
/// If `archived` is the rkyv archive of `original`, then for every `other`,
/// `archived.ord_cmp(&other)` equals `original.cmp(&other)`. Storage relies on
/// this to search files that were sorted by [`Ord`], so an implementation
/// that disagrees with `Ord` silently returns the wrong rows.
///
/// `#[derive(OrdRepr)]` ([`feldera_macros::OrdRepr`]) meets the contract for
/// any struct or enum whose `Ord` is derived: it compares fields in
/// declaration order, and enum variants by declaration position, exactly as
/// `#[derive(Ord)]` does. A type with a hand-written `Ord` must hand-write
/// `OrdRepr` to match it.
///
/// Every `DBData` type has this comparison available through
/// [`ArchivedDBData::Repr`](crate::dynamic::ArchivedDBData).
pub trait OrdRepr<T: ?Sized> {
    fn ord_cmp(&self, other: &T) -> Ordering;
}

/// Lexicographic comparison, as [`Ord`] for slices: element by element, and
/// the shorter sequence first when one is a prefix of the other.
fn cmp_sequences<'a, 'b, A, B, I, J>(lhs: I, rhs: J) -> Ordering
where
    A: OrdRepr<B> + 'a,
    B: 'b,
    I: IntoIterator<Item = &'a A>,
    J: IntoIterator<Item = &'b B>,
{
    let mut lhs = lhs.into_iter();
    let mut rhs = rhs.into_iter();
    loop {
        match (lhs.next(), rhs.next()) {
            (None, None) => return Ordering::Equal,
            (None, Some(_)) => return Ordering::Less,
            (Some(_), None) => return Ordering::Greater,
            (Some(a), Some(b)) => match a.ord_cmp(b) {
                Ordering::Equal => {}
                unequal => return unequal,
            },
        }
    }
}

// rkyv archives these types as themselves (the default byte order keeps a
// primitive's archived form equal to the primitive), so the comparison is
// `Ord` itself.
macro_rules! ord_repr_for_self {
    ($($t:ty),* $(,)?) => {$(
        impl OrdRepr<$t> for $t {
            #[inline]
            fn ord_cmp(&self, other: &$t) -> Ordering {
                self.cmp(other)
            }
        }
    )*};
}

ord_repr_for_self! {
    (), bool, char,
    i8, i16, i32, i64, i128,
    u8, u16, u32, u64, u128,
    uuid::Uuid,
}

impl OrdRepr<usize> for FixedUsize {
    #[inline]
    fn ord_cmp(&self, other: &usize) -> Ordering {
        self.cmp(&(*other as FixedUsize))
    }
}

impl OrdRepr<isize> for FixedIsize {
    #[inline]
    fn ord_cmp(&self, other: &isize) -> Ordering {
        self.cmp(&(*other as FixedIsize))
    }
}

impl<T: ordered_float::FloatCore> OrdRepr<OrderedFloat<T>> for OrderedFloat<T> {
    #[inline]
    fn ord_cmp(&self, other: &OrderedFloat<T>) -> Ordering {
        self.cmp(other)
    }
}

impl OrdRepr<String> for ArchivedString {
    #[inline]
    fn ord_cmp(&self, other: &String) -> Ordering {
        self.as_str().cmp(other.as_str())
    }
}

impl OrdRepr<str> for str {
    #[inline]
    fn ord_cmp(&self, other: &str) -> Ordering {
        self.cmp(other)
    }
}

impl<T: ?Sized> OrdRepr<PhantomData<T>> for PhantomData<T> {
    #[inline]
    fn ord_cmp(&self, _other: &PhantomData<T>) -> Ordering {
        Ordering::Equal
    }
}

impl<T, U> OrdRepr<Option<U>> for ArchivedOption<T>
where
    T: OrdRepr<U>,
{
    #[inline]
    fn ord_cmp(&self, other: &Option<U>) -> Ordering {
        match (self, other) {
            (ArchivedOption::None, None) => Ordering::Equal,
            (ArchivedOption::None, Some(_)) => Ordering::Less,
            (ArchivedOption::Some(_), None) => Ordering::Greater,
            (ArchivedOption::Some(a), Some(b)) => a.ord_cmp(b),
        }
    }
}

impl<T, U> OrdRepr<Box<U>> for ArchivedBox<T>
where
    T: OrdRepr<U> + ArchivePointee + ?Sized,
    U: ?Sized,
{
    #[inline]
    fn ord_cmp(&self, other: &Box<U>) -> Ordering {
        (**self).ord_cmp(other)
    }
}

impl<T, U, F> OrdRepr<Arc<U>> for ArchivedRc<T, F>
where
    T: OrdRepr<U> + ArchivePointee + ?Sized,
    U: ?Sized,
{
    #[inline]
    fn ord_cmp(&self, other: &Arc<U>) -> Ordering {
        (**self).ord_cmp(other)
    }
}

impl<T, U> OrdRepr<[U]> for [T]
where
    T: OrdRepr<U>,
{
    fn ord_cmp(&self, other: &[U]) -> Ordering {
        cmp_sequences(self, other)
    }
}

impl<T, U, const N: usize> OrdRepr<[U; N]> for [T; N]
where
    T: OrdRepr<U>,
{
    fn ord_cmp(&self, other: &[U; N]) -> Ordering {
        cmp_sequences(self, other)
    }
}

impl<T, U> OrdRepr<Vec<U>> for ArchivedVec<T>
where
    T: OrdRepr<U>,
{
    fn ord_cmp(&self, other: &Vec<U>) -> Ordering {
        cmp_sequences(self.as_slice(), other)
    }
}

impl<T, U> OrdRepr<BTreeSet<U>> for ArchivedBTreeSet<T>
where
    T: OrdRepr<U>,
{
    fn ord_cmp(&self, other: &BTreeSet<U>) -> Ordering {
        cmp_sequences(self, other)
    }
}

impl<AK, AV, K, V> OrdRepr<BTreeMap<K, V>> for ArchivedBTreeMap<AK, AV>
where
    AK: OrdRepr<K>,
    AV: OrdRepr<V>,
{
    fn ord_cmp(&self, other: &BTreeMap<K, V>) -> Ordering {
        // `BTreeMap`'s `Ord` is lexicographic over its `(key, value)` pairs.
        let mut lhs = self.iter();
        let mut rhs = other.iter();
        loop {
            match (lhs.next(), rhs.next()) {
                (None, None) => return Ordering::Equal,
                (None, Some(_)) => return Ordering::Less,
                (Some(_), None) => return Ordering::Greater,
                (Some((ak, av)), Some((k, v))) => match ak.ord_cmp(k) {
                    Ordering::Equal => match av.ord_cmp(v) {
                        Ordering::Equal => {}
                        unequal => return unequal,
                    },
                    unequal => return unequal,
                },
            }
        }
    }
}

// The standard tuples, whose archived form is the tuple of archived
// elements, up to the 12 elements that rkyv archives.
macro_rules! ord_repr_for_tuples {
    ($(($($index:tt: $original:ident / $archived:ident),+))+) => {$(
        impl<$($original, $archived),+> OrdRepr<($($original,)+)> for ($($archived,)+)
        where
            $($archived: OrdRepr<$original>,)+
        {
            #[inline]
            fn ord_cmp(&self, other: &($($original,)+)) -> Ordering {
                $(
                    match self.$index.ord_cmp(&other.$index) {
                        Ordering::Equal => {}
                        unequal => return unequal,
                    }
                )+
                Ordering::Equal
            }
        }
    )+};
}

ord_repr_for_tuples! {
    (0: T0/A0)
    (0: T0/A0, 1: T1/A1)
    (0: T0/A0, 1: T1/A1, 2: T2/A2)
    (0: T0/A0, 1: T1/A1, 2: T2/A2, 3: T3/A3)
    (0: T0/A0, 1: T1/A1, 2: T2/A2, 3: T3/A3, 4: T4/A4)
    (0: T0/A0, 1: T1/A1, 2: T2/A2, 3: T3/A3, 4: T4/A4, 5: T5/A5)
    (0: T0/A0, 1: T1/A1, 2: T2/A2, 3: T3/A3, 4: T4/A4, 5: T5/A5, 6: T6/A6)
    (0: T0/A0, 1: T1/A1, 2: T2/A2, 3: T3/A3, 4: T4/A4, 5: T5/A5, 6: T6/A6, 7: T7/A7)
    (0: T0/A0, 1: T1/A1, 2: T2/A2, 3: T3/A3, 4: T4/A4, 5: T5/A5, 6: T6/A6, 7: T7/A7,
     8: T8/A8)
    (0: T0/A0, 1: T1/A1, 2: T2/A2, 3: T3/A3, 4: T4/A4, 5: T5/A5, 6: T6/A6, 7: T7/A7,
     8: T8/A8, 9: T9/A9)
    (0: T0/A0, 1: T1/A1, 2: T2/A2, 3: T3/A3, 4: T4/A4, 5: T5/A5, 6: T6/A6, 7: T7/A7,
     8: T8/A8, 9: T9/A9, 10: T10/A10)
    (0: T0/A0, 1: T1/A1, 2: T2/A2, 3: T3/A3, 4: T4/A4, 5: T5/A5, 6: T6/A6, 7: T7/A7,
     8: T8/A8, 9: T9/A9, 10: T10/A10, 11: T11/A11)
}

#[cfg(test)]
mod tests {
    use super::OrdRepr;
    use crate::{
        algebra::F64,
        dynamic::{ArchivedDBData, BSet, DeserializeDyn, DeserializeImpl, DynData, Erase, LeanVec},
        operator::group::{CmpFunc, WithCustomOrd},
        storage::file::to_bytes,
        utils::{Tup0, Tup2, Tup10},
    };
    use feldera_macros::{IsNone, OrdRepr};
    use proptest::prelude::*;
    use rkyv::{Archive, Deserialize, Serialize, archived_root};
    use size_of::SizeOf;
    use std::{cmp::Ordering, collections::BTreeSet, fmt::Debug};

    /// Orders the archive of `lhs` against `rhs`, the way storage does.
    fn archived_cmp<T: ArchivedDBData>(lhs: &T, rhs: &T) -> Ordering {
        let bytes = to_bytes(lhs).unwrap();
        let archived = unsafe { archived_root::<T>(&bytes[..]) };
        archived.ord_cmp(rhs)
    }

    /// The contract of [`OrdRepr`]: the archive of a value orders against
    /// another value exactly as the value itself does, in both directions.
    fn assert_agrees_with_ord<T: ArchivedDBData + Ord + Debug>(lhs: &T, rhs: &T) {
        assert_eq!(archived_cmp(lhs, rhs), lhs.cmp(rhs), "{lhs:?} vs {rhs:?}");
        assert_eq!(archived_cmp(rhs, lhs), rhs.cmp(lhs), "{rhs:?} vs {lhs:?}");
        assert_eq!(archived_cmp(lhs, lhs), Ordering::Equal, "{lhs:?} vs itself");
    }

    // A struct exercising every shape the derive handles: named and tuple
    // fields, an enum with unit, tuple, and named variants, nesting, `Option`,
    // `Vec`, arrays, standard tuples, and the integer types that rkyv archives
    // as a different type.
    #[derive(
        Clone,
        Debug,
        Default,
        PartialEq,
        Eq,
        PartialOrd,
        Ord,
        Hash,
        SizeOf,
        Archive,
        Serialize,
        Deserialize,
        IsNone,
        OrdRepr,
    )]
    #[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
    struct Row {
        id: u32,
        name: Option<String>,
        tags: Vec<Option<i16>>,
        pair: (u8, Option<char>),
        sizes: Sizes,
        kind: Kind,
        flags: [Option<bool>; 3],
        score: F64,
    }

    #[derive(
        Clone,
        Debug,
        Default,
        PartialEq,
        Eq,
        PartialOrd,
        Ord,
        Hash,
        SizeOf,
        Archive,
        Serialize,
        Deserialize,
        IsNone,
        OrdRepr,
    )]
    #[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
    struct Sizes(usize, isize);

    #[derive(
        Clone,
        Debug,
        Default,
        PartialEq,
        Eq,
        PartialOrd,
        Ord,
        Hash,
        SizeOf,
        Archive,
        Serialize,
        Deserialize,
        IsNone,
        OrdRepr,
    )]
    #[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
    enum Kind {
        #[default]
        Unit,
        Tuple(i32, Option<u8>),
        Named {
            label: String,
            on: bool,
        },
    }

    fn kind() -> impl Strategy<Value = Kind> {
        prop_oneof![
            Just(Kind::Unit),
            (-3i32..3, any::<Option<u8>>()).prop_map(|(a, b)| Kind::Tuple(a, b)),
            ("[ab]{0,2}", any::<bool>()).prop_map(|(label, on)| Kind::Named { label, on }),
        ]
    }

    // Small domains so that generated pairs collide on prefixes often, which
    // is what exercises the tie-breaking order of later fields.
    fn row() -> impl Strategy<Value = Row> {
        (
            0u32..3,
            prop::option::of("[ab]{0,2}"),
            prop::collection::vec(prop::option::of(-2i16..2), 0..3),
            (0u8..2, prop::option::of(prop::char::range('a', 'b'))),
            (0usize..3, -2isize..2).prop_map(|(a, b)| Sizes(a, b)),
            kind(),
            prop::array::uniform3(any::<Option<bool>>()),
            prop_oneof![Just(-1.5), Just(0.0), Just(2.5), Just(f64::NAN)].prop_map(F64::new),
        )
            .prop_map(|(id, name, tags, pair, sizes, kind, flags, score)| Row {
                id,
                name,
                tags,
                pair,
                sizes,
                kind,
                flags,
                score,
            })
    }

    proptest! {
        #[test]
        fn derived_struct_agrees_with_ord(lhs in row(), rhs in row()) {
            assert_agrees_with_ord(&lhs, &rhs);
        }

        #[test]
        fn primitives_agree_with_ord(
            lhs in (any::<i64>(), any::<u128>(), any::<bool>(), any::<char>(), any::<usize>(), any::<isize>()),
            rhs in (any::<i64>(), any::<u128>(), any::<bool>(), any::<char>(), any::<usize>(), any::<isize>()),
        ) {
            assert_agrees_with_ord(&lhs, &rhs);
        }

        #[test]
        fn strings_and_options_agree_with_ord(
            lhs in (prop::option::of("[a-c]{0,3}"), "[a-c]{0,3}"),
            rhs in (prop::option::of("[a-c]{0,3}"), "[a-c]{0,3}"),
        ) {
            assert_agrees_with_ord(&lhs, &rhs);
        }

        #[test]
        fn collections_agree_with_ord(
            lhs in (prop::collection::vec(0u8..3, 0..4), prop::collection::btree_set(0u8..3, 0..4)),
            rhs in (prop::collection::vec(0u8..3, 0..4), prop::collection::btree_set(0u8..3, 0..4)),
        ) {
            let as_dbsp = |(vec, set): (Vec<u8>, BTreeSet<u8>)| {
                let mut bset = BSet::default();
                bset.extend(set);
                (LeanVec::from(vec), bset)
            };
            assert_agrees_with_ord(&as_dbsp(lhs), &as_dbsp(rhs));
        }

        // `Tup2` uses the legacy tuple layout and `Tup10` the v4 layout, which
        // stores a `None` field as a bitmap bit rather than as data.
        #[test]
        fn dbsp_tuples_agree_with_ord(
            lhs in (0u8..3, prop::option::of(0u8..3), prop::option::of("[ab]{0,2}")),
            rhs in (0u8..3, prop::option::of(0u8..3), prop::option::of("[ab]{0,2}")),
        ) {
            let wide = |(a, b, c): (u8, Option<u8>, Option<String>)| {
                Tup10::new(a, b, c.clone(), Tup0::new(), a, b, c, Some(a), None::<u8>, ())
            };
            assert_agrees_with_ord(&Tup2::new(lhs.0, lhs.1), &Tup2::new(rhs.0, rhs.1));
            assert_agrees_with_ord(&wide(lhs), &wide(rhs));
        }
    }

    /// Orders `u64`s in reverse, to tell a custom order from the natural one.
    #[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, SizeOf)]
    struct Reverse;

    impl CmpFunc<u64> for Reverse {
        fn cmp(left: &u64, right: &u64) -> Ordering {
            right.cmp(left)
        }
    }

    #[test]
    fn custom_order_follows_the_comparison_function() {
        let small = WithCustomOrd::<u64, Reverse>::new(1);
        let large = WithCustomOrd::<u64, Reverse>::new(2);
        assert_eq!(small.cmp(&large), Ordering::Greater);
        assert_agrees_with_ord(&small, &large);
    }

    #[test]
    fn none_orders_before_some_in_both_layouts() {
        let none = Tup2::new(None::<u8>, 0u8);
        let some = Tup2::new(Some(0u8), 0u8);
        assert_eq!(archived_cmp(&none, &some), Ordering::Less);
        assert_eq!(archived_cmp(&some, &none), Ordering::Greater);

        let none = Tup10::new(None::<u8>, (), (), (), (), (), (), (), (), ());
        let some = Tup10::new(Some(0u8), (), (), (), (), (), (), (), (), ());
        assert_eq!(archived_cmp(&none, &some), Ordering::Less);
        assert_eq!(archived_cmp(&some, &none), Ordering::Greater);
    }

    #[test]
    fn sequences_order_by_prefix_then_length() {
        let short = vec![1u8, 2];
        let long = vec![1u8, 2, 0];
        assert_agrees_with_ord(&short, &long);
        assert_agrees_with_ord(&BTreeSet::from([1u8, 2]), &BTreeSet::from([1u8, 3]));
        assert_agrees_with_ord(&[Some(1u8), None], &[Some(1u8), Some(0)]);
    }

    #[test]
    fn dyn_comparison_goes_through_ord_repr() {
        let lhs: Tup2<u32, Option<String>> = Tup2::new(1, Some("b".into()));
        let rhs: Tup2<u32, Option<String>> = Tup2::new(1, Some("a".into()));
        let bytes = to_bytes(&lhs).unwrap();
        let archived = unsafe { archived_root::<Tup2<u32, Option<String>>>(&bytes[..]) };
        let dyn_archived = DeserializeImpl::<Tup2<u32, Option<String>>, DynData>::new(archived);
        assert_eq!(dyn_archived.cmp_target(rhs.erase()), Ordering::Greater);
        assert!(!dyn_archived.eq_target(rhs.erase()));
        assert_eq!(dyn_archived.cmp_target(lhs.erase()), Ordering::Equal);
        assert!(dyn_archived.eq_target(lhs.erase()));
    }
}
