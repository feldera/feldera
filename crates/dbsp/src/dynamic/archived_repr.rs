//! What storage needs of an archived type to work on it as bytes.
//!
//! Storage keeps batches in rkyv's archived form and, wherever it can, works
//! on the archived bytes instead of decoding them: it searches them with keys
//! it holds decoded ([`OrdRepr`]), feeds them to membership filters by their
//! hash ([`HashRepr`]), and copies them from one file into another when it
//! merges batches.  [`ArchivedRepr`] is all of that in one trait, and
//! `#[derive(ArchivedRepr)]` implements it, together with the other two, for
//! a type whose archived form comes from `#[derive(Archive)]`.

use crate::dynamic::{HashRepr, OrdRepr};
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
    collections::{BTreeMap, BTreeSet},
    mem::align_of,
    sync::Arc,
};

/// The strictest alignment `rkyv` gives a primitive: sixteen bytes, for an
/// `i128` or a `u128`.
///
/// [`ArchivedRepr::MAX_ALIGN`] defaults to this, which is safe for any type
/// whose serializer aligns nothing more strictly than a primitive needs, and
/// is more than most types need.
pub const MAX_PRIMITIVE_ALIGN: usize = 16;

/// The larger of two alignments, for computing [`ArchivedRepr::MAX_ALIGN`] in
/// a constant expression.
///
/// # Arguments
///
/// * `a`, `b` - the two alignments.
///
/// # Returns
///
/// Whichever is larger.
pub const fn max_align(a: usize, b: usize) -> usize {
    if a > b { a } else { b }
}

/// The archived form of `T`, as storage uses it without decoding it.
///
/// It orders against `T` ([`OrdRepr`]), hashes as `T` hashes ([`HashRepr`]),
/// and says how strictly a copy of its bytes has to stay aligned
/// ([`MAX_ALIGN`](Self::MAX_ALIGN)).  Like the other two, it is implemented
/// for the archived type, with the decoded type as the parameter.
///
/// `#[derive(ArchivedRepr)]` ([`feldera_macros::ArchivedRepr`]) implements
/// all three for a struct or enum and works the alignment out from the
/// fields; `declare_tuple!` does the same for the tuples it declares.  A
/// type that implements `OrdRepr` or `HashRepr` by hand implements this by
/// hand too.
pub trait ArchivedRepr<T: ?Sized>: OrdRepr<T> + HashRepr {
    /// The strictest alignment of anything an archived value writes: its
    /// root, and everything it keeps out of line, at any depth.
    ///
    /// A merge copies an archived value's bytes from one block into another
    /// unchanged.  Its relative pointers survive any shift, but each object
    /// they point at stays aligned only if the shift is a multiple of that
    /// object's alignment, so the copy keeps the value's position in its
    /// block modulo this constant; alignments are powers of two, so that
    /// keeps every one of them.  A value that keeps nothing out of line needs
    /// only its root's alignment.
    ///
    /// Too large a value costs padding.  Too small a value is unsound: a copy
    /// can land an object misaligned, and reading it is undefined behavior.
    /// Debug builds check every value the storage layer encodes against this
    /// constant.  The default, [`MAX_PRIMITIVE_ALIGN`], is safe for any type
    /// that never asks its serializer for more alignment than a primitive
    /// needs.
    const MAX_ALIGN: usize = MAX_PRIMITIVE_ALIGN;
}

// Types `rkyv` archives as themselves, which keep nothing out of line.
macro_rules! archived_repr_for_self {
    ($($t:ty),* $(,)?) => {$(
        impl ArchivedRepr<$t> for $t {
            const MAX_ALIGN: usize = align_of::<$t>();
        }
    )*};
}

archived_repr_for_self! {
    (), bool, char,
    i8, i16, i32, i64, i128,
    u8, u16, u32, u64, u128,
    uuid::Uuid,
}

impl ArchivedRepr<usize> for FixedUsize {
    const MAX_ALIGN: usize = align_of::<FixedUsize>();
}

impl ArchivedRepr<isize> for FixedIsize {
    const MAX_ALIGN: usize = align_of::<FixedIsize>();
}

// The text lies out of line as bytes, which need no alignment, so the root,
// which holds its length and where it is, is the strictest.
impl ArchivedRepr<String> for ArchivedString {
    const MAX_ALIGN: usize = align_of::<ArchivedString>();
}

impl<T, U> ArchivedRepr<Option<U>> for ArchivedOption<T>
where
    T: ArchivedRepr<U>,
{
    const MAX_ALIGN: usize = max_align(align_of::<Self>(), T::MAX_ALIGN);
}

// What a box or a shared pointer points at lies out of line, aligned as
// itself.
impl<T, U> ArchivedRepr<Box<U>> for ArchivedBox<T>
where
    T: ArchivedRepr<U> + ArchivePointee + ?Sized,
    U: ?Sized,
{
    const MAX_ALIGN: usize = max_align(align_of::<Self>(), T::MAX_ALIGN);
}

impl<T, U, F> ArchivedRepr<Arc<U>> for ArchivedRc<T, F>
where
    T: ArchivedRepr<U> + ArchivePointee + ?Sized,
    U: ?Sized,
{
    const MAX_ALIGN: usize = max_align(align_of::<Self>(), T::MAX_ALIGN);
}

impl<T, U, const N: usize> ArchivedRepr<[U; N]> for [T; N]
where
    T: ArchivedRepr<U>,
{
    const MAX_ALIGN: usize = max_align(align_of::<Self>(), T::MAX_ALIGN);
}

// The elements lie out of line, each aligned as `T`.
impl<T, U> ArchivedRepr<Vec<U>> for ArchivedVec<T>
where
    T: ArchivedRepr<U>,
{
    const MAX_ALIGN: usize = max_align(align_of::<Self>(), T::MAX_ALIGN);
}

// `rkyv` writes a map's nodes out of line, each aligned to the larger of its
// header's alignment and its entries'.  The header holds a length and a
// relative pointer, as the map's own root does, so the root's alignment
// stands in for it.
impl<AK, AV, K, V> ArchivedRepr<BTreeMap<K, V>> for ArchivedBTreeMap<AK, AV>
where
    AK: ArchivedRepr<K>,
    AV: ArchivedRepr<V>,
{
    const MAX_ALIGN: usize = max_align(align_of::<Self>(), max_align(AK::MAX_ALIGN, AV::MAX_ALIGN));
}

// A set is a map of its elements to `()`.
impl<T, U> ArchivedRepr<BTreeSet<U>> for ArchivedBTreeSet<T>
where
    T: ArchivedRepr<U>,
{
    const MAX_ALIGN: usize = max_align(align_of::<Self>(), T::MAX_ALIGN);
}

// The standard tuples, up to the eight elements `HashRepr` covers.
macro_rules! archived_repr_for_tuples {
    ($(($($original:ident / $archived:ident),+))+) => {$(
        impl<$($original, $archived),+> ArchivedRepr<($($original,)+)> for ($($archived,)+)
        where
            $($archived: ArchivedRepr<$original>,)+
        {
            const MAX_ALIGN: usize = {
                let mut max = align_of::<Self>();
                $(max = max_align(max, $archived::MAX_ALIGN);)+
                max
            };
        }
    )+};
}

archived_repr_for_tuples! {
    (T0/A0)
    (T0/A0, T1/A1)
    (T0/A0, T1/A1, T2/A2)
    (T0/A0, T1/A1, T2/A2, T3/A3)
    (T0/A0, T1/A1, T2/A2, T3/A3, T4/A4)
    (T0/A0, T1/A1, T2/A2, T3/A3, T4/A4, T5/A5)
    (T0/A0, T1/A1, T2/A2, T3/A3, T4/A4, T5/A5, T6/A6)
    (T0/A0, T1/A1, T2/A2, T3/A3, T4/A4, T5/A5, T6/A6, T7/A7)
}

#[cfg(test)]
mod test {
    //! Is each built-in `MAX_ALIGN` both safe and no larger than it has to be?
    //!
    //! Safe means that archiving no value of the type asks the serializer for
    //! a stricter alignment; tight means that some value asks for exactly
    //! that much.  The values below are picked so that at least one of them
    //! writes the type's most strictly aligned object.

    use super::{ArchivedRepr, MAX_PRIMITIVE_ALIGN};
    use crate::storage::file::{DbspSerializer, archived_alignment};
    use rkyv::{Archive, Archived, Serialize};
    use std::collections::{BTreeMap, BTreeSet};
    use std::fmt::Debug;
    use std::sync::Arc;

    /// Checks that `T`'s archived form claims exactly the strictest alignment
    /// archiving `values` asks for.
    ///
    /// # Arguments
    ///
    /// * `values` - values of `T`, at least one of which writes the type's
    ///   most strictly aligned object.
    fn check_exact<T>(values: &[T])
    where
        T: Archive + for<'a> Serialize<DbspSerializer<'a>> + Debug,
        Archived<T>: ArchivedRepr<T>,
    {
        let claimed = <Archived<T> as ArchivedRepr<T>>::MAX_ALIGN;
        for value in values {
            let asked = archived_alignment(value);
            assert!(
                asked <= claimed,
                "archiving {value:?} asked for alignment {asked}, more than the {claimed} its type claims",
            );
        }
        let strictest = values.iter().map(archived_alignment).max().unwrap();
        assert_eq!(
            strictest,
            claimed,
            "{} claims alignment {claimed}, but archiving {values:?} never asked for more than {strictest}",
            std::any::type_name::<T>(),
        );
    }

    #[test]
    fn primitives_need_their_own_alignment() {
        check_exact(&[()]);
        check_exact(&[false, true]);
        check_exact(&['x']);
        check_exact(&[-1i8]);
        check_exact(&[1u16]);
        check_exact(&[-1i32]);
        check_exact(&[1i64]);
        check_exact(&[1u128]);
        check_exact(&[-1i128]);
        check_exact(&[usize::MAX]);
        check_exact(&[isize::MIN]);
        check_exact(&[uuid::Uuid::from_u128(7)]);
    }

    /// Text and other out-of-line data aligned less strictly than the root
    /// leave the root's alignment as the strictest.
    #[test]
    fn a_string_needs_only_its_roots_alignment() {
        check_exact(&[String::new(), "x".repeat(100)]);
        check_exact(&[Some("x".repeat(100)), None]);
        check_exact(&[vec![1u8, 2, 3]]);
        check_exact(&[vec!["x".repeat(30), String::new()]]);
        check_exact(&[(1u8, "x".repeat(30))]);
        check_exact(&[Box::new(1u8)]);
    }

    /// A sixteen-byte primitive anywhere inside a value, inline or out of
    /// line, makes the whole value need sixteen.
    #[test]
    fn a_wide_integer_anywhere_needs_sixteen() {
        check_exact(&[vec![1u128]]);
        check_exact(&[Some(1i128), None]);
        check_exact(&[vec![vec![1u128]]]);
        check_exact(&[vec![Some(1i128)]]);
        check_exact(&[[1u128, 2]]);
        check_exact(&[(1u8, vec![1i128])]);
        check_exact(&[Box::new(1u128)]);
        check_exact(&[Arc::new(1u128)]);
        check_exact(&[BTreeMap::from([(1u8, 1u128)])]);
        check_exact(&[BTreeSet::from([1i128])]);
        assert_eq!(
            <Archived<Vec<u128>> as ArchivedRepr<Vec<u128>>>::MAX_ALIGN,
            MAX_PRIMITIVE_ALIGN
        );
    }

    /// A map's nodes need at least the alignment of their headers, whatever
    /// the entries need.
    #[test]
    fn a_maps_nodes_need_their_headers_alignment() {
        check_exact(&[BTreeMap::from([(1u8, 2u8)])]);
        check_exact(&[BTreeMap::from([(1u32, "x".repeat(20))])]);
        check_exact(&[BTreeSet::from([1u8, 2, 3])]);
    }

    // Shapes `#[derive(ArchivedRepr)]` works an alignment out for.  None of
    // them needs to be `DBData`, so they derive only what archiving them
    // takes.
    #[derive(Debug, Hash, Archive, Serialize, feldera_macros::ArchivedRepr)]
    struct Text {
        name: String,
        tags: Vec<String>,
    }

    #[derive(Debug, Hash, Archive, Serialize, feldera_macros::ArchivedRepr)]
    struct Amount {
        name: String,
        cents: Option<i128>,
    }

    #[derive(Debug, Hash, Archive, Serialize, feldera_macros::ArchivedRepr)]
    struct Nothing;

    #[derive(Debug, Hash, Archive, Serialize, feldera_macros::ArchivedRepr)]
    struct Generic<T> {
        one: T,
        many: Vec<T>,
    }

    #[derive(Debug, Hash, Archive, Serialize, feldera_macros::ArchivedRepr)]
    enum Shape {
        Empty,
        Named(String),
        Wide { values: Vec<u128> },
    }

    #[derive(Debug, Hash, Archive, Serialize, feldera_macros::ArchivedRepr)]
    #[archive(bound(serialize = "__S: rkyv::ser::ScratchSpace + rkyv::ser::Serializer"))]
    struct Tree {
        value: i32,
        #[omit_bounds]
        children: Vec<Tree>,
    }

    /// A derived struct or enum needs what its strictest field needs, and no
    /// more.
    #[test]
    fn a_derived_type_needs_what_its_fields_need() {
        check_exact(&[Text {
            name: "x".repeat(30),
            tags: vec![String::new()],
        }]);
        check_exact(&[Amount {
            name: String::new(),
            cents: Some(-1),
        }]);
        check_exact(&[Nothing]);
        check_exact(&[Generic {
            one: 1u8,
            many: vec![2u8],
        }]);
        check_exact(&[Generic {
            one: 1u128,
            many: vec![],
        }]);
        check_exact(&[
            Shape::Empty,
            Shape::Named("x".repeat(30)),
            Shape::Wide { values: vec![1] },
        ]);
    }

    /// A recursive field cannot name its own alignment, so it counts as the
    /// strictest a primitive can need: more than this tree needs, which is
    /// safe, and never less.
    #[test]
    fn a_recursive_field_counts_as_the_strictest_primitive() {
        let tree = Tree {
            value: 1,
            children: vec![Tree {
                value: 2,
                children: vec![],
            }],
        };
        assert!(archived_alignment(&tree) < MAX_PRIMITIVE_ALIGN);
        assert_eq!(
            <ArchivedTree as ArchivedRepr<Tree>>::MAX_ALIGN,
            MAX_PRIMITIVE_ALIGN
        );
    }

    /// A tuple of up to eight fields is the tuple of its archived fields.
    #[test]
    fn a_narrow_declared_tuple_needs_what_its_fields_need() {
        use crate::utils::{Tup2, Tup3};

        check_exact(&[Tup3::new(1u8, "x".repeat(30), 2i64)]);
        check_exact(&[Tup2::new(1u8, 2u128)]);
        check_exact(&[Tup2::new(1u8, 2u16)]);
    }

    /// A tuple of more than eight fields keeps its fields out of line, in a
    /// dense body or behind the relative pointers of a sparse one; the
    /// sparse body's vector of pointers is what makes it need eight even
    /// when every field needs less.
    #[test]
    fn a_wide_declared_tuple_needs_its_body_and_its_fields() {
        use crate::utils::Tup10;

        type Narrow = Tup10<
            Option<i32>,
            Option<i32>,
            Option<i32>,
            Option<i32>,
            Option<i32>,
            Option<i32>,
            Option<i32>,
            Option<i32>,
            Option<i32>,
            Option<i32>,
        >;
        let sparse = Narrow::new(
            Some(1),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
        let dense = Narrow::new(
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            Some(5),
            Some(6),
            Some(7),
            Some(8),
            Some(9),
            Some(10),
        );
        check_exact(&[sparse, dense]);

        type Decimal = Tup10<
            Option<i128>,
            Option<i32>,
            Option<i32>,
            Option<i32>,
            Option<i32>,
            Option<i32>,
            Option<i32>,
            Option<i32>,
            Option<i32>,
            Option<String>,
        >;
        check_exact(&[Decimal::new(
            Some(1),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some("x".repeat(30)),
        )]);
    }
}
