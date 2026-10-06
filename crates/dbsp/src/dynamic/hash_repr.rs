//! Hashing an archived value exactly as its decoded form would hash.
//!
//! A merge that splices a run of keys never decodes them, but the batch it
//! writes carries a membership filter, and that filter hashes every key.  The
//! filter is queried later from a decoded key, so a hash taken from the
//! archived form has to equal the hash the decoded form would have produced.
//! A mismatch is a false negative, which a membership filter is never allowed
//! to produce: the lookup finds nothing and the query is silently wrong.
//!
//! Two archived forms do not hash like their decoded counterparts, so this
//! cannot simply forward to [`Hash`]:
//!
//! - A map.  [`BTreeMap`](std::collections::BTreeMap) writes a length prefix
//!   before its entries and `rkyv`'s archived map does not, which diverges for
//!   every map, the empty one included.
//! - Any enum whose archived hash is derived.  `rkyv` gives the archived type
//!   the narrowest unsigned repr that fits its variants, so its discriminant
//!   is usually one byte where the decoded enum's is eight.
//!
//! # Opting in
//!
//! [`HashRepr::FAITHFUL`] says whether an implementation reproduces the
//! decoded hash.  A composite is faithful only if everything it contains is,
//! and [`archived_hash`] returns `None` when it is not, which tells a caller
//! to decode the value and hash that instead.  Being slow is the worst that a
//! type nobody has got to yet can do; it cannot be wrong.
//!
//! An implementation that claims to be faithful is checked against the decoded
//! hash by the tests in `sqllib/tests/archived_ord.rs`, over hand-picked
//! values and generated ones.

use std::collections::{BTreeMap, BTreeSet};
use std::hash::{Hash, Hasher};
use std::rc::Rc;
use std::sync::Arc;

use rkyv::collections::btree_map::ArchivedBTreeMap;
use rkyv::collections::btree_set::ArchivedBTreeSet;
use rkyv::option::ArchivedOption;
use rkyv::rc::ArchivedRc;
use rkyv::string::ArchivedString;
use rkyv::vec::ArchivedVec;

use crate::hash::default_hasher;

/// Hashes an archived value the way its decoded form hashes.
///
/// This cannot forward to [`Hash`], because two archived forms do not hash
/// like their decoded counterparts: an archived map leaves out the length
/// prefix [`BTreeMap`](std::collections::BTreeMap) writes before its entries,
/// and an archived enum with a derived hash writes a discriminant narrower
/// than the decoded one, usually one byte where the decoded enum's is eight.
///
/// [`FAITHFUL`](Self::FAITHFUL) says whether an implementation reproduces the
/// decoded hash.  A composite is faithful only if everything it holds is, and
/// [`archived_hash`] answers `None` when it is not, which tells a caller to
/// decode the value and hash that instead.
///
/// Faithful means the same sequence of writes of the same bytes, not merely
/// the same bytes in some order, so that the guarantee does not rest on the
/// hasher being insensitive to where one write ends and the next begins.  It
/// does not mean the same [`Hasher`] methods.  An archived `usize` is a `u64`
/// of the same width, so it writes the same bytes, but through `write_u64`
/// where the decoded value calls `write_usize`; an archived `isize` does the
/// same through `write_i64`.  Two things are therefore asked of the hasher:
///
/// - that it write a length the way the standard library's sequences do, as
///   a `usize`; a hasher that overrode `Hasher::write_length_prefix`, which
///   is unstable, would diverge;
/// - that it write an integer as the integer's bytes, whichever `write_*`
///   method carries it, which is what [`Hasher`]'s default methods do; a
///   hasher that told `write_u64` from `write_usize` would diverge on every
///   `usize` and `isize`.
pub trait HashRepr {
    /// Whether [`hash_repr`](Self::hash_repr) writes what the decoded value
    /// would write.
    ///
    /// `false` means the archived form cannot be hashed faithfully, either
    /// because its own encoding loses the distinction or because something it
    /// contains cannot.  Callers fall back to decoding.
    const FAITHFUL: bool;

    /// Writes to `state` exactly what hashing the decoded value would write.
    ///
    /// Meaningless, though not unsound, when [`FAITHFUL`](Self::FAITHFUL) is
    /// `false`.
    fn hash_repr<H: Hasher>(&self, state: &mut H);

    /// Writes what hashing a slice of the decoded values would write.
    ///
    /// [`Hash`] lets a type replace the element-by-element loop with a single
    /// bulk write, and the integers take it: hashing a `[i64]` is one
    /// [`write`](Hasher::write) of the whole slice's bytes, not one
    /// `write_i64` an element.  A hasher may tell those apart, so an archived
    /// form whose decoded counterpart overrides [`Hash::hash_slice`] has to
    /// override this in step, or a sequence of them diverges.
    ///
    /// The default is the loop, which is also [`Hash`]'s default.
    #[inline]
    fn hash_slice_repr<H: Hasher>(data: &[Self], state: &mut H)
    where
        Self: Sized,
    {
        for element in data {
            element.hash_repr(state);
        }
    }
}

/// The hash of an archived value, or `None` if this type cannot be hashed
/// without decoding it.
pub fn archived_hash<T>(value: &T) -> Option<u64>
where
    T: HashRepr + ?Sized,
{
    T::FAITHFUL.then(|| {
        let mut hasher = default_hasher();
        value.hash_repr(&mut hasher);
        hasher.finish()
    })
}

/// Writes a length the way the standard library prefixes a sequence with one.
///
/// `Hasher::write_length_prefix` is what the standard library calls and is
/// still unstable; its default writes the length as a `usize`, which is what a
/// third-party hasher such as ours receives.  Calling `write_usize` directly
/// reproduces that.  The tests compare against the real decoded hash, so if
/// this ever stops matching they say so.
#[inline]
fn write_length_prefix<H: Hasher>(state: &mut H, len: usize) {
    state.write_usize(len);
}

/// Hashes an [`Option`]'s discriminant the way a decoded `Option` hashes it.
///
/// A derived [`Hash`] hashes [`std::mem::discriminant`], and how that hashes
/// is not specified, so this asks for the real thing rather than reproducing
/// it.  The discriminant of `Option<()>` hashes identically to that of
/// `Option<T>` for any `T`, because both carry the same value and `Hash` for
/// [`Discriminant`](std::mem::Discriminant) writes only that value, so the
/// payload type does not have to be named here.
#[inline]
fn hash_option_discriminant<H: Hasher>(state: &mut H, is_some: bool) {
    let probe = if is_some { Some(()) } else { None };
    std::mem::discriminant(&probe).hash(state);
}

/// Forwards to [`Hash`] for archived forms that already hash like their
/// decoded counterparts.
///
/// Used only where the two have been checked to agree, which for a primitive
/// is because the archived type *is* the decoded type.  Not exported: it
/// declares the hash faithful without checking, and on a type whose archived
/// form hashes differently that makes lookups miss rows.
macro_rules! impl_hash_repr_via_hash {
    ($($ty:ty),* $(,)?) => {$(
        impl $crate::dynamic::HashRepr for $ty {
            const FAITHFUL: bool = true;

            #[inline]
            fn hash_repr<H: ::std::hash::Hasher>(&self, state: &mut H) {
                ::std::hash::Hash::hash(self, state)
            }
        }
    )*};
}

/// The same, for the types whose [`Hash`] also writes a slice of them in one
/// go rather than one element at a time.
///
/// The standard library does this for every integer width and for nothing
/// else -- not `bool`, not `char` -- so the list here is that list.  The body
/// is `Hash::hash_slice`'s: reinterpret the slice as bytes and write them.
/// That is the same reinterpretation on both sides, because `rkyv` archives
/// an integer to itself, in the machine's own byte order.  Not exported, for
/// the same reason as `impl_hash_repr_via_hash`.
macro_rules! impl_hash_repr_via_hash_and_slice {
    ($($ty:ty),* $(,)?) => {$(
        impl $crate::dynamic::HashRepr for $ty {
            const FAITHFUL: bool = true;

            #[inline]
            fn hash_repr<H: ::std::hash::Hasher>(&self, state: &mut H) {
                ::std::hash::Hash::hash(self, state)
            }

            #[inline]
            fn hash_slice_repr<H: ::std::hash::Hasher>(data: &[Self], state: &mut H) {
                ::std::hash::Hash::hash_slice(data, state)
            }
        }
    )*};
}

// `rkyv` without the endian-aware features archives a primitive to itself, so
// these are the decoded implementations.
impl_hash_repr_via_hash!(bool, char, ());

impl_hash_repr_via_hash_and_slice!(
    i8, i16, i32, i64, i128, isize, u8, u16, u32, u64, u128, usize,
);

/// An archived `usize` is a `u64`, and an archived `isize` an `i64`, so the
/// two hash through the implementations of `u64` and `i64` above.  They write
/// the bytes the decoded value writes only where both forms have one width,
/// which `rkyv`'s `size_64` feature makes true on every 64-bit target.
const _: () = assert!(
    size_of::<usize>() == size_of::<rkyv::Archived<usize>>(),
    "HashRepr needs usize and its archived form to have one width"
);

// A `uuid` archives to itself, so the decoded implementation is the archived
// one.
impl_hash_repr_via_hash!(uuid::Uuid);

// `ArchivedString` hashes through `as_str`, which is what `String` does.
impl_hash_repr_via_hash!(ArchivedString);

impl<T> HashRepr for ArchivedOption<T>
where
    T: HashRepr,
{
    const FAITHFUL: bool = T::FAITHFUL;

    fn hash_repr<H: Hasher>(&self, state: &mut H) {
        // `Option`'s derived hash writes its discriminant and then, for
        // `Some`, the payload.  Spelled out rather than delegated because the
        // payload has to hash through `HashRepr`, not through `Hash`.
        match self {
            ArchivedOption::None => hash_option_discriminant(state, false),
            ArchivedOption::Some(value) => {
                hash_option_discriminant(state, true);
                value.hash_repr(state);
            }
        }
    }
}

impl<T> HashRepr for ArchivedVec<T>
where
    T: HashRepr,
{
    const FAITHFUL: bool = T::FAITHFUL;

    fn hash_repr<H: Hasher>(&self, state: &mut H) {
        // A slice writes its length and then its elements, through whichever
        // of the two ways the element type's decoded `Hash` writes them.
        write_length_prefix(state, self.len());
        T::hash_slice_repr(self.as_slice(), state);
    }
}

impl<T, const N: usize> HashRepr for [T; N]
where
    T: HashRepr,
{
    const FAITHFUL: bool = T::FAITHFUL;

    fn hash_repr<H: Hasher>(&self, state: &mut H) {
        // An array hashes as the slice of its elements, which writes the
        // length before them.  `rkyv` archives `[T; N]` to `[T::Archived; N]`,
        // so this one implementation covers the archived form and the decoded
        // one alike.
        write_length_prefix(state, N);
        T::hash_slice_repr(self, state);
    }
}

impl<K, V> HashRepr for ArchivedBTreeMap<K, V>
where
    K: HashRepr,
    V: HashRepr,
{
    const FAITHFUL: bool = K::FAITHFUL && V::FAITHFUL;

    fn hash_repr<H: Hasher>(&self, state: &mut H) {
        // The length prefix `rkyv`'s own implementation leaves out.  Each
        // entry then hashes as a pair, which writes its two halves and
        // nothing else.
        write_length_prefix(state, self.len());
        for (key, value) in self.iter() {
            key.hash_repr(state);
            value.hash_repr(state);
        }
    }
}

impl<K> HashRepr for ArchivedBTreeSet<K>
where
    K: HashRepr,
{
    const FAITHFUL: bool = K::FAITHFUL;

    fn hash_repr<H: Hasher>(&self, state: &mut H) {
        // A set hashes as the map of its elements to `()`, whose entries are
        // pairs whose second half writes nothing.  The length prefix is again
        // the one `rkyv`'s own implementation leaves out.
        write_length_prefix(state, self.len());
        for key in self.iter() {
            key.hash_repr(state);
        }
    }
}

impl<T, F> HashRepr for ArchivedRc<T, F>
where
    T: HashRepr + rkyv::ArchivePointee + ?Sized,
{
    const FAITHFUL: bool = T::FAITHFUL;

    fn hash_repr<H: Hasher>(&self, state: &mut H) {
        // `Arc` and `Rc` hash whatever they point at.
        (**self).hash_repr(state);
    }
}

/// A decoded value's faithful hash is simply [`Hash`], by definition: it is
/// the answer everything else has to match.  Writing these out again would
/// create a second definition to keep in step with the first, so they
/// forward.
///
/// These exist because a decoded type is sometimes its own archived form, and
/// because a composite needs to ask its fields whether they are faithful.
macro_rules! decoded_hash_repr {
    ($([$($generics:tt)*] $ty:ty $(where $($bound:tt)*)?),* $(,)?) => {$(
        impl<$($generics)*> HashRepr for $ty
        where
            $ty: Hash,
            $($($bound)*)?
        {
            const FAITHFUL: bool = true;

            #[inline]
            fn hash_repr<H: Hasher>(&self, state: &mut H) {
                self.hash(state)
            }
        }
    )*};
}

decoded_hash_repr!(
    [] String,
    [T] Vec<T>,
    [T] Option<T>,
    [K, V] BTreeMap<K, V>,
    [T] BTreeSet<T>,
    [T: ?Sized] Arc<T>,
    [T: ?Sized] Rc<T>,
);

/// A tuple hashes its fields in order and writes nothing else, and `rkyv`
/// archives one to the tuple of its fields' archived forms, so the archived
/// tuple reproduces the decoded hash by hashing each field the same way.
///
/// This is the same argument the narrow tuple layout in `feldera-macros`
/// makes for `TupN`; these are Rust's own tuples, which `DBData` also
/// covers.
macro_rules! tuple_hash_repr {
    ($(($($name:ident $idx:tt),+))*) => {$(
        impl<$($name),+> HashRepr for ($($name,)+)
        where
            $($name: HashRepr,)+
        {
            const FAITHFUL: bool = true $(&& $name::FAITHFUL)+;

            #[inline]
            fn hash_repr<H: Hasher>(&self, state: &mut H) {
                $(self.$idx.hash_repr(state);)+
            }
        }
    )*};
}

tuple_hash_repr! {
    (A 0)
    (A 0, B 1)
    (A 0, B 1, C 2)
    (A 0, B 1, C 2, D 3)
    (A 0, B 1, C 2, D 3, E 4)
    (A 0, B 1, C 2, D 3, E 4, F 5)
    (A 0, B 1, C 2, D 3, E 4, F 5, G 6)
    (A 0, B 1, C 2, D 3, E 4, F 5, G 6, I 7)
}

/// `rkyv`'s box declines, the one archived form here that is not dbsp's own.
///
/// It could forward to what it points at and be faithful the day something
/// needs it; nothing does, and declining is slow rather than wrong. The
/// impls for dbsp's own containers, its unit weight and its times sit beside
/// those types, for the same reason and with the same note.
impl<T> HashRepr for rkyv::boxed::ArchivedBox<T>
where
    T: rkyv::ArchivePointee + ?Sized,
{
    const FAITHFUL: bool = false;

    #[inline]
    fn hash_repr<H: Hasher>(&self, _state: &mut H) {}
}

#[cfg(test)]
mod test {
    //! Does a faithful implementation really reproduce the decoded hash?
    //!
    //! The broad coverage lives in `sqllib/tests/archived_ord.rs`, where the
    //! sqllib types are reachable.  These check the pieces defined here, and
    //! in particular the two that `rkyv`'s own `Hash` gets wrong.

    use std::collections::{BTreeMap, BTreeSet};

    use feldera_macros::{ArchivedRepr, IsNone};
    use rkyv::{Archive, Deserialize, Serialize};
    use size_of::SizeOf;

    use std::fmt::Debug;
    use std::hash::Hash;

    use super::{HashRepr, archived_hash};
    use crate::dynamic::ArchivedDBData;
    use crate::hash::default_hash;
    use crate::storage::file::{DbspSerializer, to_bytes};

    /// Archives `value`, hashes both forms, and insists they agree.
    fn check_archived<T>(value: &T)
    where
        T: Archive + for<'a> Serialize<DbspSerializer<'a>> + Hash + Debug,
        T::Archived: HashRepr,
    {
        let bytes = to_bytes(value).unwrap();
        // SAFETY: `bytes` came from `to_bytes::<T>` on the line above.
        let archived = unsafe { rkyv::archived_root::<T>(bytes.as_slice()) };
        assert_eq!(
            archived_hash(archived),
            Some(default_hash(value)),
            "the archived form of {value:?} hashes differently from the decoded one",
        );
    }

    /// The same, for a type that also implements the trait undecoded, where
    /// that implementation has to agree with `Hash` as well, since it stands
    /// in for the same value.
    fn check<T>(value: &T)
    where
        T: ArchivedDBData + HashRepr + Hash + Debug,
        T::Repr: HashRepr,
    {
        check_archived(value);
        assert_eq!(archived_hash(value), Some(default_hash(value)));
    }

    /// A hasher that records what it was asked to write rather than a hash.
    ///
    /// Every `write_*` is left on its default, which routes through `write`,
    /// so the log distinguishes one write of a slice's bytes from one write
    /// per element -- which the hasher `archived_hash` uses cannot, being
    /// insensitive to where one write ends and the next begins.  That is what
    /// makes it the right instrument here: a sequence of primitives is
    /// exactly where the two forms could make different writes and still
    /// agree on the answer.
    #[derive(Default)]
    struct CallLog(Vec<Vec<u8>>);

    impl std::hash::Hasher for CallLog {
        fn finish(&self) -> u64 {
            0
        }

        fn write(&mut self, bytes: &[u8]) {
            self.0.push(bytes.to_vec());
        }
    }

    /// Checks that hashing the archived form asks the hasher for the same
    /// things, in the same order, as hashing the decoded one.
    fn check_calls<T>(value: &T)
    where
        T: Archive + for<'a> Serialize<DbspSerializer<'a>> + Hash + Debug,
        T::Archived: HashRepr,
    {
        let bytes = to_bytes(value).unwrap();
        // SAFETY: `bytes` came from `to_bytes::<T>` on the line above.
        let archived = unsafe { rkyv::archived_root::<T>(bytes.as_slice()) };

        let mut decoded = CallLog::default();
        Hash::hash(value, &mut decoded);
        let mut archived_calls = CallLog::default();
        archived.hash_repr(&mut archived_calls);

        assert_eq!(
            decoded.0, archived_calls.0,
            "hashing the archived form of {value:?} asks the hasher for \
             something different from hashing the decoded one",
        );
    }

    /// Archives `value` and insists that its archived form declines to hash.
    ///
    /// # Arguments
    ///
    /// * `value` - the decoded value whose archived form should decline.
    ///
    /// # Panics
    ///
    /// When [`archived_hash`] answers with a hash.
    fn check_declines<T>(value: &T)
    where
        T: Archive + for<'a> Serialize<DbspSerializer<'a>> + Debug,
        T::Archived: HashRepr,
    {
        let bytes = to_bytes(value).unwrap();
        // SAFETY: `bytes` came from `to_bytes::<T>` on the line above.
        let archived = unsafe { rkyv::archived_root::<T>(bytes.as_slice()) };
        assert_eq!(
            archived_hash(archived),
            None,
            "the archived form of {value:?} hashes rather than declining",
        );
    }

    /// The standard library hashes a slice of integers in one write, so the
    /// archived form has to as well.
    ///
    /// Answering with the same bytes split across one call an element would
    /// pass [`check`], because the hasher it uses cannot tell the two apart,
    /// and would diverge under one that can.
    #[test]
    fn a_sequence_asks_the_hasher_for_what_the_decoded_one_asks_for() {
        check_calls(&vec![1i64, 2, 3]);
        check_calls(&Vec::<i64>::new());
        check_calls(&vec![0u8, 1, 255]);
        check_calls(&vec![1i32, -1]);
        check_calls(&vec![1u128, 2]);

        // Element types the standard library has no bulk write for, where
        // both forms loop and the calls match that way instead.
        check_calls(&vec![true, false]);
        check_calls(&vec!['a', 'b']);
        check_calls(&vec![Some(1i64), None]);
        check_calls(&vec![String::from("a"), String::new()]);
        check_calls(&vec![vec![1u8], vec![]]);
    }

    #[test]
    fn primitives_and_strings() {
        check(&0i64);
        check(&i64::MIN);
        check(&u128::MAX);
        check(&true);
        check(&String::new());
        check(&"hello".to_string());
    }

    /// An archived `usize` is a `u64` and an archived `isize` an `i64`, so
    /// they hash through the implementations of `u64` and `i64` rather than
    /// their own.  They still write what the decoded values write, in the
    /// same writes, alone and in whatever holds them.
    #[test]
    fn usize_and_isize_hash_like_the_decoded_ones() {
        for value in [0usize, 1, usize::MAX] {
            check(&value);
        }
        for value in [isize::MIN, -1, 0, isize::MAX] {
            check(&value);
        }
        check(&vec![1usize, 2, 3]);
        check(&Some(7isize));
        check(&Option::<usize>::None);
        check(&(1usize, String::from("a")));
        check_archived(&PointerSized {
            len: usize::MAX,
            offsets: vec![-1, 1],
        });

        // One write of an integer's bytes, and one bulk write for a slice of
        // them.  The archived side makes the first through `write_u64` or
        // `write_i64`, which the log sees only as the bytes they write.
        check_calls(&5usize);
        check_calls(&-5isize);
        check_calls(&vec![1usize, 2, 3]);
        check_calls(&[-1isize, 1]);
        check_calls(&(1usize, -1isize));
    }

    #[test]
    fn options_and_sequences() {
        check(&Option::<i64>::None);
        check(&Some(7i64));
        check(&Some(String::new()));
        check(&Vec::<i64>::new());
        check(&vec![1i64, 2, 3]);
        check(&vec![String::from("a")]);
        check(&vec![Some(1i64), None]);
    }

    /// The case `rkyv`'s own `Hash` gets wrong, by leaving out the length
    /// prefix that `BTreeMap` writes.
    #[test]
    fn maps_carry_their_length_prefix() {
        check(&BTreeMap::<i64, i64>::new());
        check(&BTreeMap::from([(1i64, 2i64)]));
        check(&BTreeMap::from([(1i64, 2i64), (3, 4)]));
        check(&BTreeMap::from([(String::from("a"), vec![1i64])]));
        check(&BTreeMap::from([(1i64, BTreeMap::from([(2i64, 3i64)]))]));
    }

    /// A set hashes as the map it wraps, so the length prefix is again the
    /// thing `rkyv`'s own `Hash` leaves out.
    #[test]
    fn sets_carry_their_length_prefix() {
        check(&BTreeSet::<i64>::new());
        check(&BTreeSet::from([1i64]));
        check(&BTreeSet::from([1i64, 2, 3]));
        check(&BTreeSet::from([String::from("a"), String::from("b")]));
        check_calls(&BTreeSet::from([1i64, 2, 3]));

        // A set large enough to fill more than one node, since that is where
        // the archived layout changes and the iteration order could not be
        // taken for granted.
        check(&(0i64..1000).collect::<BTreeSet<_>>());
    }

    /// An array hashes as the slice of its elements, bulk write and all.
    #[test]
    fn arrays_hash_as_the_slice_of_their_elements() {
        check(&[1i64, 2, 3]);
        check(&[0u8; 0]);
        check(&[Some(1i64), None]);
        check(&[String::from("a"), String::new()]);

        // The length prefix and the one bulk write a slice of integers asks
        // for, which the hasher `check` uses cannot tell from three writes.
        check_calls(&[1i64, 2, 3]);
        check_calls(&[0u8; 0]);
    }

    /// An array or vector of arrays or vectors hashes each inner one as a
    /// slice inside the outer one: a length prefix, then the elements, in one
    /// bulk write where they are integers.
    #[test]
    fn nested_arrays_and_vectors_hash_as_nested_slices() {
        check(&[[1u8, 2], [3, 4], [5, 6]]);
        check(&vec![[1i64, 2], [3, 4]]);
        check(&[vec![1u8], vec![]]);
        check(&vec![
            vec![Some(String::from("a"))],
            vec![None, Some(String::new())],
        ]);
        check(&[[(); 2]; 2]);

        check_calls(&[[1u8, 2], [3, 4], [5, 6]]);
        check_calls(&vec![[1i64, 2], [3, 4]]);
        check_calls(&[vec![1u8], vec![]]);
    }

    /// A tuple hashes its fields in order, and the archived form has to do
    /// the same.  Only the narrow layout is faithful; the wide one stores its
    /// fields sparsely and declines until someone writes that out.
    #[test]
    fn tuples_hash_their_fields_in_order() {
        use crate::utils::{Tup1, Tup2, Tup3, Tup8};

        check(&Tup1::new(1i64));
        check(&Tup2::new(1i64, 2u32));
        check(&Tup2::new(Some(1i64), Option::<String>::None));
        check(&Tup2::new(String::from("a"), vec![1i64, 2]));
        check(&Tup3::new(1i64, String::new(), Option::<i64>::None));
        check(&Tup8::new(1i64, 2u32, 3i16, 4u8, 5i8, 6u16, 7i32, 8u64));

        // `check` would already have failed had the narrow layout declined,
        // since it insists on a hash rather than accepting the `None` a
        // declining type answers with.  Worth stating at compile time as
        // well, because that is the form a caller reads to decide between
        // hashing the archived value and decoding it.
        const { assert!(<Tup2<i64, u32> as HashRepr>::FAITHFUL) };
    }

    /// Rust's own tuples hash their fields in order, as `TupN` does, and
    /// hash each field through `HashRepr` rather than `Hash`, so a map inside
    /// one still writes the length prefix `rkyv`'s own `Hash` leaves out.
    #[test]
    fn native_tuples_hash_their_fields_in_order() {
        check(&(1i64,));
        check(&(1i64, String::from("a")));
        check(&(Some(String::from("x")), vec![1i64, 2], Option::<i32>::None));
        check(&(1i64, BTreeMap::from([(2i64, 3i64)])));
        check(&vec![(1i64, String::from("a")), (2, String::new())]);

        // The widest tuple with an implementation, eight fields of mixed
        // width.
        let widest = (1u8, 2u16, 3u32, 4u64, 5u128, 6i8, 7i16, 8i32);
        check(&widest);
        check_calls(&widest);
        check_calls(&vec![(1i64, 2u8), (3, 4)]);
    }

    /// A zero-sized value writes nothing, so whatever holds one writes only
    /// its own length or discriminant.
    #[test]
    fn zero_sized_values_write_nothing_of_their_own() {
        use crate::utils::{Tup0, Tup2};

        check(&());
        check_archived(&Tup0());
        check(&vec![(); 3]);
        check(&[(); 3]);
        check(&vec![Tup0(); 2]);
        check(&Some(()));
        check(&Tup2::new((), 5i64));

        check_calls(&());
        check_calls(&vec![(); 3]);
        check_calls(&Some(()));
    }

    /// The special floats, which are where a hash that went through the raw
    /// bits rather than through `OrderedFloat` would diverge.
    #[test]
    fn floats_including_the_awkward_ones() {
        use crate::algebra::{F32, F64};

        for value in [
            f64::NEG_INFINITY,
            f64::MIN,
            -0.0,
            0.0,
            f64::MIN_POSITIVE,
            f64::MAX,
            f64::INFINITY,
            f64::NAN,
        ] {
            check(&F64::from(value));
        }
        for value in [f32::NEG_INFINITY, -0.0, 0.0, f32::INFINITY, f32::NAN] {
            check(&F32::from(value));
        }
        check(&Some(F64::from(f64::NAN)));
        check(&vec![F64::from(-0.0), F64::from(0.0)]);
    }

    /// A type that cannot be hashed from its archived form must say so, and
    /// must take with it everything built from it.
    ///
    /// Note that the poisoning applies to archived forms only.  A *decoded*
    /// value is always faithful, because hashing it is the answer the
    /// archived side has to match.
    #[test]
    fn an_unfaithful_archived_form_poisons_what_holds_it() {
        use rkyv::collections::btree_map::ArchivedBTreeMap;
        use rkyv::option::ArchivedOption;
        use rkyv::vec::ArchivedVec;

        struct Opaque;

        impl HashRepr for Opaque {
            const FAITHFUL: bool = false;

            fn hash_repr<H: std::hash::Hasher>(&self, _state: &mut H) {}
        }

        assert_eq!(archived_hash(&Opaque), None);
        const { assert!(!<ArchivedOption<Opaque> as HashRepr>::FAITHFUL) };
        const { assert!(!<ArchivedVec<Opaque> as HashRepr>::FAITHFUL) };
        const { assert!(!<ArchivedBTreeMap<i64, Opaque> as HashRepr>::FAITHFUL) };
        const { assert!(!<ArchivedBTreeMap<Opaque, i64> as HashRepr>::FAITHFUL) };
        // And a faithful one is not dragged down by its neighbours.
        const { assert!(<ArchivedVec<i64> as HashRepr>::FAITHFUL) };
    }

    // The shapes `#[derive(HashRepr)]` sees: a struct with named fields, a
    // tuple struct, a struct with no fields, an enum, and a struct that
    // holds the enum.  They derive `ArchivedRepr`, whose `HashRepr` is the
    // one `#[derive(HashRepr)]` generates, because `check` needs `DBData`.
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
        ArchivedRepr,
    )]
    #[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
    struct Named {
        id: u32,
        label: String,
        tags: Vec<Option<i16>>,
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
        ArchivedRepr,
    )]
    #[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
    struct Pair(u32, Option<String>);

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
        ArchivedRepr,
    )]
    #[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
    struct NoFields();

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
        ArchivedRepr,
    )]
    #[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
    enum Kind {
        #[default]
        Unit,
        Tuple(i32),
        Named {
            label: String,
        },
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
        ArchivedRepr,
    )]
    #[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
    struct Holder {
        id: u32,
        kind: Kind,
    }

    /// A derived implementation hashes the fields in declaration order, which
    /// is what `#[derive(Hash)]` does.
    #[test]
    fn a_derived_struct_hashes_its_fields_in_order() {
        let named = Named {
            id: 7,
            label: String::from("label"),
            tags: vec![Some(1), None],
        };
        check_archived(&named);
        check_archived(&Named::default());
        check_archived(&Pair(1, Some(String::from("a"))));
        check_archived(&Pair(u32::MAX, None));
        check_archived(&NoFields());

        // A struct writes its fields and nothing else, in the same calls.
        check_calls(&named);
        check_calls(&NoFields());

        const { assert!(<ArchivedNamed as HashRepr>::FAITHFUL) };
        const { assert!(<ArchivedPair as HashRepr>::FAITHFUL) };
        const { assert!(<ArchivedNoFields as HashRepr>::FAITHFUL) };
    }

    /// An archived enum writes a narrower discriminant than the decoded one,
    /// so the derive declines for an enum, and for whatever holds one.
    #[test]
    fn a_derived_enum_declines_and_takes_its_holder_with_it() {
        const { assert!(!<ArchivedKind as HashRepr>::FAITHFUL) };
        const { assert!(!<ArchivedHolder as HashRepr>::FAITHFUL) };

        for value in [
            Holder::default(),
            Holder {
                id: 1,
                kind: Kind::Tuple(2),
            },
            Holder {
                id: 1,
                kind: Kind::Named {
                    label: String::from("a"),
                },
            },
        ] {
            let bytes = to_bytes(&value).unwrap();
            // SAFETY: `bytes` came from `to_bytes::<Holder>` on the line above.
            let archived = unsafe { rkyv::archived_root::<Holder>(bytes.as_slice()) };
            assert_eq!(archived_hash(archived), None);
        }
    }

    // More shapes `#[derive(HashRepr)]` sees: a unit struct, a generic
    // struct, a struct of pointer-sized integers, the two field attributes
    // the derive declines for, and a field whose archived form declines.
    // None of them needs to be `DBData`, so they derive only what archiving
    // and hashing them takes.
    #[derive(Debug, Hash, Archive, Serialize, feldera_macros::HashRepr)]
    struct UnitStruct;

    #[derive(Debug, Hash, Archive, Serialize, feldera_macros::HashRepr)]
    struct Generic<T> {
        one: T,
        many: Vec<T>,
        maybe: Option<T>,
    }

    #[derive(Debug, Hash, Archive, Serialize, feldera_macros::HashRepr)]
    struct PointerSized {
        len: usize,
        offsets: Vec<isize>,
    }

    #[derive(Debug, Hash, Archive, Serialize, feldera_macros::HashRepr)]
    struct SkipsAField {
        kept: i64,
        #[with(rkyv::with::Skip)]
        skipped: i64,
    }

    #[derive(Debug, Hash, Archive, Serialize, feldera_macros::HashRepr)]
    #[archive(bound(serialize = "__S: rkyv::ser::ScratchSpace + rkyv::ser::Serializer"))]
    struct Tree {
        value: i64,
        #[omit_bounds]
        children: Vec<Tree>,
    }

    #[derive(Debug, Hash, Archive, Serialize, feldera_macros::HashRepr)]
    struct HoldsABox {
        value: i64,
        boxed: Box<i64>,
    }

    /// A unit struct writes nothing, which is what `#[derive(Hash)]` writes
    /// for it.
    #[test]
    fn a_derived_unit_struct_writes_nothing() {
        check_archived(&UnitStruct);
        check_calls(&UnitStruct);
        const { assert!(<ArchivedUnitStruct as HashRepr>::FAITHFUL) };
    }

    /// A generic struct is faithful exactly when its parameter is, since
    /// each of its fields, a `T`, a `Vec<T>` and an `Option<T>`, is.
    #[test]
    fn a_derived_generic_struct_is_faithful_when_its_parameter_is() {
        let numbers = Generic {
            one: 1i64,
            many: vec![2, 3],
            maybe: Some(4),
        };
        check_archived(&numbers);
        check_calls(&numbers);
        check_archived(&Generic {
            one: String::from("a"),
            many: vec![String::new()],
            maybe: None,
        });
        const { assert!(<ArchivedGeneric<i64> as HashRepr>::FAITHFUL) };

        // A box declines, and so does the struct that holds it.
        check_declines(&Generic {
            one: Box::new(1i64),
            many: vec![],
            maybe: None,
        });
        const { assert!(!<ArchivedGeneric<Box<i64>> as HashRepr>::FAITHFUL) };
    }

    /// A field with a `#[with]` wrapper archives in the wrapper's form rather
    /// than its own, which for `rkyv::with::Skip` holds nothing of the field,
    /// and `#[omit_bounds]` marks a field that makes the struct recursive.
    /// The derive declines for either.
    #[test]
    fn a_derived_struct_declines_for_a_wrapped_or_recursive_field() {
        check_declines(&SkipsAField {
            kept: 1,
            skipped: 2,
        });
        check_declines(&Tree {
            value: 1,
            children: vec![Tree {
                value: 2,
                children: vec![],
            }],
        });
        const { assert!(!<ArchivedSkipsAField as HashRepr>::FAITHFUL) };
        const { assert!(!<ArchivedTree as HashRepr>::FAITHFUL) };
    }

    /// `rkyv`'s box declines, and takes whatever holds one with it.
    #[test]
    fn a_box_declines_and_takes_its_holder_with_it() {
        check_declines(&Box::new(5i64));
        check_declines(&vec![Box::new(5i64)]);
        check_declines(&Some(Box::new(String::from("a"))));
        check_declines(&HoldsABox {
            value: 1,
            boxed: Box::new(2),
        });
        const { assert!(!<ArchivedHoldsABox as HashRepr>::FAITHFUL) };
    }
}
