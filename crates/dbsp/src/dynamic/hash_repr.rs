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

use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::rc::Rc;
use std::sync::Arc;

use rkyv::collections::btree_map::ArchivedBTreeMap;
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
/// Faithful means the same sequence of [`Hasher`] calls, not merely the same
/// bytes in some order, so that the guarantee does not rest on the hasher
/// being insensitive to where one call ends and the next begins.  The one
/// thing still asked of the hasher is that it write a length the way the
/// standard library's sequences do, as a `usize`; a hasher that overrode
/// `Hasher::write_length_prefix`, which is unstable, would diverge.
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
/// is because the archived type *is* the decoded type.
#[macro_export]
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
/// an integer to itself, in the machine's own byte order.
#[macro_export]
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
    [T: ?Sized] Arc<T>,
    [T: ?Sized] Rc<T>,
);

#[cfg(test)]
mod test {
    //! Does a faithful implementation really reproduce the decoded hash?
    //!
    //! The broad coverage lives in `sqllib/tests/archived_ord.rs`, where the
    //! sqllib types are reachable.  These check the pieces defined here, and
    //! in particular the two that `rkyv`'s own `Hash` gets wrong.

    use std::collections::BTreeMap;

    use super::{HashRepr, archived_hash};
    use crate::DBData;
    use crate::hash::default_hash;
    use crate::storage::file::to_bytes;

    /// Archives `value`, hashes both forms, and insists they agree.
    fn check<T>(value: &T)
    where
        T: DBData + HashRepr,
        T::Repr: HashRepr,
    {
        let bytes = to_bytes(value).unwrap();
        // SAFETY: `bytes` came from `to_bytes::<T>` on the line above.
        let archived = unsafe { rkyv::archived_root::<T>(bytes.as_slice()) };
        assert_eq!(
            archived_hash(archived),
            Some(default_hash(value)),
            "the archived form of {value:?} hashes differently from the decoded one",
        );
        // The decoded implementation has to agree with `Hash` as well, since
        // it stands in for the same value.
        assert_eq!(archived_hash(value), Some(default_hash(value)));
    }

    /// A hasher that records what it was asked to write rather than a hash.
    ///
    /// Every `write_*` is left on its default, which routes through `write`,
    /// so the log distinguishes one write of a slice's bytes from one write
    /// an element -- which the hasher `archived_hash` uses cannot, being
    /// insensitive to where one call ends and the next begins.  That is what
    /// makes it the right instrument here: a sequence of primitives is
    /// exactly where the two forms could make different calls and still agree
    /// on the answer.
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
        T: DBData + std::hash::Hash,
        T::Repr: HashRepr,
    {
        let bytes = to_bytes(value).unwrap();
        // SAFETY: `bytes` came from `to_bytes::<T>` on the line above.
        let archived = unsafe { rkyv::archived_root::<T>(bytes.as_slice()) };

        let mut decoded = CallLog::default();
        std::hash::Hash::hash(value, &mut decoded);
        let mut archived_calls = CallLog::default();
        archived.hash_repr(&mut archived_calls);

        assert_eq!(
            decoded.0, archived_calls.0,
            "hashing the archived form of {value:?} asks the hasher for \
             something different from hashing the decoded one",
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
}
