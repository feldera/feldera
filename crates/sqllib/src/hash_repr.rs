//! Hashing archived SQL values the way their decoded forms hash.
//!
//! See [`dbsp::dynamic::HashRepr`] for why this cannot simply forward to
//! [`Hash`](std::hash::Hash) and what it means for an implementation to be
//! faithful.  A type with no implementation here is not wrong, only slow: a
//! caller that cannot hash the archived form decodes the value instead.

/// Implements `HashRepr` for a struct and its archived form.
///
/// Both hash their fields in order and write nothing else, which is what a
/// derived [`Hash`](std::hash::Hash) does for a struct.  Each field is named
/// with its decoded type so that faithfulness is computed from the fields
/// rather than asserted: a struct holding something that cannot be hashed
/// from its archived form is not faithful either.
///
/// Use it only on a struct whose own `Hash` is derived.  A hand-written one
/// may write something else entirely, and then this would not match it.
macro_rules! hash_repr_struct {
    ($($decoded:ty => $archived:ty { $($field:tt : $field_ty:ty),+ $(,)? }),* $(,)?) => {$(
        impl $crate::__hash_repr::HashRepr for $decoded {
            const FAITHFUL: bool =
                true $(&& <$field_ty as $crate::__hash_repr::HashRepr>::FAITHFUL)+;

            #[inline]
            fn hash_repr<H: ::std::hash::Hasher>(&self, state: &mut H) {
                $($crate::__hash_repr::HashRepr::hash_repr(&self.$field, state);)+
            }
        }

        impl $crate::__hash_repr::HashRepr for $archived {
            const FAITHFUL: bool = true $(&& <::rkyv::Archived<$field_ty> as
                $crate::__hash_repr::HashRepr>::FAITHFUL)+;

            #[inline]
            fn hash_repr<H: ::std::hash::Hasher>(&self, state: &mut H) {
                $($crate::__hash_repr::HashRepr::hash_repr(&self.$field, state);)+
            }
        }
    )*};
}

pub(crate) use hash_repr_struct;
