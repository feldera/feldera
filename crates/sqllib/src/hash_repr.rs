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
        impl $crate::__HashRepr for $decoded {
            const FAITHFUL: bool =
                true $(&& <$field_ty as $crate::__HashRepr>::FAITHFUL)+;

            #[inline]
            fn hash_repr<H: ::std::hash::Hasher>(&self, state: &mut H) {
                $($crate::__HashRepr::hash_repr(&self.$field, state);)+
            }
        }

        impl $crate::__HashRepr for $archived {
            const FAITHFUL: bool = true $(&& <::rkyv::Archived<$field_ty> as
                $crate::__HashRepr>::FAITHFUL)+;

            #[inline]
            fn hash_repr<H: ::std::hash::Hasher>(&self, state: &mut H) {
                $($crate::__HashRepr::hash_repr(&self.$field, state);)+
            }
        }
    )*};
}

pub(crate) use hash_repr_struct;

#[cfg(test)]
mod test {
    //! Does a composite work out its faithfulness from its fields?
    //!
    //! Every type the macro is used on holds only faithful fields, so the
    //! `false` side of that computation has no other test: a macro that
    //! asserted `true` instead of computing it would pass all of them, and a
    //! key holding a wide tuple would then claim a hash it cannot reproduce.
    //! That claim is what a membership filter turns into a false negative.

    use crate::__HashRepr;
    use dbsp::utils::Tup9;
    use rkyv::{Archive, Deserialize, Serialize};

    type Wide = Tup9<i64, i64, i64, i64, i64, i64, i64, i64, i64>;

    /// Asks a type whether it hashes faithfully.
    ///
    /// The answer is a constant, and asking for it through a function is what
    /// keeps these assertions from being constant expressions themselves,
    /// which is how one of them would be read at a glance.
    fn faithful<T: __HashRepr>() -> bool {
        T::FAITHFUL
    }

    #[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Archive, Serialize, Deserialize)]
    struct Faithful {
        id: i64,
        name: String,
    }

    hash_repr_struct! {
        Faithful => ArchivedFaithful { id: i64, name: String },
    }

    #[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Archive, Serialize, Deserialize)]
    struct HoldsAWideTuple {
        id: i64,
        wide: Wide,
    }

    hash_repr_struct! {
        HoldsAWideTuple => ArchivedHoldsAWideTuple { id: i64, wide: Wide },
    }

    #[test]
    fn a_struct_of_faithful_fields_is_faithful() {
        assert!(faithful::<Faithful>());
        assert!(faithful::<ArchivedFaithful>());
    }

    /// A tuple of more than eight fields archives sparsely, behind a bitmap,
    /// and its archived form declines; the decoded tuple hashes as its fields
    /// do.  A struct holding one inherits each answer.
    #[test]
    fn a_struct_declines_when_a_field_does() {
        assert!(!faithful::<rkyv::Archived<Wide>>());
        assert!(!faithful::<ArchivedHoldsAWideTuple>());

        assert!(faithful::<Wide>());
        assert!(faithful::<HoldsAWideTuple>());
    }
}
