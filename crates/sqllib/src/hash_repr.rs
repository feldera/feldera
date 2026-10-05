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
///
/// List the fields in the order the struct declares them, which is the order
/// a derived `Hash` writes them in.  A list that leaves a field out, names
/// one the struct lacks, or gives one a type other than its own fails to
/// compile.  The order is not checked: a list out of order compiles, and only
/// the tests that compare the archived hash with the decoded one catch it.
macro_rules! hash_repr_struct {
    ($($decoded:ty => $archived:ty { $($field:tt : $field_ty:ty),+ $(,)? }),* $(,)?) => {$(
        impl $crate::__HashRepr for $decoded {
            const FAITHFUL: bool =
                true $(&& <$field_ty as $crate::__HashRepr>::FAITHFUL)+;

            #[inline]
            fn hash_repr<H: ::std::hash::Hasher>(&self, state: &mut H) {
                // The list names every field, each with its own type.
                let Self { $($field: _),+ } = self;
                $(let _: &$field_ty = &self.$field;)+
                $($crate::__HashRepr::hash_repr(&self.$field, state);)+
            }
        }

        impl $crate::__HashRepr for $archived {
            const FAITHFUL: bool = true $(&& <::rkyv::Archived<$field_ty> as
                $crate::__HashRepr>::FAITHFUL)+;

            #[inline]
            fn hash_repr<H: ::std::hash::Hasher>(&self, state: &mut H) {
                let Self { $($field: _),+ } = self;
                $(let _: &::rkyv::Archived<$field_ty> = &self.$field;)+
                $($crate::__HashRepr::hash_repr(&self.$field, state);)+
            }
        }
    )*};
}

pub(crate) use hash_repr_struct;

/// Implements `ArchivedRepr` for the archived form of a struct that keeps
/// nothing out of line, which therefore needs only its own alignment.
///
/// The struct's `OrdRepr` and `HashRepr` come from elsewhere, such as a
/// derive and [`hash_repr_struct!`].  Use it only on a struct whose fields
/// are all held inline: one that points at anything would need what that
/// needs too, and debug builds catch a struct that does.
macro_rules! archived_repr_inline {
    ($($decoded:ty => $archived:ty),* $(,)?) => {$(
        impl ::dbsp::dynamic::ArchivedRepr<$decoded> for $archived {
            const MAX_ALIGN: usize = ::core::mem::align_of::<$archived>();
        }
    )*};
}

pub(crate) use archived_repr_inline;

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
