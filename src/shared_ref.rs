//! Shared reference trait.

use std::{borrow::Borrow, rc::Rc};

/// A trait that generalizes shared pointers like `Rc` and `Arc`.
///
/// The holder of a shared reference can extract an owned copy
/// of the reference object from it if there are no other references
/// to the same object (the refcount is 0).
pub trait SharedRef: Borrow<Self::Target> + Clone + Sized {
    type Target;

    /// Try to extract the owned value from `self`.
    ///
    /// If `self` is the last reference to the object, returns
    /// the object; otherwise, returns `Err(self)`.
    fn try_into_owned(self) -> Result<Self::Target, Self>;
}

/// Implement `SharedRef<Target=Self>` for a type.
#[macro_export]
macro_rules! shared_ref_self {
    ($type:ty) => {
        impl $crate::SharedRef for $type {
            type Target = Self;

            fn try_into_owned(self) -> std::result::Result<Self::Target, Self> {
                Ok(self)
            }
        }
    };
}

/// Implement `SharedRef<Target=Self>` for a type.
#[macro_export]
macro_rules! shared_ref_self_generic {
    (<$($typearg:ident),*>, $type:ty) => {
        impl<$($typearg),*> $crate::SharedRef for $type
        where
            $($typearg: Clone),*
        {
            type Target = Self;

            fn try_into_owned(self) -> std::result::Result<Self::Target, Self> {
                Ok(self)
            }
        }
    };
}

shared_ref_self!(u8);
shared_ref_self!(u16);
shared_ref_self!(u32);
shared_ref_self!(u64);
shared_ref_self!(u128);
shared_ref_self!(usize);

shared_ref_self!(i8);
shared_ref_self!(i16);
shared_ref_self!(i32);
shared_ref_self!(i64);
shared_ref_self!(i128);
shared_ref_self!(isize);

shared_ref_self!(String);
shared_ref_self!(&'static str);

shared_ref_self!(());
shared_ref_self_generic!(<T1, T2>, (T1, T2));
shared_ref_self_generic!(<T1, T2, T3>, (T1, T2, T3));
shared_ref_self_generic!(<T1, T2, T3, T4>, (T1, T2, T3, T4));
shared_ref_self_generic!(<T1, T2, T3, T4, T5>, (T1, T2, T3, T4, T5));
shared_ref_self_generic!(<T1, T2, T3, T4, T5, T6>, (T1, T2, T3, T4, T5, T6));
shared_ref_self_generic!(<T1, T2, T3, T4, T5, T6, T7>, (T1, T2, T3, T4, T5, T6, T7));
shared_ref_self_generic!(<T1, T2, T3, T4, T5, T6, T7, T8>, (T1, T2, T3, T4, T5, T6, T7, T8));
shared_ref_self_generic!(<T1, T2, T3, T4, T5, T6, T7, T8, T9>, (T1, T2, T3, T4, T5, T6, T7, T8, T9));
shared_ref_self_generic!(<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>, (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10));
shared_ref_self_generic!(<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>, (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11));
shared_ref_self_generic!(<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12>, (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12));

impl<T> SharedRef for Rc<T> {
    type Target = T;

    fn try_into_owned(self) -> Result<Self::Target, Self> {
        Rc::try_unwrap(self)
    }
}
