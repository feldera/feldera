//! Trait to identify Options which are None in a generic way
//! so we can reduce these values to 1 bit in our storage layer.

use crate::dynamic::{BSet, LeanVec};
use crate::time::UnitTimestamp;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::Arc;
use uuid::Uuid;

pub trait IsNone {
    type Inner;

    fn is_none(&self) -> bool;

    fn unwrap_or_self(&self) -> &Self::Inner;

    fn from_inner(inner: Self::Inner) -> Self;
}

impl<T> IsNone for Option<T> {
    type Inner = T;

    fn is_none(&self) -> bool {
        self.is_none()
    }

    fn unwrap_or_self(&self) -> &Self::Inner {
        self.as_ref()
            .expect("IsNone::unwrap_or_self called on None")
    }

    fn from_inner(inner: Self::Inner) -> Self {
        Some(inner)
    }
}

#[macro_export]
macro_rules! never_none {
    ($($ty:ty),* $(,)?) => {
        $(
            impl $crate::utils::IsNone for $ty {
                type Inner = $ty;

                fn is_none(&self) -> bool { false }

                fn unwrap_or_self(&self) -> &Self::Inner { self }

                fn from_inner(inner: Self::Inner) -> Self { inner }
            }
        )*
    };
}

never_none!(
    (),
    bool,
    char,
    i8,
    i16,
    i32,
    i64,
    i128,
    u8,
    u16,
    u32,
    u64,
    u128,
    f32,
    f64,
    usize,
    isize,
    String,
    UnitTimestamp,
    Uuid
);

#[macro_export]
macro_rules! never_none_1 {
    ($($wrapper:ident),* $(,)?) => {
        $(
            impl<T> $crate::utils::IsNone for $wrapper<T> {
                type Inner = $wrapper<T>;

                fn is_none(&self) -> bool { false }

                fn unwrap_or_self(&self) -> &Self::Inner { self }

                fn from_inner(inner: Self::Inner) -> Self { inner }
            }
        )*
    };
}

never_none_1!(Vec, LeanVec, BSet,);

#[macro_export]
macro_rules! delegate_is_none {
    ($($wrapper:ident),* $(,)?) => {
        $(
            impl<T: $crate::utils::IsNone> $crate::utils::IsNone for $wrapper<T> {
                type Inner = $wrapper<T>;

                fn is_none(&self) -> bool {
                    //self.as_ref().is_none()
                    // but for simplicity lets just start by making this always false
                    false
                }

                fn unwrap_or_self(&self) -> &Self::Inner { self }

                fn from_inner(inner: Self::Inner) -> Self { inner }
            }
        )*
    };
}

delegate_is_none!(Box, Rc, Arc,);

#[macro_export]
macro_rules! never_none_tuples {
    // Entry point: generate up to N elements
    ($($name:ident),+) => {
        impl<$($name),+> IsNone for ($($name,)+) {
            type Inner = ($($name,)+);

            fn is_none(&self) -> bool {
                false
            }

            fn unwrap_or_self(&self) -> &Self::Inner { self }

            fn from_inner(inner: Self::Inner) -> Self { inner }
        }
    };
}

never_none_tuples!(A);
never_none_tuples!(A, B);
never_none_tuples!(A, B, C);
never_none_tuples!(A, B, C, D);
never_none_tuples!(A, B, C, D, E);
never_none_tuples!(A, B, C, D, E, F);

impl<K, V> IsNone for BTreeMap<K, V> {
    type Inner = BTreeMap<K, V>;

    fn is_none(&self) -> bool {
        false
    }

    fn unwrap_or_self(&self) -> &Self::Inner { self }

    fn from_inner(inner: Self::Inner) -> Self { inner }
}

#[cfg(test)]
mod test {
    use feldera_macros::IsNone;

    #[derive(IsNone)]
    struct X {
        _i: i32,
    }

    #[test]
    fn x_not_none() {
        use dbsp::utils::IsNone;
        let x = X { _i: 0 };
        assert!(!x.is_none());
    }
}
