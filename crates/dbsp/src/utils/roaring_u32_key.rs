//! Trait for key types that can be mapped exactly into a `u32` domain for
//! roaring-bitmap membership tests.

use crate::dynamic::{BSet, LeanVec};
use crate::time::UnitTimestamp;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::Arc;
use uuid::Uuid;

pub trait RoaringU32Key {
    #[inline]
    fn as_roaring_u32(&self) -> Option<u32> {
        None
    }

    #[inline]
    fn can_use_roaring_u32() -> bool
    where
        Self: Sized,
    {
        false
    }
}

#[macro_export]
macro_rules! never_roaring_u32 {
    ($($ty:ty),* $(,)?) => {
        $(
            impl $crate::utils::RoaringU32Key for $ty {}
        )*
    };
}

never_roaring_u32!(
    (),
    bool,
    char,
    i8,
    i16,
    i64,
    i128,
    u8,
    u16,
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

impl RoaringU32Key for u32 {
    #[inline]
    fn as_roaring_u32(&self) -> Option<u32> {
        Some(*self)
    }

    #[inline]
    fn can_use_roaring_u32() -> bool
    where
        Self: Sized,
    {
        true
    }
}

impl RoaringU32Key for i32 {
    #[inline]
    fn as_roaring_u32(&self) -> Option<u32> {
        // Preserve the full `i32` bit pattern, which keeps membership exact.
        Some(*self as u32)
    }

    #[inline]
    fn can_use_roaring_u32() -> bool
    where
        Self: Sized,
    {
        true
    }
}

impl<T> RoaringU32Key for Option<T> {}

#[macro_export]
macro_rules! never_roaring_u32_1 {
    ($($wrapper:ident),* $(,)?) => {
        $(
            impl<T> $crate::utils::RoaringU32Key for $wrapper<T> {}
        )*
    };
}

never_roaring_u32_1!(Vec, LeanVec, BSet);

#[macro_export]
macro_rules! delegate_roaring_u32 {
    ($($wrapper:ident),* $(,)?) => {
        $(
            impl<T: $crate::utils::RoaringU32Key> $crate::utils::RoaringU32Key for $wrapper<T> {
                #[inline]
                fn as_roaring_u32(&self) -> Option<u32> {
                    self.as_ref().as_roaring_u32()
                }

                #[inline]
                fn can_use_roaring_u32() -> bool
                where
                    Self: Sized,
                {
                    T::can_use_roaring_u32()
                }
            }
        )*
    };
}

delegate_roaring_u32!(Box, Rc, Arc);

#[macro_export]
macro_rules! never_roaring_u32_tuples {
    ($($name:ident),+) => {
        impl<$($name),+> RoaringU32Key for ($($name,)+) {}
    };
}

never_roaring_u32_tuples!(A);
never_roaring_u32_tuples!(A, B);
never_roaring_u32_tuples!(A, B, C);
never_roaring_u32_tuples!(A, B, C, D);
never_roaring_u32_tuples!(A, B, C, D, E);
never_roaring_u32_tuples!(A, B, C, D, E, F);

impl<K, V> RoaringU32Key for BTreeMap<K, V> {}

#[cfg(test)]
mod test {
    use super::RoaringU32Key;
    use crate::utils::Tup1;

    #[test]
    fn supported_roaring_u32_keys() {
        assert!(u32::can_use_roaring_u32());
        assert_eq!(7u32.as_roaring_u32(), Some(7));

        assert!(i32::can_use_roaring_u32());
        assert_eq!((-7i32).as_roaring_u32(), Some((-7i32) as u32));

        assert!(Tup1::<i32>::can_use_roaring_u32());
        assert_eq!(Tup1(-7i32).as_roaring_u32(), Some((-7i32) as u32));
    }

    #[test]
    fn unsupported_roaring_u32_keys() {
        assert!(!String::can_use_roaring_u32());
        assert_eq!("feldera".to_string().as_roaring_u32(), None);
    }
}
