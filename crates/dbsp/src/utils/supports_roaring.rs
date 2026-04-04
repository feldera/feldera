//! Trait for key types that can be mapped into a roaring bitmap domain.

use crate::dynamic::{BSet, LeanVec};
use crate::time::UnitTimestamp;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::Arc;
use uuid::Uuid;

pub trait SupportsRoaring {
    #[inline]
    fn supports_roaring32(&self) -> bool {
        false
    }

    #[inline]
    fn into_roaring_u32(&self, _min: &Self) -> Option<u32>
    where
        Self: Sized,
    {
        self.roaring_u32_value()
    }

    #[doc(hidden)]
    #[inline]
    fn roaring_u32_value(&self) -> Option<u32> {
        None
    }
}

#[macro_export]
macro_rules! never_roaring_filter {
    ($($ty:ty),* $(,)?) => {
        $(
            impl $crate::utils::SupportsRoaring for $ty {}
        )*
    };
}

never_roaring_filter!(
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

impl SupportsRoaring for u32 {
    #[inline]
    fn supports_roaring32(&self) -> bool {
        true
    }

    #[inline]
    fn roaring_u32_value(&self) -> Option<u32> {
        Some(*self)
    }
}

impl SupportsRoaring for i32 {
    #[inline]
    fn supports_roaring32(&self) -> bool {
        true
    }

    #[inline]
    fn roaring_u32_value(&self) -> Option<u32> {
        Some(*self as u32)
    }
}

impl<T> SupportsRoaring for Option<T> {}

#[macro_export]
macro_rules! never_roaring_filter_1 {
    ($($wrapper:ident),* $(,)?) => {
        $(
            impl<T> $crate::utils::SupportsRoaring for $wrapper<T> {}
        )*
    };
}

never_roaring_filter_1!(Vec, LeanVec, BSet);

#[macro_export]
macro_rules! delegate_supports_roaring {
    ($($wrapper:ident),* $(,)?) => {
        $(
            impl<T: $crate::utils::SupportsRoaring> $crate::utils::SupportsRoaring for $wrapper<T> {
                #[inline]
                fn supports_roaring32(&self) -> bool {
                    self.as_ref().supports_roaring32()
                }

                #[inline]
                fn into_roaring_u32(&self, min: &Self) -> Option<u32> {
                    self.as_ref().into_roaring_u32(min.as_ref())
                }

                #[inline]
                fn roaring_u32_value(&self) -> Option<u32> {
                    self.as_ref().roaring_u32_value()
                }
            }
        )*
    };
}

delegate_supports_roaring!(Box, Rc, Arc);

#[macro_export]
macro_rules! never_roaring_filter_tuples {
    ($($name:ident),+) => {
        impl<$($name),+> SupportsRoaring for ($($name,)+) {}
    };
}

never_roaring_filter_tuples!(A);
never_roaring_filter_tuples!(A, B);
never_roaring_filter_tuples!(A, B, C);
never_roaring_filter_tuples!(A, B, C, D);
never_roaring_filter_tuples!(A, B, C, D, E);
never_roaring_filter_tuples!(A, B, C, D, E, F);

impl<K, V> SupportsRoaring for BTreeMap<K, V> {}

#[cfg(test)]
mod test {
    use super::SupportsRoaring;
    use crate::utils::Tup1;

    #[test]
    fn supported_roaring_keys() {
        assert!(7u32.supports_roaring32());
        assert_eq!(7u32.into_roaring_u32(&0), Some(7));

        assert!((-7i32).supports_roaring32());
        assert_eq!((-7i32).into_roaring_u32(&0), Some((-7i32) as u32));

        assert!(Tup1(-7i32).supports_roaring32());
        assert_eq!(Tup1(-7i32).into_roaring_u32(&Tup1(0)), Some((-7i32) as u32));
    }

    #[test]
    fn unsupported_roaring_keys() {
        assert!(!"feldera".to_string().supports_roaring32());
        assert_eq!("feldera".to_string().into_roaring_u32(&String::new()), None);

        assert!(!11u64.supports_roaring32());
        assert_eq!(11u64.into_roaring_u32(&0), None);
    }
}
