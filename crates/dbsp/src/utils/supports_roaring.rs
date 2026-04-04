//! Trait for key types that can be mapped into a roaring bitmap domain.

use crate::dynamic::{BSet, DowncastTrait, DynData, LeanVec};
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
    fn roaring_u32_offset(&self, _min: &Self) -> Option<u32>
    where
        Self: Sized,
    {
        None
    }

    #[inline]
    fn into_roaring_u32(&self, _min: &DynData) -> Option<u32> {
        None
    }

    #[inline]
    fn into_roaring_u32_checked(&self, min: &DynData) -> u32 {
        self.into_roaring_u32(min)
            .expect("roaring-u32 filter was selected for a key outside the planned batch range")
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
    i128,
    u8,
    u16,
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
    fn roaring_u32_offset(&self, min: &Self) -> Option<u32> {
        self.checked_sub(*min)
    }

    #[inline]
    fn into_roaring_u32(&self, min: &DynData) -> Option<u32> {
        self.roaring_u32_offset(min.downcast_checked::<u32>())
    }
}

impl SupportsRoaring for i32 {
    #[inline]
    fn supports_roaring32(&self) -> bool {
        true
    }

    #[inline]
    fn roaring_u32_offset(&self, min: &Self) -> Option<u32> {
        let diff = i64::from(*self) - i64::from(*min);
        (0..=i64::from(u32::MAX))
            .contains(&diff)
            .then_some(diff as u32)
    }

    #[inline]
    fn into_roaring_u32(&self, min: &DynData) -> Option<u32> {
        self.roaring_u32_offset(min.downcast_checked::<i32>())
    }
}

impl SupportsRoaring for u64 {
    #[inline]
    fn supports_roaring32(&self) -> bool {
        true
    }

    #[inline]
    fn roaring_u32_offset(&self, min: &Self) -> Option<u32> {
        self.checked_sub(*min)
            .filter(|diff| *diff <= u64::from(u32::MAX))
            .map(|diff| diff as u32)
    }

    #[inline]
    fn into_roaring_u32(&self, min: &DynData) -> Option<u32> {
        self.roaring_u32_offset(min.downcast_checked::<u64>())
    }
}

impl SupportsRoaring for i64 {
    #[inline]
    fn supports_roaring32(&self) -> bool {
        true
    }

    #[inline]
    fn roaring_u32_offset(&self, min: &Self) -> Option<u32> {
        let diff = i128::from(*self) - i128::from(*min);
        (0..=i128::from(u32::MAX))
            .contains(&diff)
            .then_some(diff as u32)
    }

    #[inline]
    fn into_roaring_u32(&self, min: &DynData) -> Option<u32> {
        self.roaring_u32_offset(min.downcast_checked::<i64>())
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
                fn roaring_u32_offset(&self, min: &Self) -> Option<u32> {
                    self.as_ref().roaring_u32_offset(min.as_ref())
                }

                #[inline]
                fn into_roaring_u32(&self, min: &$crate::dynamic::DynData) -> Option<u32> {
                    self.as_ref().into_roaring_u32(min)
                }

                #[inline]
                fn into_roaring_u32_checked(&self, min: &$crate::dynamic::DynData) -> u32 {
                    self.as_ref().into_roaring_u32_checked(min)
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

impl<T: SupportsRoaring + 'static> SupportsRoaring for crate::utils::Tup1<T> {
    #[inline]
    fn supports_roaring32(&self) -> bool {
        self.0.supports_roaring32()
    }

    #[inline]
    fn roaring_u32_offset(&self, min: &Self) -> Option<u32> {
        self.0.roaring_u32_offset(&min.0)
    }

    #[inline]
    fn into_roaring_u32(&self, min: &DynData) -> Option<u32> {
        self.roaring_u32_offset(min.downcast_checked::<Self>())
    }

    #[inline]
    fn into_roaring_u32_checked(&self, min: &DynData) -> u32 {
        self.roaring_u32_offset(min.downcast_checked::<Self>())
            .expect("roaring-u32 filter was selected for a key outside the planned batch range")
    }
}

#[cfg(test)]
mod test {
    use super::SupportsRoaring;
    use crate::{dynamic::DynData, utils::Tup1};

    #[test]
    fn supported_roaring_keys() {
        assert!(7u32.supports_roaring32());
        assert_eq!(7u32.into_roaring_u32((&0u32) as &DynData), Some(7));

        assert!((-7i32).supports_roaring32());
        assert_eq!((-7i32).into_roaring_u32((&-10i32) as &DynData), Some(3));

        assert!(Tup1(-7i32).supports_roaring32());
        assert_eq!(
            Tup1(-7i32).into_roaring_u32((&Tup1(-10i32)) as &DynData),
            Some(3)
        );

        assert!(11u64.supports_roaring32());
        assert_eq!(11u64.into_roaring_u32((&9u64) as &DynData), Some(2));

        assert!((-2i64).supports_roaring32());
        assert_eq!((-2i64).into_roaring_u32((&-5i64) as &DynData), Some(3));
    }

    #[test]
    fn unsupported_roaring_keys() {
        assert!(!"feldera".to_string().supports_roaring32());
        assert_eq!(
            "feldera"
                .to_string()
                .into_roaring_u32((&String::new()) as &DynData),
            None
        );

        assert_eq!(11u64.into_roaring_u32((&(u64::MAX - 1)) as &DynData), None);
        assert_eq!(5i64.into_roaring_u32((&10i64) as &DynData), None);
    }
}
