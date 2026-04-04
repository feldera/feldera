//! Trait for key types that can be mapped into a roaring bitmap domain.

use crate::dynamic::{BSet, DowncastTrait, DynData, LeanVec};
use crate::time::UnitTimestamp;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::Arc;
use uuid::Uuid;

/// Key types that can be mapped into a `u32` domain of a roaring bitmap
/// while inside of a batch.
///
/// A roaring bitmap stores `u32` values. To use one as a batch filter,
/// each key must be translated into a `u32` offset relative to the minimum key
/// in the batch. Implementors define how (and whether) this translation is
/// possible for a given key type.
pub trait SupportsRoaring {
    /// Returns `true` if this key type can be represented in a 32-bit roaring
    /// bitmap.
    #[inline]
    fn supports_roaring32(&self) -> bool {
        false
    }

    /// Computes the `u32` offset of `self` relative to `min`.
    ///
    /// Returns `Some(offset)` when `self >= min` and the difference fits in a
    /// `u32`; returns `None` otherwise.
    #[inline]
    fn roaring_u32_offset(&self, _min: &Self) -> Option<u32>
    where
        Self: Sized,
    {
        None
    }

    /// Like [`roaring_u32_offset`](Self::roaring_u32_offset), but accepts `min`
    /// as a type-erased [`DynData`] reference.
    #[inline]
    fn roaring_u32_offset_dyn(&self, _min: &DynData) -> Option<u32> {
        None
    }

    /// Like [`roaring_u32_offset_dyn`](Self::roaring_u32_offset_dyn), but
    /// panics when the offset cannot be computed.
    #[inline]
    fn roaring_u32_offset_dyn_checked(&self, min: &DynData) -> u32 {
        self.roaring_u32_offset_dyn(min)
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

// Types excluded from roaring: non-numeric, floating-point (no meaningful
// integer offset), or platform-dependent width (usize/isize).
never_roaring_filter!(
    (),
    bool,
    char,
    f32,
    f64,
    usize,
    isize,
    String,
    UnitTimestamp,
    Uuid
);

impl SupportsRoaring for u8 {
    #[inline]
    fn supports_roaring32(&self) -> bool {
        true
    }

    #[inline]
    fn roaring_u32_offset(&self, min: &Self) -> Option<u32> {
        self.checked_sub(*min).map(u32::from)
    }

    #[inline]
    fn roaring_u32_offset_dyn(&self, min: &DynData) -> Option<u32> {
        // Safety: the caller guarantees `min` wraps `u8`.
        self.roaring_u32_offset(unsafe { min.downcast::<u8>() })
    }
}

impl SupportsRoaring for i8 {
    #[inline]
    fn supports_roaring32(&self) -> bool {
        true
    }

    #[inline]
    fn roaring_u32_offset(&self, min: &Self) -> Option<u32> {
        (*self >= *min).then_some(u32::from(self.abs_diff(*min)))
    }

    #[inline]
    fn roaring_u32_offset_dyn(&self, min: &DynData) -> Option<u32> {
        // Safety: the caller guarantees `min` wraps `i8`.
        self.roaring_u32_offset(unsafe { min.downcast::<i8>() })
    }
}

impl SupportsRoaring for u16 {
    #[inline]
    fn supports_roaring32(&self) -> bool {
        true
    }

    #[inline]
    fn roaring_u32_offset(&self, min: &Self) -> Option<u32> {
        self.checked_sub(*min).map(u32::from)
    }

    #[inline]
    fn roaring_u32_offset_dyn(&self, min: &DynData) -> Option<u32> {
        // Safety: the caller guarantees `min` wraps `u16`.
        self.roaring_u32_offset(unsafe { min.downcast::<u16>() })
    }
}

impl SupportsRoaring for i16 {
    #[inline]
    fn supports_roaring32(&self) -> bool {
        true
    }

    #[inline]
    fn roaring_u32_offset(&self, min: &Self) -> Option<u32> {
        (*self >= *min).then_some(u32::from(self.abs_diff(*min)))
    }

    #[inline]
    fn roaring_u32_offset_dyn(&self, min: &DynData) -> Option<u32> {
        // Safety: the caller guarantees `min` wraps `i16`.
        self.roaring_u32_offset(unsafe { min.downcast::<i16>() })
    }
}

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
    fn roaring_u32_offset_dyn(&self, min: &DynData) -> Option<u32> {
        // Safety: the caller guarantees `min` wraps `u32`.
        self.roaring_u32_offset(unsafe { min.downcast::<u32>() })
    }
}

impl SupportsRoaring for i32 {
    #[inline]
    fn supports_roaring32(&self) -> bool {
        true
    }

    #[inline]
    fn roaring_u32_offset(&self, min: &Self) -> Option<u32> {
        (*self >= *min).then_some(self.abs_diff(*min))
    }

    #[inline]
    fn roaring_u32_offset_dyn(&self, min: &DynData) -> Option<u32> {
        // Safety: the caller guarantees `min` wraps `i32`.
        self.roaring_u32_offset(unsafe { min.downcast::<i32>() })
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
            .and_then(|diff| u32::try_from(diff).ok())
    }

    #[inline]
    fn roaring_u32_offset_dyn(&self, min: &DynData) -> Option<u32> {
        // Safety: the caller guarantees `min` wraps `u64`.
        self.roaring_u32_offset(unsafe { min.downcast::<u64>() })
    }
}

impl SupportsRoaring for i64 {
    #[inline]
    fn supports_roaring32(&self) -> bool {
        true
    }

    #[inline]
    fn roaring_u32_offset(&self, min: &Self) -> Option<u32> {
        (*self >= *min)
            .then_some(self.abs_diff(*min))
            .and_then(|diff| diff.try_into().ok())
    }

    #[inline]
    fn roaring_u32_offset_dyn(&self, min: &DynData) -> Option<u32> {
        // Safety: the caller guarantees `min` wraps `i64`.
        self.roaring_u32_offset(unsafe { min.downcast::<i64>() })
    }
}

impl SupportsRoaring for u128 {
    #[inline]
    fn supports_roaring32(&self) -> bool {
        true
    }

    #[inline]
    fn roaring_u32_offset(&self, min: &Self) -> Option<u32> {
        self.checked_sub(*min)
            .and_then(|diff| u32::try_from(diff).ok())
    }

    #[inline]
    fn roaring_u32_offset_dyn(&self, min: &DynData) -> Option<u32> {
        // Safety: the caller guarantees `min` wraps `u128`.
        self.roaring_u32_offset(unsafe { min.downcast::<u128>() })
    }
}

impl SupportsRoaring for i128 {
    #[inline]
    fn supports_roaring32(&self) -> bool {
        true
    }

    #[inline]
    fn roaring_u32_offset(&self, min: &Self) -> Option<u32> {
        (*self >= *min)
            .then_some(self.abs_diff(*min))
            .and_then(|diff| diff.try_into().ok())
    }

    #[inline]
    fn roaring_u32_offset_dyn(&self, min: &DynData) -> Option<u32> {
        // Safety: the caller guarantees `min` wraps `i128`.
        self.roaring_u32_offset(unsafe { min.downcast::<i128>() })
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
                fn roaring_u32_offset_dyn(&self, min: &$crate::dynamic::DynData) -> Option<u32> {
                    self.as_ref().roaring_u32_offset_dyn(min)
                }

                #[inline]
                fn roaring_u32_offset_dyn_checked(&self, min: &$crate::dynamic::DynData) -> u32 {
                    self.as_ref().roaring_u32_offset_dyn_checked(min)
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
    fn roaring_u32_offset_dyn(&self, min: &DynData) -> Option<u32> {
        // Safety: the caller guarantees `min` wraps `Tup1<T>`.
        self.roaring_u32_offset(unsafe { min.downcast::<Self>() })
    }

    #[inline]
    fn roaring_u32_offset_dyn_checked(&self, min: &DynData) -> u32 {
        // Safety: the caller guarantees `min` wraps `Tup1<T>`.
        self.roaring_u32_offset(unsafe { min.downcast::<Self>() })
            .expect("roaring-u32 filter was selected for a key outside the planned batch range")
    }
}

#[cfg(test)]
mod test {
    use super::SupportsRoaring;
    use crate::{dynamic::DynData, utils::Tup1};

    #[test]
    fn u32_roaring_offset_boundaries() {
        assert!(0u32.supports_roaring32());
        assert_eq!(0u32.roaring_u32_offset(&0), Some(0));
        assert_eq!(u32::MAX.roaring_u32_offset(&0), Some(u32::MAX));
        assert_eq!(5u32.roaring_u32_offset(&7), None);

        assert_eq!(7u32.roaring_u32_offset_dyn((&0u32) as &DynData), Some(7));
    }

    #[test]
    fn i32_roaring_offset_boundaries() {
        assert!(0i32.supports_roaring32());
        assert_eq!(i32::MIN.roaring_u32_offset(&i32::MIN), Some(0));
        assert_eq!(i32::MAX.roaring_u32_offset(&i32::MIN), Some(u32::MAX));
        assert_eq!((-7i32).roaring_u32_offset(&-10), Some(3));
        assert_eq!((-11i32).roaring_u32_offset(&-10), None);

        assert_eq!(
            (-7i32).roaring_u32_offset_dyn((&-10i32) as &DynData),
            Some(3)
        );
    }

    #[test]
    fn u64_roaring_offset_boundaries() {
        let min = (u64::from(u32::MAX) << 8) + 11;

        assert!(min.supports_roaring32());
        assert_eq!(min.roaring_u32_offset(&min), Some(0));
        assert_eq!(
            min.wrapping_add(u64::from(u32::MAX))
                .roaring_u32_offset(&min),
            Some(u32::MAX)
        );
        assert_eq!(
            min.wrapping_add(u64::from(u32::MAX) + 1)
                .roaring_u32_offset(&min),
            None
        );
        assert_eq!(11u64.roaring_u32_offset(&(u64::MAX - 1)), None);

        assert_eq!(11u64.roaring_u32_offset_dyn((&9u64) as &DynData), Some(2));
    }

    #[test]
    fn i64_roaring_offset_boundaries() {
        let min = -5i64;
        let max_fitting = min + i64::from(u32::MAX);

        assert!(min.supports_roaring32());
        assert_eq!(min.roaring_u32_offset(&min), Some(0));
        assert_eq!(max_fitting.roaring_u32_offset(&min), Some(u32::MAX));
        assert_eq!((max_fitting + 1).roaring_u32_offset(&min), None);
        assert_eq!((min - 1).roaring_u32_offset(&min), None);

        assert_eq!(
            (-2i64).roaring_u32_offset_dyn((&-5i64) as &DynData),
            Some(3)
        );
    }

    #[test]
    fn tup1_delegates_roaring_support() {
        assert!(Tup1(-7i32).supports_roaring32());
        assert_eq!(
            Tup1(-7i32).roaring_u32_offset_dyn((&Tup1(-10i32)) as &DynData),
            Some(3)
        );
    }

    #[test]
    fn u8_roaring_offset_boundaries() {
        assert!(0u8.supports_roaring32());
        assert_eq!(0u8.roaring_u32_offset(&0), Some(0));
        assert_eq!(u8::MAX.roaring_u32_offset(&0), Some(255));
        assert_eq!(5u8.roaring_u32_offset(&7), None);

        assert_eq!(7u8.roaring_u32_offset_dyn((&0u8) as &DynData), Some(7));
    }

    #[test]
    fn i8_roaring_offset_boundaries() {
        assert!(0i8.supports_roaring32());
        assert_eq!(i8::MIN.roaring_u32_offset(&i8::MIN), Some(0));
        assert_eq!(i8::MAX.roaring_u32_offset(&i8::MIN), Some(255));
        assert_eq!((-7i8).roaring_u32_offset(&-10), Some(3));
        assert_eq!((-11i8).roaring_u32_offset(&-10), None);

        assert_eq!((-7i8).roaring_u32_offset_dyn((&-10i8) as &DynData), Some(3));
    }

    #[test]
    fn u16_roaring_offset_boundaries() {
        assert!(0u16.supports_roaring32());
        assert_eq!(0u16.roaring_u32_offset(&0), Some(0));
        assert_eq!(u16::MAX.roaring_u32_offset(&0), Some(65535));
        assert_eq!(5u16.roaring_u32_offset(&7), None);

        assert_eq!(7u16.roaring_u32_offset_dyn((&0u16) as &DynData), Some(7));
    }

    #[test]
    fn i16_roaring_offset_boundaries() {
        assert!(0i16.supports_roaring32());
        assert_eq!(i16::MIN.roaring_u32_offset(&i16::MIN), Some(0));
        assert_eq!(i16::MAX.roaring_u32_offset(&i16::MIN), Some(65535));
        assert_eq!((-7i16).roaring_u32_offset(&-10), Some(3));
        assert_eq!((-11i16).roaring_u32_offset(&-10), None);

        assert_eq!(
            (-7i16).roaring_u32_offset_dyn((&-10i16) as &DynData),
            Some(3)
        );
    }

    #[test]
    fn u128_roaring_offset_boundaries() {
        let min = u128::from(u32::MAX) << 64;

        assert!(min.supports_roaring32());
        assert_eq!(min.roaring_u32_offset(&min), Some(0));
        assert_eq!(
            (min + u128::from(u32::MAX)).roaring_u32_offset(&min),
            Some(u32::MAX)
        );
        assert_eq!(
            (min + u128::from(u32::MAX) + 1).roaring_u32_offset(&min),
            None
        );
        assert_eq!(11u128.roaring_u32_offset(&(u128::MAX - 1)), None);

        assert_eq!(11u128.roaring_u32_offset_dyn((&9u128) as &DynData), Some(2));
    }

    #[test]
    fn i128_roaring_offset_boundaries() {
        let min = -5i128;
        let max_fitting = min + i128::from(u32::MAX);

        assert!(min.supports_roaring32());
        assert_eq!(min.roaring_u32_offset(&min), Some(0));
        assert_eq!(max_fitting.roaring_u32_offset(&min), Some(u32::MAX));
        assert_eq!((max_fitting + 1).roaring_u32_offset(&min), None);
        assert_eq!((min - 1).roaring_u32_offset(&min), None);

        assert_eq!(
            (-2i128).roaring_u32_offset_dyn((&-5i128) as &DynData),
            Some(3)
        );
    }

    #[test]
    fn unsupported_roaring_keys() {
        assert!(!"feldera".to_string().supports_roaring32());
        assert_eq!(
            "feldera"
                .to_string()
                .roaring_u32_offset_dyn((&String::new()) as &DynData),
            None
        );
    }
}
