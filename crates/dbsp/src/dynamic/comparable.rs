use std::cmp::Ordering;

/// An object-safe verson of comparison traits.
///
/// Traits like `Eq` and `Ord` are not object-safe since they take
/// a reference to `Self` as the second argument.  This trait is an
/// object-safe (but runtime-unsafe) version of `Eq` and `Ord` , which
/// takes the second argument as a `* u8`.  It is meant to only be
/// used inside the `[derive_comparison_traits]` macro to derive
/// `Ord` and `Eq` for trait objects.
pub trait Comparable: 'static {
    /// Test if `self` and `other` are equal.
    ///
    /// # Safety
    ///
    /// `other` must be a valid reference to a value of type `Self`.
    unsafe fn equal(&self, other: *const u8) -> bool;

    /// Returns `true` iff `self < other`.
    ///
    /// # Safety
    ///
    /// `other` must be a valid reference to a value of type `Self`.
    unsafe fn less_than(&self, other: *const u8) -> bool;

    /// Compare `self` with `other`.
    ///
    /// # Safety
    ///
    /// `other` must be a valid reference to a value of type `Self`.
    unsafe fn compare(&self, other: *const u8) -> Ordering;
}

impl<T> Comparable for T
where
    T: Ord + Eq + 'static,
{
    #[inline]
    unsafe fn compare(&self, other: *const u8) -> Ordering {
        self.cmp(&*(other as *const Self))
    }

    #[inline]
    unsafe fn equal(&self, other: *const u8) -> bool {
        self.eq(&*(other as *const Self))
    }

    #[inline]
    unsafe fn less_than(&self, other: *const u8) -> bool {
        self.lt(&*(other as *const Self))
    }
}

/// Derive `PartialEq`, `Eq`, `PartialOrd`, `Ord` for trait objects that wrap
/// around concrete types that implement these traits.
// FIXME: This implementation is fundamentally unsafe, since it assumes that the
// second argument of `eq` and `cmp` points to the same types as `self`.  The
// type system doesn't guarantee this and the runtime check is only performed in
// debug mode.  This was done in the name of convenience, but is probably a bad
// choice in the hindsight.  We should introduce an unsafe version of comparison
// traits and implement those instead.
#[macro_export]
macro_rules! derive_comparison_traits {
    ($type_alias:ident <$($generic:ident),*>
    where
        $($bound:tt)*
    ) =>
    {
        impl<$($generic),*> PartialEq for $type_alias<$($generic),*>
        where
            $($bound)*
        {
            fn eq(&self, other: &Self) -> bool {
                debug_assert_eq!(std::any::Any::type_id(self.as_any()), std::any::Any::type_id(other.as_any()));
                unsafe{ self.equal(other as *const _ as *const u8) }
            }
        }

        impl<$($generic),*> Eq for $type_alias<$($generic),*>
        where
            $($bound)*
        {}

        impl<$($generic),*> PartialOrd for $type_alias<$($generic),*>
        where
            $($bound)*
        {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                Some(self.cmp(other))
            }
        }

        impl<$($generic),*> Ord for $type_alias<$($generic),*>
        where
            $($bound)*
        {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                debug_assert_eq!(std::any::Any::type_id(self.as_any()), std::any::Any::type_id(other.as_any()));

                unsafe{ self.compare(other as *const _  as *const u8) }
            }
        }
    };
}
