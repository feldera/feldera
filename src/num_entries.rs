//! Trait to report object size as the number of entries.

use impl_trait_for_tuples::impl_for_tuples;
use std::rc::Rc;

/// Trait to report object size as the number of entries.
pub trait NumEntries {
    /// Returns the number of entries in `self`.
    fn num_entries_shallow(&self) -> usize;

    /// Recursively computes the number of entries in a container by
    /// calling this method on each entry in `self`.
    ///
    /// Scalars have size 1.  Container (e.g., vector, map) size is
    /// the sum of sizes of its elements.  If elements have constant size,
    /// container size can be efficiently measured by multiplying the
    /// number of elements by the size of each element.
    fn num_entries_deep(&self) -> usize;

    /// Returns `Some(n)` if `Self` has constant size or `None` otherwise.
    const CONST_NUM_ENTRIES: Option<usize>;
}

/// Macro to implement [`NumEntries`] for a scalar type whose size is 1.
#[macro_export]
macro_rules! num_entries_scalar {
    ($type:ty) => {
        impl $crate::NumEntries for $type {
            #[inline]
            fn num_entries_shallow(&self) -> usize {
                1
            }

            #[inline]
            fn num_entries_deep(&self) -> usize {
                1
            }

            const CONST_NUM_ENTRIES: Option<usize> = Some(1);
        }
    };
}

num_entries_scalar!(u8);
num_entries_scalar!(u16);
num_entries_scalar!(u32);
num_entries_scalar!(u64);
num_entries_scalar!(u128);
num_entries_scalar!(usize);

num_entries_scalar!(i8);
num_entries_scalar!(i16);
num_entries_scalar!(i32);
num_entries_scalar!(i64);
num_entries_scalar!(i128);
num_entries_scalar!(isize);

num_entries_scalar!(String);
num_entries_scalar!(&'static str);

#[impl_for_tuples(12)]
impl NumEntries for Tuple {
    fn num_entries_shallow(&self) -> usize {
        1
    }
    fn num_entries_deep(&self) -> usize {
        1
    }

    const CONST_NUM_ENTRIES: Option<usize> = Some(1);
}

impl<T> NumEntries for Vec<T>
where
    T: NumEntries,
{
    fn num_entries_shallow(&self) -> usize {
        self.len()
    }
    fn num_entries_deep(&self) -> usize {
        match T::CONST_NUM_ENTRIES {
            None => self.iter().map(T::num_entries_deep).sum(),
            Some(n) => n * self.len(),
        }
    }

    const CONST_NUM_ENTRIES: Option<usize> = None;
}

impl<T> NumEntries for Rc<T>
where
    T: NumEntries,
{
    fn num_entries_shallow(&self) -> usize {
        self.as_ref().num_entries_shallow()
    }
    fn num_entries_deep(&self) -> usize {
        self.as_ref().num_entries_deep()
    }

    const CONST_NUM_ENTRIES: Option<usize> = T::CONST_NUM_ENTRIES;
}
