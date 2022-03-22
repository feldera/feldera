//! Trait to report object size as the number of entries.

use impl_trait_for_tuples::impl_for_tuples;
use std::rc::Rc;

/// Trait to report object size as the number of entries.
///
/// Scalars have size 1.  Container (e.g., vector, map) size is
/// the sum of sizes of its elements.  If elements have constant size,
/// container size can be efficiently measured by multiplying the
/// number of elements by the size of each element.
pub trait NumEntries {
    /// Returns the number of entries in `self`.
    fn num_entries(&self) -> usize;

    /// Returns `Some(n)` if `Self` has constant size or `None` otherwise.
    fn const_num_entries() -> Option<usize>;
}

/// Macro to implement [`NumEntries`] for a scalar type whose size is 1.
#[macro_export]
macro_rules! num_entries_scalar {
    ($type:ty) => {
        impl $crate::NumEntries for $type {
            fn num_entries(&self) -> usize {
                1
            }
            fn const_num_entries() -> Option<usize> {
                Some(1)
            }
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
    fn num_entries(&self) -> usize {
        1
    }
    fn const_num_entries() -> Option<usize> {
        Some(1)
    }
}

impl<T> NumEntries for Vec<T>
where
    T: NumEntries,
{
    fn num_entries(&self) -> usize {
        match T::const_num_entries() {
            None => {
                let mut res = 0;
                for x in self.iter() {
                    res += x.num_entries();
                }
                res
            }
            Some(n) => n * self.len(),
        }
    }
    fn const_num_entries() -> Option<usize> {
        None
    }
}

impl<T> NumEntries for Rc<T>
where
    T: NumEntries,
{
    fn num_entries(&self) -> usize {
        self.as_ref().num_entries()
    }
    fn const_num_entries() -> Option<usize> {
        T::const_num_entries()
    }
}
