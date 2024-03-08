//! Trait to report object size as the number of entries.

use impl_trait_for_tuples::impl_for_tuples;
use std::{
    num::{
        NonZeroI128, NonZeroI16, NonZeroI32, NonZeroI64, NonZeroI8, NonZeroIsize, NonZeroU128,
        NonZeroU16, NonZeroU32, NonZeroU64, NonZeroU8, NonZeroUsize,
    },
    rc::Rc,
};

/// Trait to report object size as the number of entries.
pub trait NumEntries {
    /// Returns `Some(n)` if `Self` has constant size or `None` otherwise.
    const CONST_NUM_ENTRIES: Option<usize>;

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
}

impl<T> NumEntries for &T
where
    T: NumEntries,
{
    const CONST_NUM_ENTRIES: Option<usize> = T::CONST_NUM_ENTRIES;

    #[inline]
    fn num_entries_shallow(&self) -> usize {
        T::num_entries_shallow(self)
    }

    #[inline]
    fn num_entries_deep(&self) -> usize {
        T::num_entries_deep(self)
    }
}

/// Macro to implement [`NumEntries`] for a scalar type whose size is 1.
#[macro_export]
macro_rules! num_entries_scalar {
    ($($type:ty),+ $(,)?) => {
        $(
            impl $crate::NumEntries for $type {
                const CONST_NUM_ENTRIES: Option<usize> = Some(1);

                #[inline]
                fn num_entries_shallow(&self) -> usize {
                    1
                }

                #[inline]
                fn num_entries_deep(&self) -> usize {
                    1
                }
            }
        )+
    };
}

macro_rules! num_entries_scalar_test {
    ($($type:ty),+ $(,)?) => {
        #[test]
        fn check_scalar_entries() {
            $(assert_eq!(<$type>::CONST_NUM_ENTRIES, Some(1));)+
            $(assert_eq!(<$type>::default().num_entries_shallow(), 1);)+
            $(assert_eq!(<$type>::default().num_entries_deep(), 1);)+
        }

        #[test]
        fn check_scalar_entry_refs() {
            $(assert_eq!(<&$type>::CONST_NUM_ENTRIES, Some(1));)+
            $(assert_eq!(<&$type as $crate::NumEntries>::num_entries_shallow(&&<$type>::default()), 1);)+
            $(assert_eq!(<&$type as $crate::NumEntries>::num_entries_deep(&&<$type>::default()), 1);)+
        }

        #[test]
        fn check_scalar_entry_rc() {
            use std::rc::Rc;
            $(assert_eq!(<Rc<$type>>::CONST_NUM_ENTRIES, Some(1));)+
            $(assert_eq!(<Rc<$type> as $crate::NumEntries>::num_entries_shallow(&Rc::new(<$type>::default())), 1);)+
            $(assert_eq!(<Rc<$type> as $crate::NumEntries>::num_entries_deep(&Rc::new(<$type>::default())), 1);)+
        }
    };
}

num_entries_scalar! {
    u8,
    u16,
    u32,
    u64,
    u128,
    usize,
    i8,
    i16,
    i32,
    i64,
    i128,
    isize,

    NonZeroU8,
    NonZeroI8,
    NonZeroU16,
    NonZeroI16,
    NonZeroU32,
    NonZeroI32,
    NonZeroU64,
    NonZeroI64,
    NonZeroU128,
    NonZeroI128,
    NonZeroUsize,
    NonZeroIsize,

    String,
}

num_entries_scalar_test! {
    u8,
    u16,
    u32,
    u64,
    u128,
    usize,
    i8,
    i16,
    i32,
    i64,
    i128,
    isize,

    String,
}

// FIXME: This is incorrect, it doesn't take into account any entries
//        of the underlying types
#[impl_for_tuples(12)]
impl NumEntries for Tuple {
    const CONST_NUM_ENTRIES: Option<usize> = Some(1);

    #[inline]
    fn num_entries_shallow(&self) -> usize {
        1
    }

    #[inline]
    fn num_entries_deep(&self) -> usize {
        1
    }
}

impl NumEntries for str {
    const CONST_NUM_ENTRIES: Option<usize> = Some(1);

    #[inline]
    fn num_entries_shallow(&self) -> usize {
        1
    }

    #[inline]
    fn num_entries_deep(&self) -> usize {
        1
    }
}

impl<T> NumEntries for [T]
where
    T: NumEntries,
{
    const CONST_NUM_ENTRIES: Option<usize> = None;

    #[inline]
    fn num_entries_shallow(&self) -> usize {
        self.len()
    }

    #[inline]
    fn num_entries_deep(&self) -> usize {
        match T::CONST_NUM_ENTRIES {
            None => self.iter().map(T::num_entries_deep).sum(),
            Some(n) => n * self.len(),
        }
    }
}

impl<T> NumEntries for Vec<T>
where
    T: NumEntries,
{
    const CONST_NUM_ENTRIES: Option<usize> = None;

    #[inline]
    fn num_entries_shallow(&self) -> usize {
        self.len()
    }

    #[inline]
    fn num_entries_deep(&self) -> usize {
        match T::CONST_NUM_ENTRIES {
            None => self.iter().map(T::num_entries_deep).sum(),
            Some(n) => n * self.len(),
        }
    }
}

impl<const N: usize, T> NumEntries for [T; N]
where
    T: NumEntries,
{
    const CONST_NUM_ENTRIES: Option<usize> = match T::CONST_NUM_ENTRIES {
        Some(entries) => Some(entries * N),
        None => None,
    };

    #[inline]
    fn num_entries_shallow(&self) -> usize {
        N
    }

    #[inline]
    fn num_entries_deep(&self) -> usize {
        match T::CONST_NUM_ENTRIES {
            None => self.iter().map(T::num_entries_deep).sum(),
            Some(entries) => entries * N,
        }
    }
}

impl<T> NumEntries for Rc<T>
where
    T: NumEntries,
{
    const CONST_NUM_ENTRIES: Option<usize> = T::CONST_NUM_ENTRIES;

    #[inline]
    fn num_entries_shallow(&self) -> usize {
        self.as_ref().num_entries_shallow()
    }

    #[inline]
    fn num_entries_deep(&self) -> usize {
        self.as_ref().num_entries_deep()
    }
}

impl<T: ?Sized> NumEntries for Box<T>
where
    T: NumEntries,
{
    const CONST_NUM_ENTRIES: Option<usize> = T::CONST_NUM_ENTRIES;

    #[inline]
    fn num_entries_shallow(&self) -> usize {
        self.as_ref().num_entries_shallow()
    }

    #[inline]
    fn num_entries_deep(&self) -> usize {
        self.as_ref().num_entries_deep()
    }
}

impl<T> NumEntries for Option<T>
where
    T: NumEntries,
{
    const CONST_NUM_ENTRIES: Option<usize> = None;

    #[inline]
    fn num_entries_shallow(&self) -> usize {
        if let Some(inner) = self {
            inner.num_entries_shallow()
        } else {
            1
        }
    }

    #[inline]
    fn num_entries_deep(&self) -> usize {
        if let Some(inner) = self {
            inner.num_entries_shallow()
        } else {
            1
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::NumEntries;

    #[test]
    fn vec_entries() {
        let x: Vec<u8> = Vec::new();
        assert_eq!(x.num_entries_shallow(), 0);
        assert_eq!(x.num_entries_deep(), 0);

        let x: Vec<u8> = vec![0; 256];
        assert_eq!(x.num_entries_shallow(), 256);
        assert_eq!(x.num_entries_deep(), 256);

        let x: Vec<Vec<u8>> = vec![vec![0]; 256];
        assert_eq!(x.num_entries_shallow(), 256);
        assert_eq!(x.num_entries_deep(), 256);
    }

    #[test]
    fn slice_entries() {
        let x: Vec<u8> = Vec::new();
        assert_eq!(x.as_slice().num_entries_shallow(), 0);
        assert_eq!(x.as_slice().num_entries_deep(), 0);

        let x: Vec<u8> = vec![0; 256];
        assert_eq!(x.as_slice().num_entries_shallow(), 256);
        assert_eq!(x.as_slice().num_entries_deep(), 256);

        let x: Vec<Vec<u8>> = vec![vec![0]; 256];
        assert_eq!(x.as_slice().num_entries_shallow(), 256);
        assert_eq!(x.as_slice().num_entries_deep(), 256);
    }

    #[test]
    fn array_entries() {
        let x: [u8; 0] = [];
        assert_eq!(x.num_entries_shallow(), 0);
        assert_eq!(x.num_entries_deep(), 0);

        let x: [u8; 256] = [0; 256];
        assert_eq!(x.num_entries_shallow(), 256);
        assert_eq!(x.num_entries_deep(), 256);

        let x: [Vec<u8>; 256] = [(); 256].map(|_| vec![0]);
        assert_eq!(x.num_entries_shallow(), 256);
        assert_eq!(x.num_entries_deep(), 256);
    }

    #[test]
    fn str_entries() {
        let x = "";
        assert_eq!(x.num_entries_shallow(), 1);
        assert_eq!(x.num_entries_deep(), 1);

        let x = "sdgfdsgfggregw";
        assert_eq!(x.num_entries_shallow(), 1);
        assert_eq!(x.num_entries_deep(), 1);
    }
}
