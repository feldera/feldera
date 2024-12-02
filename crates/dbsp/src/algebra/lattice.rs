//! Partially ordered elements with a least upper bound.
//!
//! All logical times in DBSP must implement the `Lattice` trait.

use crate::{
    algebra::PartialOrder,
    time::{Antichain, AntichainRef},
};
use std::{
    cmp::{max, min},
    time::Duration,
};

/// A "lattice" is a partially ordered set in which every pair of elements has a
/// least upper bound, called a "join", and greatest lower bound, called a
/// "meet".  See <https://en.wikipedia.org/wiki/Lattice_(order)>.
///
/// DBSP uses lattices to work with [Timestamp]s, which are only [partially
/// ordered].
///
/// [Timestamp]: crate::time::Timestamp
/// [partially ordered]: crate::time#comparing-times
pub trait Lattice: PartialOrder {
    /// The smallest element greater than or equal to both arguments.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() {
    ///
    /// use dbsp::{
    ///     algebra::{Lattice, PartialOrder},
    ///     time::Product,
    /// };
    ///
    /// let time1 = Product::new(3, 7);
    /// let time2 = Product::new(4, 6);
    /// let join = time1.join(&time2);
    ///
    /// assert_eq!(join, Product::new(4, 7));
    /// # }
    /// ```
    fn join(&self, other: &Self) -> Self;

    /// Updates `self` to the smallest element greater than or equal to both
    /// arguments.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() {
    ///
    /// use dbsp::{
    ///     algebra::{Lattice, PartialOrder},
    ///     time::Product,
    /// };
    ///
    /// let mut time1 = Product::new(3, 7);
    /// let time2 = Product::new(4, 6);
    /// time1.join_assign(&time2);
    ///
    /// assert_eq!(time1, Product::new(4, 7));
    /// # }
    /// ```
    #[inline]
    fn join_assign(&mut self, other: &Self)
    where
        Self: Sized,
    {
        *self = self.join(other);
    }

    /// The largest element less than or equal to both arguments.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() {
    ///
    /// use dbsp::{
    ///     algebra::{Lattice, PartialOrder},
    ///     time::Product,
    /// };
    ///
    /// let time1 = Product::new(3, 7);
    /// let time2 = Product::new(4, 6);
    /// let meet = time1.meet(&time2);
    ///
    /// assert_eq!(meet, Product::new(3, 6));
    /// # }
    /// ```
    fn meet(&self, other: &Self) -> Self;

    /// Updates `self` to the largest element less than or equal to both
    /// arguments.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() {
    ///
    /// use dbsp::{
    ///     algebra::{Lattice, PartialOrder},
    ///     time::Product,
    /// };
    ///
    /// let mut time1 = Product::new(3, 7);
    /// let time2 = Product::new(4, 6);
    /// time1.meet_assign(&time2);
    ///
    /// assert_eq!(time1, Product::new(3, 6));
    /// # }
    /// ```
    #[inline]
    fn meet_assign(&mut self, other: &Self)
    where
        Self: Sized,
    {
        *self = self.meet(other);
    }

    /// Advances self to the largest time indistinguishable under `frontier`.
    ///
    /// This method produces the "largest" lattice element with the property
    /// that for every lattice element greater than some element of
    /// `frontier`, both the result and `self` compare identically to the
    /// lattice element. The result is the "largest" element in
    /// the sense that any other element with the same property (compares
    /// identically to times greater or equal to `frontier`) must be less or
    /// equal to the result.
    ///
    /// When provided an empty frontier `self` is not modified.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() {
    ///
    /// use dbsp::{
    ///     algebra::{Lattice, PartialOrder},
    ///     time::{Antichain, AntichainRef, Product},
    /// };
    ///
    /// let time = Product::new(3, 7);
    /// let mut advanced = Product::new(3, 7);
    /// let frontier = Antichain::from(vec![Product::new(4, 8), Product::new(5, 3)]);
    /// advanced.advance_by(frontier.as_ref());
    ///
    /// // `time` and `advanced` are indistinguishable to elements >= an element of `frontier`
    /// for i in 0..10 {
    ///     for j in 0..10 {
    ///         let test = Product::new(i, j);
    ///         // for `test` in the future of `frontier` ..
    ///         if frontier.less_equal(&test) {
    ///             assert_eq!(time.less_equal(&test), advanced.less_equal(&test));
    ///         }
    ///     }
    /// }
    ///
    /// assert_eq!(advanced, Product::new(4, 7));
    /// # }
    /// ```
    #[inline]
    fn advance_by(&mut self, frontier: AntichainRef<Self>)
    where
        Self: Sized,
    {
        let mut iter = frontier.iter();
        if let Some(first) = iter.next() {
            let mut result = self.join(first);
            for f in iter {
                result.meet_assign(&self.join(f));
            }
            *self = result;
        }
    }
}

macro_rules! implement_lattice {
    ($index_type:ty, $minimum:expr) => {
        impl Lattice for $index_type {
            #[inline]
            fn join(&self, other: &Self) -> Self {
                max(*self, *other)
            }

            #[inline]
            fn meet(&self, other: &Self) -> Self {
                min(*self, *other)
            }
        }
    };
}

implement_lattice!(Duration, Duration::new(0, 0));
implement_lattice!(usize, 0);
implement_lattice!(u128, 0);
implement_lattice!(u64, 0);
implement_lattice!(u32, 0);
implement_lattice!(u16, 0);
implement_lattice!(u8, 0);
implement_lattice!(isize, 0);
implement_lattice!(i128, 0);
implement_lattice!(i64, 0);
implement_lattice!(i32, 0);
implement_lattice!(i16, 0);
implement_lattice!(i8, 0);
implement_lattice!((), ());

// TODO: Manually implement `.join_assign()`, `.meet_assign()` and
// `.advance_by()` to reuse buffers
impl<T> Lattice for Antichain<T>
where
    T: Lattice + Clone,
{
    #[inline]
    fn join(&self, other: &Self) -> Self {
        let mut upper = Antichain::new();
        for time1 in self {
            for time2 in other {
                upper.insert(time1.join(time2));
            }
        }

        upper
    }

    #[inline]
    fn meet(&self, other: &Self) -> Self {
        let mut upper = Antichain::new();
        for time1 in self {
            upper.insert(time1.clone());
        }

        for time2 in other {
            upper.insert(time2.clone());
        }

        upper
    }
}

impl<T> AntichainRef<'_, T>
where
    T: Lattice + Clone,
{
    #[inline]
    pub fn join(self, other: Self) -> Antichain<T> {
        let mut upper = Antichain::new();
        for time1 in self {
            for time2 in other {
                upper.insert(time1.join(time2));
            }
        }

        upper
    }

    #[inline]
    pub fn meet(self, other: Self) -> Antichain<T> {
        let mut upper = Antichain::new();
        for time1 in self {
            upper.insert(time1.clone());
        }

        for time2 in other {
            upper.insert(time2.clone());
        }

        upper
    }
}
