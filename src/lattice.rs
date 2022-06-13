//! Partially ordered elements with a least upper bound.
//!
//! All logical times in DBSP must implement the `Lattice` trait.

use timely::{
    order::PartialOrder,
    progress::{frontier::AntichainRef, Antichain},
};

/// A bounded partially ordered type supporting joins and meets.
pub trait Lattice: PartialOrder {
    /// The smallest element greater than or equal to both arguments.
    ///
    /// # Examples
    ///
    /// ```
    /// # use timely::PartialOrder;
    /// # use dbsp::time::Product;
    /// # use dbsp::lattice::Lattice;
    /// # fn main() {
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
    /// # use timely::PartialOrder;
    /// # use dbsp::time::Product;
    /// # use dbsp::lattice::Lattice;
    /// # fn main() {
    ///
    /// let mut time1 = Product::new(3, 7);
    /// let time2 = Product::new(4, 6);
    /// time1.join_assign(&time2);
    ///
    /// assert_eq!(time1, Product::new(4, 7));
    /// # }
    /// ```
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
    /// # use timely::PartialOrder;
    /// # use dbsp::time::Product;
    /// # use dbsp::lattice::Lattice;
    /// # fn main() {
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
    /// # use timely::PartialOrder;
    /// # use dbsp::time::Product;
    /// # use dbsp::lattice::Lattice;
    /// # fn main() {
    ///
    /// let mut time1 = Product::new(3, 7);
    /// let time2 = Product::new(4, 6);
    /// time1.meet_assign(&time2);
    ///
    /// assert_eq!(time1, Product::new(3, 6));
    /// # }
    /// ```
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
    /// # use timely::PartialOrder;
    /// # use dbsp::time::Product;
    /// # use dbsp::lattice::Lattice;
    /// # fn main() {
    ///
    /// use timely::progress::frontier::{Antichain, AntichainRef};
    ///
    /// let time = Product::new(3, 7);
    /// let mut advanced = Product::new(3, 7);
    /// let frontier = Antichain::from(vec![Product::new(4, 8), Product::new(5, 3)]);
    /// advanced.advance_by(frontier.borrow());
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
                ::std::cmp::max(*self, *other)
            }

            #[inline]
            fn meet(&self, other: &Self) -> Self {
                ::std::cmp::min(*self, *other)
            }
        }
    };
}

use std::time::Duration;

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

impl<T: Lattice + Clone> Lattice for Antichain<T> {
    fn join(&self, other: &Self) -> Self {
        let mut upper = Antichain::new();
        for time1 in self.elements().iter() {
            for time2 in other.elements().iter() {
                upper.insert(time1.join(time2));
            }
        }
        upper
    }

    fn meet(&self, other: &Self) -> Self {
        let mut upper = Antichain::new();
        for time1 in self.elements().iter() {
            upper.insert(time1.clone());
        }
        for time2 in other.elements().iter() {
            upper.insert(time2.clone());
        }
        upper
    }
}
