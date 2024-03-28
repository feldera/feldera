use crate::{
    algebra::{Lattice, PartialOrder},
    dynamic::{DataTrait, WeightTrait},
    time::Timestamp,
    trace::{OrdKeyBatch, OrdValBatch},
    Scope,
};
use rkyv::{Archive, Deserialize, Serialize};
use size_of::SizeOf;
use std::fmt::{Debug, Display, Formatter};

/// A nested pair of timestamps, one outer and one inner.
#[derive(
    Copy,
    Clone,
    Hash,
    Eq,
    PartialEq,
    Default,
    Ord,
    PartialOrd,
    SizeOf,
    Archive,
    Serialize,
    Deserialize,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
#[archive(bound(
    archive = "TOuter: Archive, TInner: Archive, <TOuter as Archive>::Archived: Ord, <TInner as Archive>::Archived: Ord"
))]
#[archive(compare(PartialEq, PartialOrd))]
pub struct Product<TOuter, TInner> {
    /// Outer timestamp.
    pub outer: TOuter,
    /// Inner timestamp.
    pub inner: TInner,
}

impl<TOuter, TInner> Product<TOuter, TInner> {
    /// Creates a new product from outer and inner coordinates.
    pub fn new(outer: TOuter, inner: TInner) -> Product<TOuter, TInner> {
        Product { outer, inner }
    }
}

impl<T1: Lattice, T2: Lattice> Lattice for Product<T1, T2> {
    #[inline]
    fn join(&self, other: &Product<T1, T2>) -> Product<T1, T2> {
        Product {
            outer: self.outer.join(&other.outer),
            inner: self.inner.join(&other.inner),
        }
    }

    #[inline]
    fn meet(&self, other: &Product<T1, T2>) -> Product<T1, T2> {
        Product {
            outer: self.outer.meet(&other.outer),
            inner: self.inner.meet(&other.inner),
        }
    }
}

/// Debug implementation to avoid seeing fully qualified path names.
impl<TOuter: Debug, TInner: Debug> Debug for Product<TOuter, TInner> {
    fn fmt(&self, f: &mut Formatter) -> Result<(), std::fmt::Error> {
        f.write_str(&format!("({:?}, {:?})", self.outer, self.inner))
    }
}

impl<TOuter: Display, TInner: Display> Display for Product<TOuter, TInner> {
    fn fmt(&self, f: &mut Formatter) -> Result<(), std::fmt::Error> {
        f.write_str(&format!("({}, {})", self.outer, self.inner))
    }
}

impl<TOuter: PartialOrder, TInner: PartialOrder> PartialOrder for Product<TOuter, TInner> {
    #[inline(always)]
    fn less_equal(&self, other: &Self) -> bool {
        self.outer.less_equal(&other.outer) && self.inner.less_equal(&other.inner)
    }
}

impl<TOuter, TInner> Timestamp for Product<TOuter, TInner>
where
    TOuter: Timestamp,
    TInner: Timestamp,
{
    type Nested = Product<Self, u32>;

    type OrdValBatch<K: DataTrait + ?Sized, V: DataTrait + ?Sized, R: WeightTrait + ?Sized> =
        OrdValBatch<K, V, Self, R>;
    type OrdKeyBatch<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> = OrdKeyBatch<K, Self, R>;

    fn minimum() -> Self {
        Self::new(TOuter::minimum(), TInner::minimum())
    }

    fn clock_start() -> Self {
        Self::new(TOuter::clock_start(), TInner::clock_start())
    }

    fn advance(&self, scope: Scope) -> Self {
        if scope == 0 {
            Self::new(self.outer.clone(), self.inner.advance(0))
        } else {
            Self::new(self.outer.advance(scope - 1), TInner::minimum())
        }
    }

    fn recede(&self, scope: Scope) -> Self {
        if scope == 0 {
            Self::new(self.outer.clone(), self.inner.recede(0))
        } else {
            Self::new(self.outer.recede(scope - 1), self.inner.clone())
        }
    }

    fn checked_recede(&self, scope: Scope) -> Option<Self> {
        if scope == 0 {
            self.inner
                .checked_recede(0)
                .map(|inner| Self::new(self.outer.clone(), inner))
        } else {
            self.outer
                .checked_recede(scope - 1)
                .map(|outer| Self::new(outer, self.inner.clone()))
        }
    }

    fn epoch_start(&self, scope: Scope) -> Self {
        if scope == 0 {
            Self::new(self.outer.clone(), TInner::minimum())
        } else {
            Self::new(self.outer.epoch_start(scope - 1), TInner::minimum())
        }
    }

    fn epoch_end(&self, scope: Scope) -> Self {
        if scope == 0 {
            Self::new(self.outer.clone(), self.inner.epoch_end(0))
        } else {
            Self::new(self.outer.epoch_end(scope - 1), self.inner.epoch_end(0))
        }
    }
}
