use crate::{
    algebra::MonoidValue, circuit::Scope, lattice::Lattice, time::Timestamp,
    trace::ord::OrdValBatch,
};
use deepsize::DeepSizeOf;
use std::fmt::{Debug, Display, Formatter};
use timely::PartialOrder;

const INNER_MASK: u32 = 0x7fffffff;
const EPOCH_MASK: u32 = 0x80000000;

/// Nested timestamp that allocates one bit for the parent clock and the
/// remaining 31 bits for the child clock.
///
/// This representation precisely captures the nested clock value, but only
/// distinguishes the latest parent clock cycle, or "epoch", (higher-order bit
/// set to `1`) from all previous epochs (higher order bit is `0`).
#[derive(Clone, DeepSizeOf, Default, Eq, PartialEq, Debug, Hash, PartialOrd, Ord)]
pub struct NestedTimestamp32(u32);

impl Display for NestedTimestamp32 {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(
            f,
            "({}, {})",
            if self.epoch() { "new" } else { "old" },
            self.inner(),
        )
    }
}

impl NestedTimestamp32 {
    #[inline]
    pub fn new(epoch: bool, inner: u32) -> Self {
        debug_assert_eq!(inner >> 31, 0);

        let epoch = if epoch { EPOCH_MASK } else { 0 };
        Self(epoch | inner)
    }

    #[inline]
    pub fn epoch(&self) -> bool {
        self.0 >> 31 == 1
    }

    #[inline]
    pub fn inner(&self) -> u32 {
        self.0 & INNER_MASK
    }
}

impl PartialOrder for NestedTimestamp32 {
    #[inline]
    fn less_equal(&self, other: &Self) -> bool {
        self.epoch().le(&other.epoch()) && self.inner().less_equal(&other.inner())
    }
}

impl Timestamp for NestedTimestamp32 {
    type OrdValBatch<
        K: Ord + Clone + DeepSizeOf + 'static,
        V: Ord + Clone + DeepSizeOf + 'static,
        R: MonoidValue + DeepSizeOf,
    > = OrdValBatch<K, V, Self, R>;

    fn minimum() -> Self {
        Self::new(false, 0)
    }

    /// Start with epoch set to 1 (current epoch).
    fn clock_start() -> Self {
        Self::new(true, 0)
    }

    fn advance(&self, scope: Scope) -> Self {
        if scope == 0 {
            if self.0 & INNER_MASK == INNER_MASK {
                panic!("NestedTimestamp32::advance timestamp overflow");
            }
            Self(self.0 + 1)
        } else {
            Self::new(true, 0)
        }
    }

    fn recede(&self, scope: Scope) -> Self {
        if scope == 0 {
            if self.0 & INNER_MASK == 0 {
                panic!("NestedTimestamp32::recede timestamp underflow");
            }
            Self(self.0 - 1)
        } else if scope == 1 {
            Self(self.0 & INNER_MASK)
        } else {
            self.clone()
        }
    }

    fn epoch_start(&self, scope: Scope) -> Self {
        if scope == 0 {
            Self::new(self.epoch(), 0x0)
        } else if scope == 1 {
            Self::new(false, 0x0)
        } else {
            unreachable!()
        }
    }

    fn epoch_end(&self, scope: Scope) -> Self {
        if scope == 0 {
            Self::new(self.epoch(), INNER_MASK)
        } else if scope == 1 {
            Self::new(true, INNER_MASK)
        } else {
            unreachable!()
        }
    }
}

impl Lattice for NestedTimestamp32 {
    #[inline]
    fn join(&self, other: &NestedTimestamp32) -> NestedTimestamp32 {
        Self::new(
            self.epoch() || other.epoch(),
            self.inner().join(&other.inner()),
        )
    }
    #[inline]
    fn meet(&self, other: &NestedTimestamp32) -> NestedTimestamp32 {
        Self::new(
            self.epoch() && other.epoch(),
            self.inner().meet(&other.inner()),
        )
    }
}
