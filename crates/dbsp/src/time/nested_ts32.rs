use crate::{
    algebra::{Lattice, PartialOrder},
    dynamic::{DataTrait, WeightTrait},
    time::{FileValBatch, Product, Timestamp},
    trace::{FileKeyBatch, OrdKeyBatch, OrdValBatch},
    Scope,
};
use rkyv::{Archive, Deserialize, Serialize};
use size_of::SizeOf;
use std::fmt::{Debug, Display, Formatter};

const INNER_MASK: u32 = 0x7fffffff;
const EPOCH_MASK: u32 = 0x80000000;

/// Nested timestamp that allocates one bit for the parent clock and the
/// remaining 31 bits for the child clock.
///
/// This representation precisely captures the nested clock value, but only
/// distinguishes the latest parent clock cycle, or "epoch" (higher-order bit
/// set to `1`) from all previous epochs (higher order bit is `0`).
///
/// This is useful because, in DBSP, computations inside a nested circuit only
/// need to distinguish between updates added during the current run of the
/// nested circuit from those added all previous runs.  Thus, we only need a
/// single bit for the outer time stamp.  When the nested clock epoch completes,
/// all tuples with outer timestamp `1` are demoted to `0`, with [`recede_to`],
/// so they appear as old updates during the next run of the circuit.
///
/// The downside of the 1-bit clock is that it requires rewriting timestamps
/// and rearranging batches in the trace at the end of every clock epoch.
/// Unlike merging of batches, which can be done in the background, this
/// work must be completed synchronously before the start of the next
/// epoch. This cost should be roughly proportional to the number of
/// updates added to the trace during the last epoch.
///
/// [`recede_to`]: crate::trace::Trace::recede_to
#[derive(
    Clone,
    SizeOf,
    Default,
    Eq,
    PartialEq,
    Debug,
    Hash,
    PartialOrd,
    Ord,
    Archive,
    Serialize,
    Deserialize,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
#[archive(compare(PartialEq, PartialOrd))]
#[repr(transparent)]
pub struct NestedTimestamp32(u32);

#[cfg(test)]
impl proptest::arbitrary::Arbitrary for NestedTimestamp32 {
    type Parameters = ();
    type Strategy = proptest::prelude::BoxedStrategy<Self>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        use proptest::prelude::*;
        (any::<bool>(), 0u32..10)
            .prop_map(|(epoch, x)| NestedTimestamp32::new(epoch, x))
            .boxed()
    }
}

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
    type Nested = Product<Self, u32>;
    type MemValBatch<K: DataTrait + ?Sized, V: DataTrait + ?Sized, R: WeightTrait + ?Sized> =
        OrdValBatch<K, V, Self, R>;
    type MemKeyBatch<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> = OrdKeyBatch<K, Self, R>;

    type FileValBatch<K: DataTrait + ?Sized, V: DataTrait + ?Sized, R: WeightTrait + ?Sized> =
        FileValBatch<K, V, Self, R>;
    type FileKeyBatch<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> = FileKeyBatch<K, Self, R>;

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
        self.checked_recede(scope)
            .expect("NestedTimestamp32::recede timestamp underflow")
    }

    fn checked_recede(&self, scope: Scope) -> Option<Self> {
        if scope == 0 {
            if self.0 & INNER_MASK == 0 {
                None
            } else {
                Some(Self(self.0 - 1))
            }
        } else if scope == 1 {
            Some(Self(self.0 & INNER_MASK))
        } else {
            Some(self.clone())
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
