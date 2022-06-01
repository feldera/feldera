use crate::{circuit::Scope, lattice::Lattice};
use deepsize_derive::DeepSizeOf;
use timely::{progress::PathSummary, PartialOrder};

/// Logical timestamp.
///
/// A DBSP circuit runs on a discrete logical clock that starts at 0
/// on the `clock_start` event and increments by 1 at each clock tick.
/// A nested circuit has a two-dimensional clock that consists of the
/// parent clock `c1 `and its local clock `c2`: `(c1, c2)`.  A second-level
/// nested circuit has a three-dimensional clock: `((c1, c2), c3)`, etc.
///
/// Every value produced by an operator is logically labelled by the
/// time when it was produced.  Depending on what we do with the data,
/// this time may or may not be stored explicitly.  In the former case,
/// it is often beneficial to use a lossy representation that stores just
/// enough timing information required by operators that compute on the
/// data.
///
/// Thus, while we can represent any DBSP timestamp as an array of
/// integers whose length is equal to the nesting depth of the circuit
/// and whose `i`th element is the local time of the circuit at the `i`th
/// nesting level, in practice we use several more compact "lossy"
/// representations.  For example, we use unit type `()` for untimed data,
/// where we only track values and not the time when each value was generated.
///
/// As another example, the [`NestedTimestamp32`] type represents nested
/// timestamp `(c1, c2)`, but only uses 1 bit for `c1` to distinguish data
/// generated in the latest clock epoch from all earlier epochs (which is
/// sufficient for all existing DBSP operators).
///
/// The `Timestamp` trait captures common functionality of any (lossy or
/// lossless) time representation.
///
/// # Example: Untimed data
///
/// The [`integrate_trace`](`crate::circuit::Stream::integrate_trace`) operator
/// computes the union of all Z-sets in its input stream generated during the
/// current clock epoch, i.e., since the last
/// [`clock_start`](`crate::circuit::operator_traits::Operator::clock_start`)
/// invocation.  Its consumers only need to know the current value of the
/// sum and not when each update was produced.  It therefore stores its output
/// in an untimed [`Trace`]([`crate::trace::Trace`]), i.e., a trace whose
/// timestamp type is `()`.
///
/// # Example: `NestedTimestamp32`.
///
/// [`join_trace`](`crate::circuit::Stream::join_trace`) and
/// [`distinct_trace`](`crate::circuit::Stream::distinct_trace`) methods compute
/// incremental versions
/// of `join` and `distinct` operators within nested scopes.  They need to know
/// the exact local time in the child circuit when each key-value tuple was
/// generated; however they do not require the exact parent clock value as long
/// as they can distinguish between values generated during the current epoch
/// (i.e., the current parent clock cycle) from older values.  They therefore
/// take advantage of the lossy timestamp representation implemented by the
/// `NestedTimestamp32` type.
// TODO: Eliminate timely dependency.
// TODO: Conversion to/from the most general time representation (`[usize]`).
// TODO: Model overflow by having `advance` return Option<Self>.
pub trait Timestamp: PartialOrder + Clone + Ord + PartialEq + Eq + 'static {
    fn minimum() -> Self;

    /// Advance the timestamp by one clock tick.
    ///
    /// Advance `self` by one clock tick by incrementing the clock at the
    /// specified nesting level by one, while resetting all clocks at deeper
    /// nesting levels to `0`. `scope` identifies the nesting level of the
    /// circuit whoce clock is ticking.  `0` refers to the innermost
    /// circuit.  `1` is its parent circuit, etc. Returns the most accurate
    /// approximation of the new timestamp value supported by this time
    /// representation.
    ///
    /// # Example
    ///
    /// Assume a two-dimensional time modeled as (parent time, child time)
    /// tuple.
    ///
    /// ```ignore
    /// assert_eq!((2, 3).advance(0), (2, 4));
    /// assert_eq!((2, 3).advance(1), (3, 0));
    /// ```
    fn advance(&self, scope: Scope) -> Self;

    /// Push the timestamp back by one clock tick.
    ///
    /// Push `self` back by one clock cycle by decrementing the clock at the
    /// specified nesting level by one.  `scope` identifies the nesting
    /// level of the circuit whoce clock is ticking. `0` refers to the
    /// innermost circuit.  `1` is its parent circuit, etc.
    ///
    /// While time does not normally flow backward in DBSP, it is sometimes
    /// useful to push values back in time in order to "forget" the
    /// distinction between multiple older timestamps. See
    /// [`Trace::recede_to`](`crate::trace::Trace::recede_to`) for more details.
    ///
    /// # Example
    ///
    /// Assume a two-dimensional time modeled as (parent time, child time)
    /// tuple.
    ///
    /// ```ignore
    /// assert_eq!((2, 3).recede(0), (2, 2));
    /// assert_eq!((2, 3).recede(1), (1, 3));
    /// ```
    fn recede(&self, scope: Scope) -> Self;
}

fn bool_followed_by(this: bool, other: bool) -> Option<bool> {
    if this && other {
        None
    } else {
        Some(this || other)
    }
}

/// Nested timestamp that allocates one bit for the parent clock and the
/// remaining 31 bits for the child clock.
///
/// This representation precisely captures the nested clock value, but only
/// distinguishes the latest parent clock cycle, or "epoch", (higher-order bit
/// set to `1`) from all previous epochs (higher order bit is `0`).
#[derive(Clone, DeepSizeOf, Default, Eq, PartialEq, Debug, Hash, PartialOrd, Ord)]
pub struct NestedTimestamp32(u32);

impl NestedTimestamp32 {
    #[inline]
    pub fn new(epoch: bool, inner: u32) -> Self {
        debug_assert_eq!(inner >> 31, 0);

        let epoch = if epoch { 0x80000000 } else { 0 };
        Self(epoch | inner)
    }

    #[inline]
    pub fn epoch(&self) -> bool {
        self.0 >> 31 == 1
    }

    #[inline]
    pub fn inner(&self) -> u32 {
        self.0 & 0x7fffffff
    }
}

impl PartialOrder for NestedTimestamp32 {
    #[inline]
    fn less_equal(&self, other: &Self) -> bool {
        self.epoch().le(&other.epoch()) && self.inner().less_equal(&other.inner())
    }
}

// TODO: We probably don't need this trait in DBSP.  Drop is as we eliminate
// the timely dataflow dependency.
impl PathSummary<NestedTimestamp32> for NestedTimestamp32 {
    #[inline]
    fn results_in(&self, src: &Self) -> Option<Self> {
        bool_followed_by(self.epoch(), src.epoch()).and_then(|epoch| {
            self.inner()
                .results_in(&src.inner())
                .map(|inner| Self::new(epoch, inner))
        })
    }
    #[inline]
    fn followed_by(&self, other: &Self) -> Option<Self> {
        bool_followed_by(self.epoch(), other.epoch()).and_then(|epoch| {
            self.inner()
                .followed_by(&other.inner())
                .map(|inner| Self::new(epoch, inner))
        })
    }
}

impl Timestamp for NestedTimestamp32 {
    fn minimum() -> Self {
        Self::new(false, 0)
    }

    fn advance(&self, scope: Scope) -> Self {
        if scope == 0 {
            if self.0 & 0x7fffffff == 0x7fffffff {
                panic!("NestedTimestamp32::advance timestamp overflow");
            }
            Self(self.0 + 1)
        } else {
            Self::new(true, 0)
        }
    }

    fn recede(&self, scope: Scope) -> Self {
        if scope == 0 {
            if self.0 & 0x7fffffff == 0 {
                panic!("NestedTimestamp32::recede timestamp underflow");
            }
            Self(self.0 - 1)
        } else if scope == 1 {
            Self(self.0 & 0x7fffffff)
        } else {
            self.clone()
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

impl Timestamp for () {
    fn minimum() -> Self {}
    fn advance(&self, _scope: Scope) -> Self {}
    fn recede(&self, _scope: Scope) -> Self {}
}
