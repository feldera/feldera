//! Types that represent logical time in DBSP.

mod nested_ts32;
mod product;

use crate::{
    algebra::MonoidValue,
    circuit::Scope,
    lattice::Lattice,
    trace::{ord::OrdValBatch, Batch},
    OrdIndexedZSet,
};
use std::{fmt::Debug, hash::Hash};
use timely::PartialOrder;

use deepsize::DeepSizeOf;
pub use nested_ts32::NestedTimestamp32;
pub use product::Product;

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
/// [`join`](`crate::circuit::Stream::join`) and
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
pub trait Timestamp:
    PartialOrder + Lattice + Debug + Clone + Ord + PartialEq + Eq + Hash + 'static
{
    /// A default `Batch` type for batches using this timestamp.
    ///
    /// We sometimes need to instantiate a batch with the given key, value,
    /// timestamp, and weight types to store the intermediate result of a
    /// computation, and we don't want to bother the user with specifying the
    /// concrete batch type to use.  A reasonable default is one of
    /// `OrdValBatch` and `OrdIndexedZSet` depending on the timestamp type.
    /// The former works for all timestamps, while the latter is more
    /// compact and efficient, but is only applicable to batches with unit
    /// timestamps `()`.
    ///
    /// We automate this choice by making it an associated type of
    /// `trait Timestamp` -- not a very elegant solution, but I couldn't
    /// think of a better one.
    type OrdValBatch<
        K: Ord + Clone + DeepSizeOf + 'static,
        V: Ord + Clone + DeepSizeOf + 'static,
        R: MonoidValue + DeepSizeOf>
            : Batch<Key=K, Val=V, Time=Self, R=R> + DeepSizeOf;

    fn minimum() -> Self;

    /// The value of the timestamp when the clock starts ticking.
    ///
    /// This is typically but not always equal to `Self::minimum`.  For example,
    /// we use 1-bit timestamp that starts at value 1 (current clock epoch),
    /// value 0 (previous epochs) can only be obtained by calling
    /// [`recede`](`Timestamp::recede`).
    fn clock_start() -> Self {
        Self::minimum()
    }

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

    /// Returns the first time stamp of the current clock epoch in `scope`.
    fn epoch_start(&self, scope: Scope) -> Self;

    /// Advance `self` to the end of the current clock epoch in `scope`.
    fn epoch_end(&self, scope: Scope) -> Self;
}

impl Timestamp for () {
    type OrdValBatch<
        K: Ord + Clone + DeepSizeOf + 'static,
        V: Ord + Clone + DeepSizeOf + 'static,
        R: MonoidValue + DeepSizeOf,
    > = OrdIndexedZSet<K, V, R>;

    fn minimum() -> Self {}
    fn advance(&self, _scope: Scope) -> Self {}
    fn recede(&self, _scope: Scope) -> Self {}
    fn epoch_start(&self, _scope: Scope) -> Self {}
    fn epoch_end(&self, _scope: Scope) -> Self {}
}

impl Timestamp for u32 {
    type OrdValBatch<
        K: Ord + Clone + DeepSizeOf + 'static,
        V: Ord + Clone + DeepSizeOf + 'static,
        R: MonoidValue + DeepSizeOf,
    > = OrdValBatch<K, V, Self, R>;

    fn minimum() -> Self {
        0
    }
    fn advance(&self, _scope: Scope) -> Self {
        self + 1
    }
    fn recede(&self, _scope: Scope) -> Self {
        self - 1
    }
    fn epoch_start(&self, _scope: Scope) -> Self {
        0
    }
    fn epoch_end(&self, _scope: Scope) -> Self {
        Self::MAX
    }
}
