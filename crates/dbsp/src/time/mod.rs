//! # Logical time
//!
//! Every value produced by a DBSP operator is logically labelled by the time
//! when it was produced.  A logical time can be represented as an array of
//! integers whose length is equal to the nesting depth of the circuit and whose
//! `i`th element is the local time of the circuit at the `i`th nesting level.
//! A root circuit has a 1-dimensional logical clock, which starts at 0 and
//! increments by 1 at each clock tick, a nested circuit has a 2-dimensional
//! logical clock, and so on.
//!
//! The [`Timestamp`] trait captures common functionality of logical time
//! representation.
//!
//! # Lossy times
//!
//! In practice, if a logical time needs to be stored explicitly (which is not
//! always necessary), we use a more compact "lossy" representation of logical
//! time that stores just enough timing information for operators that compute
//! on the data.
//!
//! ## Example: Untimed data
//!
//! We use unit type `()` for untimed data, where we only track values and not
//! the time when each value was generated.
//!
//! The [`integrate_trace`](`crate::circuit::Stream::integrate_trace`) operator
//! computes the union of all Z-sets in its input stream generated during the
//! current clock epoch, i.e., since the last
//! [`clock_start`](`crate::circuit::operator_traits::Operator::clock_start`)
//! invocation.  Its consumers only need to know the current value of the
//! sum and not when each update was produced.  It therefore stores its output
//! in an untimed [`Trace`]([`crate::trace::Trace`]), i.e., a trace whose
//! timestamp type is `()`.
//!
//! # Comparing times
//!
//! Timestamps partially order logical time.  Multidimensional timestamps, in
//! particular, are not totally ordered: `(x1,x2) <= (y1,y2)` if and only if `x1
//! <= y1 && x2 <= y2`, so that, e.g. `(1,2)` and `(2,1)` are not comparable.
//! The [`PartialOrder`] trait that bounds `Timestamp` allows this logical time
//! ordering to be separate from the ordinary [`PartialOrd`] used for, e.g.,
//! sorting.

mod antichain;
mod product;

use crate::{
    algebra::{Lattice, PartialOrder},
    dynamic::{DataTrait, WeightTrait},
    trace::{Batch, FallbackIndexedWSet, FallbackKeyBatch, FallbackValBatch, FallbackWSet},
    DBData, Scope,
};
use rkyv::{Archive, Deserialize, Serialize};
use size_of::SizeOf;
use std::{fmt::Debug, hash::Hash};

use crate::dynamic::DynUnit;
pub use antichain::{Antichain, AntichainRef};
//pub use nested_ts32::NestedTimestamp32;
pub use product::Product;

/// Logical timestamp.
///
/// See [crate documentation](crate::time) for an overview of logical time in
/// DBSP.
// TODO: Conversion to/from the most general time representation (`[usize]`).
// TODO: Model overflow by having `advance` return Option<Self>.
pub trait Timestamp: DBData + PartialOrder + Lattice {
    type Nested: Timestamp;

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
    type ValBatch<K: DataTrait + ?Sized, V: DataTrait + ?Sized, R: WeightTrait + ?Sized>: Batch<Key = K, Val = V, Time = Self, R = R>
        + SizeOf
        + Send
        + Sync;

    type KeyBatch<K: DataTrait + ?Sized, R: WeightTrait + ?Sized>: Batch<Key = K, Val = DynUnit, Time = Self, R = R>
        + SizeOf;

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
    /// circuit whose clock is ticking.  `0` refers to the innermost
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
    /// level of the circuit whose clock is ticking. `0` refers to the
    /// innermost circuit.  `1` is its parent circuit, etc.
    ///
    /// # Panics
    ///
    /// Panics if the clock at the `scope` nesting level is equal to zero
    /// (i.e., we are running the first clock cycle) and hence cannot be
    /// decremented.
    fn recede(&self, scope: Scope) -> Self {
        self.checked_recede(scope).unwrap()
    }

    /// Like `recede`, but returns `None` if the clock at level `scope` is
    /// equal to zero (i.e., we are running the first clock cycle) and hence
    /// cannot be decremented.
    fn checked_recede(&self, scope: Scope) -> Option<Self>;

    /// Returns the first time stamp of the current clock epoch in `scope`.
    fn epoch_start(&self, scope: Scope) -> Self;

    /// Advance `self` to the end of the current clock epoch in `scope`.
    fn epoch_end(&self, scope: Scope) -> Self;
}

/// Zero-dimensional clock that doesn't need to count ticks.
///
/// This type is only used to bootstrap the recursive definition of the
/// `WithClock` trait.  You can otherwise use `()` as the type for an empty
/// timestamp.
#[derive(
    Clone, Debug, Hash, PartialOrd, Ord, PartialEq, Eq, SizeOf, Archive, Serialize, Deserialize,
)]
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd, Hash))]
#[archive(compare(PartialEq, PartialOrd))]
#[archive_attr(doc(hidden))]
pub struct UnitTimestamp;

impl Default for UnitTimestamp {
    fn default() -> Self {
        UnitTimestamp
    }
}

impl PartialOrder for UnitTimestamp {
    fn less_equal(&self, _other: &Self) -> bool {
        true
    }
}

impl Lattice for UnitTimestamp {
    fn join(&self, _other: &Self) -> Self {
        UnitTimestamp
    }

    fn meet(&self, _other: &Self) -> Self {
        UnitTimestamp
    }
}

impl Timestamp for UnitTimestamp {
    type Nested = ();

    type ValBatch<K: DataTrait + ?Sized, V: DataTrait + ?Sized, R: WeightTrait + ?Sized> =
        FallbackValBatch<K, V, Self, R>;
    type KeyBatch<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> = FallbackKeyBatch<K, Self, R>;
    fn minimum() -> Self {
        UnitTimestamp
    }
    fn advance(&self, _scope: Scope) -> Self {
        UnitTimestamp
    }
    fn recede(&self, _scope: Scope) -> Self {
        UnitTimestamp
    }
    fn checked_recede(&self, _scope: Scope) -> Option<Self> {
        None
    }
    fn epoch_start(&self, _scope: Scope) -> Self {
        UnitTimestamp
    }
    fn epoch_end(&self, _scope: Scope) -> Self {
        UnitTimestamp
    }
}

impl Timestamp for () {
    type Nested = Product<u32, u32>;

    type ValBatch<K: DataTrait + ?Sized, V: DataTrait + ?Sized, R: WeightTrait + ?Sized> =
        FallbackIndexedWSet<K, V, R>;
    type KeyBatch<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> = FallbackWSet<K, R>;

    fn minimum() -> Self {}
    fn advance(&self, _scope: Scope) -> Self {}
    fn recede(&self, _scope: Scope) -> Self {}
    fn checked_recede(&self, _scope: Scope) -> Option<Self> {
        Some(())
    }
    fn epoch_start(&self, _scope: Scope) -> Self {}
    fn epoch_end(&self, _scope: Scope) -> Self {}
}

impl Timestamp for u32 {
    type Nested = Product<u32, u32>;

    type ValBatch<K: DataTrait + ?Sized, V: DataTrait + ?Sized, R: WeightTrait + ?Sized> =
        FallbackValBatch<K, V, Self, R>;
    type KeyBatch<K: DataTrait + ?Sized, R: WeightTrait + ?Sized> = FallbackKeyBatch<K, Self, R>;

    fn minimum() -> Self {
        0
    }
    fn advance(&self, _scope: Scope) -> Self {
        self + 1
    }
    fn recede(&self, _scope: Scope) -> Self {
        self - 1
    }
    fn checked_recede(&self, _scope: Scope) -> Option<Self> {
        self.checked_sub(1)
    }
    fn epoch_start(&self, _scope: Scope) -> Self {
        0
    }
    fn epoch_end(&self, _scope: Scope) -> Self {
        Self::MAX
    }
}
