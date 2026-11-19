//! Counting the retractions of a batch with times while it is built.

use crate::{DynZWeight, ZWeight, dynamic::WeightTrait};
use size_of::SizeOf;
use std::any::TypeId;

/// Estimates the records of a batch with times that a merge can remove once
/// its frontier allows, as a builder receives them.
///
/// See [`Batch::retraction_count`](crate::trace::Batch::retraction_count).
/// The builder passes each weight of a pair to [`Self::push`], in time order,
/// and calls [`Self::end_pair`] after the pair's last one.  Only [`ZWeight`]
/// weights count; for other weight types, both counts stay 0.
///
/// It does not branch on the weights, so weights of mixed signs cost no
/// mispredicted branches.
#[derive(Clone, Debug, Default, SizeOf)]
pub(crate) struct RetractionCounter {
    /// Sum of the current pair's weights so far.
    partial_sum: ZWeight,

    /// Smallest partial sum of the current pair's weights so far, or 0 if
    /// none is negative.
    min_partial_sum: ZWeight,

    /// Number of weights of the current pair so far.
    pair_tuples: u64,

    /// Number of (time, weight) tuples in the counted pairs so far.
    tuples: u64,

    /// Number of counted pairs so far.
    pairs: u64,
}

impl RetractionCounter {
    /// Adds a weight to the current pair.
    ///
    /// # Arguments
    ///
    /// * `weight` - the pair's weight at its next time, in time order.
    #[inline]
    pub fn push<R>(&mut self, weight: &R)
    where
        R: WeightTrait + ?Sized,
    {
        if TypeId::of::<R>() == TypeId::of::<DynZWeight>() {
            // SAFETY: `R` is `DynZWeight`, whose concrete type is `ZWeight`.
            let weight = unsafe { *weight.downcast::<ZWeight>() };
            self.partial_sum += weight;
            self.min_partial_sum = self.min_partial_sum.min(self.partial_sum);
            self.pair_tuples += 1;
        }
    }

    /// Ends the current pair and counts it if one of its partial sums is
    /// negative.
    #[inline]
    pub fn end_pair(&mut self) {
        let counted = u64::from(self.min_partial_sum < 0);
        self.tuples += counted * self.pair_tuples;
        self.pairs += counted;
        self.partial_sum = 0;
        self.min_partial_sum = 0;
        self.pair_tuples = 0;
    }

    /// Returns the number of (time, weight) tuples in the counted pairs.
    pub fn tuples(&self) -> u64 {
        self.tuples
    }

    /// Returns the number of counted pairs.
    pub fn pairs(&self) -> u64 {
        self.pairs
    }
}
