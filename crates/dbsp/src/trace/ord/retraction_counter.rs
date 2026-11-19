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
#[derive(Clone, Debug, Default, SizeOf)]
pub(crate) struct RetractionCounter {
    /// Sum of the current pair's weights so far.  It saturates instead of
    /// overflowing: the count is only an estimate.
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
            self.partial_sum = self.partial_sum.saturating_add(weight);
            // `min` rather than an `if`: whether a weight is negative varies
            // from tuple to tuple, so the processor would often mispredict a
            // conditional jump on it, and each misprediction costs more than
            // this arithmetic.
            self.min_partial_sum = self.min_partial_sum.min(self.partial_sum);
            self.pair_tuples += 1;
        }
    }

    /// Ends the current pair and counts it if one of its partial sums is
    /// negative.
    #[inline]
    pub fn end_pair(&mut self) {
        // 0 or 1, so that counting needs no conditional jump either.
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

#[cfg(test)]
mod test {
    use super::RetractionCounter;
    use crate::{
        DynZWeight, ZWeight,
        dynamic::{DynWeight, Erase},
    };

    /// Returns a counter fed with `pairs`, each the weights of one pair in
    /// time order.
    fn count(pairs: &[&[ZWeight]]) -> RetractionCounter {
        let mut counter = RetractionCounter::default();
        for weights in pairs {
            for weight in *weights {
                counter.push::<DynZWeight>(weight.erase());
            }
            counter.end_pair();
        }
        counter
    }

    /// A pair counts, with every one of its tuples, if one of its partial
    /// sums is negative, whatever its total.
    #[test]
    fn a_negative_partial_sum_counts_the_pair() {
        // Partial sums 1, 0: not counted.  -1, 0: counted.  2, 1, -1:
        // counted.  -3: counted.
        let counter = count(&[&[1, -1], &[-1, 1], &[2, -1, -2], &[-3]]);
        assert_eq!(counter.pairs(), 3);
        assert_eq!(counter.tuples(), 6);
    }

    /// Weights whose sum overflows `ZWeight` saturate the partial sum
    /// instead of panicking or wrapping around.
    #[test]
    fn an_overflowing_sum_saturates() {
        // Partial sums MAX, MAX, MAX - 1: not counted, where wrapping around
        // would make the second one negative.  MIN, MIN, MIN + 1: counted.
        let counter = count(&[&[ZWeight::MAX, ZWeight::MAX, -1], &[ZWeight::MIN, -1, 1]]);
        assert_eq!(counter.pairs(), 1);
        assert_eq!(counter.tuples(), 3);
    }

    /// Weights of other types count nothing.
    #[test]
    fn other_weight_types_count_nothing() {
        let mut counter = RetractionCounter::default();
        let weight: ZWeight = -1;
        counter.push::<DynWeight>(weight.erase());
        counter.end_pair();
        assert_eq!(counter.pairs(), 0);
        assert_eq!(counter.tuples(), 0);
    }
}
