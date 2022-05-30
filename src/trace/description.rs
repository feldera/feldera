//! Descriptions of intervals of partially ordered times.
//!
//! A description provides a characterization of a batch of updates in terms of
//! upper and lower bounds on timestamps in the update: all updates in the batch
//! are greater or equal to some element of `lower` but not greater or equal to
//! any element of `upper`.

use timely::{progress::Antichain, PartialOrder};

/// Describes an interval of partially ordered times.
#[derive(Clone, Debug)]
pub struct Description<Time> {
    /// Lower frontier of contained updates.
    lower: Antichain<Time>,
    /// Upper frontier of contained updates.
    upper: Antichain<Time>,
}

impl<Time: PartialOrder + Clone> Description<Time> {
    /// Returns a new description from its component parts.
    pub fn new(lower: Antichain<Time>, upper: Antichain<Time>) -> Self {
        assert!(!lower.elements().is_empty()); // this should always be true.
                                               // assert!(upper.len() > 0);           // this may not always be true.
        Description { lower, upper }
    }
}

impl<Time> Description<Time> {
    /// The lower envelope for times in the interval.
    pub fn lower(&self) -> &Antichain<Time> {
        &self.lower
    }
    /// The upper envelope for times in the interval.
    pub fn upper(&self) -> &Antichain<Time> {
        &self.upper
    }
}

impl<Time: PartialEq> PartialEq for Description<Time> {
    fn eq(&self, other: &Self) -> bool {
        self.lower.eq(other.lower()) && self.upper.eq(other.upper())
    }
}

impl<Time: Eq> Eq for Description<Time> {}
