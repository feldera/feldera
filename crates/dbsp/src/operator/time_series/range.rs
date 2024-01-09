//! Data structures that specify contiguous time ranges.

use crate::trace::Cursor;
use num::PrimInt;
use std::{
    cmp::max,
    marker::PhantomData,
    ops::{Add, Neg, Sub},
};

/// Relative time offset.
///
/// Specifies relative time as an offset.  This is valuable for unsigned integer
/// type `TS` because it allows representing times in the past.
///
/// `RelOffset::Before(0)` and `RelOffset::After(0)` both represent the same
/// relative time.  This is also true for `RelOffset::Before(1)` and
/// `RelOffset::After(-1)`, but the former is preferred.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
pub enum RelOffset<TS> {
    Before(TS),
    After(TS),
}

impl<TS> Neg for RelOffset<TS> {
    type Output = Self;

    fn neg(self) -> Self {
        match self {
            Self::Before(ts) => Self::After(ts),
            Self::After(ts) => Self::Before(ts),
        }
    }
}

impl<TS> Add<Self> for RelOffset<TS>
where
    TS: PrimInt,
{
    type Output = Self;

    fn add(self, rhs: Self) -> Self {
        match (self, rhs) {
            (Self::Before(ts1), Self::Before(ts2)) => Self::Before(ts1.saturating_add(ts2)),
            (Self::After(ts1), Self::After(ts2)) => Self::After(ts1.saturating_add(ts2)),
            (Self::After(ts1), Self::Before(ts2)) => {
                if ts1 >= ts2 {
                    Self::After(ts1.saturating_sub(ts2))
                } else {
                    Self::Before(ts2.saturating_sub(ts1))
                }
            }
            (Self::Before(ts1), Self::After(ts2)) => {
                if ts1 >= ts2 {
                    Self::Before(ts1.saturating_sub(ts2))
                } else {
                    Self::After(ts2.saturating_sub(ts1))
                }
            }
        }
    }
}

impl<TS> Sub<Self> for RelOffset<TS>
where
    TS: PrimInt,
{
    type Output = Self;

    fn sub(self, rhs: Self) -> Self {
        self.add(rhs.neg())
    }
}

/// Relative time range.
///
/// Specifies a closed time interval relative to a given moment in time.
///
/// `RelRange::new(RelOffset::Before(0), RelOffset::Before(0))` spans 1 unit of
/// time; `RelRange::new(RelOffset::Before(2), RelOffset::Before(0))` spans 3
/// units:
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
pub struct RelRange<TS> {
    pub from: RelOffset<TS>,
    pub to: RelOffset<TS>,
}

impl<TS> RelRange<TS>
where
    TS: PrimInt,
{
    pub fn new(from: RelOffset<TS>, to: RelOffset<TS>) -> Self {
        Self { from, to }
    }

    /// Computes relative range of timestamp `ts`.
    ///
    /// Returns `None` if the range is completely outside the range of type
    /// `TS`.
    ///
    /// # Example
    ///
    /// Starting from 5, a relative range of `[-3,-1]` is the same as absolute
    /// range `[2,4]`:
    ///
    /// ```
    /// use dbsp::operator::time_series::{Range, RelOffset, RelRange};
    ///
    /// let rr = RelRange::new(RelOffset::Before(3), RelOffset::Before(1));
    /// assert_eq!(rr.range_of(&5), Some(Range::new(2, 4)));
    /// ```
    pub fn range_of(&self, ts: &TS) -> Option<Range<TS>> {
        let from = match self.from {
            RelOffset::Before(off) => ts.saturating_sub(off),
            RelOffset::After(off) => ts.checked_add(&off)?,
        };
        let to = match self.to {
            RelOffset::Before(off) => ts.checked_sub(&off)?,
            RelOffset::After(off) => ts.saturating_add(off),
        };

        Some(Range { from, to })
    }

    /// Returns a range containing all times `t` such that `ts ∈
    /// self.range_of(t)` or `None` if the range is completely outside
    /// the range of type `TS`.
    ///
    /// # Example
    ///
    /// If and only if `6 <= x <= 8`, starting from `x`, a relative range of
    /// `[-3,-1]` contains 5:
    ///
    /// ```
    /// use dbsp::operator::time_series::{Range, RelOffset, RelRange};
    ///
    /// let rr = RelRange::new(RelOffset::Before(3), RelOffset::Before(1));
    /// assert_eq!(rr.affected_range_of(&5), Some(Range::new(6, 8)));
    /// ```
    pub fn affected_range_of(&self, ts: &TS) -> Option<Range<TS>> {
        let from = match self.to {
            RelOffset::Before(off) => ts.checked_add(&off)?,
            RelOffset::After(off) => ts.saturating_sub(off),
        };
        let to = match self.from {
            RelOffset::Before(off) => ts.saturating_add(off),
            RelOffset::After(off) => ts.checked_sub(&off)?,
        };

        Some(Range::new(from, to))
    }
}

/// Absolute time range.
///
/// Specifies a time range containing all timestamps in the closed interval
/// `[from..to]`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Range<TS: PrimInt> {
    pub from: TS,
    pub to: TS,
}

impl<TS> Range<TS>
where
    TS: PrimInt,
{
    pub fn new(from: TS, to: TS) -> Self {
        debug_assert!(from <= to);

        Self { from, to }
    }
}

/// Multiple non-overlapping ordered time ranges.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ranges<TS: PrimInt>(Vec<Range<TS>>);

impl<TS> Ranges<TS>
where
    TS: PrimInt,
{
    pub fn new() -> Self {
        Self(Vec::new())
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self(Vec::with_capacity(capacity))
    }

    /// Returns the number of non-overlapping ranges in `self`.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns a reference to the range at the given index.
    ///
    /// Precondition: `idx < self.len()`.
    pub fn range(&self, idx: usize) -> &Range<TS> {
        &self.0[idx]
    }

    /// Merge two ordered sets of ranges.
    pub fn merge(&self, other: &Self) -> Self {
        let mut result = Self::with_capacity(self.len() + other.len());
        let mut i = 0;
        let mut j = 0;

        while i < self.len() && j < other.len() {
            if self.range(i).from <= other.range(j).from {
                result.push_monotonic(self.range(i).clone());
                i += 1;
            } else {
                result.push_monotonic(other.range(j).clone());
                j += 1;
            }
        }

        while i < self.len() {
            result.push_monotonic(self.range(i).clone());
            i += 1;
        }

        while j < other.len() {
            result.push_monotonic(other.range(j).clone());
            j += 1;
        }

        result
    }

    /// Add a range whose lower bound is greater than or equal than the
    /// lower bound of the last range in `self`.
    ///
    /// Precondition: `self.len() == 0 || range.from <=
    /// self.last.unwrap().from`.
    pub fn push_monotonic(&mut self, range: Range<TS>) {
        match self.0.last_mut() {
            Some(last) if last.to >= range.from => {
                debug_assert!(last.from <= range.from);
                last.to = max(last.to, range.to);
            }
            _ => self.0.push(range),
        }
    }
}

/// Cursor that restricts an underlying time series cursor to timestamps within
/// a given set of ranges.
///
/// Behaves as `cursor` with all keys outside of `ranges` removed.
pub struct RangeCursor<TS, V, R, C>
where
    TS: PrimInt,
{
    cursor: C,
    ranges: Ranges<TS>,
    current_range: usize,
    phantom: PhantomData<(V, R)>,
}

impl<TS, V, R, C> RangeCursor<TS, V, R, C>
where
    TS: PrimInt,
    C: Cursor<TS, V, (), R>,
{
    /// Create a new `RangeCursor` that restricts keys in `cursor` to `ranges`.
    pub fn new(cursor: C, ranges: Ranges<TS>) -> Self {
        let mut res = Self {
            cursor,
            ranges,
            current_range: 0,
            phantom: PhantomData,
        };

        res.advance();
        res
    }

    /// Helper: advance `self.cursor` to the nearest key within `self.ranges`.
    /// Leaves the cursor unmodified if the current key is within `self.ranges`.
    fn advance(&mut self) {
        while self.current_range < self.ranges.len() {
            let range = self.ranges.range(self.current_range);
            self.cursor.seek_key(&range.from);
            if !self.cursor.key_valid() {
                break;
            }

            if self.cursor.key() <= &range.to {
                break;
            } else {
                self.current_range += 1;
            }
        }
    }
}

impl<TS, V, R, C> Cursor<TS, V, (), R> for RangeCursor<TS, V, R, C>
where
    TS: PrimInt,
    C: Cursor<TS, V, (), R>,
{
    fn key_valid(&self) -> bool {
        self.cursor.key_valid() && self.current_range < self.ranges.len()
    }

    fn val_valid(&self) -> bool {
        self.cursor.val_valid()
    }

    fn key(&self) -> &TS {
        self.cursor.key()
    }

    fn val(&self) -> &V {
        self.cursor.val()
    }

    fn fold_times<F, U>(&mut self, init: U, fold: F) -> U
    where
        F: FnMut(U, &(), &R) -> U,
    {
        self.cursor.fold_times(init, fold)
    }

    fn fold_times_through<F, U>(&mut self, _upper: &(), init: U, fold: F) -> U
    where
        F: FnMut(U, &(), &R) -> U,
    {
        self.cursor.fold_times(init, fold)
    }

    fn weight(&mut self) -> R {
        self.cursor.weight()
    }

    fn step_key(&mut self) {
        self.cursor.step_key();
        self.advance();
    }

    fn step_key_reverse(&mut self) {
        unimplemented!()
    }

    fn seek_key_with<P>(&mut self, _predicate: P)
    where
        P: Fn(&TS) -> bool + Clone,
    {
        unimplemented!()
    }

    fn seek_key_with_reverse<P>(&mut self, _predicate: P)
    where
        P: Fn(&TS) -> bool + Clone,
    {
        unimplemented!()
    }

    fn step_val(&mut self) {
        self.cursor.step_val();
    }

    fn seek_val(&mut self, val: &V) {
        self.cursor.seek_val(val)
    }

    fn seek_val_with<P>(&mut self, predicate: P)
    where
        P: Fn(&V) -> bool + Clone,
    {
        self.cursor.seek_val_with(predicate)
    }

    fn rewind_keys(&mut self) {
        unimplemented!()
    }

    fn fast_forward_keys(&mut self) {
        unimplemented!()
    }

    fn rewind_vals(&mut self) {
        self.cursor.rewind_vals()
    }

    fn step_val_reverse(&mut self) {
        self.cursor.step_val_reverse();
    }

    fn seek_val_reverse(&mut self, val: &V) {
        self.cursor.seek_val_reverse(val)
    }

    fn seek_val_with_reverse<P>(&mut self, predicate: P)
    where
        P: Fn(&V) -> bool + Clone,
    {
        self.cursor.seek_val_with_reverse(predicate)
    }

    fn fast_forward_vals(&mut self) {
        self.cursor.fast_forward_vals()
    }
}

#[cfg(test)]
mod test {
    use crate::operator::time_series::range::{Range, Ranges};
    use num::PrimInt;

    fn ranges_from_bounds<T: PrimInt>(bounds: &[(T, T)]) -> Ranges<T> {
        let mut ranges = Ranges::new();

        for (from, to) in bounds.iter() {
            ranges.push_monotonic(Range::new(*from, *to));
        }

        ranges
    }

    #[test]
    fn test_merge() {
        let bounds1 = [(0, 0), (1, 3), (5, 10), (15, 15)];
        let ranges1 = ranges_from_bounds(&bounds1);

        let bounds2 = [(0, 0), (2, 4), (5, 7), (8, 11), (12, 13), (20, 30)];
        let ranges2 = ranges_from_bounds(&bounds2);

        let expected_bounds = [(0, 0), (1, 4), (5, 11), (12, 13), (15, 15), (20, 30)];
        let expected = ranges_from_bounds(&expected_bounds);

        let merged = ranges1.merge(&ranges2);
        assert_eq!(merged, expected);

        let merged = ranges2.merge(&ranges1);
        assert_eq!(merged, expected);
    }
}
