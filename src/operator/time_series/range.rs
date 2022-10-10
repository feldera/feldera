//! Data structures that specify contiguous time ranges.

use crate::trace::Cursor;
use num::PrimInt;
use std::{cmp::max, marker::PhantomData};

/// Relative time offset.
///
/// Specifies relative time as an offset.
#[derive(Clone, Eq, PartialEq)]
pub enum RelOffset<TS> {
    Before(TS),
    After(TS),
}

/// Relative time range.
///
/// Specifies a time interval relative to a given point in time.
#[derive(Clone)]
pub struct RelRange<TS> {
    from: RelOffset<TS>,
    to: RelOffset<TS>,
}

impl<TS> RelRange<TS>
where
    TS: PrimInt,
{
    pub fn new(from: RelOffset<TS>, to: RelOffset<TS>) -> Self {
        Self { from, to }
    }

    /// Computes relative range of timestamp `ts`.
    pub fn range_of(&self, ts: &TS) -> Range<TS> {
        let from = match self.from {
            RelOffset::Before(off) => ts.saturating_sub(off),
            RelOffset::After(off) => ts.saturating_add(off),
        };
        let to = match self.to {
            RelOffset::Before(off) => ts.saturating_sub(off),
            RelOffset::After(off) => ts.saturating_add(off),
        };

        Range { from, to }
    }

    /// Returns a range containing all times `t` such that `ts ∈
    /// self.range_of(t)`.
    pub fn affected_range_of(&self, ts: &TS) -> Range<TS> {
        let from = match self.to {
            RelOffset::Before(off) => ts.saturating_add(off),
            RelOffset::After(off) => ts.saturating_sub(off),
        };
        let to = match self.from {
            RelOffset::Before(off) => ts.saturating_add(off),
            RelOffset::After(off) => ts.saturating_sub(off),
        };

        Range::new(from, to)
    }
}

/// Absolute time range.
///
/// Specifies a time range containing all timestamps in the closed interval
/// `[from..to]`.
#[derive(Clone, Debug)]
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
#[derive(Debug, Clone)]
pub struct Ranges<TS: PrimInt>(Vec<Range<TS>>);

impl<TS> Ranges<TS>
where
    TS: PrimInt,
{
    pub fn new() -> Self {
        Self(Vec::new())
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

    /// Add a range whose lower bound is greater than or equal than the
    /// lowe bound of the last range in `self`.
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

impl<'a, TS, V, R, C> RangeCursor<TS, V, R, C>
where
    TS: PrimInt,
    C: Cursor<'a, TS, V, (), R>,
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

impl<'a, TS, V, R, C> Cursor<'a, TS, V, (), R> for RangeCursor<TS, V, R, C>
where
    TS: PrimInt,
    C: Cursor<'a, TS, V, (), R>,
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

    fn map_times<L: FnMut(&(), &R)>(&mut self, logic: L) {
        self.cursor.map_times(logic)
    }

    fn map_times_through<L: FnMut(&(), &R)>(&mut self, logic: L, upper: &()) {
        self.cursor.map_times_through(logic, upper)
    }

    fn weight(&mut self) -> R {
        self.cursor.weight()
    }

    fn step_key(&mut self) {
        self.cursor.step_key();
        self.advance();
    }

    fn seek_key(&mut self, _key: &TS) {
        unimplemented!()
    }

    fn last_key(&mut self) -> Option<&TS> {
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

    fn rewind_vals(&mut self) {
        self.cursor.rewind_vals()
    }
}
