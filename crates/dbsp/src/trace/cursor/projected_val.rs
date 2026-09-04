//! A cursor that hides a trailing column from a paired value type.

use dyn_clone::clone_box;

use crate::dynamic::{DataTrait, DynPair, Factory, WeightTrait};
use crate::trace::cursor::{Cursor, Position};

/// Presents a cursor over `DynPair<V, H>` values as a cursor over `V`, hiding
/// the trailing `H` column.
///
/// Because `H` is the last component of the sort key, records sharing a `V`
/// form a contiguous run, so the projection is a matter of consolidating each
/// run into a single value whose weight is the run's sum. Runs summing to zero
/// disappear, and so does a key whose every run does: the `Cursor` contract
/// promises that a valid position carries a non-zero weight, and both mergers
/// assume every exposed key has at least one value.
///
/// # Invariants
///
/// The inner cursor is never left inside the current run. After any move it
/// sits on the first record of the *next* run (or the previous one, when
/// iterating values backwards), and `val`/`weight` hold the projected value and
/// its consolidated weight. Reading ahead like this is what lets `val()` hand
/// out a reference that survives the `&mut self` calls its callers interleave:
/// [`CursorList`] transmutes that reference to `&'static V` and holds it across
/// `map_times` and `weight` on the same cursor.
///
/// So the scratch buffers are written by the moving methods and by nothing
/// else. `weight()`, `weight_checked()`, `map_times()` and `val()` must stay
/// free of side effects on them.
///
/// Like every other cursor here, this one relies on the value seeks receiving
/// the monotone predicate they document, and the two directions want opposite
/// shapes: `seek_val_with` needs `false..false true..true` in value order and
/// lands on the first `true`, while `seek_val_with_reverse` needs
/// `true..true false..false` and lands on the last one. Since the predicate
/// reads only the projected component it is constant across a run, so those
/// landings are a run's first and last record respectively, which is where the
/// two scans need to start.
///
/// Monotonicity has to hold over the values of the UNDERLYING batch, not merely
/// over the ones the projection exposes. A predicate can be monotone across the
/// surviving values and still change sign on a value hidden by a cancelling
/// run, and the search sees that value.
///
/// What this asks of the inner cursor is slightly stronger than the wording of
/// `seek_val_with`: it must stop at the FIRST satisfying record, not merely at
/// some satisfying one. Landing later can be inside a run, and a run seeded
/// from its middle reports a partial sum -- a weight no record carried, which
/// the merger then writes out as fact. No local check can catch that, so it is
/// pinned by a test instead.
///
/// [`CursorList`]: crate::trace::cursor::CursorList
#[derive(Debug)]
pub struct ProjectedValCursor<K, V, H, R, C>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    H: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    C: Cursor<K, DynPair<V, H>, (), R>,
{
    inner: C,

    /// The projected value of the current run.
    val: Box<V>,

    /// Consolidated weight of the current run. Non-zero whenever `val_valid`.
    weight: Box<R>,

    /// Whether `val`/`weight` describe a surviving run.
    val_valid: bool,

    /// Set when `seek_key_exact` reported absence, so `key_valid` can hide the
    /// key the inner cursor is parked on. Cleared by every other key move.
    probe_missed: bool,

    phantom: std::marker::PhantomData<fn(&K, &H)>,
}

impl<K, V, H, R, C> ProjectedValCursor<K, V, H, R, C>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    H: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    C: Cursor<K, DynPair<V, H>, (), R>,
{
    /// Wraps `inner`, positioning on its first surviving value.
    pub fn new(inner: C, val_factory: &'static dyn Factory<V>) -> Self {
        let weight = inner.weight_factory().default_box();
        let mut this = Self {
            val: val_factory.default_box(),
            weight,
            inner,
            val_valid: false,
            probe_missed: false,
            phantom: std::marker::PhantomData,
        };
        // Do not trust where the caller left the cursor. Seeding a run from a
        // record in its middle would report a weight that is a suffix sum, and
        // an inner left in reverse key mode would fault on the first step.
        this.inner.rewind_keys();
        this.rewind_inner_vals();
        this.seek_live_key_forward();
        this
    }

    /// Consolidates the run the inner cursor sits on, and every following run,
    /// until one survives or the key runs out.
    ///
    /// Leaves the inner cursor just past the surviving run.
    fn next_run_forward(&mut self) {
        loop {
            if !self.inner_on_value() {
                self.val_valid = false;
                return;
            }
            // Seed from the first record and advance, so the loop never
            // compares the value against the copy just taken from it.
            self.inner.val().fst().clone_to(&mut *self.val);
            self.inner.weight().clone_to(&mut *self.weight);
            self.inner.step_val();
            while self.inner_on_value() && self.inner.val().fst() == self.val.as_ref() {
                self.weight.add_assign(self.inner.weight());
                self.inner.step_val();
            }
            // The whole design rests on runs being contiguous, which holds only
            // while the hidden column is the last sort component. If it ever is
            // not, the same projected value reappears in a later run and the
            // batch silently gains duplicates.
            debug_assert!(
                !self.inner_on_value() || self.inner.val().fst() > self.val.as_ref(),
                "values must be ordered by the projected component"
            );
            if !self.weight.is_zero() {
                self.val_valid = true;
                return;
            }
        }
    }

    /// Consolidates the run the inner cursor sits on, and every preceding run,
    /// until one survives or the key runs out.
    ///
    /// Leaves the inner cursor just before the surviving run. The inner cursor
    /// must be iterating values backwards.
    fn prev_run_reverse(&mut self) {
        loop {
            if !self.inner_on_value() {
                self.val_valid = false;
                return;
            }
            self.inner.val().fst().clone_to(&mut *self.val);
            self.inner.weight().clone_to(&mut *self.weight);
            self.inner.step_val_reverse();
            while self.inner_on_value() && self.inner.val().fst() == self.val.as_ref() {
                self.weight.add_assign(self.inner.weight());
                self.inner.step_val_reverse();
            }
            debug_assert!(
                !self.inner_on_value() || self.inner.val().fst() < self.val.as_ref(),
                "values must be ordered by the projected component"
            );
            if !self.weight.is_zero() {
                self.val_valid = true;
                return;
            }
        }
    }

    /// Advances over keys whose every run cancels.
    fn seek_live_key_forward(&mut self) {
        while self.inner.key_valid() {
            self.next_run_forward();
            if self.val_valid {
                return;
            }
            self.inner.step_key();
        }
        self.val_valid = false;
    }

    /// Retreats over keys whose every run cancels.
    ///
    /// Values are still scanned forwards: the `Cursor` contract resets the
    /// value direction whenever the cursor moves to a new key.
    fn seek_live_key_reverse(&mut self) {
        while self.inner.key_valid() {
            self.next_run_forward();
            if self.val_valid {
                return;
            }
            self.inner.step_key_reverse();
        }
        self.val_valid = false;
    }

    /// True when the inner cursor is on a value that can be read.
    ///
    /// Both halves matter: a backend may report `val_valid` at an invalid key
    /// (the vec cursor does, after rewinding values past the last key), and its
    /// `weight()` asserts on the KEY being valid.
    fn inner_on_value(&self) -> bool {
        self.inner.key_valid() && self.inner.val_valid()
    }

    /// Rewinds the inner cursor's value position.
    fn rewind_inner_vals(&mut self) {
        if self.inner.key_valid() {
            self.inner.rewind_vals();
        }
    }
}

/// Hand-written because `derive` would demand `K: Clone` on unsized parameters.
impl<K, V, H, R, C> Clone for ProjectedValCursor<K, V, H, R, C>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    H: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    C: Cursor<K, DynPair<V, H>, (), R> + Clone,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            val: clone_box(&*self.val),
            weight: clone_box(&*self.weight),
            val_valid: self.val_valid,
            probe_missed: self.probe_missed,
            phantom: std::marker::PhantomData,
        }
    }
}

impl<K, V, H, R, C> Cursor<K, V, (), R> for ProjectedValCursor<K, V, H, R, C>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    H: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    C: Cursor<K, DynPair<V, H>, (), R>,
{
    fn weight_factory(&self) -> &'static dyn Factory<R> {
        self.inner.weight_factory()
    }

    fn key_valid(&self) -> bool {
        // A missed probe parks the inner on some other key; the projection is
        // not positioned on anything the caller asked for.
        self.inner.key_valid() && !self.probe_missed
    }

    fn val_valid(&self) -> bool {
        self.val_valid
    }

    fn key(&self) -> &K {
        self.inner.key()
    }

    fn val(&self) -> &V {
        debug_assert!(self.val_valid, "value position is not valid");
        &self.val
    }

    fn map_times(&mut self, logic: &mut dyn FnMut(&(), &R)) {
        if self.val_valid {
            logic(&(), &self.weight);
        }
    }

    fn map_times_through(&mut self, _upper: &(), logic: &mut dyn FnMut(&(), &R)) {
        self.map_times(logic);
    }

    fn weight(&mut self) -> &R {
        debug_assert!(self.val_valid);
        &self.weight
    }

    fn weight_checked(&mut self) -> &R {
        debug_assert!(self.val_valid);
        debug_assert!(!self.weight.is_zero());
        &self.weight
    }

    fn map_values(&mut self, logic: &mut dyn FnMut(&V, &R)) {
        while self.val_valid {
            logic(&self.val, &self.weight);
            self.step_val();
        }
    }

    fn step_key(&mut self) {
        self.probe_missed = false;
        self.inner.step_key();
        self.seek_live_key_forward();
    }

    fn step_key_reverse(&mut self) {
        self.probe_missed = false;
        self.inner.step_key_reverse();
        self.seek_live_key_reverse();
    }

    fn seek_key(&mut self, key: &K) {
        self.probe_missed = false;
        self.inner.seek_key(key);
        // A seek that does not move leaves the value position past the run the
        // projection is on; a seek that does move resets it anyway.
        self.rewind_inner_vals();
        self.seek_live_key_forward();
    }

    fn seek_key_exact(&mut self, key: &K, hash: Option<u64>) -> bool {
        self.probe_missed = false;
        if !self.inner.seek_key_exact(key, hash) {
            self.probe_missed = true;
            self.val_valid = false;
            return false;
        }
        self.rewind_inner_vals();
        self.next_run_forward();
        if self.val_valid {
            return true;
        }
        // Present in the underlying batch, but every run cancels, so it is not
        // in the projection. Report absence and leave the inner parked: callers
        // of `seek_key_exact` may only follow it with another, for a greater key.
        self.probe_missed = true;
        false
    }

    fn seek_key_with(&mut self, predicate: &dyn Fn(&K) -> bool) {
        self.probe_missed = false;
        self.inner.seek_key_with(predicate);
        self.rewind_inner_vals();
        self.seek_live_key_forward();
    }

    fn seek_key_with_reverse(&mut self, predicate: &dyn Fn(&K) -> bool) {
        self.probe_missed = false;
        self.inner.seek_key_with_reverse(predicate);
        self.rewind_inner_vals();
        self.seek_live_key_reverse();
    }

    fn seek_key_reverse(&mut self, key: &K) {
        self.probe_missed = false;
        self.inner.seek_key_reverse(key);
        self.rewind_inner_vals();
        self.seek_live_key_reverse();
    }

    fn step_val(&mut self) {
        // The inner cursor already sits past the current run.
        self.next_run_forward();
    }

    fn step_val_reverse(&mut self) {
        self.prev_run_reverse();
    }

    fn seek_val(&mut self, val: &V) {
        // Seeks only advance, and the inner cursor already sits past the current
        // run, so it cannot come back to it: answer from the projected value.
        if self.val_valid && self.val.as_ref() >= val {
            return;
        }
        // Monotone over the lexicographic pair order, so one search suffices.
        self.inner.seek_val_with(&|pair| pair.fst() >= val);
        self.next_run_forward();
    }

    fn seek_val_reverse(&mut self, val: &V) {
        if self.val_valid && self.val.as_ref() <= val {
            return;
        }
        self.inner.seek_val_with_reverse(&|pair| pair.fst() <= val);
        self.prev_run_reverse();
    }

    fn seek_val_with(&mut self, predicate: &dyn Fn(&V) -> bool) {
        if self.val_valid && predicate(&self.val) {
            return;
        }
        self.inner.seek_val_with(&|pair| predicate(pair.fst()));
        self.next_run_forward();
        // Catches a predicate that is not monotone over the underlying values:
        // then the search may stop early and skipping a cancelling run can
        // settle on a value the caller rejects. It does not catch a search that
        // landed inside a run, which would report a partial sum instead.
        debug_assert!(!self.val_valid || predicate(&self.val));
    }

    fn seek_val_with_reverse(&mut self, predicate: &dyn Fn(&V) -> bool) {
        if self.val_valid && predicate(&self.val) {
            return;
        }
        self.inner
            .seek_val_with_reverse(&|pair| predicate(pair.fst()));
        self.prev_run_reverse();
        debug_assert!(!self.val_valid || predicate(&self.val));
    }

    fn rewind_keys(&mut self) {
        self.probe_missed = false;
        self.inner.rewind_keys();
        self.rewind_inner_vals();
        self.seek_live_key_forward();
    }

    fn fast_forward_keys(&mut self) {
        self.probe_missed = false;
        self.inner.fast_forward_keys();
        self.rewind_inner_vals();
        self.seek_live_key_reverse();
    }

    fn rewind_vals(&mut self) {
        self.rewind_inner_vals();
        self.next_run_forward();
    }

    fn fast_forward_vals(&mut self) {
        if self.inner.key_valid() {
            self.inner.fast_forward_vals();
        }
        self.prev_run_reverse();
    }

    fn position(&self) -> Option<Position> {
        // Measured in the underlying batch, so `total` counts keys the
        // projection drops and `offset` can advance by more than one per step.
        // Callers use it for progress and chunking, which tolerates that.
        // `CursorList` unwraps it for every child, so it must not be `None`
        // when the inner is `Some`.
        self.inner.position()
    }
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;

    use proptest::{collection::vec, prelude::*};

    use super::{Position, ProjectedValCursor};
    use crate::dynamic::{DowncastTrait, DynData, DynPair, Erase, Factory, LeanVec, WithFactory};
    use crate::trace::cursor::{CursorList, DelegatingCursor};
    use crate::trace::test::run_in_circuit_with_storage;
    use crate::trace::{
        Batch, BatchReader, BatchReaderFactories, Cursor, FileIndexedWSet,
        FileIndexedWSetFactories, VecIndexedWSet, VecIndexedWSetFactories,
    };
    use crate::utils::Tup2;
    use crate::{DynZWeight, ZWeight};

    type Pair = DynPair<DynData, DynData>;
    type Batched = VecIndexedWSet<DynData, Pair, DynZWeight>;

    /// `(key, value, hidden stamp, weight)`.
    type Row = (i32, i32, u32, ZWeight);

    fn build(rows: &[Row]) -> Batched {
        let factories = <VecIndexedWSetFactories<DynData, Pair, DynZWeight>>::new::<
            i32,
            Tup2<i32, u32>,
            ZWeight,
        >();
        let tuples: Vec<Tup2<Tup2<i32, Tup2<i32, u32>>, ZWeight>> = rows
            .iter()
            .map(|&(k, v, h, w)| Tup2(Tup2(k, Tup2(v, h)), w))
            .collect();
        let mut tuples = Box::new(LeanVec::from(tuples)).erase_box();
        Batched::dyn_from_tuples(&factories, (), &mut tuples)
    }

    fn cursor(batch: &Batched) -> impl Cursor<DynData, DynData, (), DynZWeight> + '_ {
        ProjectedValCursor::new(batch.cursor(), <DynData as WithFactory<i32>>::FACTORY)
    }

    /// What the projection should produce: the batch consolidates rows sharing
    /// `(key, value, stamp)`, then hiding the stamp merges rows sharing
    /// `(key, value)`. Zero sums vanish, and so do keys left with nothing.
    fn oracle(rows: &[Row]) -> Vec<(i32, i32, ZWeight)> {
        let mut consolidated: BTreeMap<(i32, i32, u32), ZWeight> = BTreeMap::new();
        for &(k, v, h, w) in rows {
            *consolidated.entry((k, v, h)).or_default() += w;
        }
        let mut projected: BTreeMap<(i32, i32), ZWeight> = BTreeMap::new();
        for ((k, v, _h), w) in consolidated {
            if w != 0 {
                *projected.entry((k, v)).or_default() += w;
            }
        }
        projected
            .into_iter()
            .filter(|(_, w)| *w != 0)
            .map(|((k, v), w)| (k, v, w))
            .collect()
    }

    fn walk_forward<C: Cursor<DynData, DynData, (), DynZWeight>>(
        c: &mut C,
    ) -> Vec<(i32, i32, ZWeight)> {
        let mut out = Vec::new();
        while c.key_valid() {
            assert!(
                c.val_valid(),
                "a valid key must expose at least one value: both mergers assume it"
            );
            while c.val_valid() {
                out.push((
                    *unsafe { c.key().downcast::<i32>() },
                    *unsafe { c.val().downcast::<i32>() },
                    **c.weight(),
                ));
                c.step_val();
            }
            c.step_key();
        }
        out
    }

    /// Keys forward, values backwards.
    fn walk_vals_reverse<C: Cursor<DynData, DynData, (), DynZWeight>>(
        c: &mut C,
    ) -> Vec<(i32, i32, ZWeight)> {
        let mut out = Vec::new();
        while c.key_valid() {
            let mut per_key = Vec::new();
            c.fast_forward_vals();
            assert!(
                c.val_valid(),
                "a valid key must expose at least one value: both mergers assume it"
            );
            while c.val_valid() {
                per_key.push((
                    *unsafe { c.key().downcast::<i32>() },
                    *unsafe { c.val().downcast::<i32>() },
                    **c.weight(),
                ));
                c.step_val_reverse();
            }
            per_key.reverse();
            out.extend(per_key);
            c.step_key();
        }
        out
    }

    /// Keys backwards, values forwards.
    fn walk_keys_reverse<C: Cursor<DynData, DynData, (), DynZWeight>>(
        c: &mut C,
    ) -> Vec<(i32, i32, ZWeight)> {
        let mut out = Vec::new();
        c.fast_forward_keys();
        while c.key_valid() {
            assert!(
                c.val_valid(),
                "a valid key must expose at least one value: both mergers assume it"
            );
            let mut per_key = Vec::new();
            while c.val_valid() {
                per_key.push((
                    *unsafe { c.key().downcast::<i32>() },
                    *unsafe { c.val().downcast::<i32>() },
                    **c.weight(),
                ));
                c.step_val();
            }
            out.splice(0..0, per_key);
            c.step_key_reverse();
        }
        out
    }

    fn check(rows: &[Row]) {
        let batch = build(rows);
        let expected = oracle(rows);

        assert_eq!(walk_forward(&mut cursor(&batch)), expected, "forward");
        assert_eq!(
            walk_vals_reverse(&mut cursor(&batch)),
            expected,
            "values backwards"
        );
        assert_eq!(
            walk_keys_reverse(&mut cursor(&batch)),
            expected,
            "keys backwards"
        );
    }

    /// A run longer than one, so the cursor must actually consolidate.
    #[test]
    fn consolidates_a_run() {
        check(&[(1, 10, 0, 1), (1, 10, 1, 2), (1, 10, 2, 4), (1, 20, 0, 1)]);
    }

    /// A run that sums to zero must not be exposed: the `Cursor` contract says
    /// a valid position carries a non-zero weight, and the builders assert it.
    #[test]
    fn drops_a_cancelling_run() {
        check(&[(1, 10, 0, 1), (1, 10, 1, -1), (1, 20, 0, 3)]);
    }

    /// A key whose every run cancels must disappear, or the mergers see a key
    /// with no values.
    #[test]
    fn drops_a_fully_cancelling_key() {
        check(&[
            (1, 10, 0, 1),
            (2, 10, 0, 1),
            (2, 10, 1, -1),
            (2, 20, 0, 2),
            (2, 20, 1, -2),
            (3, 30, 0, 1),
        ]);
    }

    /// A vanishing key in the last position, which a loop that only checks
    /// "is there a next key" gets wrong.
    #[test]
    fn drops_a_cancelling_last_key() {
        check(&[(1, 10, 0, 1), (9, 99, 0, 1), (9, 99, 1, -1)]);
    }

    /// A vanishing key in the first position, which the constructor must skip
    /// before the cursor is ever read.
    #[test]
    fn drops_a_cancelling_first_key() {
        check(&[(0, 5, 0, 1), (0, 5, 1, -1), (1, 10, 0, 1)]);
    }

    #[test]
    fn empty_batch() {
        check(&[]);
    }

    /// Every run cancels, so the whole projection is empty even though the
    /// underlying batch is not.
    #[test]
    fn everything_cancels() {
        check(&[(1, 10, 0, 1), (1, 10, 1, -1), (2, 20, 0, 5), (2, 20, 1, -5)]);
    }

    /// `seek_key_exact` must not claim a key whose values all cancel, and a
    /// failed lookup must not derail the ones after it.
    ///
    /// The contract allows only a further `seek_key_exact` for a greater key,
    /// or a rewind, so this walks the sequence a real caller uses: the lazy
    /// upsert probes one key per update, in order.
    #[test]
    fn seek_key_exact_skips_a_cancelled_key() {
        let batch = build(&[
            (1, 10, 0, 1),
            (1, 10, 1, 2),
            (2, 20, 0, 1),
            (2, 20, 1, -1),
            (3, 30, 0, 4),
        ]);
        let mut c = cursor(&batch);

        assert!(c.seek_key_exact(1i32.erase(), None));
        assert_eq!(*unsafe { c.val().downcast::<i32>() }, 10);
        assert_eq!(**c.weight(), 3, "the run is consolidated");

        assert!(!c.seek_key_exact(2i32.erase(), None), "key 2 fully cancels");

        // The failed probe must leave the cursor usable for the next one.
        assert!(c.seek_key_exact(3i32.erase(), None));
        assert_eq!(*unsafe { c.val().downcast::<i32>() }, 30);
        assert_eq!(**c.weight(), 4);

        // A key past the end.
        let mut c = cursor(&batch);
        assert!(!c.seek_key_exact(9i32.erase(), None));
    }

    /// A seek whose target is the value already under the cursor reports the
    /// whole run, not the part of it the search would have skipped to.
    #[test]
    fn seek_val_with_reports_the_whole_run() {
        let batch = build(&[(1, 10, 0, 1), (1, 10, 1, 10), (1, 10, 2, 100)]);
        let mut c = cursor(&batch);

        c.seek_val_with(&|v: &DynData| *unsafe { v.downcast::<i32>() } >= 10);

        assert!(c.val_valid());
        assert_eq!(*unsafe { c.val().downcast::<i32>() }, 10);
        assert_eq!(**c.weight(), 111, "the whole run, not a suffix of it");
    }

    /// `seek_val` lands on the first surviving value at or after the target.
    #[test]
    fn seek_val_lands_on_run_start() {
        let batch = build(&[(1, 10, 0, 1), (1, 20, 0, 2), (1, 20, 1, 3), (1, 30, 0, 4)]);
        let mut c = cursor(&batch);
        c.seek_val(20i32.erase());
        assert!(c.val_valid());
        assert_eq!(*unsafe { c.val().downcast::<i32>() }, 20);
        assert_eq!(**c.weight(), 5);
    }

    /// Seeking to the value the cursor is already on must be a no-op: the inner
    /// cursor sits past that run, so a forward seek could not return to it.
    #[test]
    fn seek_val_to_current_value_stays() {
        let batch = build(&[(1, 10, 0, 1), (1, 10, 1, 2), (1, 20, 0, 4)]);
        let mut c = cursor(&batch);

        c.seek_val(10i32.erase());
        assert!(c.val_valid());
        assert_eq!(*unsafe { c.val().downcast::<i32>() }, 10);
        assert_eq!(**c.weight(), 3);
    }

    /// A run far longer than two, with mixed signs that do not cancel.
    #[test]
    fn long_run_with_mixed_signs() {
        let rows: Vec<Row> = (0..8u32)
            .map(|h| (1, 10, h, if h % 2 == 0 { 3 } else { -2 }))
            .collect();
        check(&rows);
        let batch = build(&rows);
        let mut c = cursor(&batch);
        // 4 * 3 - 4 * 2
        assert_eq!(**c.weight(), 4);
    }

    /// A long run whose weights cancel exactly, so the value must vanish and
    /// take its only key with it.
    #[test]
    fn long_run_cancelling_exactly() {
        let rows: Vec<Row> = (0..8u32)
            .map(|h| (1, 10, h, if h % 2 == 0 { 3 } else { -3 }))
            .collect();
        check(&rows);
        let batch = build(&rows);
        let c = cursor(&batch);
        assert!(!c.key_valid(), "the only key's only run cancels");
    }

    /// `map_values` walks the projection, matching what a plain cursor's
    /// implementation does.
    #[test]
    fn map_values_visits_the_projection() {
        let batch = build(&[(1, 10, 0, 1), (1, 10, 1, 2), (1, 20, 0, 4), (2, 30, 0, 8)]);
        let mut c = cursor(&batch);

        let mut seen = Vec::new();
        c.map_values(&mut |v, w| {
            seen.push((*unsafe { v.downcast::<i32>() }, **w));
        });
        assert_eq!(seen, vec![(10, 3), (20, 4)], "only the first key's values");
    }

    /// `map_times` reports the run's consolidated weight, once. The mergers
    /// accumulate what it yields, so a per-record report would double-count.
    #[test]
    fn map_times_reports_the_consolidated_weight_once() {
        let batch = build(&[(1, 10, 0, 1), (1, 10, 1, 2), (1, 10, 2, 4)]);
        let mut c = cursor(&batch);

        let mut times = Vec::new();
        c.map_times(&mut |_, w| times.push(**w));
        assert_eq!(times, vec![7]);
        assert_eq!(**c.weight_checked(), 7);
    }

    /// Every key having exactly one value, the shape backfill produces.
    #[test]
    fn one_value_per_key() {
        check(&[(1, 10, 0, 1), (2, 20, 0, 1), (3, 30, 0, 1)]);
    }

    /// A monotone predicate finds the first surviving value that satisfies it,
    /// even when a cancelling run sits in the way. The cancelled run's value
    /// also satisfies the predicate, so skipping it cannot change the answer.
    #[test]
    fn seek_val_with_finds_a_surviving_value_past_a_cancelling_run() {
        let batch = build(&[(1, 10, 0, 1), (1, 20, 0, 1), (1, 20, 1, -1), (1, 30, 0, 5)]);
        let mut c = cursor(&batch);
        c.seek_val_with(&|v: &DynData| *unsafe { v.downcast::<i32>() } >= 20);
        assert!(c.val_valid());
        assert_eq!(*unsafe { c.val().downcast::<i32>() }, 30);
        assert_eq!(**c.weight(), 5);
    }

    /// `BatchReader::Cursor` is bounded `Cursor + Clone + Send` (trace.rs:475),
    /// and `DelegatingCursor`'s `ClonableCursor` blanket adds `Debug`
    /// (cursor.rs:413). M4 wires this cursor in through both, so pin the bounds
    /// here rather than discovering it there.
    fn assert_batch_cursor_bounds<C>(_: &C)
    where
        C: Cursor<DynData, DynData, (), DynZWeight> + Clone + Send + std::fmt::Debug,
    {
    }

    #[test]
    fn clones_independently_and_meets_the_batch_cursor_bounds() {
        let batch = build(&[(1, 10, 0, 1), (1, 20, 0, 2), (2, 30, 0, 4)]);
        let mut a = ProjectedValCursor::new(batch.cursor(), <DynData as WithFactory<i32>>::FACTORY);
        assert_batch_cursor_bounds(&a);

        let mut b = a.clone();
        a.step_val();

        assert_eq!(*unsafe { a.val().downcast::<i32>() }, 20);
        assert_eq!(
            *unsafe { b.val().downcast::<i32>() },
            10,
            "the clone must not follow the original"
        );
        assert_eq!(**b.weight(), 1);
    }

    /// Characterizes what the design depends on, since nothing can check it at
    /// runtime.
    ///
    /// `seek_val_with` must stop at the first record satisfying its predicate.
    /// For a run-constant predicate that record starts a run, which is what
    /// makes the consolidated weight the run's whole sum. An inner that stopped
    /// later, inside the run, would produce a partial sum instead -- and the
    /// second half of this test is what that looks like.
    #[test]
    fn seeding_inside_a_run_would_report_a_partial_sum() {
        let batch = build(&[(1, 10, 0, 1), (1, 10, 1, 10), (1, 10, 2, 100)]);
        let mut c = ProjectedValCursor::new(batch.cursor(), <DynData as WithFactory<i32>>::FACTORY);
        assert_eq!(**c.weight(), 111, "seeded from the run's first record");

        // Stand in for an inner that stopped one record into the run.
        c.inner.rewind_vals();
        c.inner.step_val();
        c.next_run_forward();
        assert_eq!(
            **c.weight(),
            110,
            "a suffix of the run: the outcome the first-satisfying-record \
             requirement rules out"
        );
    }

    /// The reference from `val()` must stay valid across the `&mut self` calls a
    /// caller interleaves with it.
    ///
    /// This is the whole reason the inner cursor is read ahead of the current
    /// run: `CursorList` transmutes that reference to `&'static V` and holds it
    /// across `map_times`/`weight` on the same child (cursor_list.rs:436, :457).
    /// The raw pointer here stands in for that transmute.
    #[test]
    fn val_reference_survives_weight_and_map_times() {
        let batch = build(&[(1, 10, 0, 1), (1, 10, 1, 2), (1, 20, 0, 4)]);
        let mut c = cursor(&batch);

        let held: *const DynData = c.val();

        let w = **c.weight();
        c.map_times(&mut |_, _| {});
        c.weight_checked();

        assert_eq!(
            *unsafe { (*held).downcast::<i32>() },
            10,
            "val() must outlive the &mut self calls made after it"
        );
        assert_eq!(w, 3);
    }

    /// Projecting cursors merged by a `CursorList`, which is how a spine reads
    /// a mix of batches. Exercises within-run consolidation underneath
    /// across-cursor cancellation.
    #[test]
    fn merges_inside_a_cursor_list() {
        //   a: key 1 -> {10: 1, 20: 2},  key 3 -> {30: 4}
        //   b: key 1 -> {10: 5},         key 2 -> {25: 7},  key 3 -> {30: -4}
        // merged: key 1 -> {10: 6, 20: 2}, key 2 -> {25: 7}
        //         key 3 drops: its only value sums to zero across the two.
        let a = build(&[(1, 10, 0, 1), (1, 20, 0, 2), (3, 30, 0, 4)]);
        let b = build(&[(1, 10, 0, 3), (1, 10, 1, 2), (2, 25, 0, 7), (3, 30, 0, -4)]);

        let mut list = CursorList::new(
            <DynZWeight as WithFactory<ZWeight>>::FACTORY,
            vec![cursor(&a), cursor(&b)],
        );

        assert_eq!(
            walk_forward(&mut list),
            vec![(1, 10, 6), (1, 20, 2), (2, 25, 7)]
        );
    }

    // ------------------------------------------------------------ file backend

    type FileBatched = FileIndexedWSet<DynData, Pair, DynZWeight>;

    /// The same rows as [`build`], in a layer file.
    ///
    /// Built through `stamped` factories, so the file carries the hidden-column
    /// mark and this exercises the whole stack: the flag from the writer, the
    /// file cursor, and the projection on top of it.
    fn build_file(rows: &[Row]) -> FileBatched {
        let factories = <FileIndexedWSetFactories<DynData, Pair, DynZWeight>>::stamped::<
            i32,
            Tup2<i32, u32>,
            ZWeight,
        >();
        let tuples: Vec<Tup2<Tup2<i32, Tup2<i32, u32>>, ZWeight>> = rows
            .iter()
            .map(|&(k, v, h, w)| Tup2(Tup2(k, Tup2(v, h)), w))
            .collect();
        let mut tuples = Box::new(LeanVec::from(tuples)).erase_box();
        FileBatched::dyn_from_tuples(&factories, (), &mut tuples)
    }

    fn file_cursor(batch: &FileBatched) -> impl Cursor<DynData, DynData, (), DynZWeight> + '_ {
        ProjectedValCursor::new(batch.cursor(), <DynData as WithFactory<i32>>::FACTORY)
    }

    /// A projecting cursor boxed the way `FallbackIndexedWSet::cursor` boxes
    /// one, which also pins the `Clone + Send + Debug` bounds that composition
    /// requires.
    fn delegating_vec(batch: &Batched) -> DelegatingCursor<'_, DynData, DynData, (), DynZWeight> {
        DelegatingCursor(Box::new(ProjectedValCursor::new(
            batch.cursor(),
            <DynData as WithFactory<i32>>::FACTORY,
        )))
    }

    fn delegating_file(
        batch: &FileBatched,
    ) -> DelegatingCursor<'_, DynData, DynData, (), DynZWeight> {
        DelegatingCursor(Box::new(ProjectedValCursor::new(
            batch.cursor(),
            <DynData as WithFactory<i32>>::FACTORY,
        )))
    }

    /// The rows every file test uses: runs longer than one, a run that cancels,
    /// a key that cancels entirely, and a cancelling last key.
    const FILE_ROWS: &[Row] = &[
        (1, 10, 0, 1),
        (1, 10, 1, 2),
        (1, 20, 0, 4),
        (2, 25, 0, 3),
        (2, 25, 1, -3),
        (3, 30, 0, 8),
        (4, 40, 0, 5),
        (4, 40, 1, -5),
    ];

    /// A file-backed batch projects exactly like a vec-backed one, in all three
    /// traversal directions.
    ///
    /// The file cursor is the one whose borrows the read-ahead design was
    /// written for: it deserializes into a box it overwrites on every move,
    /// where the vec cursor hands out references into stable storage.
    #[test]
    fn file_batch_projects_like_a_vec_batch() {
        run_in_circuit_with_storage(|| {
            let batch = build_file(FILE_ROWS);
            let expected = oracle(FILE_ROWS);

            assert_eq!(walk_forward(&mut file_cursor(&batch)), expected, "forward");
            assert_eq!(
                walk_vals_reverse(&mut file_cursor(&batch)),
                expected,
                "values backwards"
            );
            assert_eq!(
                walk_keys_reverse(&mut file_cursor(&batch)),
                expected,
                "keys backwards"
            );
        });
    }

    /// The `val()` reference must outlive later `&mut self` calls on the file
    /// backend too, where the inner cursor overwrites its value box on a move.
    #[test]
    fn file_val_reference_survives_weight_and_map_times() {
        run_in_circuit_with_storage(|| {
            let batch = build_file(FILE_ROWS);
            let mut c = file_cursor(&batch);

            let held: *const DynData = c.val();
            let w = **c.weight();
            c.map_times(&mut |_, _| {});

            assert_eq!(*unsafe { (*held).downcast::<i32>() }, 10);
            assert_eq!(w, 3);
        });
    }

    /// Seeks and steps on the file backend, against the same reference the vec
    /// model uses. One circuit, a fixed seed, so it stays deterministic and
    /// does not pay for a runtime per case.
    #[test]
    fn file_backend_matches_the_materialized_projection() {
        use rand::{Rng, SeedableRng};
        use rand_chacha::ChaCha8Rng;

        run_in_circuit_with_storage(|| {
            let mut rng = ChaCha8Rng::seed_from_u64(0xF11E);
            for case in 0..64 {
                let rows: Vec<Row> = (0..rng.gen_range(0..24))
                    .map(|_| {
                        (
                            rng.gen_range(0..4),
                            rng.gen_range(0..4),
                            rng.gen_range(0..3),
                            rng.gen_range(-2..3),
                        )
                    })
                    .collect();
                let ops: Vec<Op> = (0..rng.gen_range(0..16))
                    .map(|_| random_op(&mut rng))
                    .collect();

                let file = build_file(&rows);
                let reference = reference(&rows);
                let mut actual = file_cursor(&file);
                let mut expected = reference.cursor();

                drive(&mut actual, &mut expected, &ops, &rows, case);
            }
        });
    }

    /// A `CursorList` merging a file-backed and a vec-backed projection, which
    /// is what a spine does once a batch has spilled but its neighbours have
    /// not. Both go through `DelegatingCursor`, so the list sees one type.
    ///
    /// Note what a list can and cannot check. It catches a wrong consolidated
    /// weight, but not a run wrongly exposed with weight zero: `CursorList`
    /// suppresses those itself (cursor_list.rs:166-174), so it masks the
    /// projection's own zero-suppression. That has to be tested directly.
    #[test]
    fn cursor_list_merges_file_and_vec_projections() {
        run_in_circuit_with_storage(|| {
            //  file: key 1 -> {10: 3}, key 2 -> {} (its run cancels), key 3 -> {30: 4}
            //   vec: key 1 -> {10: 5}, key 2 -> {25: 7}, key 3 -> {30: -4}
            //
            // Key 2 exercises both kinds of disappearance at once: value 20 must
            // be dropped by the file batch's own run consolidation, while key 3
            // survives that stage and only cancels across the two cursors.
            let file_rows: &[Row] = &[
                (1, 10, 0, 1),
                (1, 10, 1, 2),
                (2, 20, 0, 5),
                (2, 20, 1, -5),
                (3, 30, 0, 4),
            ];
            let vec_rows: &[Row] = &[(1, 10, 0, 5), (2, 25, 0, 7), (3, 30, 0, -4)];

            let file = build_file(file_rows);
            let vecb = build(vec_rows);

            let mut list = CursorList::new(
                <DynZWeight as WithFactory<ZWeight>>::FACTORY,
                vec![delegating_file(&file), delegating_vec(&vecb)],
            );

            assert_eq!(
                walk_forward(&mut list),
                vec![(1, 10, 8), (2, 25, 7)],
                "file and vec projections merged"
            );
        });
    }

    // ------------------------------------------------- minimal conforming inner

    /// An inner cursor that resets its value position only when a key move
    /// actually changes the key, which is all the `Cursor` contract promises
    /// (cursor.rs:76-79).
    ///
    /// Both real backends reset unconditionally -- every vec key move rebuilds
    /// the child cursor, and the file cursor rebuilds `val_cursor` in
    /// `move_key` -- so against them the `rewind_inner_vals` guards are
    /// unobservable, and a test that deletes them still passes. This restores
    /// the value position after a no-op key move, so those guards are the only
    /// thing standing between the projection and a run seeded from its middle.
    #[derive(Debug, Clone)]
    struct LazyResetCursor<C> {
        inner: C,
    }

    impl<C> LazyResetCursor<C>
    where
        C: Cursor<DynData, Pair, (), DynZWeight>,
    {
        fn new(inner: C) -> Self {
            Self { inner }
        }

        /// Runs `mv`, and if the key did not change, puts the value position
        /// back where it was.
        fn key_move(&mut self, mv: impl FnOnce(&mut C)) {
            let before_key = self
                .inner
                .key_valid()
                .then(|| *unsafe { self.inner.key().downcast::<i32>() });
            let before_val = (self.inner.key_valid() && self.inner.val_valid())
                .then(|| *unsafe { self.inner.val().downcast::<Tup2<i32, u32>>() });

            mv(&mut self.inner);

            let after_key = self
                .inner
                .key_valid()
                .then(|| *unsafe { self.inner.key().downcast::<i32>() });
            if before_key.is_none() || before_key != after_key {
                return;
            }
            self.inner.rewind_vals();
            match before_val {
                Some(v) => self
                    .inner
                    .seek_val_with(&|p: &Pair| *unsafe { p.downcast::<Tup2<i32, u32>>() } >= v),
                // The values were already exhausted; leave them so.
                None => self.inner.seek_val_with(&|_| false),
            }
        }
    }

    impl<C> Cursor<DynData, Pair, (), DynZWeight> for LazyResetCursor<C>
    where
        C: Cursor<DynData, Pair, (), DynZWeight>,
    {
        fn weight_factory(&self) -> &'static dyn Factory<DynZWeight> {
            self.inner.weight_factory()
        }
        fn key_valid(&self) -> bool {
            self.inner.key_valid()
        }
        fn val_valid(&self) -> bool {
            self.inner.val_valid()
        }
        fn key(&self) -> &DynData {
            self.inner.key()
        }
        fn val(&self) -> &Pair {
            self.inner.val()
        }
        fn map_times(&mut self, logic: &mut dyn FnMut(&(), &DynZWeight)) {
            self.inner.map_times(logic)
        }
        fn map_times_through(&mut self, u: &(), logic: &mut dyn FnMut(&(), &DynZWeight)) {
            self.inner.map_times_through(u, logic)
        }
        fn weight(&mut self) -> &DynZWeight {
            self.inner.weight()
        }
        fn weight_checked(&mut self) -> &DynZWeight {
            self.inner.weight_checked()
        }
        fn map_values(&mut self, logic: &mut dyn FnMut(&Pair, &DynZWeight)) {
            self.inner.map_values(logic)
        }
        fn step_key(&mut self) {
            self.key_move(|c| c.step_key())
        }
        fn step_key_reverse(&mut self) {
            self.key_move(|c| c.step_key_reverse())
        }
        fn seek_key(&mut self, key: &DynData) {
            self.key_move(|c| c.seek_key(key))
        }
        fn seek_key_exact(&mut self, key: &DynData, hash: Option<u64>) -> bool {
            let mut found = false;
            self.key_move(|c| found = c.seek_key_exact(key, hash));
            found
        }
        fn seek_key_with(&mut self, p: &dyn Fn(&DynData) -> bool) {
            self.key_move(|c| c.seek_key_with(p))
        }
        fn seek_key_with_reverse(&mut self, p: &dyn Fn(&DynData) -> bool) {
            self.key_move(|c| c.seek_key_with_reverse(p))
        }
        fn seek_key_reverse(&mut self, key: &DynData) {
            self.key_move(|c| c.seek_key_reverse(key))
        }
        fn step_val(&mut self) {
            self.inner.step_val()
        }
        fn step_val_reverse(&mut self) {
            self.inner.step_val_reverse()
        }
        fn seek_val(&mut self, val: &Pair) {
            self.inner.seek_val(val)
        }
        fn seek_val_reverse(&mut self, val: &Pair) {
            self.inner.seek_val_reverse(val)
        }
        fn seek_val_with(&mut self, p: &dyn Fn(&Pair) -> bool) {
            self.inner.seek_val_with(p)
        }
        fn seek_val_with_reverse(&mut self, p: &dyn Fn(&Pair) -> bool) {
            self.inner.seek_val_with_reverse(p)
        }
        fn rewind_keys(&mut self) {
            self.key_move(|c| c.rewind_keys())
        }
        fn fast_forward_keys(&mut self) {
            self.key_move(|c| c.fast_forward_keys())
        }
        fn rewind_vals(&mut self) {
            self.inner.rewind_vals()
        }
        fn fast_forward_vals(&mut self) {
            self.inner.fast_forward_vals()
        }
        fn position(&self) -> Option<Position> {
            self.inner.position()
        }
    }

    fn lazy_cursor(batch: &Batched) -> impl Cursor<DynData, DynData, (), DynZWeight> + '_ {
        ProjectedValCursor::new(
            LazyResetCursor::new(batch.cursor()),
            <DynData as WithFactory<i32>>::FACTORY,
        )
    }

    /// The same expectations as `seek_key_to_current_key_keeps_values` and
    /// `key_rewind_restarts_values_when_the_key_does_not_move`, but over an
    /// inner that resets values only when the key really changes. Against the
    /// real backends those two pass even with the guards deleted.
    #[test]
    fn key_moves_restart_values_on_a_minimally_conforming_inner() {
        let batch = build(&[(1, 10, 0, 1), (1, 20, 0, 2), (2, 30, 0, 4)]);

        let mut c = lazy_cursor(&batch);
        c.step_val();
        assert_eq!(*unsafe { c.val().downcast::<i32>() }, 20);
        c.seek_key(1i32.erase());
        assert_eq!(*unsafe { c.val().downcast::<i32>() }, 10, "seek_key");

        let mut c = lazy_cursor(&batch);
        c.step_val();
        c.rewind_keys();
        assert_eq!(*unsafe { c.val().downcast::<i32>() }, 10, "rewind_keys");

        let mut c = lazy_cursor(&batch);
        c.step_val();
        c.seek_key_with(&|k: &DynData| *unsafe { k.downcast::<i32>() } >= 1);
        assert_eq!(*unsafe { c.val().downcast::<i32>() }, 10, "seek_key_with");

        // The last key needs two values for `step_val` to land on a second one.
        let batch = build(&[(1, 10, 0, 1), (2, 30, 0, 4), (2, 40, 0, 5)]);
        let mut c = lazy_cursor(&batch);
        c.fast_forward_keys();
        assert_eq!(*unsafe { c.val().downcast::<i32>() }, 30);
        c.step_val();
        assert_eq!(*unsafe { c.val().downcast::<i32>() }, 40);
        c.fast_forward_keys();
        assert_eq!(
            *unsafe { c.val().downcast::<i32>() },
            30,
            "fast_forward_keys"
        );
    }

    /// A full walk over the minimally conforming inner still matches the oracle.
    ///
    /// A sanity check on the wrapper rather than a control: a forward walk only
    /// ever moves to a different key, so it never exercises the no-op key move
    /// the guards exist for.
    #[test]
    fn walks_correctly_over_a_minimally_conforming_inner() {
        let rows: &[Row] = &[
            (1, 10, 0, 1),
            (1, 10, 1, 2),
            (1, 20, 0, 4),
            (2, 30, 0, 1),
            (2, 30, 1, -1),
            (3, 40, 0, 8),
        ];
        let batch = build(rows);
        assert_eq!(walk_forward(&mut lazy_cursor(&batch)), oracle(rows));
    }

    // ---------------------------------------------------------------- model

    /// A plain batch holding exactly what the projection should yield, so its
    /// cursor is a reference implementation to compare against.
    fn reference(rows: &[Row]) -> VecIndexedWSet<DynData, DynData, DynZWeight> {
        let factories =
            <VecIndexedWSetFactories<DynData, DynData, DynZWeight>>::new::<i32, i32, ZWeight>();
        let tuples: Vec<Tup2<Tup2<i32, i32>, ZWeight>> = oracle(rows)
            .into_iter()
            .map(|(k, v, w)| Tup2(Tup2(k, v), w))
            .collect();
        let mut tuples = Box::new(LeanVec::from(tuples)).erase_box();
        VecIndexedWSet::dyn_from_tuples(&factories, (), &mut tuples)
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Dir {
        Fwd,
        Bwd,
    }

    #[derive(Debug, Clone, Copy)]
    enum Op {
        StepKey,
        StepKeyRev,
        SeekKey(i32),
        SeekKeyRev(i32),
        SeekKeyExact(i32),
        SeekKeyWith(i32),
        SeekKeyWithRev(i32),
        RewindKeys,
        FastForwardKeys,
        StepVal,
        StepValRev,
        SeekVal(i32),
        SeekValRev(i32),
        SeekValWith(i32),
        SeekValWithRev(i32),
        RewindVals,
        FastForwardVals,
    }

    impl Op {
        /// The `Cursor` contract fixes a key and a value direction, and only
        /// some methods are meaningful in each. Sequences that mix them are
        /// caller errors, so the model does not generate them.
        fn legal(self, keys: Dir, vals: Dir) -> bool {
            match self {
                Op::StepKey | Op::SeekKey(_) | Op::SeekKeyExact(_) | Op::SeekKeyWith(_) => {
                    keys == Dir::Fwd
                }
                Op::StepKeyRev | Op::SeekKeyRev(_) | Op::SeekKeyWithRev(_) => keys == Dir::Bwd,
                Op::StepVal | Op::SeekVal(_) | Op::SeekValWith(_) => vals == Dir::Fwd,
                Op::StepValRev | Op::SeekValRev(_) | Op::SeekValWithRev(_) => vals == Dir::Bwd,
                Op::RewindKeys | Op::FastForwardKeys | Op::RewindVals | Op::FastForwardVals => true,
            }
        }

        /// Directions after applying this op. Moving to a new key resets the
        /// value direction to forward.
        fn advance(self, keys: Dir, vals: Dir) -> (Dir, Dir) {
            match self {
                Op::RewindKeys => (Dir::Fwd, Dir::Fwd),
                Op::FastForwardKeys => (Dir::Bwd, Dir::Fwd),
                Op::StepKey
                | Op::StepKeyRev
                | Op::SeekKey(_)
                | Op::SeekKeyRev(_)
                | Op::SeekKeyExact(_)
                | Op::SeekKeyWith(_)
                | Op::SeekKeyWithRev(_) => (keys, Dir::Fwd),
                Op::RewindVals => (keys, Dir::Fwd),
                Op::FastForwardVals => (keys, Dir::Bwd),
                _ => (keys, vals),
            }
        }
    }

    /// Applies `op` and returns what a `seek_key_exact` reported, if it was one.
    fn apply<C: Cursor<DynData, DynData, (), DynZWeight>>(c: &mut C, op: Op) -> Option<bool> {
        match op {
            Op::StepKey => c.step_key(),
            Op::StepKeyRev => c.step_key_reverse(),
            Op::SeekKey(k) => c.seek_key(k.erase()),
            Op::SeekKeyRev(k) => c.seek_key_reverse(k.erase()),
            Op::SeekKeyExact(k) => return Some(c.seek_key_exact(k.erase(), None)),
            Op::SeekKeyWith(k) => {
                c.seek_key_with(&|x: &DynData| *unsafe { x.downcast::<i32>() } >= k)
            }
            Op::SeekKeyWithRev(k) => {
                c.seek_key_with_reverse(&|x: &DynData| *unsafe { x.downcast::<i32>() } <= k)
            }
            Op::RewindKeys => c.rewind_keys(),
            Op::FastForwardKeys => c.fast_forward_keys(),
            Op::StepVal => c.step_val(),
            Op::StepValRev => c.step_val_reverse(),
            Op::SeekVal(v) => c.seek_val(v.erase()),
            Op::SeekValRev(v) => c.seek_val_reverse(v.erase()),
            Op::SeekValWith(v) => {
                c.seek_val_with(&|x: &DynData| *unsafe { x.downcast::<i32>() } >= v)
            }
            Op::SeekValWithRev(v) => {
                c.seek_val_with_reverse(&|x: &DynData| *unsafe { x.downcast::<i32>() } <= v)
            }
            Op::RewindVals => c.rewind_vals(),
            Op::FastForwardVals => c.fast_forward_vals(),
        }
        None
    }

    fn observe<C: Cursor<DynData, DynData, (), DynZWeight>>(
        c: &mut C,
    ) -> (bool, Option<i32>, bool, Option<i32>, Option<ZWeight>) {
        let kv = c.key_valid();
        let k = kv.then(|| *unsafe { c.key().downcast::<i32>() });
        // Values only carry meaning at a valid key: implementations differ on
        // what `val_valid` reports once the keys are exhausted.
        let vv = kv && c.val_valid();
        let v = vv.then(|| *unsafe { c.val().downcast::<i32>() });
        let w = vv.then(|| **c.weight());
        (kv, k, vv, v, w)
    }

    /// Picks one operation uniformly, with targets that span the data domain
    /// and fall outside it on both sides.
    fn random_op<R: rand::Rng>(rng: &mut R) -> Op {
        let target = rng.gen_range(-1..5);
        match rng.gen_range(0..17) {
            0 => Op::StepKey,
            1 => Op::StepKeyRev,
            2 => Op::SeekKey(target),
            3 => Op::SeekKeyRev(target),
            4 => Op::SeekKeyExact(target),
            5 => Op::SeekKeyWith(target),
            6 => Op::SeekKeyWithRev(target),
            7 => Op::RewindKeys,
            8 => Op::FastForwardKeys,
            9 => Op::StepVal,
            10 => Op::StepValRev,
            11 => Op::SeekVal(target),
            12 => Op::SeekValRev(target),
            13 => Op::SeekValWith(target),
            14 => Op::SeekValWithRev(target),
            15 => Op::RewindVals,
            _ => Op::FastForwardVals,
        }
    }

    /// Applies `ops` to both cursors, skipping those illegal in the current
    /// direction, and compares observable state after each.
    fn drive<A, B>(actual: &mut A, expected: &mut B, ops: &[Op], rows: &[Row], case: usize)
    where
        A: Cursor<DynData, DynData, (), DynZWeight>,
        B: Cursor<DynData, DynData, (), DynZWeight>,
    {
        assert_eq!(
            observe(actual),
            observe(expected),
            "case {case}: initial position on rows {rows:?}"
        );

        let (mut keys, mut vals) = (Dir::Fwd, Dir::Fwd);
        for (i, &op) in ops.iter().enumerate() {
            if !op.legal(keys, vals) {
                continue;
            }
            let a = apply(actual, op);
            let e = apply(expected, op);
            assert_eq!(a, e, "case {case} op {i} {op:?}: seek_key_exact result");

            if matches!(op, Op::SeekKeyExact(_)) {
                // `seek_key_exact` may only be followed by another for a greater
                // key, or by a rewind (cursor.rs:216-226), and where it leaves an
                // unsuccessful lookup is not specified. Compare the answer, then
                // reset, which is the contract's own escape hatch.
                apply(actual, Op::RewindKeys);
                apply(expected, Op::RewindKeys);
                (keys, vals) = (Dir::Fwd, Dir::Fwd);
            } else {
                assert_eq!(
                    observe(actual),
                    observe(expected),
                    "case {case} after op {i} {op:?} on rows {rows:?}"
                );
                (keys, vals) = op.advance(keys, vals);
            }
        }
    }

    /// Drives the projecting cursor and a plain cursor over the materialized
    /// projection through the same legal op sequence, comparing after each.
    fn run_model(rows: &[Row], ops: &[Op]) {
        let batch = build(rows);
        let reference = reference(rows);
        let mut actual = cursor(&batch);
        let mut expected = reference.cursor();
        drive(&mut actual, &mut expected, ops, rows, 0);
    }

    fn rows() -> impl Strategy<Value = Vec<Row>> {
        // A small domain so runs, cancellations and vanishing keys all occur.
        vec((0i32..4, 0i32..4, 0u32..3, -2i64..3), 0..40)
    }

    fn ops() -> impl Strategy<Value = Vec<Op>> {
        // Targets span the data domain and fall outside it on both sides, so
        // seeks land before the first key, past the last, and between runs.
        let target = -1i32..5;
        let op = prop_oneof![
            Just(Op::StepKey),
            Just(Op::StepKeyRev),
            target.clone().prop_map(Op::SeekKey),
            target.clone().prop_map(Op::SeekKeyRev),
            target.clone().prop_map(Op::SeekKeyExact),
            target.clone().prop_map(Op::SeekKeyWith),
            target.clone().prop_map(Op::SeekKeyWithRev),
            Just(Op::RewindKeys),
            Just(Op::FastForwardKeys),
            Just(Op::StepVal),
            Just(Op::StepValRev),
            target.clone().prop_map(Op::SeekVal),
            target.clone().prop_map(Op::SeekValRev),
            target.clone().prop_map(Op::SeekValWith),
            target.prop_map(Op::SeekValWithRev),
            Just(Op::RewindVals),
            Just(Op::FastForwardVals),
        ];
        vec(op, 0..24)
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(512))]

        #[test]
        fn matches_oracle_in_every_direction(rows in rows()) {
            check(&rows);
        }

        /// The strongest property available: for any data and any legal
        /// sequence of cursor operations, the projecting cursor is
        /// indistinguishable from a plain cursor over the projected data.
        #[test]
        fn indistinguishable_from_a_materialized_projection(
            rows in rows(),
            ops in ops(),
        ) {
            run_model(&rows, &ops);
        }
    }
}
