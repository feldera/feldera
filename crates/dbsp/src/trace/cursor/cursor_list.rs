//! A generic cursor implementation merging multiple cursors.

use crate::dynamic::{DataTrait, Factory, WeightTrait};
use crate::trace::cursor::Position;
use crate::utils::binary_heap::BinaryHeap;
use std::marker::PhantomData;
use std::mem::{take, transmute};
use std::{any::TypeId, cmp::Ordering};

use super::{Cursor, Direction};

/// Provides a cursor interface over a list of cursors.
///
/// The `CursorList` tracks the indices of cursors with the minimum key, and
/// the indices of cursors with the minimum key and minimum value.
///
/// # Design
///
/// The implementation uses a binary heap to keep the cursors partially
/// sorted by key and value. This allows us to find the new min/max
/// key/value in logarithmic time compared to linear time for a naive linear scan.
///
/// Why binary heap and not a tournament tree? Tournament trees are generally better
/// for merge sorting, as they require exactly log(n) comparisons to find the new
/// winner vs the worst-case 2 x log(n) comparisons for a binary heap. The problem
/// with tournament trees is that they don't offer an efficient way to peek _all_
/// max/min values, and not just one (you need to remove or update the current max first
/// and pay another log(n) comparison to find the next max). In contrast, our customized
/// binary heap implementation offers an efficient way to peek all max/min values at once.
pub struct CursorList<K, V, T, R: WeightTrait, C>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    T: 'static,
    R: WeightTrait + ?Sized,
    C: Cursor<K, V, T, R>,
{
    cursors: Vec<C>,

    /// Indexes of cursors that hold the current minimum key.
    current_key: Vec<usize>,

    /// Indexes of cursors in `current_key` in `key_priority_heap`.
    current_key_indexes: Vec<usize>,

    /// A priority heap that keeps cursors partially sorted by key.
    /// The first element is the index of the cursor in `cursors`,
    /// the second element is a pointer to the key. It should be equal
    /// to `cursors[index].key()`.
    ///
    /// # Safety
    ///
    /// We assume that the key reference returned by the cursor remains valid
    /// until the cursor moves to the next key. This is not officially part of the
    /// cursor API and I hope we can find a more strongly typed way to achieve this;
    /// however both in-memory and file-backed batches we have today are well-behaved
    /// in this regard.
    key_priority_heap: Vec<(usize, &'static K)>,

    /// Indexes of cursors that hold the current minimum value.
    current_val: Vec<usize>,

    /// Indexes of cursors in `current_val` in `val_priority_heap`.
    current_val_indexes: Vec<usize>,

    /// A priority heap that keeps cursors partially sorted by value.
    val_priority_heap: Vec<(usize, &'static V)>,

    /// The direction is None after calling `seek_key_exact`. In this case we don't sort the cursors by key.
    /// step_key, seek_key, etc., cannot be called on such a cursor.
    #[cfg(debug_assertions)]
    key_direction: Option<Direction>,
    #[cfg(debug_assertions)]
    val_direction: Direction,
    weight: Box<R>,
    /// Scratch space for use by `peek_all`.
    scratch: Vec<usize>,
    weight_factory: &'static dyn Factory<R>,
    __type: PhantomData<fn(&K, &V, &T, &R)>,
}

impl<K, V, T, R, C> CursorList<K, V, T, R, C>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    C: Cursor<K, V, T, R>,
    T: 'static,
{
    /// Creates a new cursor list from pre-existing cursors.
    pub fn new(weight_factory: &'static dyn Factory<R>, cursors: Vec<C>) -> Self {
        let num_cursors = cursors.len();
        let mut result = Self {
            cursors,
            current_key: Vec::new(),
            current_key_indexes: Vec::new(),
            key_priority_heap: Vec::with_capacity(num_cursors),
            current_val: Vec::new(),
            current_val_indexes: Vec::new(),
            val_priority_heap: Vec::with_capacity(num_cursors),
            #[cfg(debug_assertions)]
            key_direction: Some(Direction::Forward),
            #[cfg(debug_assertions)]
            val_direction: Direction::Forward,
            weight: weight_factory.default_box(),
            weight_factory,
            scratch: Vec::new(),
            __type: PhantomData,
        };

        result.minimize_keys();

        result.skip_zero_weight_keys_forward();

        result
    }

    #[cfg(debug_assertions)]
    fn set_key_direction(&mut self, direction: Option<Direction>) {
        self.key_direction = direction;
    }

    #[cfg(not(debug_assertions))]
    fn set_key_direction(&mut self, _direction: Option<Direction>) {}

    #[cfg(debug_assertions)]
    fn set_val_direction(&mut self, direction: Direction) {
        self.val_direction = direction;
    }

    #[cfg(not(debug_assertions))]
    fn set_val_direction(&mut self, _direction: Direction) {}

    #[cfg(debug_assertions)]
    fn assert_key_direction(&self, direction: Direction) {
        debug_assert_eq!(self.key_direction, Some(direction));
    }

    #[cfg(not(debug_assertions))]
    fn assert_key_direction(&self, _direction: Direction) {}

    #[cfg(debug_assertions)]
    fn assert_val_direction(&self, direction: Direction) {
        debug_assert_eq!(self.val_direction, direction);
    }

    #[cfg(not(debug_assertions))]
    fn assert_val_direction(&self, _direction: Direction) {}

    fn is_zero_weight(&mut self) -> bool {
        if TypeId::of::<T>() == TypeId::of::<()>() {
            debug_assert!(self.key_valid());
            debug_assert!(self.val_valid());
            debug_assert!(self.cursors[self.current_val[0]].val_valid());
            self.weight.as_mut().set_zero();
            for &index in self.current_val.iter() {
                // TODO: use weight_checked
                self.cursors[index].map_times(&mut |_, w| self.weight.add_assign(w));
            }
            self.weight.is_zero()
        } else {
            false
        }
    }

    fn skip_zero_weight_vals_forward(&mut self) {
        self.assert_val_direction(Direction::Forward);

        while self.val_valid() && self.is_zero_weight() {
            for &index in self.current_val.iter() {
                self.cursors[index].step_val();
            }
            self.update_min_vals();
        }
    }

    fn skip_zero_weight_vals_reverse(&mut self) {
        self.assert_val_direction(Direction::Backward);

        while self.val_valid() && self.is_zero_weight() {
            for &index in self.current_val.iter() {
                self.cursors[index].step_val_reverse();
            }
            self.update_max_vals();
        }
    }

    fn skip_zero_weight_keys_forward(&mut self) {
        self.assert_key_direction(Direction::Forward);

        while self.key_valid() {
            self.skip_zero_weight_vals_forward();
            if self.val_valid() {
                break;
            }
            for &index in self.current_key.iter() {
                self.cursors[index].step_key();
            }
            self.set_val_direction(Direction::Forward);
            self.update_min_keys();
        }
    }

    fn skip_zero_weight_keys_reverse(&mut self) {
        self.assert_key_direction(Direction::Backward);

        while self.key_valid() {
            self.skip_zero_weight_vals_forward();
            if self.val_valid() {
                break;
            }
            for &index in self.current_key.iter() {
                self.cursors[index].step_key_reverse();
            }
            self.set_val_direction(Direction::Forward);
            self.update_max_keys();
        }
    }

    /// Find all cursors that point to the maximum (according to `comparator`) key and store the indexes in `current_key`.
    ///
    /// Uses naive linear scan to find the maximum key by performing num_cursors comparisons.
    /// This is a fallback used for small number of cursors where a linear scan is faster than
    /// maintaining the binary heap.
    fn current_keys_linear(&mut self, comparator: impl Fn(&K, &K) -> Ordering) {
        self.current_key.clear();

        // Determine the index of the cursor with minimum key.
        let mut max_key_opt: Option<&K> = None;
        for (index, cursor) in self.cursors.iter().enumerate() {
            if let Some(key) = cursor.get_key() {
                if let Some(max_key_opt) = &mut max_key_opt {
                    match (comparator)(key, max_key_opt) {
                        Ordering::Greater => {
                            *max_key_opt = key;
                            self.current_key.clear();
                            self.current_key.push(index);
                        }
                        Ordering::Equal => {
                            self.current_key.push(index);
                        }
                        _ => (),
                    }
                } else {
                    max_key_opt = Some(key);
                    self.current_key.push(index);
                }
            }
        }
    }

    /// Update current_key to contain the indexes of cursors with the maximum (according to `comparator`) key.
    ///
    /// If `rebuild` is true, assumes that all cursors have moved from their previous positions.
    /// Otherwise, it assumes that only the cursors in `current_key` have moved.
    ///
    /// The implementation uses maximize_keys_linear for a small number of cursors. For larger numbers of cursors,
    /// it uses a binary heap to keep the cursors partially sorted by key.
    fn sort_keys(&mut self, comparator: impl Fn(&K, &K) -> Ordering, rebuild: bool) {
        self.assert_val_direction(Direction::Forward);

        // Use linear scan for small number of cursors.
        //
        // Binary heap cost:
        //
        // if rebuild is true:
        //   cost <= 2*num_cursors + 2.
        // else:
        //   costs <= 2*log2(num_cursors) + 2.
        //
        // The +2 component above is the cost of peek_all assuming a single max element;
        // in general it's 2x the number of max elements.
        //
        // Linear scan cost: num_cursors-1 in both cases.
        //
        // We pick 5 as a break-even point beyond which the binary heap is faster.
        match self.cursors.len() {
            0 => {
                self.current_key.clear();
                return;
            }
            1 => {
                self.current_key.clear();
                // SAFETY: `cursors.len() = 1`.
                if self.cursors[0].key_valid() {
                    self.current_key.push(0);
                }
                return;
            }
            n if n <= 5 => {
                self.current_keys_linear(comparator);
                return;
            }
            _ => {}
        }

        let cmp = |a: &(usize, &'static K), b: &(usize, &'static K)| comparator(a.1, b.1);

        let heap = if rebuild {
            // Build heap from scratch.
            self.key_priority_heap.clear();
            for (i, cursor) in self.cursors.iter().enumerate() {
                if let Some(key) = cursor.get_key() {
                    // SAFETY: We will not access the key after the cursor is moved.
                    self.key_priority_heap
                        .push((i, unsafe { transmute::<&K, &'static K>(key) }))
                }
            }

            BinaryHeap::<(usize, &'static K), _>::from_vec(take(&mut self.key_priority_heap), cmp)
        } else {
            // Start with a previously built heap.
            let mut heap = unsafe {
                BinaryHeap::<(usize, &'static K), _>::from_vec_unchecked(
                    take(&mut self.key_priority_heap),
                    cmp,
                )
            };

            // We may have moved cursors in `current_key` since the last time we built the heap
            // and need to update the heap accordingly.
            // IMPORTANT: we iterate over the indexes returned by peek_all in reverse order, which
            // guarantees the modifying or removing the elements does not affect the indexes of the
            // remaining elements.
            for (pos, i) in self
                .current_key_indexes
                .iter()
                .rev()
                .zip(self.current_key.iter().rev())
            {
                // If the cursor still points to a valid key, update the key in the heap; otherwise,
                // remove the cursor from the heap.
                if let Some(key) = unsafe { self.cursors.get_unchecked(*i).get_key() } {
                    // SAFETY: We know that `index` is in bounds since it's in `current_key`.
                    // SAFETY: We will not access the key after the cursor is moved.
                    unsafe {
                        heap.update_pos_sift_down(*pos, ((*i), transmute::<&K, &'static K>(key)))
                    };
                } else {
                    heap.remove(*pos);
                }
            }

            heap
        };

        self.current_key.clear();
        self.current_key_indexes.clear();

        // Find the new set of cursors with the maximum key and record them in `current_key` and `current_key_indexes`,
        // without removing them from the heap. This is 2x more efficient that removing them and re-inserting during the
        // next call to sort_keys.
        heap.peek_all(
            |pos, &(i, _)| {
                self.current_key.push(i);
                self.current_key_indexes.push(pos);
            },
            &mut self.scratch,
        );

        self.key_priority_heap = heap.into_vec();
    }

    /// Find all cursors that point to the maximum (according to `comparator`) value and store the indexes in `current_val`.
    ///
    /// Uses naive linear scan to find the maximum value by performing current_key.len() comparisons.
    /// This is a fallback used for small number of cursors where a linear scan is faster than
    /// maintaining the binary heap.
    fn current_vals_linear(&mut self, comparator: impl Fn(&V, &V) -> Ordering) {
        self.current_val.clear();

        let mut max_val: Option<&V> = None;
        for &index in self.current_key.iter() {
            // SAFETY: `index` is in bounds since it's in `current_key`.
            if let Some(val) = unsafe { self.cursors.get_unchecked(index).get_val() } {
                if let Some(max_val) = &mut max_val {
                    match (comparator)(val, max_val) {
                        Ordering::Greater => {
                            *max_val = val;
                            self.current_val.clear();
                            self.current_val.push(index);
                        }
                        Ordering::Equal => self.current_val.push(index),
                        _ => (),
                    }
                } else {
                    max_val = Some(val);
                    self.current_val.push(index);
                }
            }
        }
    }

    /// Update `current_val` to contain the indexes of cursors with the maximum (according to `comparator`) values.
    ///
    /// If `rebuild` is true, assumes that all cursors have moved from their previous positions.
    /// Otherwise, it assumes that only the cursors in `current_val` have moved.
    ///
    /// The implementation uses maximize_vals_linear for a small number of cursors. For larger numbers of cursors,
    /// it uses a binary heap to keep the cursors partially sorted by value.
    fn sort_vals(&mut self, comparator: impl Fn(&V, &V) -> Ordering, rebuild: bool) {
        debug_assert!(
            self.current_key
                .iter()
                .all(|i| self.cursors[*i].key_valid())
        );

        match self.current_key.len() {
            0 => {
                self.current_val.clear();
                return;
            }
            1 => {
                self.current_val.clear();
                let i = unsafe { *self.current_key.get_unchecked(0) };
                if unsafe { self.cursors.get_unchecked(i) }.val_valid() {
                    self.current_val.push(i);
                }
                return;
            }
            n if n <= 5 => {
                self.current_vals_linear(comparator);
                return;
            }
            _ => {}
        }

        let cmp = |a: &(usize, &'static V), b: &(usize, &'static V)| comparator(a.1, b.1);

        let heap = if rebuild {
            self.val_priority_heap.clear();
            for i in self.current_key.iter() {
                if let Some(val) = unsafe { self.cursors.get_unchecked(*i).get_val() } {
                    // SAFETY: We will not access the value after the cursor is moved.
                    self.val_priority_heap
                        .push(((*i), unsafe { transmute::<&V, &'static V>(val) }));
                }
            }
            BinaryHeap::<(usize, &'static V), _>::from_vec(take(&mut self.val_priority_heap), cmp)
        } else {
            let mut heap = unsafe {
                BinaryHeap::<(usize, &'static V), _>::from_vec_unchecked(
                    take(&mut self.val_priority_heap),
                    cmp,
                )
            };

            for (pos, i) in self
                .current_val_indexes
                .iter()
                .rev()
                .zip(self.current_val.iter().rev())
            {
                if let Some(val) = unsafe { self.cursors.get_unchecked(*i).get_val() } {
                    // SAFETY: We will not access the value after the cursor is moved.
                    unsafe {
                        heap.update_pos_sift_down(*pos, ((*i), transmute::<&V, &'static V>(val)))
                    };
                } else {
                    heap.remove(*pos);
                }
            }

            heap
        };

        self.current_val.clear();
        self.current_val_indexes.clear();
        heap.peek_all(
            |pos, &(i, _)| {
                self.current_val.push(i);
                self.current_val_indexes.push(pos);
            },
            &mut self.scratch,
        );

        self.val_priority_heap = heap.into_vec();
    }

    /// Sort all cursors by key (ascending); initialize current_key with the indices of cursors with the minimum key.
    ///
    /// Invoked after calling seek_key, seek_key_with, or rewind_keys on all cursors.
    ///
    /// Once finished, it invokes `minimize_vals()` to ensure the value cursor is
    /// in a consistent state as well.
    fn minimize_keys(&mut self) {
        self.assert_key_direction(Direction::Forward);

        self.sort_keys(|c1, c2| c2.cmp(c1), true);
        self.minimize_vals();
    }

    /// Sort all cursors by key (descending); initialize current_key with the indices of cursors with the maximum key.
    ///
    /// Invoked after calling seek_key_reverse, seek_key_with_reverse, or fast_forward_keys on all cursors.
    ///
    /// Once finished, it invokes `minimize_vals()` to ensure the value cursor is
    /// in a consistent state as well.
    fn maximize_keys(&mut self) {
        self.assert_key_direction(Direction::Backward);

        self.sort_keys(|c1, c2| c1.cmp(c2), true);
        self.minimize_vals();
    }

    /// Sort all cursors is self.current_key by values (ascending); initialize current_val with the
    /// indices of cursors with the minimum value.
    fn minimize_vals(&mut self) {
        self.assert_val_direction(Direction::Forward);

        self.sort_vals(|c1, c2| c2.cmp(c1), true);
    }

    /// Sort all cursors is self.current_key by values (descending); initialize current_val with the
    /// indices of cursors with the maximum value.
    fn maximize_vals(&mut self) {
        self.assert_val_direction(Direction::Backward);

        self.sort_vals(|c1, c2| c1.cmp(c2), true);
    }

    /// Update the current_key array after stepping all cursors in self.current_key.
    ///
    /// Assumes that cursors in key_priority_heap are valid and sorted by key (ascending).
    ///
    /// Inserts valid cursors in current_key to key_priority_heap and pops the new min key cursors
    /// from key_priority_heap into current_key.
    fn update_min_keys(&mut self) {
        self.assert_key_direction(Direction::Forward);

        self.sort_keys(|c1, c2| c2.cmp(c1), false);
        self.minimize_vals();
    }

    /// Update the current_key array after stepping all cursors in self.current_key in reverse.
    ///
    /// Assumes that cursors in key_priority_heap are valid and sorted by key (descending).
    ///
    /// Inserts valid cursors in current_key to key_priority_heap and pops the new max key cursors
    /// from key_priority_heap into current_key.
    fn update_max_keys(&mut self) {
        self.assert_key_direction(Direction::Backward);

        self.sort_keys(|c1, c2| c1.cmp(c2), false);
        self.minimize_vals();
    }

    /// Update the current_val array after stepping all cursors in self.current_val with step_val.
    ///
    /// Assumes that cursors in val_priority_heap are valid and sorted by value (ascending).
    ///
    /// Inserts valid cursors in current_val to val_priority_heap and pops the new min value cursors
    /// from val_priority_heap into current_val.
    fn update_min_vals(&mut self) {
        self.assert_val_direction(Direction::Forward);
        self.sort_vals(|c1, c2| c2.cmp(c1), false);
    }

    /// Update the current_val array after stepping all cursors in self.current_val with step_val_reverse.
    ///
    /// Assumes that cursors in val_priority_heap are valid and sorted by value (descending).
    ///
    /// Inserts valid cursors in current_val to val_priority_heap and pops the new max value cursors
    /// from val_priority_heap into current_val.
    fn update_max_vals(&mut self) {
        self.assert_val_direction(Direction::Backward);
        self.sort_vals(|c1, c2| c1.cmp(c2), false);
    }
}

impl<K, V, T, R, C: Cursor<K, V, T, R>> Cursor<K, V, T, R> for CursorList<K, V, T, R, C>
where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
    T: 'static,
{
    fn weight_factory(&self) -> &'static dyn Factory<R> {
        self.weight_factory
    }

    fn key_valid(&self) -> bool {
        !self.current_key.is_empty()
    }

    fn val_valid(&self) -> bool {
        !self.current_val.is_empty()
    }

    fn key(&self) -> &K {
        debug_assert!(self.key_valid());
        debug_assert!(self.cursors[self.current_key[0]].key_valid());
        self.cursors[self.current_key[0]].key()
    }

    fn val(&self) -> &V {
        debug_assert!(self.key_valid());
        debug_assert!(self.val_valid());
        debug_assert!(self.cursors[self.current_val[0]].val_valid());
        self.cursors[self.current_val[0]].val()
    }

    fn map_times(&mut self, logic: &mut dyn FnMut(&T, &R)) {
        debug_assert!(self.key_valid());
        debug_assert!(self.val_valid());
        for &index in self.current_val.iter() {
            self.cursors[index].map_times(logic);
        }
    }

    fn map_times_through(&mut self, upper: &T, logic: &mut dyn FnMut(&T, &R)) {
        debug_assert!(self.key_valid());
        debug_assert!(self.val_valid());
        for &index in self.current_val.iter() {
            self.cursors[index].map_times_through(upper, logic);
        }
    }

    fn weight(&mut self) -> &R
    where
        T: PartialEq<()>,
    {
        self.weight_checked()
    }

    fn weight_checked(&mut self) -> &R {
        if TypeId::of::<T>() == TypeId::of::<()>() {
            debug_assert!(self.key_valid());
            debug_assert!(self.val_valid());
            debug_assert!(self.cursors[self.current_val[0]].val_valid());

            // Weight should already be computed by `is_zero_weight`, which is always
            // called as part of every operation that moves the cursor.
            debug_assert!(!self.weight.is_zero());

            &self.weight
        } else {
            panic!("CursorList::weight_checked called on non-unit timestamp type");
        }
    }

    fn map_values(&mut self, logic: &mut dyn FnMut(&V, &R))
    where
        T: PartialEq<()>,
    {
        debug_assert!(self.key_valid());
        while self.val_valid() {
            let val = self.val();
            logic(val, &self.weight);
            self.step_val();
        }
    }

    fn step_key(&mut self) {
        self.assert_key_direction(Direction::Forward);

        for &index in self.current_key.iter() {
            debug_assert!(self.cursors[index].key_valid());
            self.cursors[index].step_key();
        }

        self.set_val_direction(Direction::Forward);
        self.update_min_keys();
        self.skip_zero_weight_keys_forward();
    }

    fn step_key_reverse(&mut self) {
        self.assert_key_direction(Direction::Backward);

        for &index in self.current_key.iter() {
            debug_assert!(self.cursors[index].key_valid());
            self.cursors[index].step_key_reverse();
        }

        self.set_val_direction(Direction::Forward);
        self.update_max_keys();
        self.skip_zero_weight_keys_reverse();
    }

    fn seek_key(&mut self, key: &K) {
        self.assert_key_direction(Direction::Forward);

        for cursor in self.cursors.iter_mut() {
            cursor.seek_key(key);
        }

        self.set_val_direction(Direction::Forward);
        self.minimize_keys();
        self.skip_zero_weight_keys_forward();
    }

    fn seek_key_exact(&mut self, key: &K, hash: Option<u64>) -> bool {
        self.set_key_direction(None);

        let hash = hash.unwrap_or_else(|| key.default_hash());
        self.current_key.clear();

        let mut result = false;

        for (index, cursor) in self.cursors.iter_mut().enumerate() {
            if cursor.seek_key_exact(key, Some(hash)) {
                self.current_key.push(index);
                result = true;
            }
        }

        self.set_val_direction(Direction::Forward);
        self.minimize_vals();

        if result {
            self.skip_zero_weight_vals_forward();
            self.val_valid()
        } else {
            false
        }
    }

    fn seek_key_with(&mut self, predicate: &dyn Fn(&K) -> bool) {
        self.assert_key_direction(Direction::Forward);

        for cursor in self.cursors.iter_mut() {
            cursor.seek_key_with(&predicate);
        }

        self.set_val_direction(Direction::Forward);
        self.minimize_keys();
        self.skip_zero_weight_keys_forward();
    }

    fn seek_key_with_reverse(&mut self, predicate: &dyn Fn(&K) -> bool) {
        self.assert_key_direction(Direction::Backward);

        for cursor in self.cursors.iter_mut() {
            cursor.seek_key_with_reverse(&predicate);
        }

        self.set_val_direction(Direction::Forward);
        self.maximize_keys();
        self.skip_zero_weight_keys_reverse();
    }

    fn seek_key_reverse(&mut self, key: &K) {
        self.assert_key_direction(Direction::Backward);

        for cursor in self.cursors.iter_mut() {
            cursor.seek_key_reverse(key);
        }

        self.set_val_direction(Direction::Forward);
        self.maximize_keys();
        self.skip_zero_weight_keys_reverse();
    }

    fn step_val(&mut self) {
        self.assert_val_direction(Direction::Forward);

        for &index in self.current_val.iter() {
            debug_assert!(self.cursors[index].key_valid());
            debug_assert!(self.cursors[index].val_valid());
            self.cursors[index].step_val();
        }
        self.update_min_vals();

        self.skip_zero_weight_vals_forward();
    }

    fn seek_val(&mut self, val: &V) {
        self.assert_val_direction(Direction::Forward);

        for &index in self.current_key.iter() {
            debug_assert!(self.cursors[index].key_valid());
            self.cursors[index].seek_val(val);
        }
        self.minimize_vals();
        self.skip_zero_weight_vals_forward();
    }

    fn seek_val_with(&mut self, predicate: &dyn Fn(&V) -> bool) {
        self.assert_val_direction(Direction::Forward);

        for &index in self.current_key.iter() {
            debug_assert!(self.cursors[index].key_valid());
            self.cursors[index].seek_val_with(predicate);
        }
        self.minimize_vals();
        self.skip_zero_weight_vals_forward();
    }

    fn rewind_keys(&mut self) {
        self.set_key_direction(Some(Direction::Forward));

        for cursor in self.cursors.iter_mut() {
            cursor.rewind_keys();
        }

        self.set_val_direction(Direction::Forward);
        self.minimize_keys();
        self.skip_zero_weight_keys_forward();
    }

    fn fast_forward_keys(&mut self) {
        self.set_key_direction(Some(Direction::Backward));

        for cursor in self.cursors.iter_mut() {
            cursor.fast_forward_keys();
        }

        self.set_val_direction(Direction::Forward);
        self.maximize_keys();
        self.skip_zero_weight_keys_reverse();
    }

    fn rewind_vals(&mut self) {
        for &index in self.current_key.iter() {
            self.cursors[index].rewind_vals();
        }

        self.set_val_direction(Direction::Forward);
        self.minimize_vals();
        self.skip_zero_weight_vals_forward();
    }

    fn step_val_reverse(&mut self) {
        self.assert_val_direction(Direction::Backward);

        for &index in self.current_val.iter() {
            debug_assert!(self.cursors[index].key_valid());
            debug_assert!(self.cursors[index].val_valid());
            self.cursors[index].step_val_reverse();
        }
        self.update_max_vals();
        self.skip_zero_weight_vals_reverse();
    }

    fn seek_val_reverse(&mut self, val: &V) {
        self.assert_val_direction(Direction::Backward);

        for &index in self.current_key.iter() {
            debug_assert!(self.cursors[index].key_valid());
            self.cursors[index].seek_val_reverse(val);
        }
        self.maximize_vals();
        self.skip_zero_weight_vals_reverse();
    }

    fn seek_val_with_reverse(&mut self, predicate: &dyn Fn(&V) -> bool) {
        self.assert_val_direction(Direction::Backward);

        for &index in self.current_key.iter() {
            debug_assert!(self.cursors[index].key_valid());
            self.cursors[index].seek_val_with_reverse(predicate);
        }
        self.maximize_vals();
        self.skip_zero_weight_vals_reverse();
    }

    fn fast_forward_vals(&mut self) {
        for &index in self.current_key.iter() {
            debug_assert!(self.cursors[index].key_valid());
            self.cursors[index].fast_forward_vals();
        }

        self.set_val_direction(Direction::Backward);
        self.maximize_vals();
        self.skip_zero_weight_vals_reverse();
    }

    fn position(&self) -> Option<Position> {
        let mut num_keys = 0;
        let mut current_key = 0;

        for cursor in self.cursors.iter() {
            let position = cursor.position().unwrap();
            num_keys += position.total;
            current_key += position.offset;
        }
        Some(Position {
            total: num_keys,
            offset: current_key,
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::IndexedZSetReader;
    use crate::utils::Tup2;
    use crate::{
        dynamic::DowncastTrait,
        indexed_zset,
        trace::{BatchReader, BatchReaderFactories},
    };
    use proptest::{collection::vec, prelude::*};

    pub type TestBatch = crate::OrdIndexedZSet<u64, u64>;

    /// Collect (key, value, weight) tuples from a cursor by iterating forward.
    pub fn cursor_to_tuples<C>(cursor: &mut C) -> Vec<(u64, u64, i64)>
    where
        C: Cursor<crate::dynamic::DynData, crate::dynamic::DynData, (), crate::DynZWeight>,
    {
        let mut result = Vec::new();
        while cursor.key_valid() {
            while cursor.val_valid() {
                let k = *cursor.key().downcast_checked::<u64>();
                let v = *cursor.val().downcast_checked::<u64>();
                let w = *cursor.weight().downcast_checked::<i64>();
                assert_ne!(w, 0);
                result.push((k, v, w));

                cursor.step_val();
            }
            cursor.step_key();
        }
        result
    }

    /// Collect tuples from batches via merge_batches_by_reference (uses cursor_to_tuples).
    pub fn merged_batch_to_tuples(batches: &[TestBatch]) -> Vec<(u64, u64, i64)> {
        if batches.is_empty() {
            return Vec::new();
        }

        let mut tuples = batches.iter().flat_map(|b| b.iter()).collect::<Vec<_>>();
        tuples.sort();

        tuples.dedup_by(|(key1, v1, w1), (key2, v2, w2)| {
            if key1 == key2 && v1 == v2 {
                *w2 += std::mem::replace(w1, 0);
                true
            } else {
                false
            }
        });
        tuples.retain(|(_, _, w)| *w != 0);
        tuples
    }

    #[test]
    fn cursor_list_matches_merge_batches() {
        let batch1: TestBatch = indexed_zset! { 1 => { 1 => 1, 2 => 2 }, 2 => { 1 => 1 } };
        let batch2: TestBatch = indexed_zset! { 1 => { 2 => -1, 3 => 2 } };
        let batch3: TestBatch = indexed_zset! { 2 => { 2 => 1 }, 3 => { 1 => 1 }, 4 => { 1 => 1 } };

        let batches = vec![batch1, batch2, batch3];
        let cursors: Vec<_> = batches.iter().map(|b| b.cursor()).collect();
        let weight_factory = batches[0].factories().weight_factory();
        let mut cursor_list = CursorList::new(weight_factory, cursors);

        let cursor_output = cursor_to_tuples(&mut cursor_list);

        let expected = merged_batch_to_tuples(&batches);

        assert_eq!(cursor_output, expected);
    }

    #[test]
    fn cursor_list_empty_batches() {
        let batch1: TestBatch = indexed_zset! {};
        let batch2: TestBatch = indexed_zset! { 1 => { 1 => 1 } };
        let batch3: TestBatch = indexed_zset! {};

        let batches = vec![batch1, batch2, batch3];
        let cursors: Vec<_> = batches.iter().map(|b| b.cursor()).collect();
        let weight_factory = batches[0].factories().weight_factory();
        let mut cursor_list = CursorList::new(weight_factory, cursors);

        let cursor_output = cursor_to_tuples(&mut cursor_list);
        let expected = merged_batch_to_tuples(&batches);

        assert_eq!(cursor_output, expected);
        assert_eq!(cursor_output, vec![(1, 1, 1)]);
    }

    #[test]
    fn cursor_list_single_batch() {
        let batch: TestBatch = indexed_zset! { 1 => { 1 => 1, 2 => 2 }, 2 => { 1 => -1 } };

        let batches = vec![batch];
        let cursors: Vec<_> = batches.iter().map(|b| b.cursor()).collect();
        let weight_factory = batches[0].factories().weight_factory();
        let mut cursor_list = CursorList::new(weight_factory, cursors);

        let cursor_output = cursor_to_tuples(&mut cursor_list);
        let expected = merged_batch_to_tuples(&batches);

        assert_eq!(cursor_output, expected);
    }

    #[test]
    fn cursor_list_weights_consolidate() {
        // Multiple batches with same (k,v) - weights should sum
        let batch1: TestBatch = indexed_zset! { 1 => { 1 => 2, 2 => 1 } };
        let batch2: TestBatch = indexed_zset! { 1 => { 1 => 3, 2 => -1 } };
        let batch3: TestBatch = indexed_zset! { 1 => { 1 => -1 } };

        let batches = vec![batch1, batch2, batch3];
        let cursors: Vec<_> = batches.iter().map(|b| b.cursor()).collect();
        let weight_factory = batches[0].factories().weight_factory();
        let mut cursor_list = CursorList::new(weight_factory, cursors);

        let cursor_output = cursor_to_tuples(&mut cursor_list);
        let expected = merged_batch_to_tuples(&batches);

        assert_eq!(cursor_output, expected);
        // (1,1): 2+3-1=4, (1,2): 1-1=0 (filtered out)
        assert_eq!(cursor_output, vec![(1, 1, 4)]);
    }

    fn batches(
        (max_key, max_value, weight_min, weight_max, max_batch_size, max_num_batches): (
            u64,
            u64,
            i64,
            i64,
            usize,
            usize,
        ),
    ) -> impl Strategy<Value = Vec<TestBatch>> {
        let tuple_strategy = (0..max_key, 0..max_value, weight_min..=weight_max)
            .prop_map(|(k, v, w)| Tup2(Tup2(k, v), w));
        vec(
            vec(tuple_strategy, 0..max_batch_size)
                .prop_map(|tuples| TestBatch::from_tuples((), tuples)),
            1..=max_num_batches,
        )
    }

    fn test_cursor_list_matches_merge_batches(batches: &[TestBatch]) {
        let cursors: Vec<_> = batches.iter().map(|b| b.cursor()).collect();
        let weight_factory = batches[0].factories().weight_factory();
        let mut cursor_list = CursorList::new(weight_factory, cursors);

        let cursor_output = cursor_to_tuples(&mut cursor_list);
        let expected = merged_batch_to_tuples(&batches);

        assert_eq!(cursor_output, expected);
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(100))]

        #[test]
        fn proptest_cursor_list_matches_merge_batches1(batches in batches((100, 100, 0, 2, 100, 100))) {
            test_cursor_list_matches_merge_batches(&batches);
        }

        #[test]
        fn proptest_cursor_list_matches_merge_batches2(batches in batches((100, 100, -2, 2, 100, 100))) {
            test_cursor_list_matches_merge_batches(&batches);
        }

        #[test]
        fn proptest_cursor_list_matches_merge_batches3(batches in batches((1, 1, -2, 2, 100, 100))) {
            test_cursor_list_matches_merge_batches(&batches);
        }

        #[test]
        fn proptest_cursor_list_matches_merge_batches4(batches in batches((1000, 1, -2, 2, 100, 100))) {
            test_cursor_list_matches_merge_batches(&batches);
        }

        #[test]
        fn proptest_cursor_list_matches_merge_batches5(batches in batches((1, 1000, -2, 2, 100, 100))) {
            test_cursor_list_matches_merge_batches(&batches);
        }

    }
}
