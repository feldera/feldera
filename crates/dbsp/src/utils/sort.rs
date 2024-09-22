//! This module contains a dynamically typed re-implementation of the stable
//! sorting algorithm from the Rust standard library.  DBSP stores data in sorted vectors.
//! Using the `std` implementation of sorting, we ended up compiling specialized
//! implementations of the sorting algorithm for hundreds of concrete types, which slowed down
//! compilation significantly.  The dynamic implementation in this module works for all typed by
//! taking the comparison function as `&dyn Fn`, along with the size and alignment of a vector
//! element.

use std::ptr;
use std::{
    alloc, cmp,
    cmp::Ordering,
    fmt::Debug,
    mem::{align_of, size_of, MaybeUninit},
    ops::Range,
};

fn vec_reverse(v: &mut [u8], val_size: usize) {
    let len = v.len() / val_size;

    let half_len = len / 2;
    let Range { start, end } = v.as_mut_ptr_range();

    let mut i = 0;
    while i < half_len {
        unsafe {
            ptr::swap_nonoverlapping(
                start.add(i * val_size),
                end.sub((1 + i) * val_size),
                val_size,
            );
        }

        i += 1;
    }
}

// When dropped, copies from `src` into `dest`.
struct InsertionHole {
    src: *const u8,
    dest: *mut u8,
    val_size: usize,
}

impl Drop for InsertionHole {
    fn drop(&mut self) {
        // SAFETY: This is a helper class. Please refer to its usage for correctness.
        // Namely, one must be sure that `src` and `dst` does not overlap as
        // required by `ptr::copy_nonoverlapping` and are both valid for writes.
        unsafe {
            ptr::copy_nonoverlapping(self.src, self.dest, self.val_size);
        }
    }
}

/// Inserts `v[v.len() - 1]` into pre-sorted sequence `v[..v.len() - 1]` so that
/// whole `v[..]` becomes sorted.
unsafe fn insert_tail(
    v: &mut [u8],
    val_size: usize,
    is_less: &dyn Fn(*const u8, *const u8) -> bool,
    scratch: *mut u8,
) {
    debug_assert!(v.len() / val_size >= 2);

    let arr_ptr = v.as_mut_ptr();
    let i = v.len() / val_size - 1;

    // SAFETY: caller must ensure v is at least len 2.
    unsafe {
        // See insert_head which talks about why this approach is beneficial.
        let i_ptr = arr_ptr.add(i * val_size);

        // It's important that we use i_ptr here. If this check is positive and we
        // continue, We want to make sure that no other copy of the value was
        // seen by is_less. Otherwise we would have to copy it back.
        if is_less(i_ptr, i_ptr.sub(val_size)) {
            // It's important, that we use tmp for comparison from now on. As it is the
            // value that will be copied back. And notionally we could have
            // created a divergence if we copy back the wrong value.
            ptr::copy_nonoverlapping(i_ptr, scratch, val_size);
            //let tmp = mem::ManuallyDrop::new(ptr::read(i_ptr));

            // Intermediate state of the insertion process is always tracked by `hole`,
            // which serves two purposes:
            // 1. Protects integrity of `v` from panics in `is_less`.
            // 2. Fills the remaining hole in `v` in the end.
            //
            // Panic safety:
            //
            // If `is_less` panics at any point during the process, `hole` will get dropped
            // and fill the hole in `v` with `tmp`, thus ensuring that `v` still
            // holds every object it initially held exactly once.
            let mut hole = InsertionHole {
                src: scratch,
                dest: i_ptr.sub(val_size),
                val_size,
            };
            ptr::copy_nonoverlapping(hole.dest, i_ptr, val_size);

            // SAFETY: We know i is at least 1.
            for j in (0..(i - 1)).rev() {
                let j_ptr = arr_ptr.add(j * val_size);
                if !is_less(scratch, j_ptr) {
                    break;
                }

                ptr::copy_nonoverlapping(j_ptr, hole.dest, val_size);
                hole.dest = j_ptr;
            }
            // `hole` gets dropped and thus copies `tmp` into the remaining hole
            // in `v`.
        }
    }
}

/// Sort `v` assuming `v[..offset]` is already sorted.
///
/// Never inline this function to avoid code bloat. It still optimizes nicely
/// and has practically no performance impact. Even improving performance in
/// some cases.
#[inline(never)]
pub(super) fn insertion_sort_shift_left(
    v: &mut [u8],
    val_size: usize,
    offset: usize,
    is_less: &dyn Fn(*const u8, *const u8) -> bool,
    scratch: *mut u8,
) {
    let len = v.len() / val_size;

    // Using assert here improves performance.
    assert!(offset != 0 && offset <= len);

    // Shift each element of the unsorted region v[i..] as far left as is needed to
    // make v sorted.
    for i in offset..len {
        // SAFETY: we tested that `offset` must be at least 1, so this loop is only
        // entered if len >= 2. The range is exclusive and we know `i` must be
        // at least 1 so this slice has at >least len 2.
        unsafe {
            insert_tail(
                &mut v[..=(i + 1) * val_size - 1],
                val_size,
                is_less,
                scratch,
            );
        }
    }
}

/// Merges non-decreasing runs `v[..mid]` and `v[mid..]` using `buf` as
/// temporary storage, and stores the result into `v[..]`.
///
/// # Safety
///
/// The two slices must be non-empty and `mid` must be in bounds. Buffer `buf`
/// must be long enough to hold a copy of the shorter slice. Also, `T` must not
/// be a zero-sized type.
unsafe fn merge(
    v: &mut [u8],
    val_size: usize,
    mid: usize,
    buf: *mut u8,
    is_less: &dyn Fn(*const u8, *const u8) -> bool,
) {
    let len = v.len() / val_size;
    let v = v.as_mut_ptr();

    // SAFETY: mid and len must be in-bounds of v.
    let (v_mid, v_end) = unsafe { (v.add(mid * val_size), v.add(len * val_size)) };

    // The merge process first copies the shorter run into `buf`. Then it traces the
    // newly copied run and the longer run forwards (or backwards), comparing
    // their next unconsumed elements and copying the lesser (or greater) one
    // into `v`.
    //
    // As soon as the shorter run is fully consumed, the process is done. If the
    // longer run gets consumed first, then we must copy whatever is left of the
    // shorter run into the remaining hole in `v`.
    //
    // Intermediate state of the process is always tracked by `hole`, which serves
    // two purposes:
    // 1. Protects integrity of `v` from panics in `is_less`.
    // 2. Fills the remaining hole in `v` if the longer run gets consumed first.
    //
    // Panic safety:
    //
    // If `is_less` panics at any point during the process, `hole` will get dropped
    // and fill the hole in `v` with the unconsumed range in `buf`, thus
    // ensuring that `v` still holds every object it initially held exactly
    // once.
    let mut hole;

    if mid <= len - mid {
        // The left run is shorter.

        // SAFETY: buf must have enough capacity for `v[..mid]`.
        unsafe {
            ptr::copy_nonoverlapping(v, buf, mid * val_size);
            hole = MergeHole {
                start: buf,
                end: buf.add(mid * val_size),
                dest: v,
            };
        }

        // Initially, these pointers point to the beginnings of their arrays.
        let left = &mut hole.start;
        let mut right = v_mid;
        let out = &mut hole.dest;

        while *left < hole.end && right < v_end {
            // Consume the lesser side.
            // If equal, prefer the left run to maintain stability.

            // SAFETY: left and right must be valid and part of v same for out.
            unsafe {
                let is_l = is_less(right, *left);
                let to_copy = if is_l { right } else { *left };
                ptr::copy_nonoverlapping(to_copy, *out, val_size);
                *out = out.add(val_size);
                right = right.add((is_l as usize) * val_size);
                *left = left.add((!is_l as usize) * val_size);
            }
        }
    } else {
        // The right run is shorter.

        // SAFETY: buf must have enough capacity for `v[mid..]`.
        unsafe {
            ptr::copy_nonoverlapping(v_mid, buf, (len - mid) * val_size);
            hole = MergeHole {
                start: buf,
                end: buf.add((len - mid) * val_size),
                dest: v_mid,
            };
        }

        // Initially, these pointers point past the ends of their arrays.
        let left = &mut hole.dest;
        let right = &mut hole.end;
        let mut out = v_end;

        while v < *left && buf < *right {
            // Consume the greater side.
            // If equal, prefer the right run to maintain stability.

            // SAFETY: left and right must be valid and part of v same for out.
            unsafe {
                let is_l = is_less(&*right.sub(val_size), &*left.sub(val_size));
                *left = left.sub((is_l as usize) * val_size);
                *right = right.sub((!is_l as usize) * val_size);
                let to_copy = if is_l { *left } else { *right };
                out = out.sub(val_size);
                ptr::copy_nonoverlapping(to_copy, out, val_size);
            }
        }
    }
    // Finally, `hole` gets dropped. If the shorter run was not fully consumed,
    // whatever remains of it will now be copied into the hole in `v`.

    // When dropped, copies the range `start..end` into `dest..`.
    struct MergeHole {
        start: *mut u8,
        end: *mut u8,
        dest: *mut u8,
    }

    impl Drop for MergeHole {
        fn drop(&mut self) {
            // SAFETY: `T` is not a zero-sized type, and these are pointers into a slice's
            // elements.
            unsafe {
                let len = self.end as usize - self.start as usize;
                ptr::copy_nonoverlapping(self.start, self.dest, len);
            }
        }
    }
}

/// This merge sort borrows some (but not all) ideas from TimSort, which used to
/// be described in detail [here](https://github.com/python/cpython/blob/main/Objects/listsort.txt). However Python
/// has switched to a Powersort based implementation.
///
/// The algorithm identifies strictly descending and non-descending
/// subsequences, which are called natural runs. There is a stack of pending
/// runs yet to be merged. Each newly found run is pushed onto the stack, and
/// then some pairs of adjacent runs are merged until these two invariants are
/// satisfied:
///
/// 1. for every `i` in `1..runs.len()`: `runs[i - 1].len > runs[i].len`
/// 2. for every `i` in `2..runs.len()`: `runs[i - 2].len > runs[i - 1].len +
///    runs[i].len`
///
/// The invariants ensure that the total running time is *O*(*n* \* log(*n*))
/// worst-case.
pub fn merge_sort(
    v: &mut [u8],
    val_size: usize,
    align: usize,
    is_less: &dyn Fn(*const u8, *const u8) -> bool,
    scratch: *mut u8,
) {
    // Slices of up to this length get sorted using insertion sort.
    const MAX_INSERTION: usize = 20;

    if val_size == 0 {
        // Sorting has no meaningful behavior on zero-sized types. Do nothing.
        return;
    }

    debug_assert_eq!(v.len() % val_size, 0);

    let len = v.len() / val_size;

    // Short arrays get sorted in-place via insertion sort to avoid allocations.
    if len <= MAX_INSERTION {
        if len >= 2 {
            insertion_sort_shift_left(v, val_size, 1, is_less, scratch);
        }
        return;
    }

    // Allocate a buffer to use as scratch memory. We keep the length 0 so we can
    // keep in it shallow copies of the contents of `v` without risking the
    // dtors running on copies if `is_less` panics. When merging two sorted
    // runs, this buffer holds a copy of the shorter run, which will always have
    // length at most `len / 2`.
    let buf = BufGuard::new(len / 2, val_size, align);
    let buf_ptr = buf.buf_ptr.as_ptr();

    let mut runs = RunVec::new();

    let mut end = 0;
    let mut start = 0;

    // Scan forward. Memory pre-fetching prefers forward scanning vs backwards
    // scanning, and the code-gen is usually better. For the most sensitive
    // types such as integers, these are merged bidirectionally at once. So
    // there is no benefit in scanning backwards.
    while end < len {
        let (streak_end, was_reversed) = find_streak(&v[start * val_size..], val_size, is_less);
        end += streak_end;
        if was_reversed {
            vec_reverse(&mut v[start * val_size..end * val_size], val_size);
        }

        // Insert some more elements into the run if it's too short. Insertion sort is
        // faster than merge sort on short sequences, so this significantly
        // improves performance.
        end = provide_sorted_batch(v, val_size, start, end, is_less, scratch);

        // Push this run onto the stack.
        runs.push(TimSortRun {
            start,
            len: end - start,
        });
        start = end;

        // Merge some pairs of adjacent runs to satisfy the invariants.
        while let Some(r) = collapse(runs.as_slice(), len) {
            let left = runs[r];
            let right = runs[r + 1];
            let merge_slice = &mut v[left.start * val_size..(right.start + right.len) * val_size];
            // SAFETY: `buf_ptr` must hold enough capacity for the shorter of the two sides,
            // and neither side may be on length 0.
            unsafe {
                merge(merge_slice, val_size, left.len, buf_ptr, is_less);
            }
            runs[r + 1] = TimSortRun {
                start: left.start,
                len: left.len + right.len,
            };
            runs.remove(r);
        }
    }

    // Finally, exactly one run must remain in the stack.
    debug_assert!(runs.len() == 1 && runs[0].start == 0 && runs[0].len == len);

    // Examines the stack of runs and identifies the next pair of runs to merge.
    // More specifically, if `Some(r)` is returned, that means `runs[r]` and
    // `runs[r + 1]` must be merged next. If the algorithm should continue
    // building a new run instead, `None` is returned.
    //
    // TimSort is infamous for its buggy implementations, as described here:
    // http://envisage-project.eu/timsort-specification-and-verification/
    //
    // The gist of the story is: we must enforce the invariants on the top four runs
    // on the stack. Enforcing them on just top three is not sufficient to
    // ensure that the invariants will still hold for *all* runs in the stack.
    //
    // This function correctly checks invariants for the top four runs.
    // Additionally, if the top run starts at index 0, it will always demand a
    // merge operation until the stack is fully collapsed, in order to complete
    // the sort.
    #[inline]
    fn collapse(runs: &[TimSortRun], stop: usize) -> Option<usize> {
        let n = runs.len();
        if n >= 2
            && (runs[n - 1].start + runs[n - 1].len == stop
                || runs[n - 2].len <= runs[n - 1].len
                || (n >= 3 && runs[n - 3].len <= runs[n - 2].len + runs[n - 1].len)
                || (n >= 4 && runs[n - 4].len <= runs[n - 3].len + runs[n - 2].len))
        {
            if n >= 3 && runs[n - 3].len < runs[n - 1].len {
                Some(n - 3)
            } else {
                Some(n - 2)
            }
        } else {
            None
        }
    }

    // Extremely basic versions of Vec.
    // Their use is super limited and by having the code here, it allows reuse
    // between the sort implementations.
    struct BufGuard {
        buf_ptr: ptr::NonNull<u8>,
        capacity: usize,
        val_size: usize,
        align: usize,
    }

    impl BufGuard {
        fn new(len: usize, val_size: usize, align: usize) -> Self {
            let buf_ptr = unsafe {
                alloc::alloc(alloc::Layout::from_size_align_unchecked(
                    val_size * len,
                    align,
                ))
            };

            Self {
                buf_ptr: ptr::NonNull::new(buf_ptr).unwrap(),
                capacity: len,
                val_size,
                align,
            }
        }
    }

    impl Drop for BufGuard {
        fn drop(&mut self) {
            unsafe {
                alloc::dealloc(
                    self.buf_ptr.as_ptr(),
                    alloc::Layout::from_size_align_unchecked(
                        self.val_size * self.capacity,
                        self.align,
                    ),
                )
            }
        }
    }

    struct RunVec {
        buf_ptr: ptr::NonNull<TimSortRun>,
        capacity: usize,
        len: usize,
    }

    impl RunVec {
        fn new() -> Self {
            // Most slices can be sorted with at most 16 runs in-flight.
            const START_RUN_CAPACITY: usize = 16;

            Self {
                buf_ptr: ptr::NonNull::new(run_alloc(START_RUN_CAPACITY)).unwrap(),
                capacity: START_RUN_CAPACITY,
                len: 0,
            }
        }

        fn push(&mut self, val: TimSortRun) {
            if self.len == self.capacity {
                let old_capacity = self.capacity;
                let old_buf_ptr = self.buf_ptr.as_ptr();

                self.capacity *= 2;
                self.buf_ptr = ptr::NonNull::new(run_alloc(self.capacity)).unwrap();

                // SAFETY: buf_ptr new and old were correctly allocated and old_buf_ptr has
                // old_capacity valid elements.
                unsafe {
                    ptr::copy_nonoverlapping(old_buf_ptr, self.buf_ptr.as_ptr(), old_capacity);
                }

                run_dealloc(old_buf_ptr, old_capacity);
            }

            // SAFETY: The invariant was just checked.
            unsafe {
                self.buf_ptr.as_ptr().add(self.len).write(val);
            }
            self.len += 1;
        }

        fn remove(&mut self, index: usize) {
            if index >= self.len {
                panic!("Index out of bounds");
            }

            // SAFETY: buf_ptr needs to be valid and len invariant upheld.
            unsafe {
                // the place we are taking from.
                let ptr = self.buf_ptr.as_ptr().add(index);

                // Shift everything down to fill in that spot.
                ptr::copy(ptr.add(1), ptr, self.len - index - 1);
            }
            self.len -= 1;
        }

        fn as_slice(&self) -> &[TimSortRun] {
            // SAFETY: Safe as long as buf_ptr is valid and len invariant was upheld.
            unsafe { &*ptr::slice_from_raw_parts(self.buf_ptr.as_ptr(), self.len) }
        }

        fn len(&self) -> usize {
            self.len
        }
    }

    impl core::ops::Index<usize> for RunVec {
        type Output = TimSortRun;

        fn index(&self, index: usize) -> &Self::Output {
            if index < self.len {
                // SAFETY: buf_ptr and len invariant must be upheld.
                unsafe {
                    return &*(self.buf_ptr.as_ptr().add(index));
                }
            }

            panic!("Index out of bounds");
        }
    }

    impl core::ops::IndexMut<usize> for RunVec {
        fn index_mut(&mut self, index: usize) -> &mut Self::Output {
            if index < self.len {
                // SAFETY: buf_ptr and len invariant must be upheld.
                unsafe {
                    return &mut *(self.buf_ptr.as_ptr().add(index));
                }
            }

            panic!("Index out of bounds");
        }
    }

    impl Drop for RunVec {
        fn drop(&mut self) {
            // As long as TimSortRun is Copy we don't need to drop them individually but
            // just the whole allocation.
            run_dealloc(self.buf_ptr.as_ptr(), self.capacity);
        }
    }
}

/// Internal type used by merge_sort.
#[derive(Clone, Copy, Debug)]
pub struct TimSortRun {
    len: usize,
    start: usize,
}

/// Takes a range as denoted by start and end, that is already sorted and
/// extends it to the right if necessary with sorts optimized for smaller ranges
/// such as insertion sort.
fn provide_sorted_batch(
    v: &mut [u8],
    val_size: usize,
    start: usize,
    mut end: usize,
    is_less: &dyn Fn(*const u8, *const u8) -> bool,
    scratch: *mut u8,
) -> usize {
    debug_assert_eq!(v.len() % val_size, 0);

    let len = v.len() / val_size;
    assert!(end >= start && end <= len);

    // This value is a balance between least comparisons and best performance, as
    // influenced by for example cache locality.
    const MIN_INSERTION_RUN: usize = 10;

    // Insert some more elements into the run if it's too short. Insertion sort is
    // faster than merge sort on short sequences, so this significantly improves
    // performance.
    let start_end_diff = end - start;

    if start_end_diff < MIN_INSERTION_RUN && end < len {
        // v[start_found..end] are elements that are already sorted in the input. We
        // want to extend the sorted region to the left, so we push up
        // MIN_INSERTION_RUN - 1 to the right. Which is more efficient that
        // trying to push those already sorted elements to the left.
        end = cmp::min(start + MIN_INSERTION_RUN, len);
        let presorted_start = cmp::max(start_end_diff, 1);

        insertion_sort_shift_left(
            &mut v[start * val_size..end * val_size],
            val_size,
            presorted_start,
            is_less,
            scratch,
        );
    }

    end
}

/// Finds a streak of presorted elements starting at the beginning of the slice.
/// Returns the first value that is not part of said streak, and a bool denoting
/// whether the streak was reversed. Streaks can be increasing or decreasing.
fn find_streak(
    v: &[u8],
    val_size: usize,
    is_less: &dyn Fn(*const u8, *const u8) -> bool,
) -> (usize, bool) {
    debug_assert_eq!(v.len() % val_size, 0);

    let len = v.len() / val_size;

    if len < 2 {
        return (len, false);
    }

    let v = v.as_ptr();

    let mut end = 2;

    // SAFETY: See below specific.
    unsafe {
        // SAFETY: We checked that len >= 2, so 0 and 1 are valid indices.
        let assume_reverse = is_less(v.add(val_size), v);

        // SAFETY: We know end >= 2 and check end < len.
        // From that follows that accessing v at end and end - 1 is safe.
        if assume_reverse {
            while end < len && is_less(v.add(end * val_size), v.add((end - 1) * val_size)) {
                end += 1;
            }

            (end, true)
        } else {
            while end < len && !is_less(v.add(end * val_size), v.add((end - 1) * val_size)) {
                end += 1;
            }
            (end, false)
        }
    }
}

fn run_alloc(len: usize) -> *mut TimSortRun {
    // SAFETY: Creating the layout is safe as long as merge_sort never calls this
    // with an obscene length or 0.
    unsafe {
        alloc::alloc(alloc::Layout::array::<TimSortRun>(len).unwrap_unchecked()) as *mut TimSortRun
    }
}

fn run_dealloc(buf_ptr: *mut TimSortRun, len: usize) {
    // SAFETY: The caller must ensure that buf_ptr was created by elem_alloc_fn with
    // the same len.
    unsafe {
        alloc::dealloc(
            buf_ptr as *mut u8,
            alloc::Layout::array::<TimSortRun>(len).unwrap_unchecked(),
        );
    }
}

pub fn stable_sort<T: Ord>(slice: &mut [T]) {
    stable_sort_by(slice, |x, y| x.cmp(y))
}

pub fn stable_sort_by<T, F>(slice: &mut [T], cmp: F)
where
    F: Fn(&T, &T) -> Ordering,
{
    let byte_slice = unsafe {
        std::slice::from_raw_parts_mut(slice.as_mut_ptr() as *mut u8, std::mem::size_of_val(slice))
    };
    let is_less = (&|x: *const u8, y: *const u8| {
        let x = unsafe { &*(x as *const T) };
        let y = unsafe { &*(y as *const T) };
        cmp(x, y) == Ordering::Less
    }) as &dyn Fn(*const u8, *const u8) -> bool;

    merge_sort(
        byte_slice,
        size_of::<T>(),
        align_of::<T>(),
        is_less,
        <MaybeUninit<T>>::uninit().as_mut_ptr() as *mut u8,
    );

    // println!("sorted: {slice:?}");
}

#[cfg(test)]
mod test {
    use super::stable_sort;
    use proptest::{collection::vec, prelude::*};

    #[test]
    fn test_stable_sort() {
        let long_sorted: Vec<i32> = (0..1000).collect();
        let long_reversed: Vec<i32> = (0..1000).rev().collect();
        let corpus: Vec<Vec<i32>> = vec![
            vec![],
            vec![10],
            vec![5, 4],
            vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            vec![16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 3, 4, 1, 2],
            vec![
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1560281088, 234560113, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            ],
            long_sorted,
            long_reversed,
        ];

        for mut vec in corpus.into_iter() {
            let mut expected = vec.clone();
            expected.sort();

            stable_sort(&mut vec);

            assert_eq!(vec, expected);
        }
    }

    prop_compose! {
        fn short_vec()(batch in vec(any::<u32>(), 0..30)) -> Vec<u32> {
            batch
        }
    }

    prop_compose! {
        fn long_vec()(batch in vec(any::<u32>(), 0..300)) -> Vec<u32> {
            batch
        }
    }

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
    struct TestStruct {
        f1: u16,
        f2: String,
        f3: i64,
        f4: i8,
        f5: String,
        f6: u8,
        f7: Vec<u8>,
    }

    fn test_struct() -> impl Strategy<Value = TestStruct> {
        (
            any::<u16>(),
            any::<String>(),
            any::<i64>(),
            any::<i8>(),
            any::<String>(),
            any::<u8>(),
            vec(any::<u8>(), 0..20),
        )
            .prop_map(|(f1, f2, f3, f4, f5, f6, f7)| TestStruct {
                f1,
                f2,
                f3,
                f4,
                f5,
                f6,
                f7,
            })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(10000))]

        #[test]
        fn stable_sort_small_proptest(mut v in short_vec()) {
            let mut expected = v.clone();
            expected.sort();

            stable_sort(&mut v);
            assert_eq!(v, expected);
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(1000))]
        #[test]
        fn stable_sort_small_structs_proptest(mut v in vec(test_struct(), 0..25)) {
            let mut expected = v.clone();
            expected.sort();

            stable_sort(&mut v);
            assert_eq!(v, expected);
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(1000))]
        #[test]
        fn stable_sort_large_proptest(mut v in long_vec()) {
            let mut expected = v.clone();
            expected.sort();

            stable_sort(&mut v);
            assert_eq!(v, expected);
        }
    }
}
