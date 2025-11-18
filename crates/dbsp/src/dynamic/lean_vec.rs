use rkyv::{
    out_field,
    ser::{ScratchSpace, Serializer},
    Archive, Archived, Deserialize, DeserializeUnsized, Fallible, RelPtr, Serialize,
    SerializeUnsized,
};
use size_of::{Context, SizeOf};
use std::{
    alloc::{self, handle_alloc_error, Layout},
    cmp::{max, min, Ordering},
    fmt::{self, Debug, Display, Formatter},
    hash::{Hash, Hasher},
    iter::FusedIterator,
    marker::PhantomData,
    mem::{align_of, forget, size_of, take, ManuallyDrop},
    ops::{Bound, Index, IndexMut, RangeBounds},
    pin::Pin,
    ptr::{self, drop_in_place, NonNull},
    slice,
};

#[macro_export]
macro_rules! lean_vec {
    () => (
        $crate::dynamic::LeanVec::from(vec![])
    );
    ($elem:expr; $n:expr) => (
        $crate::dynamic::LeanVec::from(vec![$elem;$n])
    );
    ($($x:expr),+ $(,)?) => (
        $crate::dynamic::LeanVec::from(vec![$($x),+])
    );
}

pub struct RawVec {
    ptr: *mut u8,
    val_size: usize,
    align: usize,
    length: usize,
    capacity: usize,
}

impl RawVec {
    pub fn new(val_size: usize, align: usize, dangling: *mut u8) -> Self {
        Self {
            ptr: dangling,
            val_size,
            align,
            length: 0,
            capacity: if val_size == 0 { usize::MAX } else { 0 },
        }
    }

    pub fn with_capacity(
        val_size: usize,
        align: usize,
        capacity: usize,
        dangling: *mut u8,
    ) -> Self {
        let mut this = Self::new(val_size, align, dangling);
        this.reserve_exact(capacity);
        this
    }

    /// Creates a `RawVec` directly from a pointer, a length, and a capacity.
    ///
    /// # Safety
    ///
    /// - `ptr` must be a valid pointer to `capacity` allocated bytes of which `length` are valid
    /// - `val_size` must be the size of the elements in the vector
    /// - `align` must be the alignment of the elements in the vector
    /// - `length` must be less than or equal to `capacity`
    pub unsafe fn from_raw_parts(
        ptr: *mut u8,
        length: usize,
        capacity: usize,
        val_size: usize,
        align: usize,
    ) -> Self {
        debug_assert!(capacity >= length);
        Self {
            ptr,
            val_size,
            align,
            length,
            capacity,
        }
    }

    pub const fn len(&self) -> usize {
        self.length
    }

    pub fn spare_capacity(&self) -> usize {
        self.capacity() - self.len()
    }

    pub fn has_spare_capacity(&self) -> bool {
        self.spare_capacity() > 0
    }

    /// Sets the current vector's length to `new_len`
    ///
    /// # Safety
    ///
    /// - `new_len` must be less than or equal to [`self.capacity()`].
    /// - The elements at `old_len..new_len` must be initialized.
    ///
    /// [`self.capacity()`]: RawVec ::capacity
    pub unsafe fn set_len(&mut self, new_len: usize) {
        debug_assert!(new_len <= self.capacity());
        self.length = new_len;
    }

    pub const fn is_empty(&self) -> bool {
        self.length == 0
    }

    pub const fn capacity(&self) -> usize {
        self.capacity
    }

    pub const fn as_ptr(&self) -> *const u8 {
        self.ptr as *const u8
    }

    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr
    }

    pub fn reserve(&mut self, additional: usize) {
        let required = self.len() + additional;
        if required <= self.capacity() {
            return;
        }

        debug_assert_ne!(required, 0);
        let mut new_capacity = self.next_capacity(required);
        while new_capacity < required {
            new_capacity = self.next_capacity(new_capacity);
        }

        self.realloc(new_capacity);
    }

    pub fn reserve_exact(&mut self, additional: usize) {
        let required = self.len() + additional;
        if required <= self.capacity() {
            return;
        }

        self.realloc(required);
    }

    pub fn clear(&mut self, drop_slice_in_place: &dyn Fn(*mut u8, usize)) {
        if self.is_empty() {
            return;
        }

        let length = self.len();
        unsafe {
            // Pre-poop our pants, set our length to zero in case dropping elements panics
            self.set_len(0);

            // Drop all elements
            drop_slice_in_place(self.as_mut_ptr(), length);
        }
    }

    pub fn index(&self, idx: usize) -> *const u8 {
        if idx < self.len() {
            // Safety: The index is inbounds
            unsafe { self.as_ptr().add(idx * self.val_size) }
        } else {
            index_out_of_bounds(idx, self.len())
        }
    }

    pub fn index_mut(&mut self, idx: usize) -> *mut u8 {
        if idx < self.len() {
            // Safety: The index is inbounds
            unsafe { self.as_mut_ptr().add(idx * self.val_size) }
        } else {
            index_out_of_bounds(idx, self.len())
        }
    }

    /// # Safety
    ///
    /// - `idx` must be less than the length of the vector
    pub unsafe fn index_unchecked(&self, idx: usize) -> *const u8 {
        debug_assert!(idx < self.len());
        self.as_ptr().add(idx * self.val_size)
    }

    /// # Safety
    ///
    /// - `idx` must be less than the length of the vector
    pub unsafe fn index_mut_unchecked(&mut self, idx: usize) -> *mut u8 {
        debug_assert!(idx < self.len());
        self.as_mut_ptr().add(idx * self.val_size)
    }

    pub fn range<R>(&self, range: R) -> (*const u8, usize)
    where
        R: RangeBounds<usize>,
    {
        self.range_inner(range.start_bound().cloned(), range.end_bound().cloned())
    }

    pub fn range_mut<R>(&mut self, range: R) -> (*mut u8, usize)
    where
        R: RangeBounds<usize>,
    {
        let (ptr, len) = self.range_inner(range.start_bound().cloned(), range.end_bound().cloned());
        (ptr as *mut u8, len)
    }

    /// Push a value to the current vector
    ///
    /// # Safety
    ///
    /// `value` must be a valid pointer to an instance of the current vector's
    /// type
    pub unsafe fn push_raw(&mut self, value: *const u8) {
        // Reserve space for the new element
        self.reserve(1);

        // Copy the element into the vector
        // Safety: The caller guarantees that the given pointer is valid
        unsafe {
            ptr::copy_nonoverlapping(
                value,
                self.as_mut_ptr().add(self.len() * self.val_size),
                self.val_size,
            );
        }

        // Increment the vector's length
        self.length += 1;
    }

    /// # Safety
    ///
    /// - `value` must be a valid pointer to an instance of the current vector's type
    /// - the vector must have available capacity greater than zero
    pub unsafe fn push_unchecked(&mut self, value: *const u8) {
        debug_assert_ne!(self.capacity(), 0, "cannot push to a vec of length zero");
        debug_assert!(
            self.len() < self.capacity(),
            "cannot push to a vec without spare capacity",
        );

        // Copy the element into the vector
        // Safety: The caller guarantees that the given pointer is valid
        unsafe {
            ptr::copy_nonoverlapping(
                value,
                self.as_mut_ptr().add(self.len() * self.val_size),
                self.val_size,
            );
        }

        self.length += 1;
    }

    /// # Safety
    ///
    /// - `start` must be a valid pointer to a slice of length `length` of the current vector's type
    pub unsafe fn append_raw_range(&mut self, start: *const u8, length: usize) {
        // Reserve space for the new elements
        self.reserve(length);

        // Copy the elements into the vector
        // Safety: The caller guarantees that the given pointer is valid
        unsafe {
            ptr::copy_nonoverlapping(
                start,
                self.as_mut_ptr().add(self.len() * self.val_size),
                self.val_size * length,
            );
        }

        // Increment the vector's length
        self.length += length;
    }

    pub fn eq(&self, other: &Self, eq_func: &dyn Fn(*const u8, *const u8) -> bool) -> bool {
        if self.len() != other.len() {
            return false;
        }

        for i in 0..self.len() {
            if !eq_func(self.index(i), other.index(i)) {
                return false;
            }
        }

        true
    }

    pub fn cmp(
        &self,
        other: &Self,
        cmp_func: &dyn Fn(*const u8, *const u8) -> Ordering,
    ) -> Ordering {
        let common_len = min(self.len(), other.len());

        for i in 0..common_len {
            let ordering = cmp_func(self.index(i), other.index(i));
            if ordering != Ordering::Equal {
                return ordering;
            }
        }

        self.len().cmp(&other.len())
    }

    #[allow(clippy::should_implement_trait)]
    pub fn hash(&self, hash_func: &mut dyn FnMut(*const u8)) {
        for i in 0..self.len() {
            hash_func(self.index(i));
        }
    }

    pub fn size_of_children(
        &self,
        context: &mut size_of::Context,
        size_of_children_func: &mut dyn Fn(*const u8, &mut size_of::Context),
    ) {
        // We only cause an allocation to occur when we have allocated capacity and our
        // element type isn't a zst
        if self.capacity() != 0 && self.val_size != 0 {
            context
                .add_vectorlike(self.len(), self.capacity(), self.val_size)
                .add_distinct_allocation();
        }

        for i in 0..self.len() {
            size_of_children_func(self.index(i), context);
        }
    }

    pub fn debug(
        &self,
        f: &mut fmt::Formatter<'_>,
        debug_func: &dyn Fn(*const u8, &mut fmt::Formatter) -> fmt::Result,
    ) -> fmt::Result {
        struct DebugErased<'a>(
            *const u8,
            &'a dyn Fn(*const u8, &mut fmt::Formatter) -> fmt::Result,
        );

        impl Debug for DebugErased<'_> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                (self.1)(self.0, f)
            }
        }

        let mut list = f.debug_list();
        for i in 0..self.len() {
            list.entry(&DebugErased(self.index(i), debug_func));
        }
        list.finish()
    }

    pub fn dedup_starting_at(
        &mut self,
        starting_point: usize,
        same_bucket: &mut dyn FnMut(*mut u8, *mut u8) -> bool,
        drop_in_place: &mut dyn FnMut(*mut u8),
    ) {
        let len = self.len();
        if len <= 1 || starting_point + 1 >= len {
            return;
        }

        // INVARIANT: vec.len() > read >= write > write-1 >= 0
        struct FillGapOnDrop<'a> {
            // Offset of the element we want to check if it is duplicate
            read: usize,

            // Offset of the place where we want to place the non-duplicate
            // when we find it.
            write: usize,

            // The Vec that would need correction if `same_bucket` panicked
            vec: &'a mut RawVec,
        }

        impl Drop for FillGapOnDrop<'_> {
            fn drop(&mut self) {
                // This code gets executed when `same_bucket` panics

                // SAFETY: invariant guarantees that `read - write`
                // and `len - read` never overflow and that the copy is always
                // in-bounds.
                unsafe {
                    let len = self.vec.len();

                    let key_ptr = self.vec.as_mut_ptr();

                    // How many items were left when `same_bucket` panicked.
                    // Basically vec[read..].len()
                    let items_left = len.wrapping_sub(self.read);

                    // Pointer to first item in vec[write..write+items_left] slice
                    let dropped_key_ptr = key_ptr.add(self.write * self.vec.val_size);

                    // Pointer to first item in vec[read..] slice
                    let valid_key_ptr = key_ptr.add(self.read * self.vec.val_size);

                    // Copy `vec[read..]` to `vec[write..write+items_left]`.
                    // The slices can overlap, so `copy_nonoverlapping` cannot be used
                    ptr::copy(
                        valid_key_ptr,
                        dropped_key_ptr,
                        items_left * self.vec.val_size,
                    );

                    // How many items have been already dropped
                    // Basically vec[read..write].len()
                    let dropped = self.read.wrapping_sub(self.write);

                    self.vec.set_len(len - dropped);
                }
            }
        }

        let mut gap = FillGapOnDrop {
            read: starting_point + 1,
            write: starting_point + 1,
            vec: self,
        };
        let ptr = gap.vec.as_mut_ptr();

        // Drop items while going through Vec, it should be more efficient than
        // doing slice partition_dedup + truncate

        // SAFETY: Because of the invariant, read_ptr, prev_ptr and write_ptr
        // are always in-bounds and read_ptr never aliases prev_ptr
        unsafe {
            while gap.read < len {
                let read_ptr = ptr.add(gap.read * gap.vec.val_size);
                let prev_ptr = ptr.add(gap.write.wrapping_sub(1) * gap.vec.val_size);

                if same_bucket(read_ptr, prev_ptr) {
                    // Increase `gap.read` now since the drop may panic.
                    gap.read += 1;

                    // We have found duplicate, drop it in-place
                    drop_in_place(read_ptr);
                } else {
                    let write_key_ptr = ptr.add(gap.write * gap.vec.val_size);

                    // Because `read_ptr` can be equal to `write_ptr`, we either
                    // have to use `copy` or conditional `copy_nonoverlapping`.
                    // Looks like the first option is faster.
                    ptr::copy(read_ptr, write_key_ptr, gap.vec.val_size);

                    // We have filled that place, so go further
                    gap.write += 1;
                    gap.read += 1;
                }
            }

            // Technically we could let `gap` clean up with its Drop, but
            // when `same_bucket` is guaranteed to not panic, this bloats a little
            // the codegen, so we just do it manually
            gap.vec.set_len(gap.write);
            forget(gap);
        }
    }
    /// An implementation of `Vec::retain()` that takes a starting point
    ///
    /// Modified from [the stdlib](https://doc.rust-lang.org/std/vec/struct.Vec.html#method.retain)
    pub fn retain_starting_at(
        &mut self,
        starting_point: usize,
        retain: &mut dyn FnMut(*mut u8) -> bool,
        drop_in_place: &mut dyn FnMut(*mut u8),
    ) {
        if self.is_empty() || starting_point >= self.len() {
            return;
        }

        let original_len = self.len();
        // Avoid double drop if the drop guard is not executed,
        // since we may make some holes during the process.
        unsafe { self.set_len(0) };

        // Vec: [Kept, Kept, Hole, Hole, Hole, Hole, Unchecked, Unchecked]
        //      |<-              processed len   ->| ^- next to check
        //                  |<-  deleted cnt     ->|
        //      |<-              original_len                          ->|
        // Kept: Elements which predicate returns true on.
        // Hole: Moved or dropped element slot.
        // Unchecked: Unchecked valid elements.
        //
        // This drop guard will be invoked when predicate or `drop` of element panicked.
        // It shifts unchecked elements to cover holes and `set_len` to the correct
        // length. In cases when predicate and `drop` never panic, it will be
        // optimized out.
        struct BackshiftOnDrop<'a> {
            v: &'a mut RawVec,
            processed_len: usize,
            deleted_cnt: usize,
            original_len: usize,
        }

        impl Drop for BackshiftOnDrop<'_> {
            fn drop(&mut self) {
                if self.deleted_cnt > 0 {
                    // SAFETY: Trailing unchecked items must be valid since we never touch them.
                    unsafe {
                        ptr::copy(
                            self.v.as_ptr().add(self.processed_len * self.v.val_size),
                            self.v
                                .as_mut_ptr()
                                .add((self.processed_len - self.deleted_cnt) * self.v.val_size),
                            (self.original_len - self.processed_len) * self.v.val_size,
                        );
                    }
                }

                // SAFETY: After filling holes, all items are in contiguous memory.
                unsafe {
                    self.v.set_len(self.original_len - self.deleted_cnt);
                }
            }
        }

        let mut g = BackshiftOnDrop {
            v: self,
            processed_len: starting_point,
            deleted_cnt: 0,
            original_len,
        };

        fn process_loop<const DELETED: bool>(
            original_len: usize,
            f: &mut dyn FnMut(*mut u8) -> bool,
            g: &mut BackshiftOnDrop<'_>,
            drop_in_place: &mut dyn FnMut(*mut u8),
        ) {
            while g.processed_len != original_len {
                // SAFETY: Unchecked element must be valid.
                let cur = unsafe { g.v.as_mut_ptr().add(g.processed_len * g.v.val_size) };
                if !f(cur) {
                    // Advance early to avoid double drop if `drop_in_place` panicked.
                    g.processed_len += 1;
                    g.deleted_cnt += 1;

                    // SAFETY: We never touch this element again after dropped.
                    drop_in_place(cur);

                    // We already advanced the counter.
                    if DELETED {
                        continue;
                    } else {
                        break;
                    }
                }

                if DELETED {
                    // SAFETY: `deleted_cnt` > 0, so the hole slot must not overlap with current
                    // element. We use copy for move, and never touch this element
                    // again.
                    unsafe {
                        let hole_slot =
                            g.v.as_mut_ptr()
                                .add((g.processed_len - g.deleted_cnt) * g.v.val_size);
                        ptr::copy_nonoverlapping(cur, hole_slot, g.v.val_size);
                    }
                }

                g.processed_len += 1;
            }
        }

        // Stage 1: Nothing was deleted.
        process_loop::<false>(original_len, retain, &mut g, drop_in_place);

        // Stage 2: Some elements were deleted.
        process_loop::<true>(original_len, retain, &mut g, drop_in_place);

        // All items are processed. This can be optimized to `set_len` by LLVM.
        drop(g);
    }

    fn truncate(&mut self, len: usize, drop_slice_in_place: &dyn Fn(*mut u8, usize)) {
        // This is safe because:
        //
        // * the slice passed to `drop_in_place` is valid; the `len > self.len` case
        //   avoids creating an invalid slice, and
        // * the `len` of the vector is shrunk before calling `drop_in_place`, such that
        //   no value will be dropped twice in case `drop_in_place` were to panic once
        //   (if it panics twice, the program aborts).
        unsafe {
            // Note: It's intentional that this is `>` and not `>=`.
            //       Changing it to `>=` has negative performance
            //       implications in some cases. See #78884 for more.
            if len > self.length {
                return;
            }
            let remaining_len = self.length - len;
            self.length = len;
            drop_slice_in_place(self.as_mut_ptr().add(len * self.val_size), remaining_len);
        }
    }

    fn is_sorted_by(&self, compare: &dyn Fn(*const u8, *const u8) -> Ordering) -> bool {
        if self.len() <= 1 {
            return true;
        }

        let mut last = self.index(0);

        for i in 1..self.len() {
            let current = self.index(i);
            if compare(last, current) == Ordering::Greater {
                return false;
            }
            last = current;
        }
        true
    }
}

// Internals
impl RawVec {
    /// Sets the current vector's pointer
    ///
    /// # Safety
    ///
    /// `ptr` must be a pointer to `capacity` allocated bytes of which `length`
    /// are valid
    unsafe fn set_ptr(&mut self, ptr: *mut u8) {
        self.ptr = ptr;
    }

    /// Sets the current vector's capacity
    ///
    /// # Safety
    ///
    /// `capacity` must be the exact amount of memory currently allocated
    unsafe fn set_capacity(&mut self, capacity: usize) {
        self.capacity = capacity;
    }

    fn realloc(&mut self, new_capacity: usize) {
        debug_assert!(self.val_size > 0);
        debug_assert!(new_capacity >= self.len());
        if new_capacity == self.capacity() {
            return;
        }

        let current_layout = self.layout_for(self.capacity());
        let new_ptr = if new_capacity > 0 {
            let new_layout = self.layout_for(new_capacity);
            let new_ptr = unsafe {
                if self.capacity() == 0 {
                    alloc::alloc(new_layout)
                } else {
                    alloc::realloc(self.as_mut_ptr(), current_layout, new_layout.size())
                }
            };

            // If (re)allocation fails, raise an allocation error
            if new_ptr.is_null() {
                handle_alloc_error(new_layout);
            }
            new_ptr
        } else {
            unsafe { alloc::dealloc(self.as_mut_ptr(), current_layout) };

            // This is equivalent to `NonNull::<T>::dangling().as_ptr() as *mut
            // u8` but it doesn't require `T` to be available.
            self.align as *mut u8
        };

        unsafe {
            self.set_capacity(new_capacity);
            self.set_ptr(new_ptr);
        }
    }

    fn layout_for(&self, capacity: usize) -> Layout {
        let bytes = self.next_aligned_capacity(capacity * self.val_size);
        Layout::from_size_align(bytes, self.align).unwrap()
    }

    pub fn shrink_to_fit(&mut self) {
        if self.has_spare_capacity() && self.capacity != usize::MAX {
            self.realloc(self.len())
        }
    }

    /// Selects the given capacity's next growth target
    ///
    /// Expects that the current element's size is non-zero
    // TODO: We could be allocator-aware here and specialize for something like
    // mimalloc here
    fn next_capacity(&self, capacity: usize) -> usize {
        debug_assert_ne!(self.val_size, 0);
        let size = self.val_size;

        if capacity == 0 {
            max(128 / size, 1)

            // For large vectors, grow by 2x
        } else if capacity > (4096 * 32) / size {
            capacity * 2

            // For medium vectors, grow by 1.5x
        } else {
            (capacity * 3).div_ceil(2)
        }
    }

    fn next_aligned_capacity(&self, capacity: usize) -> usize {
        let remaining = capacity % self.align;
        if remaining == 0 {
            capacity
        } else {
            capacity + (self.align - remaining)
        }
    }

    fn range_inner(
        &self,
        start_bound: Bound<usize>,
        end_bound: Bound<usize>,
    ) -> (*const u8, usize) {
        let (start, start_overflowed) = match start_bound {
            Bound::Included(start) => (start, false),
            Bound::Excluded(start) => start.overflowing_add(1),
            Bound::Unbounded => (0, false),
        };
        let (end, end_overflowed) = match end_bound {
            Bound::Included(end) => end.overflowing_add(1),
            Bound::Excluded(end) => (end, false),
            Bound::Unbounded => (self.len(), false),
        };

        if !start_overflowed && !end_overflowed && start <= end && end <= self.len() {
            // Safety: The index is inbounds
            unsafe {
                let len = end - start;
                let start = self.as_ptr().add(start * self.val_size);
                (start, len)
            }
        } else {
            range_out_of_bounds(start_bound, end_bound, self.len())
        }
    }

    fn drop_vec(&mut self, drop_slice_in_place: &dyn Fn(*mut u8, usize)) {
        // Drop all remaining elements
        self.clear(drop_slice_in_place);

        if self.capacity > 0 && self.val_size > 0 {
            // Deallocate all allocated memory
            let layout = self.layout_for(self.capacity());
            // Safety: The current pointer was allocated with the given layout
            unsafe { alloc::dealloc(self.as_mut_ptr(), layout) };
        }
    }
}

#[cold]
#[inline(never)]
fn index_out_of_bounds(index: usize, length: usize) -> ! {
    panic!("index out of bounds, length is {length} but the index was {index}")
}

#[cold]
#[inline(never)]
fn range_out_of_bounds(start: Bound<usize>, end: Bound<usize>, length: usize) -> ! {
    struct RangeFmt(Bound<usize>, Bound<usize>);

    impl Display for RangeFmt {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match (self.0, self.1) {
                (Bound::Included(start), Bound::Included(end)) => write!(f, "{start}..={end}"),
                (Bound::Included(start), Bound::Excluded(end)) => write!(f, "{start}..{end}"),
                (Bound::Included(start), Bound::Unbounded) => write!(f, "{start}.."),
                (Bound::Excluded(start), Bound::Included(end)) => {
                    write!(f, "{}..={end}", start.saturating_add(1))
                }
                (Bound::Excluded(start), Bound::Excluded(end)) => {
                    write!(f, "{}..{end}", start.saturating_add(1))
                }
                (Bound::Excluded(start), Bound::Unbounded) => {
                    write!(f, "{}..", start.saturating_add(1))
                }
                (Bound::Unbounded, Bound::Included(end)) => write!(f, "..={end}"),
                (Bound::Unbounded, Bound::Excluded(end)) => write!(f, "..{end}"),
                (Bound::Unbounded, Bound::Unbounded) => f.write_str(".."),
            }
        }
    }

    panic!(
        "index {} is out of bounds of a slice with length {length}",
        RangeFmt(start, end),
    )
}

pub struct RawIter<'a> {
    ptr: *mut u8,
    start: usize,
    end: usize,
    val_size: usize,
    __marker: PhantomData<&'a ()>,
}

impl<'a> RawIter<'a> {
    fn new(vec: &'a RawVec) -> Self {
        let ptr = vec.ptr;

        Self {
            ptr,
            start: 0,
            end: vec.len(),
            val_size: vec.val_size,
            __marker: PhantomData,
        }
    }

    unsafe fn get_unchecked(&self, n: usize) -> *mut u8 {
        self.ptr.add(self.val_size * n)
    }

    fn iter_len(&self) -> usize {
        self.end - self.start
    }
}

impl Iterator for RawIter<'_> {
    type Item = *const u8;

    fn next(&mut self) -> Option<Self::Item> {
        if self.start == self.end {
            None
        } else {
            self.start += 1;
            // Safety: The iterator is non-empty
            Some(unsafe { self.get_unchecked(self.start - 1) })
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.iter_len();
        (len, Some(len))
    }

    fn count(self) -> usize {
        self.iter_len()
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        if n >= self.iter_len() {
            self.start = self.end;
            None
        } else {
            self.start += n;
            Some(unsafe { self.get_unchecked(self.start - 1) })
        }
    }

    fn last(mut self) -> Option<Self::Item> {
        if self.start >= self.end {
            None
        } else {
            self.start = self.end;
            Some(unsafe { self.get_unchecked(self.start - 1) })
        }
    }
}

impl DoubleEndedIterator for RawIter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.end == self.start {
            None
        } else {
            self.end -= 1;
            // Safety: The iterator is non-empty
            Some(unsafe { self.get_unchecked(self.end) })
        }
    }

    fn nth_back(&mut self, n: usize) -> Option<Self::Item> {
        if n >= self.iter_len() {
            self.end = self.start;
            None
        } else {
            self.end -= n;
            Some(unsafe { self.get_unchecked(self.end) })
        }
    }
}

impl ExactSizeIterator for RawIter<'_> {
    fn len(&self) -> usize {
        self.iter_len()
    }

    // TODO: We'd like to be able to implement `ExactSizeIterator::is_empty()` but
    // it's currently unstable
}

impl FusedIterator for RawIter<'_> {}

// FIXME: properly implement rkyv traits for LeanVec
pub struct LeanVec<T> {
    pub vec: RawVec,
    phantom: PhantomData<T>,
}

/// An archived [`LeanVec`]
// The code for `ArchivedLeanVec` is copied out of `rkyv`, which has the
// following license statement:
//
// Copyright 2021 David Koloski
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
pub struct ArchivedLeanVec<T> {
    ptr: RelPtr<T>,
    len: Archived<usize>,
}

impl<T> ArchivedLeanVec<T> {
    /// Returns a pointer to the first element of the archived vec.
    #[inline]
    pub fn as_ptr(&self) -> *const T {
        self.ptr.as_ptr()
    }

    /// Returns the number of elements in the archived vec.
    #[inline]
    pub fn len(&self) -> usize {
        self.len as usize
    }

    /// Returns whether the archived vec is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Gets the elements of the archived vec as a slice.
    #[inline]
    pub fn as_slice(&self) -> &[T] {
        unsafe { core::slice::from_raw_parts(self.as_ptr(), self.len()) }
    }

    /// Gets the elements of the archived vec as a pinned mutable slice.
    #[inline]
    pub fn pin_mut_slice(self: Pin<&mut Self>) -> Pin<&mut [T]> {
        unsafe {
            self.map_unchecked_mut(|s| core::slice::from_raw_parts_mut(s.ptr.as_mut_ptr(), s.len()))
        }
    }

    // This method can go away once pinned slices have indexing support
    // https://github.com/rust-lang/rust/pull/78370

    /// Gets the element at the given index ot this archived vec as a pinned
    /// mutable reference.
    #[inline]
    pub fn index_pin<I>(self: Pin<&mut Self>, index: I) -> Pin<&mut <[T] as Index<I>>::Output>
    where
        [T]: IndexMut<I>,
    {
        unsafe { self.pin_mut_slice().map_unchecked_mut(|s| &mut s[index]) }
    }

    /// Resolves an archived `LeanVec` from a given slice.
    ///
    /// # Safety
    ///
    /// - `pos` must be the position of `out` within the archive
    /// - `resolver` must be the result of serializing `value`
    #[inline]
    pub unsafe fn resolve_from_slice<U: Archive<Archived = T>>(
        slice: &[U],
        pos: usize,
        resolver: LeanVecResolver,
        out: *mut Self,
    ) {
        Self::resolve_from_len(slice.len(), pos, resolver, out);
    }

    /// Resolves an archived `LeanVec` from a given length.
    ///
    /// # Safety
    ///
    /// - `pos` must be the position of `out` within the archive
    /// - `resolver` must bet he result of serializing `value`
    #[inline]
    pub unsafe fn resolve_from_len(
        len: usize,
        pos: usize,
        resolver: LeanVecResolver,
        out: *mut Self,
    ) {
        let (fp, fo) = out_field!(out.ptr);
        RelPtr::emplace(pos + fp, resolver.pos, fo);
        let (fp, fo) = out_field!(out.len);
        usize::resolve(&len, pos + fp, (), fo);
    }

    /// Serializes an archived `LeanVec` from a given slice.
    #[inline]
    pub fn serialize_from_slice<U: Serialize<S, Archived = T>, S: Serializer + ?Sized>(
        slice: &[U],
        serializer: &mut S,
    ) -> Result<LeanVecResolver, S::Error>
    where
        // This bound is necessary only in no-alloc, no-std situations
        // SerializeUnsized is only implemented for U: Serialize<Resolver = ()> in that case
        [U]: SerializeUnsized<S>,
    {
        Ok(LeanVecResolver {
            pos: slice.serialize_unsized(serializer)?,
        })
    }
}

///The resolver for an archived [`LeanVec`]
pub struct LeanVecResolver {
    pos: usize,
}
impl<T> Archive for LeanVec<T>
where
    T: Archive,
{
    type Archived = ArchivedLeanVec<T::Archived>;
    type Resolver = LeanVecResolver;
    unsafe fn resolve(&self, pos: usize, resolver: Self::Resolver, out: *mut Self::Archived) {
        ArchivedLeanVec::resolve_from_slice(self.as_slice(), pos, resolver, out);
    }
}
impl<S, T> Serialize<S> for LeanVec<T>
where
    T: Serialize<S>,
    S: ScratchSpace + Serializer + ?Sized,
{
    fn serialize(&self, serializer: &mut S) -> ::core::result::Result<Self::Resolver, S::Error> {
        ArchivedLeanVec::<T::Archived>::serialize_from_slice(self.as_slice(), serializer)
    }
}
impl<D, T> Deserialize<LeanVec<T>, D> for Archived<LeanVec<T>>
where
    D: Fallible + ?Sized,
    T: Archive,
    [T::Archived]: DeserializeUnsized<[T], D>,
{
    fn deserialize(&self, deserializer: &mut D) -> ::core::result::Result<LeanVec<T>, D::Error> {
        unsafe {
            let data_address = self
                .as_slice()
                .deserialize_unsized(deserializer, |layout| alloc::alloc(layout))?;
            let metadata = self.as_slice().deserialize_metadata(deserializer)?;
            let ptr = ptr_meta::from_raw_parts_mut(data_address, metadata);
            Ok(Box::<[T]>::from_raw(ptr).into())
        }
    }
}

impl<T> PartialEq for ArchivedLeanVec<T>
where
    T: Ord,
{
    fn eq(&self, other: &Self) -> bool {
        self.as_slice().eq(other.as_slice())
    }
}

impl<T> Eq for ArchivedLeanVec<T> where T: Ord {}

impl<T> PartialOrd for ArchivedLeanVec<T>
where
    T: Ord,
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for ArchivedLeanVec<T>
where
    T: Ord,
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_slice().cmp(other.as_slice())
    }
}

impl<T> LeanVec<T> {
    pub fn new() -> Self {
        Self {
            vec: RawVec::new(
                size_of::<T>(),
                align_of::<T>(),
                NonNull::<T>::dangling().as_ptr() as *mut u8,
            ),
            phantom: PhantomData,
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            vec: RawVec::with_capacity(
                size_of::<T>(),
                align_of::<T>(),
                capacity,
                NonNull::<T>::dangling().as_ptr() as *mut u8,
            ),
            phantom: PhantomData,
        }
    }

    /// Creates a `LeanVec<T>` directly from a pointer, a capacity, and a
    /// length.
    ///
    /// # Safety
    ///
    /// See `std::vec::from_raw_parts`.
    pub unsafe fn from_raw_parts(ptr: *mut T, length: usize, capacity: usize) -> Self {
        Self {
            vec: RawVec::from_raw_parts(
                ptr as *mut u8,
                length,
                capacity,
                size_of::<T>(),
                align_of::<T>(),
            ),
            phantom: PhantomData,
        }
    }
    pub const fn len(&self) -> usize {
        self.vec.len()
    }

    pub fn spare_capacity(&self) -> usize {
        self.vec.spare_capacity()
    }

    pub fn has_spare_capacity(&self) -> bool {
        self.vec.has_spare_capacity()
    }

    /// Sets the current vector's length to `new_len`
    ///
    /// # Safety
    ///
    /// - `new_len` must be less than or equal to `self.capacity()`.
    /// - The elements at `old_len..new_len` must be initialized.
    pub unsafe fn set_len(&mut self, new_len: usize) {
        self.vec.set_len(new_len)
    }

    pub const fn is_empty(&self) -> bool {
        self.vec.is_empty()
    }

    pub const fn capacity(&self) -> usize {
        self.vec.capacity()
    }

    pub const fn as_ptr(&self) -> *const T {
        self.vec.as_ptr() as *const T
    }

    pub fn as_slice(&self) -> &[T] {
        unsafe { slice::from_raw_parts(self.as_ptr(), self.len()) }
    }

    pub fn as_mut_ptr(&mut self) -> *mut T {
        self.vec.as_mut_ptr() as *mut T
    }

    pub fn as_mut_slice(&mut self) -> &mut [T] {
        unsafe { slice::from_raw_parts_mut(self.as_mut_ptr(), self.len()) }
    }

    pub(crate) fn get(&self, index: usize) -> &T {
        unsafe { &*(self.vec.index(index) as *const T) }
    }

    pub(crate) fn get_mut(&mut self, index: usize) -> &mut T {
        unsafe { &mut *(self.vec.index_mut(index) as *mut T) }
    }

    pub(crate) unsafe fn get_unchecked(&self, index: usize) -> &T {
        &*(self.vec.index_unchecked(index) as *const T)
    }

    pub(crate) unsafe fn get_mut_unchecked(&mut self, index: usize) -> &mut T {
        &mut *(self.vec.index_mut_unchecked(index) as *mut T)
    }

    pub fn reserve(&mut self, additional: usize) {
        self.vec.reserve(additional)
    }

    pub fn reserve_exact(&mut self, additional: usize) {
        self.vec.reserve_exact(additional)
    }

    pub fn clear(&mut self) {
        self.vec.clear(&Self::drop_slice_in_place)
    }

    fn drop_slice_in_place(ptr: *mut u8, size: usize) {
        for i in 0..size {
            unsafe { drop_in_place((ptr as *mut T).add(i)) }
        }
    }

    pub fn truncate(&mut self, len: usize) {
        self.vec.truncate(len, &Self::drop_slice_in_place);
    }

    pub fn push(&mut self, value: T) {
        let value = ManuallyDrop::new(value);

        unsafe { self.vec.push_raw(&*value as *const T as *const u8) };
    }

    /// Push `value` to `self` without checking for spare capacity or allocating
    /// new capacity.
    ///
    /// # Safety
    ///
    /// Requires that `self.capacity() > self.len()`.
    pub unsafe fn push_unchecked(&mut self, value: T) {
        let value = ManuallyDrop::new(value);
        self.vec.push_unchecked(&*value as *const T as *const u8);
    }

    pub fn extend_from_slice(&mut self, other: &[T])
    where
        T: Clone,
    {
        // TODO: a more efficient implementation using RawVec::clone_from_range to
        // avoid an extra copy + bookkeeping.
        self.reserve(other.len());
        for val in other.iter() {
            self.push(val.clone());
        }
    }

    pub fn append_from_slice(&mut self, other: &mut [T])
    where
        T: Default,
    {
        self.reserve(other.len());
        for val in other.iter_mut() {
            self.push(take(val))
        }
    }

    pub fn append(&mut self, other: &mut Self) {
        unsafe {
            self.vec.append_raw_range(other.vec.as_ptr(), other.len());
            other.vec.set_len(0);
        }
    }

    pub fn dedup(&mut self)
    where
        T: Eq,
    {
        self.dedup_by(|x, y| x == y)
    }

    pub fn dedup_by<F>(&mut self, same_bucket: F)
    where
        F: FnMut(&mut T, &mut T) -> bool,
    {
        self.dedup_by_starting_at(0, same_bucket);
    }

    pub fn dedup_by_starting_at<F>(&mut self, start: usize, mut same_bucket: F)
    where
        F: FnMut(&mut T, &mut T) -> bool,
    {
        self.vec.dedup_starting_at(
            start,
            &mut |x, y| {
                let x = unsafe { &mut *(x as *mut T) };
                let y = unsafe { &mut *(y as *mut T) };

                same_bucket(x, y)
            },
            &mut |x| {
                let x = x as *mut T;
                unsafe { ptr::drop_in_place(x) };
            },
        )
    }

    pub fn retain<F>(&mut self, f: F)
    where
        F: FnMut(&mut T) -> bool,
    {
        self.retain_starting_at(0, f);
    }

    pub fn retain_starting_at<F>(&mut self, start: usize, mut f: F)
    where
        F: FnMut(&mut T) -> bool,
    {
        self.vec.retain_starting_at(
            start,
            &mut |x| {
                let x = unsafe { &mut *(x as *mut T) };
                f(x)
            },
            &mut |x| {
                let x = x as *mut T;
                unsafe { ptr::drop_in_place(x) };
            },
        )
    }

    pub fn is_sorted_by<F>(&self, compare: F) -> bool
    where
        F: Fn(&T, &T) -> Ordering,
    {
        self.vec.is_sorted_by(&|x, y| {
            let x = unsafe { &*(x as *const T) };
            let y = unsafe { &*(y as *const T) };
            compare(x, y)
        })
    }

    pub fn raw_iter(&self) -> RawIter<'_> {
        RawIter::new(&self.vec)
    }

    /*pub fn clone_from_range(&mut self, other: &Self, Range { start, end }: Range<usize>) {
        if self.vtable().type_id() != other.vtable().type_id() {
            mismatched_push_type_ids(other.vtable().type_name(), self.vtable().type_name());
        } else if start == end {
            return;
        }

        let (range_start, elements) =
            other.range_inner(Bound::Included(start), Bound::Excluded(end));

        // Reserve the required capacity
        self.reserve(elements);

        // Clone the values from the source slice
        unsafe {
            self.vtable().clone_slice(
                range_start,
                self.as_mut_ptr().add(self.len() * self.vtable().size_of()),
                elements,
            );
        }

        // Increment the vec's length
        self.length += elements;
    }*/
}

impl<T> From<Vec<T>> for LeanVec<T> {
    fn from(other: Vec<T>) -> Self {
        let mut other = ManuallyDrop::new(other);
        unsafe { Self::from_raw_parts(other.as_mut_ptr(), other.len(), other.capacity()) }
    }
}

impl<T> From<Box<[T]>> for LeanVec<T> {
    fn from(other: Box<[T]>) -> Self {
        let mut other = ManuallyDrop::new(other);
        unsafe { Self::from_raw_parts(other.as_mut_ptr(), other.len(), other.len()) }
    }
}

impl<T> From<LeanVec<T>> for Vec<T> {
    fn from(vec: LeanVec<T>) -> Self {
        let mut vec = ManuallyDrop::new(vec);
        unsafe { Vec::from_raw_parts(vec.as_mut_ptr(), vec.len(), vec.capacity()) }
    }
}

impl<T> Drop for LeanVec<T> {
    fn drop(&mut self) {
        self.vec.drop_vec(&Self::drop_slice_in_place);
    }
}

// Safety: `RawVec VTable` requires that the type its associated with is Send
unsafe impl<T: Send> Send for LeanVec<T> {}

// Safety: `RawVec VTable` requires that the type its associated with is Sync
unsafe impl<T: Sync> Sync for LeanVec<T> {}

impl<T, R: RangeBounds<usize>> Index<R> for LeanVec<T> {
    type Output = [T];

    fn index(&self, range: R) -> &Self::Output {
        let (ptr, len) = self.vec.range(range);
        unsafe { slice::from_raw_parts(ptr as *const T, len) }
    }
}

impl<T, R: RangeBounds<usize>> IndexMut<R> for LeanVec<T> {
    fn index_mut(&mut self, range: R) -> &mut Self::Output {
        let (ptr, len) = self.vec.range_mut(range);
        unsafe { slice::from_raw_parts_mut(ptr as *mut T, len) }
    }
}

impl<T: Clone> Clone for LeanVec<T> {
    fn clone(&self) -> Self {
        let mut result = Self::with_capacity(self.len());
        result.extend_from_slice(self.as_slice());
        result
    }
}

impl<T> Default for LeanVec<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: PartialEq> PartialEq for LeanVec<T> {
    fn eq(&self, other: &Self) -> bool {
        self.vec.eq(&other.vec, &|x, y| {
            let x = unsafe { &*(x as *const T) };
            let y = unsafe { &*(y as *const T) };
            x == y
        })
    }
}

impl<T: Eq> Eq for LeanVec<T> {}

impl<T: Ord> PartialOrd for LeanVec<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: Ord> Ord for LeanVec<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.vec.cmp(&other.vec, &|x, y| {
            let x = unsafe { &*(x as *const T) };
            let y = unsafe { &*(y as *const T) };
            x.cmp(y)
        })
    }
}

impl<T: Hash> Hash for LeanVec<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.vec.hash(&mut |x| x.hash(state));
    }
}

impl<T: SizeOf> SizeOf for LeanVec<T> {
    fn size_of_children(&self, context: &mut Context) {
        self.vec.size_of_children(context, &mut |x, ctx| {
            let x = unsafe { &*(x as *const T) };
            T::size_of_children(x, ctx);
        });
    }
}

impl<T: Debug> Debug for LeanVec<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.vec.debug(f, &|x, f| {
            let x = unsafe { &*(x as *const T) };
            Debug::fmt(x, f)
        })
    }
}
