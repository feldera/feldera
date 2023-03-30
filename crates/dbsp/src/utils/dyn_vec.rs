use size_of::{Context, SizeOf};
use std::{
    alloc,
    any::{self, TypeId},
    fmt::{self, Debug, Display},
    iter::FusedIterator,
    marker::PhantomData,
    mem::MaybeUninit,
    ops::{Bound, Range, RangeBounds},
    ptr, slice,
};
use std::{
    alloc::{handle_alloc_error, Layout},
    cmp::max,
    num::NonZeroUsize,
    ptr::NonNull,
};

// TODO: Drop, Clone, Debug
pub struct DynVec<VTable: DynVecVTable> {
    ptr: NonNull<u8>,
    length: usize,
    capacity: usize,
    vtable: &'static VTable,
}

impl<VTable> DynVec<VTable>
where
    VTable: DynVecVTable,
{
    pub fn new(vtable: &'static VTable) -> Self {
        Self {
            ptr: vtable.dangling(),
            length: 0,
            capacity: if vtable.is_zst() { usize::MAX } else { 0 },
            vtable,
        }
    }

    pub fn with_capacity(vtable: &'static VTable, capacity: usize) -> Self {
        let mut this = Self::new(vtable);
        this.reserve_exact(capacity);
        this
    }

    pub const fn len(&self) -> usize {
        self.length
    }

    /// Sets the current vector's length to `new_len`
    ///
    /// # Safety
    ///
    /// - `new_len` must be less than or equal to [`self.capacity()`].
    /// - The elements at `old_len..new_len` must be initialized.
    ///
    /// [`self.capacity()`]: DynVec::capacity
    pub unsafe fn set_len(&mut self, new_len: usize) {
        self.length = new_len;
    }

    pub const fn is_empty(&self) -> bool {
        self.length == 0
    }

    pub const fn capacity(&self) -> usize {
        self.capacity
    }

    pub const fn vtable(&self) -> &'static VTable {
        self.vtable
    }

    pub const fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr() as *const u8
    }

    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr.as_ptr()
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

        self.grow(new_capacity);
    }

    pub fn reserve_exact(&mut self, additional: usize) {
        let required = self.len() + additional;
        if required <= self.capacity() {
            return;
        }

        self.grow(required);
    }

    pub fn clear(&mut self) {
        if self.is_empty() {
            return;
        }

        let length = self.len();
        unsafe {
            // Pre-poop our pants, set our length to zero in case dropping elements panics
            self.set_len(0);

            // Drop all elements
            self.vtable().drop_slice_in_place(self.as_mut_ptr(), length);
        }
    }

    pub fn iter(&self) -> DynIter<'_, VTable> {
        DynIter::new(self)
    }

    // pub fn iter_mut(&mut self) -> DynIterMut<'_> {
    //     DynIterMut::new(self)
    // }

    pub fn index(&self, idx: usize) -> *const u8 {
        if idx < self.len() {
            // Safety: The index is inbounds
            unsafe { self.as_ptr().add(idx * self.vtable().size_of()) }
        } else {
            index_out_of_bounds(idx, self.len())
        }
    }

    pub fn index_as<T: 'static>(&self, idx: usize) -> &T {
        if TypeId::of::<T>() != self.vtable().type_id() {
            mismatched_index_type_ids(any::type_name::<T>(), self.vtable().type_name())
        }

        unsafe { &*(self.index(idx) as *const T) }
    }

    pub fn index_mut(&mut self, idx: usize) -> *mut u8 {
        if idx < self.len() {
            // Safety: The index is inbounds
            unsafe { self.as_mut_ptr().add(idx * self.vtable().size_of()) }
        } else {
            index_out_of_bounds(idx, self.len())
        }
    }

    pub fn range<R>(&self, range: R) -> &[MaybeUninit<u8>]
    where
        R: RangeBounds<usize>,
    {
        let (ptr, len) = self.range_inner(range.start_bound().cloned(), range.end_bound().cloned());

        // Safety: The slice is valid as uninit data
        unsafe {
            slice::from_raw_parts(ptr as *const MaybeUninit<u8>, len * self.vtable().size_of())
        }
    }

    pub fn range_mut<R>(&mut self, range: R) -> (*mut u8, usize)
    where
        R: RangeBounds<usize>,
    {
        let (ptr, len) = self.range_inner(range.start_bound().cloned(), range.end_bound().cloned());
        (ptr as *mut u8, len)
    }

    pub fn contains_type<T: 'static>(&self) -> bool {
        self.vtable().type_id() == TypeId::of::<T>()
    }

    /// Push a value to the current vector
    pub fn push<T: 'static>(&mut self, value: T) {
        if !self.contains_type::<T>() {
            mismatched_push_type_ids(any::type_name::<T>(), self.vtable().type_name());
        }

        // Safety: We've ensured that the given type is the same as the vec's element
        // type
        unsafe { self.push_untypechecked(value) }
    }

    /// Push a value to the current vector without checking its type
    ///
    /// # Safety
    ///
    /// The current vector must contain elements of type `T`
    pub unsafe fn push_untypechecked<T: 'static>(&mut self, value: T) {
        if cfg!(debug_assertions) && !self.contains_type::<T>() {
            mismatched_push_type_ids(any::type_name::<T>(), self.vtable().type_name());
        }

        // Reserve space for the new element
        self.reserve(1);

        // Write the value into the vector
        // Safety: We've reserved space for the new element
        unsafe {
            self.as_mut_ptr()
                .add(self.len() * self.vtable().size_of())
                .cast::<T>()
                .write(value)
        };

        // Increment the vector's length
        self.length += 1;
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
                self.as_mut_ptr().add(self.len() * self.vtable().size_of()),
                self.vtable().size_of(),
            );
        }

        // Increment the vector's length
        self.length += 1;
    }

    pub fn clone_from_range(&mut self, other: &Self, Range { start, end }: Range<usize>) {
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
    }
}

// Internals
impl<VTable> DynVec<VTable>
where
    VTable: DynVecVTable,
{
    /// Sets the current vector's pointer
    ///
    /// # Safety
    ///
    /// `ptr` must be a pointer to `capacity` allocated bytes of which `length`
    /// are valid
    unsafe fn set_ptr(&mut self, ptr: NonNull<u8>) {
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

    fn grow(&mut self, new_capacity: usize) {
        debug_assert!(!self.vtable.is_zst());
        debug_assert!(new_capacity >= self.capacity());
        if new_capacity == self.capacity() {
            return;
        }

        let new_layout = self.layout_for(new_capacity);
        let new_ptr = unsafe {
            if self.capacity() == 0 {
                alloc::alloc(new_layout)
            } else {
                let current_layout = self.layout_for(self.capacity());
                alloc::realloc(self.as_mut_ptr(), current_layout, new_layout.size())
            }
        };

        match NonNull::new(new_ptr) {
            Some(new_ptr) => unsafe {
                self.set_capacity(new_capacity);
                self.set_ptr(new_ptr);
            },

            // If (re)allocation fails, raise an allocation error
            None => handle_alloc_error(new_layout),
        }
    }

    fn layout_for(&self, capacity: usize) -> Layout {
        let bytes = self.next_aligned_capacity(capacity * self.vtable.size_of());
        Layout::from_size_align(bytes, self.vtable.align_of().get()).unwrap()
    }

    /// Selects the given capacity's next growth target
    ///
    /// Expects that the current element's size is non-zero
    // TODO: We could be allocator-aware here and specialize for something like
    // mimalloc here
    fn next_capacity(&self, capacity: usize) -> usize {
        debug_assert!(!self.vtable.is_zst());
        let size = self.vtable.size_of();

        if capacity == 0 {
            max(128 / size, 1)

        // For large vectors, grow by 2x
        } else if capacity > (4096 * 32) / size {
            capacity * 2

        // For medium vectors, grow by 1.5x
        } else {
            (capacity * 3 + 1) / 2
        }
    }

    fn next_aligned_capacity(&self, capacity: usize) -> usize {
        let remaining = capacity % self.vtable.align_of();
        if remaining == 0 {
            capacity
        } else {
            capacity + (self.vtable.align_of().get() - remaining)
        }
    }

    fn realloc_with_new_vtable(&mut self, new_vtable: &'static VTable, new_capacity: usize) {
        debug_assert_eq!(self.len(), 0);

        let old_vtable = self.vtable();

        // If the previous and the current types are compatible can simply reuse the
        // current buffer
        //
        // FIXME: This may be unsound when alignments are different, regardless of
        // whether they're compatible alignments. `realloc()` offers no interface to
        // change alignments, so we may be skunked on the `realloc()` call since the
        // layout we provide to it has a different alignment than the one the allocation
        // was created with
        //
        // FIXME: If we could use the `Allocator` api this would be a lot simpler and
        // easier, we could reallocate any input vec into another just by using
        // `Allocator::grow()`
        if new_vtable.align_of() <= old_vtable.align_of() {
            // If the sizes are identical we just have to replace the old vtable and reserve
            // the requested capacity
            if new_vtable.size_of() == old_vtable.size_of() {
                self.vtable = new_vtable;
                self.reserve_exact(new_capacity);
                return;

            // If neither types are zsts and the current buffer's size is a
            // multiple of the new element's size we can reuse the current
            // buffer. This most often manifests for things like `DynVec<[u32]>
            // -> DynVec<[u8]>` where the alignments are compatible so all we
            // have to do is multiply the current vec's capacity to fit the new
            // element size, so a `DynVec<[u32]>` with a capacity of 4 would
            // become a `DynVec<[u8]>` with a capacity of 16.
            // After we do this resizing/rescaling we reserve the requested
            // capacity
            } else if !new_vtable.is_zst()
                && !old_vtable.is_zst()
                && (self.capacity() * old_vtable.size_of()) % new_vtable.size_of() == 0
            {
                let new_capacity = (self.capacity() * old_vtable.size_of()) / new_vtable.size_of();
                unsafe { self.set_capacity(new_capacity) };

                // Replace the old vtable and reserve the requested capacity
                self.vtable = new_vtable;
                self.reserve_exact(new_capacity);
                return;
            }
        }

        // Otherwise we have to start fresh, meaning we have to drop the old vector and
        // create an entirely new one
        //
        // This happens in cases when:
        // - The new vtable's alignment is greater than the current vtables, `realloc()`
        //   calls cannot rectify alignment mismatches
        // - Either the old vtable or the new vtable is a zst (but not both of them)
        // - The sizes of the two element types can't be rectified
        *self = Self::with_capacity(new_vtable, new_capacity);
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
                let start = self.as_ptr().add(start * self.vtable().size_of());
                (start, len)
            }
        } else {
            range_out_of_bounds(start_bound, end_bound, self.len())
        }
    }
}

impl<VTable> SizeOf for DynVec<VTable>
where
    VTable: DynVecVTable + SizeOf,
{
    fn size_of_children(&self, context: &mut size_of::Context) {
        SizeOf::size_of_children(&self.vtable(), context);

        // We only cause an allocation to occur when we have allocated capacity and our
        // element type isn't a zst
        if self.capacity() != 0 && !self.vtable().is_zst() {
            context
                .add_vectorlike(
                    self.len(),
                    self.capacity(),
                    DynVecVTable::size_of(self.vtable()),
                )
                .add_distinct_allocation();
        }

        for elem in self.iter() {
            // Safety: `elem` is a valid pointer to a valid value
            unsafe { DynVecVTable::size_of_children(self.vtable(), elem, context) };
        }
    }
}

impl<VTable> Clone for DynVec<VTable>
where
    VTable: DynVecVTable,
{
    fn clone(&self) -> Self {
        let mut clone = Self::with_capacity(self.vtable(), self.len());
        unsafe {
            self.vtable
                .clone_slice(self.as_ptr(), clone.as_mut_ptr(), self.len());
            clone.set_len(self.len());
        }

        clone
    }

    fn clone_from(&mut self, source: &Self) {
        // Clear the current vector's contents
        self.clear();

        // Resize the current vector while simultaneously swapping its vtable for the
        // source vtable
        self.realloc_with_new_vtable(source.vtable(), source.len());
        debug_assert!(self.capacity() >= source.len());

        // Clone the elements over from the source vector
        unsafe {
            self.vtable
                .clone_slice(source.as_ptr(), self.as_mut_ptr(), source.len());
            self.set_len(source.len());
        }
    }
}

impl<VTable> Drop for DynVec<VTable>
where
    VTable: DynVecVTable,
{
    fn drop(&mut self) {
        // Drop all remaining elements
        self.clear();

        // Deallocate all allocated memory
        let layout = self.layout_for(self.capacity());
        // Safety: The current pointer was allocated with the given layout
        unsafe { alloc::dealloc(self.as_mut_ptr(), layout) };
    }
}

// Safety: `DynVecVTable` requires that the type its associated with is Send
unsafe impl<VTable: DynVecVTable> Send for DynVec<VTable> {}

// Safety: `DynVecVTable` requires that the type its associated with is Sync
unsafe impl<VTable: DynVecVTable> Sync for DynVec<VTable> {}

impl<VTable> Debug for DynVec<VTable>
where
    VTable: DynVecVTable,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        struct DebugErased<'a, VTable>(*const u8, &'a VTable);

        impl<VTable> Debug for DebugErased<'_, VTable>
        where
            VTable: DynVecVTable,
        {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                unsafe { self.1.debug(self.0, f) }
            }
        }

        f.debug_list()
            .entries(self.iter().map(|x| DebugErased(x, self.vtable)))
            .finish()
    }
}

#[cold]
#[inline(never)]
fn index_out_of_bounds(index: usize, length: usize) -> ! {
    panic!("index out of bounds, length is {length} but the index was {index}")
}

#[cold]
#[inline(never)]
fn mismatched_push_type_ids(element: &str, expected: &str) -> ! {
    panic!("attempted to push a value of type {element} to a DynVec containing elements of type {expected}")
}

#[cold]
#[inline(never)]
fn mismatched_index_type_ids(element: &str, expected: &str) -> ! {
    panic!("attempted to index a value of type {element} from a DynVec containing elements of type {expected}")
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

impl<VTable> PartialEq for DynVec<VTable>
where
    VTable: DynVecVTable,
{
    fn eq(&self, other: &Self) -> bool {
        if self.vtable().type_id() != other.vtable().type_id() {
            mismatched_push_type_ids(other.vtable().type_name(), self.vtable().type_name());
        } else if self.len() != other.len() {
            return false;
        }

        // TODO: I wonder if this occurs enough to warrant a `slice_eq` function. It's
        // only really beneficial for inner types which consist entirely of
        // scalar values, but in that instance it can consist of a single `memcmp()`
        debug_assert_eq!(self.len(), other.len());
        for idx in 0..self.len() {
            // TODO: `.index_unchecked()`?
            let lhs = self.index(idx);
            let rhs = other.index(idx);

            // Safety: Both vtable's types are the same
            if unsafe { !self.vtable.eq(lhs, rhs) } {
                return false;
            }
        }

        true
    }
}

/// A [`DynVec`]'s vtable
///
/// # Safety
///
/// - [`DynVecVTable::align_of`] must return a non-zero power of two
/// - The type the vtable is associated with must be [`Send`] and [`Sync`]
pub unsafe trait DynVecVTable: Copy + Send + Sync + Sized + 'static {
    fn size_of(&self) -> usize;

    fn align_of(&self) -> NonZeroUsize;

    fn type_id(&self) -> TypeId;

    fn type_name(&self) -> &'static str;

    /// Clones `count` elements from `src` into `dest`
    ///
    /// # Safety
    ///
    /// - `src` and `dest` must be valid pointers aligned to
    ///   [`Self::align_of()`]
    /// - `src` and `dest` must be valid for `count` elements of size
    ///   [`Self::size_of()`] of the current vtable's type
    /// - `src` must have `count` valid elements
    unsafe fn clone_slice(&self, src: *const u8, dest: *mut u8, count: usize);

    /// Drops `count` elements from `ptr` in place
    ///
    /// # Safety
    ///
    /// - `ptr` must be a valid pointer aligned to [`Self::align_of()`]
    /// - `ptr` must be valid for `count` elements of size [`Self::size_of()`]
    ///   of the current vtable's type
    /// - `ptr` must have `count` valid elements
    unsafe fn drop_slice_in_place(&self, ptr: *mut u8, length: usize);

    /// Calls [`SizeOf`] on the given item
    ///
    /// # Safety
    ///
    /// - `ptr` must be a valid pointer to an initialized value of the current
    ///   vtable's type
    unsafe fn size_of_children(&self, ptr: *const u8, context: &mut Context);

    /// Calls [`Debug::fmt`] on the given item
    ///
    /// # Safety
    ///
    /// - `ptr` must be a valid pointer to an initialized value of the current
    ///   vtable's type
    unsafe fn debug(&self, value: *const u8, f: &mut fmt::Formatter<'_>) -> fmt::Result;

    /// Compares the given values for equality
    ///
    /// # Safety
    ///
    /// - `lhs` and `rhs` must be valid pointers to initialized values of the
    ///   current vtable's type
    unsafe fn eq(&self, lhs: *const u8, rhs: *const u8) -> bool;

    fn is_zst(&self) -> bool {
        self.size_of() == 0
    }

    fn dangling(&self) -> NonNull<u8> {
        // Safety: `.align_of()` is a non-zero integer
        unsafe { NonNull::new_unchecked(self.align_of().get() as *mut u8) }
    }
}

pub struct DynIter<'a, VTable: DynVecVTable> {
    ptr: NonNull<u8>,
    // If T is a ZST, this is actually ptr+len. This encoding is picked so that
    // ptr == end is a quick test for the Iterator being empty that works
    // for both ZST and non-ZST types.
    end: *const u8,
    size_of: usize,
    vtable: &'static VTable,
    __marker: PhantomData<&'a ()>,
}

impl<'a, VTable> DynIter<'a, VTable>
where
    VTable: DynVecVTable,
{
    fn new(vec: &'a DynVec<VTable>) -> Self {
        let ptr = vec.ptr;
        let size_of = vec.vtable().size_of();
        let end = if size_of == 0 {
            ptr.as_ptr().wrapping_add(vec.len())
        } else {
            unsafe { ptr.as_ptr().add(vec.len() * size_of) }
        };

        Self {
            ptr,
            end,
            size_of,
            vtable: vec.vtable(),
            __marker: PhantomData,
        }
    }

    fn iter_len(&self) -> usize {
        if self.size_of == 0 {
            (self.end as usize).wrapping_sub(self.ptr.as_ptr() as usize)
        } else {
            (self.end as usize - self.ptr.as_ptr() as usize) / self.size_of
        }
    }

    fn iter_empty(&self) -> bool {
        self.ptr.as_ptr() as *const u8 == self.end
    }

    // Helper function for moving the start of the iterator forwards by `offset`
    // elements, returning the old start.
    //
    // # Safety
    //
    // `offset` must not exceed `self.iter_len()`
    unsafe fn advance_start_by(&mut self, offset: usize) -> *mut u8 {
        debug_assert!(offset <= self.iter_len());

        if self.size_of == 0 {
            self.end = self.end.wrapping_sub(offset);
            self.ptr.as_ptr()
        } else {
            let old = self.ptr.as_ptr();
            // Safety: the caller guarantees that `offset` doesn't exceed `self.len()`,
            // so this new pointer is inside `self` and thus guaranteed to be non-null.
            self.ptr =
                unsafe { NonNull::new_unchecked(self.ptr.as_ptr().add(offset * self.size_of)) };
            old
        }
    }

    // Helper function for moving the end of the iterator backwards by `offset`
    // elements, returning the new end
    //
    // # Safety
    //
    // `offset` must not exceed `self.iter_len()`
    unsafe fn advance_end_by(&mut self, offset: usize) -> *mut u8 {
        debug_assert!(offset <= self.iter_len());

        if self.size_of == 0 {
            self.end = self.end.wrapping_sub(offset);
            self.ptr.as_ptr()
        } else {
            let old = self.ptr.as_ptr();
            // Safety: the caller guarantees that `offset` doesn't exceed `self.len()`,
            // which is guaranteed to not overflow an `isize`. Also, the resulting pointer
            // is in bounds of `slice`, which fulfills the other requirements for `offset`.
            self.ptr =
                unsafe { NonNull::new_unchecked(self.ptr.as_ptr().sub(offset * self.size_of)) };
            old
        }
    }
}

impl<'a, VTable> Iterator for DynIter<'a, VTable>
where
    VTable: DynVecVTable,
{
    type Item = *const u8;

    fn next(&mut self) -> Option<Self::Item> {
        if self.iter_empty() {
            None
        } else {
            // Safety: The iterator is non-empty
            Some(unsafe { self.advance_start_by(1) })
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
        // `n` is greater than the current iterator's length we exhaust the current
        // iterator and return none
        if n >= self.iter_len() {
            if self.vtable.is_zst() {
                self.end = self.ptr.as_ptr() as *const u8;
            } else {
                debug_assert!(!self.end.is_null());
                // Safety: `end` will be a non-null pointer
                self.ptr = unsafe { NonNull::new_unchecked(self.end as *mut u8) };
            }

            None

        // Otherwise we advance `n` times and then return the next element
        } else {
            unsafe {
                self.advance_start_by(n);
                Some(self.advance_start_by(1))
            }
        }
    }

    fn last(mut self) -> Option<Self::Item> {
        self.next_back()
    }
}

impl<'a, VTable> DoubleEndedIterator for DynIter<'a, VTable>
where
    VTable: DynVecVTable,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.iter_empty() {
            None
        } else {
            // Safety: The iterator is non-empty
            Some(unsafe { self.advance_end_by(1) })
        }
    }

    fn nth_back(&mut self, n: usize) -> Option<Self::Item> {
        if n >= self.iter_len() {
            self.end = self.ptr.as_ptr();
            None
        } else {
            unsafe {
                self.advance_end_by(n);
                Some(self.advance_end_by(1))
            }
        }
    }
}

impl<'a, VTable> ExactSizeIterator for DynIter<'a, VTable>
where
    VTable: DynVecVTable,
{
    fn len(&self) -> usize {
        self.iter_len()
    }

    // TODO: We'd like to be able to implement `ExactSizeIterator::is_empty()` but
    // it's currently unstable
}

impl<'a, VTable> FusedIterator for DynIter<'a, VTable> where VTable: DynVecVTable {}

// Ideally we'd implement `TrustedLen` too but that's unstable

#[cfg(test)]
mod tests {
    use crate::{trace::layers::erased::IntoErasedData, utils::DynVec};

    #[test]
    fn smoke() {
        let mut vec = DynVec::new(&<(usize, usize)>::DATA_VTABLE);
        assert_eq!(vec.len(), 0);
        assert_eq!(vec.capacity(), 0);

        vec.push((1usize, 2usize));
        assert_eq!(vec.len(), 1);
        assert!(vec.capacity() > 0);
        assert_eq!(unsafe { *vec.index(0).cast::<(usize, usize)>() }, (1, 2));
        assert_eq!(vec.index_as::<(usize, usize)>(0), &(1, 2));

        vec.push((3usize, 4usize));
        assert_eq!(vec.len(), 2);
        assert!(vec.capacity() > 0);
        assert_eq!(unsafe { *vec.index(0).cast::<(usize, usize)>() }, (1, 2));
        assert_eq!(vec.index_as::<(usize, usize)>(0), &(1, 2));
        assert_eq!(unsafe { *vec.index(1).cast::<(usize, usize)>() }, (3, 4));
        assert_eq!(vec.index_as::<(usize, usize)>(1), &(3, 4));
    }

    // FIXME: `#[should_panic]` isn't working for some reason?
    // #[test]
    // #[should_panic]
    // fn push_incorrect_type() {
    //     let mut vec = DynVec::new(&<(usize, usize)>::DATA_VTABLE);
    //     vec.push(10u32);
    // }
}
