//! A custom buffer type that works with our read/write APIs and the
//! buffer-cache.

// The code in this file is originally derived from the rkyv `AlignedVec`
// type, and adapted for use within the Feldera storage engine.
//
// Original rkyv attribution of the code:
//
// SPDX-FileCopyrightText: Copyright © 2021 David Koloski
//
// SPDX-License-Identifier: MIT

use std::{
    alloc,
    borrow::{Borrow, BorrowMut},
    fmt,
    io::{self, ErrorKind, Read},
    mem,
    ops::{Deref, DerefMut, Index, IndexMut},
    ptr::NonNull,
    slice,
};

use monoio::buf::{IoBuf, IoBufMut};
use rkyv::{
    ser::{ScratchSpace, Serializer},
    vec::ArchivedVec,
    Archive, Archived, Serialize,
};
use rkyv::{vec::VecResolver, ArchiveUnsized, Fallible, Infallible, RelPtr};

/// A custom buffer type that works with our read/write APIs and the
/// buffer-cache.
///
/// # Invariants
/// - `data.as_ptr()` has 512-byte alignment: This is the minimal alignment
///   required for Direct IO.
pub struct FBuf {
    ptr: NonNull<u8>,
    cap: usize,
    len: usize,
}

impl Drop for FBuf {
    #[inline]
    fn drop(&mut self) {
        if self.cap != 0 {
            unsafe {
                alloc::dealloc(self.ptr.as_ptr(), self.layout());
            }
        }
    }
}

impl FBuf {
    /// The alignment of the vector
    pub const ALIGNMENT: usize = 512;

    /// Maximum capacity of the vector.
    /// Dictated by the requirements of
    /// [`alloc::Layout`](https://doc.rust-lang.org/alloc/alloc/struct.Layout.html).
    /// "`size`, when rounded up to the nearest multiple of `align`, must not
    /// overflow `isize` (i.e. the rounded value must be less than or equal
    /// to `isize::MAX`)".
    pub const MAX_CAPACITY: usize = isize::MAX as usize - (Self::ALIGNMENT - 1);

    /// Constructs a new, empty `FBuf`.
    ///
    /// The vector will not allocate until elements are pushed into it.
    #[inline]
    pub fn new() -> Self {
        FBuf {
            ptr: NonNull::dangling(),
            cap: 0,
            len: 0,
        }
    }

    /// Constructs a new, empty `FBuf` with the specified capacity.
    ///
    /// The vector will be able to hold exactly `capacity` bytes without
    /// reallocating. If `capacity` is 0, the vector will not allocate.
    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        if capacity == 0 {
            Self::new()
        } else {
            assert!(
                capacity <= Self::MAX_CAPACITY,
                "`capacity` cannot exceed isize::MAX - 15"
            );
            let ptr = unsafe {
                let layout = alloc::Layout::from_size_align_unchecked(capacity, Self::ALIGNMENT);
                let ptr = alloc::alloc(layout);
                if ptr.is_null() {
                    alloc::handle_alloc_error(layout);
                }
                NonNull::new_unchecked(ptr)
            };
            Self {
                ptr,
                cap: capacity,
                len: 0,
            }
        }
    }

    #[inline]
    fn layout(&self) -> alloc::Layout {
        unsafe { alloc::Layout::from_size_align_unchecked(self.cap, Self::ALIGNMENT) }
    }

    /// Clears the vector, removing all values.
    ///
    /// Note that this method has no effect on the allocated capacity of the
    /// vector.
    #[inline]
    pub fn clear(&mut self) {
        self.len = 0;
    }

    /// Change capacity of vector.
    ///
    /// Will set capacity to exactly `new_cap`.
    /// Can be used to either grow or shrink capacity.
    /// Backing memory will be reallocated.
    ///
    /// Usually the safe methods `reserve` or `reserve_exact` are a better
    /// choice. This method only exists as a micro-optimization for very
    /// performance-sensitive code where the calculation of capacity
    /// required has already been performed, and you want to avoid doing it
    /// again, or if you want to implement a different growth strategy.
    ///
    /// # Safety
    ///
    /// - `new_cap` must be less than or equal to
    ///   [`MAX_CAPACITY`](FBuf::MAX_CAPACITY)
    /// - `new_cap` must be greater than or equal to [`len()`](FBuf::len)
    #[inline]
    pub unsafe fn change_capacity(&mut self, new_cap: usize) {
        debug_assert!(new_cap <= Self::MAX_CAPACITY);
        debug_assert!(new_cap >= self.len);

        if new_cap > 0 {
            let new_ptr = if self.cap > 0 {
                let new_ptr = alloc::realloc(self.ptr.as_ptr(), self.layout(), new_cap);
                if new_ptr.is_null() {
                    alloc::handle_alloc_error(alloc::Layout::from_size_align_unchecked(
                        new_cap,
                        Self::ALIGNMENT,
                    ));
                }
                new_ptr
            } else {
                let layout = alloc::Layout::from_size_align_unchecked(new_cap, Self::ALIGNMENT);
                let new_ptr = alloc::alloc(layout);
                if new_ptr.is_null() {
                    alloc::handle_alloc_error(layout);
                }
                new_ptr
            };
            self.ptr = NonNull::new_unchecked(new_ptr);
            self.cap = new_cap;
        } else if self.cap > 0 {
            alloc::dealloc(self.ptr.as_ptr(), self.layout());
            self.ptr = NonNull::dangling();
            self.cap = 0;
        }
    }

    /// Shrinks the capacity of the vector as much as possible.
    ///
    /// It will drop down as close as possible to the length but the allocator
    /// may still inform the vector that there is space for a few more
    /// elements.
    #[inline]
    pub fn shrink_to_fit(&mut self) {
        if self.cap != self.len {
            // New capacity cannot exceed max as it's shrinking
            unsafe { self.change_capacity(self.len) };
        }
    }

    /// Returns an unsafe mutable pointer to the vector's buffer.
    ///
    /// The caller must ensure that the vector outlives the pointer this
    /// function returns, or else it will end up pointing to garbage.
    /// Modifying the vector may cause its buffer to be reallocated, which
    /// would also make any pointers to it invalid.
    #[inline]
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    /// Extracts a mutable slice of the entire vector.
    ///
    /// Equivalent to `&mut s[..]`.
    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }

    /// Returns a raw pointer to the vector's buffer.
    ///
    /// The caller must ensure that the vector outlives the pointer this
    /// function returns, or else it will end up pointing to garbage.
    /// Modifying the vector may cause its buffer to be reallocated, which
    /// would also make any pointers to it invalid.
    ///
    /// The caller must also ensure that the memory the pointer
    /// (non-transitively) points to is never written to (except inside an
    /// `UnsafeCell`) using this pointer or any pointer derived from it. If
    /// you need to mutate the contents of the slice, use
    /// [`as_mut_ptr`](FBuf::as_mut_ptr).
    #[inline]
    pub fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }

    /// Extracts a slice containing the entire vector.
    ///
    /// Equivalent to `&s[..]`.
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }

    /// Returns the number of elements the vector can hold without reallocating.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.cap
    }

    /// Reserves capacity for at least `additional` more bytes to be inserted
    /// into the given `FBuf`. The collection may reserve more space
    /// to avoid frequent reallocations. After calling `reserve`, capacity
    /// will be greater than or equal to `self.len() + additional`. Does
    /// nothing if capacity is already sufficient.
    ///
    /// # Panics
    ///
    /// Panics if the new capacity exceeds `isize::MAX - 15` bytes.
    #[inline]
    pub fn reserve(&mut self, additional: usize) {
        // Cannot wrap because capacity always exceeds len,
        // but avoids having to handle potential overflow here
        let remaining = self.cap.wrapping_sub(self.len);
        if additional > remaining {
            self.do_reserve(additional);
        }
    }

    /// Extend capacity after `reserve` has found it's necessary.
    ///
    /// Actually performing the extension is in this separate function marked
    /// `#[cold]` to hint to compiler that this branch is not often taken.
    /// This keeps the path for common case where capacity is already sufficient
    /// as fast as possible, and makes `reserve` more likely to be inlined.
    /// This is the same trick that Rust's `Vec::reserve` uses.
    #[cold]
    fn do_reserve(&mut self, additional: usize) {
        let new_cap = self
            .len
            .checked_add(additional)
            .expect("cannot reserve a larger FBuf");
        unsafe { self.grow_capacity_to(new_cap) };
    }

    /// Grows total capacity of vector to `new_cap` or more.
    ///
    /// Capacity after this call will be `new_cap` rounded up to next power of
    /// 2, unless that would exceed maximum capacity, in which case capacity
    /// is capped at the maximum.
    ///
    /// This is same growth strategy used by `reserve`, `push` and
    /// `extend_from_slice`.
    ///
    /// Usually the safe methods `reserve` or `reserve_exact` are a better
    /// choice. This method only exists as a micro-optimization for very
    /// performance-sensitive code where where the calculation of capacity
    /// required has already been performed, and you want to avoid doing it
    /// again.
    ///
    /// Maximum capacity is `isize::MAX - 15` bytes.
    ///
    /// # Panics
    ///
    /// Panics if `new_cap` exceeds `isize::MAX - 15` bytes.
    ///
    /// # Safety
    ///
    /// - `new_cap` must be greater than current [`capacity()`](FBuf::capacity)
    #[inline]
    pub unsafe fn grow_capacity_to(&mut self, new_cap: usize) {
        debug_assert!(new_cap > self.cap);

        let new_cap = if new_cap > (isize::MAX as usize + 1) >> 1 {
            // Rounding up to next power of 2 would result in `isize::MAX + 1` or higher,
            // which exceeds max capacity. So cap at max instead.
            assert!(
                new_cap <= Self::MAX_CAPACITY,
                "cannot reserve a larger FBuf"
            );
            Self::MAX_CAPACITY
        } else {
            // Cannot overflow due to check above
            new_cap.next_power_of_two()
        };
        self.change_capacity(new_cap);
    }

    /// Resizes the Vec in-place so that len is equal to new_len.
    ///
    /// If new_len is greater than len, the Vec is extended by the difference,
    /// with each additional slot filled with value. If new_len is less than
    /// len, the Vec is simply truncated.
    ///
    /// # Panics
    ///
    /// Panics if the new length exceeds `isize::MAX - 15` bytes.
    #[inline]
    pub fn resize(&mut self, new_len: usize, value: u8) {
        if new_len > self.len {
            let additional = new_len - self.len;
            self.reserve(additional);
            unsafe {
                core::ptr::write_bytes(self.ptr.as_ptr().add(self.len), value, additional);
            }
        }
        unsafe {
            self.set_len(new_len);
        }
    }

    /// Returns `true` if the vector contains no elements.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the number of elements in the vector, also referred to as its
    /// 'length'.
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Copies and appends all bytes in a slice to the `FBuf`.
    ///
    /// The elements of the slice are appended in-order.
    #[inline]
    pub fn extend_from_slice(&mut self, other: &[u8]) {
        if !other.is_empty() {
            self.reserve(other.len());
            unsafe {
                core::ptr::copy_nonoverlapping(
                    other.as_ptr(),
                    self.as_mut_ptr().add(self.len()),
                    other.len(),
                );
            }
            self.len += other.len();
        }
    }

    /// Removes the last element from a vector and returns it, or `None` if it
    /// is empty.
    #[inline]
    pub fn pop(&mut self) -> Option<u8> {
        if self.len == 0 {
            None
        } else {
            let result = self[self.len - 1];
            self.len -= 1;
            Some(result)
        }
    }

    /// Appends an element to the back of a collection.
    ///
    /// # Panics
    ///
    /// Panics if the new capacity exceeds `isize::MAX - 15` bytes.
    #[inline]
    pub fn push(&mut self, value: u8) {
        if self.len == self.cap {
            self.reserve_for_push();
        }

        unsafe {
            self.as_mut_ptr().add(self.len).write(value);
            self.len += 1;
        }
    }

    /// Extend capacity by at least 1 byte after `push` has found it's
    /// necessary.
    ///
    /// Actually performing the extension is in this separate function marked
    /// `#[cold]` to hint to compiler that this branch is not often taken.
    /// This keeps the path for common case where capacity is already sufficient
    /// as fast as possible, and makes `push` more likely to be inlined.
    /// This is the same trick that Rust's `Vec::push` uses.
    #[cold]
    fn reserve_for_push(&mut self) {
        // `len` is always less than `isize::MAX`, so no possibility of overflow here
        let new_cap = self.len + 1;
        unsafe { self.grow_capacity_to(new_cap) };
    }

    /// Reserves the minimum capacity for exactly `additional` more elements to
    /// be inserted in the given `FBuf`. After calling
    /// `reserve_exact`, capacity will be greater than or equal
    /// to `self.len() + additional`. Does nothing if the capacity is already
    /// sufficient.
    ///
    /// Note that the allocator may give the collection more space than it
    /// requests. Therefore, capacity can not be relied upon to be precisely
    /// minimal. Prefer reserve if future insertions are expected.
    ///
    /// # Panics
    ///
    /// Panics if the new capacity overflows `isize::MAX - 15`.
    #[inline]
    pub fn reserve_exact(&mut self, additional: usize) {
        // This function does not use the hot/cold paths trick that `reserve`
        // and `push` do, on assumption that user probably knows this will require
        // an increase in capacity. Otherwise, they'd likely use `reserve`.
        let new_cap = self
            .len
            .checked_add(additional)
            .expect("cannot reserve a larger FBuf");
        if new_cap > self.cap {
            assert!(
                new_cap <= Self::MAX_CAPACITY,
                "cannot reserve a larger FBuf"
            );
            unsafe { self.change_capacity(new_cap) };
        }
    }

    /// Forces the length of the vector to `new_len`.
    ///
    /// This is a low-level operation that maintains none of the normal
    /// invariants of the type.
    ///
    /// # Safety
    ///
    /// - `new_len` must be less than or equal to [`capacity()`](FBuf::capacity)
    /// - The elements at `old_len..new_len` must be initialized
    #[inline]
    pub unsafe fn set_len(&mut self, new_len: usize) {
        debug_assert!(new_len <= self.capacity());

        self.len = new_len;
    }

    /// Converts the vector into `Box<[u8]>`.
    ///
    /// This method reallocates and copies the underlying bytes. Any excess
    /// capacity is dropped.
    #[inline]
    pub fn into_boxed_slice(self) -> Box<[u8]> {
        self.into_vec().into_boxed_slice()
    }

    /// Converts the vector into `Vec<u8>`.
    ///
    /// This method reallocates and copies the underlying bytes. Any excess
    /// capacity is dropped.
    #[inline]
    pub fn into_vec(self) -> Vec<u8> {
        Vec::from(self.as_ref())
    }

    /// Reads all bytes until EOF from `r` and appends them to this
    /// `FBuf`.
    ///
    /// If successful, this function will return the total number of bytes
    /// read.
    pub fn extend_from_reader<R: Read + ?Sized>(&mut self, r: &mut R) -> io::Result<usize> {
        let start_len = self.len();
        let start_cap = self.capacity();

        // Extra initialized bytes from previous loop iteration.
        let mut initialized = 0;
        loop {
            if self.len() == self.capacity() {
                // No available capacity, reserve some space.
                self.reserve(32);
            }

            let read_buf_start = unsafe { self.as_mut_ptr().add(self.len) };
            let read_buf_len = self.capacity() - self.len();

            // Initialize the uninitialized portion of the available space.
            unsafe {
                // The first `initialized` bytes don't need to be zeroed.
                // This leaves us `read_buf_len - initialized` bytes to zero
                // starting at `initialized`.
                core::ptr::write_bytes(
                    read_buf_start.add(initialized),
                    0,
                    read_buf_len - initialized,
                );
            }

            // The entire read buffer is now initialized, so we can create a
            // mutable slice of it.
            let read_buf = unsafe { core::slice::from_raw_parts_mut(read_buf_start, read_buf_len) };

            match r.read(read_buf) {
                Ok(read) => {
                    // We filled `read` additional bytes.
                    unsafe {
                        self.set_len(self.len() + read);
                    }
                    initialized = read_buf_len - read;

                    if read == 0 {
                        return Ok(self.len() - start_len);
                    }
                }
                Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            }

            if self.len() == self.capacity() && self.capacity() == start_cap {
                // The buffer might be an exact fit. Let's read into a probe buffer
                // and see if it returns `Ok(0)`. If so, we've avoided an
                // unnecessary doubling of the capacity. But if not, append the
                // probe buffer to the primary buffer and let its capacity grow.
                let mut probe = [0u8; 32];

                loop {
                    match r.read(&mut probe) {
                        Ok(0) => return Ok(self.len() - start_len),
                        Ok(n) => {
                            self.extend_from_slice(&probe[..n]);
                            break;
                        }
                        Err(ref e) if e.kind() == ErrorKind::Interrupted => continue,
                        Err(e) => return Err(e),
                    }
                }
            }
        }
    }
}

impl From<FBuf> for Vec<u8> {
    #[inline]
    fn from(aligned: FBuf) -> Self {
        aligned.to_vec()
    }
}

impl AsMut<[u8]> for FBuf {
    #[inline]
    fn as_mut(&mut self) -> &mut [u8] {
        self.as_mut_slice()
    }
}

impl AsRef<[u8]> for FBuf {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl Borrow<[u8]> for FBuf {
    #[inline]
    fn borrow(&self) -> &[u8] {
        self.as_slice()
    }
}

impl BorrowMut<[u8]> for FBuf {
    #[inline]
    fn borrow_mut(&mut self) -> &mut [u8] {
        self.as_mut_slice()
    }
}

impl Clone for FBuf {
    #[inline]
    fn clone(&self) -> Self {
        unsafe {
            let mut result = FBuf::with_capacity(self.len);
            result.len = self.len;
            core::ptr::copy_nonoverlapping(self.as_ptr(), result.as_mut_ptr(), self.len);
            result
        }
    }
}

impl fmt::Debug for FBuf {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_slice().fmt(f)
    }
}

impl Default for FBuf {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl Deref for FBuf {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl DerefMut for FBuf {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut_slice()
    }
}

impl<I: slice::SliceIndex<[u8]>> Index<I> for FBuf {
    type Output = <I as slice::SliceIndex<[u8]>>::Output;

    #[inline]
    fn index(&self, index: I) -> &Self::Output {
        &self.as_slice()[index]
    }
}

impl<I: slice::SliceIndex<[u8]>> IndexMut<I> for FBuf {
    #[inline]
    fn index_mut(&mut self, index: I) -> &mut Self::Output {
        &mut self.as_mut_slice()[index]
    }
}

impl io::Write for FBuf {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.extend_from_slice(buf);
        Ok(buf.len())
    }

    #[inline]
    fn write_vectored(&mut self, bufs: &[io::IoSlice<'_>]) -> io::Result<usize> {
        let len = bufs.iter().map(|b| b.len()).sum();
        self.reserve(len);
        for buf in bufs {
            self.extend_from_slice(buf);
        }
        Ok(len)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }

    #[inline]
    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.extend_from_slice(buf);
        Ok(())
    }
}

unsafe impl IoBufMut for FBuf {
    fn write_ptr(&mut self) -> *mut u8 {
        self.as_mut_ptr()
    }

    fn bytes_total(&mut self) -> usize {
        self.capacity()
    }

    unsafe fn set_init(&mut self, init_len: usize) {
        self.set_len(init_len);
    }
}

unsafe impl IoBuf for FBuf {
    fn read_ptr(&self) -> *const u8 {
        self.as_ptr()
    }

    fn bytes_init(&self) -> usize {
        self.len()
    }
}

#[cfg(test)]
impl Eq for FBuf {}

#[cfg(test)]
impl PartialEq<Self> for FBuf {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

#[cfg(test)]
impl PartialOrd for FBuf {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.as_slice().partial_cmp(other.as_slice())
    }
}

impl Archive for FBuf {
    type Archived = ArchivedVec<u8>;
    type Resolver = VecResolver;

    #[inline]
    unsafe fn resolve(&self, pos: usize, resolver: Self::Resolver, out: *mut Self::Archived) {
        ArchivedVec::resolve_from_slice(self.as_slice(), pos, resolver, out);
    }
}

impl<S: ScratchSpace + Serializer + ?Sized> Serialize<S> for FBuf {
    #[inline]
    fn serialize(&self, serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        serializer.align(Self::ALIGNMENT)?;
        ArchivedVec::<Archived<u8>>::serialize_from_slice(self.as_slice(), serializer)
    }
}

// SAFETY: FBuf is safe to send to another thread
unsafe impl Send for FBuf {}

// SAFETY: FBuf is safe to share between threads
unsafe impl Sync for FBuf {}

impl Unpin for FBuf {}

/// An [`rkyv`] serializer made specifically to work with [`FBuf`].
///
/// This serializer makes it easier for the compiler to perform emplacement
/// optimizations and may give better performance than a basic
/// `WriteSerializer`.
#[derive(Debug)]
pub struct FBufSerializer<A> {
    inner: A,
}

impl<A: Borrow<FBuf>> FBufSerializer<A> {
    /// Creates a new `FBufSerializer` by wrapping a `Borrow<FBuf>`.
    #[inline]
    pub fn new(inner: A) -> Self {
        Self { inner }
    }

    /// Consumes the serializer and returns the underlying type.
    #[inline]
    pub fn into_inner(self) -> A {
        self.inner
    }
}

impl<A: Default> Default for FBufSerializer<A> {
    #[inline]
    fn default() -> Self {
        Self {
            inner: A::default(),
        }
    }
}

impl<A> Fallible for FBufSerializer<A> {
    type Error = Infallible;
}

impl<A: Borrow<FBuf> + BorrowMut<FBuf>> Serializer for FBufSerializer<A> {
    #[inline]
    fn pos(&self) -> usize {
        self.inner.borrow().len()
    }

    #[inline]
    fn write(&mut self, bytes: &[u8]) -> Result<(), Self::Error> {
        self.inner.borrow_mut().extend_from_slice(bytes);
        Ok(())
    }

    #[inline]
    unsafe fn resolve_aligned<T: Archive + ?Sized>(
        &mut self,
        value: &T,
        resolver: T::Resolver,
    ) -> Result<usize, Self::Error> {
        let pos = self.pos();
        debug_assert_eq!(pos & (mem::align_of::<T::Archived>() - 1), 0);
        let vec = self.inner.borrow_mut();
        let additional = mem::size_of::<T::Archived>();
        vec.reserve(additional);
        vec.set_len(vec.len() + additional);

        let ptr = vec.as_mut_ptr().add(pos).cast::<T::Archived>();
        ptr.write_bytes(0, 1);
        value.resolve(pos, resolver, ptr);

        Ok(pos)
    }

    #[inline]
    unsafe fn resolve_unsized_aligned<T: ArchiveUnsized + ?Sized>(
        &mut self,
        value: &T,
        to: usize,
        metadata_resolver: T::MetadataResolver,
    ) -> Result<usize, Self::Error> {
        let from = self.pos();
        debug_assert_eq!(from & (mem::align_of::<RelPtr<T::Archived>>() - 1), 0);
        let vec = self.inner.borrow_mut();
        let additional = mem::size_of::<RelPtr<T::Archived>>();
        vec.reserve(additional);
        vec.set_len(vec.len() + additional);

        let ptr = vec.as_mut_ptr().add(from).cast::<RelPtr<T::Archived>>();
        ptr.write_bytes(0, 1);

        value.resolve_unsized(from, to, metadata_resolver, ptr);
        Ok(from)
    }
}
