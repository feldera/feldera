use size_of::{Context, SizeOf};
use std::{
    alloc::Layout,
    cmp::{max, Ordering},
    fmt::{self, Debug, Display},
    hash::{Hash, Hasher},
    marker::PhantomData,
    mem::{align_of, size_of, ManuallyDrop},
    ops::{Deref, DerefMut},
    ptr::{self, addr_of, addr_of_mut, NonNull},
    slice,
    str::{self, Utf8Error},
};

// Ensure that `ThinStr` and `ThinStrRef` have layouts identical to a pointer
const _: () = assert!(
    size_of::<ThinStr>() == size_of::<*const u8>()
        && align_of::<ThinStr>() == align_of::<*const u8>()
        && size_of::<ThinStr>() == size_of::<ThinStrRef<'_>>()
        && align_of::<ThinStr>() == align_of::<ThinStrRef<'_>>(),
);

static EMPTY: StrHeader = StrHeader {
    length: 0,
    capacity: 0,
    _data: [],
};

#[repr(C)]
struct StrHeader {
    length: usize,
    capacity: usize,
    _data: [u8; 0],
}

#[repr(transparent)]
pub struct ThinStr {
    buf: NonNull<StrHeader>,
}

impl ThinStr {
    #[inline]
    pub fn new() -> Self {
        Self {
            buf: NonNull::from(&EMPTY),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        unsafe { (*self.buf.as_ptr()).length }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        unsafe { (*self.buf.as_ptr()).capacity }
    }

    #[inline]
    pub fn as_ptr(&self) -> *const u8 {
        unsafe { addr_of!((*self.buf.as_ptr())._data).cast() }
    }

    #[inline]
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        unsafe { addr_of_mut!((*self.buf.as_ptr())._data).cast() }
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        // Safety: All bytes up to self.len() are valid
        unsafe { slice::from_raw_parts(self.as_ptr(), self.len()) }
    }

    #[inline]
    pub fn as_str(&self) -> &str {
        unsafe { str::from_utf8_unchecked(self.as_bytes()) }
    }

    #[inline]
    pub fn as_mut_bytes(&mut self) -> &mut [u8] {
        // Safety: All bytes up to self.len() are valid
        unsafe { slice::from_raw_parts_mut(self.as_mut_ptr(), self.len()) }
    }

    #[inline]
    pub fn as_mut_str(&mut self) -> &mut str {
        unsafe { str::from_utf8_unchecked_mut(self.as_mut_bytes()) }
    }

    /// Sets the length of the current `ThinStr`
    ///
    /// # Safety
    ///
    /// - `length` must be less than or equal to [`ThinStr::capacity()`]
    /// - The elements at `self.len()..length` must be initialized and valid
    ///   UTF-8
    /// - The current string cannot be the sigil string ([`ThinStr::is_sigil()`]
    ///   must return `false`)
    #[inline]
    unsafe fn set_len(&mut self, length: usize) {
        debug_assert!(!self.is_sigil());
        debug_assert!(length <= self.capacity());
        unsafe { addr_of_mut!((*self.buf.as_ptr()).length).write(length) }
    }

    /// Sets the capacity of the current `ThinStr`
    ///
    /// # Safety
    ///
    /// - The current string must have allocated data
    /// - The current string cannot be the sigil string ([`ThinStr::is_sigil()`]
    ///   must return `false`)
    #[inline]
    unsafe fn set_capacity(&mut self, capacity: usize) {
        debug_assert!(!self.is_sigil());
        unsafe { addr_of_mut!((*self.buf.as_ptr()).capacity).write(capacity) }
    }

    #[inline]
    pub fn into_raw(self) -> *mut () {
        let this = ManuallyDrop::new(self);
        this.buf.as_ptr().cast()
    }

    /// Constructs a `ThinStr` from a raw pointer
    ///
    /// After calling this function, the raw pointer is owned by the resulting
    /// `ThinStr`. Specifically, the `ThinStr` destructor will run and free
    /// the allocated memory. For this to be safe, the memory must have been
    /// allocated in accordance with the memory layout used by `ThinStr`.
    ///
    /// # Safety
    ///
    /// The pointer passed must have come from [`ThinStr::into_raw()`]
    #[inline]
    pub unsafe fn from_raw(raw: *mut ()) -> Self {
        debug_assert!(
            !raw.is_null(),
            "Cannot call `ThinStr::from_raw()` on a null pointer",
        );

        Self {
            buf: NonNull::new_unchecked(raw.cast()),
        }
    }

    #[inline]
    pub const fn as_thin_ref(&self) -> ThinStrRef<'_> {
        ThinStrRef {
            buf: self.buf,
            __lifetime: PhantomData,
        }
    }

    #[inline]
    fn from_str(string: &str) -> Self {
        if string.is_empty() {
            return Self::new();
        }

        unsafe {
            let length = string.len();
            let mut this = Self::with_capacity_uninit(length, length);
            ptr::copy_nonoverlapping(string.as_ptr(), this.as_mut_ptr(), length);
            this
        }
    }

    /// Allocates a `ThinStr` with capacity for `capacity` string bytes
    /// and writes `length` and `capacity` to the header
    ///
    /// # Safety
    ///
    /// This leaves all of the allocated capacity uninitialized, so all
    /// elements up to `length` must be initialized after calling this
    unsafe fn with_capacity_uninit(capacity: usize, length: usize) -> Self {
        let layout = Self::layout_for(capacity);

        debug_assert_ne!(layout.size(), 0);
        let ptr = unsafe { std::alloc::alloc(layout) };
        let buf = match NonNull::new(ptr.cast::<StrHeader>()) {
            Some(buf) => buf,
            None => std::alloc::handle_alloc_error(layout),
        };

        unsafe {
            addr_of_mut!((*buf.as_ptr()).length).write(length);
            addr_of_mut!((*buf.as_ptr()).capacity).write(capacity);
        }

        Self { buf }
    }

    #[inline]
    fn layout_for(capacity: usize) -> Layout {
        #[cold]
        #[inline(never)]
        fn failed_thinstr_layout(capacity: usize) -> ! {
            panic!("failed to create ThinStr layout for {capacity} bytes")
        }

        let header = Layout::new::<StrHeader>();

        let bytes = Layout::from_size_align(capacity, 16)
            .unwrap_or_else(|_| failed_thinstr_layout(capacity));

        let (layout, _) = header
            .extend(bytes)
            .unwrap_or_else(|_| failed_thinstr_layout(capacity));

        // Pad out the layout
        layout.pad_to_align()
    }

    /// Create a layout for the given capacity without checking that it's valid
    ///
    /// # Safety
    ///
    /// The entire `ThinStr`'s layout must obey the preconditions of
    /// [`Layout::from_size_align`]
    #[inline]
    unsafe fn layout_for_unchecked(capacity: usize) -> Layout {
        let header = Layout::new::<StrHeader>();

        let align = align_of::<usize>();
        debug_assert!(Layout::from_size_align(capacity, align).is_ok());
        let bytes = Layout::from_size_align_unchecked(capacity, 16);
        let (layout, _) = header.extend(bytes).unwrap_unchecked();

        // Pad out the layout
        layout.pad_to_align()
    }

    fn grow(&mut self, additional: usize) {
        debug_assert!(additional > 0);

        #[cold]
        #[inline(never)]
        fn grow_thinstr_overflow(current: usize, additional: usize) -> ! {
            panic!("attempted to grow a ThinStr with capacity {current} by {additional}, but {current} + {additional} overflows a usize")
        }

        let current = self.capacity();

        // The minimum possible capacity we have to allocate
        let minimum = current
            .checked_add(additional)
            .unwrap_or_else(|| grow_thinstr_overflow(current, additional));

        // Choose the largest possible capacity to ensure exponential growth, either
        // growing the current capacity by 1.5 times or choosing our the minimum
        // required capacity.
        // Why 1.5x?
        // - https://github.com/facebook/folly/blob/main/folly/docs/FBVector.md
        // - https://stackoverflow.com/questions/1100311/what-is-the-ideal-growth-rate-for-a-dynamically-allocated-array
        //
        //  `x + (x >> 1)` is equivalent to `x * 1.5`
        let capacity = max(
            current + (current >> 1),
            max(minimum, 64 - (size_of::<usize>() * 2)),
        );
        // We align our strings to 16 bytes, so we can always take advantage of that
        // "extra" capacity we'll allocate
        let capacity = next_multiple_of(capacity, 16);

        // For sigil values, allocate
        if self.is_sigil() {
            debug_assert_eq!(self.capacity(), 0);
            unsafe { *self = Self::with_capacity_uninit(additional, 0) };

        // Otherwise realloc
        } else {
            debug_assert!(!self.is_sigil());

            let current_layout = Self::layout_for(self.capacity());
            let new_layout = Self::layout_for(capacity);

            let ptr = unsafe {
                std::alloc::realloc(self.buf.as_ptr().cast(), current_layout, new_layout.size())
            };
            let buf = match NonNull::new(ptr.cast::<StrHeader>()) {
                Some(buf) => buf,
                None => std::alloc::handle_alloc_error(new_layout),
            };

            self.buf = buf;
            unsafe { self.set_capacity(capacity) };
        }
    }

    fn grow_exact(&mut self, capacity: usize) {
        debug_assert!(capacity > 0);

        // For sigil values, allocate
        if self.is_sigil() {
            debug_assert_eq!(self.capacity(), 0);
            unsafe { *self = Self::with_capacity_uninit(capacity, 0) };

        // Otherwise realloc
        } else {
            debug_assert!(!self.is_sigil());

            // Safety: The current layout was created in order to
            let current_layout = unsafe { Self::layout_for_unchecked(self.capacity()) };
            let new_layout = Self::layout_for(capacity);

            let ptr = unsafe {
                std::alloc::realloc(self.buf.as_ptr().cast(), current_layout, new_layout.size())
            };
            let buf = match NonNull::new(ptr.cast::<StrHeader>()) {
                Some(buf) => buf,
                None => std::alloc::handle_alloc_error(new_layout),
            };

            self.buf = buf;
            unsafe { self.set_capacity(capacity) };
        }
    }

    pub fn push(&mut self, char: char) {
        let mut buf = [0; 4];
        let char_str = char.encode_utf8(&mut buf);
        self.push_str(char_str);
    }

    pub fn push_str(&mut self, string: &str) {
        let len = self.len();
        let string_len = string.len();
        let remaining = self.capacity() - len;

        if string_len > remaining {
            self.grow(string_len - remaining);
        }

        unsafe {
            debug_assert!(!self.is_sigil());
            ptr::copy_nonoverlapping(string.as_ptr(), self.as_mut_ptr().add(len), string_len);
        }
    }

    pub fn shrink_to_fit(&mut self) {
        if self.capacity() > self.len() {
            debug_assert!(!self.is_sigil());

            let current_layout = Self::layout_for(self.capacity());
            let new_layout = Self::layout_for(self.len());

            unsafe {
                let ptr = std::alloc::realloc(
                    self.buf.as_ptr().cast(),
                    current_layout,
                    new_layout.size(),
                );
                let buf = match NonNull::new(ptr.cast::<StrHeader>()) {
                    Some(buf) => buf,
                    None => std::alloc::handle_alloc_error(new_layout),
                };

                self.buf = buf;
                self.set_capacity(self.len());
            }
        }
    }

    #[inline]
    pub fn from_utf8(bytes: &[u8]) -> Result<Self, Utf8Error> {
        std::str::from_utf8(bytes).map(Self::from_str)
    }

    /// Creates a `ThinStr` from a slice of bytes without checking that the
    /// string contains valid UTF-8
    ///
    /// # Safety
    ///
    /// - `bytes` must contain valid UTF-8
    #[inline]
    pub unsafe fn from_utf8_unchecked(bytes: &[u8]) -> Self {
        debug_assert!(std::str::from_utf8(bytes).is_ok());
        Self::from_str(std::str::from_utf8_unchecked(bytes))
    }

    /// Returns `true` if the current `ThinStr` points to the sigil empty string
    #[inline]
    fn is_sigil(&self) -> bool {
        self.buf == NonNull::from(&EMPTY)
    }
}

impl Default for ThinStr {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for ThinStr {
    #[inline]
    fn clone(&self) -> Self {
        let string = self.as_str();
        if string.is_empty() {
            return ThinStr::new();
        }

        let length = string.len();
        // Safety: The layout was created in order for `self` to exist, so it's valid
        let layout = unsafe { ThinStr::layout_for_unchecked(length) };

        debug_assert_ne!(layout.size(), 0);
        let ptr = unsafe { std::alloc::alloc(layout) };
        let buf = match NonNull::new(ptr.cast::<StrHeader>()) {
            Some(buf) => buf,
            None => std::alloc::handle_alloc_error(layout),
        };

        // Safety: buf is a valid allocation
        unsafe {
            // Write the string's length
            addr_of_mut!((*buf.as_ptr()).length).write(length);
            // Write the allocated capacity
            addr_of_mut!((*buf.as_ptr()).capacity).write(length);

            // Copy over the string's bytes
            ptr::copy_nonoverlapping(
                string.as_ptr(),
                addr_of_mut!((*buf.as_ptr())._data).cast(),
                length,
            );
        }

        Self { buf }
    }

    fn clone_from(&mut self, source: &Self) {
        // If self is sigil, we can't reallocate it
        if self.is_sigil() {
            // If self is sigil and source isn't we can just directly clone source
            if !source.is_sigil() {
                *self = source.clone();
            }
            // Otherwise if both self and source are sigil, we're done

            return;
        }

        // If source is empty we can set self's length to zero
        if source.is_empty() {
            // Safety: 0 is always <= capacity and self isn't sigil
            unsafe { self.set_len(0) };
            return;
        }

        // If the current string's capacity is insufficent, grow it
        if self.capacity() < source.len() {
            self.grow_exact(source.len());
        }

        // Copy over the bytes from source to self
        // Safety: self has sufficient capacity to store all bytes from source
        unsafe {
            let length = source.len();
            debug_assert!(length <= self.capacity());

            ptr::copy_nonoverlapping(source.as_ptr(), self.as_mut_ptr(), length);
            self.set_len(length);
        }
    }
}

impl Drop for ThinStr {
    fn drop(&mut self) {
        if !self.is_sigil() {
            // Safety: The current layout is valid since we must have created it in order to
            // make the current `ThinStr` and we're deallocating a valid allocation
            unsafe {
                let layout = Self::layout_for_unchecked(self.capacity());
                std::alloc::dealloc(self.buf.as_ptr().cast(), layout);
            }
        }
    }
}

impl Debug for ThinStr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Debug::fmt(self.as_str(), f)
    }
}

impl Display for ThinStr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Hash for ThinStr {
    #[inline]
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.as_thin_ref().hash(state);
    }
}

impl PartialEq for ThinStr {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.buf == other.buf || self.as_str().eq(other.as_str())
    }
}

impl Eq for ThinStr {}

impl PartialOrd for ThinStr {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.as_str().partial_cmp(other.as_str())
    }
}

impl Ord for ThinStr {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_str().cmp(other.as_str())
    }
}

impl Deref for ThinStr {
    type Target = str;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl DerefMut for ThinStr {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut_str()
    }
}

impl From<&str> for ThinStr {
    #[inline]
    fn from(string: &str) -> Self {
        Self::from_str(string)
    }
}

impl SizeOf for ThinStr {
    fn size_of_children(&self, context: &mut Context) {
        self.as_thin_ref().owned_size_of_children(context);
    }
}

#[derive(Clone, Copy)]
#[repr(transparent)]
pub struct ThinStrRef<'a> {
    buf: NonNull<StrHeader>,
    __lifetime: PhantomData<&'a ()>,
}

impl<'a> ThinStrRef<'a> {
    #[inline]
    pub fn len(&self) -> usize {
        unsafe { (*self.buf.as_ptr()).length }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        unsafe { (*self.buf.as_ptr()).capacity }
    }

    #[inline]
    pub fn as_ptr(&self) -> *const u8 {
        unsafe { addr_of!((*self.buf.as_ptr())._data).cast() }
    }

    #[inline]
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        unsafe { addr_of_mut!((*self.buf.as_ptr())._data).cast() }
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        // Safety: All bytes up to self.len() are valid
        unsafe { slice::from_raw_parts(self.as_ptr(), self.len()) }
    }

    #[inline]
    pub fn as_str(&self) -> &str {
        unsafe { str::from_utf8_unchecked(self.as_bytes()) }
    }

    #[inline]
    pub fn to_owned(self) -> ThinStr {
        ThinStr::from_str(self.as_str())
    }

    #[inline]
    fn is_sigil(&self) -> bool {
        self.buf == NonNull::from(&EMPTY)
    }

    /// Returns [`SizeOf::size_of_children()`] as if this was an owned
    /// [`ThinStr`]
    pub(crate) fn owned_size_of_children(&self, context: &mut Context) {
        if !self.is_sigil() {
            context
                .add_distinct_allocation()
                .add(size_of::<StrHeader>())
                .add_vectorlike(self.len(), self.capacity(), 1);
        }
    }
}

impl Deref for ThinStrRef<'_> {
    type Target = str;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl Debug for ThinStrRef<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Debug::fmt(self.as_str(), f)
    }
}

impl Display for ThinStrRef<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Hash for ThinStrRef<'_> {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        // TODO: `Hasher::write_str()` via rust/#96762
        // state.write_str(self.as_str());
        state.write(self.as_bytes());
        state.write_u8(0xff);
    }
}

impl PartialEq for ThinStrRef<'_> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.buf == other.buf || self.as_str().eq(other.as_str())
    }
}

impl Eq for ThinStrRef<'_> {}

impl PartialOrd for ThinStrRef<'_> {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.as_str().partial_cmp(other.as_str())
    }
}

impl Ord for ThinStrRef<'_> {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_str().cmp(other.as_str())
    }
}

// FIXME: Replace with `usize::next_multiple_of()`
#[inline]
const fn next_multiple_of(lhs: usize, rhs: usize) -> usize {
    match lhs % rhs {
        0 => lhs,
        rem => lhs + (rhs - rem),
    }
}
