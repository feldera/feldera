use size_of::{Context, SizeOf};
use std::{
    alloc::Layout,
    cmp::{max, Ordering},
    fmt::{self, Debug, Display},
    marker::PhantomData,
    mem::{align_of, size_of, ManuallyDrop},
    ops::{Deref, DerefMut},
    ptr::{self, addr_of, addr_of_mut, NonNull},
    slice,
    str::{self, Utf8Error},
};

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

    #[inline]
    pub unsafe fn set_len(&mut self, length: usize) {
        debug_assert!(!self.is_sigil());
        unsafe { addr_of_mut!((*self.buf.as_ptr()).length).write(length) }
    }

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

    fn layout_for(capacity: usize) -> Layout {
        let header = Layout::new::<StrHeader>();

        let align = align_of::<usize>();
        let bytes = Layout::from_size_align(capacity, align)
            .expect("failed to create layout for string bytes");

        let (layout, _) = header
            .extend(bytes)
            .expect("failed to add header and string bytes layouts");

        // Pad out the layout
        layout.pad_to_align()
    }

    fn grow(&mut self, additional: usize) {
        debug_assert!(additional > 0);

        let current_cap = self.capacity();
        let capacity = current_cap.checked_add(additional).unwrap();
        let capacity = max(max(current_cap * 2, capacity), 16);

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

    #[inline]
    pub unsafe fn from_utf8_unchecked(bytes: &[u8]) -> Self {
        debug_assert!(std::str::from_utf8(bytes).is_ok());
        Self::from_str(std::str::from_utf8_unchecked(bytes))
    }

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
        Self::from_str(self.as_str())
    }

    fn clone_from(&mut self, source: &Self) {
        if source.is_empty() {
            unsafe { self.set_len(0) };
            return;
        }

        // If the current string's capacity is insufficent, grow it
        if self.capacity() < source.len() {
            self.grow_exact(source.len());
        }

        unsafe {
            let length = source.len();
            ptr::copy_nonoverlapping(source.as_ptr(), self.as_mut_ptr(), length);
            self.set_len(length);
        }
    }
}

impl Drop for ThinStr {
    fn drop(&mut self) {
        if !self.is_sigil() {
            let layout = Self::layout_for(self.capacity());
            unsafe { std::alloc::dealloc(self.buf.as_ptr().cast(), layout) };
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

impl PartialEq for ThinStr {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.as_str().eq(other.as_str())
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
        if !self.is_sigil() {
            context
                .add(size_of::<StrHeader>())
                .add_vectorlike(self.len(), self.capacity(), 1);
        }
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
    pub fn owned_size_of_children(&self, context: &mut Context) {
        if !self.is_sigil() {
            context
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

impl PartialEq for ThinStrRef<'_> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.as_str().eq(other.as_str())
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
