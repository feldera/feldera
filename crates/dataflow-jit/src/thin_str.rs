use std::{
    alloc::Layout,
    cmp::Ordering,
    fmt::{self, Debug, Display},
    marker::PhantomData,
    mem::{align_of, ManuallyDrop},
    ops::{Deref, DerefMut},
    ptr::{self, addr_of, addr_of_mut, NonNull},
    slice, str,
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
        debug_assert!(self.buf != NonNull::from(&EMPTY));
        unsafe { addr_of_mut!((*self.buf.as_ptr()).length).write(length) }
    }

    #[inline]
    pub unsafe fn set_capacity(&mut self, capacity: usize) {
        debug_assert!(self.buf != NonNull::from(&EMPTY));
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

            let mut this = Self::with_capacity_uninit(length);
            ptr::copy_nonoverlapping(string.as_ptr(), this.as_mut_ptr(), length);
            this.set_capacity(string.len());
            this.set_len(string.len());

            this
        }
    }

    unsafe fn with_capacity_uninit(capacity: usize) -> Self {
        let layout = Self::layout_for(capacity);

        let ptr = unsafe { std::alloc::alloc(layout) };
        let buf = match NonNull::new(ptr.cast::<StrHeader>()) {
            Some(buf) => buf,
            None => std::alloc::handle_alloc_error(layout),
        };

        Self { buf }
    }

    fn layout_for(capacity: usize) -> Layout {
        let header = Layout::new::<StrHeader>();

        let align = align_of::<usize>();
        let bytes = Layout::from_size_align(round_to_align(capacity, align), align)
            .expect("failed to create layout for string bytes");

        let (layout, _) = header
            .extend(bytes)
            .expect("failed to add header and string bytes layouts");

        // Pad out the layout
        layout.pad_to_align()
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
            *self = Self::new();
            return;
        }

        if self.capacity() >= source.len() {
            unsafe {
                let length = source.len();
                ptr::copy_nonoverlapping(source.as_ptr(), self.as_mut_ptr(), length);
                self.set_len(length);
            }
        } else {
            // FIXME: Resize the allocation
            *self = source.clone();
        }
    }
}

impl Drop for ThinStr {
    fn drop(&mut self) {
        if self.buf != NonNull::from(&EMPTY) {
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

#[inline]
const fn round_to_align(size: usize, align: usize) -> usize {
    size.wrapping_add(align).wrapping_sub(1) & !align.wrapping_sub(1)
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
