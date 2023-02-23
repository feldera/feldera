use crate::thin_str::{StrHeader, ThinStr, EMPTY};
use size_of::{Context, SizeOf};
use std::{
    cmp::Ordering,
    fmt::{self, Debug, Display},
    hash::{Hash, Hasher},
    marker::PhantomData,
    mem::size_of,
    ops::Deref,
    ptr::{addr_of, addr_of_mut, NonNull},
    slice,
    str::{self},
};

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

    #[inline]
    pub(super) const unsafe fn from_raw(buf: NonNull<StrHeader>) -> Self {
        Self {
            buf,
            __lifetime: PhantomData,
        }
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

impl SizeOf for ThinStrRef<'_> {
    fn size_of_children(&self, _context: &mut Context) {}
}
