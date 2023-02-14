use crate::codegen::VTable;
use bincode::{de::Decoder, enc::Encoder, error::EncodeError, Decode, Encode};
use size_of::SizeOf;
use std::{
    alloc::Layout,
    cmp::Ordering,
    fmt::{self, Debug},
    hash::{Hash, Hasher},
    ptr::NonNull,
};

/// An individually-allocated, dynamically generated row
pub struct Row {
    data: NonNull<u8>,
    vtable: &'static VTable,
}

impl Row {
    pub fn type_name(&self) -> &str {
        self.vtable.type_name()
    }

    #[inline]
    pub const fn vtable(&self) -> &VTable {
        self.vtable
    }

    #[inline]
    pub const fn as_ptr(&self) -> *const u8 {
        self.data.as_ptr()
    }

    #[inline]
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.data.as_ptr()
    }

    /// Creates an uninitalized row
    ///
    /// # Safety
    ///
    /// Must fully initialize the inner data before manipulating it
    pub unsafe fn uninit(vtable: &'static VTable) -> Self {
        Self {
            data: Self::alloc(vtable),
            vtable,
        }
    }

    fn alloc(vtable: &VTable) -> NonNull<u8> {
        if vtable.size_of == 0 {
            return NonNull::dangling();
        }

        debug_assert!(Layout::from_size_align(vtable.size_of, vtable.align_of.get()).is_ok());

        // Safety: We've already validated the preconditions of `Layout` when creating
        // our own layouts
        let layout =
            unsafe { Layout::from_size_align_unchecked(vtable.size_of, vtable.align_of.get()) };

        match NonNull::new(unsafe { std::alloc::alloc(layout) }) {
            Some(ptr) => ptr,
            None => std::alloc::handle_alloc_error(layout),
        }
    }
}

impl PartialEq for Row {
    fn eq(&self, other: &Self) -> bool {
        debug_assert_eq!(self.vtable.layout_id, other.vtable.layout_id);
        unsafe { (self.vtable.eq)(self.data.as_ptr(), other.data.as_ptr()) }
    }
}

impl Eq for Row {}

impl PartialOrd for Row {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        debug_assert_eq!(self.vtable.layout_id, other.vtable.layout_id);
        unsafe { Some((self.vtable.cmp)(self.data.as_ptr(), other.data.as_ptr())) }
    }

    fn lt(&self, other: &Self) -> bool {
        debug_assert_eq!(self.vtable.layout_id, other.vtable.layout_id);
        unsafe { (self.vtable.lt)(self.data.as_ptr(), other.data.as_ptr()) }
    }
}

impl Ord for Row {
    fn cmp(&self, other: &Self) -> Ordering {
        debug_assert_eq!(self.vtable.layout_id, other.vtable.layout_id);
        unsafe { (self.vtable.cmp)(self.data.as_ptr(), other.data.as_ptr()) }
    }
}

impl Hash for Row {
    fn hash<H: Hasher>(&self, state: &mut H) {
        unsafe { (self.vtable.hash)(&mut (state as &mut dyn Hasher), self.as_ptr()) }
    }
}

impl SizeOf for Row {
    fn size_of_children(&self, context: &mut size_of::Context) {
        if self.vtable.size_of != 0 {
            context.add_distinct_allocation().add(self.vtable.size_of);
            unsafe { (self.vtable.size_of_children)(self.data.as_ptr(), context) };
        }
    }
}

impl Encode for Row {
    fn encode<E>(&self, _encoder: &mut E) -> Result<(), EncodeError>
    where
        E: Encoder,
    {
        todo!()
    }
}

impl Decode for Row {
    fn decode<D>(_decoder: &mut D) -> Result<Self, bincode::error::DecodeError>
    where
        D: Decoder,
    {
        todo!()
    }
}

impl Debug for Row {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if unsafe { (self.vtable.debug)(self.data.as_ptr(), f) } {
            Ok(())
        } else {
            Err(fmt::Error)
        }
    }
}

impl Clone for Row {
    fn clone(&self) -> Self {
        let data = Self::alloc(self.vtable);
        unsafe { (self.vtable.clone)(self.data.as_ptr(), data.as_ptr()) };

        Self {
            data,
            vtable: self.vtable,
        }
    }
}

unsafe impl Send for Row {}

unsafe impl Sync for Row {}

impl Drop for Row {
    fn drop(&mut self) {
        if self.vtable.size_of != 0 {
            debug_assert!(
                Layout::from_size_align(self.vtable.size_of, self.vtable.align_of.get()).is_ok(),
            );

            // Safety: We've already validated the preconditions of `Layout` when creating
            // our own layouts
            let layout = unsafe {
                Layout::from_size_align_unchecked(self.vtable.size_of, self.vtable.align_of.get())
            };

            unsafe {
                (self.vtable.drop_in_place)(self.data.as_ptr());
                std::alloc::dealloc(self.data.as_ptr(), layout);
            }
        }
    }
}
