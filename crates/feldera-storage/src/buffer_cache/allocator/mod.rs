use std::alloc::Layout;
use std::alloc::{alloc, dealloc};
use std::cell::RefCell;
use std::rc::Rc;
use std::{fmt, ptr};

use buddy_alloc::buddy_alloc::BuddyAlloc;
use buddy_alloc::BuddyAllocParam;

use super::fbuf::FBuf;

#[cfg(test)]
mod tests;

pub(crate) struct BufferAllocator {
    data: ptr::NonNull<u8>,
    size: usize,
    allocator: RefCell<BuddyAlloc>,
    layout: Layout,
}

impl Drop for BufferAllocator {
    fn drop(&mut self) {
        unsafe {
            dealloc(self.data.as_ptr(), self.layout);
        }
    }
}

impl fmt::Debug for BufferAllocator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BufferAllocator")
            .field("data", &self.data)
            .finish()
    }
}

impl BufferAllocator {
    pub(super) fn new(size: usize) -> Self {
        let layout = Layout::from_size_align(size, 4096).expect("Invalid layout");
        let (data, allocator) = unsafe {
            let data = alloc(layout);
            let data = ptr::NonNull::new(data).unwrap();
            let allocator = BuddyAlloc::new(BuddyAllocParam::new(
                data.as_ptr(),
                layout.size(),
                layout.align(),
            ));
            (data, RefCell::new(allocator))
        };

        BufferAllocator {
            data,
            size,
            allocator,
            layout,
        }
    }

    fn free(&self, ptr: ptr::NonNull<u8>) {
        let mut allocator = self.allocator.borrow_mut();
        allocator.free(ptr.as_ptr());
    }

    fn alloc(self: &Rc<Self>, _size: usize) -> Option<FBuf> {
        Some(FBuf::new())
    }
}
