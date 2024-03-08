use crate::utils::assume;
use std::{mem::forget, ptr};

// TODO: Migrate retain and quicksort to use payloads

/// An implementation of `Vec::dedup_by()` that takes a starting point and
/// operates over paired key and payload vectors
///
/// Modified from [the stdlib](https://doc.rust-lang.org/std/vec/struct.Vec.html#method.dedup_by)
pub(super) fn dedup_payload_starting_at<'a, T, P, F>(
    keys: &'a mut Vec<T>,
    payload: P,
    starting_point: usize,
    mut same_bucket: F,
) where
    P: Payload<'a>,
    F: FnMut(&mut T, P::ItemRefMut, &mut T, P::ItemRefMut) -> bool,
{
    assert!(payload.len().equal_to(keys.len()));

    let len = keys.len();
    if len <= 1 || starting_point + 1 >= keys.len() {
        return;
    }

    // INVARIANT: vec.len() > read >= write > write-1 >= 0
    struct FillGapOnDrop<'a, T, P>
    where
        P: Payload<'a>,
    {
        // Offset of the element we want to check if it is duplicate
        read: usize,

        // Offset of the place where we want to place the non-duplicate
        // when we find it.
        write: usize,

        // The Vec that would need correction if `same_bucket` panicked
        keys: &'a mut Vec<T>,
        payload: P,
    }

    impl<'a, T, P> Drop for FillGapOnDrop<'a, T, P>
    where
        P: Payload<'a>,
    {
        fn drop(&mut self) {
            // This code gets executed when `same_bucket` panics

            // SAFETY: invariant guarantees that `read - write`
            // and `len - read` never overflow and that the copy is always
            // in-bounds.
            unsafe {
                let len = self.keys.len();
                assume(self.payload.len().equal_to(self.keys.len()));

                let key_ptr = self.keys.as_mut_ptr();
                let payload_ptr = self.payload.as_mut_ptr();

                // How many items were left when `same_bucket` panicked.
                // Basically vec[read..].len()
                let items_left = len.wrapping_sub(self.read);

                // Pointer to first item in vec[write..write+items_left] slice
                let dropped_key_ptr = key_ptr.add(self.write);
                let dropped_payload_ptr = payload_ptr.add(self.write);

                // Pointer to first item in vec[read..] slice
                let valid_key_ptr = key_ptr.add(self.read);
                let valid_payload_ptr = payload_ptr.add(self.read);

                // Copy `vec[read..]` to `vec[write..write+items_left]`.
                // The slices can overlap, so `copy_nonoverlapping` cannot be used
                ptr::copy(valid_key_ptr, dropped_key_ptr, items_left);
                P::copy_payload(valid_payload_ptr, dropped_payload_ptr, items_left);

                // How many items have been already dropped
                // Basically vec[read..write].len()
                let dropped = self.read.wrapping_sub(self.write);

                self.keys.set_len(len - dropped);
                self.payload.set_len(len - dropped);
            }
        }
    }

    let mut gap = FillGapOnDrop {
        read: starting_point + 1,
        write: starting_point + 1,
        keys,
        payload,
    };
    let key_ptr = gap.keys.as_mut_ptr();
    let payload_ptr = gap.payload.as_mut_ptr();

    // Drop items while going through Vec, it should be more efficient than
    // doing slice partition_dedup + truncate

    // SAFETY: Because of the invariant, read_ptr, prev_ptr and write_ptr
    // are always in-bounds and read_ptr never aliases prev_ptr
    unsafe {
        while gap.read < len {
            let read_key_ptr = key_ptr.add(gap.read);
            let read_payload_ptr = payload_ptr.add(gap.read);
            let prev_key_ptr = key_ptr.add(gap.write.wrapping_sub(1));
            let prev_payload_ptr = payload_ptr.add(gap.write.wrapping_sub(1));

            if same_bucket(
                &mut *read_key_ptr,
                P::deref_mut(read_payload_ptr),
                &mut *prev_key_ptr,
                P::deref_mut(prev_payload_ptr),
            ) {
                // Increase `gap.read` now since the drop may panic.
                gap.read += 1;

                // We have found duplicate, drop it in-place
                ptr::drop_in_place(read_key_ptr);
                P::drop_payload_in_place(read_payload_ptr);
            } else {
                let write_key_ptr = key_ptr.add(gap.write);
                let write_payload_ptr = payload_ptr.add(gap.write);

                // Because `read_ptr` can be equal to `write_ptr`, we either
                // have to use `copy` or conditional `copy_nonoverlapping`.
                // Looks like the first option is faster.
                ptr::copy(read_key_ptr, write_key_ptr, 1);
                P::copy_payload(read_payload_ptr, write_payload_ptr, 1);

                // We have filled that place, so go further
                gap.write += 1;
                gap.read += 1;
            }
        }

        // Technically we could let `gap` clean up with its Drop, but
        // when `same_bucket` is guaranteed to not panic, this bloats a little
        // the codegen, so we just do it manually
        gap.keys.set_len(gap.write);
        gap.payload.set_len(gap.write);
        forget(gap);
    }
}

/// An implementation of `Vec::retain()` that takes a starting point
///
/// Modified from [the stdlib](https://doc.rust-lang.org/std/vec/struct.Vec.html#method.retain)
#[cfg(test)]
pub(super) fn retain_starting_at<T, F>(vec: &mut Vec<T>, starting_point: usize, mut retain: F)
where
    F: FnMut(&mut T) -> bool,
{
    if vec.is_empty() || starting_point >= vec.len() {
        return;
    }

    let original_len = vec.len();
    // Avoid double drop if the drop guard is not executed,
    // since we may make some holes during the process.
    unsafe { vec.set_len(0) };

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
    // length. In cases when predicate and `drop` never panick, it will be
    // optimized out.
    struct BackshiftOnDrop<'a, T> {
        v: &'a mut Vec<T>,
        processed_len: usize,
        deleted_cnt: usize,
        original_len: usize,
    }

    impl<T> Drop for BackshiftOnDrop<'_, T> {
        fn drop(&mut self) {
            if self.deleted_cnt > 0 {
                // SAFETY: Trailing unchecked items must be valid since we never touch them.
                unsafe {
                    ptr::copy(
                        self.v.as_ptr().add(self.processed_len),
                        self.v
                            .as_mut_ptr()
                            .add(self.processed_len - self.deleted_cnt),
                        self.original_len - self.processed_len,
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
        v: vec,
        processed_len: starting_point,
        deleted_cnt: 0,
        original_len,
    };

    fn process_loop<F, T, const DELETED: bool>(
        original_len: usize,
        f: &mut F,
        g: &mut BackshiftOnDrop<'_, T>,
    ) where
        F: FnMut(&mut T) -> bool,
    {
        while g.processed_len != original_len {
            // SAFETY: Unchecked element must be valid.
            let cur = unsafe { &mut *g.v.as_mut_ptr().add(g.processed_len) };
            if !f(cur) {
                // Advance early to avoid double drop if `drop_in_place` panicked.
                g.processed_len += 1;
                g.deleted_cnt += 1;

                // SAFETY: We never touch this element again after dropped.
                unsafe { ptr::drop_in_place(cur) };

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
                    let hole_slot = g.v.as_mut_ptr().add(g.processed_len - g.deleted_cnt);
                    ptr::copy_nonoverlapping(cur, hole_slot, 1);
                }
            }

            g.processed_len += 1;
        }
    }

    // Stage 1: Nothing was deleted.
    process_loop::<F, T, false>(original_len, &mut retain, &mut g);

    // Stage 2: Some elements were deleted.
    process_loop::<F, T, true>(original_len, &mut retain, &mut g);

    // All item are processed. This can be optimized to `set_len` by LLVM.
    drop(g);
}

/// An implementation of `Vec::retain()` that takes a starting point
///
/// Modified from [the stdlib](https://doc.rust-lang.org/std/vec/struct.Vec.html#method.retain)
pub(super) fn retain_payload_starting_at<K, P, F>(
    keys: &mut Vec<K>,
    payload: &mut Vec<P>,
    starting_point: usize,
    mut retain: F,
) where
    F: FnMut(&mut K, &mut P) -> bool,
{
    assert_eq!(keys.len(), payload.len());
    if keys.is_empty() || starting_point >= keys.len() {
        return;
    }

    let original_len = keys.len();
    // Avoid double drop if the drop guard is not executed,
    // since we may make some holes during the process.
    unsafe {
        keys.set_len(0);
        payload.set_len(0);
    }

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
    // length. In cases when predicate and `drop` never panick, it will be
    // optimized out.
    struct BackshiftOnDrop<'a, K, P> {
        keys: &'a mut Vec<K>,
        payload: &'a mut Vec<P>,
        processed_len: usize,
        deleted_cnt: usize,
        original_len: usize,
    }

    impl<K, P> Drop for BackshiftOnDrop<'_, K, P> {
        fn drop(&mut self) {
            if self.deleted_cnt > 0 {
                // SAFETY: Trailing unchecked items must be valid since we never touch them.
                unsafe {
                    let src = self.processed_len;
                    let dest = self.processed_len - self.deleted_cnt;
                    let count = self.original_len - self.processed_len;

                    ptr::copy(
                        self.keys.as_ptr().add(src),
                        self.keys.as_mut_ptr().add(dest),
                        count,
                    );

                    ptr::copy(
                        self.payload.as_ptr().add(src),
                        self.payload.as_mut_ptr().add(dest),
                        count,
                    );
                }
            }

            // SAFETY: After filling holes, all items are in contiguous memory.
            unsafe {
                let length = self.original_len - self.deleted_cnt;
                self.keys.set_len(length);
                self.payload.set_len(length);
            }
        }
    }

    let mut guard = BackshiftOnDrop {
        keys,
        payload,
        processed_len: starting_point,
        deleted_cnt: 0,
        original_len,
    };

    fn process_loop<K, P, F, const DELETED: bool>(
        original_len: usize,
        retain: &mut F,
        guard: &mut BackshiftOnDrop<'_, K, P>,
    ) where
        F: FnMut(&mut K, &mut P) -> bool,
    {
        while guard.processed_len != original_len {
            // SAFETY: Unchecked element must be valid.
            let (current_key, current_payload) = unsafe {
                (
                    &mut *guard.keys.as_mut_ptr().add(guard.processed_len),
                    &mut *guard.payload.as_mut_ptr().add(guard.processed_len),
                )
            };

            if !retain(current_key, current_payload) {
                // Advance early to avoid double drop if `drop_in_place` panicked.
                guard.processed_len += 1;
                guard.deleted_cnt += 1;

                // SAFETY: We never touch this element again after dropped.
                // TODO: If we pushed drop responsibility onto the user (maybe by giving them an
                // `(&mut MaybeUninit<K>, &mut MaybeUninit<P>)`) then they could take ownership
                // of the value when not retaining it
                unsafe {
                    ptr::drop_in_place(current_key);
                    ptr::drop_in_place(current_payload);
                }

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
                    let slot_start = guard.processed_len - guard.deleted_cnt;

                    let key_hole_slot = guard.keys.as_mut_ptr().add(slot_start);
                    ptr::copy_nonoverlapping(current_key, key_hole_slot, 1);

                    let payload_hole_slot = guard.payload.as_mut_ptr().add(slot_start);
                    ptr::copy_nonoverlapping(current_payload, payload_hole_slot, 1);
                }
            }

            guard.processed_len += 1;
        }
    }

    // Stage 1: Nothing was deleted.
    process_loop::<K, P, F, false>(original_len, &mut retain, &mut guard);

    // Stage 2: Some elements were deleted.
    process_loop::<K, P, F, true>(original_len, &mut retain, &mut guard);

    // All item are processed. This can be optimized to `set_len` by LLVM.
    drop(guard);
}

pub(super) trait Payload<'a> {
    type Len: PayloadLen;
    type Ptr: PayloadPtr;
    type ItemRefMut;

    fn len(&self) -> Self::Len;

    fn as_ptr(&self) -> Self::Ptr;

    fn as_mut_ptr(&mut self) -> Self::Ptr;

    unsafe fn set_len(&mut self, len: usize);

    unsafe fn copy_payload(src: Self::Ptr, dest: Self::Ptr, count: usize);

    unsafe fn copy_payload_nonoverlapping(src: Self::Ptr, dest: Self::Ptr, count: usize);

    unsafe fn drop_payload_in_place(payload: Self::Ptr);

    unsafe fn deref_mut(payload: Self::Ptr) -> Self::ItemRefMut;
}

impl<'a> Payload<'a> for () {
    type Len = ();
    type Ptr = ();
    type ItemRefMut = ();

    fn len(&self) -> Self::Len {}

    fn as_ptr(&self) -> Self::Ptr {}

    fn as_mut_ptr(&mut self) -> Self::Ptr {}

    unsafe fn set_len(&mut self, _len: usize) {}

    unsafe fn copy_payload(_src: Self::Ptr, _dest: Self::Ptr, _count: usize) {}

    unsafe fn copy_payload_nonoverlapping(_src: Self::Ptr, _dest: Self::Ptr, _count: usize) {}

    unsafe fn drop_payload_in_place(_payload: Self::Ptr) {}

    unsafe fn deref_mut(_payload: Self::Ptr) -> Self::ItemRefMut {}
}

impl<'a, T> Payload<'a> for &'a mut Vec<T> {
    type Len = usize;
    type Ptr = *mut T;
    type ItemRefMut = &'a mut T;

    fn len(&self) -> Self::Len {
        Vec::len(self)
    }

    fn as_ptr(&self) -> Self::Ptr {
        Vec::as_ptr(self) as *mut T
    }

    fn as_mut_ptr(&mut self) -> Self::Ptr {
        Vec::as_mut_ptr(self)
    }

    unsafe fn set_len(&mut self, len: usize) {
        Vec::set_len(self, len);
    }

    unsafe fn copy_payload(src: Self::Ptr, dest: Self::Ptr, count: usize) {
        ptr::copy(src, dest, count);
    }

    unsafe fn copy_payload_nonoverlapping(src: Self::Ptr, dest: Self::Ptr, count: usize) {
        ptr::copy_nonoverlapping(src, dest, count);
    }

    unsafe fn drop_payload_in_place(payload: Self::Ptr) {
        ptr::drop_in_place(payload);
    }

    unsafe fn deref_mut(payload: Self::Ptr) -> Self::ItemRefMut {
        &mut *payload
    }
}

impl<'a, P1, P2> Payload<'a> for (P1, P2)
where
    P1: Payload<'a>,
    P2: Payload<'a>,
{
    type Len = (P1::Len, P2::Len);
    type Ptr = (P1::Ptr, P2::Ptr);
    type ItemRefMut = (P1::ItemRefMut, P2::ItemRefMut);

    fn len(&self) -> Self::Len {
        (self.0.len(), self.1.len())
    }

    fn as_ptr(&self) -> Self::Ptr {
        (self.0.as_ptr(), self.1.as_ptr())
    }

    fn as_mut_ptr(&mut self) -> Self::Ptr {
        (self.0.as_mut_ptr(), self.1.as_mut_ptr())
    }

    unsafe fn set_len(&mut self, len: usize) {
        self.0.set_len(len);
        self.1.set_len(len);
    }

    unsafe fn copy_payload(src: Self::Ptr, dest: Self::Ptr, count: usize) {
        P1::copy_payload(src.0, dest.0, count);
        P2::copy_payload(src.1, dest.1, count);
    }

    unsafe fn copy_payload_nonoverlapping(src: Self::Ptr, dest: Self::Ptr, count: usize) {
        P1::copy_payload_nonoverlapping(src.0, dest.0, count);
        P2::copy_payload_nonoverlapping(src.1, dest.1, count);
    }

    unsafe fn drop_payload_in_place(payload: Self::Ptr) {
        P1::drop_payload_in_place(payload.0);
        P2::drop_payload_in_place(payload.1);
    }

    unsafe fn deref_mut(payload: Self::Ptr) -> Self::ItemRefMut {
        (P1::deref_mut(payload.0), P2::deref_mut(payload.1))
    }
}

pub(super) trait PayloadLen: Copy + 'static {
    fn equal_to(&self, len: usize) -> bool;

    fn is_empty(&self) -> bool {
        self.equal_to(0)
    }

    fn from_usize(len: usize) -> Self;
}

impl PayloadLen for () {
    fn equal_to(&self, _len: usize) -> bool {
        true
    }

    fn from_usize(_len: usize) -> Self {}
}

impl PayloadLen for usize {
    fn equal_to(&self, len: usize) -> bool {
        len == *self
    }

    fn from_usize(len: usize) -> Self {
        len
    }
}

impl<L1, L2> PayloadLen for (L1, L2)
where
    L1: PayloadLen,
    L2: PayloadLen,
{
    fn equal_to(&self, len: usize) -> bool {
        self.0.equal_to(len) && self.1.equal_to(len)
    }

    fn from_usize(len: usize) -> Self {
        (L1::from_usize(len), L2::from_usize(len))
    }
}

pub(super) trait PayloadPtr: Copy {
    unsafe fn offset(self, count: isize) -> Self;

    unsafe fn add(self, count: usize) -> Self;

    unsafe fn sub(self, count: usize) -> Self;
}

impl PayloadPtr for () {
    unsafe fn offset(self, _count: isize) -> Self {}

    unsafe fn add(self, _count: usize) -> Self {}

    unsafe fn sub(self, _count: usize) -> Self {}
}

impl<T> PayloadPtr for *mut T {
    unsafe fn offset(self, count: isize) -> Self {
        self.offset(count)
    }

    unsafe fn add(self, count: usize) -> Self {
        self.add(count)
    }

    unsafe fn sub(self, count: usize) -> Self {
        self.sub(count)
    }
}

impl<P1, P2> PayloadPtr for (P1, P2)
where
    P1: PayloadPtr,
    P2: PayloadPtr,
{
    unsafe fn offset(self, count: isize) -> Self {
        (self.0.offset(count), self.1.offset(count))
    }

    unsafe fn add(self, count: usize) -> Self {
        (self.0.add(count), self.1.add(count))
    }

    unsafe fn sub(self, count: usize) -> Self {
        (self.0.sub(count), self.1.sub(count))
    }
}
