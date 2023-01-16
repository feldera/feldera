use crate::{
    algebra::{AddAssignByRef, AddByRef, HasZero, NegByRef},
    utils::{self, DynVecVTable},
};
use core::slice;
use size_of::{Context, SizeOf};
use std::{
    any::{self, TypeId},
    cmp::Ordering,
    fmt::{self, Debug},
    mem::{self, align_of, size_of, MaybeUninit},
    num::NonZeroUsize,
    ops::{AddAssign, Neg},
    ptr,
};

#[derive(Clone, Copy, SizeOf)]
pub struct ErasedVTable {
    pub size_of: usize,
    pub align_of: NonZeroUsize,
    pub eq: unsafe extern "C" fn(*const u8, *const u8) -> bool,
    pub lt: unsafe extern "C" fn(*const u8, *const u8) -> bool,
    pub cmp: unsafe extern "C" fn(*const u8, *const u8) -> Ordering,
    pub clone: unsafe extern "C" fn(*const u8, *mut u8),
    pub clone_into_slice: unsafe extern "C" fn(*const u8, *mut u8, usize),
    pub size_of_children: unsafe extern "C" fn(*const u8, &mut Context),
    pub debug: unsafe extern "C" fn(*const u8, *mut fmt::Formatter<'_>) -> bool,
    pub drop_in_place: unsafe extern "C" fn(*mut u8),
    pub drop_slice_in_place: unsafe extern "C" fn(*mut u8, usize),
    pub type_id: fn() -> TypeId,
    // Writes the string's length to the out pointer and returns the string's start
    pub type_name: unsafe extern "C" fn(*mut usize) -> *const u8,
}

impl ErasedVTable {
    pub fn type_name(&self) -> &'static str {
        unsafe {
            let mut len = 0;
            let ptr = (self.type_name)(&mut len);

            let bytes = slice::from_raw_parts(ptr, len);
            debug_assert!(std::str::from_utf8(bytes).is_ok());
            std::str::from_utf8_unchecked(bytes)
        }
    }

    /// # Safety
    ///
    /// `value` must be a valid pointer to a value associated with the current
    /// vtable
    pub unsafe fn debug(&self, value: *const u8, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if (self.debug)(value, f) {
            Ok(())
        } else {
            Err(fmt::Error)
        }
    }
}

impl PartialEq for ErasedVTable {
    fn eq(&self, other: &Self) -> bool {
        (self.type_id)() == (other.type_id)()
    }
}

impl Eq for ErasedVTable {}

impl Debug for ErasedVTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ErasedVTable")
            .field("size_of", &self.size_of)
            .field("align_of", &self.align_of)
            .field("eq", &self.eq)
            .field("lt", &self.lt)
            .field("cmp", &self.cmp)
            .field("clone", &self.clone)
            .field("clone_into_slice", &self.clone_into_slice)
            .field("size_of_children", &(self.size_of_children as *const ()))
            .field("debug", &(self.debug as *const ()))
            .field("drop_in_place", &self.drop_in_place)
            .field("drop_slice_in_place", &self.drop_slice_in_place)
            .field("type_id", &self.type_id)
            .field("type_name", &self.type_name)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, SizeOf)]
pub struct DataVTable {
    pub common: ErasedVTable,
}

// Safety: `IntoErased` requires `Send` and `Sync`
unsafe impl DynVecVTable for DataVTable {
    fn size_of(&self) -> usize {
        self.common.size_of
    }

    fn align_of(&self) -> NonZeroUsize {
        self.common.align_of
    }

    fn type_id(&self) -> TypeId {
        (self.common.type_id)()
    }

    fn type_name(&self) -> &'static str {
        self.common.type_name()
    }

    unsafe fn clone_slice(&self, src: *const u8, dest: *mut u8, count: usize) {
        (self.common.clone_into_slice)(src, dest, count);
    }

    unsafe fn drop_slice_in_place(&self, ptr: *mut u8, length: usize) {
        (self.common.drop_slice_in_place)(ptr, length)
    }

    unsafe fn size_of_children(&self, ptr: *const u8, context: &mut Context) {
        (self.common.size_of_children)(ptr, context);
    }

    unsafe fn debug(&self, value: *const u8, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.common.debug(value, f)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, SizeOf)]
pub struct DiffVTable {
    pub common: ErasedVTable,
    pub is_zero: unsafe fn(*const u8) -> bool,
    pub add_by_ref: unsafe fn(*const u8, *const u8, *mut u8),
    pub neg_slice: unsafe fn(*mut u8, usize),
    pub neg_slice_by_ref: unsafe fn(*const u8, *mut u8, usize),
}

// Safety: `IntoErased` requires `Send` and `Sync`
unsafe impl DynVecVTable for DiffVTable {
    fn size_of(&self) -> usize {
        self.common.size_of
    }

    fn align_of(&self) -> NonZeroUsize {
        self.common.align_of
    }

    fn type_id(&self) -> TypeId {
        (self.common.type_id)()
    }

    fn type_name(&self) -> &'static str {
        self.common.type_name()
    }

    unsafe fn clone_slice(&self, src: *const u8, dest: *mut u8, count: usize) {
        (self.common.clone_into_slice)(src, dest, count);
    }

    unsafe fn drop_slice_in_place(&self, ptr: *mut u8, length: usize) {
        (self.common.drop_slice_in_place)(ptr, length)
    }

    unsafe fn size_of_children(&self, ptr: *const u8, context: &mut Context) {
        (self.common.size_of_children)(ptr, context);
    }

    unsafe fn debug(&self, value: *const u8, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.common.debug(value, f)
    }
}

pub trait IntoErased: Sized + Eq + Ord + Clone + Send + SizeOf + Debug + 'static {
    // + Sync
    const ERASED_VTABLE: ErasedVTable;
}

impl<T> IntoErased for T
where
    T: Sized + Eq + Ord + Clone + Send + SizeOf + Debug + 'static, // + Sync
{
    const ERASED_VTABLE: ErasedVTable = {
        unsafe extern "C" fn eq<T: PartialEq>(lhs: *const u8, rhs: *const u8) -> bool {
            debug_assert!(!lhs.is_null() && !rhs.is_null());
            unsafe { T::eq(&*lhs.cast(), &*rhs.cast()) }
        }

        unsafe extern "C" fn lt<T: Ord>(lhs: *const u8, rhs: *const u8) -> bool {
            debug_assert!(!lhs.is_null() && !rhs.is_null());
            unsafe { T::lt(&*lhs.cast(), &*rhs.cast()) }
        }

        unsafe extern "C" fn cmp<T: Ord>(lhs: *const u8, rhs: *const u8) -> Ordering {
            debug_assert!(!lhs.is_null() && !rhs.is_null());
            unsafe { T::cmp(&*lhs.cast(), &*rhs.cast()) }
        }

        unsafe extern "C" fn clone<T: Clone>(src: *const u8, dest: *mut u8) {
            debug_assert!(!src.is_null() && !dest.is_null());
            unsafe { dest.cast::<T>().write(T::clone(&*src.cast())) };
        }

        unsafe extern "C" fn clone_into_slice<T: Clone>(
            src: *const u8,
            dest: *mut u8,
            count: usize,
        ) {
            debug_assert!(!src.is_null() && !dest.is_null());

            if count == 0 {
                return;
            }

            unsafe {
                let src = slice::from_raw_parts(src.cast(), count);
                let dest = slice::from_raw_parts_mut(dest.cast(), count);
                utils::write_uninit_slice_cloned::<T>(dest, src);
            }
        }

        unsafe extern "C" fn size_of_children<T: SizeOf>(value: *const u8, context: &mut Context) {
            debug_assert!(!value.is_null());
            unsafe { T::size_of_children(&*value.cast(), context) }
        }

        unsafe extern "C" fn debug<T: Debug>(value: *const u8, f: *mut fmt::Formatter<'_>) -> bool {
            debug_assert!(!value.is_null() && !f.is_null());
            unsafe { <T as Debug>::fmt(&*value.cast(), &mut *f).is_ok() }
        }

        unsafe extern "C" fn drop_in_place<T>(value: *mut u8) {
            debug_assert!(!value.is_null());
            unsafe { ptr::drop_in_place(value.cast::<T>()) }
        }

        unsafe extern "C" fn drop_slice_in_place<T>(ptr: *mut u8, length: usize) {
            debug_assert!(!ptr.is_null());
            unsafe { ptr::drop_in_place(slice::from_raw_parts_mut(ptr.cast::<T>(), length)) }
        }

        unsafe extern "C" fn type_name<T>(length: *mut usize) -> *const u8 {
            debug_assert!(!length.is_null());

            let name = any::type_name::<T>();
            length.write(name.len());
            name.as_ptr()
        }

        ErasedVTable {
            size_of: size_of::<Self>(),
            align_of: match NonZeroUsize::new(align_of::<Self>()) {
                Some(align) => align,
                None => panic!("alignments cannot be zero"),
            },
            eq: eq::<Self>,
            lt: lt::<Self>,
            cmp: cmp::<Self>,
            clone: clone::<Self>,
            clone_into_slice: clone_into_slice::<Self>,
            size_of_children: size_of_children::<Self>,
            debug: debug::<Self>,
            drop_in_place: drop_in_place::<Self>,
            drop_slice_in_place: drop_slice_in_place::<Self>,
            type_id: TypeId::of::<Self>,
            type_name: type_name::<Self>,
        }
    };
}

pub trait IntoErasedData: IntoErased {
    const DATA_VTABLE: DataVTable;
}

impl<T> IntoErasedData for T
where
    T: IntoErased,
{
    const DATA_VTABLE: DataVTable = {
        DataVTable {
            common: Self::ERASED_VTABLE,
        }
    };
}

pub trait IntoErasedDiff:
    HasZero + AddByRef + AddAssign + AddAssignByRef + Neg<Output = Self> + NegByRef + IntoErased
{
    const DIFF_VTABLE: DiffVTable;
}

impl<T> IntoErasedDiff for T
where
    T: HasZero + AddByRef + AddAssign + AddAssignByRef + Neg<Output = T> + NegByRef + IntoErased,
{
    const DIFF_VTABLE: DiffVTable = {
        unsafe fn is_zero<T: HasZero>(value: *const u8) -> bool {
            unsafe { T::is_zero(&*value.cast()) }
        }

        unsafe fn add_by_ref<T: AddByRef>(lhs: *const u8, rhs: *const u8, out: *mut u8) {
            unsafe {
                out.cast::<T>()
                    .write(T::add_by_ref(&*lhs.cast(), &*rhs.cast()));
            }
        }

        unsafe fn neg_slice<T: Neg<Output = T>>(ptr: *mut u8, count: usize) {
            struct Canary;

            impl Drop for Canary {
                fn drop(&mut self) {
                    panic!()
                }
            }

            let slice = unsafe { slice::from_raw_parts_mut(ptr.cast::<MaybeUninit<T>>(), count) };

            // This can cause ub if any of the neg calls panic, so we have a canary to force
            // an abort by double panicking
            let canary = Canary;
            for elem in slice {
                unsafe { elem.write(elem.assume_init_read().neg()) };
            }

            mem::forget(canary);
        }

        unsafe fn neg_slice_by_ref<T: NegByRef>(source: *const u8, dest: *mut u8, count: usize) {
            struct Dropper<T> {
                start: *mut T,
                current: *mut T,
            }

            impl<T> Drop for Dropper<T> {
                fn drop(&mut self) {
                    let length = unsafe { self.current.offset_from(self.start) as usize };
                    let initialized = ptr::slice_from_raw_parts_mut(self.start, length);
                    unsafe { ptr::drop_in_place(initialized) };
                }
            }

            let source = unsafe { slice::from_raw_parts(source.cast::<T>(), count) };

            // In the event that any of the `.neg_by_ref()` calls panic, `Dropper`
            // drops all previously initialized elements
            let dest = dest.cast::<T>();
            let mut negater = Dropper {
                start: dest,
                current: dest,
            };

            for elem in source {
                unsafe {
                    negater.current.write(elem.neg_by_ref());
                    negater.current = negater.current.add(1);
                }
            }

            mem::forget(negater);
        }

        DiffVTable {
            common: Self::ERASED_VTABLE,
            is_zero: is_zero::<Self>,
            add_by_ref: add_by_ref::<Self>,
            neg_slice: neg_slice::<Self>,
            neg_slice_by_ref: neg_slice_by_ref::<Self>,
        }
    };
}
