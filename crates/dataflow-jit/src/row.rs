use crate::{
    codegen::{BitSetType, NativeLayout, VTable},
    ir::{
        literal::{NullableConstant, RowLiteral},
        Constant,
    },
    ThinStr,
};
use bincode::{
    de::Decoder,
    enc::Encoder,
    error::{DecodeError, EncodeError},
    Decode, Encode,
};
use size_of::SizeOf;
use std::{
    alloc::Layout,
    cmp::Ordering,
    fmt::{self, Debug},
    hash::{Hash, Hasher},
    ptr::NonNull,
    slice,
};

// TODO: Column value read/write functions

#[repr(C)]
pub struct UninitRow {
    data: NonNull<u8>,
    vtable: &'static VTable,
}

impl UninitRow {
    pub fn new(vtable: &'static VTable) -> Self {
        Self {
            data: Self::alloc(vtable),
            vtable,
        }
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

    pub fn type_name(&self) -> &str {
        self.vtable.type_name()
    }

    /// Turns an uninitialized row into an initialized one
    ///
    /// # Safety
    ///
    /// All columns of the row must be initialized
    #[inline]
    pub const unsafe fn assume_init(self) -> Row {
        Row { inner: self }
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

    // TODO: Ideally we'd retain enough info within the vtable to not require the
    // `layout` argument TODO: Make sure that `layout` corresponds to the
    // current row's layout
    pub fn set_column_null(&mut self, column: usize, layout: &NativeLayout, null: bool) {
        if layout.column_type_of(column).is_string() {
            if null {
                let offset = layout.offset_of(column) as usize;
                let string_ptr = unsafe { self.as_mut_ptr().add(offset).cast::<usize>() };
                unsafe { string_ptr.write(0) };
            }

            return;
        }

        let (ty, bit_offset, bit) = layout.nullability_of(column);

        let bitset = unsafe { self.as_mut_ptr().add(bit_offset as usize) };
        debug_assert_eq!(bitset as usize % ty.align() as usize, 0);

        let value = if layout.bitset_occupants(column) == 1 {
            // If there's only one occupant in the bitset we can set it directly
            null as u64

        // If there's more than one occupant in the bitset we need to load,
        // set/unset the bit and then store it
        } else {
            // Load the bitset's current value
            let mut mask = unsafe {
                match ty {
                    BitSetType::U8 => bitset.read() as u64,
                    BitSetType::U16 => bitset.cast::<u16>().read() as u64,
                    BitSetType::U32 => bitset.cast::<u32>().read() as u64,
                    BitSetType::U64 => bitset.cast::<u64>().read(),
                }
            };

            // Set or unset the bit
            if null {
                mask |= 1 << bit;
            } else {
                mask &= !(1 << bit);
            }

            mask
        };

        // Store the modified bitset
        unsafe {
            match ty {
                BitSetType::U8 => bitset.write(value as u8),
                BitSetType::U16 => bitset.cast::<u16>().write(value as u16),
                BitSetType::U32 => bitset.cast::<u32>().write(value as u32),
                BitSetType::U64 => bitset.cast::<u64>().write(value),
            }
        }
    }

    /// Returns `true` if the given column is null
    ///
    /// # Safety
    ///
    /// The null flag for the given column must have been initialized
    ///
    /// # Panics
    ///
    /// Panics if the given column is not nullable
    // TODO: Ideally we'd retain enough info within the vtable to not require the
    // `layout` argument TODO: Make sure that `layout` corresponds to the
    // current row's layout
    pub unsafe fn column_is_null(&self, column: usize, layout: &NativeLayout) -> bool {
        if layout.column_type_of(column).is_string() {
            let offset = layout.offset_of(column) as usize;
            let string = unsafe { self.as_ptr().add(offset).cast::<*mut u8>().read() };
            return string.is_null();
        }

        let (ty, bit_offset, bit) = layout.nullability_of(column);

        let bitset = unsafe { self.as_ptr().add(bit_offset as usize) };
        debug_assert_eq!(bitset as usize % ty.align() as usize, 0);

        let value = unsafe {
            match ty {
                BitSetType::U8 => bitset.read() as u64,
                BitSetType::U16 => bitset.cast::<u16>().read() as u64,
                BitSetType::U32 => bitset.cast::<u32>().read() as u64,
                BitSetType::U64 => bitset.cast::<u64>().read(),
            }
        };

        if layout.bitset_occupants(column) == 1 {
            value != 0
        } else {
            value & (1 << bit) != 0
        }
    }
}

impl Drop for UninitRow {
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

            // Safety: This is a valid allocation
            unsafe { std::alloc::dealloc(self.as_mut_ptr(), layout) }
        }
    }
}

/// An individually-allocated, dynamically generated row
#[repr(transparent)]
pub struct Row {
    inner: UninitRow,
}

impl Row {
    #[inline]
    pub const fn vtable(&self) -> &VTable {
        self.inner.vtable()
    }

    #[inline]
    pub const fn as_ptr(&self) -> *const u8 {
        self.inner.as_ptr()
    }

    #[inline]
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.inner.as_mut_ptr()
    }

    pub fn type_name(&self) -> &str {
        self.inner.type_name()
    }

    pub fn column_is_null(&self, column: usize, layout: &NativeLayout) -> bool {
        // Safety: The current row is initialized
        unsafe { self.inner.column_is_null(column, layout) }
    }

    pub fn set_column_null(&mut self, column: usize, layout: &NativeLayout, null: bool) {
        self.inner.set_column_null(column, layout, null);
    }
}

impl PartialEq for Row {
    fn eq(&self, other: &Self) -> bool {
        debug_assert_eq!(self.vtable().layout_id, other.vtable().layout_id);
        unsafe { (self.vtable().eq)(self.as_ptr(), other.as_ptr()) }
    }
}

impl PartialEq<&Self> for Row {
    fn eq(&self, other: &&Self) -> bool {
        debug_assert_eq!(self.vtable().layout_id, other.vtable().layout_id);
        unsafe { (self.vtable().eq)(self.as_ptr(), other.as_ptr()) }
    }
}

impl<'a> PartialEq<Row> for &'a Row {
    fn eq(&self, other: &Row) -> bool {
        debug_assert_eq!(self.vtable().layout_id, other.vtable().layout_id);
        unsafe { (self.vtable().eq)(self.as_ptr(), other.as_ptr()) }
    }
}

impl Eq for Row {}

impl PartialOrd for Row {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        debug_assert_eq!(self.vtable().layout_id, other.vtable().layout_id);
        unsafe { Some((self.vtable().cmp)(self.as_ptr(), other.as_ptr())) }
    }

    fn lt(&self, other: &Self) -> bool {
        debug_assert_eq!(self.vtable().layout_id, other.vtable().layout_id);
        unsafe { (self.vtable().lt)(self.as_ptr(), other.as_ptr()) }
    }
}

impl Ord for Row {
    fn cmp(&self, other: &Self) -> Ordering {
        debug_assert_eq!(self.vtable().layout_id, other.vtable().layout_id);
        unsafe { (self.vtable().cmp)(self.as_ptr(), other.as_ptr()) }
    }
}

impl Hash for Row {
    fn hash<H: Hasher>(&self, state: &mut H) {
        unsafe { (self.vtable().hash)(&mut (state as &mut dyn Hasher), self.as_ptr()) }
    }
}

impl SizeOf for Row {
    fn size_of_children(&self, context: &mut size_of::Context) {
        if self.vtable().size_of != 0 {
            context.add_distinct_allocation().add(self.vtable().size_of);
            unsafe { (self.vtable().size_of_children)(self.as_ptr(), context) };
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
    fn decode<D>(_decoder: &mut D) -> Result<Self, DecodeError>
    where
        D: Decoder,
    {
        todo!()
    }
}

impl Default for Row {
    fn default() -> Self {
        unimplemented!()
    }
}

impl Clone for Row {
    fn clone(&self) -> Self {
        let mut data = UninitRow::new(self.inner.vtable);
        unsafe { (self.vtable().clone)(self.as_ptr(), data.as_mut_ptr()) };

        let clone = unsafe { data.assume_init() };
        debug_assert_eq!(self, clone);

        clone
    }
}

unsafe impl Send for Row {}

unsafe impl Sync for Row {}

impl Drop for Row {
    fn drop(&mut self) {
        // Drop the row's inner data
        if self.vtable().size_of != 0 {
            unsafe { (self.vtable().drop_in_place)(self.as_mut_ptr()) }
        }

        // UninitRow::drop() cleans up the allocated memory
    }
}

impl Debug for Row {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // If the alternative flag is passed, debug the raw bytes of the row
        if f.alternate() {
            #[derive(Debug)]
            #[allow(dead_code)]
            struct DebugRow<'a> {
                layout: &'a str,
                size: usize,
                align: usize,
                debug: String,
                // TODO: Hex output for the raw bytes
                bytes: &'a [u8],
                // TODO: Debug column types & offsets as well as bitsets
            }

            // This is really unsound since we're reading uninit memory but whatever
            let bytes = unsafe {
                // Make an asm black box to freeze the underlying memory
                let mut ptr = self.as_ptr();
                std::arch::asm!(
                    "/* {ptr} */",
                    ptr = inout(reg) ptr,
                    options(pure, nomem, preserves_flags, nostack),
                );

                slice::from_raw_parts(ptr, self.vtable().size_of)
            };

            let debug = DebugRow {
                layout: self.type_name(),
                size: self.vtable().size_of,
                align: self.vtable().align_of.get(),
                debug: format!("{:?}", self),
                bytes,
            };
            write!(f, "{debug:#?}",)

        // Otherwise debug normally
        } else if unsafe { (self.vtable().debug)(self.as_ptr(), f) } {
            Ok(())
        } else {
            Err(fmt::Error)
        }
    }
}

/// # Safety
///
/// TODO
pub unsafe fn row_from_literal(
    literal: &RowLiteral,
    vtable: &'static VTable,
    layout: &NativeLayout,
) -> Row {
    let mut row = UninitRow::new(vtable);

    for (idx, column) in literal.rows().iter().enumerate() {
        match column {
            NullableConstant::NonNull(constant) => unsafe {
                let column_ptr = row.as_mut_ptr().add(layout.offset_of(idx) as usize);
                write_constant_to(constant, column_ptr);
            },

            NullableConstant::Nullable(constant) => {
                row.set_column_null(idx, layout, constant.is_none());

                if let Some(constant) = constant {
                    unsafe {
                        let column_ptr = row.as_mut_ptr().add(layout.offset_of(idx) as usize);

                        write_constant_to(constant, column_ptr)
                    }
                }
            }
        }
    }

    unsafe { row.assume_init() }
}

unsafe fn write_constant_to(constant: &Constant, ptr: *mut u8) {
    match *constant {
        Constant::Unit => ptr.cast::<()>().write(()),

        Constant::U8(value) => ptr.cast::<u8>().write(value),
        Constant::I8(value) => ptr.cast::<i8>().write(value),

        Constant::U16(value) => ptr.cast::<u16>().write(value),
        Constant::I16(value) => ptr.cast::<i16>().write(value),

        Constant::U32(value) => ptr.cast::<u32>().write(value),
        Constant::I32(value) => ptr.cast::<i32>().write(value),

        Constant::U64(value) => ptr.cast::<u64>().write(value),
        Constant::I64(value) => ptr.cast::<i64>().write(value),

        Constant::Usize(value) => ptr.cast::<usize>().write(value),
        Constant::Isize(value) => ptr.cast::<isize>().write(value),

        Constant::F32(value) => ptr.cast::<f32>().write(value),
        Constant::F64(value) => ptr.cast::<f64>().write(value),

        Constant::Bool(value) => ptr.cast::<bool>().write(value),

        Constant::String(ref value) => ptr.cast::<ThinStr>().write(ThinStr::from(&**value)),
        // Constant::Date(date) => ptr.cast::<i32>().write(date),
        // Constant::Timestamp(timestamp) => ptr.cast::<i64>().write(timestamp),
    }
}
