use crate::ir::{ColumnType, RowLayout};
use cranelift::prelude::{
    isa::{CallConv, TargetFrontendConfig},
    types, Type as ClifType,
};
use std::{
    alloc::Layout as StdLayout,
    error::Error,
    fmt::{self, Debug, Display},
    ptr::NonNull,
};
use target_lexicon::PointerWidth;
use tinyvec::TinyVec;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum NativeType {
    U8,
    I8,
    U16,
    I16,
    U32,
    I32,
    U64,
    I64,
    F32,
    F64,
    Ptr,
    Bool,
    Usize,
}

impl NativeType {
    pub(crate) fn native_type(self, target: &TargetFrontendConfig) -> ClifType {
        match self {
            Self::Ptr | Self::Usize => target.pointer_type(),
            Self::U64 | Self::I64 => types::I64,
            Self::U32 | Self::I32 => types::I32,
            Self::F64 => types::F64,
            Self::F32 => types::F32,
            Self::U16 | Self::I16 => types::I16,
            Self::U8 | Self::I8 | Self::Bool => types::I8,
        }
    }

    pub(crate) fn size(self, target: &TargetFrontendConfig) -> u32 {
        match self {
            Self::Ptr | Self::Usize => target.pointer_bytes() as u32,
            Self::U64 | Self::I64 | Self::F64 => 8,
            Self::U32 | Self::I32 | Self::F32 => 4,
            Self::U16 | Self::I16 => 2,
            Self::U8 | Self::I8 | Self::Bool => 1,
        }
    }

    pub(crate) fn align(self, target: &TargetFrontendConfig) -> u32 {
        match self {
            Self::Ptr | Self::Usize => target.pointer_bytes() as u32,
            Self::U64 | Self::I64 | Self::F64 => 8,
            Self::U32 | Self::I32 | Self::F32 => 4,
            Self::U16 | Self::I16 => 2,
            Self::U8 | Self::I8 | Self::Bool => 1,
        }
    }

    /// Computes `log2(effective_align)`
    pub(crate) fn effective_align(self, target: &TargetFrontendConfig) -> u32 {
        self.align(target).max(self.size(target)).trailing_zeros()
    }

    /// Creates a type from the given [`ColumnType`], returning `None`
    /// if it's a [`ColumnType::Unit`]
    #[must_use]
    pub const fn from_column_type(column_type: ColumnType) -> Option<Self> {
        column_type.native_type()
    }

    const fn to_str(self) -> &'static str {
        match self {
            Self::U8 => "u8",
            Self::I8 => "i8",
            Self::U16 => "u16",
            Self::I16 => "i16",
            Self::U32 => "u32",
            Self::I32 => "i32",
            Self::U64 => "u64",
            Self::I64 => "i64",
            Self::F32 => "f32",
            Self::F64 => "f64",
            Self::Ptr => "ptr",
            Self::Bool => "bool",
            Self::Usize => "usize",
        }
    }

    #[must_use]
    pub const fn is_u8(&self) -> bool {
        matches!(self, Self::U8)
    }

    #[must_use]
    pub const fn is_i8(&self) -> bool {
        matches!(self, Self::I8)
    }

    #[must_use]
    pub const fn is_u16(&self) -> bool {
        matches!(self, Self::U16)
    }

    #[must_use]
    pub const fn is_i16(&self) -> bool {
        matches!(self, Self::I16)
    }

    #[must_use]
    pub const fn is_u32(&self) -> bool {
        matches!(self, Self::U32)
    }

    #[must_use]
    pub const fn is_i32(&self) -> bool {
        matches!(self, Self::I32)
    }

    #[must_use]
    pub const fn is_u64(&self) -> bool {
        matches!(self, Self::U64)
    }

    #[must_use]
    pub const fn is_i64(&self) -> bool {
        matches!(self, Self::I64)
    }

    #[must_use]
    pub const fn is_f32(&self) -> bool {
        matches!(self, Self::F32)
    }

    #[must_use]
    pub const fn is_f64(&self) -> bool {
        matches!(self, Self::F64)
    }

    #[must_use]
    pub const fn is_ptr(&self) -> bool {
        matches!(self, Self::Ptr)
    }

    #[must_use]
    pub const fn is_bool(&self) -> bool {
        matches!(self, Self::Bool)
    }

    #[must_use]
    pub const fn is_usize(&self) -> bool {
        matches!(self, Self::Usize)
    }
}

impl From<BitSetType> for NativeType {
    fn from(bitset: BitSetType) -> Self {
        match bitset {
            BitSetType::U8 => Self::U8,
            BitSetType::U16 => Self::U16,
            BitSetType::U32 => Self::U32,
            BitSetType::U64 => Self::U64,
        }
    }
}

impl Display for NativeType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.to_str())
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum BitSetType {
    U8,
    U16,
    U32,
    U64,
}

impl BitSetType {
    pub fn native_type(self, target: &TargetFrontendConfig) -> ClifType {
        NativeType::from(self).native_type(target)
    }

    fn size(self, target: &TargetFrontendConfig) -> u32 {
        NativeType::from(self).size(target)
    }

    fn align(self, target: &TargetFrontendConfig) -> u32 {
        NativeType::from(self).align(target)
    }

    /// Computes `log2(effective_align)`
    fn effective_align(self, target: &TargetFrontendConfig) -> u32 {
        self.align(target).max(self.size(target)).trailing_zeros()
    }
}

impl TryFrom<NativeType> for BitSetType {
    type Error = InvalidBitsetType;

    fn try_from(ty: NativeType) -> Result<Self, Self::Error> {
        Ok(match ty {
            NativeType::U8 => Self::U8,
            NativeType::U16 => Self::U16,
            NativeType::U32 => Self::U32,
            NativeType::U64 => Self::U64,

            NativeType::I8
            | NativeType::I16
            | NativeType::I32
            | NativeType::I64
            | NativeType::F32
            | NativeType::F64
            | NativeType::Ptr
            | NativeType::Usize
            | NativeType::Bool => return Err(InvalidBitsetType),
        })
    }
}

#[derive(Debug, Clone)]
pub struct InvalidBitsetType;

impl Display for InvalidBitsetType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Invalid bitset type")
    }
}

impl Error for InvalidBitsetType {}

#[derive(Clone)]
pub struct LayoutConfig {
    target: TargetFrontendConfig,
    /// If true, layouts will be optimized
    optimize_layouts: bool,
}

impl Debug for LayoutConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LayoutConfig")
            .field("call_config", &self.target.default_call_conv)
            .field("pointer_width", &self.target.pointer_width)
            .field("optimize_layouts", &self.optimize_layouts)
            .finish()
    }
}

impl LayoutConfig {
    pub const fn new(target: TargetFrontendConfig, optimize_layouts: bool) -> Self {
        Self {
            target,
            optimize_layouts,
        }
    }
}

#[derive(Debug, Clone)]
pub struct NativeLayout {
    /// The total size of the layout
    size: u32,
    /// The alignment of the layout
    align: u32,
    /// The native type of each column's data
    /// For zsts this is meaningless
    types: Vec<NativeType>,
    /// The offset of each column's data
    /// For zsts this is meaningless
    offsets: Vec<u32>,
    /// The bitset associated with each column, will be `None` if the given
    /// column doesn't have a bitset. If the row has no nullable columns,
    /// this will be empty
    bitsets: Vec<Option<(BitSetType, u32, u8)>>,
    /// Each field of the layout (columns and bitsets) in the order they appear
    /// within the concrete layout
    memory_order: Vec<MemoryEntry>,
    /// The offsets of all padding bytes within the layout
    padding_bytes: Vec<u32>,
}

impl NativeLayout {
    /// Returns the offset and type of all columns within the current layout
    pub fn columns(&self) -> impl Iterator<Item = (u32, NativeType)> + '_ {
        self.offsets
            .iter()
            .zip(&self.types)
            .map(|(&offset, &ty)| (offset, ty))
    }

    /// Returns all constituent memory entries that make up the current layout
    pub fn memory_order(&self) -> &[MemoryEntry] {
        &self.memory_order
    }

    /// Returns `true` if the current layout has any padding bytes
    pub fn has_padding_bytes(&self) -> bool {
        !self.padding_bytes.is_empty()
    }

    /// Returns the offsets of all padding bytes
    pub fn padding_bytes(&self) -> &[u32] {
        &self.padding_bytes
    }

    /// Returns the offset of the given column
    pub fn offset_of(&self, column: usize) -> u32 {
        self.offsets[column]
    }

    /// Returns the type of the given column
    pub fn type_of(&self, column: usize) -> NativeType {
        self.types[column]
    }

    /// Returns `true` if the given column is nullable
    pub fn is_nullable(&self, column: usize) -> bool {
        !self.bitsets.is_empty() && self.bitsets[column].is_some()
    }

    /// Returns the offset, type and bit offset of the given column's
    /// nullability
    ///
    /// Panics if `column` isn't nullable
    pub fn nullability_of(&self, column: usize) -> (BitSetType, u32, u8) {
        self.bitsets[column].unwrap()
    }

    // TODO: Strings can use zero as their null value, this requires actual
    //       null-checking abstractions for writing code with though
    pub fn from_row(layout: &RowLayout, config: &LayoutConfig) -> Self {
        algorithm::compute_native_layout(layout, config)
    }

    pub fn size(&self) -> u32 {
        self.size
    }

    pub fn is_zero_sized(&self) -> bool {
        self.size == 0
    }

    pub fn align(&self) -> u32 {
        self.align
    }

    pub fn alloc(&self) -> Option<NonNull<u8>> {
        if self.is_zero_sized() {
            Some(NonNull::dangling())
        } else {
            let layout = self.rust_layout();
            NonNull::new(unsafe { std::alloc::alloc(layout) })
        }
    }

    /// Deallocates the given pointer with the current layout
    ///
    /// # Safety
    ///
    /// - `ptr` must denote a block of memory currently allocated via
    ///   [`NativeLayout::alloc()`]
    /// - The current layout must be the same layout that was used to allocate
    ///   that block of memory
    pub unsafe fn dealloc(&self, ptr: *mut u8) {
        debug_assert!(!ptr.is_null());

        if !self.is_zero_sized() {
            let layout = self.rust_layout();
            unsafe { std::alloc::dealloc(ptr, layout) }
        }
    }

    pub fn alloc_array(&self, length: usize) -> Option<NonNull<u8>> {
        if self.is_zero_sized() || length == 0 {
            Some(NonNull::dangling())
        } else {
            let single =
                StdLayout::from_size_align(self.size as usize, self.align as usize).unwrap();

            let mut layout = single;
            for _ in 1..length {
                layout = layout.extend(single).unwrap().0;
            }

            NonNull::new(unsafe { std::alloc::alloc(layout) })
        }
    }

    /// Deallocates the given pointer with an array of the current layout
    ///
    /// # Safety
    ///
    /// - `ptr` must denote a block of memory currently allocated via
    ///   [`NativeLayout::alloc_array()`]
    /// - The current layout must be the same layout that was used to allocate
    ///   that block of memory
    /// - `length` must be the same length that the pointer was allocated with
    pub unsafe fn dealloc_array(&self, ptr: *mut u8, length: usize) {
        debug_assert!(!ptr.is_null());

        if !self.is_zero_sized() && length != 0 {
            let single = self.rust_layout();

            // FIXME: Replace with `Layout::repeat()` via rust/#55724
            let mut layout = single;
            for _ in 1..length {
                layout = layout.extend(single).unwrap().0;
            }

            unsafe { std::alloc::dealloc(ptr, layout) }
        }
    }

    #[inline]
    pub fn rust_layout(&self) -> StdLayout {
        if cfg!(debug_assertions) {
            assert_ne!(self.align, 0);
            assert!(self.align.is_power_of_two());
            assert!(next_multiple_of(self.size, self.align) as isize <= isize::MAX);
        }

        unsafe { StdLayout::from_size_align_unchecked(self.size as usize, self.align as usize) }
    }
}

impl Display for NativeLayout {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        struct LayoutField {
            ty: NativeType,
            offset: u32,
            bitset: Option<(BitSetType, u32, u8)>,
        }

        impl Debug for LayoutField {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                let frontend = TargetFrontendConfig {
                    default_call_conv: CallConv::Fast,
                    pointer_width: if cfg!(target_pointer_width = "16") {
                        PointerWidth::U16
                    } else if cfg!(target_pointer_width = "32") {
                        PointerWidth::U32
                    } else if cfg!(target_pointer_width = "64") {
                        PointerWidth::U64
                    } else {
                        panic!("unsupported pointer width")
                    },
                };

                if let Some((bitset, bitset_offset, bitset_bit)) = self.bitset {
                    write!(
                        f,
                        "{} @ {}..{}, null @ bit {bitset_bit} of {bitset_offset}..{}",
                        self.ty.to_str(),
                        self.offset,
                        self.offset + self.ty.size(&frontend),
                        bitset_offset + bitset.size(&frontend),
                    )
                } else {
                    write!(
                        f,
                        "{} @ {}..{}",
                        self.ty.to_str(),
                        self.offset,
                        self.offset + self.ty.size(&frontend),
                    )
                }
            }
        }

        let mut debug = f.debug_tuple("NativeLayout");
        for ((offset, ty), &bitset) in self.columns().zip(&self.bitsets) {
            debug.field(&LayoutField { ty, offset, bitset });
        }

        // let mut bitsets: Vec<_> = self.bitsets.iter().copied().flatten().collect();
        // bitsets.sort_by_key(|&(_, offset, _)| offset);
        // bitsets.dedup_by_key(|&mut (_, offset, _)| offset);

        debug.finish()
    }
}

#[derive(Debug, Clone)]
pub enum MemoryEntry {
    /// The data associated with a column
    Column {
        /// The offset this column resides at
        offset: u32,
        /// The type of the column
        ty: NativeType,
        /// The column associated with this entry
        column: u32,
    },

    /// The data associated with a bitset
    BitSet {
        /// The offset this bitset resides at
        offset: u32,
        /// The type of this bitset
        ty: BitSetType,
        /// The columns which use this bitset to store nullability
        columns: TinyVec<[u32; 8]>,
    },

    /// Padding bytes
    Padding {
        /// The memory offset the padding bytes reside at
        offset: u32,
        /// The number of padding bytes this entry contains
        bytes: u8,
    },
}

impl MemoryEntry {
    /// Returns the offset this memory entry resides at
    pub const fn offset(&self) -> u32 {
        match *self {
            Self::Column { offset, .. }
            | Self::BitSet { offset, .. }
            | Self::Padding { offset, .. } => offset,
        }
    }

    /// Returns the number of bytes this memory entry occupies
    pub fn size(&self, target: &TargetFrontendConfig) -> u32 {
        match *self {
            Self::Column { ty, .. } => ty.size(target),
            Self::BitSet { ty, .. } => ty.size(target),
            Self::Padding { bytes, .. } => bytes as u32,
        }
    }
}

// TODO: Replace with `u32::next_multiple_of()` via rust/#88581
#[inline]
const fn next_multiple_of(lhs: u32, rhs: u32) -> u32 {
    match lhs % rhs {
        0 => lhs,
        rem => lhs + (rhs - rem),
    }
}

mod algorithm {
    use crate::{
        codegen::{
            layout::{next_multiple_of, LayoutConfig, MemoryEntry},
            BitSetType, NativeLayout, NativeType,
        },
        ir::RowLayout,
    };
    use std::cmp::Reverse;
    use tinyvec::TinyVec;

    enum Field {
        Column {
            column: u32,
            ty: Option<NativeType>,
        },
        BitSet {
            // TODO: Could use delta encoding on these
            columns: TinyVec<[u32; 8]>,
            ty: BitSetType,
        },
    }

    // TODO: Strings can use zero as their null value, this requires actual
    //       null-checking abstractions for writing code with though
    pub(super) fn compute_native_layout(layout: &RowLayout, config: &LayoutConfig) -> NativeLayout {
        // Ensure that the given layout has less than u32::MAX fields
        debug_assert!(
            layout.len() <= u32::MAX as usize,
            "a row layout with {} columns exceeds u32::MAX",
            layout.len(),
        );

        let null_columns = layout.total_null_columns();
        let mut fields = Vec::with_capacity(layout.len());
        fields.extend(
            layout
                .columns()
                .iter()
                .enumerate()
                .map(|(column, &ty)| Field::Column {
                    column: column as u32,
                    ty: NativeType::from_column_type(ty),
                }),
        );
        bitsets(
            null_columns,
            layout
                .nullability()
                .iter()
                .by_vals()
                .enumerate()
                .filter_map(|(column, nullable)| nullable.then_some(column as u32)),
            &mut fields,
        );

        if config.optimize_layouts {
            fields.sort_by_key(|field| {
                let (is_unit, effective_align, is_ptr) = match field {
                    // Zst columns
                    Field::Column { ty: None, .. } => (true, 1, false),

                    Field::Column { ty: Some(ty), .. } => {
                        (false, ty.effective_align(&config.target), ty.is_ptr())
                    }

                    Field::BitSet { ty, .. } => (false, ty.effective_align(&config.target), false),
                };

                // Place zsts first so they don't do anything weird.
                // Place the largest alignments first, pointers first
                // within their given alignment class. This is to hopefully
                // allow linearizing as much code as possible without having
                // pointers break up the cpu's branch prediction or our ability
                // to memcpy things
                (!is_unit, Reverse((effective_align, is_ptr)))
            });
        }

        let (mut align, mut offset) = (1, 0u32);
        let mut offsets = vec![0; layout.len()];
        let mut types = vec![NativeType::U8; layout.len()];
        // If the row doesn't have any nullable columns we use an empty bitsets vec as
        // an optimization
        let mut bitsets = if null_columns == 0 {
            Vec::new()
        } else {
            vec![None; layout.len()]
        };
        let mut memory_order = Vec::with_capacity(fields.len());
        let mut padding_bytes = Vec::new();

        for field in &fields {
            let field_ty = match *field {
                Field::Column { ty, .. } => ty,
                Field::BitSet { ty, .. } => Some(ty.into()),
            };

            let field_size = if let Some(field_ty) = field_ty {
                let field_align = field_ty.align(&config.target);

                let padding = padding_needed_for(offset, field_align);
                // Record all padding bytes
                padding_bytes.extend((0..padding).map(|i| offset + i));

                offset = offset
                    .checked_add(padding)
                    .expect("layout overflowed u32::MAX");
                align = align.max(field_align);

                match *field {
                    Field::Column { column, .. } => types[column as usize] = field_ty,

                    Field::BitSet { ref columns, ty } => {
                        debug_assert!(columns.len() <= u8::MAX as usize);
                        for (bit_idx, &column) in columns.iter().enumerate() {
                            bitsets[column as usize] = Some((ty, offset, bit_idx as u8));
                        }
                    }
                }

                field_ty.size(&config.target)

            // Only zsts should reach here, they're given a size of zero
            } else {
                0
            };

            match *field {
                Field::Column { column, ty } => {
                    offsets[column as usize] = offset;

                    if let Some(ty) = ty {
                        memory_order.push(MemoryEntry::Column { offset, ty, column });
                    }
                }

                Field::BitSet { ref columns, ty } => {
                    memory_order.push(MemoryEntry::BitSet {
                        offset,
                        ty,
                        columns: columns.clone(),
                    });
                }
            }

            offset = offset
                .checked_add(field_size)
                .expect("layout overflowed u32::MAX");
        }

        let size = offset
            .checked_add(padding_needed_for(offset, align))
            .expect("layout overflowed u32::MAX");

        // Ensure that the preconditions for
        // `std::alloc::Layout::from_size_align_unchecked()` are met
        assert_ne!(align, 0, "align must be non-zero");
        assert!(
            align.is_power_of_two(),
            "align must be a power of two ({align} is not a power of two)",
        );
        // TODO: Use `u32::next_multiple_of()` via rust/#88581
        assert!(next_multiple_of(size, align) as isize <= isize::MAX);

        NativeLayout {
            size,
            align,
            types,
            offsets,
            bitsets,
            memory_order,
            padding_bytes,
        }
    }

    // TODO: Should we allow configuring what types can be used for bitsets? Should
    // we delay type selection for bitsets until we know what padding we have
    // unused?
    fn bitsets<C>(mut total_null: usize, nullable_columns: C, bitsets: &mut Vec<Field>)
    where
        C: IntoIterator<Item = u32>,
    {
        let mut nullable_columns = nullable_columns.into_iter();

        if total_null == 0 {
            debug_assert!(nullable_columns.next().is_none());
            return;
        }

        // Create as many u64 bitsets as possible
        bitsets.reserve(total_null / 64);
        while let Some(total) = total_null.checked_sub(64) {
            total_null = total;

            let columns = nullable_columns.by_ref().take(64).collect();
            bitsets.push(Field::BitSet {
                columns,
                ty: BitSetType::U64,
            });
        }

        // Create as many u32 bitsets as possible
        bitsets.reserve(total_null / 32);
        while let Some(total) = total_null.checked_sub(32) {
            total_null = total;

            let columns = nullable_columns.by_ref().take(32).collect();
            bitsets.push(Field::BitSet {
                columns,
                ty: BitSetType::U32,
            });
        }

        // Create as many u16 bitsets as possible
        bitsets.reserve(total_null / 16);
        while let Some(total) = total_null.checked_sub(16) {
            total_null = total;

            let columns = nullable_columns.by_ref().take(16).collect();
            bitsets.push(Field::BitSet {
                columns,
                ty: BitSetType::U16,
            });
        }

        // Finish off any remaining null flags with u8s
        while total_null != 0 {
            total_null = total_null.saturating_sub(8);

            let columns = nullable_columns.by_ref().take(8).collect();
            bitsets.push(Field::BitSet {
                columns,
                ty: BitSetType::U8,
            });
        }

        debug_assert_eq!(total_null, 0);
        debug_assert!(nullable_columns.next().is_none());
    }

    #[inline]
    const fn padding_needed_for(size: u32, align: u32) -> u32 {
        debug_assert!(align.is_power_of_two());
        let len_rounded_up = size.wrapping_add(align).wrapping_sub(1) & !align.wrapping_sub(1);
        len_rounded_up.wrapping_sub(size)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        codegen::{layout::LayoutConfig, NativeLayout},
        ir::{ColumnType, RowLayoutBuilder},
    };
    use cranelift::prelude::isa::{CallConv, TargetFrontendConfig};
    use target_lexicon::PointerWidth;

    #[test]
    fn layout_normalization() {
        let config = LayoutConfig {
            target: TargetFrontendConfig {
                default_call_conv: CallConv::Fast,
                pointer_width: PointerWidth::U64,
            },
            optimize_layouts: true,
        };

        let row = RowLayoutBuilder::new()
            .with_column(ColumnType::U32, false)
            .with_column(ColumnType::U16, false)
            .with_column(ColumnType::U64, false)
            .with_column(ColumnType::U16, false)
            .with_column(ColumnType::Unit, false)
            .build();
        let layout = NativeLayout::from_row(&row, &config);
        println!("{layout}");

        let row = RowLayoutBuilder::new()
            .with_column(ColumnType::U32, true)
            .with_column(ColumnType::U16, false)
            .with_column(ColumnType::U64, false)
            .with_column(ColumnType::U16, false)
            .with_column(ColumnType::String, true)
            .build();
        let layout = NativeLayout::from_row(&row, &config);
        println!("{layout}");

        let mut builder = RowLayoutBuilder::new();
        for ty in [
            ColumnType::Bool,
            ColumnType::U16,
            ColumnType::U32,
            ColumnType::U64,
            ColumnType::I16,
            ColumnType::I32,
            ColumnType::I64,
            ColumnType::F32,
            ColumnType::F64,
            ColumnType::Unit,
            ColumnType::String,
        ] {
            builder.add_column(ty, false);
            builder.add_column(ty, true);
        }
        let row = builder.build();
        let layout = NativeLayout::from_row(&row, &config);
        println!("{layout:#?}\n{layout}");
    }
}
