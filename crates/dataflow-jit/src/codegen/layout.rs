use crate::ir::{RowLayout, RowType};
use cranelift::prelude::{isa::TargetFrontendConfig, types, Type as ClifType};
use std::cmp::{max, Reverse};

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum Type {
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

impl Type {
    pub fn native_type(&self, target: &TargetFrontendConfig) -> ClifType {
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

    fn size(&self, target: &TargetFrontendConfig) -> u32 {
        match self {
            Self::Ptr | Self::Usize => target.pointer_bytes() as u32,
            Self::U64 | Self::I64 | Self::F64 => 8,
            Self::U32 | Self::I32 | Self::F32 => 4,
            Self::U16 | Self::I16 => 2,
            Self::U8 | Self::I8 | Self::Bool => 1,
        }
    }

    fn align(&self, target: &TargetFrontendConfig) -> u32 {
        match self {
            Self::Ptr | Self::Usize => target.pointer_bytes() as u32,
            Self::U64 | Self::I64 | Self::F64 => 8,
            Self::U32 | Self::I32 | Self::F32 => 4,
            Self::U16 | Self::I16 => 2,
            Self::U8 | Self::I8 | Self::Bool => 1,
        }
    }

    fn bits(&self, target: &TargetFrontendConfig) -> u8 {
        match self {
            Self::Ptr | Self::Usize => target.pointer_bits(),
            Self::U64 | Self::I64 | Self::F64 => 64,
            Self::U32 | Self::I32 | Self::F32 => 32,
            Self::U16 | Self::I16 => 16,
            Self::U8 | Self::I8 | Self::Bool => 8,
        }
    }

    /// Returns `true` if the type is a [`U8`].
    ///
    /// [`U8`]: Type::U8
    #[must_use]
    pub const fn is_u8(&self) -> bool {
        matches!(self, Self::U8)
    }

    /// Creates a type from the given [`RowType`], returning `None`
    /// if it's a [`RowType::Unit`]
    const fn from_row_type(row_type: RowType) -> Option<Self> {
        Some(match row_type {
            RowType::Bool => Self::Bool,
            RowType::U16 => Self::U16,
            RowType::I16 => Self::I16,
            RowType::U32 => Self::U32,
            RowType::I32 => Self::I32,
            RowType::U64 => Self::U64,
            RowType::I64 => Self::I64,
            RowType::F32 => Self::F32,
            RowType::F64 => Self::F64,
            // Strings are represented as a pointer to a length-prefixed string (maybe???)
            RowType::String => Self::Ptr,
            RowType::Unit => return None,
        })
    }
}

#[derive(Clone)]
pub struct LayoutConfig {
    target: TargetFrontendConfig,
    /// If true, layouts will be optimized
    optimize_layouts: bool,
}

impl LayoutConfig {
    pub fn new(frontend: TargetFrontendConfig, optimize_layouts: bool) -> Self {
        Self {
            target: frontend,
            optimize_layouts,
        }
    }
}

#[derive(Debug)]
pub struct Layout {
    size: u32,
    align: u32,
    types: Vec<Type>,
    offsets: Vec<u32>,
    // A mapping from the source `RowLayout`'s indices to the real indices
    // we store values at
    index_mappings: Vec<u32>,
    // A mapping from the source `RowLayout`'s indices to the index of
    // the bitset and the bit offset containing its null-ness.
    // Will be `None` if the row isn't nullable
    bitflag_mappings: Vec<Option<(u32, u8)>>,
    is_unit: bool,
}

impl Layout {
    /// Returns the offset of the given row
    pub fn row_offset(&self, row: usize) -> u32 {
        self.offsets[self.index_mappings[row] as usize]
    }

    /// Returns the type of the given row
    pub fn row_type(&self, row: usize) -> Type {
        self.types[self.index_mappings[row] as usize]
    }

    /// Returns the offset, type and bit offset of the given row's nullability
    ///
    /// Panics if `row` isn't nullable
    pub fn row_nullability(&self, row: usize) -> (Type, u32, u8) {
        let (idx, bit) = self.bitflag_mappings[row].unwrap();
        (self.types[idx as usize], self.offsets[idx as usize], bit)
    }

    // FIXME: All unit types should be eliminated before this point
    // TODO: We need to do layout optimization here
    pub fn from_row(layout: &RowLayout, target: &TargetFrontendConfig) -> Self {
        let is_unit = layout.is_unit();

        // The number of bitflag niches we have to fill
        // FIXME: Instead of just splitting the number of required bitflags into
        // bytes up front, we should probably take advantage of differently sized
        // integers where possible, e.g. using a u32 where there's 4 bytes of padding
        // available
        // TODO: We could also take advantage of niches within types, e.g. the 7 bits
        // available within a bool
        let required_bitflags = layout.nullability().count_ones();
        let mut bitflags = bits_to_bitflags(required_bitflags);
        let mut bitflag_indices = Vec::with_capacity(bitflags.len());

        let mut offsets = Vec::with_capacity(layout.rows().len());
        let mut types = Vec::with_capacity(layout.rows().len());
        let mut index_mappings = Vec::with_capacity(layout.rows().len());

        let (mut index, mut size, mut align) = (0, 0, 1);

        for row in layout.rows() {
            let ty = match row {
                RowType::Bool => Type::Bool,
                RowType::U16 => Type::U16,
                RowType::I16 => Type::I16,
                RowType::U32 => Type::U32,
                RowType::I32 => Type::I32,
                RowType::U64 => Type::U64,
                RowType::I64 => Type::I64,
                RowType::F32 => Type::F32,
                RowType::F64 => Type::F64,

                // Strings are represented as a pointer to a length-prefixed string (maybe???)
                RowType::String => Type::Ptr,

                // Unit types are noops
                RowType::Unit => {
                    index_mappings.push(index);
                    continue;
                }
            };

            let field_align = ty.align(target);
            align = max(align, field_align);

            let mut required_padding = padding_needed_for(size, field_align);
            while required_padding != 0 {
                if let Some(flag) = bitflags.pop() {
                    debug_assert!(flag.is_u8());

                    bitflag_indices.push(index);
                    offsets.push(size);
                    types.push(flag);

                    size += 1;
                    required_padding -= 1;
                    align = max(align, 1);
                    index += 1;
                } else {
                    break;
                }
            }

            size += required_padding;
            offsets.push(size);
            size += ty.size(target);

            index_mappings.push(index);
            types.push(ty);

            index += 1;
        }

        for flag in bitflags {
            debug_assert!(flag.is_u8());

            bitflag_indices.push(index);
            offsets.push(size);
            types.push(flag);

            size += 1;
            align = max(align, 1);
            index += 1;
        }
        size += padding_needed_for(size, align);

        let mut bitflag_mappings = vec![None; layout.rows().len()];
        let (mut flag_idx, mut bit_idx) = (0, 0);
        for (idx, nullable) in layout.nullability().iter().by_vals().enumerate() {
            if !nullable {
                continue;
            }

            let flag_offset = bitflag_indices[flag_idx];
            let flag_bits = types[flag_offset as usize].bits(target);

            if bit_idx < flag_bits {
                bitflag_mappings[idx] = Some((bitflag_indices[flag_idx], bit_idx));
                bit_idx += 1;
            } else {
                flag_idx += 1;
                bit_idx = 0;
                bitflag_mappings[idx] = Some((bitflag_indices[flag_idx], 0));
            }
        }

        Self {
            size,
            align,
            types,
            offsets,
            index_mappings,
            bitflag_mappings,
            is_unit,
        }
    }

    // FIXME: This algorithm is better than the previous one, it just needs
    //        null flags to be implemented
    fn from_row2(layout: &RowLayout, config: &LayoutConfig) -> Self {
        debug_assert!(
            u32::try_from(layout.len()).is_ok(),
            "{} is out of bounds for a u32 (0..={})",
            layout.len(),
            u32::MAX,
        );
        let mut inverse_field_index: Vec<u32> = (0..layout.len() as u32).collect();

        if config.optimize_layouts {
            inverse_field_index.sort_by_key(|&x| {
                let row_type = layout.rows()[x as usize];

                // Computes `log2(effective_align)`, assumes that size is
                // an integer multiple of align (except for zsts)
                let effective_align = if let Some(ty) = Type::from_row_type(row_type) {
                    ty.align(&config.target).max(ty.size(&config.target))
                } else {
                    // Zsts have an alignment of 1
                    1
                }
                .trailing_zeros() as u64;

                // Currently we don't do any niching, although we could give
                // bools niches in the future
                let niches = 0u32;

                // Place zsts first so they don't do anything weird.
                // Place the largest alignments first, largest niches first
                // within any given alignment group
                (!row_type.is_unit(), Reverse((effective_align, niches)))
            });
        }

        let mut offsets = vec![0; layout.len()];
        let mut types = vec![Type::U8; layout.len()];
        let (mut align, mut offset) = (1, 0u32);

        for &idx in &inverse_field_index {
            let field = layout.rows()[idx as usize];
            let mut field_size = 0;

            if let Some(field_ty) = Type::from_row_type(field) {
                field_size = field_ty.size(&config.target);
                let field_align = field_ty.align(&config.target);

                offset = offset
                    .checked_add(padding_needed_for(offset, field_align))
                    .expect("layout overflowed u32::MAX");
                align = align.max(field_align);

                types[idx as usize] = field_ty;
            }

            offsets[idx as usize] = offset;

            offset = offset
                .checked_add(field_size)
                .expect("layout overflowed u32::MAX");
        }

        // Invert our bijective mapping into the in-memory order of the
        // fields
        let memory_index = if config.optimize_layouts {
            invert_mapping(&inverse_field_index)
        } else {
            inverse_field_index
        };

        let size = offset
            .checked_add(padding_needed_for(offset, align))
            .expect("layout overflowed u32::MAX");

        dbg!(memory_index, offsets, types, size, align);

        let is_unit = layout.rows().is_empty() || layout.rows().iter().all(RowType::is_unit);

        todo!()
    }

    pub fn is_unit(&self) -> bool {
        self.is_unit
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
}

fn invert_mapping(map: &[u32]) -> Vec<u32> {
    let mut inverse = vec![0; map.len()];
    for idx in 0..map.len() {
        inverse[map[idx] as usize] = idx as u32;
    }

    inverse
}

// We store bitflags as bytes which are dispersed throughout the struct,
// used to fill padding bytes or tacked onto the end of the struct
fn bits_to_bitflags(required_bitflags: usize) -> Vec<Type> {
    let bytes = (required_bitflags / 8) + (required_bitflags % 8 != 0) as usize;
    vec![Type::U8; bytes]
}

const fn padding_needed_for(size: u32, align: u32) -> u32 {
    let len_rounded_up = size.wrapping_add(align).wrapping_sub(1) & !align.wrapping_sub(1);
    len_rounded_up.wrapping_sub(size)
}

#[cfg(test)]
mod tests {
    use crate::{
        codegen::{layout::LayoutConfig, Layout},
        ir::{RowLayoutBuilder, RowType},
    };
    use cranelift::prelude::isa::{CallConv, TargetFrontendConfig};
    use target_lexicon::PointerWidth;

    const DEFAULT_TARGET: TargetFrontendConfig = TargetFrontendConfig {
        default_call_conv: CallConv::Fast,
        pointer_width: PointerWidth::U64,
    };

    #[test]
    fn layout_normalization() {
        let row = RowLayoutBuilder::new()
            .with_row(RowType::U32, false)
            .with_row(RowType::U16, false)
            .with_row(RowType::U64, false)
            .with_row(RowType::U16, false)
            .with_row(RowType::Unit, false)
            .build();

        let layout = Layout::from_row(&row, &DEFAULT_TARGET);
        dbg!(layout);

        Layout::from_row2(&row, &LayoutConfig::new(DEFAULT_TARGET, true));
    }
}
