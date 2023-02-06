use crate::{
    codegen::NativeType,
    ir::{function::InputFlags, LayoutId, RowLayoutCache},
};
use bitvec::vec::BitVec;
use std::fmt::{self, Debug, Display, Write};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ColumnType {
    Bool,
    U16,
    I16,
    U32,
    I32,
    U64,
    I64,
    F32,
    F64,
    /// Represents the days since Jan 1 1970 as an `i32`
    Date,
    Unit,
    String,
    /// Represents the milliseconds since Jan 1 1970 as an `i64`
    Timestamp,
}

impl ColumnType {
    #[must_use]
    pub const fn is_int(self) -> bool {
        matches!(
            self,
            Self::U16 | Self::I16 | Self::U32 | Self::I32 | Self::U64 | Self::I64,
        )
    }

    #[must_use]
    pub const fn is_signed_int(self) -> bool {
        matches!(self, Self::I16 | Self::I32 | Self::I64)
    }

    #[must_use]
    pub const fn is_unsigned_int(self) -> bool {
        matches!(self, Self::U16 | Self::U32 | Self::U64)
    }

    #[must_use]
    pub const fn is_float(self) -> bool {
        matches!(self, Self::F32 | Self::F64)
    }

    #[must_use]
    pub const fn needs_drop(&self) -> bool {
        matches!(self, Self::String)
    }

    #[must_use]
    pub const fn requires_nontrivial_clone(&self) -> bool {
        matches!(self, Self::String)
    }

    const fn to_str(self) -> &'static str {
        match self {
            Self::Bool => "bool",
            Self::U16 => "u16",
            Self::U32 => "u32",
            Self::U64 => "u64",
            Self::I16 => "i16",
            Self::I32 => "i32",
            Self::I64 => "i64",
            Self::F32 => "f32",
            Self::F64 => "f64",
            Self::Date => "date",
            Self::Unit => "unit",
            Self::String => "str",
            Self::Timestamp => "timestamp",
        }
    }

    #[must_use]
    pub const fn native_type(self) -> Option<NativeType> {
        Some(match self {
            Self::Bool => NativeType::Bool,
            Self::U16 => NativeType::U16,
            Self::I16 => NativeType::I16,
            Self::U32 => NativeType::U32,
            Self::I32 => NativeType::I32,
            Self::U64 => NativeType::U64,
            Self::I64 => NativeType::I64,
            Self::F32 => NativeType::F32,
            Self::F64 => NativeType::F64,
            Self::Date => NativeType::I32,
            // Strings are represented as a pointer to a length-prefixed string (maybe???)
            Self::String => NativeType::Ptr,
            Self::Timestamp => NativeType::I64,
            Self::Unit => return None,
        })
    }

    #[must_use]
    pub const fn is_bool(&self) -> bool {
        matches!(self, Self::Bool)
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
    pub const fn is_date(&self) -> bool {
        matches!(self, Self::Date)
    }

    #[must_use]
    pub const fn is_unit(&self) -> bool {
        matches!(self, Self::Unit)
    }

    #[must_use]
    pub const fn is_string(&self) -> bool {
        matches!(self, Self::String)
    }

    #[must_use]
    pub const fn is_timestamp(&self) -> bool {
        matches!(self, Self::Timestamp)
    }
}

impl Display for ColumnType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.to_str())
    }
}

pub struct RowLayoutBuilder {
    columns: Vec<ColumnType>,
    nullability: BitVec,
}

impl RowLayoutBuilder {
    pub const fn new() -> Self {
        Self {
            columns: Vec::new(),
            nullability: BitVec::EMPTY,
        }
    }

    pub fn with_column(mut self, row_type: ColumnType, nullable: bool) -> Self {
        self.add_column(row_type, nullable);
        self
    }

    pub fn add_column(&mut self, row_type: ColumnType, nullable: bool) -> &mut Self {
        self.columns.push(row_type);
        self.nullability.push(nullable);
        self
    }

    pub fn build(self) -> RowLayout {
        debug_assert_eq!(self.columns.len(), self.nullability.len());

        RowLayout {
            columns: self.columns,
            nullability: self.nullability,
        }
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct RowLayout {
    columns: Vec<ColumnType>,
    nullability: BitVec,
}

impl RowLayout {
    pub fn len(&self) -> usize {
        debug_assert_eq!(self.columns.len(), self.nullability.len());
        self.columns.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn columns(&self) -> &[ColumnType] {
        &self.columns
    }

    pub fn column_type(&self, row: usize) -> ColumnType {
        self.columns[row]
    }

    pub fn try_column_type(&self, row: usize) -> Option<ColumnType> {
        self.columns.get(row).copied()
    }

    pub fn column_nullable(&self, row: usize) -> bool {
        self.nullability[row]
    }

    pub fn nullability(&self) -> &BitVec {
        &self.nullability
    }

    pub fn is_unit(&self) -> bool {
        self.columns == [ColumnType::Unit] && self.nullability.not_any()
    }

    pub fn is_zero_sized(&self) -> bool {
        self.columns.is_empty()
            || (self.columns.iter().all(ColumnType::is_unit) && self.nullability.not_any())
    }

    pub fn unit() -> Self {
        let mut nullability = BitVec::with_capacity(1);
        nullability.push(false);

        Self {
            columns: vec![ColumnType::Unit],
            nullability,
        }
    }

    // TODO: We probably want this to be configurable so that we can change the weight type
    pub fn weight() -> Self {
        let mut nullability = BitVec::with_capacity(1);
        nullability.push(false);

        Self {
            columns: vec![ColumnType::I32],
            nullability,
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = (ColumnType, bool)> + '_ {
        assert_eq!(self.columns.len(), self.nullability.len());
        self.columns
            .iter()
            .copied()
            .zip(self.nullability.iter().by_vals())
    }

    /// Returns `true` if the current row requires any sort of non-trivial
    /// drop operation, e.g. containing a string
    pub fn needs_drop(&self) -> bool {
        self.columns.iter().any(ColumnType::needs_drop)
    }

    /// Returns `true` if the current row requires any sort of non-trivial
    /// cloning operation, e.g. containing a string
    pub fn requires_nontrivial_clone(&self) -> bool {
        self.columns
            .iter()
            .any(ColumnType::requires_nontrivial_clone)
    }

    /// Return the number of columns that are null
    pub fn total_null_columns(&self) -> usize {
        self.nullability.count_ones()
    }
}

impl Debug for RowLayout {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        struct DebugRowLayout<'a>(&'a ColumnType, bool);
        impl Debug for DebugRowLayout<'_> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                let Self(row, nullable) = *self;
                if nullable {
                    f.write_char('?')?;
                }

                f.write_str(row.to_str())
            }
        }

        f.debug_set()
            .entries(
                self.columns
                    .iter()
                    .zip(self.nullability.iter().by_vals())
                    .map(|(row, nullable)| DebugRowLayout(row, nullable)),
            )
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Signature {
    args: Vec<LayoutId>,
    arg_flags: Vec<InputFlags>,
    ret: ColumnType,
}

impl Signature {
    pub fn new(args: Vec<LayoutId>, arg_flags: Vec<InputFlags>, ret: ColumnType) -> Self {
        Self {
            args,
            arg_flags,
            ret,
        }
    }

    pub fn args(&self) -> &[LayoutId] {
        &self.args
    }

    pub fn arg_flags(&self) -> &[InputFlags] {
        &self.arg_flags
    }

    pub fn ret(&self) -> ColumnType {
        self.ret
    }

    pub(crate) fn display<'a>(&'a self, layout_cache: &'a RowLayoutCache) -> impl Display + 'a {
        struct DisplaySig<'a>(&'a Signature, &'a RowLayoutCache);

        impl Display for DisplaySig<'_> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("fn(")?;
                for (idx, (&layout_id, &flags)) in
                    self.0.args.iter().zip(&self.0.arg_flags).enumerate()
                {
                    let mut has_prefix = false;
                    if flags.contains(InputFlags::INPUT) {
                        f.write_str("in")?;
                        has_prefix = true;
                    }
                    if flags.contains(InputFlags::OUTPUT) {
                        f.write_str("out")?;
                        has_prefix = true;
                    }
                    if has_prefix {
                        f.write_char(' ')?;
                    }

                    let layout = self.1.get(layout_id);
                    write!(f, "{layout:?}")?;

                    if idx != self.0.args.len() - 1 {
                        f.write_str(", ")?;
                    }
                }
                f.write_char(')')?;

                if self.0.ret != ColumnType::Unit {
                    f.write_str(" -> ")?;
                    write!(f, "{}", self.0.ret)?;
                }

                Ok(())
            }
        }

        DisplaySig(self, layout_cache)
    }
}
