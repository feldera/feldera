use crate::ir::{function::InputFlags, LayoutId};
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
    Unit,
    String,
}

impl ColumnType {
    /// Returns `true` if the row type is [`Unit`].
    ///
    /// [`Unit`]: RowType::Unit
    #[must_use]
    pub const fn is_unit(&self) -> bool {
        matches!(self, Self::Unit)
    }

    #[must_use]
    pub const fn is_string(&self) -> bool {
        matches!(self, Self::String)
    }

    pub const fn needs_drop(&self) -> bool {
        matches!(self, Self::String)
    }

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
            Self::Unit => "unit",
            Self::String => "str",
        }
    }
}

impl Display for ColumnType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.to_str())
    }
}

pub struct RowLayoutBuilder {
    rows: Vec<ColumnType>,
    nullability: BitVec,
}

impl RowLayoutBuilder {
    pub const fn new() -> Self {
        Self {
            rows: Vec::new(),
            nullability: BitVec::EMPTY,
        }
    }

    pub fn with_column(mut self, row_type: ColumnType, nullable: bool) -> Self {
        self.add_column(row_type, nullable);
        self
    }

    pub fn add_column(&mut self, row_type: ColumnType, nullable: bool) -> &mut Self {
        self.rows.push(row_type);
        self.nullability.push(nullable);
        self
    }

    pub fn build(self) -> RowLayout {
        debug_assert_eq!(self.rows.len(), self.nullability.len());

        RowLayout {
            columns: self.rows,
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

    pub fn get_row_type(&self, row: usize) -> Option<ColumnType> {
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
}
