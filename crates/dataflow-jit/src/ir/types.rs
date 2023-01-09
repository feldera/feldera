use crate::ir::{function::InputFlags, LayoutId};
use bitvec::vec::BitVec;
use std::fmt::{self, Debug, Display, Write};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RowType {
    Bool,
    U16,
    U32,
    U64,
    I16,
    I32,
    I64,
    F32,
    F64,
    Unit,
    String,
}

impl RowType {
    /// Returns `true` if the row type is [`Unit`].
    ///
    /// [`Unit`]: RowType::Unit
    #[must_use]
    pub const fn is_unit(&self) -> bool {
        matches!(self, Self::Unit)
    }

    pub fn needs_drop(&self) -> bool {
        matches!(self, Self::String)
    }

    fn to_str(&self) -> &'static str {
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

impl Display for RowType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.to_str())
    }
}

pub struct RowLayoutBuilder {
    rows: Vec<RowType>,
    nullability: BitVec,
}

impl RowLayoutBuilder {
    pub const fn new() -> Self {
        Self {
            rows: Vec::new(),
            nullability: BitVec::EMPTY,
        }
    }

    pub fn with_row(mut self, row_type: RowType, nullable: bool) -> Self {
        self.add_row(row_type, nullable);
        self
    }

    pub fn add_row(&mut self, row_type: RowType, nullable: bool) -> &mut Self {
        self.rows.push(row_type);
        self.nullability.push(nullable);
        self
    }

    pub fn build(self) -> RowLayout {
        debug_assert_eq!(self.rows.len(), self.nullability.len());

        RowLayout {
            rows: self.rows,
            nullability: self.nullability,
        }
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct RowLayout {
    rows: Vec<RowType>,
    nullability: BitVec,
}

impl RowLayout {
    pub fn len(&self) -> usize {
        debug_assert_eq!(self.rows.len(), self.nullability.len());
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn rows(&self) -> &[RowType] {
        &self.rows
    }

    pub fn get_row_type(&self, row: usize) -> Option<RowType> {
        self.rows.get(row).copied()
    }

    pub fn row_is_nullable(&self, row: usize) -> bool {
        self.nullability[row]
    }

    pub fn nullability(&self) -> &BitVec {
        &self.nullability
    }

    pub fn is_unit(&self) -> bool {
        self.rows == [RowType::Unit] && self.nullability.not_any()
    }

    pub fn is_zero_sized(&self) -> bool {
        self.rows.iter().all(RowType::is_unit) && self.nullability.not_any()
    }

    pub fn needs_drop(&self) -> bool {
        self.rows.iter().any(RowType::needs_drop)
    }

    pub fn unit() -> Self {
        let mut nullability = BitVec::with_capacity(1);
        nullability.push(false);

        Self {
            rows: vec![RowType::Unit],
            nullability,
        }
    }

    pub fn weight() -> Self {
        let mut nullability = BitVec::with_capacity(1);
        nullability.push(false);

        Self {
            rows: vec![RowType::I32],
            nullability,
        }
    }
}

impl Debug for RowLayout {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        struct DebugRowLayout<'a>(&'a RowType, bool);
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
                self.rows
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
    ret: LayoutId,
}

impl Signature {
    pub fn new(args: Vec<LayoutId>, arg_flags: Vec<InputFlags>, ret: LayoutId) -> Self {
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

    pub fn ret(&self) -> LayoutId {
        self.ret
    }
}
