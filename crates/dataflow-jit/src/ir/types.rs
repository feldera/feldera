use crate::ir::{expr::InputFlags, layout_cache::LayoutId};
use bitvec::vec::BitVec;
use std::fmt::{self, Debug, Display, Write};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RowType {
    Bool,
    U32,
    U64,
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
            RowType::Bool => "bool",
            RowType::U32 => "u32",
            RowType::U64 => "u64",
            RowType::I32 => "i32",
            RowType::I64 => "i64",
            RowType::F32 => "f32",
            RowType::F64 => "f64",
            RowType::Unit => "unit",
            RowType::String => "str",
        }
    }
}

impl Display for RowType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.to_str())
    }
}

pub struct RowLayoutBuilder {
    rows: Vec<(RowType, bool)>,
}

impl RowLayoutBuilder {
    pub const fn new() -> Self {
        Self { rows: Vec::new() }
    }

    pub fn with_row(mut self, row_type: RowType, nullable: bool) -> Self {
        self.add_row(row_type, nullable);
        self
    }

    pub fn add_row(&mut self, row_type: RowType, nullable: bool) -> &mut Self {
        self.rows.push((row_type, nullable));
        self
    }

    pub fn build(mut self) -> RowLayout {
        let mut rows = Vec::with_capacity(self.rows.len());
        let mut nullability = BitVec::with_capacity(self.rows.len());

        for (row_type, nullable) in self.rows {
            rows.push(row_type);
            nullability.push(nullable);
        }

        RowLayout { rows, nullability }
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct RowLayout {
    rows: Vec<RowType>,
    nullability: BitVec,
}

impl RowLayout {
    pub fn rows(&self) -> &[RowType] {
        &self.rows
    }

    pub fn nullability(&self) -> &BitVec {
        &self.nullability
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

    pub fn needs_drop(&self) -> bool {
        self.rows.iter().any(RowType::needs_drop)
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
}
