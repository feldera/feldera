use crate::ir::ColumnType;
use bitvec::vec::BitVec;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Debug, Display, Write};

// TODO: Newtyping for column indices

/// The layout of a row
#[derive(Clone, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[serde(from = "SerRowLayout", into = "SerRowLayout")]
pub struct RowLayout {
    /// The type of each column within the row
    columns: Vec<ColumnType>,
    /// The nullability of each column within the current row, a `true` at index
    /// `n` means that `columns[n]` is nullable
    nullability: BitVec,
}

impl RowLayout {
    /// Returns the number of columns within the current row
    pub fn len(&self) -> usize {
        debug_assert_eq!(self.columns.len(), self.nullability.len());
        self.columns.len()
    }

    /// Returns `true` if the current row has zero columns
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn columns(&self) -> &[ColumnType] {
        &self.columns
    }

    pub fn column_type(&self, column: usize) -> ColumnType {
        self.columns[column]
    }

    pub fn try_column_type(&self, column: usize) -> Option<ColumnType> {
        self.columns.get(column).copied()
    }

    pub fn column_nullable(&self, column: usize) -> bool {
        self.nullability[column]
    }

    pub fn try_column_nullable(&self, column: usize) -> Option<bool> {
        self.nullability.get(column).map(|nullable| *nullable)
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

    /// Returns a row containing only a single unit column
    pub fn unit() -> Self {
        let mut nullability = BitVec::with_capacity(1);
        nullability.push(false);

        Self {
            columns: vec![ColumnType::Unit],
            nullability,
        }
    }

    // TODO: We probably want this to be configurable so that we can change the
    // weight type
    pub fn weight() -> Self {
        let mut nullability = BitVec::with_capacity(1);
        nullability.push(false);

        Self {
            columns: vec![ColumnType::I32],
            nullability,
        }
    }

    pub fn row_vector() -> RowLayout {
        let mut nullability = BitVec::with_capacity(2);
        nullability.extend([false; 2]);

        Self {
            columns: vec![ColumnType::Ptr, ColumnType::Ptr],
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

    /// Returns `true` if any of the columns within the current layout are
    /// nullable
    pub fn has_nullable_columns(&self) -> bool {
        self.nullability.any()
    }
}

impl Debug for RowLayout {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        struct DebugColumnLayout<'a>(&'a ColumnType, bool);

        impl Debug for DebugColumnLayout<'_> {
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
                    .map(|(column, nullable)| DebugColumnLayout(column, nullable)),
            )
            .finish()
    }
}

impl Display for RowLayout {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Debug::fmt(self, f)
    }
}

impl JsonSchema for RowLayout {
    fn schema_name() -> String {
        "RowLayout".to_owned()
    }

    fn json_schema(gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
        SerRowLayout::json_schema(gen)
    }
}

#[derive(Deserialize, Serialize, JsonSchema)]
struct SerRowLayout {
    columns: Vec<SerColumnLayout>,
}

impl From<RowLayout> for SerRowLayout {
    fn from(layout: RowLayout) -> Self {
        Self {
            columns: layout
                .iter()
                .map(|(ty, nullable)| SerColumnLayout { ty, nullable })
                .collect(),
        }
    }
}

impl From<SerRowLayout> for RowLayout {
    fn from(layout: SerRowLayout) -> Self {
        let mut columns = Vec::with_capacity(layout.columns.len());
        let mut nullability = BitVec::with_capacity(layout.columns.len());

        for SerColumnLayout { ty, nullable } in layout.columns {
            columns.push(ty);
            nullability.push(nullable);
        }

        Self {
            columns,
            nullability,
        }
    }
}

#[derive(Deserialize, Serialize, JsonSchema)]
struct SerColumnLayout {
    ty: ColumnType,
    nullable: bool,
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

    pub fn with_column(mut self, column_type: ColumnType, nullable: bool) -> Self {
        self.add_column(column_type, nullable);
        self
    }

    pub fn add_column(&mut self, column_type: ColumnType, nullable: bool) -> &mut Self {
        self.columns.push(column_type);
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
