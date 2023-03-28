use crate::ir::Constant;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum StreamLiteral {
    Set(Vec<(RowLiteral, i32)>),
    Map(Vec<(RowLiteral, RowLiteral, i32)>),
}

impl StreamLiteral {
    pub fn len(&self) -> usize {
        match self {
            StreamLiteral::Set(set) => set.len(),
            StreamLiteral::Map(map) => map.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
pub enum NullableConstant {
    NonNull(Constant),
    Nullable(Option<Constant>),
}

impl NullableConstant {
    pub const fn null() -> Self {
        Self::Nullable(None)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
pub struct RowLiteral {
    rows: Vec<NullableConstant>,
}

impl RowLiteral {
    pub fn new(rows: Vec<NullableConstant>) -> Self {
        Self { rows }
    }

    pub fn rows(&self) -> &[NullableConstant] {
        &self.rows
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}
