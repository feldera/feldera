use crate::ir::{nodes::StreamLayout, Constant};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize, JsonSchema)]
pub struct StreamLiteral {
    /// The literal's layout
    layout: StreamLayout,
    /// The values within the stream
    value: StreamCollection,
}

impl StreamLiteral {
    /// Create a new stream literal
    pub const fn new(layout: StreamLayout, value: StreamCollection) -> Self {
        Self { layout, value }
    }

    /// Create an empty stream literal
    pub const fn empty(layout: StreamLayout) -> Self {
        let value = match layout {
            StreamLayout::Set(..) => StreamCollection::Set(Vec::new()),
            StreamLayout::Map(..) => StreamCollection::Map(Vec::new()),
        };

        Self { layout, value }
    }

    pub const fn layout(&self) -> StreamLayout {
        self.layout
    }

    pub const fn value(&self) -> &StreamCollection {
        &self.value
    }

    pub fn layout_mut(&mut self) -> &mut StreamLayout {
        &mut self.layout
    }

    pub fn value_mut(&mut self) -> &mut StreamCollection {
        &mut self.value
    }

    pub fn is_set(&self) -> bool {
        self.layout.is_set()
    }

    pub fn is_map(&self) -> bool {
        self.layout.is_map()
    }

    pub fn len(&self) -> usize {
        self.value.len()
    }

    pub fn is_empty(&self) -> bool {
        self.value.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize, JsonSchema)]
pub enum StreamCollection {
    Set(Vec<(RowLiteral, i32)>),
    Map(Vec<(RowLiteral, RowLiteral, i32)>),
}

impl StreamCollection {
    pub fn len(&self) -> usize {
        match self {
            Self::Set(set) => set.len(),
            Self::Map(map) => map.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize, JsonSchema)]
pub enum NullableConstant {
    NonNull(Constant),
    Nullable(Option<Constant>),
}

impl NullableConstant {
    pub const fn null() -> Self {
        Self::Nullable(None)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize, JsonSchema)]
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
