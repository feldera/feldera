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

    pub fn consolidate(&mut self) {
        self.value.consolidate();
    }

    pub fn to_consolidated(&self) -> Self {
        Self {
            layout: self.layout,
            value: self.value.to_consolidated(),
        }
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

    pub const fn empty(layout: StreamLayout) -> StreamCollection {
        match layout {
            StreamLayout::Set(_) => Self::Set(Vec::new()),
            StreamLayout::Map(..) => Self::Map(Vec::new()),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn consolidate(&mut self) {
        match self {
            Self::Set(set) => {
                // FIXME: We really should be sorting by the criteria that the
                // runtime rows will be sorted by so we have less work to do at
                // runtime, but technically any sorting criteria works as long
                // as it's consistent and allows us to deduplicate the stream
                set.sort_by(|(a, _), (b, _)| a.cmp(b));

                // Deduplicate rows and combine their weights
                set.dedup_by(|(a, weight_a), (b, weight_b)| {
                    if a == b {
                        *weight_b = weight_b
                            .checked_add(*weight_a)
                            .expect("weight overflow in constant stream");

                        true
                    } else {
                        false
                    }
                });

                // Remove all zero weights
                set.retain(|&(_, weight)| weight != 0);
            }

            Self::Map(map) => {
                // FIXME: We really should be sorting by the criteria that the
                // runtime rows will be sorted by so we have less work to do at
                // runtime, but technically any sorting criteria works as long
                // as it's consistent and allows us to deduplicate the stream
                map.sort_by(|(key_a, value_a, _), (key_b, value_b, _)| {
                    key_a.cmp(key_b).then_with(|| value_a.cmp(value_b))
                });

                // Deduplicate rows and combine their weights
                map.dedup_by(|(key_a, value_a, weight_a), (key_b, value_b, weight_b)| {
                    if key_a == key_b && value_a == value_b {
                        *weight_b = weight_b
                            .checked_add(*weight_a)
                            .expect("weight overflow in constant stream");

                        true
                    } else {
                        false
                    }
                });

                // Remove all zero weights
                map.retain(|&(_, _, weight)| weight != 0);
            }
        }
    }

    pub fn to_consolidated(&self) -> Self {
        let mut this = self.clone();
        this.consolidate();
        this
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
