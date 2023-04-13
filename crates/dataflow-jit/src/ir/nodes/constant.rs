use crate::ir::{
    layout_cache::RowLayoutCache,
    literal::StreamLiteral,
    nodes::{DataflowNode, StreamKind, StreamLayout},
    LayoutId, NodeId,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize, JsonSchema)]
pub struct ConstantStream {
    value: StreamLiteral,
    layout: StreamLayout,
    #[serde(skip_deserializing)]
    consolidated: bool,
}

impl ConstantStream {
    pub const fn new(value: StreamLiteral, layout: StreamLayout) -> Self {
        Self {
            value,
            layout,
            consolidated: false,
        }
    }

    /// Create an empty stream
    pub const fn empty(layout: StreamLayout) -> Self {
        Self::new(
            match layout {
                StreamLayout::Set(_) => StreamLiteral::Set(Vec::new()),
                StreamLayout::Map(..) => StreamLiteral::Map(Vec::new()),
            },
            layout,
        )
    }

    pub const fn value(&self) -> &StreamLiteral {
        &self.value
    }

    pub fn value_mut(&mut self) -> &mut StreamLiteral {
        &mut self.value
    }

    pub const fn layout(&self) -> StreamLayout {
        self.layout
    }

    pub const fn consolidated(&self) -> bool {
        self.consolidated
    }

    pub fn consolidate(&mut self) {
        if !self.consolidated {
            let start_len = self.value.len();
            match &mut self.value {
                StreamLiteral::Set(set) => {
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

                StreamLiteral::Map(map) => {
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

            let removed = start_len - self.value.len();
            if removed != 0 {
                tracing::trace!("removed {removed} items from constant stream");
            }

            self.consolidated = true;
        }
    }
}

impl DataflowNode for ConstantStream {
    fn map_inputs<F>(&self, _map: &mut F)
    where
        F: FnMut(NodeId),
    {
    }

    fn map_inputs_mut<F>(&mut self, _map: &mut F)
    where
        F: FnMut(&mut NodeId),
    {
    }

    fn output_kind(&self, _inputs: &[StreamLayout]) -> Option<StreamKind> {
        Some(self.layout.kind())
    }

    fn output_stream(&self, _inputs: &[StreamLayout]) -> Option<StreamLayout> {
        Some(self.layout)
    }

    fn validate(&self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) {
        // TODO: Ensure that the data matches the node's layout
        // TODO: Ensure that the data's actually consolidated if `self.consolidated ==
        // true`
        todo!()
    }

    fn optimize(&mut self, _layout_cache: &RowLayoutCache) {
        self.consolidate();
    }

    fn map_layouts<F>(&self, map: &mut F)
    where
        F: FnMut(LayoutId),
    {
        self.layout.map_layouts(map);
    }

    fn remap_layouts(&mut self, mappings: &BTreeMap<LayoutId, LayoutId>) {
        self.layout.remap_layouts(mappings);
    }
}
