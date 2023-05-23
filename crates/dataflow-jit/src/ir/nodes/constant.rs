use crate::ir::{
    layout_cache::RowLayoutCache,
    literal::StreamLiteral,
    nodes::{DataflowNode, StreamLayout},
    LayoutId, NodeId,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize, JsonSchema)]
pub struct ConstantStream {
    value: StreamLiteral,
    layout: StreamLayout,
    #[serde(default)]
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
        Self::new(StreamLiteral::empty(layout), layout)
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
            self.value.consolidate();

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

    fn output_stream(&self, _inputs: &[StreamLayout]) -> Option<StreamLayout> {
        Some(self.layout)
    }

    fn validate(&self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) {
        // TODO: Ensure that the data matches the node's layout
        // TODO: Ensure that the data's actually consolidated if `self.consolidated ==
        // true`
        assert_eq!(self.layout, self.value.layout());
    }

    fn optimize(&mut self, _layout_cache: &RowLayoutCache) {
        self.consolidate();
    }

    fn map_layouts<F>(&self, map: &mut F)
    where
        F: FnMut(LayoutId),
    {
        self.layout.map_layouts(map);
        self.value.layout().map_layouts(map);
    }

    fn remap_layouts(&mut self, mappings: &BTreeMap<LayoutId, LayoutId>) {
        self.layout.remap_layouts(mappings);
        self.value.layout_mut().remap_layouts(mappings);
    }
}
