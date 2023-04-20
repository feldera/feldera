use crate::ir::{
    function::Function,
    layout_cache::RowLayoutCache,
    nodes::{DataflowNode, StreamLayout},
    LayoutId, NodeId,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct IndexWith {
    input: NodeId,
    /// Expects a function with a signature of `fn(input_layout, mut key_layout,
    /// mut value_layout)`
    index_fn: Function,
    key_layout: LayoutId,
    value_layout: LayoutId,
}

impl IndexWith {
    pub fn new(
        input: NodeId,
        index_fn: Function,
        key_layout: LayoutId,
        value_layout: LayoutId,
    ) -> Self {
        Self {
            input,
            index_fn,
            key_layout,
            value_layout,
        }
    }

    pub const fn input(&self) -> NodeId {
        self.input
    }

    pub const fn index_fn(&self) -> &Function {
        &self.index_fn
    }

    pub const fn key_layout(&self) -> LayoutId {
        self.key_layout
    }

    pub const fn value_layout(&self) -> LayoutId {
        self.value_layout
    }
}

impl DataflowNode for IndexWith {
    fn map_inputs<F>(&self, map: &mut F)
    where
        F: FnMut(NodeId),
    {
        map(self.input);
    }

    fn map_inputs_mut<F>(&mut self, map: &mut F)
    where
        F: FnMut(&mut NodeId),
    {
        map(&mut self.input);
    }

    fn output_stream(&self, _inputs: &[StreamLayout]) -> Option<StreamLayout> {
        Some(StreamLayout::Map(self.key_layout, self.value_layout))
    }

    fn validate(&self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) {
        // TODO
    }

    fn optimize(&mut self, layout_cache: &RowLayoutCache) {
        self.index_fn.optimize(layout_cache);
    }

    fn functions<'a>(&'a self, functions: &mut Vec<&'a Function>) {
        functions.push(&self.index_fn);
    }

    fn functions_mut<'a>(&'a mut self, functions: &mut Vec<&'a mut Function>) {
        functions.push(&mut self.index_fn);
    }

    fn map_layouts<F>(&self, map: &mut F)
    where
        F: FnMut(LayoutId),
    {
        map(self.key_layout);
        map(self.value_layout);
        self.index_fn.map_layouts(map);
    }

    fn remap_layouts(&mut self, mappings: &BTreeMap<LayoutId, LayoutId>) {
        self.key_layout = mappings[&self.key_layout];
        self.value_layout = mappings[&self.value_layout];
        self.index_fn.remap_layouts(mappings);
    }
}
