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

/// Creates an index from an input set, using the `key_column`th column of the
/// input as the index's key and discarding all columns within `discarded_values`
/// in the values field (with the implicit addition of `key_column`, which will
/// never appear in the output value)
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
pub struct IndexByColumn {
    input: NodeId,
    input_layout: LayoutId,
    /// The key column
    key_column: usize,
    /// Values to be discarded
    discarded_values: Vec<usize>,
    key_layout: LayoutId,
    value_layout: LayoutId,
}

impl IndexByColumn {
    pub const fn new(
        input: NodeId,
        input_layout: LayoutId,
        key_column: usize,
        discarded_values: Vec<usize>,
        key_layout: LayoutId,
        value_layout: LayoutId,
    ) -> Self {
        Self {
            input,
            input_layout,
            key_column,
            discarded_values,
            key_layout,
            value_layout,
        }
    }

    pub const fn input(&self) -> NodeId {
        self.input
    }

    pub const fn input_layout(&self) -> LayoutId {
        self.input_layout
    }

    pub const fn key_column(&self) -> usize {
        self.key_column
    }

    pub fn discarded_values(&self) -> &[usize] {
        &self.discarded_values
    }

    pub const fn key_layout(&self) -> LayoutId {
        self.key_layout
    }

    pub const fn value_layout(&self) -> LayoutId {
        self.value_layout
    }
}

impl DataflowNode for IndexByColumn {
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

    fn validate(&self, inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) {
        assert_eq!(inputs.len(), 1);
        assert_eq!(inputs[0], StreamLayout::Set(self.input_layout));
    }

    fn optimize(&mut self, _layout_cache: &RowLayoutCache) {}

    fn functions<'a>(&'a self, _functions: &mut Vec<&'a Function>) {}

    fn functions_mut<'a>(&'a mut self, _functions: &mut Vec<&'a mut Function>) {}

    fn map_layouts<F>(&self, map: &mut F)
    where
        F: FnMut(LayoutId),
    {
        map(self.input_layout);
        map(self.key_layout);
        map(self.value_layout);
    }

    fn remap_layouts(&mut self, mappings: &BTreeMap<LayoutId, LayoutId>) {
        self.input_layout = mappings[&self.input_layout];
        self.key_layout = mappings[&self.key_layout];
        self.value_layout = mappings[&self.value_layout];
    }
}
