use crate::ir::{
    function::Function, layout_cache::RowLayoutCache, types::Signature, ColumnType, DataflowNode,
    InputFlags, LayoutId, NodeId, StreamKind, StreamLayout,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Deserialize, Serialize)]
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
    fn inputs(&self, inputs: &mut Vec<NodeId>) {
        inputs.push(self.input);
    }

    fn output_kind(&self, _inputs: &[StreamLayout]) -> Option<StreamKind> {
        Some(StreamKind::Map)
    }

    fn output_stream(&self, _inputs: &[StreamLayout]) -> Option<StreamLayout> {
        Some(StreamLayout::Map(self.key_layout, self.value_layout))
    }

    fn signature(&self, inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) -> Signature {
        debug_assert_eq!(inputs.len(), 1);

        let (args, flags) = match inputs[0] {
            StreamLayout::Set(value) => (
                vec![value, self.key_layout, self.value_layout],
                vec![InputFlags::INPUT, InputFlags::OUTPUT, InputFlags::OUTPUT],
            ),
            StreamLayout::Map(key, value) => (
                vec![key, value, self.key_layout, self.value_layout],
                vec![
                    InputFlags::INPUT,
                    InputFlags::INPUT,
                    InputFlags::OUTPUT,
                    InputFlags::OUTPUT,
                ],
            ),
        };

        Signature::new(args, flags, ColumnType::Unit)
    }

    fn validate(&self, inputs: &[StreamLayout], layout_cache: &RowLayoutCache) {
        assert_eq!(
            self.signature(inputs, layout_cache),
            self.index_fn.signature()
        );
    }

    fn optimize(&mut self, _inputs: &[StreamLayout], layout_cache: &RowLayoutCache) {
        self.index_fn.optimize(layout_cache);
    }

    fn functions<'a>(&'a self, functions: &mut Vec<&'a Function>) {
        functions.push(&self.index_fn);
    }

    fn functions_mut<'a>(&'a mut self, functions: &mut Vec<&'a mut Function>) {
        functions.push(&mut self.index_fn);
    }

    fn layouts(&self, layouts: &mut Vec<LayoutId>) {
        layouts.extend([self.key_layout, self.value_layout]);
    }

    fn remap_layouts(&mut self, mappings: &BTreeMap<LayoutId, LayoutId>) {
        self.key_layout = mappings[&self.key_layout];
        self.value_layout = mappings[&self.value_layout];
        self.index_fn.remap_layouts(mappings);
    }
}
