use crate::ir::{
    function::Function, layout_cache::RowLayoutCache, literal::RowLiteral, types::Signature,
    DataflowNode, LayoutId, NodeId, StreamKind, StreamLayout,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Min {
    input: NodeId,
}

impl Min {
    pub const fn new(input: NodeId) -> Self {
        Self { input }
    }

    pub const fn input(&self) -> NodeId {
        self.input
    }
}

impl DataflowNode for Min {
    fn inputs(&self, inputs: &mut Vec<NodeId>) {
        inputs.push(self.input);
    }

    fn output_kind(&self, inputs: &[StreamLayout]) -> Option<StreamKind> {
        Some(inputs[0].kind())
    }

    fn output_stream(&self, inputs: &[StreamLayout]) -> Option<StreamLayout> {
        Some(inputs[0])
    }

    fn signature(&self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) -> Signature {
        todo!()
    }

    fn validate(&self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) {
        todo!()
    }

    fn optimize(&mut self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) {}

    fn layouts(&self, _layouts: &mut Vec<LayoutId>) {}

    fn remap_layouts(&mut self, _mappings: &BTreeMap<LayoutId, LayoutId>) {}
}

// TODO: Fully flesh this api out, init being an expr probably doesn't make
// since since usually we'll be folding a row, so we'd really want a row
// constructor
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Fold {
    input: NodeId,
    /// The initial value of the fold, should be the same layout as `acc_layout`
    init: RowLiteral,
    /// The step function, should have a signature of
    /// `fn(acc_layout, input_layout, weight_layout) -> acc_layout`
    step_fn: Function,
    /// The finish function, should have a signature of
    /// `fn(acc_layout) -> output_layout`
    finish_fn: Function,
    /// The layout of the accumulator value
    acc_layout: LayoutId,
    /// The layout of the step value
    step_layout: LayoutId,
    /// The layout of the output stream
    output_layout: LayoutId,
}

impl Fold {
    pub fn new(
        input: NodeId,
        init: RowLiteral,
        step_fn: Function,
        finish_fn: Function,
        acc_layout: LayoutId,
        step_layout: LayoutId,
        output_layout: LayoutId,
    ) -> Self {
        Self {
            input,
            init,
            step_fn,
            finish_fn,
            acc_layout,
            step_layout,
            output_layout,
        }
    }

    pub const fn init(&self) -> &RowLiteral {
        &self.init
    }

    pub const fn step_fn(&self) -> &Function {
        &self.step_fn
    }

    pub const fn finish_fn(&self) -> &Function {
        &self.finish_fn
    }

    pub const fn acc_layout(&self) -> LayoutId {
        self.acc_layout
    }

    pub const fn step_layout(&self) -> LayoutId {
        self.step_layout
    }

    pub const fn output_layout(&self) -> LayoutId {
        self.output_layout
    }
}

impl DataflowNode for Fold {
    fn inputs(&self, inputs: &mut Vec<NodeId>) {
        inputs.push(self.input);
    }

    fn output_kind(&self, _inputs: &[StreamLayout]) -> Option<StreamKind> {
        todo!()
    }

    fn output_stream(&self, _inputs: &[StreamLayout]) -> Option<StreamLayout> {
        todo!()
    }

    fn signature(&self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) -> Signature {
        todo!()
    }

    fn validate(&self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) {
        todo!()
    }

    fn optimize(&mut self, _inputs: &[StreamLayout], layout_cache: &RowLayoutCache) {
        self.step_fn.optimize(layout_cache);
        self.finish_fn.optimize(layout_cache);
    }

    fn functions<'a>(&'a self, functions: &mut Vec<&'a Function>) {
        functions.extend([self.step_fn(), self.finish_fn()]);
    }

    fn functions_mut<'a>(&'a mut self, functions: &mut Vec<&'a mut Function>) {
        functions.extend([&mut self.step_fn, &mut self.finish_fn]);
    }

    fn layouts(&self, layouts: &mut Vec<LayoutId>) {
        layouts.extend([self.acc_layout, self.step_layout, self.output_layout]);
    }

    fn remap_layouts(&mut self, mappings: &BTreeMap<LayoutId, LayoutId>) {
        self.acc_layout = mappings[&self.acc_layout];
        self.step_layout = mappings[&self.step_layout];
        self.output_layout = mappings[&self.output_layout];
        self.step_fn.remap_layouts(mappings);
        self.finish_fn.remap_layouts(mappings);
    }
}
