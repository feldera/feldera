use crate::ir::{
    expr::Expr, function::Function, layout_cache::RowLayoutCache, types::Signature, DataflowNode,
    LayoutId, NodeId, Stream, StreamKind,
};
use serde::{Deserialize, Serialize};

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

    fn output_kind(&self, inputs: &[Stream]) -> Option<StreamKind> {
        Some(inputs[0].kind())
    }

    fn output_stream(&self, inputs: &[Stream]) -> Option<Stream> {
        Some(inputs[0])
    }

    fn signature(&self, _inputs: &[Stream], _layout_cache: &RowLayoutCache) -> Signature {
        todo!()
    }

    fn validate(&self, _inputs: &[Stream], _layout_cache: &RowLayoutCache) {
        todo!()
    }

    fn optimize(&mut self, _inputs: &[Stream], _layout_cache: &RowLayoutCache) {}

    fn layouts(&self, _layouts: &mut Vec<LayoutId>) {}
}

// TODO: Fully flesh this api out, init being an expr probably doesn't make since
// since usually we'll be folding a row, so we'd really want a row constructor
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Fold {
    input: NodeId,
    /// The initial value of the fold, should be the same layout as `acc_layout`
    #[allow(dead_code)]
    init: Expr,
    /// The step function, should have a signature of
    /// `fn(acc_layout, input_layout, weight_layout) -> acc_layout`
    step: Function,
    /// The finish function, should have a signature of
    /// `fn(acc_layout) -> output_layout`
    finish: Function,
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
        init: Expr,
        step: Function,
        finish: Function,
        acc_layout: LayoutId,
        step_layout: LayoutId,
        output_layout: LayoutId,
    ) -> Self {
        Self {
            input,
            init,
            step,
            finish,
            acc_layout,
            step_layout,
            output_layout,
        }
    }

    pub const fn step_fn(&self) -> &Function {
        &self.step
    }

    pub const fn finish_fn(&self) -> &Function {
        &self.finish
    }
}

impl DataflowNode for Fold {
    fn inputs(&self, inputs: &mut Vec<NodeId>) {
        inputs.push(self.input);
    }

    fn output_kind(&self, _inputs: &[Stream]) -> Option<StreamKind> {
        todo!()
    }

    fn output_stream(&self, _inputs: &[Stream]) -> Option<Stream> {
        todo!()
    }

    fn signature(&self, _inputs: &[Stream], _layout_cache: &RowLayoutCache) -> Signature {
        todo!()
    }

    fn validate(&self, _inputs: &[Stream], _layout_cache: &RowLayoutCache) {
        todo!()
    }

    fn optimize(&mut self, _inputs: &[Stream], layout_cache: &RowLayoutCache) {
        self.step.optimize(layout_cache);
        self.finish.optimize(layout_cache);
    }

    fn functions<'a>(&'a self, functions: &mut Vec<&'a Function>) {
        functions.extend([self.step_fn(), self.finish_fn()]);
    }

    fn layouts(&self, layouts: &mut Vec<LayoutId>) {
        layouts.extend([self.acc_layout, self.step_layout, self.output_layout]);
    }
}
