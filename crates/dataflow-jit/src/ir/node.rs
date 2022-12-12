use crate::ir::{
    expr::Expr,
    function::{Function, InputFlags},
    layout_cache::LayoutCache,
    types::Signature,
    LayoutId, NodeId,
};
use enum_dispatch::enum_dispatch;

#[enum_dispatch]
pub trait DataflowNode {
    fn inputs(&self, inputs: &mut Vec<NodeId>);

    fn signature(&self, inputs: &[LayoutId], layout_cache: &LayoutCache) -> Signature;

    fn validate(&self, _inputs: &[LayoutId], _layout_cache: &LayoutCache);

    fn optimize(&mut self, inputs: &[LayoutId], layout_cache: &LayoutCache);
}

#[enum_dispatch(DataflowNode)]
#[derive(Debug)]
pub enum Node {
    Map(Map),
    Neg(Neg),
    Sum(Sum),
    Fold(Fold),
    Sink(Sink),
    Source(Source),
    Filter(Filter),
    IndexWith(IndexWith),
    Differentiate(Differentiate),
}

#[derive(Debug)]
pub struct Source {
    /// The type of the source stream
    layout: LayoutId,
}

impl Source {
    pub const fn new(layout: LayoutId) -> Self {
        Self { layout }
    }
}

impl DataflowNode for Source {
    fn inputs(&self, _inputs: &mut Vec<NodeId>) {}

    fn signature(&self, _inputs: &[LayoutId], _layout_cache: &LayoutCache) -> Signature {
        Signature::new(Vec::new(), Vec::new(), self.layout)
    }

    fn validate(&self, _inputs: &[LayoutId], _layout_cache: &LayoutCache) {}

    fn optimize(&mut self, _inputs: &[LayoutId], _layout_cache: &LayoutCache) {}
}

#[derive(Debug)]
pub struct Sink {
    input: NodeId,
}

impl DataflowNode for Sink {
    fn inputs(&self, inputs: &mut Vec<NodeId>) {
        inputs.push(self.input);
    }

    fn signature(&self, _inputs: &[LayoutId], _layout_cache: &LayoutCache) -> Signature {
        todo!()
    }

    fn validate(&self, _inputs: &[LayoutId], _layout_cache: &LayoutCache) {}

    fn optimize(&mut self, _inputs: &[LayoutId], _layout_cache: &LayoutCache) {}
}

#[derive(Debug)]
pub struct Map {
    input: NodeId,
    map: Function,
    layout: LayoutId,
}

impl Map {
    pub const fn new(input: NodeId, map: Function, layout: LayoutId) -> Self {
        Self { input, map, layout }
    }
}

impl DataflowNode for Map {
    fn inputs(&self, inputs: &mut Vec<NodeId>) {
        inputs.push(self.input);
    }

    fn signature(&self, _inputs: &[LayoutId], _layout_cache: &LayoutCache) -> Signature {
        todo!()
    }

    fn validate(&self, _inputs: &[LayoutId], _layout_cache: &LayoutCache) {
        todo!()
    }

    fn optimize(&mut self, _inputs: &[LayoutId], layout_cache: &LayoutCache) {
        self.map.optimize(layout_cache);
    }
}

#[derive(Debug)]
pub struct Filter {
    input: NodeId,
    filter: Function,
}

impl Filter {
    pub fn new(input: NodeId, filter: Function) -> Self {
        Self { input, filter }
    }
}

impl DataflowNode for Filter {
    fn inputs(&self, inputs: &mut Vec<NodeId>) {
        inputs.push(self.input);
    }

    fn signature(&self, _inputs: &[LayoutId], _layout_cache: &LayoutCache) -> Signature {
        todo!()
    }

    fn validate(&self, _inputs: &[LayoutId], _layout_cache: &LayoutCache) {
        todo!()
    }

    fn optimize(&mut self, _inputs: &[LayoutId], layout_cache: &LayoutCache) {
        self.filter.optimize(layout_cache);
    }
}

#[derive(Debug)]
pub struct IndexWith {
    input: NodeId,
    /// Expects a function with a signature of `fn(input_layout, mut key_layout, mut value_layout)`
    index: Function,
    key_layout: LayoutId,
    value_layout: LayoutId,
}

impl IndexWith {
    pub fn new(
        input: NodeId,
        index: Function,
        key_layout: LayoutId,
        value_layout: LayoutId,
    ) -> Self {
        Self {
            input,
            index,
            key_layout,
            value_layout,
        }
    }
}

impl DataflowNode for IndexWith {
    fn inputs(&self, inputs: &mut Vec<NodeId>) {
        inputs.push(self.input);
    }

    fn signature(&self, inputs: &[LayoutId], layout_cache: &LayoutCache) -> Signature {
        Signature::new(
            vec![inputs[0], self.key_layout, self.value_layout],
            vec![
                InputFlags::empty(),
                InputFlags::MUTABLE,
                InputFlags::MUTABLE,
            ],
            layout_cache.unit(),
        )
    }

    fn validate(&self, inputs: &[LayoutId], layout_cache: &LayoutCache) {
        assert_eq!(self.signature(inputs, layout_cache), self.index.signature());
    }

    fn optimize(&mut self, _inputs: &[LayoutId], layout_cache: &LayoutCache) {
        self.index.optimize(layout_cache);
    }
}

#[derive(Debug)]
pub struct Fold {
    input: NodeId,
    /// The initial value of the fold, should be the same layout as `acc_layout`
    init: Expr,
    /// The step function, should have a signature of
    /// `fn(acc_layout,input_layout, weight_layout) -> acc_layout`
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
}

impl DataflowNode for Fold {
    fn inputs(&self, inputs: &mut Vec<NodeId>) {
        inputs.push(self.input);
    }

    fn signature(&self, _inputs: &[LayoutId], _layout_cache: &LayoutCache) -> Signature {
        todo!()
    }

    fn validate(&self, _inputs: &[LayoutId], _layout_cache: &LayoutCache) {
        todo!()
    }

    fn optimize(&mut self, _inputs: &[LayoutId], layout_cache: &LayoutCache) {
        self.step.optimize(layout_cache);
        self.finish.optimize(layout_cache);
    }
}

#[derive(Debug)]
pub struct Neg {
    input: NodeId,
}

impl Neg {
    pub fn new(input: NodeId) -> Self {
        Self { input }
    }
}

impl DataflowNode for Neg {
    fn inputs(&self, inputs: &mut Vec<NodeId>) {
        inputs.push(self.input);
    }

    fn signature(&self, _inputs: &[LayoutId], _layout_cache: &LayoutCache) -> Signature {
        todo!()
    }

    fn validate(&self, _inputs: &[LayoutId], _layout_cache: &LayoutCache) {
        todo!()
    }

    fn optimize(&mut self, _inputs: &[LayoutId], _layout_cache: &LayoutCache) {}
}

#[derive(Debug)]
pub struct Differentiate {
    input: NodeId,
}

impl Differentiate {
    pub fn new(input: NodeId) -> Self {
        Self { input }
    }
}

impl DataflowNode for Differentiate {
    fn inputs(&self, inputs: &mut Vec<NodeId>) {
        inputs.push(self.input);
    }

    fn signature(&self, _inputs: &[LayoutId], _layout_cache: &LayoutCache) -> Signature {
        todo!()
    }

    fn validate(&self, _inputs: &[LayoutId], _layout_cache: &LayoutCache) {
        todo!()
    }

    fn optimize(&mut self, _inputs: &[LayoutId], _layout_cache: &LayoutCache) {}
}

#[derive(Debug)]
pub struct Sum {
    inputs: Vec<NodeId>,
}

impl Sum {
    pub fn new(inputs: Vec<NodeId>) -> Self {
        Self { inputs }
    }
}

impl DataflowNode for Sum {
    fn inputs(&self, inputs: &mut Vec<NodeId>) {
        inputs.extend(self.inputs.iter().copied());
    }

    fn signature(&self, _inputs: &[LayoutId], _layout_cache: &LayoutCache) -> Signature {
        todo!()
    }

    fn validate(&self, _inputs: &[LayoutId], _layout_cache: &LayoutCache) {
        todo!()
    }

    fn optimize(&mut self, _inputs: &[LayoutId], _layout_cache: &LayoutCache) {}
}
