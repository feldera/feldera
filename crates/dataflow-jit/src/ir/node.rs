use crate::ir::{
    expr::Expr,
    function::{Function, InputFlags},
    layout_cache::LayoutCache,
    types::Signature,
    ColumnType, LayoutId, NodeId,
};
use enum_dispatch::enum_dispatch;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum StreamKind {
    Set,
    Map,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Stream {
    Set(LayoutId),
    Map(LayoutId, LayoutId),
}

#[enum_dispatch]
pub trait DataflowNode {
    fn inputs(&self, inputs: &mut Vec<NodeId>);

    fn output_kind(&self, inputs: &[Stream]) -> Option<StreamKind>;

    fn output_stream(&self, inputs: &[Stream]) -> Option<Stream>;

    fn signature(&self, inputs: &[Stream], layout_cache: &LayoutCache) -> Signature;

    fn validate(&self, inputs: &[Stream], layout_cache: &LayoutCache);

    fn optimize(&mut self, inputs: &[Stream], layout_cache: &LayoutCache);
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
    /// The type of the source's produced stream
    layout: LayoutId,
}

impl Source {
    pub const fn new(layout: LayoutId) -> Self {
        Self { layout }
    }

    /// The type of the source's produced stream
    pub const fn layout(&self) -> LayoutId {
        self.layout
    }
}

impl DataflowNode for Source {
    fn inputs(&self, _inputs: &mut Vec<NodeId>) {}

    fn output_kind(&self, _inputs: &[Stream]) -> Option<StreamKind> {
        Some(StreamKind::Set)
    }

    fn output_stream(&self, _inputs: &[Stream]) -> Option<Stream> {
        Some(Stream::Set(self.layout))
    }

    fn signature(&self, _inputs: &[Stream], _layout_cache: &LayoutCache) -> Signature {
        todo!()
    }

    fn validate(&self, _inputs: &[Stream], _layout_cache: &LayoutCache) {}

    fn optimize(&mut self, _inputs: &[Stream], _layout_cache: &LayoutCache) {}
}

#[derive(Debug)]
pub struct Sink {
    input: NodeId,
}

impl Sink {
    pub fn new(input: NodeId) -> Self {
        Self { input }
    }

    pub const fn input(&self) -> NodeId {
        self.input
    }
}

impl DataflowNode for Sink {
    fn inputs(&self, inputs: &mut Vec<NodeId>) {
        inputs.push(self.input);
    }

    fn output_kind(&self, _inputs: &[Stream]) -> Option<StreamKind> {
        None
    }

    fn output_stream(&self, _inputs: &[Stream]) -> Option<Stream> {
        None
    }

    fn signature(&self, _inputs: &[Stream], _layout_cache: &LayoutCache) -> Signature {
        todo!()
    }

    fn validate(&self, _inputs: &[Stream], _layout_cache: &LayoutCache) {}

    fn optimize(&mut self, _inputs: &[Stream], _layout_cache: &LayoutCache) {}
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

    pub const fn input(&self) -> NodeId {
        self.input
    }

    pub const fn map_fn(&self) -> &Function {
        &self.map
    }

    pub const fn layout(&self) -> LayoutId {
        self.layout
    }
}

impl DataflowNode for Map {
    fn inputs(&self, inputs: &mut Vec<NodeId>) {
        inputs.push(self.input);
    }

    fn output_kind(&self, inputs: &[Stream]) -> Option<StreamKind> {
        Some(match inputs[0] {
            Stream::Set(_) => StreamKind::Set,
            Stream::Map(..) => StreamKind::Map,
        })
    }

    fn output_stream(&self, inputs: &[Stream]) -> Option<Stream> {
        Some(match inputs[0] {
            Stream::Set(_) => Stream::Set(self.layout),
            Stream::Map(key_layout, _) => Stream::Map(key_layout, self.layout),
        })
    }

    fn signature(&self, _inputs: &[Stream], _layout_cache: &LayoutCache) -> Signature {
        todo!()
    }

    fn validate(&self, _inputs: &[Stream], _layout_cache: &LayoutCache) {
        todo!()
    }

    fn optimize(&mut self, _inputs: &[Stream], layout_cache: &LayoutCache) {
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

    pub const fn input(&self) -> NodeId {
        self.input
    }

    pub const fn filter_fn(&self) -> &Function {
        &self.filter
    }
}

impl DataflowNode for Filter {
    fn inputs(&self, inputs: &mut Vec<NodeId>) {
        inputs.push(self.input);
    }

    fn output_kind(&self, inputs: &[Stream]) -> Option<StreamKind> {
        Some(match inputs[0] {
            Stream::Set(_) => StreamKind::Set,
            Stream::Map(..) => StreamKind::Map,
        })
    }

    fn output_stream(&self, inputs: &[Stream]) -> Option<Stream> {
        Some(match inputs[0] {
            Stream::Set(value) => Stream::Set(value),
            Stream::Map(key, value) => Stream::Map(key, value),
        })
    }

    fn signature(&self, _inputs: &[Stream], _layout_cache: &LayoutCache) -> Signature {
        todo!()
    }

    fn validate(&self, _inputs: &[Stream], _layout_cache: &LayoutCache) {
        todo!()
    }

    fn optimize(&mut self, _inputs: &[Stream], layout_cache: &LayoutCache) {
        self.filter.optimize(layout_cache);
    }
}

#[derive(Debug)]
pub struct IndexWith {
    input: NodeId,
    /// Expects a function with a signature of `fn(input_layout, mut key_layout,
    /// mut value_layout)`
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

    pub const fn input(&self) -> NodeId {
        self.input
    }

    pub const fn index_fn(&self) -> &Function {
        &self.index
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

    fn output_kind(&self, _inputs: &[Stream]) -> Option<StreamKind> {
        Some(StreamKind::Map)
    }

    fn output_stream(&self, _inputs: &[Stream]) -> Option<Stream> {
        Some(Stream::Map(self.key_layout, self.value_layout))
    }

    fn signature(&self, inputs: &[Stream], _layout_cache: &LayoutCache) -> Signature {
        debug_assert_eq!(inputs.len(), 1);

        let (args, flags) = match inputs[0] {
            Stream::Set(value) => (
                vec![value, self.key_layout, self.value_layout],
                vec![InputFlags::INPUT, InputFlags::OUTPUT, InputFlags::OUTPUT],
            ),
            Stream::Map(key, value) => (
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

    fn validate(&self, inputs: &[Stream], layout_cache: &LayoutCache) {
        assert_eq!(self.signature(inputs, layout_cache), self.index.signature());
    }

    fn optimize(&mut self, _inputs: &[Stream], layout_cache: &LayoutCache) {
        self.index.optimize(layout_cache);
    }
}

#[derive(Debug)]
pub struct Fold {
    input: NodeId,
    /// The initial value of the fold, should be the same layout as `acc_layout`
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

    fn signature(&self, _inputs: &[Stream], _layout_cache: &LayoutCache) -> Signature {
        todo!()
    }

    fn validate(&self, _inputs: &[Stream], _layout_cache: &LayoutCache) {
        todo!()
    }

    fn optimize(&mut self, _inputs: &[Stream], layout_cache: &LayoutCache) {
        self.step.optimize(layout_cache);
        self.finish.optimize(layout_cache);
    }
}

#[derive(Debug)]
pub struct Neg {
    input: NodeId,
    output_layout: LayoutId,
}

impl Neg {
    pub fn new(input: NodeId, output_layout: LayoutId) -> Self {
        Self {
            input,
            output_layout,
        }
    }

    pub const fn input(&self) -> NodeId {
        self.input
    }

    pub const fn output_layout(&self) -> LayoutId {
        self.output_layout
    }
}

impl DataflowNode for Neg {
    fn inputs(&self, inputs: &mut Vec<NodeId>) {
        inputs.push(self.input);
    }

    fn output_kind(&self, inputs: &[Stream]) -> Option<StreamKind> {
        Some(match inputs[0] {
            Stream::Set(_) => StreamKind::Set,
            Stream::Map(..) => StreamKind::Map,
        })
    }

    fn output_stream(&self, inputs: &[Stream]) -> Option<Stream> {
        Some(match inputs[0] {
            Stream::Set(value) => Stream::Set(value),
            Stream::Map(key, value) => Stream::Map(key, value),
        })
    }

    fn signature(&self, _inputs: &[Stream], _layout_cache: &LayoutCache) -> Signature {
        todo!()
    }

    fn validate(&self, _inputs: &[Stream], _layout_cache: &LayoutCache) {
        todo!()
    }

    fn optimize(&mut self, _inputs: &[Stream], _layout_cache: &LayoutCache) {}
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

    fn output_kind(&self, inputs: &[Stream]) -> Option<StreamKind> {
        Some(match inputs[0] {
            Stream::Set(_) => StreamKind::Set,
            Stream::Map(..) => StreamKind::Map,
        })
    }

    fn output_stream(&self, inputs: &[Stream]) -> Option<Stream> {
        Some(match inputs[0] {
            Stream::Set(value) => Stream::Set(value),
            Stream::Map(key, value) => Stream::Map(key, value),
        })
    }

    fn signature(&self, _inputs: &[Stream], _layout_cache: &LayoutCache) -> Signature {
        todo!()
    }

    fn validate(&self, _inputs: &[Stream], _layout_cache: &LayoutCache) {
        todo!()
    }

    fn optimize(&mut self, _inputs: &[Stream], _layout_cache: &LayoutCache) {}
}

#[derive(Debug)]
pub struct Sum {
    inputs: Vec<NodeId>,
}

impl Sum {
    pub fn new(inputs: Vec<NodeId>) -> Self {
        Self { inputs }
    }

    pub fn inputs(&self) -> &[NodeId] {
        &self.inputs
    }
}

impl DataflowNode for Sum {
    fn inputs(&self, inputs: &mut Vec<NodeId>) {
        inputs.extend(self.inputs.iter().copied());
    }

    fn output_kind(&self, inputs: &[Stream]) -> Option<StreamKind> {
        Some(match inputs[0] {
            Stream::Set(_) => StreamKind::Set,
            Stream::Map(..) => StreamKind::Map,
        })
    }

    fn output_stream(&self, inputs: &[Stream]) -> Option<Stream> {
        Some(match inputs[0] {
            Stream::Set(value) => Stream::Set(value),
            Stream::Map(key, value) => Stream::Map(key, value),
        })
    }

    fn signature(&self, _inputs: &[Stream], _layout_cache: &LayoutCache) -> Signature {
        todo!()
    }

    fn validate(&self, _inputs: &[Stream], _layout_cache: &LayoutCache) {
        todo!()
    }

    fn optimize(&mut self, _inputs: &[Stream], _layout_cache: &LayoutCache) {}
}
