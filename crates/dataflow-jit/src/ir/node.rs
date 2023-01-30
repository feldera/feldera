use crate::ir::{
    expr::Expr,
    function::{Function, InputFlags},
    graph::{self, GraphExt},
    layout_cache::LayoutCache,
    types::Signature,
    ColumnType, LayoutId, NodeId,
};
use derive_more::{IsVariant, Unwrap};
use enum_dispatch::enum_dispatch;
use petgraph::prelude::DiGraphMap;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, IsVariant)]
pub enum StreamKind {
    Set,
    Map,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, IsVariant, Unwrap)]
pub enum Stream {
    Set(LayoutId),
    Map(LayoutId, LayoutId),
}

impl Stream {
    pub const fn kind(self) -> StreamKind {
        match self {
            Self::Set(_) => StreamKind::Set,
            Self::Map(_, _) => StreamKind::Map,
        }
    }
}

#[enum_dispatch]
pub trait DataflowNode {
    fn inputs(&self, inputs: &mut Vec<NodeId>);

    fn output_kind(&self, inputs: &[Stream]) -> Option<StreamKind>;

    fn output_stream(&self, inputs: &[Stream]) -> Option<Stream>;

    fn signature(&self, inputs: &[Stream], layout_cache: &LayoutCache) -> Signature;

    fn validate(&self, inputs: &[Stream], layout_cache: &LayoutCache);

    fn optimize(&mut self, inputs: &[Stream], layout_cache: &LayoutCache);

    fn functions<'a>(&'a self, _functions: &mut Vec<&'a Function>) {}

    fn layouts(&self, layouts: &mut Vec<LayoutId>);
}

#[enum_dispatch(DataflowNode)]
#[derive(Debug, Clone, IsVariant, Unwrap)]
pub enum Node {
    Map(Map),
    Neg(Neg),
    Sum(Sum),
    Fold(Fold),
    Sink(Sink),
    Source(Source),
    SourceMap(SourceMap),
    Filter(Filter),
    IndexWith(IndexWith),
    Differentiate(Differentiate),
    Delta0(Delta0),
    DelayedFeedback(DelayedFeedback),
    Min(Min),
    Distinct(Distinct),
    JoinCore(JoinCore),
    Subgraph(Subgraph),
    Export(Export),
    ExportedNode(ExportedNode),
}

#[derive(Debug, Clone)]
pub struct Subgraph {
    // A map of external nodes to their subgraph import nodes
    inputs: BTreeMap<NodeId, NodeId>,
    subgraph: graph::Subgraph,
    // A map of subgraph nodes to their export nodes
    outputs: BTreeMap<NodeId, NodeId>,
    feedback: BTreeSet<NodeId>,
    // Connections from streams to a feedback node within `feedback`
    feedback_connections: BTreeMap<NodeId, NodeId>,
}

impl Subgraph {
    pub fn new(subgraph: graph::Subgraph) -> Self {
        Self {
            inputs: BTreeMap::new(),
            subgraph,
            outputs: BTreeMap::new(),
            feedback: BTreeSet::new(),
            feedback_connections: BTreeMap::new(),
        }
    }

    pub fn subgraph(&self) -> &graph::Subgraph {
        &self.subgraph
    }

    pub fn delta0(&mut self, import: NodeId) -> NodeId {
        let delta0 = self.add_node(Delta0::new(import));
        self.inputs.insert(import, delta0);
        delta0
    }

    pub fn export(&mut self, output: NodeId, layout: Stream) -> NodeId {
        let export = self.add_node(Export::new(output, layout));
        self.outputs.insert(output, export);
        export
    }

    pub fn delayed_feedback(&mut self, layout: LayoutId) -> NodeId {
        let delay = self.add_node(DelayedFeedback::new(layout));
        self.feedback.insert(delay);
        delay
    }

    pub fn connect_feedback(&mut self, from: NodeId, feedback: NodeId) {
        debug_assert!(self.feedback.contains(&feedback));
        self.feedback_connections.insert(from, feedback);
    }

    pub(crate) fn input_nodes(&self) -> &BTreeMap<NodeId, NodeId> {
        &self.inputs
    }

    pub(crate) fn output_nodes(&self) -> &BTreeMap<NodeId, NodeId> {
        &self.outputs
    }

    pub(crate) fn feedback_connections(&self) -> &BTreeMap<NodeId, NodeId> {
        &self.feedback_connections
    }
}

impl GraphExt for Subgraph {
    fn layout_cache(&self) -> &LayoutCache {
        self.subgraph.layout_cache()
    }

    fn nodes(&self) -> &BTreeMap<NodeId, Node> {
        self.subgraph.nodes()
    }

    fn nodes_mut(&mut self) -> &mut BTreeMap<NodeId, Node> {
        self.subgraph.nodes_mut()
    }

    fn next_node(&self) -> NodeId {
        self.subgraph.next_node()
    }

    fn create_node<N>(&mut self, node_id: NodeId, node: N) -> NodeId
    where
        N: Into<Node>,
    {
        self.subgraph.create_node(node_id, node)
    }

    fn subgraph<F, T>(&mut self, build: F) -> (NodeId, T)
    where
        F: FnOnce(&mut Subgraph) -> T,
    {
        self.subgraph.subgraph(build)
    }

    fn optimize(&mut self) {
        self.subgraph.optimize();
    }

    fn edges(&self) -> &DiGraphMap<NodeId, ()> {
        self.subgraph.edges()
    }
}

impl DataflowNode for Subgraph {
    fn inputs(&self, inputs: &mut Vec<NodeId>) {
        inputs.extend(self.inputs.keys().copied());
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

    fn functions<'a>(&'a self, functions: &mut Vec<&'a Function>) {
        self.subgraph.functions(functions);
    }

    fn layouts(&self, layouts: &mut Vec<LayoutId>) {
        self.subgraph.layouts(layouts);
    }
}

#[derive(Debug, Clone)]
pub struct JoinCore {
    lhs: NodeId,
    rhs: NodeId,
    // fn(key, lhs_val, rhs_val, key_out, val_out)
    join_fn: Function,
    key_layout: LayoutId,
    value_layout: LayoutId,
    output_kind: StreamKind,
}

impl JoinCore {
    pub const fn new(
        lhs: NodeId,
        rhs: NodeId,
        join_fn: Function,
        key_layout: LayoutId,
        value_layout: LayoutId,
        output_kind: StreamKind,
    ) -> Self {
        Self {
            lhs,
            rhs,
            join_fn,
            key_layout,
            value_layout,
            output_kind,
        }
    }

    pub const fn lhs(&self) -> NodeId {
        self.lhs
    }

    pub const fn rhs(&self) -> NodeId {
        self.rhs
    }

    pub const fn join_fn(&self) -> &Function {
        &self.join_fn
    }

    pub const fn key_layout(&self) -> LayoutId {
        self.key_layout
    }

    pub const fn value_layout(&self) -> LayoutId {
        self.value_layout
    }

    pub(crate) fn result_kind(&self) -> StreamKind {
        self.output_kind
    }
}

impl DataflowNode for JoinCore {
    fn inputs(&self, inputs: &mut Vec<NodeId>) {
        inputs.extend([self.lhs, self.rhs]);
    }

    fn output_kind(&self, _inputs: &[Stream]) -> Option<StreamKind> {
        Some(self.output_kind)
    }

    fn output_stream(&self, _inputs: &[Stream]) -> Option<Stream> {
        Some(match self.output_kind {
            StreamKind::Set => Stream::Set(self.key_layout),
            StreamKind::Map => Stream::Map(self.key_layout, self.value_layout),
        })
    }

    fn signature(&self, _inputs: &[Stream], _layout_cache: &LayoutCache) -> Signature {
        todo!()
    }

    fn validate(&self, _inputs: &[Stream], layout_cache: &LayoutCache) {
        if self.output_kind.is_set() {
            assert_eq!(self.value_layout, layout_cache.unit());
        }
    }

    fn optimize(&mut self, _inputs: &[Stream], _layout_cache: &LayoutCache) {}

    fn functions<'a>(&'a self, functions: &mut Vec<&'a Function>) {
        functions.push(&self.join_fn);
    }

    fn layouts(&self, layouts: &mut Vec<LayoutId>) {
        layouts.extend([self.key_layout, self.value_layout]);
    }
}

#[derive(Debug, Clone)]
pub struct ExportedNode {
    subgraph: NodeId,
    input: NodeId,
    layout: Stream,
}

impl ExportedNode {
    pub fn new(subgraph: NodeId, input: NodeId, layout: Stream) -> Self {
        Self {
            subgraph,
            input,
            layout,
        }
    }

    pub fn subgraph(&self) -> NodeId {
        self.subgraph
    }

    pub fn input(&self) -> NodeId {
        self.input
    }

    pub fn layout(&self) -> Stream {
        self.layout
    }
}

impl DataflowNode for ExportedNode {
    fn inputs(&self, inputs: &mut Vec<NodeId>) {
        inputs.extend([self.subgraph]);
    }

    fn output_kind(&self, _inputs: &[Stream]) -> Option<StreamKind> {
        Some(self.layout.kind())
    }

    fn output_stream(&self, _inputs: &[Stream]) -> Option<Stream> {
        Some(self.layout)
    }

    fn signature(&self, _inputs: &[Stream], _layout_cache: &LayoutCache) -> Signature {
        todo!()
    }

    fn validate(&self, _inputs: &[Stream], _layout_cache: &LayoutCache) {}

    fn optimize(&mut self, _inputs: &[Stream], _layout_cache: &LayoutCache) {}

    fn layouts(&self, _layouts: &mut Vec<LayoutId>) {}
}

#[derive(Debug, Clone)]
pub struct Export {
    input: NodeId,
    layout: Stream,
}

impl Export {
    pub fn new(input: NodeId, layout: Stream) -> Self {
        Self { input, layout }
    }

    pub fn input(&self) -> NodeId {
        self.input
    }

    pub fn layout(&self) -> Stream {
        self.layout
    }
}

impl DataflowNode for Export {
    fn inputs(&self, inputs: &mut Vec<NodeId>) {
        inputs.push(self.input);
    }

    fn output_kind(&self, _inputs: &[Stream]) -> Option<StreamKind> {
        Some(self.layout.kind())
    }

    fn output_stream(&self, _inputs: &[Stream]) -> Option<Stream> {
        Some(self.layout)
    }

    fn signature(&self, _inputs: &[Stream], _layout_cache: &LayoutCache) -> Signature {
        todo!()
    }

    fn validate(&self, _inputs: &[Stream], _layout_cache: &LayoutCache) {}

    fn optimize(&mut self, _inputs: &[Stream], _layout_cache: &LayoutCache) {}

    fn layouts(&self, _layouts: &mut Vec<LayoutId>) {}
}

#[derive(Debug, Clone)]
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

    fn layouts(&self, layouts: &mut Vec<LayoutId>) {
        layouts.push(self.layout);
    }
}

#[derive(Debug, Clone)]
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

    fn signature(&self, _inputs: &[Stream], _layout_cache: &LayoutCache) -> Signature {
        todo!()
    }

    fn validate(&self, _inputs: &[Stream], _layout_cache: &LayoutCache) {
        todo!()
    }

    fn optimize(&mut self, _inputs: &[Stream], _layout_cache: &LayoutCache) {}

    fn layouts(&self, _layouts: &mut Vec<LayoutId>) {}
}

#[derive(Debug, Clone)]
pub struct Distinct {
    input: NodeId,
}

impl Distinct {
    pub const fn new(input: NodeId) -> Self {
        Self { input }
    }

    pub const fn input(&self) -> NodeId {
        self.input
    }
}

impl DataflowNode for Distinct {
    fn inputs(&self, inputs: &mut Vec<NodeId>) {
        inputs.push(self.input);
    }

    fn output_kind(&self, inputs: &[Stream]) -> Option<StreamKind> {
        Some(inputs[0].kind())
    }

    fn output_stream(&self, inputs: &[Stream]) -> Option<Stream> {
        Some(inputs[0])
    }

    fn signature(&self, _inputs: &[Stream], _layout_cache: &LayoutCache) -> Signature {
        todo!()
    }

    fn validate(&self, _inputs: &[Stream], _layout_cache: &LayoutCache) {
        todo!()
    }

    fn optimize(&mut self, _inputs: &[Stream], _layout_cache: &LayoutCache) {}

    fn layouts(&self, _layouts: &mut Vec<LayoutId>) {}
}

// FIXME: DelayedFeedback with maps
#[derive(Debug, Clone)]
pub struct DelayedFeedback {
    layout: LayoutId,
}

impl DelayedFeedback {
    pub const fn new(layout: LayoutId) -> Self {
        Self { layout }
    }

    pub const fn layout(&self) -> LayoutId {
        self.layout
    }
}

impl DataflowNode for DelayedFeedback {
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

    fn layouts(&self, layouts: &mut Vec<LayoutId>) {
        layouts.push(self.layout);
    }
}

#[derive(Debug, Clone)]
pub struct Delta0 {
    input: NodeId,
}

impl Delta0 {
    pub const fn new(input: NodeId) -> Self {
        Self { input }
    }

    pub const fn input(&self) -> NodeId {
        self.input
    }
}

impl DataflowNode for Delta0 {
    fn inputs(&self, inputs: &mut Vec<NodeId>) {
        inputs.push(self.input);
    }

    fn output_kind(&self, inputs: &[Stream]) -> Option<StreamKind> {
        Some(inputs[0].kind())
    }

    fn output_stream(&self, inputs: &[Stream]) -> Option<Stream> {
        Some(inputs[0])
    }

    fn signature(&self, _inputs: &[Stream], _layout_cache: &LayoutCache) -> Signature {
        todo!()
    }

    fn validate(&self, _inputs: &[Stream], _layout_cache: &LayoutCache) {}

    fn optimize(&mut self, _inputs: &[Stream], _layout_cache: &LayoutCache) {}

    fn layouts(&self, _layouts: &mut Vec<LayoutId>) {}
}

#[derive(Debug, Clone)]
pub struct SourceMap {
    key: LayoutId,
    value: LayoutId,
}

impl SourceMap {
    pub const fn new(key: LayoutId, value: LayoutId) -> Self {
        Self { key, value }
    }

    /// The key type of the source's produced stream
    pub const fn key(&self) -> LayoutId {
        self.key
    }

    /// The value type of the source's produced stream
    pub const fn value(&self) -> LayoutId {
        self.value
    }
}

impl DataflowNode for SourceMap {
    fn inputs(&self, _inputs: &mut Vec<NodeId>) {}

    fn output_kind(&self, _inputs: &[Stream]) -> Option<StreamKind> {
        Some(StreamKind::Map)
    }

    fn output_stream(&self, _inputs: &[Stream]) -> Option<Stream> {
        Some(Stream::Map(self.key, self.value))
    }

    fn signature(&self, _inputs: &[Stream], _layout_cache: &LayoutCache) -> Signature {
        todo!()
    }

    fn validate(&self, _inputs: &[Stream], _layout_cache: &LayoutCache) {}

    fn optimize(&mut self, _inputs: &[Stream], _layout_cache: &LayoutCache) {}

    fn layouts(&self, layouts: &mut Vec<LayoutId>) {
        layouts.extend([self.key, self.value]);
    }
}

#[derive(Debug, Clone)]
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

    fn layouts(&self, _layouts: &mut Vec<LayoutId>) {}
}

#[derive(Debug, Clone)]
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
        Some(inputs[0].kind())
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

    fn functions<'a>(&'a self, functions: &mut Vec<&'a Function>) {
        functions.push(self.map_fn());
    }

    fn layouts(&self, layouts: &mut Vec<LayoutId>) {
        layouts.push(self.layout);
    }
}

#[derive(Debug, Clone)]
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
        Some(inputs[0].kind())
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

    fn functions<'a>(&'a self, functions: &mut Vec<&'a Function>) {
        functions.push(self.filter_fn());
    }

    fn layouts(&self, _layouts: &mut Vec<LayoutId>) {}
}

#[derive(Debug, Clone)]
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
        assert_eq!(
            self.signature(inputs, layout_cache),
            self.index_fn.signature()
        );
    }

    fn optimize(&mut self, _inputs: &[Stream], layout_cache: &LayoutCache) {
        self.index_fn.optimize(layout_cache);
    }

    fn functions<'a>(&'a self, functions: &mut Vec<&'a Function>) {
        functions.push(self.index_fn());
    }

    fn layouts(&self, layouts: &mut Vec<LayoutId>) {
        layouts.extend([self.key_layout, self.value_layout]);
    }
}

#[derive(Debug, Clone)]
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

    fn functions<'a>(&'a self, functions: &mut Vec<&'a Function>) {
        functions.extend([self.step_fn(), self.finish_fn()]);
    }

    fn layouts(&self, layouts: &mut Vec<LayoutId>) {
        layouts.extend([self.acc_layout, self.step_layout, self.output_layout]);
    }
}

#[derive(Debug, Clone)]
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
        Some(inputs[0].kind())
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

    fn layouts(&self, layouts: &mut Vec<LayoutId>) {
        layouts.push(self.output_layout);
    }
}

#[derive(Debug, Clone)]
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
        Some(inputs[0].kind())
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

    fn layouts(&self, _layouts: &mut Vec<LayoutId>) {}
}

#[derive(Debug, Clone)]
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
        Some(inputs[0].kind())
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

    fn layouts(&self, _layouts: &mut Vec<LayoutId>) {}
}
