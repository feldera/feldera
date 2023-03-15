use crate::ir::{
    function::Function, layout_cache::RowLayoutCache, literal::RowLiteral, types::Signature,
    ColumnType, DataflowNode, InputFlags, LayoutId, NodeId, RowLayoutBuilder, StreamKind,
    StreamLayout,
};
use dbsp::operator::time_series::RelRange;
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

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Fold {
    input: NodeId,
    /// The initial value of the fold, should be the same layout as `acc_layout`
    init: RowLiteral,
    /// The step function, should have a signature of
    /// `fn(acc_layout, step_layout, weight_layout) -> acc_layout`
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

    pub const fn input(&self) -> NodeId {
        self.input
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
        Some(StreamKind::Map)
    }

    fn output_stream(&self, inputs: &[StreamLayout]) -> Option<StreamLayout> {
        // FIXME: Should this be able to operate on sets too?
        Some(StreamLayout::Map(
            inputs[0].unwrap_map().0,
            self.output_layout,
        ))
    }

    fn signature(&self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) -> Signature {
        todo!()
    }

    fn validate(&self, inputs: &[StreamLayout], layout_cache: &RowLayoutCache) {
        assert_eq!(inputs.len(), 1);
        assert!(inputs[0].is_map());
        assert_eq!(inputs[0].unwrap_map().1, self.step_layout);

        // Step function
        {
            assert_eq!(self.step_fn.args().len(), 3);

            let acc_arg = &self.step_fn.args()[0];
            assert_eq!(acc_arg.layout, self.acc_layout);
            assert_eq!(acc_arg.flags, InputFlags::INOUT);

            let step_arg = &self.step_fn.args()[1];
            assert_eq!(step_arg.layout, self.step_layout);
            assert_eq!(step_arg.flags, InputFlags::INPUT);

            let weight_layout = layout_cache.add(
                RowLayoutBuilder::new()
                    .with_column(ColumnType::I32, false)
                    .build(),
            );
            let weight_arg = &self.step_fn.args()[2];
            assert_eq!(weight_arg.layout, weight_layout);
            assert_eq!(weight_arg.flags, InputFlags::INPUT);
        }

        // Finish function
        {
            assert_eq!(self.finish_fn.args().len(), 2);

            let acc_arg = &self.finish_fn.args()[0];
            assert_eq!(acc_arg.layout, self.acc_layout);
            assert_eq!(acc_arg.flags, InputFlags::INPUT);

            let output_arg = &self.finish_fn.args()[1];
            assert_eq!(output_arg.layout, self.output_layout);
            assert_eq!(output_arg.flags, InputFlags::OUTPUT);
        }
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

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PartitionedRollingFold {
    input: NodeId,
    /// The time range to fold over
    // FIXME: Support more timestamps
    range: RelRange<i32>,
    /// The initial value of the fold, should be the same layout as `acc_layout`
    init: RowLiteral,
    /// The step function, should have a signature of
    /// `fn(acc_layout, step_layout, weight_layout) -> acc_layout`
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

impl PartitionedRollingFold {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        input: NodeId,
        range: RelRange<i32>,
        init: RowLiteral,
        step_fn: Function,
        finish_fn: Function,
        acc_layout: LayoutId,
        step_layout: LayoutId,
        output_layout: LayoutId,
    ) -> Self {
        Self {
            input,
            range,
            init,
            step_fn,
            finish_fn,
            acc_layout,
            step_layout,
            output_layout,
        }
    }

    pub const fn input(&self) -> NodeId {
        self.input
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

    pub const fn range(&self) -> RelRange<i32> {
        self.range
    }
}

impl DataflowNode for PartitionedRollingFold {
    fn inputs(&self, inputs: &mut Vec<NodeId>) {
        inputs.push(self.input);
    }

    fn output_kind(&self, _inputs: &[StreamLayout]) -> Option<StreamKind> {
        Some(StreamKind::Map)
    }

    fn output_stream(&self, inputs: &[StreamLayout]) -> Option<StreamLayout> {
        // FIXME: Should this be able to operate on sets too?
        Some(StreamLayout::Map(
            inputs[0].unwrap_map().0,
            self.output_layout,
        ))
    }

    fn signature(&self, _inputs: &[StreamLayout], _layout_cache: &RowLayoutCache) -> Signature {
        todo!()
    }

    fn validate(&self, inputs: &[StreamLayout], layout_cache: &RowLayoutCache) {
        assert_eq!(inputs.len(), 1);
        assert!(inputs[0].is_map());
        assert_eq!(inputs[0].unwrap_map().1, self.step_layout);

        // Step function
        {
            assert_eq!(self.step_fn.args().len(), 3);

            let acc_arg = &self.step_fn.args()[0];
            assert_eq!(acc_arg.layout, self.acc_layout);
            assert_eq!(acc_arg.flags, InputFlags::INOUT);

            let step_arg = &self.step_fn.args()[1];
            assert_eq!(step_arg.layout, self.step_layout);
            assert_eq!(step_arg.flags, InputFlags::INPUT);

            let weight_layout = layout_cache.add(
                RowLayoutBuilder::new()
                    .with_column(ColumnType::I32, false)
                    .build(),
            );
            let weight_arg = &self.step_fn.args()[2];
            assert_eq!(weight_arg.layout, weight_layout);
            assert_eq!(weight_arg.flags, InputFlags::INPUT);
        }

        // Finish function
        {
            assert_eq!(self.finish_fn.args().len(), 2);

            let acc_arg = &self.finish_fn.args()[0];
            assert_eq!(acc_arg.layout, self.acc_layout);
            assert_eq!(acc_arg.flags, InputFlags::INPUT);

            let output_arg = &self.finish_fn.args()[1];
            assert_eq!(output_arg.layout, self.output_layout);
            assert_eq!(output_arg.flags, InputFlags::OUTPUT);
        }
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
