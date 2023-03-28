use crate::ir::{
    block::UnsealedBlock,
    exprs::{ArgType, Call},
    layout_cache::RowLayoutCache,
    BinaryOp, BinaryOpKind, Block, BlockId, BlockIdGen, Branch, Cast, ColumnType, Constant, Copy,
    CopyRowTo, Expr, ExprId, ExprIdGen, IsNull, Jump, LayoutId, Load, RValue, Return, Select,
    SetNull, Signature, Store, Terminator, UnaryOp, UnaryOpKind, UninitRow,
};
use petgraph::{
    algo::dominators::{self, Dominators},
    prelude::DiGraphMap,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::{self, Display},
    mem::swap,
};

bitflags::bitflags! {
    #[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Clone, Copy, Deserialize, Serialize)]
    #[serde(try_from = "String", into = "String")]
    pub struct InputFlags: u8 {
        /// The parameter can be used as an input
        const INPUT = 1 << 0;
        /// The parameter can be used as an output
        const OUTPUT = 1 << 1;
        /// The parameter can be used as both an input and output
        const INOUT = Self::INPUT.bits() | Self::OUTPUT.bits();
    }
}

impl InputFlags {
    pub const fn is_input(&self) -> bool {
        self.contains(Self::INPUT)
    }

    pub const fn is_output(&self) -> bool {
        self.contains(Self::OUTPUT)
    }

    pub const fn is_inout(&self) -> bool {
        self.contains(Self::INOUT)
    }

    /// Returns `true` if the parameter is only a input and not an output
    pub const fn is_readonly(&self) -> bool {
        self.is_input() && !self.is_output()
    }
}

#[derive(Debug, Clone)]
pub struct InvalidInputFlag(Box<str>);

impl Display for InvalidInputFlag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Invalid input flag, expected one of \"input\", \"output\" or \"inout\", got {:?}",
            self.0,
        )
    }
}

// TODO: Maybe this would be better represented as a comma-delimited list, e.g.
// `"input,output"`
impl TryFrom<&str> for InputFlags {
    type Error = InvalidInputFlag;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Ok(match value {
            "" => Self::empty(),
            "input" => Self::INPUT,
            "output" => Self::OUTPUT,
            "inout" => Self::INOUT,
            invalid => return Err(InvalidInputFlag(Box::from(invalid))),
        })
    }
}

impl TryFrom<String> for InputFlags {
    type Error = InvalidInputFlag;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::try_from(&*value)
    }
}

impl From<InputFlags> for &'static str {
    fn from(flags: InputFlags) -> Self {
        match flags {
            InputFlags::INPUT => "input",
            InputFlags::OUTPUT => "output",
            InputFlags::INOUT => "inout",
            _ => unreachable!(),
        }
    }
}

impl From<InputFlags> for String {
    fn from(flags: InputFlags) -> Self {
        <&'static str>::from(flags).to_owned()
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FuncArg {
    /// The id that the pointer is associated with and the flags that are
    /// associated with the argument. All function arguments are passed by
    /// pointer since we can't know the type's exact size at compile time
    pub id: ExprId,
    /// The layout of the argument
    pub layout: LayoutId,
    /// The flags associated with the argument
    pub flags: InputFlags,
}

impl FuncArg {
    pub const fn new(id: ExprId, layout: LayoutId, flags: InputFlags) -> Self {
        Self { id, layout, flags }
    }
}

#[serde_with::serde_as]
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Function {
    args: Vec<FuncArg>,
    ret: ColumnType,
    entry_block: BlockId,
    #[serde_as(as = "BTreeMap<serde_with::DisplayFromStr, _>")]
    blocks: BTreeMap<BlockId, Block>,
    #[serde(skip)]
    cfg: DiGraphMap<BlockId, ()>,
}

impl Function {
    pub fn args(&self) -> &[FuncArg] {
        &self.args
    }

    pub fn entry_block(&self) -> BlockId {
        self.entry_block
    }

    pub fn blocks(&self) -> &BTreeMap<BlockId, Block> {
        &self.blocks
    }

    pub const fn return_type(&self) -> ColumnType {
        self.ret
    }

    pub fn dominators(&self) -> Dominators<BlockId> {
        dominators::simple_fast(&self.cfg, self.entry_block)
    }

    pub fn signature(&self) -> Signature {
        Signature::new(
            self.args.iter().map(|arg| arg.layout).collect(),
            self.args.iter().map(|arg| arg.flags).collect(),
            self.ret,
        )
    }

    pub(crate) fn set_cfg(&mut self, cfg: DiGraphMap<BlockId, ()>) {
        self.cfg = cfg;
    }

    #[tracing::instrument(skip_all)]
    pub fn optimize(&mut self, layout_cache: &RowLayoutCache) {
        // self.remove_redundant_casts();
        self.remove_unit_memory_operations(layout_cache);
        self.deduplicate_input_loads();
        self.simplify_branches();
        self.truncate_zero();
        self.concat_empty_strings();
        self.dce();
        // self.remove_noop_copies(layout_cache)
        // TODO: Tree shaking to remove unreachable nodes
    }

    fn concat_empty_strings(&mut self) {
        // TODO: Simplify/eliminate concat calls where one of the strings is empty
        // TODO: Propagate string length info around so that we can do this in more
        // general situations
        // TODO: Constant propagation
        // TODO: Do the same with `concat_clone`, except clone the non-empty string

        let mut empty_strings = BTreeSet::new();
        for block in self.blocks.values() {
            for &(expr_id, ref expr) in block.body() {
                if let Expr::Constant(Constant::String(string)) = expr {
                    if string.is_empty() {
                        empty_strings.insert(expr_id);
                    }
                }
            }
        }

        for block in self.blocks.values_mut() {
            for (_, expr) in block.body_mut() {
                if let Expr::Call(call) = expr {
                    if call.function() == "dbsp.str.concat" {
                        let (lhs, rhs) = (call.args()[0], call.args()[1]);
                        let (lhs_empty, rhs_empty) =
                            (empty_strings.contains(&lhs), empty_strings.contains(&rhs));

                        // If both strings are empty we turn the expression into an empty string constant
                        if lhs_empty && rhs_empty {
                            tracing::debug!(
                                "turned @dbsp.str.truncate({lhs}, {rhs}) into an empty string constant (both strings are empty)",
                            );
                            *expr = Expr::Constant(Constant::String(String::new()));

                        // If just one of them is empty we want to rewrite all uses of the expression
                        // to consume the non-empty string
                        } else if lhs_empty {
                            todo!()
                        } else if rhs_empty {
                            todo!()
                        }
                    }
                }
            }
        }
    }

    // Turn all `@dbsp.str.truncate(string, 0)` calls into `@dbsp.str.clear(string)` calls
    // TODO: Eliminate all truncate/clear calls when the length is already less than
    // or equal to the target length
    // TODO: Do the same with `truncate_clone`
    fn truncate_zero(&mut self) {
        // TODO: Constant propagation
        let mut zeroes = BTreeSet::new();
        for block in self.blocks.values() {
            for &(expr_id, ref expr) in block.body() {
                if let Expr::Constant(Constant::Usize(0)) = *expr {
                    zeroes.insert(expr_id);
                }
            }
        }

        for block in self.blocks.values_mut() {
            for (_, expr) in block.body_mut() {
                if let Expr::Call(call) = expr {
                    if call.function() == "dbsp.str.truncate" && zeroes.contains(&call.args()[1]) {
                        let string = call.args()[0];
                        tracing::debug!(
                            "turned @dbsp.str.truncate({string}, {}) into @dbsp.str.clear({string})",
                            call.args()[1],
                        );

                        // Turn the truncate call into a clear call
                        *call = Call::new(
                            "dbsp.str.clear".to_owned(),
                            vec![string],
                            vec![ArgType::Scalar(ColumnType::String)],
                            ColumnType::Unit,
                        );
                    }
                }
            }
        }
    }

    fn dce(&mut self) {
        // Remove unreachable blocks
        {
            let mut used = BTreeSet::new();
            let mut stack = vec![self.entry_block];

            while let Some(block) = stack.pop() {
                if used.insert(block) {
                    match self.blocks[&block].terminator() {
                        Terminator::Jump(jump) => stack.push(jump.target()),
                        Terminator::Branch(branch) => {
                            stack.extend([branch.truthy(), branch.falsy()]);
                        }
                        Terminator::Return(_) => {}
                    }
                }
            }

            // Remove all unused blocks
            let start_blocks = self.blocks.len();
            self.blocks.retain(|&block_id, _| used.contains(&block_id));

            let removed_blocks = start_blocks - self.blocks.len();
            if removed_blocks != 0 {
                tracing::debug!(
                    "dce removed {removed_blocks} block{}",
                    if removed_blocks == 1 { "" } else { "s" },
                );
            }
        }

        // Remove unused expressions
        {
            let mut used = BTreeSet::new();

            // Collect all usages
            for block in self.blocks.values() {
                for &(expr_id, ref expr) in block.body() {
                    match expr {
                        Expr::Cast(cast) => {
                            used.insert(cast.value());
                        }

                        Expr::Load(load) => {
                            used.insert(load.source());
                        }

                        Expr::Store(store) => {
                            // Stores are always regarded as used within dce
                            used.extend([expr_id, store.target()]);
                            if let &RValue::Expr(value) = store.value() {
                                used.insert(value);
                            }
                        }

                        Expr::Select(select) => {
                            used.insert(select.cond());
                            used.insert(select.if_true());
                            used.insert(select.if_false());
                        }

                        Expr::IsNull(is_null) => {
                            used.insert(is_null.target());
                        }

                        Expr::BinOp(binop) => {
                            used.insert(binop.lhs());
                            used.insert(binop.rhs());
                        }

                        Expr::Copy(copy) => {
                            used.insert(copy.value());
                        }

                        Expr::UnaryOp(unary) => {
                            used.insert(unary.value());
                        }

                        Expr::SetNull(set_null) => {
                            // Stores are always regarded as used within dce
                            used.extend([expr_id, set_null.target()]);
                            if let &RValue::Expr(is_null) = set_null.is_null() {
                                used.insert(is_null);
                            }
                        }

                        Expr::CopyRowTo(copy) => {
                            // Stores are always regarded as used within dce
                            used.extend([expr_id, copy.src(), copy.dest()]);
                        }

                        Expr::Call(call) => {
                            // Regard all calls as effectful, in the future we'll want to
                            // be able to specify whether a function call has side effects
                            // or not
                            used.insert(expr_id);
                            used.extend(call.args());
                        }

                        // These contain no expressions
                        Expr::NullRow(_) | Expr::Constant(_) | Expr::UninitRow(_) => {}
                    }
                }

                match block.terminator() {
                    Terminator::Jump(_) => {}
                    Terminator::Branch(branch) => {
                        if let &RValue::Expr(cond) = branch.cond() {
                            used.insert(cond);
                        }
                    }
                    Terminator::Return(ret) => {
                        if let &RValue::Expr(ret) = ret.value() {
                            used.insert(ret);
                        }
                    }
                }
            }

            // Remove all unused expressions
            for block in self.blocks.values_mut() {
                block.retain(|expr_id, _| used.contains(&expr_id));
            }
        }
    }

    fn deduplicate_input_loads(&mut self) {
        // The first load of each input valid for deduplication
        let mut input_loads: BTreeMap<ExprId, ExprId> = BTreeMap::new();

        {
            // Holds all inputs valid for deduplication
            let mut inputs = BTreeSet::new();
            for arg in self.args() {
                if arg.flags.is_readonly() {
                    inputs.insert(arg.id);
                }
            }

            // Collect the first load of any valid inputs
            for block in self.blocks.values() {
                for &(expr_id, ref expr) in block.body() {
                    if let Expr::Load(load) = expr {
                        if inputs.contains(&load.source()) {
                            input_loads.entry(load.source()).or_insert(expr_id);
                        }
                    }
                }
            }
        }

        let remap = |expr_id: &mut ExprId| {
            if let Some(&new_value) = input_loads.get(expr_id) {
                *expr_id = new_value;
            }
        };

        // Remap all uses of extraneous loads to use the first instance of the load
        for block in self.blocks.values_mut() {
            for (_, expr) in block.body_mut() {
                match expr {
                    Expr::Cast(cast) => remap(cast.value_mut()),

                    // Note: Any of these would be redundant or self-referential right now
                    // since we don't currently have nested rows
                    Expr::Load(_) => {}

                    Expr::Store(store) => {
                        if let RValue::Expr(value) = store.value_mut() {
                            remap(value);
                        }
                    }

                    Expr::Select(select) => {
                        remap(select.cond_mut());
                        remap(select.if_true_mut());
                        remap(select.if_false_mut());
                    }

                    // IsNull operates on row values and we currently don't have nested rows
                    Expr::IsNull(_) => {}

                    Expr::BinOp(binop) => {
                        remap(binop.lhs_mut());
                        remap(binop.rhs_mut());
                    }

                    Expr::Copy(copy) => remap(copy.value_mut()),

                    Expr::UnaryOp(unary) => remap(unary.value_mut()),

                    Expr::SetNull(set_null) => {
                        remap(set_null.target_mut());
                        if let RValue::Expr(is_null) = set_null.is_null_mut() {
                            remap(is_null);
                        }
                    }

                    Expr::Call(call) => {
                        for arg in call.args_mut() {
                            remap(arg);
                        }
                    }

                    // Constants contain no expressions
                    Expr::Constant(_) => {}

                    // These expressions operate exclusively on rows
                    Expr::CopyRowTo(_) | Expr::NullRow(_) | Expr::UninitRow(_) => todo!(),
                }
            }
        }

        // Depends on DCE to eliminate unused loads
    }

    pub fn map_layouts<F>(&self, mut map: F)
    where
        F: FnMut(LayoutId),
    {
        for FuncArg { layout, .. } in &self.args {
            map(*layout);
        }

        for block in self.blocks.values() {
            for (_, expr) in block.body() {
                expr.map_layouts(&mut map);
            }

            // Terminators don't contain layout ids
        }
    }

    pub fn remap_layouts(&mut self, mappings: &BTreeMap<LayoutId, LayoutId>) {
        for FuncArg { layout, .. } in &mut self.args {
            *layout = mappings[layout];
        }

        for block in self.blocks.values_mut() {
            for (_, expr) in block.body_mut() {
                expr.remap_layouts(mappings);
            }

            // Terminators don't contain layout ids
        }
    }

    #[allow(dead_code)]
    fn remove_redundant_casts(&mut self) {
        todo!()
    }

    fn simplify_branches(&mut self) {
        // TODO: Consume const prop dataflow graph and turn conditional branches with
        // constant conditions into unconditional ones
        // TODO: Simplify `select` calls

        // Replace any branches that have identical true/false targets with an
        // unconditional jump
        for block in self.blocks.values_mut() {
            if let Some(target) = block
                .terminator()
                .as_branch()
                .and_then(|branch| branch.targets_are_identical().then(|| branch.truthy()))
            {
                *block.terminator_mut() = Terminator::Jump(Jump::new(target));
            }
        }
    }

    #[allow(dead_code)]
    fn remove_noop_copies(&mut self, layout_cache: &RowLayoutCache) {
        let mut scalar_exprs = BTreeSet::new();
        let mut row_exprs = BTreeMap::new();
        for arg in &self.args {
            row_exprs.insert(arg.id, arg.layout);
        }

        let mut substitutions = BTreeMap::new();

        // FIXME: Doesn't work for back edges/loops
        let mut stack = vec![self.entry_block];
        while let Some(block_id) = stack.pop() {
            let block = self.blocks.get_mut(&block_id).unwrap();

            block.retain(|expr_id, expr| {
                match expr {
                    Expr::UninitRow(uninit) => {
                        row_exprs.insert(expr_id, uninit.layout());
                    }

                    Expr::NullRow(null) => {
                        row_exprs.insert(expr_id, null.layout());
                    }

                    Expr::Constant(constant) => {
                        if constant.is_unit() || constant.is_bool() || constant.is_int() {
                            scalar_exprs.insert(expr_id);
                        }
                    }

                    Expr::Load(load) => {
                        let row_layout = row_exprs[&load.source()];
                        let layout = layout_cache.get(row_layout);

                        if !layout.columns()[load.column()].requires_nontrivial_clone() {
                            scalar_exprs.insert(expr_id);
                        }
                    }

                    Expr::Copy(copy) => {
                        if scalar_exprs.contains(&copy.value()) {
                            scalar_exprs.insert(expr_id);
                            substitutions.insert(expr_id, copy.value());
                            return false;
                        }
                    }

                    Expr::BinOp(binop) => {
                        if scalar_exprs.contains(&binop.lhs())
                            || scalar_exprs.contains(&binop.rhs())
                        {
                            scalar_exprs.insert(expr_id);
                        }
                    }

                    _ => {}
                }

                true
            });

            match block.terminator_mut() {
                Terminator::Return(_) => {}
                Terminator::Jump(jump) => stack.push(jump.target()),
                Terminator::Branch(branch) => stack.extend([branch.truthy(), branch.falsy()]),
            }
        }

        if !substitutions.is_empty() {
            for block in self.blocks.values_mut() {
                for (_, expr) in block.body() {
                    match expr {
                        Expr::Call(_) => todo!(),
                        Expr::Load(_) => todo!(),
                        Expr::Store(_) => todo!(),
                        Expr::BinOp(_) => todo!(),
                        Expr::UnaryOp(_) => todo!(),
                        Expr::IsNull(_) => todo!(),
                        Expr::Copy(_) => todo!(),
                        Expr::NullRow(_) => todo!(),
                        Expr::SetNull(_) => todo!(),
                        Expr::Constant(_) => {}
                        Expr::CopyRowTo(_) => todo!(),
                        Expr::UninitRow(_) => todo!(),
                        Expr::Cast(_) => todo!(),
                        Expr::Select(_) => todo!(),
                    }
                }

                match block.terminator_mut() {
                    Terminator::Return(ret) => {
                        if let &RValue::Expr(value) = ret.value() {
                            if let Some(&subst) = substitutions.get(&value) {
                                *ret.value_mut() = RValue::Expr(subst);
                            }
                        }
                    }

                    Terminator::Branch(branch) => {
                        if let &RValue::Expr(value) = branch.cond() {
                            if let Some(&subst) = substitutions.get(&value) {
                                *branch.cond_mut() = RValue::Expr(subst);
                            }
                        }
                    }

                    Terminator::Jump(_) => {}
                }
            }
        }
    }

    fn remove_unit_memory_operations(&mut self, layout_cache: &RowLayoutCache) {
        let mut unit_exprs = BTreeSet::new();
        let mut row_exprs = BTreeMap::new();
        for arg in &self.args {
            row_exprs.insert(arg.id, arg.layout);
        }

        // FIXME: Doesn't work for back edges/loops
        let mut stack = vec![self.entry_block];
        while let Some(block_id) = stack.pop() {
            let block = self.blocks.get_mut(&block_id).unwrap();

            block.retain(|expr_id, expr| match expr {
                Expr::UninitRow(uninit) => {
                    row_exprs.insert(expr_id, uninit.layout());
                    true
                }

                Expr::NullRow(null) => {
                    row_exprs.insert(expr_id, null.layout());
                    true
                }

                Expr::Constant(Constant::Unit) => {
                    unit_exprs.insert(expr_id);
                    false
                }

                Expr::Load(load) => {
                    let row_layout = row_exprs[&load.source()];
                    let layout = layout_cache.get(row_layout);

                    if layout.columns()[load.column()].is_unit() {
                        unit_exprs.insert(expr_id);
                        false
                    } else {
                        true
                    }
                }

                Expr::Store(store) => {
                    let row_layout = row_exprs[&store.target()];
                    let layout = layout_cache.get(row_layout);
                    !layout.columns()[store.column()].is_unit()
                }

                Expr::Copy(copy) => {
                    if unit_exprs.contains(&copy.value()) {
                        unit_exprs.insert(expr_id);
                        false
                    } else {
                        true
                    }
                }

                _ => true,
            });

            match block.terminator_mut() {
                // Normalize unit returns to return unit contents
                Terminator::Return(ret) => {
                    if let &RValue::Expr(expr) = ret.value() {
                        if unit_exprs.contains(&expr) {
                            *ret = Return::new(RValue::Imm(Constant::Unit));
                        }
                    }
                }

                Terminator::Jump(jump) => stack.push(jump.target()),
                Terminator::Branch(branch) => stack.extend([branch.truthy(), branch.falsy()]),
            }
        }
    }
}

// TODO: Move to an RVSDG instead of BB form
// TODO: Function arg attributes for mutability, allow inserting into mutable
// rows
pub struct FunctionBuilder {
    args: Vec<FuncArg>,
    ret: ColumnType,
    entry_block: Option<BlockId>,

    blocks: BTreeMap<BlockId, Block>,
    unsealed_blocks: BTreeMap<BlockId, UnsealedBlock>,

    current: Option<UnsealedBlock>,

    expr_id: ExprIdGen,
    block_id: BlockIdGen,

    layout_cache: RowLayoutCache,
    expr_types: BTreeMap<ExprId, Result<ColumnType, LayoutId>>,
}

impl FunctionBuilder {
    pub fn new(layout_cache: RowLayoutCache) -> Self {
        Self {
            args: Vec::new(),
            ret: ColumnType::Unit,
            entry_block: None,
            blocks: BTreeMap::new(),
            unsealed_blocks: BTreeMap::new(),
            current: None,
            expr_id: ExprIdGen::new(),
            block_id: BlockIdGen::new(),
            layout_cache,
            expr_types: BTreeMap::new(),
        }
    }

    pub fn set_return_type(&mut self, return_type: ColumnType) -> &mut Self {
        self.ret = return_type;
        self
    }

    pub fn with_return_type(mut self, return_type: ColumnType) -> Self {
        self.ret = return_type;
        self
    }

    pub fn add_input(&mut self, input_row: LayoutId) -> ExprId {
        self.add_input_with_flags(input_row, InputFlags::INPUT)
    }

    pub fn add_output(&mut self, input_row: LayoutId) -> ExprId {
        self.add_input_with_flags(input_row, InputFlags::OUTPUT)
    }

    pub fn add_input_output(&mut self, input_row: LayoutId) -> ExprId {
        self.add_input_with_flags(input_row, InputFlags::INOUT)
    }

    pub fn add_input_with_flags(&mut self, input_row: LayoutId, flags: InputFlags) -> ExprId {
        let arg_id = self.expr_id.next();
        self.args.push(FuncArg::new(arg_id, input_row, flags));
        self.set_expr_type(arg_id, Err(input_row));
        arg_id
    }

    fn set_expr_type(&mut self, expr_id: ExprId, ty: Result<ColumnType, LayoutId>) {
        let prev = self.expr_types.insert(expr_id, ty);
        debug_assert!(prev.is_none());
    }

    fn current_block(&mut self) -> &mut UnsealedBlock {
        self.current.get_or_insert_with(|| {
            let block_id = self.block_id.next();
            if self.entry_block.is_none() {
                self.entry_block = Some(block_id);
            }

            UnsealedBlock::new(block_id)
        })
    }

    pub(crate) fn add_expr<E>(&mut self, expr: E) -> ExprId
    where
        E: Into<Expr>,
    {
        let expr = expr.into();
        let expr_id = self.expr_id.next();
        self.current_block().body.push((expr_id, expr));
        expr_id
    }

    /// Check if `value` is null
    pub fn is_null(&mut self, value: ExprId, column: usize) -> ExprId {
        let value_layout = self
            .expr_types
            .get(&value)
            .unwrap_or_else(|| panic!("failed to get type of {value}"))
            .expect_err("attempted to call `IsNull` on a scalar value");
        let expr = self.add_expr(IsNull::new(value, value_layout, column));
        self.set_expr_type(expr, Ok(ColumnType::Bool));
        expr
    }

    pub fn set_null<N>(&mut self, target: ExprId, column: usize, is_null: N)
    where
        N: Into<RValue>,
    {
        let target_layout = self
            .expr_types
            .get(&target)
            .unwrap_or_else(|| panic!("failed to get type of {target}"))
            .expect_err("attempted to call `SetNull` on a scalar value");
        self.add_expr(SetNull::new(target, target_layout, column, is_null.into()));
    }

    pub fn cast(&mut self, value: ExprId, dest_ty: ColumnType) -> ExprId {
        let value_ty = self
            .expr_types
            .get(&value)
            .unwrap_or_else(|| panic!("failed to get type of {value}"))
            .expect("attempted to call `Cast` on a row value");
        let expr = self.add_expr(Cast::new(value, value_ty, dest_ty));
        self.set_expr_type(expr, Ok(dest_ty));
        expr
    }

    pub fn copy_val(&mut self, value: ExprId) -> ExprId {
        let value_ty = self
            .expr_types
            .get(&value)
            .unwrap_or_else(|| panic!("failed to get type of {value}"))
            .expect("attempted to call `CopyVal` on a row value");
        let expr = self.add_expr(Copy::new(value, value_ty));
        self.set_expr_type(expr, Ok(value_ty));
        expr
    }

    pub fn load(&mut self, target: ExprId, column: usize) -> ExprId {
        let target_layout = self
            .expr_types
            .get(&target)
            .unwrap_or_else(|| panic!("failed to get type of {target}"))
            .expect_err("attempted to call `Load` on a scalar value");
        let column_type = self.layout_cache.get(target_layout).column_type(column);

        let expr = self.add_expr(Load::new(target, target_layout, column, column_type));
        self.set_expr_type(expr, Ok(column_type));

        expr
    }

    pub fn store<V>(&mut self, target: ExprId, column: usize, value: V)
    where
        V: Into<RValue>,
    {
        let target_layout = self
            .expr_types
            .get(&target)
            .unwrap_or_else(|| panic!("failed to get type of {target}"))
            .expect_err("attempted to call `Store` on a scalar value");
        let value_type = self.layout_cache.get(target_layout).column_type(column);

        self.add_expr(Store::new(
            target,
            target_layout,
            column,
            value.into(),
            value_type,
        ));
    }

    pub fn and(&mut self, lhs: ExprId, rhs: ExprId) -> ExprId {
        self.binary_op(lhs, rhs, BinaryOpKind::And)
    }

    pub fn or(&mut self, lhs: ExprId, rhs: ExprId) -> ExprId {
        self.binary_op(lhs, rhs, BinaryOpKind::Or)
    }

    pub fn add(&mut self, lhs: ExprId, rhs: ExprId) -> ExprId {
        self.binary_op(lhs, rhs, BinaryOpKind::Add)
    }

    pub fn sub(&mut self, lhs: ExprId, rhs: ExprId) -> ExprId {
        self.binary_op(lhs, rhs, BinaryOpKind::Sub)
    }

    pub fn mul(&mut self, lhs: ExprId, rhs: ExprId) -> ExprId {
        self.binary_op(lhs, rhs, BinaryOpKind::Mul)
    }

    pub fn div(&mut self, lhs: ExprId, rhs: ExprId) -> ExprId {
        self.binary_op(lhs, rhs, BinaryOpKind::Div)
    }

    pub fn eq(&mut self, lhs: ExprId, rhs: ExprId) -> ExprId {
        self.binary_op(lhs, rhs, BinaryOpKind::Eq)
    }

    pub fn neq(&mut self, lhs: ExprId, rhs: ExprId) -> ExprId {
        self.binary_op(lhs, rhs, BinaryOpKind::Neq)
    }

    pub fn lt(&mut self, lhs: ExprId, rhs: ExprId) -> ExprId {
        self.binary_op(lhs, rhs, BinaryOpKind::LessThan)
    }

    pub fn gt(&mut self, lhs: ExprId, rhs: ExprId) -> ExprId {
        self.binary_op(lhs, rhs, BinaryOpKind::GreaterThan)
    }

    pub fn le(&mut self, lhs: ExprId, rhs: ExprId) -> ExprId {
        self.binary_op(lhs, rhs, BinaryOpKind::LessThanOrEqual)
    }

    pub fn ge(&mut self, lhs: ExprId, rhs: ExprId) -> ExprId {
        self.binary_op(lhs, rhs, BinaryOpKind::GreaterThanOrEqual)
    }

    fn binary_op(&mut self, lhs: ExprId, rhs: ExprId, kind: BinaryOpKind) -> ExprId {
        let lhs_ty = self
            .expr_types
            .get(&lhs)
            .unwrap_or_else(|| panic!("failed to get type of {lhs}"))
            .expect("attempted to call a binary op on a row value");
        if cfg!(debug_assertions) {
            let rhs_ty = self
                .expr_types
                .get(&rhs)
                .unwrap_or_else(|| panic!("failed to get type of {rhs}"))
                .expect("attempted to call a binary op on a row value");
            assert_eq!(lhs_ty, rhs_ty);
        }

        let expr = self.add_expr(BinaryOp::new(lhs, rhs, lhs_ty, kind));

        // TODO: Is this correct?
        // TODO: Make this a method on `BinaryOpKind` for reuse
        let output_ty = match kind {
            BinaryOpKind::Add
            | BinaryOpKind::Sub
            | BinaryOpKind::Mul
            | BinaryOpKind::Div
            | BinaryOpKind::And
            | BinaryOpKind::Or
            | BinaryOpKind::Xor
            | BinaryOpKind::Min
            | BinaryOpKind::Max => lhs_ty,

            BinaryOpKind::Eq
            | BinaryOpKind::Neq
            | BinaryOpKind::LessThan
            | BinaryOpKind::GreaterThan
            | BinaryOpKind::LessThanOrEqual
            | BinaryOpKind::GreaterThanOrEqual => ColumnType::Bool,
        };
        self.set_expr_type(expr, Ok(output_ty));

        expr
    }

    pub fn string_len(&mut self, value: ExprId) -> ExprId {
        self.unary_op(value, UnaryOpKind::StringLen)
    }

    fn unary_op(&mut self, value: ExprId, kind: UnaryOpKind) -> ExprId {
        let value_ty = self
            .expr_types
            .get(&value)
            .unwrap_or_else(|| panic!("failed to get type of {value}"))
            .expect("attempted to call a unary op on a row value");

        let expr = self.add_expr(UnaryOp::new(value, value_ty, kind));

        // TODO: Implement all unary op kinds
        // TODO: Is this correct?
        // TODO: Make this a method on `UnaryOpKind` for reuse
        let output_ty = match kind {
            UnaryOpKind::StringLen => ColumnType::U64,

            _ => todo!(),
        };
        self.set_expr_type(expr, Ok(output_ty));

        expr
    }

    pub fn select(&mut self, cond: ExprId, if_true: ExprId, if_false: ExprId) -> ExprId {
        let expr = self.add_expr(Select::new(cond, if_true, if_false));
        self.set_expr_type(expr, self.expr_types[&if_true]);
        expr
    }

    pub fn constant(&mut self, constant: Constant) -> ExprId {
        let constant_type = constant.column_type();
        let expr = self.add_expr(constant);
        self.set_expr_type(expr, Ok(constant_type));
        expr
    }

    pub fn uninit_row(&mut self, layout: LayoutId) -> ExprId {
        let expr = self.add_expr(UninitRow::new(layout));
        self.set_expr_type(expr, Err(layout));
        expr
    }

    pub fn copy_row_to(&mut self, src: ExprId, dest: ExprId) {
        let src_layout = self
            .expr_types
            .get(&src)
            .unwrap_or_else(|| panic!("failed to get type of {src}"))
            .expect_err("attempted to call `CopyRowTo` on a scalar value");
        if cfg!(debug_assertions) {
            let dest_layout = self
                .expr_types
                .get(&dest)
                .unwrap_or_else(|| panic!("failed to get type of {dest}"))
                .expect_err("attempted to call `CopyRowTo` on a scalar value");
            assert_eq!(src_layout, dest_layout);
        }

        self.add_expr(CopyRowTo::new(src, dest, src_layout));
    }

    pub fn set_terminator<T>(&mut self, terminator: T)
    where
        T: Into<Terminator>,
    {
        let terminator = terminator.into();

        self.current_block().terminator = Some(terminator);
    }

    /// Terminate the current block with a return
    pub fn ret<V>(&mut self, value: V)
    where
        V: Into<RValue>,
    {
        self.set_terminator(Return::new(value.into()));
    }

    /// Terminate the current block with a unit return
    pub fn ret_unit(&mut self) {
        self.set_terminator(Return::new(RValue::Imm(Constant::Unit)));
    }

    /// Terminate the current block with a return
    pub fn branch<C>(&mut self, cond: C, truthy: BlockId, falsy: BlockId)
    where
        C: Into<RValue>,
    {
        self.set_terminator(Branch::new(cond.into(), truthy, falsy));
    }

    #[track_caller]
    pub fn seal_current(&mut self) {
        let current = self
            .current
            .take()
            .expect("Called `FunctionBuilder::seal()` without a current block")
            .into_block();

        self.blocks.insert(current.id(), current);
    }

    pub fn create_block(&mut self) -> BlockId {
        let created_id = self.block_id.next();
        let created = UnsealedBlock::new(created_id);
        self.unsealed_blocks.insert(created.id, created);

        created_id
    }

    pub fn move_to(&mut self, block: BlockId) {
        let mut target = self
            .unsealed_blocks
            .remove(&block)
            // FIXME: Better error message
            .expect("moved to a sealed block or a block that doesn't exist");

        if let Some(current) = self.current.as_mut() {
            swap(current, &mut target);
            self.unsealed_blocks.insert(target.id, target);
        } else {
            self.current = Some(target);
        }
    }

    pub fn set_entry(&mut self, block: BlockId) {
        self.entry_block = Some(block);
    }

    fn contains_block(&self, block: BlockId) -> bool {
        self.blocks.contains_key(&block)
            || self.unsealed_blocks.contains_key(&block)
            || self
                .current
                .as_ref()
                .map_or(false, |current| current.id == block)
    }

    #[track_caller]
    pub fn validate(&self) {
        for (&id, block) in &self.blocks {
            assert_eq!(id, block.id());
        }
        for (&id, block) in &self.unsealed_blocks {
            assert_eq!(id, block.id);
        }

        for block in self.blocks.values() {
            self.validate_terminator(block.id(), block.terminator());
        }
        for block in self.unsealed_blocks.values() {
            if let Some(terminator) = block.terminator.as_ref() {
                self.validate_terminator(block.id, terminator);
            }
        }
    }

    #[track_caller]
    fn validate_terminator(&self, block: BlockId, terminator: &Terminator) {
        match &terminator {
            Terminator::Jump(jump) => {
                assert!(
                    self.contains_block(jump.target()),
                    "Block jumps to block that doesn't exist: {} attempts to jump to {}",
                    block,
                    jump.target(),
                );
            }

            Terminator::Branch(branch) => {
                assert!(
                    self.contains_block(branch.truthy()),
                    "Block branches to block that doesn't exist: {} attempts to jump to {}",
                    block,
                    branch.truthy(),
                );
                assert!(
                    self.contains_block(branch.falsy()),
                    "Block branches to block that doesn't exist: {} attempts to jump to {}",
                    block,
                    branch.falsy(),
                );
            }

            Terminator::Return(_) => {}
        }
    }

    #[track_caller]
    pub fn build(mut self) -> Function {
        self.validate();

        // Make sure there's an entry block
        let entry_block = self
            .entry_block
            .expect("Called `FunctionBuilder::build()` on a builder without an entry block");

        // Finish the current block
        if let Some(current) = self.current.take() {
            let block = current.into_block();
            self.blocks.insert(block.id(), block);
        }

        // Finish all unsealed blocks
        for (id, unsealed) in self.unsealed_blocks {
            debug_assert_eq!(id, unsealed.id);
            let block = unsealed.into_block();
            self.blocks.insert(id, block);
        }

        // Build a control flow graph
        let mut cfg = DiGraphMap::with_capacity(
            self.blocks.len(),
            // blocks * 1.5
            self.blocks.len() + (self.blocks.len() >> 1),
        );
        for (&block_id, block) in &self.blocks {
            match block.terminator() {
                Terminator::Jump(jump) => {
                    cfg.add_edge(block_id, jump.target(), ());
                }

                Terminator::Branch(branch) => {
                    cfg.add_edge(block_id, branch.truthy(), ());
                    cfg.add_edge(block_id, branch.falsy(), ());
                }

                Terminator::Return(_) => {
                    cfg.add_node(block_id);
                }
            }
        }

        Function {
            args: self.args,
            ret: self.ret,
            entry_block,
            blocks: self.blocks,
            cfg,
        }
    }
}
