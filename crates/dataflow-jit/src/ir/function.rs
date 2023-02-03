use crate::ir::{
    block::{Block, UnsealedBlock},
    expr::{
        BinaryOp, BinaryOpKind, Branch, Constant, Expr, IsNull, Load, RValue, Return, SetNull,
        Store, Terminator,
    },
    layout_cache::RowLayoutCache,
    BlockId, BlockIdGen, ColumnType, ExprId, ExprIdGen, LayoutId, Signature,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    mem::swap,
};

bitflags::bitflags! {
    pub struct InputFlags: u8 {
        /// The parameter can be used as an input
        const INPUT = 1 << 0;
        /// The parameter can be used as an output
        const OUTPUT = 1 << 1;
        /// The parameter can be used as both an input and output
        const INOUT = Self::INPUT.bits | Self::OUTPUT.bits;
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

#[derive(Debug, Clone, PartialEq)]
pub struct Function {
    args: Vec<(LayoutId, ExprId, InputFlags)>,
    ret: ColumnType,
    entry_block: BlockId,
    exprs: BTreeMap<ExprId, Expr>,
    blocks: BTreeMap<BlockId, Block>,
}

impl Function {
    pub fn args(&self) -> &[(LayoutId, ExprId, InputFlags)] {
        &self.args
    }

    pub fn entry_block(&self) -> BlockId {
        self.entry_block
    }

    pub fn exprs(&self) -> &BTreeMap<ExprId, Expr> {
        &self.exprs
    }

    pub fn blocks(&self) -> &BTreeMap<BlockId, Block> {
        &self.blocks
    }

    pub const fn return_type(&self) -> ColumnType {
        self.ret
    }

    pub fn signature(&self) -> Signature {
        Signature::new(
            self.args.iter().map(|&(arg, ..)| arg).collect(),
            self.args.iter().map(|&(.., flags)| flags).collect(),
            self.ret,
        )
    }

    pub fn optimize(&mut self, layout_cache: &RowLayoutCache) {
        // self.remove_redundant_casts();
        self.remove_unit_memory_operations(layout_cache);
        // self.remove_noop_copies(layout_cache)
        // TODO: Tree shaking to remove unreachable nodes
    }

    fn remove_redundant_casts(&mut self) {
        todo!()
    }

    fn remove_noop_copies(&mut self, layout_cache: &RowLayoutCache) {
        let mut scalar_exprs = BTreeSet::new();
        let mut row_exprs = BTreeMap::new();
        for &(layout, expr, _) in &self.args {
            row_exprs.insert(expr, layout);
        }

        let mut substitutions = BTreeMap::new();

        // FIXME: Doesn't work for back edges/loops
        let mut stack = vec![self.entry_block];
        while let Some(block_id) = stack.pop() {
            let block = self.blocks.get_mut(&block_id).unwrap();

            block.body.retain(|&expr_id| {
                match &self.exprs[&expr_id] {
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

                    Expr::CopyVal(copy) => {
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

            match &mut block.terminator {
                Terminator::Return(_) => {}
                Terminator::Jump(jump) => stack.push(jump.target()),
                Terminator::Branch(branch) => stack.extend([branch.truthy(), branch.falsy()]),
            }
        }

        if !substitutions.is_empty() {
            for block in self.blocks.values_mut() {
                for expr_id in block.body() {
                    match self.exprs.get_mut(expr_id).unwrap() {
                        Expr::Load(_) => todo!(),
                        Expr::Store(_) => todo!(),
                        Expr::BinOp(_) => todo!(),
                        Expr::UnaryOp(_) => todo!(),
                        Expr::IsNull(_) => todo!(),
                        Expr::CopyVal(_) => todo!(),
                        Expr::NullRow(_) => todo!(),
                        Expr::SetNull(_) => todo!(),
                        Expr::Constant(_) => {}
                        Expr::CopyRowTo(_) => todo!(),
                        Expr::UninitRow(_) => todo!(),
                        Expr::Cast(_) => todo!(),
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
        for &(layout, expr, _) in &self.args {
            row_exprs.insert(expr, layout);
        }

        // FIXME: Doesn't work for back edges/loops
        let mut stack = vec![self.entry_block];
        while let Some(block_id) = stack.pop() {
            let block = self.blocks.get_mut(&block_id).unwrap();

            block.body.retain(|&expr_id| match &self.exprs[&expr_id] {
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

                Expr::CopyVal(copy) => {
                    if unit_exprs.contains(&copy.value()) {
                        unit_exprs.insert(expr_id);
                        false
                    } else {
                        true
                    }
                }

                _ => true,
            });

            match &mut block.terminator {
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
    args: Vec<(LayoutId, ExprId, InputFlags)>,
    ret: ColumnType,
    entry_block: Option<BlockId>,

    exprs: BTreeMap<ExprId, Expr>,
    blocks: BTreeMap<BlockId, Block>,
    unsealed_blocks: BTreeMap<BlockId, UnsealedBlock>,

    current: Option<UnsealedBlock>,

    expr_id: ExprIdGen,
    block_id: BlockIdGen,
}

impl FunctionBuilder {
    pub fn new() -> Self {
        Self {
            args: Vec::new(),
            ret: ColumnType::Unit,
            entry_block: None,
            exprs: BTreeMap::new(),
            blocks: BTreeMap::new(),
            unsealed_blocks: BTreeMap::new(),
            current: None,
            expr_id: ExprIdGen::new(),
            block_id: BlockIdGen::new(),
        }
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
        self.args.push((input_row, arg_id, flags));
        arg_id
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

    pub fn add_expr<E>(&mut self, expr: E) -> ExprId
    where
        E: Into<Expr>,
    {
        let expr = expr.into();
        let expr_id = self.expr_id.next();
        self.exprs.insert(expr_id, expr);

        let current = self.current_block();
        current.body.push(expr_id);

        expr_id
    }

    /// Check if `value` is null
    pub fn is_null(&mut self, value: ExprId, row: usize) -> ExprId {
        self.add_expr(IsNull::new(value, row))
    }

    pub fn set_null<N>(&mut self, target: ExprId, row: usize, is_null: N)
    where
        N: Into<RValue>,
    {
        self.add_expr(SetNull::new(target, row, is_null.into()));
    }

    pub fn load(&mut self, target: ExprId, row: usize) -> ExprId {
        self.add_expr(Load::new(target, row))
    }

    pub fn store<V>(&mut self, target: ExprId, row: usize, value: V)
    where
        V: Into<RValue>,
    {
        self.add_expr(Store::new(target, row, value.into()));
    }

    pub fn and(&mut self, lhs: ExprId, rhs: ExprId) -> ExprId {
        self.add_expr(BinaryOp::new(lhs, rhs, BinaryOpKind::And))
    }

    pub fn or(&mut self, lhs: ExprId, rhs: ExprId) -> ExprId {
        self.add_expr(BinaryOp::new(lhs, rhs, BinaryOpKind::Or))
    }

    pub fn add(&mut self, lhs: ExprId, rhs: ExprId) -> ExprId {
        self.add_expr(BinaryOp::new(lhs, rhs, BinaryOpKind::Add))
    }

    pub fn mul(&mut self, lhs: ExprId, rhs: ExprId) -> ExprId {
        self.add_expr(BinaryOp::new(lhs, rhs, BinaryOpKind::Mul))
    }

    pub fn constant(&mut self, constant: Constant) -> ExprId {
        self.add_expr(constant)
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
    pub fn seal(&mut self) {
        let current = self
            .current
            .take()
            .expect("Called `FunctionBuilder::seal()` without a current block");

        let terminator = current.terminator.unwrap_or_else(|| {
            panic!(
                "Called `FunctionBuilder::build()` with unfinished blocks: {} has no terminator",
                current.id,
            )
        });

        let block = Block {
            id: current.id,
            body: current.body,
            terminator,
        };
        self.blocks.insert(current.id, block);
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
            assert_eq!(id, block.id);
        }
        for (&id, block) in &self.unsealed_blocks {
            assert_eq!(id, block.id);
        }

        for block in self.blocks.values() {
            self.validate_terminator(block.id, &block.terminator);
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
            let terminator = match current.terminator {
                Some(terminator) => terminator,
                None => panic!("Called `FunctionBuilder::build()` with unfinished blocks: {} has no terminator", current.id),
            };
            let block = Block {
                id: current.id,
                body: current.body,
                terminator,
            };
            self.blocks.insert(current.id, block);
        }

        // Finish all unsealed blocks
        for (id, unsealed) in self.unsealed_blocks {
            debug_assert_eq!(id, unsealed.id);

            let terminator = match unsealed.terminator {
                Some(terminator) => terminator,
                None => panic!("Called `FunctionBuilder::build()` with unfinished blocks: {id} has no terminator"),
            };
            let block = Block {
                id: unsealed.id,
                body: unsealed.body,
                terminator,
            };
            self.blocks.insert(id, block);
        }

        Function {
            args: self.args,
            ret: self.ret,
            entry_block,
            exprs: self.exprs,
            blocks: self.blocks,
        }
    }
}
