use crate::ir::{
    block::Block,
    block::{ParamType, UnsealedBlock},
    function::FuncArg,
    layout_cache::RowLayoutCache,
    BinaryOp, BinaryOpKind, BlockId, BlockIdGen, Branch, Cast, ColumnType, Constant, Copy,
    CopyRowTo, Expr, ExprId, ExprIdGen, Function, InputFlags, IsNull, Jump, LayoutId, Load, RValue,
    Return, Select, SetNull, Store, Terminator, UnaryOp, UnaryOpKind, UninitRow,
};
use petgraph::prelude::DiGraphMap;
use std::{collections::BTreeMap, mem::swap};

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
    expr_types: BTreeMap<ExprId, ParamType>,
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
        self.set_expr_type(arg_id, input_row);
        arg_id
    }

    pub fn add_block_param<P>(&mut self, block_id: BlockId, param_ty: P) -> ExprId
    where
        P: Into<ParamType>,
    {
        let param_ty = param_ty.into();
        let param_id = self.expr_id.next();

        self.current
            .as_mut()
            .filter(|block| block.id == block_id)
            .or_else(|| self.unsealed_blocks.get_mut(&block_id))
            .expect("called `FunctionBuilder::add_block_param()` without a current block")
            .params
            .push((param_id, param_ty));
        self.set_expr_type(param_id, param_ty);

        param_id
    }

    /// Returns the parameters of the given block
    pub fn block_params(&self, block: BlockId) -> &[(ExprId, ParamType)] {
        self.current
            .as_ref()
            .or_else(|| self.unsealed_blocks.get(&block))
            .and_then(|current| (current.id == block).then_some(&*current.params))
            .unwrap_or_else(|| {
                self.blocks
                    .get(&block)
                    .unwrap_or_else(|| panic!("called `FunctionBuilder::block_params()` on {block}, which doesn't exist"))
                    .params()
            })
    }

    fn set_expr_type(&mut self, expr_id: ExprId, ty: impl Into<ParamType>) {
        let prev = self.expr_types.insert(expr_id, ty.into());
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
            .expect_row("attempted to call `IsNull` on a scalar value");
        let expr = self.add_expr(IsNull::new(value, value_layout, column));
        self.set_expr_type(expr, ColumnType::Bool);
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
            .expect_row("attempted to call `SetNull` on a scalar value");
        self.add_expr(SetNull::new(target, target_layout, column, is_null.into()));
    }

    pub fn cast(&mut self, value: ExprId, dest_ty: ColumnType) -> ExprId {
        let value_ty = self
            .expr_types
            .get(&value)
            .unwrap_or_else(|| panic!("failed to get type of {value}"))
            .expect_column("attempted to call `Cast` on a row value");
        let expr = self.add_expr(Cast::new(value, value_ty, dest_ty));
        self.set_expr_type(expr, dest_ty);
        expr
    }

    pub fn copy_val(&mut self, value: ExprId) -> ExprId {
        let value_ty = self
            .expr_types
            .get(&value)
            .unwrap_or_else(|| panic!("failed to get type of {value}"))
            .expect_column("attempted to call `CopyVal` on a row value");
        let expr = self.add_expr(Copy::new(value, value_ty));
        self.set_expr_type(expr, value_ty);
        expr
    }

    pub fn load(&mut self, target: ExprId, column: usize) -> ExprId {
        let target_layout = self
            .expr_types
            .get(&target)
            .unwrap_or_else(|| panic!("failed to get type of {target}"))
            .expect_row("attempted to call `Load` on a scalar value");
        let column_type = self.layout_cache.get(target_layout).column_type(column);

        let expr = self.add_expr(Load::new(target, target_layout, column, column_type));
        self.set_expr_type(expr, column_type);

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
            .expect_row("attempted to call `Store` on a scalar value");
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

    pub fn binary_op(&mut self, lhs: ExprId, rhs: ExprId, kind: BinaryOpKind) -> ExprId {
        let lhs_ty = self
            .expr_types
            .get(&lhs)
            .unwrap_or_else(|| panic!("failed to get type of {lhs}"))
            .expect_column("attempted to call a binary op on a row value");
        if cfg!(debug_assertions) {
            let rhs_ty = self
                .expr_types
                .get(&rhs)
                .unwrap_or_else(|| panic!("failed to get type of {rhs}"))
                .expect_column("attempted to call a binary op on a row value");
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
            | BinaryOpKind::DivFloor
            | BinaryOpKind::Rem
            | BinaryOpKind::Mod
            | BinaryOpKind::ModFloor
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
        self.set_expr_type(expr, output_ty);

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
            .expect_column("attempted to call a unary op on a row value");

        let expr = self.add_expr(UnaryOp::new(value, value_ty, kind));

        // TODO: Implement all unary op kinds
        // TODO: Is this correct?
        // TODO: Make this a method on `UnaryOpKind` for reuse
        let output_ty = match kind {
            UnaryOpKind::StringLen => ColumnType::U64,

            _ => todo!(),
        };
        self.set_expr_type(expr, output_ty);

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
        self.set_expr_type(expr, constant_type);
        expr
    }

    pub fn uninit_row(&mut self, layout: LayoutId) -> ExprId {
        let expr = self.add_expr(UninitRow::new(layout));
        self.set_expr_type(expr, layout);
        expr
    }

    pub fn copy_row_to(&mut self, src: ExprId, dest: ExprId) {
        let src_layout = self
            .expr_types
            .get(&src)
            .unwrap_or_else(|| panic!("failed to get type of {src}"))
            .expect_row("attempted to call `CopyRowTo` on a scalar value");
        if cfg!(debug_assertions) {
            let dest_layout = self
                .expr_types
                .get(&dest)
                .unwrap_or_else(|| panic!("failed to get type of {dest}"))
                .expect_row("attempted to call `CopyRowTo` on a scalar value");
            assert_eq!(src_layout, dest_layout);
        }

        self.add_expr(CopyRowTo::new(src, dest, src_layout));
    }

    // pub fn call<F>(&mut self, func: F) -> ExprId  where F: Into<String>{
    //     self.add_expr(Call::new(
    //         func.into(),
    //         vec![first, second],
    //         vec![ArgType::Scalar(ColumnType::String); 2],
    //         ColumnType::String,
    //     ));
    // }

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

    /// Terminate the current block with a jump
    pub fn jump<P>(&mut self, target: BlockId, params: P)
    where
        P: Into<Vec<ExprId>>,
    {
        self.set_terminator(Jump::new(target, params.into()));
    }

    /// Terminate the current block with a branch
    pub fn branch<C, T, F>(
        &mut self,
        cond: C,
        truthy: BlockId,
        true_params: T,
        falsy: BlockId,
        false_params: F,
    ) where
        C: Into<RValue>,
        T: Into<Vec<ExprId>>,
        F: Into<Vec<ExprId>>,
    {
        self.set_terminator(Branch::new(
            cond.into(),
            truthy,
            true_params.into(),
            falsy,
            false_params.into(),
        ));
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
