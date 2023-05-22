use crate::ir::{
    block::ParamType, exprs::ArgType, layout_cache::RowLayoutCache, ColumnType, Constant, Expr,
    Function, RValue, Return, Terminator,
};
use std::collections::{BTreeMap, BTreeSet};

impl Function {
    // TODO: Eliminate unit basic block args
    pub(super) fn remove_unit_memory_operations(&mut self, layout_cache: &RowLayoutCache) {
        let mut unit_exprs = BTreeSet::new();
        let mut row_exprs = BTreeMap::new();
        for arg in &self.args {
            row_exprs.insert(arg.id, arg.layout);
        }

        // FIXME: Doesn't work for back edges/loops
        let mut stack = vec![self.entry_block];
        while let Some(block_id) = stack.pop() {
            let block = self.blocks.get_mut(&block_id).unwrap();

            // Add all of the block's parameters
            for &(param_id, ty) in block.params() {
                match ty {
                    ParamType::Row(layout) => {
                        row_exprs.insert(param_id, layout);
                    }
                    ParamType::Column(ColumnType::Unit) => {
                        unit_exprs.insert(param_id);
                    }
                    ParamType::Column(_) => {}
                }
            }

            block.retain(|expr_id, expr| match expr {
                Expr::Constant(Constant::Unit) => {
                    unit_exprs.insert(expr_id);
                    false
                }
                Expr::Constant(_) => true,

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

                Expr::Select(select) => {
                    if unit_exprs.contains(&select.if_true())
                        || unit_exprs.contains(&select.if_false())
                    {
                        unit_exprs.insert(expr_id);
                        false
                    } else {
                        true
                    }
                }

                Expr::UninitRow(uninit) => {
                    row_exprs.insert(expr_id, uninit.layout());
                    !layout_cache.get(uninit.layout()).is_zero_sized()
                }

                Expr::NullRow(null) => {
                    row_exprs.insert(expr_id, null.layout());
                    !layout_cache.get(null.layout()).is_zero_sized()
                }

                Expr::CopyRowTo(copy_row) => !layout_cache.get(copy_row.layout()).is_zero_sized(),

                Expr::Uninit(uninit) => match uninit.value() {
                    ArgType::Row(layout) => {
                        row_exprs.insert(expr_id, layout);
                        !layout_cache.get(layout).is_zero_sized()
                    }

                    ArgType::Scalar(ColumnType::Unit) => {
                        unit_exprs.insert(expr_id);
                        false
                    }
                    ArgType::Scalar(_) => true,
                },

                // TODO: Eliminate/evaluate zst binops
                // Expr::BinOp(binop) if binop.operand_ty().is_unit() =>  {
                //
                // },
                Expr::BinOp(_) => true,

                Expr::Call(_)
                | Expr::Cast(_)
                | Expr::IsNull(_)
                | Expr::UnaryOp(_)
                | Expr::SetNull(_) => true,
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
                Terminator::Unreachable => {}
            }
        }
    }
}
