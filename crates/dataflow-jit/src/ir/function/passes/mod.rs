mod unit_ops;

use crate::ir::{
    exprs::{visit::MapExprIds, Call, Nop, RowOrScalar},
    function::FuncArg,
    layout_cache::RowLayoutCache,
    pretty::{Arena, Pretty},
    ColumnType, Constant, Expr, ExprId, Function, Jump, LayoutId, RValue, Terminator,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    mem::take,
};

impl Function {
    // TODO: Tree shaking to remove unreachable nodes
    // TODO: Eliminate unused block parameters
    // TODO: Promote conditional writes to rows to block params
    // TODO: Remove redundant `Copy`, `Drop` and `Cast` calls
    // TODO: SROA (Scalar Reification of Aggregates)
    // TODO: Simplify branching (remove immediate jumps)
    // TODO: Struct-ify opt passes
    #[tracing::instrument(skip_all)]
    pub fn optimize(&mut self, layout_cache: &RowLayoutCache) {
        let arena = Arena::<()>::new();
        tracing::trace!(
            "Pre-opt:\n{}",
            Pretty::pretty(&*self, &arena, layout_cache).pretty(80),
        );

        self.warn_string_load_store_sequences();

        self.dce();
        self.remove_unit_memory_operations(layout_cache);
        self.deduplicate_input_loads();
        self.simplify_branches();
        self.truncate_zero();
        self.concat_empty_strings();
        self.dce();

        tracing::trace!(
            "Post-opt:\n{}",
            Pretty::pretty(&*self, &arena, layout_cache).pretty(80),
        );
    }

    fn warn_string_load_store_sequences(&self) {
        let (mut string_loads, mut string_stores) = (BTreeSet::new(), BTreeMap::new());

        // Find all string loads and stores
        for block in self.blocks.values() {
            for (expr_id, expr) in block.body() {
                if let Expr::Store(store) = expr {
                    if let Some(&value) = store.value().as_expr() {
                        if store.value_type().is_string() {
                            string_stores.insert(*expr_id, value);
                        }
                    }
                } else if let Expr::Load(load) = expr {
                    if load.column_type().is_string() {
                        string_loads.insert(*expr_id);
                    }
                }
            }
        }

        // Find the stores whose direct value is a string load
        for (store_id, store_value) in string_stores {
            if string_loads.contains(&store_value) {
                tracing::warn!("store {store_id} stores string directly from the load {store_value}, the string is not properly cloned");
            }
        }
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

        let mut replacements = BTreeMap::new();
        for block in self.blocks.values_mut() {
            for (expr_id, expr) in block.body_mut() {
                if let Expr::Call(call) = expr {
                    // TODO: `@dbsp.str.concat()` calls
                    if call.function() == "dbsp.str.concat_clone" {
                        call.args_mut().retain(|arg| !empty_strings.contains(arg));

                        // If the concat call has no non-empty values, simplify it to an empty
                        // string
                        if call.args().is_empty() {
                            tracing::debug!(
                                %expr_id,
                                "turned @dbsp.str.concat_clone() call into an empty string constant (both strings are empty)",
                            );
                            *expr = Expr::Constant(Constant::String(String::new()));

                        // If there's only a single non-empty value remaining,
                        // optimize the concat call to that value
                        } else if let &[arg] = call.args() {
                            replacements.insert(*expr_id, arg);
                            *expr = Expr::Nop(Nop::new());
                        }
                    }
                }
            }
        }

        let mut remap_ids = MapExprIds::new(|expr_id: &mut ExprId| {
            if let Some(&new) = replacements.get(expr_id) {
                *expr_id = new;
            }
        });
        self.apply_mut(&mut remap_ids);
    }

    // Turn all `@dbsp.str.truncate(string, 0)` calls into `@dbsp.str.clear(string)`
    // calls TODO: Eliminate all truncate/clear calls when the length is already
    // less than or equal to the target length
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
                            vec![RowOrScalar::Scalar(ColumnType::String)],
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
                        Terminator::Return(_) | Terminator::Unreachable => {}
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

                        // TODO: Should drop *really* count as a productive use, if all something does is get dropped we don't want to retain it
                        Expr::Drop(drop) => {
                            used.insert(drop.value());
                        }

                        // These contain no expressions
                        Expr::NullRow(_)
                        | Expr::Constant(_)
                        | Expr::UninitRow(_)
                        | Expr::Uninit(_)
                        | Expr::Nop(_) => {}
                    }
                }

                match block.terminator() {
                    Terminator::Jump(jump) => used.extend(jump.params().iter().copied()),

                    Terminator::Branch(branch) => {
                        used.extend(branch.true_params().iter().copied());
                        used.extend(branch.false_params().iter().copied());

                        if let &RValue::Expr(cond) = branch.cond() {
                            used.insert(cond);
                        }
                    }

                    Terminator::Return(ret) => {
                        if let &RValue::Expr(ret) = ret.value() {
                            used.insert(ret);
                        }
                    }

                    Terminator::Unreachable => {}
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

                    Expr::Drop(drop) => remap(drop.value_mut()),

                    // Constants contain no expressions
                    Expr::Constant(_) => {}

                    // These expressions operate exclusively on rows
                    Expr::CopyRowTo(_)
                    | Expr::NullRow(_)
                    | Expr::UninitRow(_)
                    | Expr::Uninit(_)
                    | Expr::Nop(_) => {}
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

    fn simplify_branches(&mut self) {
        // TODO: Consume const prop dataflow graph and turn conditional branches with
        // constant conditions into unconditional ones
        // TODO: Simplify `select` calls

        // Replace any branches that have identical true/false targets with an
        // unconditional jump
        for block in self.blocks.values_mut() {
            if let Some((target, params)) =
                block.terminator_mut().as_branch_mut().and_then(|branch| {
                    branch
                        .targets_are_identical()
                        .then(|| (branch.truthy(), take(branch.true_params_mut())))
                })
            {
                *block.terminator_mut() = Terminator::Jump(Jump::new(target, params));
            }
        }
    }
}
