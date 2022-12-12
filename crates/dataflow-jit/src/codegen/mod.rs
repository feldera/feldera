mod layout;

pub use layout::{Layout, Type};

use crate::ir::{Expr, Function, LayoutCache, LayoutId, Signature};
use cranelift::{
    codegen::ir::{Function as ClifFunction, UserExternalName, UserFuncName},
    prelude::{
        isa::TargetIsa, AbiParam, EntityRef, FunctionBuilder, FunctionBuilderContext,
        Signature as ClifSignature, Variable,
    },
};
use std::collections::{BTreeMap, BTreeSet};

pub struct Codegen {
    target: Box<dyn TargetIsa>,
    layout_cache: LayoutCache,
    layouts: BTreeMap<LayoutId, Layout>,
    function_index: u32,
    ctx: FunctionBuilderContext,
}

impl Codegen {
    pub fn new(target: Box<dyn TargetIsa>, layout_cache: LayoutCache) -> Self {
        Self {
            target,
            layout_cache,
            layouts: BTreeMap::new(),
            function_index: 0,
            ctx: FunctionBuilderContext::new(),
        }
    }

    pub fn codegen_func(&mut self, function: &Function) {
        let sig = self.build_signature(&function.signature());

        let name = self.next_user_func_name();
        let mut func = ClifFunction::with_name_signature(name, sig);

        let mut blocks = BTreeMap::new();
        let mut exprs = BTreeMap::new();

        let mut var_gen = 0;
        let ptr_type = self.target.pointer_type();

        {
            let mut builder = FunctionBuilder::new(&mut func, &mut self.ctx);

            for &(_layout, expr, _) in function.args() {
                let var = Variable::new(var_gen);
                var_gen += 1;

                builder.declare_var(var, ptr_type);
                exprs.insert(expr, var);
            }

            // FIXME: Doesn't work for loops/back edges
            let mut stack = vec![function.entry_block()];
            let mut visited = BTreeSet::new();
            while let Some(block_id) = stack.pop() {
                if !visited.insert(block_id) {
                    continue;
                }

                let block = builder.create_block();
                blocks.insert(block_id, block);
                builder.switch_to_block(block);

                let block_contents = &function.blocks()[&block_id];

                for &expr_id in block_contents.body() {
                    let expr = &function.exprs()[&expr_id];

                    match expr {
                        Expr::BinOp(_) => todo!(),
                        Expr::Insert(_) => todo!(),
                        Expr::IsNull(_) => todo!(),
                        Expr::CopyVal(_) => todo!(),
                        Expr::Extract(_) => todo!(),
                        Expr::NullRow(_) => todo!(),
                        Expr::SetNull(_) => todo!(),
                        Expr::Constant(_) => todo!(),
                        Expr::CopyRowTo(_) => todo!(),
                        Expr::UninitRow(_) => todo!(),
                    }
                }

                builder.seal_block(block);
            }
        }
    }

    fn next_user_func_name(&mut self) -> UserFuncName {
        let name = UserFuncName::User(UserExternalName::new(0, self.function_index));
        self.function_index += 1;
        name
    }

    fn build_signature(&mut self, signature: &Signature) -> ClifSignature {
        let mut sig = ClifSignature::new(self.target.default_call_conv());

        let ret = self.compute_layout(signature.ret());
        if !ret.is_unit() {
            sig.returns.push(AbiParam::new(self.target.pointer_type()));
        }

        for _arg in signature.args() {
            sig.params.push(AbiParam::new(self.target.pointer_type()));
        }

        sig
    }

    fn compute_layout(&mut self, layout_id: LayoutId) -> &Layout {
        self.layouts.entry(layout_id).or_insert_with(|| {
            let row_layout = self.layout_cache.get(layout_id);
            Layout::from_row(&row_layout, &self.target.frontend_config())
        })
    }
}
