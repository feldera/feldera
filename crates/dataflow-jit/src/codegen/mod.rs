mod layout;

pub use layout::{Layout, Type};

use crate::ir::{
    BinOpKind, BlockId, Constant, Expr, ExprId, Function, LayoutCache, LayoutId, RValue, Signature,
    Terminator,
};
use cranelift::{
    codegen::ir::{Function as ClifFunction, StackSlot, UserExternalName, UserFuncName},
    prelude::{
        isa::{TargetFrontendConfig, TargetIsa},
        types, AbiParam, Block as ClifBlock, EntityRef, FunctionBuilder, FunctionBuilderContext,
        InstBuilder, IntCC, MemFlags, Signature as ClifSignature, StackSlotData, StackSlotKind,
        Value, Variable,
    },
};
use std::collections::{BTreeMap, BTreeSet};

struct NativeLayoutCache {
    layout_cache: LayoutCache,
    layouts: BTreeMap<LayoutId, Layout>,
    frontend_config: TargetFrontendConfig,
}

impl NativeLayoutCache {
    pub fn new(layout_cache: LayoutCache, frontend_config: TargetFrontendConfig) -> Self {
        Self {
            layout_cache,
            layouts: BTreeMap::new(),
            frontend_config,
        }
    }

    pub fn compute(&mut self, layout_id: LayoutId) -> &Layout {
        self.layouts.entry(layout_id).or_insert_with(|| {
            let row_layout = self.layout_cache.get(layout_id);
            Layout::from_row(&row_layout, &self.frontend_config)
        })
    }
}

pub struct Codegen {
    target: Box<dyn TargetIsa>,
    layout_cache: NativeLayoutCache,
    function_index: u32,
    ctx: FunctionBuilderContext,
}

struct Ctx<'a> {
    target: &'a dyn TargetIsa,
    layout_cache: &'a mut NativeLayoutCache,
    builder: FunctionBuilder<'a>,
    blocks: BTreeMap<BlockId, ClifBlock>,
    exprs: BTreeMap<ExprId, Value>,
    expr_layouts: BTreeMap<ExprId, LayoutId>,
    stack_slots: BTreeMap<ExprId, StackSlot>,
}

impl<'a> Ctx<'a> {
    fn new(
        target: &'a dyn TargetIsa,
        layout_cache: &'a mut NativeLayoutCache,
        builder: FunctionBuilder<'a>,
    ) -> Self {
        Self {
            target,
            layout_cache,
            builder,
            blocks: BTreeMap::new(),
            exprs: BTreeMap::new(),
            expr_layouts: BTreeMap::new(),
            stack_slots: BTreeMap::new(),
        }
    }
}

impl Codegen {
    pub fn new(target: Box<dyn TargetIsa>, layout_cache: LayoutCache) -> Self {
        let layout_cache = NativeLayoutCache::new(layout_cache, target.frontend_config());

        Self {
            target,
            layout_cache,
            function_index: 0,
            ctx: FunctionBuilderContext::new(),
        }
    }

    pub fn codegen_func(&mut self, function: &Function) {
        let sig = self.build_signature(&function.signature());

        let name = self.next_user_func_name();
        let mut func = ClifFunction::with_name_signature(name, sig);

        let ptr_type = self.target.pointer_type();

        {
            let mut ctx = Ctx::new(
                &*self.target,
                &mut self.layout_cache,
                FunctionBuilder::new(&mut func, &mut self.ctx),
            );

            let entry_block = ctx.builder.create_block();
            ctx.builder.switch_to_block(entry_block);
            ctx.builder
                .append_block_params_for_function_params(entry_block);
            ctx.blocks.insert(function.entry_block(), entry_block);

            for (idx, &(layout, expr, _)) in function.args().iter().enumerate() {
                let value = ctx.builder.block_params(entry_block)[idx];

                let prev = ctx.exprs.insert(expr, value);
                debug_assert!(prev.is_none());

                let prev = ctx.expr_layouts.insert(expr, layout);
                debug_assert!(prev.is_none());
            }

            // FIXME: Doesn't work for loops/back edges
            let mut stack = vec![function.entry_block()];
            let mut visited = BTreeSet::new();
            while let Some(block_id) = stack.pop() {
                if !visited.insert(block_id) {
                    continue;
                }

                let block = *ctx
                    .blocks
                    .entry(block_id)
                    .or_insert_with(|| ctx.builder.create_block());
                ctx.builder.switch_to_block(block);

                let block_contents = &function.blocks()[&block_id];

                for &expr_id in block_contents.body() {
                    let expr = &function.exprs()[&expr_id];

                    match expr {
                        Expr::BinOp(binop) => {
                            let lhs = ctx.exprs[&binop.lhs()];
                            let rhs = ctx.exprs[&binop.rhs()];

                            // FIXME: Doesn't handle floats
                            let value = match binop.kind() {
                                BinOpKind::Add => ctx.builder.ins().iadd(lhs, rhs),
                                BinOpKind::Sub => ctx.builder.ins().isub(lhs, rhs),
                                BinOpKind::Mul => todo!(),
                                BinOpKind::Eq => todo!(),
                                BinOpKind::Neq => todo!(),
                                BinOpKind::And => ctx.builder.ins().band(lhs, rhs),
                                BinOpKind::Or => ctx.builder.ins().bor(lhs, rhs),
                            };

                            let prev = ctx.exprs.insert(expr_id, value);
                            debug_assert!(prev.is_none());
                        }

                        Expr::Insert(insert) => {
                            let layout =
                                ctx.layout_cache.compute(ctx.expr_layouts[&insert.target()]);
                            let offset = layout.row_offset(insert.row());

                            let value = match insert.value() {
                                RValue::Expr(expr) => ctx.exprs[expr],
                                RValue::Imm(imm) => {
                                    if let Constant::String(_) = imm {
                                        todo!()
                                    } else if let Constant::F32(float) = *imm {
                                        ctx.builder.ins().f32const(float)
                                    } else if let Constant::F64(double) = *imm {
                                        ctx.builder.ins().f64const(double)
                                    } else {
                                        let ty = match *imm {
                                            Constant::U32(_) | Constant::I32(_) => types::I32,
                                            Constant::U64(_) | Constant::I64(_) => types::I64,
                                            Constant::Bool(_) => types::I8,

                                            Constant::Unit
                                            | Constant::F32(_)
                                            | Constant::F64(_)
                                            | Constant::String(_) => unreachable!(),
                                        };

                                        let val = match *imm {
                                            Constant::U32(int) => int as i64,
                                            Constant::I32(int) => int as i64,
                                            Constant::U64(int) => int as i64,
                                            Constant::I64(int) => int,
                                            Constant::Bool(bool) => bool as i64,

                                            Constant::Unit
                                            | Constant::F32(_)
                                            | Constant::F64(_)
                                            | Constant::String(_) => unreachable!(),
                                        };

                                        ctx.builder.ins().iconst(ty, val)
                                    }
                                }
                            };

                            if let Some(&slot) = ctx.stack_slots.get(&insert.target()) {
                                ctx.builder.ins().stack_store(value, slot, offset as i32);

                            // If it's not a stack slot it must be a pointer via function parameter
                            } else {
                                let addr = ctx.exprs[&insert.target()];
                                // TODO: We can probably add the `notrap` and `aligned` flags for stores
                                let flags = MemFlags::new();
                                ctx.builder.ins().store(flags, value, addr, offset as i32);
                            }
                        }

                        Expr::Extract(extract) => {
                            let layout = ctx
                                .layout_cache
                                .compute(ctx.expr_layouts[&extract.source()]);
                            let offset = layout.row_offset(extract.row());
                            let ty = layout
                                .row_type(extract.row())
                                .native_type(&self.target.frontend_config());

                            let value = if let Some(&slot) = ctx.stack_slots.get(&extract.source())
                            {
                                ctx.builder.ins().stack_load(ty, slot, offset as i32)

                            // If it's not a stack slot it must be a pointer via function parameter
                            } else {
                                let addr = ctx.exprs[&extract.source()];
                                // TODO: We can probably add the `notrap` and `aligned` flags for stores
                                let flags = MemFlags::new();
                                ctx.builder.ins().load(ty, flags, addr, offset as i32)
                            };

                            let prev = ctx.exprs.insert(expr_id, value);
                            debug_assert!(prev.is_none());
                        }

                        Expr::IsNull(is_null) => {
                            let layout =
                                ctx.layout_cache.compute(ctx.expr_layouts[&is_null.value()]);
                            let (bitset_ty, bitset_offset, bit_idx) =
                                layout.row_nullability(is_null.row());
                            let bitset_ty = bitset_ty.native_type(&self.target.frontend_config());

                            let bitset = if let Some(&slot) = ctx.stack_slots.get(&is_null.value())
                            {
                                ctx.builder
                                    .ins()
                                    .stack_load(bitset_ty, slot, bitset_offset as i32)

                            // If it's not a stack slot it must be a pointer via function parameter
                            } else {
                                let addr = ctx.exprs[&is_null.value()];
                                // TODO: We can probably add the `notrap` and `aligned` flags for stores
                                let flags = MemFlags::new();
                                ctx.builder
                                    .ins()
                                    .load(bitset_ty, flags, addr, bitset_offset as i32)
                            };

                            let masked = ctx.builder.ins().band_imm(bitset, 1i64 << bit_idx);
                            let is_null = ctx.builder.ins().icmp_imm(IntCC::NotEqual, masked, 0);

                            let prev = ctx.exprs.insert(expr_id, is_null);
                            debug_assert!(prev.is_none());
                        }

                        Expr::SetNull(set_null) => {
                            let layout = ctx
                                .layout_cache
                                .compute(ctx.expr_layouts[&set_null.target()]);
                            let (bitset_ty, bitset_offset, bit_idx) =
                                layout.row_nullability(set_null.row());
                            let bitset_ty = bitset_ty.native_type(&self.target.frontend_config());

                            // Load the bitset's current value
                            let bitset = if let Some(&slot) =
                                ctx.stack_slots.get(&set_null.target())
                            {
                                ctx.builder
                                    .ins()
                                    .stack_load(bitset_ty, slot, bitset_offset as i32)

                            // If it's not a stack slot it must be a pointer via function parameter
                            } else {
                                let addr = ctx.exprs[&set_null.target()];
                                // TODO: We can probably add the `notrap` and `aligned` flags for stores
                                let flags = MemFlags::new();
                                ctx.builder
                                    .ins()
                                    .load(bitset_ty, flags, addr, bitset_offset as i32)
                            };

                            // Set the given bit, these use `bitset | (1 << bit_idx)` to set the bit
                            // and `bitset & !(1 << bit_idx)` to unset the bit.
                            // For non-constant inputs this computes both results and then selects the
                            // correct one based off of the value of `is_null`
                            let masked = match set_null.is_null() {
                                RValue::Expr(expr) => {
                                    let is_null = ctx.exprs[expr];
                                    let mask = 1i64 << bit_idx;
                                    let set_bit = ctx.builder.ins().bor_imm(bitset, mask);
                                    let unset_bit = ctx.builder.ins().band_imm(bitset, !mask);
                                    ctx.builder.ins().select(is_null, set_bit, unset_bit)
                                }

                                &RValue::Imm(Constant::Bool(bool)) => {
                                    let mask = 1i64 << bit_idx;
                                    if bool {
                                        ctx.builder.ins().bor_imm(bitset, mask)
                                    } else {
                                        ctx.builder.ins().band_imm(bitset, !mask)
                                    }
                                }
                                RValue::Imm(_) => unreachable!(),
                            };

                            // Store the bit with mask set
                            if let Some(&slot) = ctx.stack_slots.get(&set_null.target()) {
                                ctx.builder
                                    .ins()
                                    .stack_store(masked, slot, bitset_offset as i32);

                            // If it's not a stack slot it must be a pointer via function parameter
                            } else {
                                let addr = ctx.exprs[&set_null.target()];
                                // TODO: We can probably add the `notrap` and `aligned` flags for stores
                                let flags = MemFlags::new();
                                ctx.builder
                                    .ins()
                                    .store(flags, masked, addr, bitset_offset as i32);
                            }
                        }

                        Expr::CopyRowTo(copy_row) => {
                            // Ignore noop copies
                            if copy_row.from() == copy_row.to() {
                                continue;
                            }

                            let src = if let Some(&slot) = ctx.stack_slots.get(&copy_row.from()) {
                                ctx.builder
                                    .ins()
                                    .stack_addr(ctx.target.pointer_type(), slot, 0)
                            } else {
                                ctx.exprs[&copy_row.from()]
                            };

                            let dest = if let Some(&slot) = ctx.stack_slots.get(&copy_row.to()) {
                                ctx.builder
                                    .ins()
                                    .stack_addr(ctx.target.pointer_type(), slot, 0)
                            } else {
                                ctx.exprs[&copy_row.to()]
                            };

                            let layout = ctx.layout_cache.compute(copy_row.layout());
                            let flags = MemFlags::new().with_aligned().with_notrap();

                            // TODO: We probably want to replace this with our own impl, it currently
                            // just emits a memcpy call on sizes greater than 4 when an inlined impl
                            // would perform much better
                            ctx.builder.emit_small_memory_copy(
                                ctx.target.frontend_config(),
                                dest,
                                src,
                                layout.size() as u64,
                                layout.align() as u8,
                                layout.align() as u8,
                                true,
                                flags,
                            );
                        }

                        Expr::CopyVal(_) => {}
                        Expr::NullRow(_) => {}

                        Expr::Constant(constant) => {
                            let value = if let Constant::String(_) = constant {
                                todo!()
                            } else if let Constant::F32(float) = *constant {
                                ctx.builder.ins().f32const(float)
                            } else if let Constant::F64(double) = *constant {
                                ctx.builder.ins().f64const(double)
                            } else {
                                let ty = match *constant {
                                    Constant::U32(_) | Constant::I32(_) => types::I32,
                                    Constant::U64(_) | Constant::I64(_) => types::I64,
                                    Constant::Bool(_) => types::I8,

                                    Constant::Unit
                                    | Constant::F32(_)
                                    | Constant::F64(_)
                                    | Constant::String(_) => unreachable!(),
                                };

                                let val = match *constant {
                                    Constant::U32(int) => int as i64,
                                    Constant::I32(int) => int as i64,
                                    Constant::U64(int) => int as i64,
                                    Constant::I64(int) => int,
                                    Constant::Bool(bool) => bool as i64,

                                    Constant::Unit
                                    | Constant::F32(_)
                                    | Constant::F64(_)
                                    | Constant::String(_) => unreachable!(),
                                };

                                ctx.builder.ins().iconst(ty, val)
                            };

                            let prev = ctx.exprs.insert(expr_id, value);
                            debug_assert!(prev.is_none());
                        }

                        Expr::UninitRow(uninit) => {
                            let layout_size = ctx.layout_cache.compute(uninit.layout()).size();
                            // TODO: Slot alignment?
                            let slot = ctx.builder.create_sized_stack_slot(StackSlotData::new(
                                StackSlotKind::ExplicitSlot,
                                layout_size,
                            ));

                            let prev = ctx.stack_slots.insert(expr_id, slot);
                            debug_assert!(prev.is_none());

                            let prev = ctx.expr_layouts.insert(expr_id, uninit.layout());
                            debug_assert!(prev.is_none());
                        }
                    }
                }

                match block_contents.terminator() {
                    Terminator::Return(ret) => match ret.value() {
                        RValue::Expr(expr) => {
                            dbg!(expr);
                            let value = ctx.exprs[expr];
                            ctx.builder.ins().return_(&[value]);
                        }

                        RValue::Imm(imm) => match imm {
                            Constant::Unit => {
                                ctx.builder.ins().return_(&[]);
                            }
                            Constant::U32(_) => todo!(),
                            Constant::U64(_) => todo!(),
                            Constant::I32(_) => todo!(),
                            Constant::I64(_) => todo!(),
                            Constant::F32(_) => todo!(),
                            Constant::F64(_) => todo!(),
                            Constant::Bool(_) => todo!(),
                            Constant::String(_) => todo!(),
                        },
                    },

                    Terminator::Jump(jump) => {
                        let target = *ctx
                            .blocks
                            .entry(jump.target())
                            .or_insert_with(|| ctx.builder.create_block());
                        // FIXME: bb args
                        ctx.builder.ins().jump(target, &[]);
                    }

                    Terminator::Branch(branch) => {
                        let truthy = *ctx
                            .blocks
                            .entry(branch.truthy())
                            .or_insert_with(|| ctx.builder.create_block());
                        let falsy = *ctx
                            .blocks
                            .entry(branch.falsy())
                            .or_insert_with(|| ctx.builder.create_block());

                        let cond = match branch.cond() {
                            RValue::Expr(expr) => ctx.exprs[expr],
                            RValue::Imm(_) => todo!(),
                        };

                        // FIXME: bb args
                        ctx.builder.ins().brz(cond, truthy, &[]);
                        ctx.builder.ins().jump(falsy, &[]);
                    }
                }

                ctx.builder.seal_block(block);
            }

            // Seal any unfinished blocks
            ctx.builder.seal_all_blocks();
            // Finish building the function
            ctx.builder.finalize();
        }

        println!("{}", func.display());
    }

    fn next_user_func_name(&mut self) -> UserFuncName {
        let name = UserFuncName::User(UserExternalName::new(0, self.function_index));
        self.function_index += 1;
        name
    }

    fn build_signature(&mut self, signature: &Signature) -> ClifSignature {
        let mut sig = ClifSignature::new(self.target.default_call_conv());

        let ret = self.layout_cache.compute(signature.ret());
        if !ret.is_unit() {
            sig.returns.push(AbiParam::new(self.target.pointer_type()));
        }

        for _arg in signature.args() {
            sig.params.push(AbiParam::new(self.target.pointer_type()));
        }

        sig
    }
}
