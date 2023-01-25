mod intrinsics;
mod layout;
mod utils;
mod vtable;

pub use layout::{BitSetType, InvalidBitsetType, NativeLayout, NativeType};
pub use vtable::{LayoutVTable, VTable};

use crate::{
    codegen::{
        intrinsics::{ImportIntrinsics, Intrinsics},
        layout::LayoutConfig,
        utils::FunctionBuilderExt,
    },
    ir::{
        BinOpKind, BlockId, ColumnType, Constant, Expr, ExprId, Function, InputFlags, LayoutCache,
        LayoutId, RValue, RowLayout, Signature, Terminator,
    },
};
use cranelift::{
    codegen::{
        ir::{GlobalValue, StackSlot, UserFuncName},
        Context,
    },
    prelude::{
        isa::{self, TargetIsa},
        settings, types, AbiParam, Block as ClifBlock, Configurable, FunctionBuilder,
        FunctionBuilderContext, InstBuilder, IntCC, MemFlags, Signature as ClifSignature,
        StackSlotData, StackSlotKind, TrapCode, Value,
    },
};
use cranelift_jit::{JITBuilder, JITModule};
use cranelift_module::{DataContext, DataId, FuncId, Module};
use std::{
    cell::Ref,
    collections::{BTreeMap, BTreeSet, HashMap},
};
use target_lexicon::Triple;

const TRAP_NULL_PTR: TrapCode = TrapCode::User(500);

// TODO: Pretty function debugging <https://github.com/bjorn3/rustc_codegen_cranelift/blob/master/src/pretty_clif.rs>

pub struct NativeLayoutCache {
    pub(crate) layout_cache: LayoutCache,
    layouts: BTreeMap<LayoutId, NativeLayout>,
    layout_config: LayoutConfig,
}

impl NativeLayoutCache {
    fn new(layout_cache: LayoutCache, layout_config: LayoutConfig) -> Self {
        Self {
            layout_cache,
            layouts: BTreeMap::new(),
            layout_config,
        }
    }

    pub fn get_layouts(&mut self, layout_id: LayoutId) -> (&NativeLayout, Ref<'_, RowLayout>) {
        let row_layout = self.layout_cache.get(layout_id);
        let native_layout = self
            .layouts
            .entry(layout_id)
            .or_insert_with(|| NativeLayout::from_row(&row_layout, &self.layout_config));

        (native_layout, row_layout)
    }

    pub fn compute(&mut self, layout_id: LayoutId) -> &NativeLayout {
        self.layouts.entry(layout_id).or_insert_with(|| {
            let row_layout = self.layout_cache.get(layout_id);
            NativeLayout::from_row(&row_layout, &self.layout_config)
        })
    }

    pub fn row_layout_cache(&self) -> &LayoutCache {
        &self.layout_cache
    }
}

// TODO: Config option for packed null flags or 1 byte booleans
#[derive(Debug, Clone)]
pub struct CodegenConfig {
    /// Whether or not to add invariant assertions into generated code
    pub debug_assertions: bool,
    /// Whether or not comparing floats should use normal float comparisons or
    /// `totalOrder` comparisons as defined in the IEEE 754 (2008 revision)
    /// floating point standard. Using `false` may be slightly faster, but
    /// may result in strange behavior around subnormals
    pub total_float_comparisons: bool,
    /// Whether or not layouts should be optimized
    pub optimize_layouts: bool,
}

impl CodegenConfig {
    pub const fn new(
        debug_assertions: bool,
        total_float_comparisons: bool,
        optimize_layouts: bool,
    ) -> Self {
        Self {
            debug_assertions,
            total_float_comparisons,
            optimize_layouts,
        }
    }

    pub const fn with_debug_assertions(mut self, debug_assertions: bool) -> Self {
        self.debug_assertions = debug_assertions;
        self
    }

    pub const fn with_total_float_comparisons(mut self, total_float_comparisons: bool) -> Self {
        self.total_float_comparisons = total_float_comparisons;
        self
    }

    pub const fn with_optimize_layouts(mut self, optimize_layouts: bool) -> Self {
        self.optimize_layouts = optimize_layouts;
        self
    }

    pub const fn debug() -> Self {
        Self {
            debug_assertions: true,
            total_float_comparisons: true,
            optimize_layouts: true,
        }
    }

    pub const fn release() -> Self {
        Self {
            debug_assertions: false,
            total_float_comparisons: true,
            optimize_layouts: true,
        }
    }
}

impl Default for CodegenConfig {
    fn default() -> Self {
        Self {
            debug_assertions: false,
            total_float_comparisons: true,
            optimize_layouts: true,
        }
    }
}

pub struct Codegen {
    layout_cache: NativeLayoutCache,
    module: JITModule,
    module_ctx: Context,
    data_ctx: DataContext,
    function_ctx: FunctionBuilderContext,
    config: CodegenConfig,
    intrinsics: Intrinsics,
    vtables: BTreeMap<LayoutId, LayoutVTable>,
    data: HashMap<Box<[u8]>, DataId>,
}

impl Codegen {
    pub fn new(layout_cache: LayoutCache, config: CodegenConfig) -> Self {
        let target = Self::target_isa();

        let layout_cache = NativeLayoutCache::new(
            layout_cache,
            LayoutConfig::new(target.frontend_config(), config.optimize_layouts),
        );

        let mut builder = JITBuilder::with_isa(
            target,
            // TODO: We may want custom impls of things
            cranelift_module::default_libcall_names(),
        );
        Intrinsics::register(&mut builder);

        let mut module = JITModule::new(builder);
        let intrinsics = Intrinsics::new(&mut module);
        let module_ctx = module.make_context();

        Self {
            layout_cache,
            module,
            module_ctx,
            data_ctx: DataContext::new(),
            function_ctx: FunctionBuilderContext::new(),
            config,
            intrinsics,
            vtables: BTreeMap::new(),
            data: HashMap::new(),
        }
    }

    fn target_isa() -> Box<dyn TargetIsa> {
        let mut settings = settings::builder();

        let options = &[
            ("opt_level", "speed"),
            ("enable_simd", "true"),
            ("use_egraphs", "true"),
            ("unwind_info", "true"),
            ("enable_verifier", "true"),
            ("enable_jump_tables", "true"),
            ("enable_alias_analysis", "true"),
            ("use_colocated_libcalls", "false"),
            // FIXME: Set back to true once the x64 backend supports it.
            ("is_pic", "false"),
        ];
        for (name, value) in options {
            settings.set(name, value).unwrap();
        }

        isa::lookup(Triple::host())
            .unwrap()
            .finish(settings::Flags::new(settings))
            .unwrap()
    }

    pub fn native_layout_cache(&self) -> &NativeLayoutCache {
        &self.layout_cache
    }

    pub fn finalize_definitions(mut self) -> (JITModule, NativeLayoutCache) {
        self.module.finalize_definitions().unwrap();
        (self.module, self.layout_cache)
    }

    pub fn codegen_func(&mut self, function: &Function) -> FuncId {
        let sig = self.build_signature(&function.signature());

        let func_id = self.module.declare_anonymous_function(&sig).unwrap();
        let func_name = UserFuncName::user(0, func_id.as_u32());

        self.module_ctx.func.signature = sig;
        self.module_ctx.func.name = func_name;

        {
            let mut ctx = CodegenCtx::new(
                &mut self.module,
                &mut self.data_ctx,
                &mut self.data,
                &mut self.layout_cache,
                self.intrinsics.import(),
            );
            let mut builder =
                FunctionBuilder::new(&mut self.module_ctx.func, &mut self.function_ctx);

            ctx.add_function_params(function, &mut builder);

            // FIXME: Doesn't work for loops/back edges
            let mut stack = vec![function.entry_block()];
            let mut visited = BTreeSet::new();
            while let Some(block_id) = stack.pop() {
                if !visited.insert(block_id) {
                    continue;
                }

                let block = ctx.switch_to_block(block_id, &mut builder);

                let block_contents = &function.blocks()[&block_id];

                for &expr_id in block_contents.body() {
                    let expr = &function.exprs()[&expr_id];

                    match expr {
                        Expr::BinOp(binop) => {
                            let lhs = ctx.exprs[&binop.lhs()];
                            let rhs = ctx.exprs[&binop.rhs()];

                            // FIXME: Doesn't handle floats
                            let value = match binop.kind() {
                                BinOpKind::Add => builder.ins().iadd(lhs, rhs),
                                BinOpKind::Sub => builder.ins().isub(lhs, rhs),
                                BinOpKind::Mul => builder.ins().imul(lhs, rhs),
                                BinOpKind::Eq => builder.ins().icmp(IntCC::Equal, lhs, rhs),
                                BinOpKind::Neq => builder.ins().icmp(IntCC::NotEqual, lhs, rhs),
                                BinOpKind::And => builder.ins().band(lhs, rhs),
                                BinOpKind::Or => builder.ins().bor(lhs, rhs),
                            };

                            ctx.add_expr(expr_id, value, None);
                        }

                        Expr::Store(store) => {
                            debug_assert!(!ctx.is_readonly(store.target()));

                            let layout = ctx.layout_of(store.target());
                            let offset = layout.offset_of(store.row());

                            let value = match store.value() {
                                RValue::Expr(expr) => ctx.exprs[expr],
                                RValue::Imm(imm) => ctx.constant(imm, &mut builder),
                            };

                            if let Some(&slot) = ctx.stack_slots.get(&store.target()) {
                                builder.ins().stack_store(value, slot, offset as i32);

                            // If it's not a stack slot it must be a pointer via
                            // function parameter
                            } else {
                                let addr = ctx.exprs[&store.target()];
                                let flags = MemFlags::trusted();
                                builder.ins().store(flags, value, addr, offset as i32);
                            }
                        }

                        Expr::Load(load) => {
                            let layout = ctx.layout_of(load.source());
                            let offset = layout.offset_of(load.row());
                            let ty = layout
                                .type_of(load.row())
                                .native_type(&ctx.module.isa().frontend_config());

                            let value = if let Some(&slot) = ctx.stack_slots.get(&load.source()) {
                                builder.ins().stack_load(ty, slot, offset as i32)

                            // If it's not a stack slot it must be a pointer via
                            // function parameter
                            } else {
                                let addr = ctx.exprs[&load.source()];

                                let mut flags = MemFlags::trusted();
                                if ctx.is_readonly(load.source()) {
                                    flags.set_readonly();
                                }

                                builder.ins().load(ty, flags, addr, offset as i32)
                            };

                            ctx.add_expr(expr_id, value, None);
                        }

                        // TODO: If the given nullish flag is the only occupant of its bitset,
                        // we can simplify the codegen a good bit by just checking if the bitset
                        // is or isn't equal to zero
                        Expr::IsNull(is_null) => {
                            let layout = ctx.layout_of(is_null.value());
                            let (bitset_ty, bitset_offset, bit_idx) =
                                layout.nullability_of(is_null.row());
                            let bitset_ty =
                                bitset_ty.native_type(&ctx.module.isa().frontend_config());

                            let bitset = if let Some(&slot) = ctx.stack_slots.get(&is_null.value())
                            {
                                builder
                                    .ins()
                                    .stack_load(bitset_ty, slot, bitset_offset as i32)

                            // If it's not a stack slot it must be a pointer via
                            // function parameter
                            } else {
                                let addr = ctx.exprs[&is_null.value()];

                                let mut flags = MemFlags::trusted();
                                if ctx.is_readonly(is_null.value()) {
                                    flags.set_readonly();
                                }

                                builder
                                    .ins()
                                    .load(bitset_ty, flags, addr, bitset_offset as i32)
                            };

                            let masked = builder.ins().band_imm(bitset, 1i64 << bit_idx);
                            let is_null = builder.ins().icmp_imm(
                                // if ctx.null_sigil.is_one() {
                                //     IntCC::NotEqual
                                // } else {
                                //     IntCC::Equal
                                // },
                                IntCC::NotEqual,
                                masked,
                                0,
                            );
                            ctx.add_expr(expr_id, is_null, None);
                        }

                        // TODO: If the given nullish flag is the only occupant of its bitset,
                        // we can simplify the codegen a good bit by just storing a value with
                        // our desired configuration instead of loading the bitset, toggling the
                        // bit and then storing the bitset
                        Expr::SetNull(set_null) => {
                            debug_assert!(!ctx.is_readonly(set_null.target()));

                            let layout = ctx.layout_of(set_null.target());
                            let (bitset_ty, bitset_offset, bit_idx) =
                                layout.nullability_of(set_null.row());
                            let bitset_ty =
                                bitset_ty.native_type(&ctx.module.isa().frontend_config());

                            // Load the bitset's current value
                            let bitset =
                                if let Some(&slot) = ctx.stack_slots.get(&set_null.target()) {
                                    builder
                                        .ins()
                                        .stack_load(bitset_ty, slot, bitset_offset as i32)

                                // If it's not a stack slot it must be a pointer via
                                // function parameter
                                } else {
                                    let addr = ctx.exprs[&set_null.target()];
                                    // TODO: We can probably add the `notrap` and `aligned` flags for
                                    // stores
                                    let flags = MemFlags::new();
                                    builder
                                        .ins()
                                        .load(bitset_ty, flags, addr, bitset_offset as i32)
                                };

                            // Set the given bit, these use `bitset | (1 << bit_idx)` to set the bit
                            // and `bitset & !(1 << bit_idx)` to unset the bit.
                            // For non-constant inputs this computes both results and then selects
                            // the correct one based off of the value of
                            // `is_null`
                            let masked = match set_null.is_null() {
                                RValue::Expr(expr) => {
                                    let is_null = ctx.exprs[expr];
                                    let mask = 1i64 << bit_idx;
                                    let set_bit = builder.ins().bor_imm(bitset, mask);
                                    let unset_bit = builder.ins().band_imm(bitset, !mask);

                                    // if ctx.null_sigil.is_one() {
                                    //     builder.ins().select(is_null, set_bit, unset_bit)
                                    // } else {
                                    //     builder.ins().select(is_null, unset_bit, set_bit)
                                    // }
                                    builder.ins().select(is_null, set_bit, unset_bit)
                                }

                                &RValue::Imm(Constant::Bool(set_null)) => {
                                    let mask = 1i64 << bit_idx;

                                    // if (ctx.null_sigil.is_one() && set_null)
                                    //     || (ctx.null_sigil.is_zero() && !set_null)
                                    // {
                                    //     builder.ins().bor_imm(bitset, mask)
                                    // } else {
                                    //     builder.ins().band_imm(bitset, !mask)
                                    // }
                                    if set_null {
                                        builder.ins().bor_imm(bitset, mask)
                                    } else {
                                        builder.ins().band_imm(bitset, !mask)
                                    }
                                }
                                RValue::Imm(_) => unreachable!(),
                            };

                            // Store the bit with mask set
                            if let Some(&slot) = ctx.stack_slots.get(&set_null.target()) {
                                builder
                                    .ins()
                                    .stack_store(masked, slot, bitset_offset as i32);

                            // If it's not a stack slot it must be a pointer via
                            // function parameter
                            } else {
                                let addr = ctx.exprs[&set_null.target()];
                                let flags = MemFlags::trusted();
                                builder
                                    .ins()
                                    .store(flags, masked, addr, bitset_offset as i32);
                            }
                        }

                        Expr::CopyRowTo(copy_row) => {
                            debug_assert_eq!(
                                ctx.layout_id(copy_row.src()),
                                ctx.layout_id(copy_row.dest()),
                            );
                            debug_assert_eq!(ctx.layout_id(copy_row.src()), copy_row.layout());
                            debug_assert!(!ctx.is_readonly(copy_row.dest()));

                            // Ignore noop copies
                            if copy_row.src() == copy_row.dest() {
                                continue;
                            }

                            let src = if let Some(&slot) = ctx.stack_slots.get(&copy_row.src()) {
                                builder
                                    .ins()
                                    .stack_addr(ctx.module.isa().pointer_type(), slot, 0)
                            } else {
                                ctx.exprs[&copy_row.src()]
                            };

                            let dest = if let Some(&slot) = ctx.stack_slots.get(&copy_row.dest()) {
                                builder
                                    .ins()
                                    .stack_addr(ctx.module.isa().pointer_type(), slot, 0)
                            } else {
                                ctx.exprs[&copy_row.dest()]
                            };

                            let layout = ctx.layout_cache.compute(copy_row.layout());

                            // FIXME: We probably want to replace this with our own impl, it
                            // currently just emits a memcpy call on
                            // sizes greater than 4 when an inlined impl
                            // would perform much better
                            builder.emit_small_memory_copy(
                                ctx.module.isa().frontend_config(),
                                dest,
                                src,
                                layout.size() as u64,
                                layout.align() as u8,
                                layout.align() as u8,
                                true,
                                MemFlags::trusted(),
                            );
                        }

                        Expr::CopyVal(copy_val) => {
                            let value = ctx.exprs[&copy_val.value()];
                            let layout = ctx.expr_layouts.get(&copy_val.value()).copied();

                            let value = if copy_val.ty() == ColumnType::String {
                                let clone_string =
                                    ctx.imports.string_clone(ctx.module, builder.func);
                                builder.call_fn(clone_string, &[value])
                            } else {
                                value
                            };

                            ctx.add_expr(expr_id, value, layout);
                        }

                        // TODO: There's a couple optimizations we can do here:
                        // - If our type is sub-word size, we can simply use a constant that's been
                        //   set to all ones. This does require generalized handling for rows that
                        //   aren't addressable, but that's a good thing we need in the future
                        //   anyways
                        // - If the null bits are sufficiently sparse and the row is sufficiently
                        //   large (something something heuristics) then memset-ing the entire thing
                        //   is just a waste and we should instead use more focused stores
                        // - If our type doesn't contain any null flags, we don't need to do
                        //   anything and can simply leave the row uninitialized. This does require
                        //   that we actually follow our contract that `NullRow` only means "any
                        //   well-defined `IsNull` call returns `true`" and don't depend on it being
                        //   initialized to any degree further than what is required by that
                        Expr::NullRow(null) => {
                            // Create a stack slot
                            let slot =
                                ctx.stack_slot_for_layout(expr_id, null.layout(), &mut builder);
                            // Get the address of the stack slot
                            let addr =
                                builder
                                    .ins()
                                    .stack_addr(ctx.module.isa().pointer_type(), slot, 0);

                            // Fill the stack slot with whatever our nullish sigil is
                            // ctx.memset_imm(addr, ctx.null_sigil as u8, null.layout());
                            ctx.memset_imm(addr, 0b1111_1111, null.layout(), &mut builder);
                        }

                        Expr::UninitRow(uninit) => {
                            ctx.stack_slot_for_layout(expr_id, uninit.layout(), &mut builder);
                        }

                        Expr::Constant(constant) => {
                            let value = ctx.constant(constant, &mut builder);
                            ctx.add_expr(expr_id, value, None);
                        }
                    }
                }

                ctx.codegen_terminator(block_contents.terminator(), &mut stack, &mut builder);

                builder.seal_block(block);
            }

            // Seal any unfinished blocks
            builder.seal_all_blocks();
            // Finish building the function
            builder.finalize();
        }

        self.finalize_function(func_id);

        func_id
    }

    fn build_signature(&mut self, signature: &Signature) -> ClifSignature {
        let mut sig = self.module.make_signature();

        if let Some(return_type) = NativeType::from_column_type(signature.ret()) {
            sig.returns.push(AbiParam::new(
                return_type.native_type(&self.module.isa().frontend_config()),
            ));
        }

        let ptr = self.module.isa().pointer_type();
        for _arg in signature.args() {
            sig.params.push(AbiParam::new(ptr));
        }

        sig
    }

    #[cfg(test)]
    pub(crate) fn layout_for(&mut self, layout_id: LayoutId) -> &NativeLayout {
        self.layout_cache.compute(layout_id)
    }

    #[track_caller]
    fn finalize_function(&mut self, func_id: FuncId) {
        // println!("pre-opt: {}", self.module_ctx.func.display());
        self.module
            .define_function(func_id, &mut self.module_ctx)
            .unwrap();

        self.module_ctx.optimize(self.module.isa()).unwrap();
        // println!("post-opt: {}", self.module_ctx.func.display());

        self.module.clear_context(&mut self.module_ctx);
    }
}

struct CodegenCtx<'a> {
    module: &'a mut JITModule,
    data_ctx: &'a mut DataContext,
    // TODO: Use an interner
    data: &'a mut HashMap<Box<[u8]>, DataId>,
    layout_cache: &'a mut NativeLayoutCache,
    blocks: BTreeMap<BlockId, ClifBlock>,
    exprs: BTreeMap<ExprId, Value>,
    expr_layouts: BTreeMap<ExprId, LayoutId>,
    stack_slots: BTreeMap<ExprId, StackSlot>,
    function_inputs: BTreeMap<ExprId, InputFlags>,
    imports: ImportIntrinsics,
    data_imports: BTreeMap<DataId, GlobalValue>,
}

impl<'a> CodegenCtx<'a> {
    fn new(
        module: &'a mut JITModule,
        data_ctx: &'a mut DataContext,
        data: &'a mut HashMap<Box<[u8]>, DataId>,
        layout_cache: &'a mut NativeLayoutCache,
        imports: ImportIntrinsics,
    ) -> Self {
        Self {
            module,
            data_ctx,
            data,
            layout_cache,
            blocks: BTreeMap::new(),
            exprs: BTreeMap::new(),
            expr_layouts: BTreeMap::new(),
            stack_slots: BTreeMap::new(),
            function_inputs: BTreeMap::new(),
            imports,
            data_imports: BTreeMap::new(),
        }
    }

    fn add_function_params(&mut self, function: &Function, builder: &mut FunctionBuilder<'_>) {
        // Create the entry block
        let entry_block = self.switch_to_block(function.entry_block(), builder);
        // Add the function params as block params
        builder.append_block_params_for_function_params(entry_block);

        // Add each param as an expression and register all function param flags
        assert_eq!(
            function.args().len(),
            builder.block_params(entry_block).len()
        );
        for (idx, &(layout, expr_id, flags)) in function.args().iter().enumerate() {
            let param = builder.block_params(entry_block)[idx];
            self.add_expr(expr_id, param, layout);

            self.function_inputs.insert(expr_id, flags);
        }
    }

    /// Returns `true` if the given expression is a `readonly` row
    fn is_readonly(&self, expr: ExprId) -> bool {
        self.function_inputs
            .get(&expr)
            .map_or(false, InputFlags::is_readonly)
    }

    fn create_data<D>(&mut self, data: D) -> DataId
    where
        D: Into<Box<[u8]>>,
    {
        let data = data.into();
        *self.data.entry(data).or_insert_with_key(|data| {
            self.data_ctx.define(data.to_owned());
            let data_id = self.module.declare_anonymous_data(false, false).unwrap();
            self.module.define_data(data_id, self.data_ctx).unwrap();
            self.data_ctx.clear();
            data_id
        })
    }

    fn import_data(&mut self, data_id: DataId, builder: &mut FunctionBuilder<'_>) -> GlobalValue {
        *self
            .data_imports
            .entry(data_id)
            .or_insert_with(|| self.module.declare_data_in_func(data_id, builder.func))
    }

    fn constant(&mut self, constant: &Constant, builder: &mut FunctionBuilder<'_>) -> Value {
        if constant.is_string() {
            self.strconst(constant, builder)
        } else if constant.is_float() {
            self.fconst(constant, builder)
        } else if constant.is_int() || constant.is_bool() {
            self.iconst(constant, builder)
        } else {
            unreachable!("cannot codegen for unit constants: {constant:?}")
        }
    }

    // FIXME: This isn't really what we want, we either need to distinguish between constant
    // strings and dynamic strings (preferable, causes less allocation) or just unconditionally
    // turn constant strings into `ThinStr`s while also keeping track of them so we can deallocate them
    fn strconst(&mut self, constant: &Constant, builder: &mut FunctionBuilder<'_>) -> Value {
        if let Constant::String(string) = constant {
            // Define the data backing the string
            let string_data = self.create_data(string.as_bytes());

            // Import the global value into the function
            let string_value = self.import_data(string_data, builder);

            // Get the value of the string
            builder
                .ins()
                .symbol_value(self.module.isa().pointer_type(), string_value)
        } else {
            unreachable!()
        }
    }

    fn fconst(&mut self, constant: &Constant, builder: &mut FunctionBuilder<'_>) -> Value {
        if let Constant::F32(float) = *constant {
            builder.ins().f32const(float)
        } else if let Constant::F64(double) = *constant {
            builder.ins().f64const(double)
        } else {
            unreachable!()
        }
    }

    fn iconst(&mut self, constant: &Constant, builder: &mut FunctionBuilder<'_>) -> Value {
        let ty = match *constant {
            Constant::U32(_) | Constant::I32(_) => types::I32,
            Constant::U64(_) | Constant::I64(_) => types::I64,
            Constant::Bool(_) => types::I8,

            Constant::Unit | Constant::F32(_) | Constant::F64(_) | Constant::String(_) => {
                unreachable!()
            }
        };

        let val = match *constant {
            Constant::U32(int) => int as i64,
            Constant::I32(int) => int as i64,
            Constant::U64(int) => int as i64,
            Constant::I64(int) => int,
            Constant::Bool(bool) => bool as i64,

            Constant::Unit | Constant::F32(_) | Constant::F64(_) | Constant::String(_) => {
                unreachable!()
            }
        };

        builder.ins().iconst(ty, val)
    }

    fn add_expr<L>(&mut self, expr_id: ExprId, value: Value, layout: L)
    where
        L: Into<Option<LayoutId>>,
    {
        let prev = self.exprs.insert(expr_id, value);
        debug_assert!(prev.is_none());

        if let Some(layout) = layout.into() {
            let prev = self.expr_layouts.insert(expr_id, layout);
            debug_assert!(prev.is_none());
        }
    }

    fn add_stack_slot(&mut self, expr_id: ExprId, slot: StackSlot, layout: LayoutId) {
        let prev = self.stack_slots.insert(expr_id, slot);
        debug_assert!(prev.is_none());

        let prev = self.expr_layouts.insert(expr_id, layout);
        debug_assert!(prev.is_none());
    }

    fn layout_id(&self, expr_id: ExprId) -> LayoutId {
        self.expr_layouts[&expr_id]
    }

    fn layout_of(&mut self, expr_id: ExprId) -> &NativeLayout {
        self.layout_cache.compute(self.layout_id(expr_id))
    }

    /// Returns `block`, creating it if it doesn't yet exist
    fn block(&mut self, block: BlockId, builder: &mut FunctionBuilder<'_>) -> ClifBlock {
        *self
            .blocks
            .entry(block)
            .or_insert_with(|| builder.create_block())
    }

    /// Switches to `block`, creating it if it doesn't yet exist
    fn switch_to_block(&mut self, block: BlockId, builder: &mut FunctionBuilder<'_>) -> ClifBlock {
        let block = self.block(block, builder);
        builder.switch_to_block(block);
        block
    }

    fn codegen_terminator(
        &mut self,
        terminator: &Terminator,
        stack: &mut Vec<BlockId>,
        builder: &mut FunctionBuilder<'_>,
    ) {
        match terminator {
            Terminator::Return(ret) => match ret.value() {
                RValue::Expr(expr) => {
                    let value = self.exprs[expr];
                    builder.ins().return_(&[value]);
                }

                RValue::Imm(imm) => {
                    if imm.is_unit() {
                        builder.ins().return_(&[]);
                    } else if imm.is_string() {
                        // How do we return strings from functions? Should we ever
                        // actually allow them to be returned directly? (Probably not)
                        todo!()
                    } else {
                        let value = self.constant(imm, builder);
                        builder.ins().return_(&[value]);
                    }
                }
            },

            Terminator::Jump(jump) => {
                let target = self.block(jump.target(), builder);

                // FIXME: bb args
                builder.ins().jump(target, &[]);

                stack.push(jump.target());
            }

            Terminator::Branch(branch) => {
                let truthy = self.block(branch.truthy(), builder);
                let falsy = self.block(branch.falsy(), builder);

                let cond = match branch.cond() {
                    RValue::Expr(expr) => self.exprs[expr],
                    RValue::Imm(_) => todo!(),
                };

                // FIXME: bb args
                builder.ins().brz(cond, truthy, &[]);
                builder.ins().jump(falsy, &[]);

                stack.extend([branch.truthy(), branch.falsy()]);
            }
        }
    }

    fn stack_slot_for_layout(
        &mut self,
        expr_id: ExprId,
        layout: LayoutId,
        builder: &mut FunctionBuilder<'_>,
    ) -> StackSlot {
        let layout_size = self.layout_cache.compute(layout).size();
        // TODO: Slot alignment?
        let slot = builder
            .create_sized_stack_slot(StackSlotData::new(StackSlotKind::ExplicitSlot, layout_size));

        self.add_stack_slot(expr_id, slot, layout);

        slot
    }

    /// Sets the given `buffer` to be filled with `value` bytes
    /// according to `layout`'s size and alignment
    fn memset_imm(
        &mut self,
        buffer: Value,
        value: u8,
        layout: LayoutId,
        builder: &mut FunctionBuilder<'_>,
    ) {
        let layout = self.layout_cache.compute(layout);

        // FIXME: We probably want to replace this with our own impl, it currently
        // just emits a memset call on sizes greater than 4 when an inlined impl
        // would perform much better
        builder.emit_small_memset(
            self.module.isa().frontend_config(),
            buffer,
            value,
            layout.size() as u64,
            layout.align().try_into().unwrap(),
            MemFlags::trusted(),
        );
    }
}

/// The kind of null sigil for us to use
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub enum NullSigil {
    /// Makes unset bits mean the corresponding row is null
    Zero = 0,
    /// Makes set bits mean the corresponding row is null
    #[default]
    One = 1,
}

impl NullSigil {
    /// Returns `true` if the null sigil is [`Zero`].
    ///
    /// [`Zero`]: NullSigil::Zero
    #[must_use]
    pub const fn is_zero(&self) -> bool {
        matches!(self, Self::Zero)
    }

    /// Returns `true` if the null sigil is [`One`].
    ///
    /// [`One`]: NullSigil::One
    #[must_use]
    pub const fn is_one(&self) -> bool {
        matches!(self, Self::One)
    }
}
