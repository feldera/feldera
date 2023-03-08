mod intrinsics;
mod layout;
mod layout_cache;
mod pretty_clif;
mod tests;
mod utils;
mod vtable;

pub use layout::{BitSetType, InvalidBitsetType, NativeLayout, NativeType};
pub use layout_cache::NativeLayoutCache;
pub use vtable::{LayoutVTable, VTable};

pub(crate) use layout::LayoutConfig;

use crate::{
    codegen::{
        intrinsics::{ImportIntrinsics, Intrinsics},
        layout::MemoryEntry,
        pretty_clif::CommentWriter,
        utils::FunctionBuilderExt,
    },
    ir::{
        BinaryOp, BinaryOpKind, BlockId, Branch, Cast, ColumnType, Constant, Expr, ExprId,
        Function, InputFlags, IsNull, LayoutId, Load, NullRow, RValue, RowLayoutCache, Select,
        SetNull, Signature, Terminator, UnaryOp, UnaryOpKind,
    },
    ThinStr,
};
use cranelift::{
    codegen::{
        ir::{GlobalValue, Inst, StackSlot, UserFuncName},
        Context,
    },
    prelude::{
        isa::{self, TargetFrontendConfig, TargetIsa},
        settings, types, AbiParam, Block as ClifBlock, Configurable, FloatCC, FunctionBuilder,
        FunctionBuilderContext, InstBuilder, IntCC, MemFlags, Signature as ClifSignature,
        StackSlotData, StackSlotKind, TrapCode, Type, Value,
    },
};
use cranelift_jit::{JITBuilder, JITModule};
use cranelift_module::{DataContext, DataId, FuncId, Module};
use std::{
    cell::{Ref, RefCell},
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, HashMap},
    rc::Rc,
    sync::Arc,
};
use target_lexicon::Triple;

const TRAP_NULL_PTR: TrapCode = TrapCode::User(0);
const TRAP_UNALIGNED_PTR: TrapCode = TrapCode::User(1);
const TRAP_INVALID_BOOL: TrapCode = TrapCode::User(2);

// TODO: Pretty function debugging https://github.com/bjorn3/rustc_codegen_cranelift/blob/master/src/pretty_clif.rs

// TODO: Config option for packed null flags or 1 byte booleans
#[derive(Debug, Clone, Copy)]
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
    /// Whether or not to annotate debugged functions with extra information
    pub clif_comments: bool,
    /// Whether or not to use saturating casts when converting floats to
    /// integers, if this option is disabled then float to int casts will
    /// trap when the float is NaN and if this option is enabled then float
    /// to int casts will yield zero when the float is NaN
    pub saturating_float_to_int_casts: bool,
}

impl CodegenConfig {
    pub const fn new(
        debug_assertions: bool,
        total_float_comparisons: bool,
        optimize_layouts: bool,
        clif_comments: bool,
        saturating_float_to_int_casts: bool,
    ) -> Self {
        Self {
            debug_assertions,
            total_float_comparisons,
            optimize_layouts,
            clif_comments,
            saturating_float_to_int_casts,
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

    pub const fn with_clif_comments(mut self, clif_comments: bool) -> Self {
        self.clif_comments = clif_comments;
        self
    }

    pub const fn with_saturating_float_to_int_casts(
        mut self,
        saturating_float_to_int_casts: bool,
    ) -> Self {
        self.saturating_float_to_int_casts = saturating_float_to_int_casts;
        self
    }

    pub const fn debug() -> Self {
        Self {
            debug_assertions: true,
            total_float_comparisons: true,
            optimize_layouts: true,
            clif_comments: true,
            saturating_float_to_int_casts: true,
        }
    }

    pub const fn release() -> Self {
        Self {
            debug_assertions: false,
            total_float_comparisons: true,
            optimize_layouts: true,
            clif_comments: false,
            saturating_float_to_int_casts: true,
        }
    }
}

impl Default for CodegenConfig {
    fn default() -> Self {
        Self::release()
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
    comment_writer: Option<Rc<RefCell<CommentWriter>>>,
}

impl Codegen {
    pub fn new(layout_cache: RowLayoutCache, config: CodegenConfig) -> Self {
        let target = Self::target_isa();
        tracing::info!(
            config = ?config,
            flags = %target.flags(),
            "creating code generator for {} {}",
            target.name(),
            target.triple(),
        );

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
            comment_writer: None,
        }
    }

    fn target_isa() -> Arc<dyn TargetIsa> {
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

    fn set_comment_writer(&mut self, symbol: &str, abi: &str) {
        self.comment_writer = self
            .config
            .clif_comments
            .then(|| Rc::new(RefCell::new(CommentWriter::new(symbol, abi))));
    }

    pub fn codegen_func(&mut self, symbol: &str, function: &Function) -> FuncId {
        let abi = function
            .signature()
            .display(self.layout_cache.row_layout_cache())
            .to_string();
        self.set_comment_writer(symbol, &abi);

        let sig = self.build_signature(&function.signature());

        let func_id = self.module.declare_anonymous_function(&sig).unwrap();
        let func_name = UserFuncName::user(0, func_id.as_u32());

        self.module_ctx.func.signature = sig;
        self.module_ctx.func.name = func_name;

        {
            let layout_cache = self.layout_cache.clone();
            let mut ctx = CodegenCtx::new(
                self.config,
                &mut self.module,
                &mut self.data_ctx,
                &mut self.data,
                self.layout_cache.clone(),
                self.intrinsics.import(self.comment_writer.clone()),
                self.comment_writer.clone(),
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

                // Ensure that all input pointers are valid
                // TODO: This may need changes if we ever have scalar function inputs
                if ctx.debug_assertions() && block_id == function.entry_block() {
                    for arg in function.args() {
                        debug_assert_eq!(ctx.layout_id(arg.id), arg.layout);
                        let layout = layout_cache.layout_of(arg.layout);
                        let arg = ctx.value(arg.id);
                        ctx.assert_ptr_valid(arg, layout.align(), &mut builder);
                    }
                }

                let block_contents = &function.blocks()[&block_id];

                for &(expr_id, ref expr) in block_contents.body() {
                    match expr {
                        Expr::Cast(cast) => ctx.cast(expr_id, cast, &mut builder),
                        Expr::BinOp(binop) => ctx.binary_op(expr_id, binop, &mut builder),
                        Expr::UnaryOp(unary) => ctx.unary_op(expr_id, unary, &mut builder),
                        Expr::Select(select) => ctx.select(expr_id, select, &mut builder),

                        Expr::Store(store) => {
                            debug_assert!(!ctx.is_readonly(store.target()));

                            let layout_id = ctx.layout_id(store.target());
                            let layout = layout_cache.layout_of(layout_id);
                            let offset = layout.offset_of(store.column());

                            let value = match store.value() {
                                RValue::Expr(expr) => ctx.exprs[expr],
                                RValue::Imm(imm) => ctx.constant(imm, &mut builder),
                            };

                            if let Some(&slot) = ctx.stack_slots.get(&store.target()) {
                                let inst = builder.ins().stack_store(value, slot, offset as i32);

                                if let Some(writer) = self.comment_writer.as_deref() {
                                    writer.borrow_mut().add_comment(
                                        inst,
                                        format!(
                                            "store {value} to {slot} at col {} (offset {offset}) of {:?}",
                                            store.column(),
                                            ctx.layout_cache.row_layout(layout_id),
                                        ),
                                    );
                                }

                            // If it's not a stack slot it must be a pointer via
                            // function parameter
                            } else {
                                let addr = ctx.exprs[&store.target()];
                                let flags = MemFlags::trusted();
                                let inst = builder.ins().store(flags, value, addr, offset as i32);

                                if let Some(writer) = self.comment_writer.as_deref() {
                                    writer.borrow_mut().add_comment(
                                        inst,
                                        format!(
                                            "store {value} to {addr} at col {} (offset {offset}) of {:?}",
                                            store.column(),
                                            ctx.layout_cache.row_layout(layout_id),
                                        ),
                                    );
                                }
                            }
                        }

                        Expr::Load(load) => ctx.load(expr_id, load, &layout_cache, &mut builder),
                        Expr::IsNull(is_null) => ctx.is_null(expr_id, is_null, &mut builder),
                        Expr::SetNull(set_null) => ctx.set_null(expr_id, set_null, &mut builder),

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

                            let layout = ctx.layout_cache.layout_of(copy_row.layout());

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
                            let ty = ctx.expr_types.get(&copy_val.value()).copied();
                            let layout = ctx.expr_layouts.get(&copy_val.value()).copied();

                            let value = if copy_val.value_ty() == ColumnType::String {
                                let clone_string =
                                    ctx.imports.string_clone(ctx.module, builder.func);
                                builder.call_fn(clone_string, &[value])
                            } else {
                                value
                            };

                            ctx.add_expr(expr_id, value, ty, layout);
                        }

                        Expr::NullRow(null) => ctx.null_row(expr_id, null, &mut builder),

                        Expr::UninitRow(uninit) => {
                            let slot =
                                ctx.stack_slot_for_layout(expr_id, uninit.layout(), &mut builder);

                            if let Some(writer) = self.comment_writer.as_deref() {
                                writer.borrow_mut().add_comment(
                                    slot,
                                    format!(
                                        "uninit row of layout {:?}",
                                        ctx.layout_cache.row_layout(uninit.layout()),
                                    ),
                                );
                            }
                        }

                        Expr::Constant(constant) => {
                            let value = ctx.constant(constant, &mut builder);
                            ctx.add_expr(expr_id, value, constant.column_type(), None);
                        }
                    }
                }

                ctx.terminator(block_contents.terminator(), &mut stack, &mut builder);

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

        let ptr = self.module.isa().pointer_type();
        for _arg in signature.args() {
            sig.params.push(AbiParam::new(ptr));
        }

        if let Some(return_type) = NativeType::from_column_type(signature.ret()) {
            sig.returns.push(AbiParam::new(
                return_type.native_type(&self.module.isa().frontend_config()),
            ));
        }

        sig
    }

    #[track_caller]
    fn finalize_function(&mut self, func_id: FuncId) {
        tracing::debug!(
            "finalizing {func_id} before optimization: \n{}",
            if let Some(writer) = self.comment_writer.as_ref() {
                let mut clif = String::new();
                cranelift::codegen::write::decorate_function(
                    &mut &*writer.borrow(),
                    &mut clif,
                    &self.module_ctx.func,
                )
                .unwrap();
                clif
            } else {
                self.module_ctx.func.display().to_string()
            },
        );

        self.module
            .define_function(func_id, &mut self.module_ctx)
            .unwrap();
        self.module_ctx.optimize(self.module.isa()).unwrap();

        tracing::debug!(
            "finalizing {func_id} after optimization: \n{}",
            if let Some(writer) = self.comment_writer.as_ref() {
                let mut clif = String::new();
                cranelift::codegen::write::decorate_function(
                    &mut &*writer.borrow(),
                    &mut clif,
                    &self.module_ctx.func,
                )
                .unwrap();
                clif
            } else {
                self.module_ctx.func.display().to_string()
            },
        );

        self.module.clear_context(&mut self.module_ctx);
        self.comment_writer = None;
    }
}

// TODO: Keep track of constants within `CodegenCtx` and remove `RValue`
struct CodegenCtx<'a> {
    config: CodegenConfig,
    module: &'a mut JITModule,
    data_ctx: &'a mut DataContext,
    // TODO: Use an interner
    data: &'a mut HashMap<Box<[u8]>, DataId>,
    layout_cache: NativeLayoutCache,
    blocks: BTreeMap<BlockId, ClifBlock>,
    exprs: BTreeMap<ExprId, Value>,
    expr_types: BTreeMap<ExprId, ColumnType>,
    expr_layouts: BTreeMap<ExprId, LayoutId>,
    stack_slots: BTreeMap<ExprId, StackSlot>,
    function_inputs: BTreeMap<ExprId, InputFlags>,
    imports: ImportIntrinsics,
    data_imports: BTreeMap<DataId, GlobalValue>,
    comment_writer: Option<Rc<RefCell<CommentWriter>>>,
}

impl<'a> CodegenCtx<'a> {
    fn new(
        config: CodegenConfig,
        module: &'a mut JITModule,
        data_ctx: &'a mut DataContext,
        data: &'a mut HashMap<Box<[u8]>, DataId>,
        layout_cache: NativeLayoutCache,
        imports: ImportIntrinsics,
        comment_writer: Option<Rc<RefCell<CommentWriter>>>,
    ) -> Self {
        Self {
            config,
            module,
            data_ctx,
            data,
            layout_cache,
            blocks: BTreeMap::new(),
            exprs: BTreeMap::new(),
            expr_types: BTreeMap::new(),
            expr_layouts: BTreeMap::new(),
            stack_slots: BTreeMap::new(),
            function_inputs: BTreeMap::new(),
            imports,
            data_imports: BTreeMap::new(),
            comment_writer,
        }
    }

    /// Get the value of an expression
    #[track_caller]
    fn value(&self, expr_id: ExprId) -> Value {
        self.exprs[&expr_id]
    }

    /// Get the type of an expression
    #[track_caller]
    fn expr_ty(&self, expr_id: ExprId) -> ColumnType {
        self.expr_types[&expr_id]
    }

    #[track_caller]
    fn clif_ty(&self, ty: ColumnType) -> Type {
        ty.native_type()
            .expect("called `CodegenCtx::clif_ty()` on a unit type")
            .native_type(&self.frontend_config())
    }

    /// Returns `true` if the given expression is a `readonly` row
    fn is_readonly(&self, expr: ExprId) -> bool {
        self.function_inputs
            .get(&expr)
            .map_or(false, InputFlags::is_readonly)
    }

    /// Returns `true` if debug assertions are enabled
    const fn debug_assertions(&self) -> bool {
        self.config.debug_assertions
    }

    /// Returns the current arch's pointer type
    fn pointer_type(&self) -> Type {
        self.module.isa().pointer_type()
    }

    /// Returns the current arch's [`TargetFrontendConfig`]
    fn frontend_config(&self) -> TargetFrontendConfig {
        self.module.isa().frontend_config()
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
        for (idx, arg) in function.args().iter().enumerate() {
            let param = builder.block_params(entry_block)[idx];
            self.add_expr(arg.id, param, None, arg.layout);
            self.function_inputs.insert(arg.id, arg.flags);
        }
    }

    /// Creates and imports the given string, returning a reference to its
    /// global value and the length of the string
    fn import_string<S>(&mut self, string: S, builder: &mut FunctionBuilder<'_>) -> (Value, Value)
    where
        S: Into<String>,
    {
        let string = string.into();
        let len = string.len();

        let global = if let Some(writer) = self.comment_writer.as_ref() {
            let comment = format!("string {string:?}");

            let data_id = *self
                .data
                .entry(string.into_bytes().into())
                .or_insert_with_key(|data| {
                    self.data_ctx.define(data.to_owned());
                    let data_id = self.module.declare_anonymous_data(false, false).unwrap();
                    self.module.define_data(data_id, self.data_ctx).unwrap();
                    self.data_ctx.clear();
                    data_id
                });

            let global = *self
                .data_imports
                .entry(data_id)
                .or_insert_with(|| self.module.declare_data_in_func(data_id, builder.func));
            writer.borrow_mut().add_comment(global, comment);

            global
        } else {
            let data_id = self.create_data(string.into_bytes());
            self.import_data(data_id, builder)
        };

        let ptr_type = self.pointer_type();
        // Get the length of the string
        let len = builder.ins().iconst(ptr_type, len as i64);
        // Get a pointer to the global value
        let ptr = builder.ins().symbol_value(ptr_type, global);

        (ptr, len)
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

    // FIXME: This isn't really what we want, we either need to distinguish between
    // constant strings and dynamic strings (preferable, causes less allocation)
    // or just unconditionally turn constant strings into `ThinStr`s while also
    // keeping track of them so we can deallocate them
    fn strconst(&mut self, constant: &Constant, builder: &mut FunctionBuilder<'_>) -> Value {
        if let Constant::String(string) = constant {
            // Define the data backing the string
            let string_data = self.create_data(string.as_bytes());

            // Import the global value into the function
            let string_value = self.import_data(string_data, builder);

            // Get the value of the string
            builder
                .ins()
                .symbol_value(self.pointer_type(), string_value)
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
        let ty = constant
            .column_type()
            .native_type()
            .expect("invalid constant type")
            .native_type(&self.frontend_config());

        let val = match *constant {
            Constant::U8(int) => int as i64,
            Constant::I8(int) => int as i64,
            Constant::U16(int) => int as i64,
            Constant::I16(int) => int as i64,
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

    fn add_expr<T, L>(&mut self, expr_id: ExprId, value: Value, ty: T, layout: L)
    where
        T: Into<Option<ColumnType>>,
        L: Into<Option<LayoutId>>,
    {
        let prev = self.exprs.insert(expr_id, value);
        debug_assert!(prev.is_none());

        if let Some(ty) = ty.into() {
            let prev = self.expr_types.insert(expr_id, ty);
            debug_assert!(prev.is_none());
        }

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

    /// Get the [`LayoutId`] associated with the given expression
    fn layout_id(&self, expr_id: ExprId) -> LayoutId {
        self.expr_layouts[&expr_id]
    }

    /// Get the layout of the given expression
    fn layout_of(&self, expr_id: ExprId) -> Ref<'_, NativeLayout> {
        self.layout_cache.layout_of(self.layout_id(expr_id))
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

    fn terminator(
        &mut self,
        terminator: &Terminator,
        stack: &mut Vec<BlockId>,
        builder: &mut FunctionBuilder<'_>,
    ) {
        match terminator {
            // TODO: If the return value is a boolean, call `.assert_bool_is_valid()` when debug
            // assertions are enabled
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

            Terminator::Branch(branch) => self.branch(branch, stack, builder),
        }
    }

    fn branch(
        &mut self,
        branch: &Branch,
        stack: &mut Vec<BlockId>,
        builder: &mut FunctionBuilder<'_>,
    ) {
        let truthy = self.block(branch.truthy(), builder);
        let falsy = self.block(branch.falsy(), builder);

        let cond = match branch.cond() {
            RValue::Expr(expr) => self.exprs[expr],
            RValue::Imm(_) => todo!(),
        };

        // Ensure that booleans are either true or false
        self.debug_assert_bool_is_valid(cond, builder);

        // FIXME: bb args
        builder.ins().brif(cond, truthy, &[], falsy, &[]);

        stack.extend([branch.truthy(), branch.falsy()]);
    }

    fn stack_slot_for_layout(
        &mut self,
        expr_id: ExprId,
        layout: LayoutId,
        builder: &mut FunctionBuilder<'_>,
    ) -> StackSlot {
        let layout_size = self.layout_cache.layout_of(layout).size();
        // TODO: Slot alignment?
        let slot = builder
            .create_sized_stack_slot(StackSlotData::new(StackSlotKind::ExplicitSlot, layout_size));

        self.add_stack_slot(expr_id, slot, layout);

        slot
    }

    /// Sets the given `buffer` to be filled with `value` bytes
    /// according to `layout`'s size and alignment
    #[allow(dead_code)]
    fn emit_small_memset(
        &mut self,
        buffer: Value,
        value: u8,
        layout: LayoutId,
        builder: &mut FunctionBuilder<'_>,
    ) {
        // Ensure that `buffer` is a pointer
        debug_assert_eq!(builder.func.dfg.value_type(buffer), self.pointer_type());

        let layout = self.layout_cache.layout_of(layout);

        // Ensure that the given buffer is a valid pointer
        self.debug_assert_ptr_valid(buffer, layout.align(), builder);

        // FIXME: We probably want to replace this with our own impl, it currently
        // just emits a memset call on sizes greater than 4 when an inlined impl
        // would perform much better
        builder.emit_small_memset(
            self.frontend_config(),
            buffer,
            value,
            layout.size() as u64,
            layout.align().try_into().unwrap(),
            MemFlags::trusted(),
        );
    }

    /// Traps if the given pointer is null or not aligned to `align`
    fn assert_ptr_valid(&self, ptr: Value, align: u32, builder: &mut FunctionBuilder<'_>) {
        self.assert_ptr_non_null(ptr, builder);
        self.assert_ptr_aligned(ptr, align, builder);
    }

    /// Traps if the given pointer is null or not aligned to `align` when debug
    /// assertions are enabled
    fn debug_assert_ptr_valid(&self, ptr: Value, align: u32, builder: &mut FunctionBuilder<'_>) {
        if self.debug_assertions() {
            self.assert_ptr_valid(ptr, align, builder);
        }
    }

    /// Traps if the given pointer is null
    fn assert_ptr_non_null(&self, ptr: Value, builder: &mut FunctionBuilder<'_>) {
        debug_assert_eq!(builder.func.dfg.value_type(ptr), self.pointer_type());

        // Trap if `ptr` is null
        let trap = builder.ins().trapz(ptr, TRAP_NULL_PTR);

        if let Some(writer) = self.comment_writer.as_deref() {
            writer
                .borrow_mut()
                .add_comment(trap, format!("trap if {ptr} is null"));
        }
    }

    /// Traps if the given pointer is not aligned to `align`
    fn assert_ptr_aligned(&self, ptr: Value, align: u32, builder: &mut FunctionBuilder<'_>) {
        debug_assert_eq!(builder.func.dfg.value_type(ptr), self.pointer_type());
        debug_assert_ne!(align, 0);
        debug_assert!(align.is_power_of_two());

        // We check alignment using `(align - 1) & ptr.addr() == 0` since we know that
        // `align` is always non-zero and a power of two. `align - 1` will never
        // underflow since align is always greater than zero
        let masked = builder.ins().band_imm(ptr, (align - 1) as i64);
        // Masked will be zero if the pointer is aligned, so we trap on non-zero values
        let trap = builder.ins().trapnz(masked, TRAP_UNALIGNED_PTR);

        if let Some(writer) = self.comment_writer.as_deref() {
            writer
                .borrow_mut()
                .add_comment(trap, format!("trap if {ptr} isn't aligned to {align}"));
        }
    }

    /// Traps if the given boolean value is not `true` or `false` when debug
    /// assertions are enabled
    fn debug_assert_bool_is_valid(&self, boolean: Value, builder: &mut FunctionBuilder<'_>) {
        if self.debug_assertions() {
            self.assert_bool_is_valid(boolean, builder);
        }
    }

    /// Traps if the given boolean value is not `true` or `false`
    fn assert_bool_is_valid(&self, boolean: Value, builder: &mut FunctionBuilder<'_>) {
        debug_assert!(builder.func.dfg.value_type(boolean).is_int());

        let is_true = builder.ins().icmp_imm(IntCC::Equal, boolean, true as i64);
        let is_false = builder.ins().icmp_imm(IntCC::Equal, boolean, false as i64);
        let is_true_or_false = builder.ins().bor(is_true, is_false);
        builder.ins().trapz(is_true_or_false, TRAP_INVALID_BOOL);

        if let Some(writer) = self.comment_writer.as_deref() {
            writer.borrow_mut().add_comment(
                builder.func.dfg.value_def(is_true).unwrap_inst(),
                format!(
                    "trap if {boolean} is not true ({}) or false ({})",
                    true as u8, false as u8,
                ),
            );
        }
    }

    fn load_from_row(
        &self,
        row_expr: ExprId,
        flags: MemFlags,
        ty: NativeType,
        offset: u32,
        builder: &mut FunctionBuilder<'_>,
    ) -> Value {
        let ty = ty.native_type(&self.frontend_config());

        // TODO: Assert alignment when debug assertions are enabled?

        if let Some(&slot) = self.stack_slots.get(&row_expr) {
            builder.ins().stack_load(ty, slot, offset as i32)

        // If it's not a stack slot it must be a pointer of some other kind
        } else {
            let addr = self.exprs[&row_expr];
            builder.ins().load(ty, flags, addr, offset as i32)
        }
    }

    // fn load_column(
    //     &self,
    //     row_expr: ExprId,
    //     column: usize,
    //     flags: MemFlags,
    //     builder: &mut FunctionBuilder<'_>,
    // ) -> Value {
    //     let layout = self.layout_of(row_expr);
    //     self.load_from_row(
    //         row_expr,
    //         flags,
    //         layout.type_of(column),
    //         layout.offset_of(column),
    //         builder,
    //     )
    // }

    // fn load_column_null_flag(
    //     &self,
    //     row_expr: ExprId,
    //     column: usize,
    //     flags: MemFlags,
    //     builder: &mut FunctionBuilder<'_>,
    // ) -> Value {
    // }

    fn store_to_row(
        &self,
        row_expr: ExprId,
        value: Value,
        flags: MemFlags,
        offset: u32,
        builder: &mut FunctionBuilder<'_>,
    ) -> Inst {
        debug_assert!(!flags.readonly(), "cannot store to readonly memory");

        // TODO: Assert alignment when debug assertions are enabled?mtuner

        // Attempt to get the pointer from a stack slot
        if let Some(&slot) = self.stack_slots.get(&row_expr) {
            builder.ins().stack_store(value, slot, offset as i32)

        // If it's not a stack slot it must be a pointer of some other kind
        } else {
            let addr = self.exprs[&row_expr];
            builder.ins().store(flags, value, addr, offset as i32)
        }
    }

    fn load(
        &mut self,
        expr_id: ExprId,
        load: &Load,
        layout_cache: &NativeLayoutCache,
        builder: &mut FunctionBuilder<'_>,
    ) {
        let layout_id = self.layout_id(load.source());
        let (layout, row_layout) = layout_cache.get_layouts(layout_id);
        let offset = layout.offset_of(load.column());
        let ty = layout
            .type_of(load.column())
            .native_type(&self.module.isa().frontend_config());

        let value = if let Some(&slot) = self.stack_slots.get(&load.source()) {
            let value = builder.ins().stack_load(ty, slot, offset as i32);

            if let Some(writer) = self.comment_writer.as_deref() {
                writer.borrow_mut().add_comment(
                    builder.func.dfg.value_def(value).unwrap_inst(),
                    format!(
                        "load {value} from {slot} at col {} (offset {offset}) of {:?}",
                        load.column(),
                        layout_cache.row_layout(layout_id),
                    ),
                );
            }

            value

        // If it's not a stack slot it must be a pointer via
        // function parameter
        } else {
            let mut flags = MemFlags::trusted();
            if self.is_readonly(load.source()) {
                flags.set_readonly();
            }

            let addr = self.exprs[&load.source()];
            let value = builder.ins().load(ty, flags, addr, offset as i32);

            if let Some(writer) = self.comment_writer.as_deref() {
                writer.borrow_mut().add_comment(
                    builder.func.dfg.value_def(value).unwrap_inst(),
                    format!(
                        "load {value} from {addr} at col {} (offset {offset}) of {:?}",
                        load.column(),
                        layout_cache.row_layout(layout_id),
                    ),
                );
            }

            value
        };

        self.add_expr(expr_id, value, row_layout.column_type(load.column()), None);
    }

    fn is_null(&mut self, expr_id: ExprId, is_null: &IsNull, builder: &mut FunctionBuilder<'_>) {
        let layout_id = self.layout_id(is_null.target());
        let layout = self.layout_cache.layout_of(layout_id);

        let (bitset_ty, bitset_offset, bit_idx) = layout.nullability_of(is_null.column());
        let bitset_occupants = layout.bitset_occupants(is_null.column());
        drop(layout);

        let mut flags = MemFlags::trusted();
        if self.is_readonly(is_null.target()) {
            flags.set_readonly();
        }

        let bitset = self.load_from_row(
            is_null.target(),
            flags,
            bitset_ty.into(),
            bitset_offset,
            builder,
        );

        if let Some(writer) = self.comment_writer.as_deref() {
            writer.borrow_mut().add_comment(
                builder.func.dfg.value_def(bitset).unwrap_inst(),
                format!(
                    "check if column {} of {} is null",
                    is_null.column(),
                    self.layout_cache.row_layout(layout_id),
                ),
            );
        }

        // If this column is the only occupant of the bitset
        let is_null = if bitset_occupants == 1 {
            // Note that we don't check if `bitset` is equal to `1` since all bits other
            // than the last are considered padding bits and have unspecified values
            // TODO: This could still be potentially problematic if the target bit is 0
            // but the rest are for some reason non-zero. Do we need to (internally)
            // constrain padding bits to being zeroed? If so, how do we
            // determine that we're the first writer to a given bitset in the
            // case of multi-tenant bitsets?
            builder.ins().icmp_imm(IntCC::NotEqual, bitset, 0)

        // If there's more than one occupant of the bitset
        } else {
            let masked = builder.ins().band_imm(bitset, 1i64 << bit_idx);
            builder.ins().icmp_imm(IntCC::NotEqual, masked, 0)
        };

        // Ensure that the produced boolean is valid
        self.debug_assert_bool_is_valid(is_null, builder);

        self.add_expr(expr_id, is_null, ColumnType::Bool, None);
    }

    // TODO: If the given nullish flag is the only occupant of its bitset,
    // we can simplify the codegen a good bit by just storing a value with
    // our desired configuration instead of loading the bitset, toggling the
    // bit and then storing the bitset
    fn set_null(
        &mut self,
        _expr_id: ExprId,
        set_null: &SetNull,
        builder: &mut FunctionBuilder<'_>,
    ) {
        debug_assert!(!self.is_readonly(set_null.target()));

        let layout = self.layout_of(set_null.target());
        let (bitset_ty, bitset_offset, bit_idx) = layout.nullability_of(set_null.column());
        let bitset_ty = bitset_ty.native_type();

        let mut first_expr = None;

        // If this column is the only occupant of the bitset we can set it directly
        let with_null_set = if layout.bitset_occupants(set_null.column()) == 1 {
            match set_null.is_null() {
                RValue::Expr(expr) => {
                    let is_null = self.exprs[expr];
                    // TODO: Assert that `is_null` is a valid boolean when debug_assertions are on
                    let null_ty = builder.func.dfg.value_type(is_null);
                    debug_assert_eq!(null_ty, types::I8);

                    // Make sure that is_null is the same type as the target bitset
                    match bitset_ty.bytes().cmp(&null_ty.bytes()) {
                        Ordering::Less => {
                            let reduce = builder.ins().ireduce(bitset_ty, is_null);
                            first_expr = Some(reduce);
                            reduce
                        }

                        Ordering::Equal => is_null,

                        Ordering::Greater => {
                            let extend = builder.ins().uextend(bitset_ty, is_null);
                            first_expr = Some(extend);
                            extend
                        }
                    }
                }

                &RValue::Imm(Constant::Bool(set_null)) => {
                    let constant = builder.ins().iconst(bitset_ty, set_null as i64);
                    first_expr = Some(constant);
                    constant
                }

                RValue::Imm(_) => unreachable!(),
            }

        // If there's more than one occupant of the bitset we have to load the
        // bitset's current value, set or unset our target bit and then store
        // the modified value
        } else {
            // Load the bitset's current value
            let bitset = if let Some(&slot) = self.stack_slots.get(&set_null.target()) {
                builder
                    .ins()
                    .stack_load(bitset_ty, slot, bitset_offset as i32)

            // If it's not a stack slot it must be a pointer via
            // function parameter
            } else {
                let addr = self.exprs[&set_null.target()];
                builder.ins().load(
                    bitset_ty,
                    // Note that readonly will never be valid here
                    MemFlags::trusted(),
                    addr,
                    bitset_offset as i32,
                )
            };
            first_expr = Some(bitset);

            // Set the given bit, these use `bitset | (1 << bit_idx)` to set the bit
            // and `bitset & !(1 << bit_idx)` to unset the bit.
            // For non-constant inputs this computes both results and then selects
            // the correct one based off of the value of `is_null`
            match set_null.is_null() {
                RValue::Expr(expr) => {
                    let is_null = self.exprs[expr];
                    let mask = 1i64 << bit_idx;
                    let set_bit = builder.ins().bor_imm(bitset, mask);
                    let unset_bit = builder.ins().band_imm(bitset, !mask);
                    builder.ins().select(is_null, set_bit, unset_bit)
                }

                &RValue::Imm(Constant::Bool(set_null)) => {
                    let mask = 1i64 << bit_idx;
                    if set_null {
                        builder.ins().bor_imm(bitset, mask)
                    } else {
                        builder.ins().band_imm(bitset, !mask)
                    }
                }

                RValue::Imm(_) => unreachable!(),
            }
        };

        // Store the bit with mask set
        let store = self.store_to_row(
            set_null.target(),
            with_null_set,
            MemFlags::trusted(),
            bitset_offset,
            builder,
        );

        if let Some(writer) = self.comment_writer.as_deref() {
            let first_expr = if let Some(first_expr) = first_expr {
                builder.func.dfg.value_def(first_expr).unwrap_inst()
            } else {
                store
            };

            writer.borrow_mut().add_comment(
                first_expr,
                format!(
                    "set_null col {} of {} ({}) with {:?}",
                    set_null.column(),
                    set_null.target(),
                    self.layout_id(set_null.target()),
                    set_null.is_null(),
                ),
            );
        }
    }

    // TODO: There's a couple optimizations we can do here:
    // - If our type is sub-word size, we can simply use a constant that's been set
    //   to all ones. This does require generalized handling for rows that aren't
    //   addressable, but that's a good thing we need in the future anyways
    // - If the null bits are sufficiently sparse and the row is sufficiently large
    //   (something something heuristics) then memset-ing the entire thing is just a
    //   waste and we should instead use more focused stores
    // - If our type doesn't contain any null flags, we don't need to do anything
    //   and can simply leave the row uninitialized. This does require that we
    //   actually follow our contract that `NullRow` only means "any well-defined
    //   `IsNull` call returns `true`" and don't depend on it being initialized to
    //   any degree further than what is required by that
    fn null_row(&mut self, expr_id: ExprId, null: &NullRow, builder: &mut FunctionBuilder<'_>) {
        // Create a stack slot
        let slot = self.stack_slot_for_layout(expr_id, null.layout(), builder);

        if let Some(writer) = self.comment_writer.as_deref() {
            writer.borrow_mut().add_comment(
                slot,
                format!(
                    "null row of layout {:?}",
                    self.layout_cache.row_layout(null.layout()),
                ),
            );
        }

        // We only need to set all nullish bits if the current layout has any nullable
        // rows
        if self
            .layout_cache
            .row_layout(null.layout())
            .has_nullable_columns()
        {
            // Get the address of the stack slot
            let addr = builder
                .ins()
                .stack_addr(self.module.isa().pointer_type(), slot, 0);

            let layout = self.layout_cache.layout_of(null.layout());
            for entry in layout.memory_order() {
                if let &MemoryEntry::BitSet {
                    offset,
                    ty,
                    ref columns,
                } = entry
                {
                    // If there's only one occupant of the bitset, set it to 1
                    let value = if columns.len() == 1 {
                        builder.ins().iconst(ty.native_type(), 1)

                    // Otherwise just set all bits high
                    } else {
                        ty.all_set(builder)
                    };

                    // Note that this store can't be readonly
                    builder
                        .ins()
                        .store(MemFlags::trusted(), value, addr, offset as i32);
                }
            }

            // // Fill the stack slot with whatever our nullish sigil is
            // self.emit_small_memset(addr, 0b1111_1111, null.layout(),
            // builder);
        }
    }

    fn binary_op(&mut self, expr_id: ExprId, binop: &BinaryOp, builder: &mut FunctionBuilder<'_>) {
        dbg!(expr_id);
        let (lhs, rhs) = (
            self.exprs[&dbg!(binop.lhs())],
            self.exprs[&dbg!(binop.rhs())],
        );
        let (lhs_ty, rhs_ty) = (self.expr_types[&binop.lhs()], self.expr_types[&binop.rhs()]);
        debug_assert_eq!(
            builder.func.dfg.value_type(lhs),
            builder.func.dfg.value_type(rhs),
        );
        debug_assert_eq!(lhs_ty, rhs_ty);

        let mut value_ty = lhs_ty;
        let value = match binop.kind() {
            BinaryOpKind::Add => {
                if lhs_ty.is_float() {
                    builder.ins().fadd(lhs, rhs)
                } else if lhs_ty.is_int() {
                    builder.ins().iadd(lhs, rhs)
                } else {
                    todo!("unknown binop type: {lhs_ty} ({binop:?})")
                }
            }
            BinaryOpKind::Sub => {
                if lhs_ty.is_float() {
                    builder.ins().fsub(lhs, rhs)
                } else if lhs_ty.is_int() {
                    builder.ins().isub(lhs, rhs)
                } else {
                    todo!("unknown binop type: {lhs_ty} ({binop:?})")
                }
            }
            BinaryOpKind::Mul => {
                if lhs_ty.is_float() {
                    builder.ins().fsub(lhs, rhs)
                } else if lhs_ty.is_int() {
                    builder.ins().imul(lhs, rhs)
                } else {
                    todo!("unknown binop type: {lhs_ty} ({binop:?})")
                }
            }
            BinaryOpKind::Div => {
                if lhs_ty.is_float() {
                    builder.ins().fdiv(lhs, rhs)
                } else if lhs_ty.is_signed_int() {
                    builder.ins().sdiv(lhs, rhs)
                } else if lhs_ty.is_unsigned_int() {
                    builder.ins().udiv(lhs, rhs)
                } else {
                    todo!("unknown binop type: {lhs_ty} ({binop:?})")
                }
            }

            BinaryOpKind::Eq => {
                value_ty = ColumnType::Bool;

                if lhs_ty.is_float() {
                    let (lhs, rhs) = if self.config.total_float_comparisons {
                        (
                            self.normalize_float(lhs, builder),
                            self.normalize_float(rhs, builder),
                        )
                    } else {
                        (lhs, rhs)
                    };
                    builder.ins().fcmp(FloatCC::Equal, lhs, rhs)
                } else {
                    builder.ins().icmp(IntCC::Equal, lhs, rhs)
                }
            }
            BinaryOpKind::Neq => {
                value_ty = ColumnType::Bool;

                if lhs_ty.is_float() {
                    let (lhs, rhs) = if self.config.total_float_comparisons {
                        (
                            self.normalize_float(lhs, builder),
                            self.normalize_float(rhs, builder),
                        )
                    } else {
                        (lhs, rhs)
                    };
                    builder.ins().fcmp(FloatCC::NotEqual, lhs, rhs)
                } else {
                    builder.ins().icmp(IntCC::NotEqual, lhs, rhs)
                }
            }
            BinaryOpKind::LessThan => {
                value_ty = ColumnType::Bool;

                if lhs_ty.is_float() {
                    let (lhs, rhs) = if self.config.total_float_comparisons {
                        (
                            self.normalize_float(lhs, builder),
                            self.normalize_float(rhs, builder),
                        )
                    } else {
                        (lhs, rhs)
                    };
                    builder.ins().fcmp(FloatCC::LessThan, lhs, rhs)
                } else if lhs_ty.is_signed_int() {
                    builder.ins().icmp(IntCC::SignedLessThan, lhs, rhs)
                } else {
                    builder.ins().icmp(IntCC::UnsignedLessThan, lhs, rhs)
                }
            }
            BinaryOpKind::GreaterThan => {
                value_ty = ColumnType::Bool;

                if lhs_ty.is_float() {
                    let (lhs, rhs) = if self.config.total_float_comparisons {
                        (
                            self.normalize_float(lhs, builder),
                            self.normalize_float(rhs, builder),
                        )
                    } else {
                        (lhs, rhs)
                    };
                    builder.ins().fcmp(FloatCC::GreaterThan, lhs, rhs)
                } else if lhs_ty.is_signed_int() {
                    builder.ins().icmp(IntCC::SignedGreaterThan, lhs, rhs)
                } else {
                    builder.ins().icmp(IntCC::UnsignedGreaterThan, lhs, rhs)
                }
            }
            BinaryOpKind::LessThanOrEqual => {
                value_ty = ColumnType::Bool;

                if lhs_ty.is_float() {
                    let (lhs, rhs) = if self.config.total_float_comparisons {
                        (
                            self.normalize_float(lhs, builder),
                            self.normalize_float(rhs, builder),
                        )
                    } else {
                        (lhs, rhs)
                    };
                    builder.ins().fcmp(FloatCC::LessThanOrEqual, lhs, rhs)
                } else if lhs_ty.is_signed_int() {
                    builder.ins().icmp(IntCC::SignedLessThanOrEqual, lhs, rhs)
                } else {
                    builder.ins().icmp(IntCC::UnsignedLessThanOrEqual, lhs, rhs)
                }
            }
            BinaryOpKind::GreaterThanOrEqual => {
                value_ty = ColumnType::Bool;

                if lhs_ty.is_float() {
                    let (lhs, rhs) = if self.config.total_float_comparisons {
                        (
                            self.normalize_float(lhs, builder),
                            self.normalize_float(rhs, builder),
                        )
                    } else {
                        (lhs, rhs)
                    };
                    builder.ins().fcmp(FloatCC::GreaterThanOrEqual, lhs, rhs)
                } else if lhs_ty.is_signed_int() {
                    builder
                        .ins()
                        .icmp(IntCC::SignedGreaterThanOrEqual, lhs, rhs)
                } else {
                    builder
                        .ins()
                        .icmp(IntCC::UnsignedGreaterThanOrEqual, lhs, rhs)
                }
            }

            BinaryOpKind::Min => {
                if lhs_ty.is_float() {
                    builder.ins().fmin(lhs, rhs)
                } else if lhs_ty.is_signed_int() {
                    builder.ins().smin(lhs, rhs)
                } else {
                    builder.ins().umin(lhs, rhs)
                }
            }
            BinaryOpKind::Max => {
                if lhs_ty.is_float() {
                    builder.ins().fmax(lhs, rhs)
                } else if lhs_ty.is_signed_int() {
                    builder.ins().smax(lhs, rhs)
                } else {
                    builder.ins().umax(lhs, rhs)
                }
            }

            BinaryOpKind::And => builder.ins().band(lhs, rhs),
            BinaryOpKind::Or => builder.ins().bor(lhs, rhs),
            BinaryOpKind::Xor => builder.ins().bxor(lhs, rhs),
        };

        self.add_expr(expr_id, value, value_ty, None);
    }

    fn unary_op(&mut self, expr_id: ExprId, unary: &UnaryOp, builder: &mut FunctionBuilder<'_>) {
        let (value, value_ty) = {
            let value_id = unary.value();
            let value = self.exprs[&value_id];
            let value_ty = self.expr_types[&value_id];
            debug_assert!(
                (unary.kind() == UnaryOpKind::StringLen || !value_ty.is_string())
                    && !value_ty.is_unit()
            );

            let value = match unary.kind() {
                UnaryOpKind::Abs => {
                    if value_ty.is_float() {
                        builder.ins().fabs(value)
                    } else if value_ty.is_signed_int() {
                        builder.ins().iabs(value)
                    } else {
                        // Abs on unsigned types is a noop
                        value
                    }
                }

                UnaryOpKind::Neg => {
                    if value_ty.is_float() {
                        builder.ins().fneg(value)
                    } else {
                        // TODO: Should we only use ineg for signed integers?
                        builder.ins().ineg(value)
                    }
                }

                UnaryOpKind::Not => {
                    let not_value = builder.ins().bnot(value);
                    if value_ty.is_bool() {
                        // Mask all bits except for the one containing the boolean value
                        builder.ins().band_imm(not_value, 0b0000_0001)
                    } else {
                        not_value
                    }
                }

                UnaryOpKind::Ceil => {
                    debug_assert!(value_ty.is_float());
                    builder.ins().ceil(value)
                }
                UnaryOpKind::Floor => {
                    debug_assert!(value_ty.is_float());
                    builder.ins().floor(value)
                }
                UnaryOpKind::Trunc => {
                    debug_assert!(value_ty.is_float());
                    builder.ins().trunc(value)
                }
                UnaryOpKind::Sqrt => {
                    if value_ty.is_float() {
                        builder.ins().sqrt(value)
                    } else {
                        todo!("integer sqrt?")
                    }
                }

                UnaryOpKind::CountOnes => {
                    debug_assert!(value_ty.is_int());
                    builder.ins().popcnt(value)
                }
                UnaryOpKind::CountZeroes => {
                    debug_assert!(value_ty.is_int());
                    // count_zeroes(x) = count_ones(!x)
                    let not_value = builder.ins().bnot(value);
                    builder.ins().popcnt(not_value)
                }

                UnaryOpKind::LeadingOnes => {
                    debug_assert!(value_ty.is_int());
                    builder.ins().cls(value)
                }
                UnaryOpKind::LeadingZeroes => {
                    debug_assert!(value_ty.is_int());
                    builder.ins().clz(value)
                }

                UnaryOpKind::TrailingOnes => {
                    debug_assert!(value_ty.is_int());
                    // trailing_ones(x) = trailing_zeroes(!x)
                    let not_value = builder.ins().bnot(value);
                    builder.ins().ctz(not_value)
                }
                UnaryOpKind::TrailingZeroes => {
                    debug_assert!(value_ty.is_int());
                    builder.ins().ctz(value)
                }

                UnaryOpKind::BitReverse => {
                    debug_assert!(value_ty.is_int());
                    builder.ins().bitrev(value)
                }
                UnaryOpKind::ByteReverse => {
                    debug_assert!(value_ty.is_int());
                    builder.ins().bswap(value)
                }

                UnaryOpKind::StringLen => {
                    debug_assert!(value_ty.is_string());

                    // Get the offset of the length field
                    let length_offset = ThinStr::length_offset();

                    // Load the length field
                    let length = builder.ins().load(
                        self.pointer_type(),
                        MemFlags::trusted(),
                        value,
                        length_offset as i32,
                    );

                    if let Some(writer) = self.comment_writer.as_deref() {
                        writer.borrow_mut().add_comment(
                            builder.func.dfg.value_def(length).unwrap_inst(),
                            format!("get the length of the string {value}"),
                        );
                    }

                    // Extend the length to a u64
                    if self.pointer_type() == types::I64 {
                        length
                    } else {
                        builder.ins().uextend(types::I64, length)
                    }
                }
            };

            (value, value_ty)
        };

        self.add_expr(expr_id, value, value_ty, None);
    }

    fn cast(&mut self, expr_id: ExprId, cast: &Cast, builder: &mut FunctionBuilder<'_>) {
        let src = self.value(cast.value());
        assert_eq!(self.expr_ty(cast.value()), cast.from());
        assert!(!cast.from().is_string() && !cast.from().is_unit());
        assert!(!cast.to().is_string() && !cast.to().is_unit());

        let (from_ty, to_ty) = (self.clif_ty(cast.from()), self.clif_ty(cast.to()));
        let value = match (cast.from(), cast.to()) {
            // Noop casts
            (a, b)
                if a == b
                    // Dates are represented as an i32
                    || ((a.is_i32() || a.is_u32()) && b.is_date())
                    // Timestamps are represented as an i64
                    || ((a.is_i64() || a.is_u64()) && b.is_timestamp())
                    // Signed <=> unsigned casts
                    || (a.is_i16() && b.is_u16())
                    || (a.is_u16() && b.is_i16())
                    || (a.is_i32() && b.is_u32())
                    || (a.is_u32() && b.is_i32())
                    || (a.is_i64() && b.is_u64())
                    || (a.is_u64() && b.is_i64()) =>
            {
                src
            }

            // Float to int
            (a, b) if a.is_float() => {
                if b.is_unsigned_int() {
                    if self.config.saturating_float_to_int_casts {
                        builder.ins().fcvt_to_uint(to_ty, src)
                    } else {
                        builder.ins().fcvt_to_uint_sat(to_ty, src)
                    }
                } else {
                    debug_assert!(b.is_signed_int());
                    if self.config.saturating_float_to_int_casts {
                        builder.ins().fcvt_to_sint(to_ty, src)
                    } else {
                        builder.ins().fcvt_to_sint_sat(to_ty, src)
                    }
                }
            }

            // Int to float
            (a, b) if b.is_float() => {
                if a.is_unsigned_int() {
                    builder.ins().fcvt_from_uint(to_ty, src)
                } else {
                    debug_assert!(a.is_signed_int());
                    builder.ins().fcvt_from_sint(to_ty, src)
                }
            }

            // Smaller int to larger int
            (a, _) if from_ty.bytes() < to_ty.bytes() => {
                if a.is_signed_int() || a.is_date() || a.is_timestamp() {
                    builder.ins().sextend(to_ty, src)
                } else {
                    debug_assert!(a.is_unsigned_int() || a.is_bool());
                    builder.ins().uextend(to_ty, src)
                }
            }

            // Larger int to smaller int
            (_, _) if from_ty.bytes() > to_ty.bytes() => builder.ins().ireduce(to_ty, src),

            (a, b) => unreachable!("cast from {a} to {b}"),
        };

        self.add_expr(expr_id, value, cast.to(), None);
    }

    fn select(&mut self, expr_id: ExprId, select: &Select, builder: &mut FunctionBuilder<'_>) {
        let (cond, if_true, if_false) = (
            self.value(select.cond()),
            self.value(select.if_true()),
            self.value(select.if_false()),
        );
        // TODO: Add type debug assertions

        let value = builder.ins().select(cond, if_true, if_false);
        self.add_expr(
            expr_id,
            value,
            self.expr_types.get(&select.if_true()).copied(),
            self.expr_layouts.get(&select.if_true()).copied(),
        );
    }

    /// Based off of rust's [`f32::total_cmp()`] and [`f64::total_cmp()`]
    /// implementations
    ///
    /// ```rust,ignore
    /// // f32::total_cmp()
    /// pub fn total_cmp(&self, other: &Self) -> Ordering {
    ///     let mut left = self.to_bits() as i32;
    ///     let mut right = other.to_bits() as i32;
    ///
    ///     // In case of negatives, flip all the bits except the sign
    ///     // to achieve a similar layout as two's complement integers
    ///     //
    ///     // Why does this work? IEEE 754 floats consist of three fields:
    ///     // Sign bit, exponent and mantissa. The set of exponent and mantissa
    ///     // fields as a whole have the property that their bitwise order is
    ///     // equal to the numeric magnitude where the magnitude is defined.
    ///     // The magnitude is not normally defined on NaN values, but
    ///     // IEEE 754 totalOrder defines the NaN values also to follow the
    ///     // bitwise order. This leads to order explained in the doc comment.
    ///     // However, the representation of magnitude is the same for negative
    ///     // and positive numbers – only the sign bit is different.
    ///     // To easily compare the floats as signed integers, we need to
    ///     // flip the exponent and mantissa bits in case of negative numbers.
    ///     // We effectively convert the numbers to "two's complement" form.
    ///     //
    ///     // To do the flipping, we construct a mask and XOR against it.
    ///     // We branchlessly calculate an "all-ones except for the sign bit"
    ///     // mask from negative-signed values: right shifting sign-extends
    ///     // the integer, so we "fill" the mask with sign bits, and then
    ///     // convert to unsigned to push one more zero bit.
    ///     // On positive values, the mask is all zeros, so it's a no-op.
    ///     left ^= (((left >> 31) as u32) >> 1) as i32;
    ///     right ^= (((right >> 31) as u32) >> 1) as i32;
    ///
    ///     left.cmp(&right)
    /// }
    /// ```
    ///
    /// [`f32::total_cmp()`]: https://doc.rust-lang.org/std/primitive.f32.html#method.total_cmp
    /// [`f64::total_cmp()`]: https://doc.rust-lang.org/std/primitive.f64.html#method.total_cmp
    fn normalize_float(&self, float: Value, builder: &mut FunctionBuilder<'_>) -> Value {
        let ty = builder.func.dfg.value_type(float);
        let (int_ty, first_shift) = if ty == types::F32 {
            (types::I32, 31)
        } else if ty == types::F64 {
            (types::I64, 63)
        } else {
            unreachable!("normalize_float() can only be called on f32 and f64: {ty}")
        };

        // float.to_bits()
        // TODO: Should we apply any flags to this?
        let int = builder.ins().bitcast(int_ty, MemFlags::new(), float);

        if let Some(writer) = self.comment_writer.as_deref() {
            writer.borrow_mut().add_comment(
                builder.func.dfg.value_def(int).unwrap_inst(),
                format!("normalize {ty} for totalOrder"),
            );
        }

        // left >> {31, 63}
        let shifted = builder.ins().sshr_imm(int, first_shift);
        // ((left >> {31, 63}) as {u32, u64}) >> 1
        let shifted = builder.ins().ushr_imm(shifted, 1);

        // left ^= shifted
        builder.ins().bxor(int, shifted)
    }
}
