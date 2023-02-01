mod clone;
mod cmp;
mod drop;
mod hash;
mod tests;

use crate::{
    codegen::{
        utils::FunctionBuilderExt, Codegen, CodegenCtx, NativeLayout, NativeType, TRAP_NULL_PTR,
    },
    ir::{ColumnType, LayoutId},
};
use cranelift::{
    codegen::ir::UserFuncName,
    prelude::{
        types, AbiParam, FunctionBuilder, InstBuilder, MemFlags, Type as ClifType,
        Value as ClifValue,
    },
};
use cranelift_jit::JITModule;
use cranelift_module::{FuncId, Module};
use dbsp::trace::layers::erased::ErasedVTable;
use std::{
    any::TypeId,
    cmp::Ordering,
    fmt::{self, Debug},
    hash::Hasher,
    mem::align_of,
    num::NonZeroUsize,
};

// TODO: The unwinding issues can be solved by creating unwind table entries for
// our generated functions, this'll also make our code more debug-able
// https://github.com/bytecodealliance/wasmtime/issues/5574
// TODO: Hash impl

macro_rules! vtable {
    ($($func:ident: $ty:ty),+ $(,)?) => {
        #[derive(Debug, Clone, Copy)]
        pub struct LayoutVTable {
            size_of: usize,
            align_of: NonZeroUsize,
            layout_id: LayoutId,
            $(pub $func: FuncId,)+
        }

        impl LayoutVTable {
            pub fn erased(&self, jit: &JITModule) -> ErasedVTable {
                // This is just a dummy function since we can't meaningfully create type ids at
                // runtime (we could technically ignore the existence of other types and hope
                // they never cross paths with the unholy abominations created here so that we
                // could make our own TypeIds that tell which layout a row originated from,
                // allowing us to check that two rows are of the same layout)
                fn type_id() -> TypeId {
                    struct DataflowJitRow;
                    TypeId::of::<DataflowJitRow>()
                }

                unsafe {
                    ErasedVTable {
                        size_of: self.size_of,
                        align_of: self.align_of,
                        $(
                            $func: std::mem::transmute::<*const u8, $ty>(
                                jit.get_finalized_function(self.$func),
                            ),
                        )+
                        type_id,
                    }
                }
            }

            pub fn marshalled(&self, jit: &JITModule) -> VTable {
                unsafe {
                    VTable {
                        size_of: self.size_of,
                        align_of: self.align_of,
                        layout_id: self.layout_id,
                        $(
                            $func: std::mem::transmute::<*const u8, $ty>(
                                jit.get_finalized_function(self.$func),
                            ),
                        )+
                    }
                }
            }
        }

        paste::paste! {
            impl Codegen {
                pub fn vtable_for(&mut self, layout_id: LayoutId) -> LayoutVTable {
                    if let Some(&vtable) = self.vtables.get(&layout_id) {
                        vtable
                    } else {
                        let vtable = self.make_vtable_for(layout_id);
                        self.vtables.insert(layout_id, vtable);
                        vtable
                    }
                }

                fn make_vtable_for(&mut self, layout_id: LayoutId) -> LayoutVTable {
                    let (size_of, align_of) = {
                        let layout = self.layout_cache.layout_of(layout_id);
                        (
                            layout.size() as usize,
                            NonZeroUsize::new(layout.align() as usize).unwrap(),
                        )
                    };

                    LayoutVTable {
                        size_of,
                        align_of,
                        layout_id,
                        $($func: {
                            // println!(concat!("vtable function for ", stringify!($func), ":"));
                            self.[<codegen_layout_ $func>](layout_id)
                        },)+
                    }
                }
            }
        }

        #[derive(Clone, Copy)]
        pub struct VTable {
            pub size_of: usize,
            pub align_of: NonZeroUsize,
            pub layout_id: LayoutId,
            $(pub $func: $ty,)+
        }

        impl VTable {
            pub fn type_name(&self) -> &str {
                unsafe {
                    let mut len = 0;
                    let ptr = (self.type_name)(&mut len);

                    let bytes = std::slice::from_raw_parts(ptr, len);
                    debug_assert!(std::str::from_utf8(bytes).is_ok());
                    std::str::from_utf8_unchecked(bytes)
                }
            }
        }

        impl Debug for VTable {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.debug_struct("VTable")
                    .field("size_of", &self.size_of)
                    .field("align_of", &self.align_of)
                    .field("layout_id", &self.layout_id)
                    $(.field(stringify!($func), &(self.$func as *const u8)))+
                    .finish()
            }
        }
    };
}

vtable! {
    eq: unsafe extern "C" fn(*const u8, *const u8) -> bool,
    lt: unsafe extern "C" fn(*const u8, *const u8) -> bool,
    cmp: unsafe extern "C" fn(*const u8, *const u8) -> Ordering,
    clone: unsafe extern "C" fn(*const u8, *mut u8),
    clone_into_slice: unsafe extern "C" fn(*const u8, *mut u8, usize),
    size_of_children: unsafe extern "C" fn(*const u8, &mut size_of::Context),
    debug: unsafe extern "C" fn(*const u8, *mut fmt::Formatter<'_>) -> bool,
    drop_in_place: unsafe extern "C" fn(*mut u8),
    drop_slice_in_place: unsafe extern "C" fn(*mut u8, usize),
    type_name: unsafe extern "C" fn(*mut usize) -> *const u8,
    hash: unsafe extern "C" fn(&mut &mut dyn Hasher, *const u8),
}

// TODO: Move these functions onto `CodegenCtx`
impl Codegen {
    fn new_vtable_fn<P>(&mut self, params: P, ret: Option<ClifType>) -> FuncId
    where
        P: IntoIterator<Item = ClifType>,
    {
        // Create the function's signature
        let mut signature = self.module.make_signature();
        signature
            .params
            .extend(params.into_iter().map(AbiParam::new));
        if let Some(ret) = ret {
            signature.returns.push(AbiParam::new(ret));
        }

        // Declare the function
        let func_id = self.module.declare_anonymous_function(&signature).unwrap();
        let func_name = UserFuncName::user(0, func_id.as_u32());

        // Set the current context to operate over that function
        self.module_ctx.func.signature = signature;
        self.module_ctx.func.name = func_name;

        func_id
    }

    #[tracing::instrument(skip(self))]
    fn codegen_layout_type_name(&mut self, layout_id: LayoutId) -> FuncId {
        tracing::info!("creating type_name vtable function for {layout_id}");

        // fn(*mut usize) -> *const u8
        let ptr_ty = self.module.isa().pointer_type();
        let func_id = self.new_vtable_fn([ptr_ty], Some(ptr_ty));

        self.set_comment_writer(
            &format!("{layout_id}_vtable_type_name"),
            "fn(*mut usize) -> *const u8",
        );

        {
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

            // Create the entry block
            let entry_block = builder.create_block();
            builder.append_block_params_for_function_params(entry_block);
            builder.switch_to_block(entry_block);

            // Write the string's length to the out param
            let length_out = builder.block_params(entry_block)[0];
            ctx.debug_assert_ptr_valid(
                length_out,
                NativeType::Usize.align(&ctx.frontend_config()),
                &mut builder,
            );

            let type_name = format!(
                "DataflowJitRow({:?})",
                ctx.layout_cache.row_layout(layout_id),
            )
            .into_bytes();
            let type_name_len = type_name.len();
            let name_id = ctx.create_data(type_name);
            let name_value = ctx.import_data(name_id, &mut builder);

            // Get the length and pointer to the type name
            let type_name_ptr = builder.ins().symbol_value(ptr_ty, name_value);
            let type_name_len = builder.ins().iconst(ptr_ty, type_name_len as i64);

            if self.config.debug_assertions {
                builder.ins().trapz(length_out, TRAP_NULL_PTR);
            }

            builder
                .ins()
                .store(MemFlags::trusted(), type_name_len, length_out, 0);

            // Return the string's pointer
            builder.ins().return_(&[type_name_ptr]);

            // Finish building the function
            builder.seal_all_blocks();
            builder.finalize();
        }

        self.finalize_function(func_id);

        func_id
    }

    #[tracing::instrument(skip(self))]
    fn codegen_layout_size_of_children(&mut self, layout_id: LayoutId) -> FuncId {
        tracing::info!("creating size_of_children vtable function for {layout_id}");

        // fn(*const u8, *mut Context)
        let func_id = self.new_vtable_fn([self.module.isa().pointer_type(); 2], None);

        self.set_comment_writer(
            &format!("{layout_id}_vtable_size_of_children"),
            &format!(
                "fn(*const {:?}, *mut size_of::Context)",
                self.layout_cache.row_layout(layout_id),
            ),
        );

        {
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

            // Create the entry block
            let entry_block = builder.create_block();
            builder.switch_to_block(entry_block);

            // Add the function params as block params
            builder.append_block_params_for_function_params(entry_block);

            let (layout, row_layout) = self.layout_cache.get_layouts(layout_id);

            let params = builder.block_params(entry_block);
            let (ptr, context) = (params[0], params[1]);
            ctx.debug_assert_ptr_valid(ptr, layout.align(), &mut builder);
            ctx.debug_assert_ptr_valid(
                context,
                align_of::<size_of::Context>() as u32,
                &mut builder,
            );

            if row_layout.columns().iter().any(ColumnType::is_string) {
                for (idx, (ty, nullable)) in row_layout
                    .iter()
                    .enumerate()
                    // Strings are the only thing that have children sizes right now
                    .filter(|(_, (ty, _))| ty.is_string())
                {
                    debug_assert_eq!(ty, ColumnType::String);

                    let next_size_of = if nullable {
                        // Zero = string isn't null, non-zero = string is null
                        let string_non_null =
                            column_non_null(idx, ptr, &layout, &mut builder, ctx.module, true);

                        // If the string is null, jump to the `next_size_of` block and don't
                        // get the size of the current string (since it's null). Otherwise
                        // (if the string isn't null) get its size and then continue recording
                        // any other fields
                        let size_of_string = builder.create_block();
                        let next_size_of = builder.create_block();
                        builder.ins().brnz(string_non_null, next_size_of, &[]);
                        builder.ins().jump(size_of_string, &[]);

                        builder.switch_to_block(size_of_string);

                        Some(next_size_of)
                    } else {
                        None
                    };

                    // Load the string
                    let offset = layout.offset_of(idx) as i32;
                    let native_ty = layout.type_of(idx).native_type(&ctx.frontend_config());
                    let flags = MemFlags::trusted().with_readonly();
                    let string = builder.ins().load(native_ty, flags, ptr, offset);

                    // Get the size of the string's children
                    let string_size_of_children = ctx
                        .imports
                        .string_size_of_children(ctx.module, builder.func);
                    builder
                        .ins()
                        .call(string_size_of_children, &[string, context]);

                    if let Some(next_drop) = next_size_of {
                        builder.ins().jump(next_drop, &[]);
                        builder.switch_to_block(next_drop);
                    }
                }
            }

            builder.ins().return_(&[]);

            // Finish building the function
            builder.seal_all_blocks();
            builder.finalize();
        }

        self.finalize_function(func_id);

        func_id
    }

    #[tracing::instrument(skip(self))]
    fn codegen_layout_debug(&mut self, layout_id: LayoutId) -> FuncId {
        tracing::info!("creating debug vtable function for {layout_id}");

        // fn(*const u8, *mut fmt::Formatter) -> bool
        let ptr_ty = self.module.isa().pointer_type();
        let func_id = self.new_vtable_fn([ptr_ty; 2], Some(types::I8));

        self.set_comment_writer(
            &format!("{layout_id}_vtable_debug"),
            &format!(
                "fn(*const {:?}, *mut fmt::Formatter) -> bool",
                self.layout_cache.row_layout(layout_id),
            ),
        );

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
            let str_debug = ctx.imports.str_debug(ctx.module, builder.func);

            // Create the entry block
            let entry_block = builder.create_block();
            builder.switch_to_block(entry_block);

            // Add the function params as block params
            builder.append_block_params_for_function_params(entry_block);

            let params = builder.block_params(entry_block);
            let (ptr, fmt) = (params[0], params[1]);

            // Ensure that the given pointers are valid
            if ctx.debug_assertions() {
                let ptr_align = ctx.layout_cache.layout_of(layout_id).align();
                ctx.assert_ptr_valid(ptr, ptr_align, &mut builder);
                ctx.assert_ptr_valid(
                    fmt,
                    align_of::<std::fmt::Formatter<'_>>() as u32,
                    &mut builder,
                );
            }

            let return_block = builder.create_block();
            builder.append_block_params_for_function_returns(return_block);

            // If the row is empty we can just write an empty row
            let row_layout = layout_cache.row_layout(layout_id);
            if row_layout.is_empty() {
                // Declare the data within the function
                // TODO: Deduplicate data entries within the module
                let empty_data = b"{}";
                let empty_id = ctx.create_data(empty_data.to_owned());
                let empty = ctx.import_data(empty_id, &mut builder);

                // Write an empty row to the output
                let empty_ptr = builder.ins().symbol_value(ptr_ty, empty);
                let empty_len = builder.ins().iconst(ptr_ty, empty_data.len() as i64);
                let result = builder.call_fn(str_debug, &[empty_ptr, empty_len, fmt]);

                builder.ins().jump(return_block, &[result]);
                builder.seal_block(entry_block);

            // Otherwise, write each row to the output
            } else {
                let start_data = b"{ ";
                let start_id = ctx.create_data(start_data.to_owned());
                let start = ctx.import_data(start_id, &mut builder);

                // Write the start of a row to the output
                let start_ptr = builder.ins().symbol_value(ptr_ty, start);
                let start_len = builder.ins().iconst(ptr_ty, start_data.len() as i64);
                let result = builder.call_fn(str_debug, &[start_ptr, start_len, fmt]);

                let debug_start = builder.create_block();
                let debug_failed = builder.false_byte();
                builder.ins().brz(result, return_block, &[debug_failed]);
                builder.ins().jump(debug_start, &[]);
                builder.seal_block(entry_block);

                builder.switch_to_block(debug_start);

                for (idx, (ty, nullable)) in row_layout.iter().enumerate() {
                    let after_debug = builder.create_block();

                    if nullable {
                        let layout = layout_cache.layout_of(layout_id);
                        // Zero = value isn't null, non-zero = value is null
                        let non_null =
                            column_non_null(idx, ptr, &layout, &mut builder, ctx.module, true);

                        // If the value is null, jump to the `write_null` block and debug a null
                        // value (since it's null). Otherwise (if the value isn't null) debug it
                        // and then continue debugging any other fields
                        let debug_value = builder.create_block();
                        let write_null = builder.create_block();
                        builder.ins().brnz(non_null, write_null, &[]);
                        builder.ins().jump(debug_value, &[]);
                        builder.seal_current();

                        builder.switch_to_block(write_null);

                        let null_data = b"null";
                        let null_id = ctx.create_data(null_data.to_owned());
                        let null = ctx.import_data(null_id, &mut builder);

                        // Write `null` to the output
                        let null_ptr = builder.ins().symbol_value(ptr_ty, null);
                        let null_len = builder.ins().iconst(ptr_ty, null_data.len() as i64);
                        let call = builder.call_fn(str_debug, &[null_ptr, null_len, fmt]);

                        // If writing `null` failed, return an error
                        let debug_failed = builder.false_byte();
                        builder.ins().brz(call, return_block, &[debug_failed]);
                        builder.ins().jump(after_debug, &[]);
                        builder.seal_block(write_null);

                        builder.switch_to_block(debug_value);
                    }

                    let result = if ty.is_unit() {
                        let unit_data = b"unit";
                        let unit_id = ctx.create_data(unit_data.to_owned());
                        let unit = ctx.import_data(unit_id, &mut builder);

                        // Write `unit` to the output
                        let unit_ptr = builder.ins().symbol_value(ptr_ty, unit);
                        let unit_len = builder.ins().iconst(ptr_ty, unit_data.len() as i64);
                        let result = builder.call_fn(str_debug, &[unit_ptr, unit_len, fmt]);

                        if let Some(writer) = ctx.comment_writer.as_deref() {
                            writer.borrow_mut().add_comment(
                                builder.func.dfg.value_def(unit_ptr).unwrap_inst(),
                                format!(
                                    "debug col {idx} ({}) of {:?}",
                                    ColumnType::Unit,
                                    ctx.layout_cache.row_layout(layout_id),
                                ),
                            );
                        }

                        result
                    } else {
                        // Load the value
                        let layout = ctx.layout_cache.layout_of(layout_id);
                        let offset = layout.offset_of(idx) as i32;
                        let native_ty = layout
                            .type_of(idx)
                            .native_type(&ctx.module.isa().frontend_config());
                        let flags = MemFlags::trusted().with_readonly();
                        let value = builder.ins().load(native_ty, flags, ptr, offset);

                        if let Some(writer) = ctx.comment_writer.as_deref() {
                            let layout = ctx.layout_cache.row_layout(layout_id);
                            writer.borrow_mut().add_comment(
                                builder.func.dfg.value_def(value).unwrap_inst(),
                                format!(
                                    "debug col {idx} ({}) of {:?}",
                                    layout.column_type(idx),
                                    layout,
                                ),
                            );
                        }

                        match ty {
                            ColumnType::Bool => {
                                let bool_debug = ctx.imports.bool_debug(ctx.module, builder.func);
                                builder.call_fn(bool_debug, &[value, fmt])
                            }

                            ColumnType::U16 | ColumnType::U32 => {
                                let uint_debug = ctx.imports.uint_debug(ctx.module, builder.func);
                                let extended = builder.ins().uextend(types::I64, value);
                                builder.call_fn(uint_debug, &[extended, fmt])
                            }
                            ColumnType::U64 => {
                                let uint_debug = ctx.imports.uint_debug(ctx.module, builder.func);
                                builder.call_fn(uint_debug, &[value, fmt])
                            }

                            ColumnType::I16 | ColumnType::I32 => {
                                let int_debug = ctx.imports.int_debug(ctx.module, builder.func);
                                let extended = builder.ins().sextend(types::I64, value);
                                builder.call_fn(int_debug, &[extended, fmt])
                            }
                            ColumnType::I64 => {
                                let int_debug = ctx.imports.int_debug(ctx.module, builder.func);
                                builder.call_fn(int_debug, &[value, fmt])
                            }

                            ColumnType::F32 => {
                                let f32_debug = ctx.imports.f32_debug(ctx.module, builder.func);
                                builder.call_fn(f32_debug, &[value, fmt])
                            }
                            ColumnType::F64 => {
                                let f64_debug = ctx.imports.f64_debug(ctx.module, builder.func);
                                builder.call_fn(f64_debug, &[value, fmt])
                            }

                            ColumnType::String => {
                                let string_debug =
                                    ctx.imports.string_debug(ctx.module, builder.func);
                                builder.call_fn(string_debug, &[value, fmt])
                            }

                            ColumnType::Unit => unreachable!(),
                        }
                    };

                    // If writing the value failed, return an error
                    let debug_failed = builder.false_byte();
                    let fail_branch = builder.ins().brz(result, return_block, &[debug_failed]);
                    builder.ins().jump(after_debug, &[]);
                    builder.seal_block(builder.current_block().unwrap());
                    builder.switch_to_block(after_debug);

                    if let Some(writer) = ctx.comment_writer.as_deref() {
                        writer
                            .borrow_mut()
                            .add_comment(fail_branch, "propagate errors if debugging failed");
                    }

                    // If this is the last column in the row, finish it off with ` }`
                    if idx == row_layout.len() - 1 {
                        let end_data = b" }";
                        let end_id = ctx.create_data(end_data.to_owned());
                        let end = ctx.import_data(end_id, &mut builder);

                        // Write the end of a row to the output
                        let end_ptr = builder.ins().symbol_value(ptr_ty, end);
                        let end_len = builder.ins().iconst(ptr_ty, end_data.len() as i64);
                        let result = builder.call_fn(str_debug, &[end_ptr, end_len, fmt]);

                        let debug_failed = builder.false_byte();
                        let debug_success = builder.true_byte();
                        builder.ins().brz(result, return_block, &[debug_failed]);
                        builder.ins().jump(return_block, &[debug_success]);

                    // Otherwise comma-separate each column
                    } else {
                        let next_debug = builder.create_block();

                        let comma_data = b", ";
                        let comma_id = ctx.create_data(comma_data.to_owned());
                        let comma = ctx.import_data(comma_id, &mut builder);

                        let comma_ptr = builder.ins().symbol_value(ptr_ty, comma);
                        let comma_len = builder.ins().iconst(ptr_ty, comma_data.len() as i64);
                        let result = builder.call_fn(str_debug, &[comma_ptr, comma_len, fmt]);

                        let debug_failed = builder.false_byte();
                        builder.ins().brz(result, return_block, &[debug_failed]);
                        builder.ins().jump(next_debug, &[]);
                        builder.switch_to_block(next_debug);
                    }

                    builder.seal_block(after_debug);
                }
            }

            builder.switch_to_block(return_block);
            let result = builder.block_params(return_block)[0];
            builder.ins().return_(&[result]);
            builder.seal_block(return_block);

            // Finish building the function
            builder.seal_all_blocks();
            builder.finalize();
        }

        self.finalize_function(func_id);

        func_id
    }
}

/// Checks if the given row is currently null, returns zero for non-null and
/// non-zero for null
// TODO: If we make sure that memory is zeroed (or at the very least that
// padding bytes are zeroed), we can simplify checks for null flags that are the
// only occupant of their given bitset. This'd allow us to go from
// `x = load; x1 = and x, 1` to just `x = load` for what should be a fairly
// common case. We could also do our best to distribute null flags across
// padding bytes when possible to try and make that happy path occur as much
// as possible
fn column_non_null(
    column: usize,
    row_ptr: ClifValue,
    layout: &NativeLayout,
    builder: &mut FunctionBuilder,
    module: &JITModule,
    readonly: bool,
) -> ClifValue {
    debug_assert!(layout.is_nullable(column));

    let (bitset_ty, bitset_offset, bit_idx) = layout.nullability_of(column);
    let bitset_ty = bitset_ty.native_type(&module.isa().frontend_config());

    // Create the flags for the load
    let mut flags = MemFlags::trusted();
    if readonly {
        flags.set_readonly();
    }

    // Load the bitset containing the given column's nullability
    let bitset = builder
        .ins()
        .load(bitset_ty, flags, row_ptr, bitset_offset as i32);

    // Zero is true (the value isn't null), non-zero is false (the value is
    // null)
    // if config.null_sigil.is_one() {
    //     // x & (1 << bit)
    //     builder.ins().band_imm(bitset, 1i64 << bit_idx)
    // } else {
    //     // !x & (1 << bit)
    //     let bitset = builder.ins().bnot(bitset);
    //     builder.ins().band_imm(bitset, 1i64 << bit_idx)
    // }
    builder.ins().band_imm(bitset, 1i64 << bit_idx)
}
