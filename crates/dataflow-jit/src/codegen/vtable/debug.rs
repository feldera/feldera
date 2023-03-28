use crate::{
    codegen::{utils::FunctionBuilderExt, vtable::column_non_null, Codegen, CodegenCtx},
    ir::{ColumnType, LayoutId},
};
use cranelift::prelude::{types, FunctionBuilder, InstBuilder, MemFlags};
use cranelift_module::{FuncId, Module};
use std::{fmt, mem::align_of};

impl Codegen {
    #[tracing::instrument(skip(self))]
    pub(super) fn codegen_layout_debug(&mut self, layout_id: LayoutId) -> FuncId {
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
                ctx.assert_ptr_valid(fmt, align_of::<fmt::Formatter<'_>>() as u32, &mut builder);
            }

            let return_block = builder.create_block();
            builder.append_block_params_for_function_returns(return_block);

            // If the row is empty we can just write an empty row
            let row_layout = layout_cache.row_layout(layout_id);
            if row_layout.is_empty() {
                // Write an empty row to the output
                let (empty_ptr, empty_len) = ctx.import_string("{}", &mut builder);
                let result = builder.call_fn(str_debug, &[empty_ptr, empty_len, fmt]);

                builder.ins().jump(return_block, &[result]);
                builder.seal_block(entry_block);

            // Otherwise, write each row to the output
            } else {
                // Write the start of a row to the output
                let (start_ptr, start_len) = ctx.import_string("{ ", &mut builder);
                let result = builder.call_fn(str_debug, &[start_ptr, start_len, fmt]);

                let debug_start = builder.create_block();
                let debug_failed = builder.false_byte();
                builder
                    .ins()
                    .brif(result, debug_start, &[], return_block, &[debug_failed]);
                builder.seal_block(entry_block);

                builder.switch_to_block(debug_start);

                for (idx, (ty, nullable)) in row_layout.iter().enumerate() {
                    let after_debug = builder.create_block();

                    if nullable {
                        let layout = layout_cache.layout_of(layout_id);
                        // Zero = value isn't null, non-zero = value is null
                        let non_null = column_non_null(idx, ptr, &layout, &mut builder, true);

                        // If the value is null, jump to the `write_null` block and debug a null
                        // value (since it's null). Otherwise (if the value isn't null) debug it
                        // and then continue debugging any other fields
                        let debug_value = builder.create_block();
                        let write_null = builder.create_block();
                        builder
                            .ins()
                            .brif(non_null, write_null, &[], debug_value, &[]);
                        builder.seal_current();

                        builder.switch_to_block(write_null);

                        // Write `null` to the output
                        let (null_ptr, null_len) = ctx.import_string("null", &mut builder);
                        let call = builder.call_fn(str_debug, &[null_ptr, null_len, fmt]);

                        // If writing `null` failed, return an error
                        let debug_failed = builder.false_byte();
                        builder
                            .ins()
                            .brif(call, after_debug, &[], return_block, &[debug_failed]);
                        builder.seal_block(write_null);

                        builder.switch_to_block(debug_value);
                    }

                    let result = if ty.is_unit() {
                        // Write `unit` to the output
                        let (unit_ptr, unit_len) = ctx.import_string("unit", &mut builder);
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
                        // TODO: We could take advantage of uload16/uload32/sload16/sload32 here
                        // instead of uext/sext later on
                        let mut value = builder.ins().load(native_ty, flags, ptr, offset);

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

                        let debug_fn = match ty {
                            // TODO: We can manually inline this
                            ColumnType::Bool => ctx.imports.bool_debug(ctx.module, builder.func),

                            ColumnType::U8 | ColumnType::U16 | ColumnType::U32 => {
                                value = builder.ins().uextend(types::I64, value);
                                ctx.imports.uint_debug(ctx.module, builder.func)
                            }
                            ColumnType::U64 => ctx.imports.uint_debug(ctx.module, builder.func),

                            ColumnType::Usize if ptr_ty == types::I64 => {
                                ctx.imports.uint_debug(ctx.module, builder.func)
                            }
                            ColumnType::Usize => {
                                value = builder.ins().uextend(types::I64, value);
                                ctx.imports.uint_debug(ctx.module, builder.func)
                            }

                            ColumnType::I8 | ColumnType::I16 | ColumnType::I32 => {
                                value = builder.ins().sextend(types::I64, value);
                                ctx.imports.int_debug(ctx.module, builder.func)
                            }
                            ColumnType::I64 => ctx.imports.int_debug(ctx.module, builder.func),

                            ColumnType::Isize if ptr_ty == types::I64 => {
                                ctx.imports.int_debug(ctx.module, builder.func)
                            }
                            ColumnType::Isize => {
                                value = builder.ins().sextend(types::I64, value);
                                ctx.imports.int_debug(ctx.module, builder.func)
                            }

                            ColumnType::F32 => ctx.imports.f32_debug(ctx.module, builder.func),
                            ColumnType::F64 => ctx.imports.f64_debug(ctx.module, builder.func),

                            ColumnType::Date => ctx.imports.date_debug(ctx.module, builder.func),
                            ColumnType::Timestamp => {
                                ctx.imports.timestamp_debug(ctx.module, builder.func)
                            }

                            ColumnType::String => {
                                ctx.imports.string_debug(ctx.module, builder.func)
                            }

                            ColumnType::Ptr | ColumnType::Unit => unreachable!(),
                        };

                        builder.call_fn(debug_fn, &[value, fmt])
                    };

                    // If writing the value failed, return an error
                    let debug_failed = builder.false_byte();
                    let fail_branch =
                        builder
                            .ins()
                            .brif(result, after_debug, &[], return_block, &[debug_failed]);
                    builder.seal_block(builder.current_block().unwrap());
                    builder.switch_to_block(after_debug);

                    if let Some(writer) = ctx.comment_writer.as_deref() {
                        writer
                            .borrow_mut()
                            .add_comment(fail_branch, "propagate errors if debugging failed");
                    }

                    // If this is the last column in the row, finish it off with ` }`
                    if idx == row_layout.len() - 1 {
                        // Write the end of a row to the output
                        let (end_ptr, end_len) = ctx.import_string(" }", &mut builder);
                        let result = builder.call_fn(str_debug, &[end_ptr, end_len, fmt]);

                        let debug_failed = builder.false_byte();
                        let debug_success = builder.true_byte();
                        builder.ins().brif(
                            result,
                            return_block,
                            &[debug_success],
                            return_block,
                            &[debug_failed],
                        );

                    // Otherwise comma-separate each column
                    } else {
                        let next_debug = builder.create_block();

                        let (comma_ptr, comma_len) = ctx.import_string(", ", &mut builder);
                        let result = builder.call_fn(str_debug, &[comma_ptr, comma_len, fmt]);

                        let debug_failed = builder.false_byte();
                        builder
                            .ins()
                            .brif(result, next_debug, &[], return_block, &[debug_failed]);
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
