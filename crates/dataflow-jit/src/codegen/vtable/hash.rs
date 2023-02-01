use crate::{
    codegen::{utils::FunctionBuilderExt, vtable::column_non_null, Codegen, CodegenCtx},
    ir::{ColumnType, LayoutId},
};
use cranelift::prelude::{types, FunctionBuilder, InstBuilder, IntCC, MemFlags};
use cranelift_module::{FuncId, Module};

impl Codegen {
    #[tracing::instrument(skip(self))]
    pub(super) fn codegen_layout_hash(&mut self, layout_id: LayoutId) -> FuncId {
        tracing::info!("creating hash vtable function for {layout_id}");

        // fn(&mut &mut dyn Hasher, *const u8)
        let func_id = self.new_vtable_fn([self.module.isa().pointer_type(); 2], None);
        let mut imports = self.intrinsics.import(self.comment_writer.clone());

        self.set_comment_writer(
            &format!("{layout_id}_vtable_hash"),
            &format!(
                "fn(&mut &mut dyn Hasher, *const {:?})",
                self.layout_cache.row_layout(layout_id),
            ),
        );

        {
            let ctx = CodegenCtx::new(
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
            let entry_block = builder.create_entry_block();
            let params = builder.block_params(entry_block);
            let (hasher, ptr) = (params[0], params[1]);

            let (layout, row_layout) = ctx.layout_cache.get_layouts(layout_id);

            // Zero sized types have nothing to hash
            if !layout.is_zero_sized() {
                for (idx, (ty, nullable)) in row_layout.iter().enumerate() {
                    if ty.is_unit() && !nullable {
                        continue;
                    }

                    let next_hash = if nullable {
                        // Zero = value isn't null, non-zero = value is null
                        let non_null =
                            column_non_null(idx, ptr, &layout, &mut builder, ctx.module, true);
                        // One = value isn't null, zero = value is null
                        let mut non_null = builder.ins().icmp_imm(IntCC::Equal, non_null, 0);
                        // Shrink the null-ness to a single byte
                        if builder.func.dfg.value_type(non_null) != types::I8 {
                            non_null = builder.ins().ireduce(types::I8, non_null);
                        }

                        // Hash the null-ness byte
                        let hash_u8 = imports.u8_hash(ctx.module, builder.func);
                        builder.ins().call(hash_u8, &[hasher, non_null]);

                        // For nullable unit types we don't need to do anything else
                        if ty.is_unit() {
                            continue;
                        }

                        // For all other types we only hash the inner value if it's non-null
                        let hash_innards = builder.create_block();
                        let next_hash = builder.create_block();
                        builder.ins().brz(non_null, next_hash, &[]);
                        builder.ins().jump(hash_innards, &[]);

                        builder.seal_block(builder.current_block().unwrap());
                        builder.switch_to_block(hash_innards);
                        Some(next_hash)
                    } else {
                        None
                    };

                    debug_assert!(!ty.is_unit());

                    let offset = layout.offset_of(idx) as i32;
                    let native_ty = layout.type_of(idx).native_type(&ctx.frontend_config());

                    // Load the source value
                    let flags = MemFlags::trusted().with_readonly();
                    let value = if !native_ty.is_float() {
                        builder.ins().load(native_ty, flags, ptr, offset)

                    // If total float comparisons are enabled, we normalize the float before hashing it
                    } else if self.config.total_float_comparisons {
                        let float = builder.ins().load(native_ty, flags, ptr, offset);
                        ctx.normalize_float(float, &mut builder)

                    // Otherwise we load the floating point value as its raw bits and then hash those raw bits
                    } else {
                        builder.ins().load(native_ty.as_int(), flags, ptr, offset)
                    };

                    let hash_function = match ty {
                        ColumnType::Bool => imports.u8_hash(ctx.module, builder.func),
                        ColumnType::U16 => imports.u16_hash(ctx.module, builder.func),
                        ColumnType::I16 => imports.i16_hash(ctx.module, builder.func),
                        ColumnType::U32 => imports.u32_hash(ctx.module, builder.func),
                        ColumnType::I32 => imports.i32_hash(ctx.module, builder.func),
                        ColumnType::U64 => imports.u64_hash(ctx.module, builder.func),
                        ColumnType::I64 => imports.i64_hash(ctx.module, builder.func),
                        ColumnType::F32 => imports.u32_hash(ctx.module, builder.func),
                        ColumnType::F64 => imports.u64_hash(ctx.module, builder.func),
                        ColumnType::String => imports.string_hash(ctx.module, builder.func),
                        ColumnType::Unit => unreachable!(),
                    };
                    builder.ins().call(hash_function, &[hasher, value]);

                    if let Some(next_clone) = next_hash {
                        builder.ins().jump(next_clone, &[]);
                        builder.seal_block(builder.current_block().unwrap());
                        builder.switch_to_block(next_clone);
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
}
