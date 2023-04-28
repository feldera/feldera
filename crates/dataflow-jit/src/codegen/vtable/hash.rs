use crate::{
    codegen::{
        utils::{column_non_null, FunctionBuilderExt},
        Codegen, CodegenCtx,
    },
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
                        let non_null = column_non_null(idx, ptr, &layout, &mut builder, true);
                        // One = value isn't null, zero = value is null
                        let mut non_null = builder.ins().icmp_imm(IntCC::Equal, non_null, 0);
                        // Shrink the null-ness to a single byte
                        if builder.func.dfg.value_type(non_null) != types::I8 {
                            non_null = builder.ins().ireduce(types::I8, non_null);
                        }

                        // Hash the null-ness byte
                        let hash_u8 = imports.get("u8_hash", ctx.module, builder.func);
                        builder.ins().call(hash_u8, &[hasher, non_null]);

                        // For nullable unit types we don't need to do anything else
                        if ty.is_unit() {
                            continue;
                        }

                        // For all other types we only hash the inner value if it's non-null
                        let hash_innards = builder.create_block();
                        let next_hash = builder.create_block();
                        builder
                            .ins()
                            .brif(non_null, hash_innards, &[], next_hash, &[]);

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

                    // If total float comparisons are enabled, we normalize the
                    // float before hashing it
                    } else if self.config.total_float_comparisons {
                        let float = builder.ins().load(native_ty, flags, ptr, offset);
                        ctx.normalize_float(float, &mut builder)

                    // Otherwise we load the floating point value as its raw
                    // bits and then hash those raw bits
                    } else {
                        builder.ins().load(native_ty.as_int(), flags, ptr, offset)
                    };

                    let hash_function = match ty {
                        ColumnType::Bool | ColumnType::U8 => "u8_hash",
                        ColumnType::I8 => "i8_hash",
                        ColumnType::U16 => "u16_hash",
                        ColumnType::I16 => "i16_hash",
                        ColumnType::U32 => "u32_hash",
                        ColumnType::I32 | ColumnType::Date => "i32_hash",
                        ColumnType::U64 => "u64_hash",
                        ColumnType::I64 | ColumnType::Timestamp => "i64_hash",
                        ColumnType::Usize => {
                            let ptr_ty = ctx.pointer_type();
                            if ptr_ty == types::I64 {
                                "u64_hash"
                            } else if ptr_ty == types::I32 {
                                "u32_hash"
                            } else if ptr_ty == types::I16 {
                                "u16_hash"
                            } else {
                                unreachable!("unsupported pointer width: {ptr_ty}")
                            }
                        }
                        ColumnType::Isize => {
                            let ptr_ty = ctx.pointer_type();
                            if ptr_ty == types::I64 {
                                "i64_hash"
                            } else if ptr_ty == types::I32 {
                                "i32_hash"
                            } else if ptr_ty == types::I16 {
                                "i16_hash"
                            } else {
                                unreachable!("unsupported pointer width: {ptr_ty}")
                            }
                        }
                        ColumnType::F32 => "u32_hash",
                        ColumnType::F64 => "u64_hash",
                        ColumnType::String => "string_hash",
                        ColumnType::Ptr | ColumnType::Unit => unreachable!(),
                    };
                    let hash_function = imports.get(hash_function, ctx.module, builder.func);
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
