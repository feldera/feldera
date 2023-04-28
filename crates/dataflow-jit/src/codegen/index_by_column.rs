use crate::{
    codegen::{
        utils::{column_non_null, FunctionBuilderExt},
        Codegen, CodegenCtx, NativeLayout,
    },
    ir::nodes::IndexByColumn,
    row::Row,
};
use cranelift::prelude::{AbiParam, FunctionBuilder, InstBuilder, IntCC, MemFlags, Value};
use cranelift_codegen::ir::UserFuncName;
use cranelift_module::{FuncId, Module};
use std::{cmp::Ordering, mem::size_of};

impl Codegen {
    // Returns `(owned, borrowed)` versions
    // TODO: Columnar version(s)
    pub(crate) fn codegen_index_by_column(&mut self, index_by: &IndexByColumn) -> (FuncId, FuncId) {
        let impl_key = (
            index_by.input_layout(),
            index_by.key_column(),
            index_by.key_layout(),
            index_by.value_layout(),
        );

        // If a satisfactory function already exists, reuse it instead of generating another
        if let Some(&index_by_column) = self.index_by_columns.get(&impl_key) {
            return index_by_column;
        }

        // Generate owned and borrowed `IndexByColumn` functions
        let owned = self.index_by_column(index_by, true);
        let borrowed = self.index_by_column(index_by, false);

        self.index_by_columns.insert(impl_key, (owned, borrowed));
        (owned, borrowed)
    }

    // TODO: We could elide the `if source_len == 0` check at the beginning of the loop
    // since that can be done within the operator while it reserves capacity for the output
    fn index_by_column(&mut self, index_by: &IndexByColumn, source_owned: bool) -> FuncId {
        let ptr_ty = self.module.isa().pointer_type();

        // fn(source_ptr, key_ptr, key_vtable, value_ptr, value_vtable, length)
        let mut signature = self.module.make_signature();
        signature.params.extend([AbiParam::new(ptr_ty); 6]);

        let func_id = self.module.declare_anonymous_function(&signature).unwrap();
        let func_name = UserFuncName::user(0, func_id.as_u32());

        self.module_ctx.func.signature = signature;
        self.module_ctx.func.name = func_name;

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

            // Get the relevant layouts
            let (src_layout, src_row_layout) =
                self.layout_cache.get_layouts(index_by.input_layout());
            let key_layout = self.layout_cache.layout_of(index_by.key_layout());
            let value_layout = self.layout_cache.layout_of(index_by.value_layout());

            // Create the entry block
            let entry_block = builder.create_block();
            builder.append_block_params_for_function_params(entry_block);
            builder.switch_to_block(entry_block);

            let tail = builder.create_block();
            let body = builder.create_block();
            builder.append_block_param(body, ptr_ty);
            builder.append_block_param(body, ptr_ty);
            builder.append_block_param(body, ptr_ty);

            // Get the input params
            let [source_ptr, key_ptr, key_vtable, value_ptr, value_vtable, length]: [_; 6] =
                builder.block_params(entry_block).try_into().unwrap();

            // Calculate the source slice's end pointer
            let length_bytes = builder.ins().imul_imm(length, size_of::<Row>() as i64);
            let source_end = builder.ins().iadd(source_ptr, length_bytes);

            // Check that `length` isn't zero and if so jump to the end
            builder
                .ins()
                .brif(length, body, &[source_ptr, key_ptr, value_ptr], tail, &[]);

            builder.seal_block(entry_block);
            builder.switch_to_block(body);

            let [source_ptr, key_ptr, value_ptr]: [_; 3] =
                builder.block_params(body).try_into().unwrap();

            let data_ptr =
                builder
                    .ins()
                    .load(ptr_ty, MemFlags::trusted().with_readonly(), source_ptr, 0);

            let alloc_fn = ctx.imports.get("alloc", ctx.module, builder.func);
            let mut row_idx = 0;
            for (idx, (ty, nullable)) in src_row_layout.iter().enumerate() {
                if idx == index_by.key_column() {
                    let allocated_row = if key_layout.is_zero_sized() {
                        // Make a dangling pointer
                        builder.ins().iconst(ptr_ty, key_layout.align() as i64)
                    } else {
                        debug_assert!(!nullable && ty.is_unit());

                        let (key_size, key_align) = (
                            builder.ins().iconst(ptr_ty, key_layout.size() as i64),
                            builder.ins().iconst(ptr_ty, key_layout.align() as i64),
                        );

                        // Allocate a row value
                        builder.call_fn(alloc_fn, &[key_size, key_align])
                    };

                    if nullable {
                        let is_null =
                            column_non_null(idx, data_ptr, &src_layout, &mut builder, true);

                        set_column_null(
                            is_null,
                            0,
                            allocated_row,
                            MemFlags::trusted(),
                            &key_layout,
                            &mut builder,
                        );

                        if let Some(native) = ty.native_type() {
                            // Load the key from source
                            let src_key = builder.ins().load(
                                native.native_type(&ctx.frontend_config()),
                                MemFlags::trusted().with_readonly(),
                                data_ptr,
                                key_layout.offset_of(idx) as i32,
                            );

                            // Clone the value if needed
                            let cloned = if ty.is_string() && !source_owned {
                                let clone_val = builder.create_block();
                                let after = builder.create_block();
                                builder.append_block_param(after, ptr_ty);

                                let null = builder.ins().null(ptr_ty);
                                builder.ins().brif(is_null, after, &[null], clone_val, &[]);

                                builder.switch_to_block(clone_val);
                                let clone_str =
                                    ctx.imports.get("string_clone", ctx.module, builder.func);
                                let cloned = builder.call_fn(clone_str, &[src_key]);
                                builder.ins().jump(after, &[cloned]);

                                builder.switch_to_block(after);
                                builder.block_params(after)[0]
                            } else {
                                src_key
                            };

                            // Store the cloned key to the allocated row
                            builder.ins().store(
                                MemFlags::trusted(),
                                cloned,
                                allocated_row,
                                key_layout.offset_of(0) as i32,
                            );
                        }
                    } else if let Some(native) = ty.native_type() {
                        // Load the key from source
                        let src_key = builder.ins().load(
                            native.native_type(&ctx.frontend_config()),
                            MemFlags::trusted().with_readonly(),
                            data_ptr,
                            key_layout.offset_of(idx) as i32,
                        );

                        // Clone the key if needed
                        let cloned = if ty.is_string() && !source_owned {
                            let clone_str =
                                ctx.imports.get("string_clone", ctx.module, builder.func);
                            builder.call_fn(clone_str, &[src_key])
                        } else {
                            src_key
                        };

                        // Store the cloned key to the allocated row
                        builder.ins().store(
                            MemFlags::trusted(),
                            cloned,
                            allocated_row,
                            key_layout.offset_of(0) as i32,
                        );
                    }

                    // Store the row pointer to the key vec
                    builder
                        .ins()
                        .store(MemFlags::trusted(), allocated_row, key_ptr, 0);

                    // Store the vtable pointer to the key vec
                    builder.ins().store(
                        MemFlags::trusted(),
                        key_vtable,
                        key_ptr,
                        size_of::<usize>() as i32,
                    );

                    continue;
                }

                // Skip discarded values
                if index_by.discarded_values().contains(&idx) {
                    continue;
                }

                // Propagate values
                let allocated_row = if value_layout.is_zero_sized() {
                    // Make a dangling pointer
                    builder.ins().iconst(ptr_ty, value_layout.align() as i64)
                } else {
                    debug_assert!(!nullable && ty.is_unit());

                    let (value_size, value_align) = (
                        builder.ins().iconst(ptr_ty, value_layout.size() as i64),
                        builder.ins().iconst(ptr_ty, value_layout.align() as i64),
                    );

                    // Allocate a row value
                    builder.call_fn(alloc_fn, &[value_size, value_align])
                };

                if nullable {
                    let is_null = column_non_null(idx, data_ptr, &src_layout, &mut builder, true);

                    set_column_null(
                        is_null,
                        row_idx,
                        allocated_row,
                        MemFlags::trusted(),
                        &value_layout,
                        &mut builder,
                    );

                    if let Some(native) = ty.native_type() {
                        // Load the value from source
                        let src_value = builder.ins().load(
                            native.native_type(&ctx.frontend_config()),
                            MemFlags::trusted().with_readonly(),
                            data_ptr,
                            value_layout.offset_of(idx) as i32,
                        );

                        // Clone the value if needed
                        let cloned = if ty.is_string() && !source_owned {
                            let clone_val = builder.create_block();
                            let after = builder.create_block();
                            builder.append_block_param(after, ptr_ty);

                            let null = builder.ins().null(ptr_ty);
                            builder.ins().brif(is_null, after, &[null], clone_val, &[]);

                            builder.switch_to_block(clone_val);
                            let clone_str =
                                ctx.imports.get("string_clone", ctx.module, builder.func);
                            let cloned = builder.call_fn(clone_str, &[src_value]);
                            builder.ins().jump(after, &[cloned]);

                            builder.switch_to_block(after);
                            builder.block_params(after)[0]
                        } else {
                            src_value
                        };

                        // Store the cloned value to the allocated row
                        builder.ins().store(
                            MemFlags::trusted(),
                            cloned,
                            allocated_row,
                            value_layout.offset_of(row_idx) as i32,
                        );
                    }
                } else if let Some(native) = ty.native_type() {
                    // Load the value from source
                    let src_value = builder.ins().load(
                        native.native_type(&ctx.frontend_config()),
                        MemFlags::trusted().with_readonly(),
                        data_ptr,
                        value_layout.offset_of(idx) as i32,
                    );

                    // Clone the value if needed
                    let cloned = if ty.is_string() && !source_owned {
                        let clone_str = ctx.imports.get("string_clone", ctx.module, builder.func);
                        builder.call_fn(clone_str, &[src_value])
                    } else {
                        src_value
                    };

                    // Store the cloned value to the allocated row
                    builder.ins().store(
                        MemFlags::trusted(),
                        cloned,
                        allocated_row,
                        value_layout.offset_of(row_idx) as i32,
                    );
                }

                // Store the row pointer to the value vec
                builder
                    .ins()
                    .store(MemFlags::trusted(), allocated_row, value_ptr, 0);

                // Store the vtable pointer to the value vec
                builder.ins().store(
                    MemFlags::trusted(),
                    value_vtable,
                    value_ptr,
                    size_of::<usize>() as i32,
                );

                row_idx += 1;
            }

            // Deallocate the source row if the input is owned
            if source_owned && !src_layout.is_zero_sized() {
                let dealloc = ctx.imports.get("dealloc", ctx.module, builder.func);
                let (size, align) = (
                    builder.ins().iconst(ptr_ty, src_layout.size() as i64),
                    builder.ins().iconst(ptr_ty, src_layout.align() as i64),
                );
                builder.ins().call(dealloc, &[source_ptr, size, align]);
            }

            // Increment all pointers
            let row_size = builder.ins().iconst(ptr_ty, size_of::<Row>() as i64);
            let src_ptr_inc = builder.ins().iadd(source_ptr, row_size);
            let key_ptr_inc = builder.ins().iadd(key_ptr, row_size);
            let value_ptr_inc = builder.ins().iadd(value_ptr, row_size);

            let src_is_oob = builder
                .ins()
                .icmp(IntCC::UnsignedLessThan, src_ptr_inc, source_end);
            builder.ins().brif(
                src_is_oob,
                tail,
                &[],
                body,
                &[src_ptr_inc, key_ptr_inc, value_ptr_inc],
            );

            builder.switch_to_block(tail);
            builder.ins().return_(&[]);

            builder.seal_all_blocks();
            builder.finalize();
        }

        self.finalize_function(func_id);
        func_id
    }
}

fn set_column_null(
    is_null: Value,
    column: usize,
    dest: Value,
    dest_flags: MemFlags,
    layout: &NativeLayout,
    builder: &mut FunctionBuilder<'_>,
) {
    // If the value is null, set the cloned value to null
    let (bitset_ty, bitset_offset, bit_idx) = layout.nullability_of(column);
    let bitset_ty = bitset_ty.native_type();

    let bitset = if layout.bitset_occupants(column) == 1 {
        let null_ty = builder.func.dfg.value_type(is_null);
        match bitset_ty.bytes().cmp(&null_ty.bytes()) {
            Ordering::Less => builder.ins().ireduce(bitset_ty, is_null),
            Ordering::Equal => is_null,
            Ordering::Greater => builder.ins().uextend(bitset_ty, is_null),
        }
    } else {
        // Load the bitset's current value
        let current_bitset = builder
            .ins()
            .load(bitset_ty, dest_flags, dest, bitset_offset as i32);

        let mask = 1 << bit_idx;
        let bitset_with_null = builder.ins().bor_imm(current_bitset, mask);
        let bitset_with_non_null = builder.ins().band_imm(current_bitset, !mask);

        builder
            .ins()
            .select(is_null, bitset_with_null, bitset_with_non_null)
    };

    // Store the newly modified bitset back into the row
    builder
        .ins()
        .store(dest_flags, bitset, dest, bitset_offset as i32);
}
