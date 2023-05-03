use crate::{
    codegen::{
        utils::{column_non_null, FunctionBuilderExt},
        Codegen, CodegenCtx, NativeLayout,
    },
    ir::{nodes::IndexByColumn, LayoutId},
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

        // If a satisfactory function already exists, reuse it instead of generating
        // another
        if let Some(&index_by_column) = self.index_by_columns.get(&impl_key) {
            return index_by_column;
        }

        // Generate owned and borrowed `IndexByColumn` functions
        let owned = self.index_by_column(index_by, true);
        let borrowed = self.index_by_column(index_by, false);

        self.index_by_columns.insert(impl_key, (owned, borrowed));
        (owned, borrowed)
    }

    // TODO: We could elide the `if source_len == 0` check at the beginning of the
    // loop since that can be done within the operator while it reserves
    // capacity for the output
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
            let key_layout = self.layout_cache.layout_of(index_by.key_layout()).clone();
            let value_layout = self.layout_cache.layout_of(index_by.value_layout()).clone();
            let (src_layout, src_row_layout) =
                self.layout_cache.get_layouts(index_by.input_layout());

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

            let key_row = if key_layout.is_zero_sized() {
                // Make a dangling pointer
                builder.ins().iconst(ptr_ty, key_layout.align() as i64)
            } else {
                let (key_size, key_align) = (
                    builder.ins().iconst(ptr_ty, key_layout.size() as i64),
                    builder.ins().iconst(ptr_ty, key_layout.align() as i64),
                );

                // Allocate a row value
                builder.call_fn(alloc_fn, &[key_size, key_align])
            };

            let value_row = if value_layout.is_zero_sized() {
                // Make a dangling pointer
                builder.ins().iconst(ptr_ty, value_layout.align() as i64)
            } else {
                let (value_size, value_align) = (
                    builder.ins().iconst(ptr_ty, value_layout.size() as i64),
                    builder.ins().iconst(ptr_ty, value_layout.align() as i64),
                );

                // Allocate a row value
                builder.call_fn(alloc_fn, &[value_size, value_align])
            };

            let mut value_idx = 0;
            for idx in 0..src_row_layout.len() {
                if idx == index_by.key_column() {
                    // Load the key from `data_ptr` (cloning if needed) and then store it to `key_row`
                    ctx.load_and_store_column(
                        data_ptr,
                        source_owned,
                        index_by.input_layout(),
                        idx,
                        key_row,
                        index_by.key_layout(),
                        0,
                        &mut builder,
                    );

                    // Store the row pointer to the key vec
                    builder
                        .ins()
                        .store(MemFlags::trusted(), key_row, key_ptr, 0);

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
                    if source_owned && src_row_layout.column_type(idx).is_string() {
                        let drop_string =
                            ctx.imports
                                .get("string_drop_in_place", ctx.module, builder.func);

                        let string = builder.ins().load(
                            src_layout.type_of(idx).native_type(&ctx.frontend_config()),
                            MemFlags::trusted().with_readonly(),
                            data_ptr,
                            src_layout.offset_of(idx) as i32,
                        );

                        if src_layout.is_nullable(idx) {
                            let drop_val = builder.create_block();
                            let after = builder.create_block();

                            builder.ins().brif(string, drop_val, &[], after, &[]);
                            builder.switch_to_block(drop_val);

                            builder.ins().call(drop_string, &[string]);

                            builder.ins().jump(after, &[]);
                            builder.switch_to_block(after);
                        } else {
                            builder.ins().call(drop_string, &[string]);
                        }
                    }

                    continue;
                }

                // Load the value from `data_ptr` (cloning if needed) and then store it to `value_row`
                ctx.load_and_store_column(
                    data_ptr,
                    source_owned,
                    index_by.input_layout(),
                    idx,
                    value_row,
                    index_by.value_layout(),
                    value_idx,
                    &mut builder,
                );
                value_idx += 1;
            }

            // Store the value pointer to the value vec
            builder
                .ins()
                .store(MemFlags::trusted(), value_row, value_ptr, 0);

            // Store the vtable pointer to the value vec
            builder.ins().store(
                MemFlags::trusted(),
                value_vtable,
                value_ptr,
                size_of::<usize>() as i32,
            );

            // Deallocate the source row if the input is owned
            if source_owned && !src_layout.is_zero_sized() {
                let dealloc = ctx.imports.get("dealloc", ctx.module, builder.func);
                let (size, align) = (
                    builder.ins().iconst(ptr_ty, src_layout.size() as i64),
                    builder.ins().iconst(ptr_ty, src_layout.align() as i64),
                );
                builder.ins().call(dealloc, &[data_ptr, size, align]);
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
                body,
                &[src_ptr_inc, key_ptr_inc, value_ptr_inc],
                tail,
                &[],
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

impl CodegenCtx<'_> {
    #[allow(clippy::too_many_arguments)]
    fn load_and_store_column(
        &mut self,
        source_ptr: Value,
        source_owned: bool,
        source_layout: LayoutId,
        source_column: usize,
        dest_ptr: Value,
        dest_layout: LayoutId,
        dest_column: usize,
        builder: &mut FunctionBuilder<'_>,
    ) {
        let is_string = self
            .layout_cache
            .row_layout(source_layout)
            .column_type(source_column)
            .is_string();

        if is_string {
            self.load_and_store_string(
                source_ptr,
                source_owned,
                source_layout,
                source_column,
                dest_ptr,
                dest_layout,
                dest_column,
                builder,
            );
        } else {
            self.load_and_store_scalar(
                source_ptr,
                source_layout,
                source_column,
                dest_ptr,
                dest_layout,
                dest_column,
                builder,
            );
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn load_and_store_string(
        &mut self,
        source_ptr: Value,
        source_owned: bool,
        source_layout: LayoutId,
        source_column: usize,
        dest_ptr: Value,
        dest_layout: LayoutId,
        dest_column: usize,
        builder: &mut FunctionBuilder<'_>,
    ) {
        let (source_value, nullable) = {
            let (source_layout, source_row_layout) = self.layout_cache.get_layouts(source_layout);
            let nullable = source_row_layout.column_nullable(source_column);

            let source_ty = source_row_layout.column_type(source_column);
            debug_assert!(source_ty.is_string());

            let native_ty = source_ty
                .native_type()
                .unwrap()
                .native_type(&self.frontend_config());

            let source_offset = source_layout.offset_of(source_column) as i32;

            // Load the source value
            let source_value = builder.ins().load(
                native_ty,
                MemFlags::trusted().with_readonly(),
                source_ptr,
                source_offset,
            );

            (source_value, nullable)
        };

        // If the source is owned, we can take ownership of its string directly
        // Since the string contains its own null niche, this also copies over
        // its nullability (if it has one)
        let output_value = if source_owned {
            source_value

        // If the string is nullable, we have to conditionally clone it
        } else if nullable {
            let clone_string = builder.create_block();
            let after = builder.create_block();
            builder.append_block_param(after, self.pointer_type());

            // If the string is non-null jump to `clone_string`, otherwise
            // jump to `after` and give it our null string
            builder
                .ins()
                .brif(source_value, clone_string, &[], after, &[source_value]);

            // Clone the string within the `clone_string` block
            builder.switch_to_block(clone_string);
            let cloned = self.clone_string(source_value, builder);
            builder.ins().jump(after, &[cloned]);

            // Get the conditionally cloned string or the null value
            builder.switch_to_block(after);
            builder.block_params(after)[0]

        // If the string isn't nullable we can unconditionally clone it
        } else {
            self.clone_string(source_value, builder)
        };

        let dest_offset = self
            .layout_cache
            .layout_of(dest_layout)
            .offset_of(dest_column) as i32;

        builder
            .ins()
            .store(MemFlags::trusted(), output_value, dest_ptr, dest_offset);
    }

    fn clone_string(&mut self, string: Value, builder: &mut FunctionBuilder<'_>) -> Value {
        let clone_str = self.imports.get("string_clone", self.module, builder.func);
        builder.call_fn(clone_str, &[string])
    }

    #[allow(clippy::too_many_arguments)]
    fn load_and_store_scalar(
        &mut self,
        source_ptr: Value,
        source_layout: LayoutId,
        source_column: usize,
        dest_ptr: Value,
        dest_layout: LayoutId,
        dest_column: usize,
        builder: &mut FunctionBuilder<'_>,
    ) {
        let (source_value, is_null) = {
            let (source_layout, source_row_layout) = self.layout_cache.get_layouts(source_layout);

            let source_ty = source_row_layout.column_type(source_column);
            debug_assert!(!source_ty.is_string() && !source_ty.needs_drop());

            if source_ty.is_unit() {
                return;
            }

            let native_ty = source_ty
                .native_type()
                .unwrap()
                .native_type(&self.frontend_config());

            let source_offset = source_layout.offset_of(source_column) as i32;

            // Load the source value
            let source_value = builder.ins().load(
                native_ty,
                MemFlags::trusted().with_readonly(),
                source_ptr,
                source_offset,
            );

            let is_null = source_row_layout
                .column_nullable(source_column)
                .then(|| column_non_null(source_column, source_ptr, &source_layout, builder, true));

            (source_value, is_null)
        };

        let dest_layout = self.layout_cache.layout_of(dest_layout);
        let dest_offset = dest_layout.offset_of(dest_column) as i32;

        // If the column is nullable, set its null flag in dest
        if let Some(is_null) = is_null {
            set_column_null(
                is_null,
                dest_column,
                dest_ptr,
                MemFlags::trusted(),
                &dest_layout,
                builder,
            );
        }

        // Store the scalar value to dest
        builder
            .ins()
            .store(MemFlags::trusted(), source_value, dest_ptr, dest_offset);
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
        let null_ty = builder.value_type(is_null);
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
