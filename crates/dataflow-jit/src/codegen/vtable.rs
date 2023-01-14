use crate::{
    codegen::Codegen,
    ir::{LayoutId, RowType},
};
use cranelift::{
    codegen::ir::UserFuncName,
    prelude::{types, AbiParam, FloatCC, FunctionBuilder, InstBuilder, IntCC, MemFlags},
};
use cranelift_module::{FuncId, Module};
use std::num::NonZeroU8;

impl Codegen {
    /// Generates a function comparing two of the given layout for equality
    // FIXME: I took the really lazy route here and pretend that uninit data
    //        either doesn't exist or is always zeroed, if padding bytes
    //        or otherwise uninitialized data doesn't conform to that this'll
    //        spuriously error
    // FIXME: This also ignores the existence of strings
    pub fn codegen_layout_eq(&mut self, layout_id: LayoutId) -> FuncId {
        // fn(*const u8, *const u8) -> bool
        let mut signature = self.module.make_signature();
        signature.returns.push(AbiParam::new(types::I8));
        signature
            .params
            .extend([AbiParam::new(self.module.isa().pointer_type()); 2]);

        let func_id = self.module.declare_anonymous_function(&signature).unwrap();
        let func_name = UserFuncName::user(0, func_id.as_u32());

        self.module_ctx.func.signature = signature;
        self.module_ctx.func.name = func_name;

        {
            let mut builder =
                FunctionBuilder::new(&mut self.module_ctx.func, &mut self.function_ctx);

            // Create the entry block
            let entry_block = builder.create_block();
            builder.switch_to_block(entry_block);

            // Add the function params as block params
            builder.append_block_params_for_function_params(entry_block);

            let layout = self.layout_cache.compute(layout_id);

            // Zero sized types are always equal
            let are_equal = if layout.is_zero_sized() {
                builder.ins().iconst(types::I8, true as i64)

            // Otherwise we emit a memcmp
            } else {
                let params = builder.block_params(entry_block);
                let (lhs, rhs) = (params[0], params[1]);

                let align = NonZeroU8::new(layout.align() as u8).unwrap();
                builder.emit_small_memory_compare(
                    self.module.isa().frontend_config(),
                    IntCC::Equal,
                    lhs,
                    rhs,
                    layout.size() as u64,
                    align,
                    align,
                    MemFlags::trusted().with_readonly(),
                )
            };

            builder.ins().return_(&[are_equal]);

            // Finish building the function
            builder.seal_all_blocks();
            builder.finalize();
        }

        self.finalize_function(func_id);

        func_id
    }

    /// Generates a function cloning the given layout
    // FIXME: This also ignores the existence of strings
    pub fn codegen_layout_clone(&mut self, layout_id: LayoutId) -> FuncId {
        // fn(*const u8, *mut u8)
        let mut signature = self.module.make_signature();
        signature
            .params
            .extend([AbiParam::new(self.module.isa().pointer_type()); 2]);

        let func_id = self.module.declare_anonymous_function(&signature).unwrap();
        let func_name = UserFuncName::user(0, func_id.as_u32());

        self.module_ctx.func.signature = signature;
        self.module_ctx.func.name = func_name;

        {
            let mut builder =
                FunctionBuilder::new(&mut self.module_ctx.func, &mut self.function_ctx);

            // Create the entry block
            let entry_block = builder.create_block();
            builder.switch_to_block(entry_block);

            // TODO: Add debug assertion that src != dest when
            // `self.config.debug_assertions` is set

            // Add the function params as block params
            builder.append_block_params_for_function_params(entry_block);

            let layout = self.layout_cache.compute(layout_id);
            let (layout_size, layout_align) = (layout.size(), layout.align());

            // Zero sized types have nothing to clone
            if layout_size != 0 {
                let params = builder.block_params(entry_block);
                let (src, dest) = (params[0], params[1]);

                let row_layout = self.layout_cache.layout_cache.get(layout_id);

                if row_layout.requires_nontrivial_clone() {
                    todo!()
                } else {
                    let align = layout_align.try_into().unwrap();

                    // TODO: We can make our own more efficient memcpy here, the one that ships with
                    // cranelift is eh
                    builder.emit_small_memory_copy(
                        self.module.isa().frontend_config(),
                        src,
                        dest,
                        layout_size as u64,
                        align,
                        align,
                        true,
                        MemFlags::trusted(),
                    );
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

    /// Generates a function cloning a slice of the given layout
    // FIXME: This also ignores the existence of strings
    pub fn codegen_layout_clone_into_slice(&mut self, layout_id: LayoutId) -> FuncId {
        // fn(*const u8, *mut u8, usize)
        let mut signature = self.module.make_signature();
        let ptr_type = self.module.isa().pointer_type();
        signature.params.extend([AbiParam::new(ptr_type); 3]);

        let func_id = self.module.declare_anonymous_function(&signature).unwrap();
        let func_name = UserFuncName::user(0, func_id.as_u32());

        self.module_ctx.func.signature = signature;
        self.module_ctx.func.name = func_name;

        {
            let mut builder =
                FunctionBuilder::new(&mut self.module_ctx.func, &mut self.function_ctx);

            // Create the entry block
            let entry_block = builder.create_block();
            builder.switch_to_block(entry_block);

            // TODO: Add debug assertion that src != dest when
            // `self.config.debug_assertions` is set

            // Add the function params as block params
            builder.append_block_params_for_function_params(entry_block);

            let layout_size = self.layout_cache.compute(layout_id).size();

            // Zero sized types have nothing to clone
            if layout_size != 0 {
                let params = builder.block_params(entry_block);
                let (src, dest, length) = (params[0], params[1], params[2]);

                let row_layout = self.layout_cache.layout_cache.get(layout_id);

                if row_layout.requires_nontrivial_clone() {
                    todo!()

                // For types consisting entirely of scalar values we can simply
                // emit a memcpy
                } else {
                    // The total size we need to copy is size_of(layout) * length
                    // TODO: Should we add a size assertion here? Just for `debug_assertions`?
                    let size = builder.ins().imul_imm(length, layout_size as i64);
                    builder.call_memcpy(self.module.isa().frontend_config(), src, dest, size);
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

    pub fn codegen_layout_lt(&mut self, layout_id: LayoutId) -> FuncId {
        // fn(*const u8, *const u8) -> bool
        let mut signature = self.module.make_signature();
        signature
            .params
            .extend([AbiParam::new(self.module.isa().pointer_type()); 2]);
        signature.returns.push(AbiParam::new(types::I8));

        let func_id = self.module.declare_anonymous_function(&signature).unwrap();
        let func_name = UserFuncName::user(0, func_id.as_u32());

        self.module_ctx.func.signature = signature;
        self.module_ctx.func.name = func_name;

        let string_lt = self.module.declare_func_in_func(
            self.intrinsics.dataflow_jit_string_lt,
            &mut self.module_ctx.func,
        );

        {
            let mut builder =
                FunctionBuilder::new(&mut self.module_ctx.func, &mut self.function_ctx);

            // Create the entry block
            let entry_block = builder.create_block();
            builder.switch_to_block(entry_block);

            // TODO: Add debug assertion that src != dest when
            // `self.config.debug_assertions` is set

            // Add the function params as block params
            builder.append_block_params_for_function_params(entry_block);

            let layout_size = self.layout_cache.compute(layout_id).size();

            let return_not_less_than = builder.create_block();

            // ZSTs always return `false`, they're always equal
            if layout_size != 0 {
                // Create a block to house our `true` return
                let return_less_than = builder.create_block();

                let params = builder.block_params(entry_block);
                let (lhs, rhs) = (params[0], params[1]);

                let row_layout = self.layout_cache.layout_cache.get(layout_id).clone();
                let layout = self.layout_cache.compute(layout_id);

                // If the type is nullable, the algorithm is as follows:
                // - If both values are non-null, compare their innards
                // - If both values are null, they're equal (and therefore not less)
                // - If the lhs is null and the rhs is non-null, the rhs is greater
                // - If the lhs is non-null and the rhs is null, the rhs is less
                //
                // This gives us the following truth table:
                // | lhs_null | rhs_null | output               |
                // | -------- | -------- | -------------------- |
                // | true     | true     | false                |
                // | true     | false    | false                |
                // | false    | true     | true                 |
                // | true     | true     | compare inner values |
                //
                for (idx, (row_type, nullable)) in row_layout.iter().enumerate() {
                    if row_type.is_unit() && !nullable {
                        continue;
                    }

                    if nullable {
                        let (bitset_ty, bitset_offset, bit_idx) = layout.row_nullability(idx);
                        let bitset_ty = bitset_ty.native_type(&self.module.isa().frontend_config());

                        let (lhs_bitset, rhs_bitset) = {
                            let flags = MemFlags::trusted().with_readonly();

                            let lhs =
                                builder
                                    .ins()
                                    .load(bitset_ty, flags, lhs, bitset_offset as i32);
                            let rhs =
                                builder
                                    .ins()
                                    .load(bitset_ty, flags, rhs, bitset_offset as i32);

                            (lhs, rhs)
                        };

                        // Zero is true (the value isn't null), non-zero is false (the value is
                        // null)
                        let (lhs_non_null, rhs_non_null) = if self.config.null_sigil.is_one() {
                            // x & (1 << bit)
                            let lhs = builder.ins().band_imm(lhs_bitset, 1i64 << bit_idx);
                            let rhs = builder.ins().band_imm(rhs_bitset, 1i64 << bit_idx);

                            (lhs, rhs)
                        } else {
                            // !x & (1 << bit)
                            let lhs = builder.ins().bnot(lhs_bitset);
                            let rhs = builder.ins().bnot(rhs_bitset);

                            let lhs = builder.ins().band_imm(lhs, 1i64 << bit_idx);
                            let rhs = builder.ins().band_imm(rhs, 1i64 << bit_idx);

                            (lhs, rhs)
                        };

                        // // FIXME: We could use the masked values directly (zero vs. non-zero) to
                        // get // rid of the intermediate comparisons
                        // let lhs_masked = builder.ins().band_imm(lhs_bitset, 1i64 << bit_idx);
                        // let rhs_masked = builder.ins().band_imm(rhs_bitset, 1i64 << bit_idx);
                        //
                        // let cmp_cond = if self.config.null_sigil.is_one() {
                        //     IntCC::Equal
                        // } else {
                        //     IntCC::NotEqual
                        // };
                        // let lhs_non_null = builder.ins().icmp_imm(cmp_cond, lhs_masked, 0);
                        // let rhs_non_null = builder.ins().icmp_imm(cmp_cond, rhs_masked, 0);

                        if row_type.is_unit() {
                            // `lhs_non_null > rhs_non_null` gives us our proper ordering, making
                            // `null(0) > null(0) = false`, `non_null(1) > non_null(1) = false`,
                            // `null(0) > non_null(1) = false` and `non_null(1) > null(0) = true`
                            let isnt_less = builder.ins().icmp(
                                IntCC::UnsignedGreaterThan,
                                lhs_non_null,
                                rhs_non_null,
                            );

                            // if isnt_less { continue }
                            let next = builder.create_block();
                            builder.ins().brz(isnt_less, next, &[]);

                            // else { return true }
                            builder.ins().jump(return_less_than, &[]);

                            builder.switch_to_block(next);
                            continue;
                        }

                        // if lhs_non_null && rhs_non_null { compare inner values }
                        let neither_null = builder.create_block();
                        let secondary_branch = builder.create_block();
                        let both_non_null = builder.ins().band(lhs_non_null, rhs_non_null);
                        builder.ins().brnz(both_non_null, neither_null, &[]);
                        builder.ins().jump(secondary_branch, &[]);

                        builder.switch_to_block(secondary_branch);

                        // if lhs_non_null && !rhs_non_null { return true }
                        let non_null_and_null = builder.ins().icmp(
                            IntCC::UnsignedGreaterThan,
                            lhs_non_null,
                            rhs_non_null,
                        );
                        builder.ins().brnz(non_null_and_null, return_less_than, &[]);

                        // if (!lhs_non_null && rhs_non_null) || (!lhs_non_null && !rhs_non_null) {
                        // return false }
                        builder.ins().jump(return_not_less_than, &[]);

                        // Switch to the neither null block so that we can compare both values
                        builder.switch_to_block(neither_null);
                    }

                    debug_assert!(!row_type.is_unit());

                    // Load each row's value
                    let (lhs, rhs) = {
                        let offset = layout.row_offset(idx) as i32;
                        let native_ty = layout
                            .row_type(idx)
                            .native_type(&self.module.isa().frontend_config());
                        let flags = MemFlags::trusted().with_readonly();

                        let lhs = builder.ins().load(native_ty, flags, lhs, offset);
                        let rhs = builder.ins().load(native_ty, flags, rhs, offset);

                        (lhs, rhs)
                    };

                    let is_less = match row_type {
                        RowType::Bool | RowType::U16 | RowType::U32 | RowType::U64 => {
                            builder.ins().icmp(IntCC::UnsignedLessThan, lhs, rhs)
                        }

                        RowType::I16 | RowType::I32 | RowType::I64 => {
                            builder.ins().icmp(IntCC::SignedLessThan, lhs, rhs)
                        }

                        RowType::F32 | RowType::F64 => {
                            builder.ins().fcmp(FloatCC::LessThan, lhs, rhs)
                        }

                        RowType::Unit => unreachable!(),

                        RowType::String => {
                            let lt = builder.ins().call(string_lt, &[lhs, rhs]);
                            builder.func.dfg.first_result(lt)
                        }
                    };

                    let next = builder.create_block();
                    builder.ins().brnz(is_less, return_less_than, &[]);
                    builder.ins().jump(next, &[]);
                    builder.switch_to_block(next);
                }

                builder.ins().jump(return_not_less_than, &[]);

                builder.switch_to_block(return_less_than);
                let true_val = builder.ins().iconst(types::I8, true as i64);
                builder.ins().return_(&[true_val]);
                builder.seal_block(return_less_than);
            }

            // If control flow reaches this point then either all fields are >= or the type
            // is just unit values
            builder.switch_to_block(return_not_less_than);
            let false_val = builder.ins().iconst(types::I8, false as i64);
            builder.ins().return_(&[false_val]);

            // Finish building the function
            builder.seal_all_blocks();
            builder.finalize();
        }

        self.finalize_function(func_id);

        func_id
    }
}

/// Macro to codegen the actual dropping of a layout, couldn't be turned into a
/// function since `FunctionBuilder` takes a mutable ref out on `self`
macro_rules! drop_layout {
    ($self:ident, $row_layout:ident, $layout:ident, $ptr:ident, $builder:ident, $string_drop_in_place:ident) => {{
        for (idx, (ty, nullable)) in $row_layout
            .iter()
            .enumerate()
            .filter(|(_, (ty, _))| ty.is_string())
        {
            debug_assert_eq!(ty, RowType::String);

            let next_drop = if nullable {
                let (bitset_ty, bitset_offset, bit_idx) = $layout.row_nullability(idx);
                let bitset_ty = bitset_ty.native_type(&$self.module.isa().frontend_config());

                let flags = MemFlags::trusted().with_readonly();
                let bitset = $builder
                    .ins()
                    .load(bitset_ty, flags, $ptr, bitset_offset as i32);

                let string_non_null = if $self.config.null_sigil.is_one() {
                    // x & (1 << bit)
                    $builder.ins().band_imm(bitset, 1i64 << bit_idx)
                } else {
                    // !x & (1 << bit)
                    let not_bitset = $builder.ins().bnot(bitset);
                    $builder.ins().band_imm(not_bitset, 1i64 << bit_idx)
                };

                // If the string is null, jump to the `next_drop` block and don't drop
                // the current string. Otherwise (if the string isn't null) drop it and
                // then continue dropping any other fields
                let drop_string = $builder.create_block();
                let next_drop = $builder.create_block();
                $builder.ins().brnz(string_non_null, next_drop, &[]);
                $builder.ins().jump(drop_string, &[]);

                $builder.switch_to_block(drop_string);

                Some(next_drop)
            } else {
                None
            };

            // Load the string
            let offset = $layout.row_offset(idx) as i32;
            let native_ty = $layout
                .row_type(idx)
                .native_type(&$self.module.isa().frontend_config());
            // Readonly isn't transitive and doesn't apply to the data pointed to
            // by the pointer we're loading
            let flags = MemFlags::trusted().with_readonly();
            let string = $builder.ins().load(native_ty, flags, $ptr, offset);
            // Drop the string
            $builder.ins().call($string_drop_in_place, &[string]);

            if let Some(next_drop) = next_drop {
                $builder.ins().jump(next_drop, &[]);
                $builder.switch_to_block(next_drop);
            }
        }
    }};
}

impl Codegen {
    pub fn codegen_layout_drop_in_place(&mut self, layout_id: LayoutId) -> FuncId {
        // fn(*mut u8)
        let mut signature = self.module.make_signature();
        signature
            .params
            .push(AbiParam::new(self.module.isa().pointer_type()));

        let func_id = self.module.declare_anonymous_function(&signature).unwrap();
        let func_name = UserFuncName::user(0, func_id.as_u32());

        self.module_ctx.func.signature = signature;
        self.module_ctx.func.name = func_name;

        let string_drop_in_place = self.module.declare_func_in_func(
            self.intrinsics.dataflow_jit_string_drop_in_place,
            &mut self.module_ctx.func,
        );

        {
            let mut builder =
                FunctionBuilder::new(&mut self.module_ctx.func, &mut self.function_ctx);

            // Create the entry block
            let entry_block = builder.create_block();
            builder.switch_to_block(entry_block);

            // TODO: Add debug assertion that src != dest when
            // `self.config.debug_assertions` is set

            // Add the function params as block params
            builder.append_block_params_for_function_params(entry_block);

            let row_layout = self.layout_cache.layout_cache.get(layout_id).clone();
            let layout = self.layout_cache.compute(layout_id);

            if row_layout.needs_drop() {
                let ptr = builder.block_params(entry_block)[0];
                drop_layout!(self, row_layout, layout, ptr, builder, string_drop_in_place);
            }

            builder.ins().return_(&[]);

            // Finish building the function
            builder.seal_all_blocks();
            builder.finalize();
        }

        self.finalize_function(func_id);

        func_id
    }

    pub fn codegen_layout_drop_slice_in_place(&mut self, layout_id: LayoutId) -> FuncId {
        // fn(*mut u8, usize)
        let mut signature = self.module.make_signature();
        let ptr_ty = self.module.isa().pointer_type();
        signature.params.extend([AbiParam::new(ptr_ty); 2]);

        let func_id = self.module.declare_anonymous_function(&signature).unwrap();
        let func_name = UserFuncName::user(0, func_id.as_u32());

        self.module_ctx.func.signature = signature;
        self.module_ctx.func.name = func_name;

        let string_drop_in_place = self.module.declare_func_in_func(
            self.intrinsics.dataflow_jit_string_drop_in_place,
            &mut self.module_ctx.func,
        );

        {
            let mut builder =
                FunctionBuilder::new(&mut self.module_ctx.func, &mut self.function_ctx);

            // Create the entry block
            let entry_block = builder.create_block();
            builder.switch_to_block(entry_block);

            // Add the function params as block params
            builder.append_block_params_for_function_params(entry_block);

            let row_layout = self.layout_cache.layout_cache.get(layout_id).clone();
            let layout = self.layout_cache.compute(layout_id);

            if row_layout.needs_drop() {
                let ptr = builder.block_params(entry_block)[0];
                let length = builder.block_params(entry_block)[1];

                let tail = builder.create_block();
                let body = builder.create_block();
                builder.append_block_param(body, ptr_ty);

                let length_bytes = builder.ins().imul_imm(length, layout.size() as i64);
                let end_ptr = builder.ins().iadd(ptr, length_bytes);

                let ptr_inbounds = builder.ins().icmp(IntCC::UnsignedLessThan, ptr, end_ptr);
                builder.ins().brz(ptr_inbounds, tail, &[]);
                builder.ins().jump(body, &[ptr]);

                builder.switch_to_block(body);

                let ptr = builder.block_params(body)[0];
                drop_layout!(self, row_layout, layout, ptr, builder, string_drop_in_place);

                let ptr = builder.ins().iadd_imm(ptr, layout.size() as i64);
                let ptr_inbounds = builder.ins().icmp(IntCC::UnsignedLessThan, ptr, end_ptr);
                builder.ins().brnz(ptr_inbounds, body, &[ptr]);
                builder.ins().jump(tail, &[]);

                builder.switch_to_block(tail);
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
