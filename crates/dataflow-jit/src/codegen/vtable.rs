use crate::{
    codegen::{intrinsics::ImportIntrinsics, Codegen, CodegenConfig, Layout},
    ir::{LayoutId, RowLayout, RowType},
};
use cranelift::{
    codegen::ir::{FuncRef, UserFuncName},
    prelude::{
        types, AbiParam, FloatCC, FunctionBuilder, InstBuilder, IntCC, MemFlags, Value as ClifValue,
    },
};
use cranelift_jit::JITModule;
use cranelift_module::{FuncId, Module};
use std::{cmp::Ordering, num::NonZeroU8};

// TODO: The unwinding issues can be solved by creating unwind table entries for
// our generated functions, this'll also make our code more debug-able
// https://github.com/bytecodealliance/wasmtime/issues/5574

macro_rules! vtable {
    ($($func:ident: $ty:ty),+ $(,)?) => {
        pub struct LayoutVTable {
            $(pub $func: FuncId,)+
        }
    };
}

vtable! {
    eq: unsafe extern "C" fn(*const u8, *const u8) -> bool,
    lt: unsafe extern "C" fn(*const u8, *const u8) -> bool,
    cmp: unsafe extern "C" fn(*const u8, *const u8) -> Ordering,
    clone: unsafe extern "C" fn(*const u8, *mut u8),
    clone_into_slice: unsafe extern "C" fn(*const u8, *mut u8, usize),
    size_of_children: unsafe extern "C" fn(*const u8, &mut Context),
    debug: unsafe fn(*const u8, *mut fmt::Formatter<'_>) -> fmt::Result,
    drop_in_place: unsafe extern "C" fn(*mut u8),
    drop_slice_in_place: unsafe extern "C" fn(*mut u8, usize),
}

// TODO: Memoize these functions so we don't create multiple instances of them
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

        let mut imports = self.intrinsics.import();

        {
            let mut builder =
                FunctionBuilder::new(&mut self.module_ctx.func, &mut self.function_ctx);

            // Create the entry block
            let entry_block = builder.create_block();
            builder.switch_to_block(entry_block);

            // Add the function params as block params
            builder.append_block_params_for_function_params(entry_block);

            let return_not_less_than = builder.create_block();

            let (layout, row_layout) = self.layout_cache.get_layouts(layout_id);

            // ZSTs always return `false`, they're always equal
            if !layout.is_zero_sized() {
                // Create a block to house our `true` return
                let return_less_than = builder.create_block();

                let params = builder.block_params(entry_block);
                let (lhs, rhs) = (params[0], params[1]);

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
                        let lhs_non_null = column_non_null(
                            idx,
                            lhs,
                            layout,
                            &mut builder,
                            &self.config,
                            &self.module,
                            true,
                        );
                        let rhs_non_null = column_non_null(
                            idx,
                            rhs,
                            layout,
                            &mut builder,
                            &self.config,
                            &self.module,
                            true,
                        );

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
                            let string_lt =
                                imports.dataflow_jit_string_lt(&mut self.module, builder.func);
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

    pub fn codegen_layout_cmp(&mut self, layout_id: LayoutId) -> FuncId {
        // fn(*const u8, *const u8) -> Ordering
        // Ordering is represented as an i8 where -1 = Less, 0 = Equal and 1 = Greater
        let mut signature = self.module.make_signature();
        signature
            .params
            .extend([AbiParam::new(self.module.isa().pointer_type()); 2]);
        signature.returns.push(AbiParam::new(types::I8));

        let func_id = self.module.declare_anonymous_function(&signature).unwrap();
        let func_name = UserFuncName::user(0, func_id.as_u32());

        self.module_ctx.func.signature = signature;
        self.module_ctx.func.name = func_name;

        let mut imports = self.intrinsics.import();

        {
            let mut builder =
                FunctionBuilder::new(&mut self.module_ctx.func, &mut self.function_ctx);

            // Create the entry block
            let entry_block = builder.create_block();
            builder.switch_to_block(entry_block);

            // Add the function params as block params
            builder.append_block_params_for_function_params(entry_block);

            let less_than = builder.ins().iconst(types::I8, Ordering::Less as i64);
            let equal_to = builder.ins().iconst(types::I8, Ordering::Equal as i64);
            let greater_than = builder.ins().iconst(types::I8, Ordering::Greater as i64);

            let (layout, row_layout) = self.layout_cache.get_layouts(layout_id);

            // ZSTs are always equal
            if layout.is_zero_sized() {
                builder.ins().return_(&[equal_to]);

            // Otherwise we have to do actual work
            // The basic algo follows this pattern:
            // ```
            // for column in layout {
            //     match lhs[column].cmp(rhs[column]) {
            //         // If both are equal, compare the next column
            //         Equal => {}
            //         // If one is less or greater than the other, return that
            //         // ordering immediately
            //         order @ (Less | Greater) => return order,
            //     }
            // }
            //
            // // If all fields were equal, the values are equal
            // return Equal;
            // ```
            // Additionally, null values are equal, non-null values are greater
            // than null ones and null values are less than non-null ones
            } else {
                // Get the input pointers
                let params = builder.block_params(entry_block);
                let (lhs, rhs) = (params[0], params[1]);

                // The final block in the function will return the ordering
                let return_block = builder.create_block();
                builder.append_block_param(return_block, types::I8);

                for (idx, (row_type, nullable)) in row_layout.iter().enumerate() {
                    if row_type.is_unit() && !nullable {
                        continue;
                    }

                    let mut next_compare = builder.create_block();

                    if nullable {
                        // Zero = non-null, non-zero = null
                        let lhs_non_null = column_non_null(
                            idx,
                            lhs,
                            layout,
                            &mut builder,
                            &self.config,
                            &self.module,
                            true,
                        );
                        let rhs_non_null = column_non_null(
                            idx,
                            rhs,
                            layout,
                            &mut builder,
                            &self.config,
                            &self.module,
                            true,
                        );

                        let check_null = builder.create_block();

                        // Compare the nullability of the fields before comparing their innards
                        // What we do here boils down to this:
                        // ```
                        // %eq = icmp eq lhs_non_null, rhs_non_null
                        // %less = icmp ult lhs_non_null, rhs_non_null
                        // // If the values aren't equal
                        // %less_or_greater = select i8 less, -1, 1
                        // ```
                        let eq = builder.ins().icmp(IntCC::Equal, lhs_non_null, rhs_non_null);
                        let less =
                            builder
                                .ins()
                                .icmp(IntCC::UnsignedLessThan, lhs_non_null, rhs_non_null);
                        let ordering = builder.ins().select(less, less_than, greater_than);
                        builder.ins().brz(eq, return_block, &[ordering]);
                        builder.ins().jump(check_null, &[]);
                        builder.seal_block(builder.current_block().unwrap());

                        let after_compare = builder.create_block();

                        // In this block we check if either are null and decide whether or not to
                        // compare their inner value
                        builder.switch_to_block(check_null);
                        let either_null = builder.ins().bor(lhs_non_null, rhs_non_null);
                        builder.ins().brnz(either_null, after_compare, &[]);
                        builder.ins().jump(next_compare, &[]);
                        builder.seal_block(check_null);

                        builder.switch_to_block(next_compare);
                        next_compare = after_compare;
                    }

                    let native_ty = layout
                        .row_type(idx)
                        .native_type(&self.module.isa().frontend_config());

                    // Load the column's values
                    let (lhs, rhs) = {
                        let offset = layout.row_offset(idx) as i32;
                        let flags = MemFlags::trusted().with_readonly();

                        let lhs = builder.ins().load(native_ty, flags, lhs, offset);
                        let rhs = builder.ins().load(native_ty, flags, rhs, offset);

                        (lhs, rhs)
                    };

                    match row_type {
                        // Unsigned integers
                        RowType::Bool | RowType::U16 | RowType::U32 | RowType::U64 => {
                            let eq = builder.ins().icmp(IntCC::Equal, lhs, rhs);
                            let less = builder.ins().icmp(IntCC::UnsignedLessThan, lhs, rhs);
                            let ordering = builder.ins().select(less, less_than, greater_than);

                            builder.ins().brz(eq, return_block, &[ordering]);
                        }

                        // Signed integers
                        RowType::I16 | RowType::I32 | RowType::I64 => {
                            let eq = builder.ins().icmp(IntCC::Equal, lhs, rhs);
                            let less = builder.ins().icmp(IntCC::SignedLessThan, lhs, rhs);
                            let ordering = builder.ins().select(less, less_than, greater_than);

                            builder.ins().brz(eq, return_block, &[ordering]);
                        }

                        // Floats
                        RowType::F32 | RowType::F64 => {
                            let eq = builder.ins().fcmp(FloatCC::Equal, lhs, rhs);
                            let less = builder.ins().fcmp(FloatCC::LessThan, lhs, rhs);
                            let ordering = builder.ins().select(less, less_than, greater_than);

                            builder.ins().brz(eq, return_block, &[ordering]);
                        }

                        RowType::Unit => {}

                        RowType::String => {
                            let string_cmp =
                                imports.dataflow_jit_string_cmp(&mut self.module, builder.func);

                            // -1 for less, 0 for equal, 1 for greater
                            let cmp = builder.ins().call(string_cmp, &[lhs, rhs]);
                            let cmp = builder.func.dfg.first_result(cmp);

                            // Zero is equal so if the value is non-zero we can return the ordering
                            // directly
                            builder.ins().brnz(cmp, return_block, &[cmp]);
                        }
                    }

                    builder.ins().jump(next_compare, &[]);
                    builder.seal_block(builder.current_block().unwrap());
                    builder.switch_to_block(next_compare);
                }

                builder.ins().jump(return_block, &[equal_to]);
                builder.seal_block(builder.current_block().unwrap());

                // Make the final block return the ordering it's given
                builder.switch_to_block(return_block);
                let final_ordering = builder.block_params(return_block)[0];
                builder.ins().return_(&[final_ordering]);
                builder.seal_block(return_block);
            }

            // Finish building the function
            builder.seal_all_blocks();
            builder.finalize();
        }

        self.finalize_function(func_id);

        func_id
    }

    pub fn codegen_layout_type_name(&mut self, layout_id: LayoutId) -> FuncId {
        // fn(*mut usize) -> *const u8
        let ptr_ty = self.module.isa().pointer_type();
        let mut signature = self.module.make_signature();
        signature.params.push(AbiParam::new(ptr_ty));
        signature.returns.push(AbiParam::new(ptr_ty));

        let func_id = self.module.declare_anonymous_function(&signature).unwrap();
        let func_name = UserFuncName::user(0, func_id.as_u32());

        self.module_ctx.func.signature = signature;
        self.module_ctx.func.name = func_name;

        {
            let mut builder =
                FunctionBuilder::new(&mut self.module_ctx.func, &mut self.function_ctx);

            // Create the entry block
            let entry_block = builder.create_block();
            builder.append_block_params_for_function_params(entry_block);
            builder.switch_to_block(entry_block);

            // Build the layout's type name and leak it so that it's static
            // FIXME: Don't leak this, store them along with the code that backs the
            // functions so we can deallocate them at the same time
            // FIXME: Should do this as a static included in the jit code
            let layout = self.layout_cache.layout_cache.get(layout_id);
            let type_name = Box::leak(String::into_boxed_str(format!(
                "DataflowJitRow({layout:?})",
            )));

            // Get the length and pointer to the type name
            let type_name_len = builder.ins().iconst(ptr_ty, type_name.len() as i64);
            let type_name_ptr = builder.ins().iconst(ptr_ty, type_name.as_ptr() as i64);

            // Write the string's length to the out param
            let length_out = builder.block_params(entry_block)[0];
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

    pub fn codegen_layout_size_of_children(&mut self, layout_id: LayoutId) -> FuncId {
        // fn(*const u8, *mut Context)
        let mut signature = self.module.make_signature();
        let ptr_ty = self.module.isa().pointer_type();
        signature.params.extend([AbiParam::new(ptr_ty); 2]);

        let func_id = self.module.declare_anonymous_function(&signature).unwrap();
        let func_name = UserFuncName::user(0, func_id.as_u32());

        self.module_ctx.func.signature = signature;
        self.module_ctx.func.name = func_name;

        let mut imports = self.intrinsics.import();

        {
            let mut builder =
                FunctionBuilder::new(&mut self.module_ctx.func, &mut self.function_ctx);

            // Create the entry block
            let entry_block = builder.create_block();
            builder.switch_to_block(entry_block);

            // Add the function params as block params
            builder.append_block_params_for_function_params(entry_block);

            let (layout, row_layout) = self.layout_cache.get_layouts(layout_id);

            if row_layout.rows().iter().any(RowType::is_string) {
                let params = builder.block_params(entry_block);
                let (ptr, context) = (params[0], params[1]);

                for (idx, (ty, nullable)) in row_layout
                    .iter()
                    .enumerate()
                    // Strings are the only thing that have children sizes right now
                    .filter(|(_, (ty, _))| ty.is_string())
                {
                    debug_assert_eq!(ty, RowType::String);

                    let next_size_of = if nullable {
                        // Zero = string isn't null, non-zero = string is null
                        let string_non_null = column_non_null(
                            idx,
                            ptr,
                            layout,
                            &mut builder,
                            &self.config,
                            &self.module,
                            true,
                        );

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
                    let offset = layout.row_offset(idx) as i32;
                    let native_ty = layout
                        .row_type(idx)
                        .native_type(&self.module.isa().frontend_config());
                    let flags = MemFlags::trusted().with_readonly();
                    let string = builder.ins().load(native_ty, flags, ptr, offset);

                    // Get the size of the string's children
                    let string_size_of_children = imports
                        .dataflow_jit_string_size_of_children(&mut self.module, builder.func);
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

    pub fn codegen_layout_debug(&mut self, layout_id: LayoutId) -> FuncId {
        // fn(*const u8, *mut fmt::Formatter) -> bool
        let mut signature = self.module.make_signature();
        let ptr_ty = self.module.isa().pointer_type();
        signature.params.extend([AbiParam::new(ptr_ty); 2]);
        signature.returns.push(AbiParam::new(types::I8));

        let func_id = self.module.declare_anonymous_function(&signature).unwrap();
        let func_name = UserFuncName::user(0, func_id.as_u32());

        self.module_ctx.func.signature = signature;
        self.module_ctx.func.name = func_name;

        let mut imports = self.intrinsics.import();

        {
            let mut builder =
                FunctionBuilder::new(&mut self.module_ctx.func, &mut self.function_ctx);
            let str_debug = imports.dataflow_jit_str_debug(&mut self.module, builder.func);

            // Create the entry block
            let entry_block = builder.create_block();
            builder.switch_to_block(entry_block);

            // Add the function params as block params
            builder.append_block_params_for_function_params(entry_block);

            let return_block = builder.create_block();
            builder.append_block_params_for_function_returns(return_block);

            let (layout, row_layout) = self.layout_cache.get_layouts(layout_id);

            let params = builder.block_params(entry_block);
            let (ptr, fmt) = (params[0], params[1]);

            // If the row is empty we can just write an empty row
            if row_layout.is_empty() {
                // TODO: Should be a static within the jit code
                static EMPTY: &str = "{}";

                // Write an empty row to the output
                let empty_ptr = builder.ins().iconst(ptr_ty, EMPTY.as_ptr() as i64);
                let empty_len = builder.ins().iconst(ptr_ty, EMPTY.len() as i64);
                let call = builder.ins().call(str_debug, &[empty_ptr, empty_len, fmt]);
                let result = builder.func.dfg.first_result(call);

                builder.ins().jump(return_block, &[result]);
                builder.seal_block(entry_block);

            // Otherwise, write each row to the output
            } else {
                // TODO: Should be a static within the jit code
                static START: &str = "{ ";

                // Write the start of a row to the output
                let start_ptr = builder.ins().iconst(ptr_ty, START.as_ptr() as i64);
                let start_len = builder.ins().iconst(ptr_ty, START.len() as i64);
                let call = builder.ins().call(str_debug, &[start_ptr, start_len, fmt]);
                let result = builder.func.dfg.first_result(call);

                let debug_start = builder.create_block();
                let debug_failed = builder.ins().iconst(types::I8, false as i64);
                builder.ins().brz(result, return_block, &[debug_failed]);
                builder.ins().jump(debug_start, &[]);
                builder.seal_block(entry_block);

                builder.switch_to_block(debug_start);

                for (idx, (ty, nullable)) in row_layout.iter().enumerate() {
                    let after_debug = builder.create_block();

                    if nullable {
                        // Zero = value isn't null, non-zero = value is null
                        let non_null = column_non_null(
                            idx,
                            ptr,
                            layout,
                            &mut builder,
                            &self.config,
                            &self.module,
                            true,
                        );

                        // If the value is null, jump to the `write_null` block and debug a null
                        // value (since it's null). Otherwise (if the value isn't null) debug it
                        // and then continue debugging any other fields
                        let debug_value = builder.create_block();
                        let write_null = builder.create_block();
                        builder.ins().brnz(non_null, write_null, &[]);
                        builder.ins().jump(debug_value, &[]);
                        builder.seal_block(builder.current_block().unwrap());

                        // TODO: Should be a static within the jit code
                        static NULL: &str = "null";

                        builder.switch_to_block(write_null);

                        // Write `null` to the output
                        let null_ptr = builder.ins().iconst(ptr_ty, NULL.as_ptr() as i64);
                        let null_len = builder.ins().iconst(ptr_ty, NULL.len() as i64);
                        let call = builder.ins().call(str_debug, &[null_ptr, null_len, fmt]);
                        let call = builder.func.dfg.first_result(call);

                        // If writing `null` failed, return an error
                        let debug_failed = builder.ins().iconst(types::I8, false as i64);
                        builder.ins().brz(call, return_block, &[debug_failed]);
                        builder.ins().jump(after_debug, &[]);
                        builder.seal_block(write_null);

                        builder.switch_to_block(debug_value);
                    }

                    let result = if ty.is_unit() {
                        // TODO: Should be a static within the jit code
                        static UNIT: &str = "()";

                        // Write `()` to the output
                        let unit_ptr = builder.ins().iconst(ptr_ty, UNIT.as_ptr() as i64);
                        let unit_len = builder.ins().iconst(ptr_ty, UNIT.len() as i64);
                        let call = builder.ins().call(str_debug, &[unit_ptr, unit_len, fmt]);
                        builder.func.dfg.first_result(call)
                    } else {
                        // Load the value
                        let offset = layout.row_offset(idx) as i32;
                        let native_ty = layout
                            .row_type(idx)
                            .native_type(&self.module.isa().frontend_config());
                        let flags = MemFlags::trusted().with_readonly();
                        let value = builder.ins().load(native_ty, flags, ptr, offset);

                        let call = match ty {
                            RowType::Bool => {
                                let bool_debug =
                                    imports.dataflow_jit_bool_debug(&mut self.module, builder.func);
                                builder.ins().call(bool_debug, &[value, fmt])
                            }

                            RowType::U16 | RowType::U32 => {
                                let uint_debug =
                                    imports.dataflow_jit_uint_debug(&mut self.module, builder.func);
                                let extended = builder.ins().uextend(types::I64, value);
                                builder.ins().call(uint_debug, &[extended, fmt])
                            }
                            RowType::U64 => {
                                let uint_debug =
                                    imports.dataflow_jit_uint_debug(&mut self.module, builder.func);
                                builder.ins().call(uint_debug, &[value, fmt])
                            }

                            RowType::I16 | RowType::I32 => {
                                let int_debug =
                                    imports.dataflow_jit_int_debug(&mut self.module, builder.func);
                                let extended = builder.ins().sextend(types::I64, value);
                                builder.ins().call(int_debug, &[extended, fmt])
                            }
                            RowType::I64 => {
                                let int_debug =
                                    imports.dataflow_jit_int_debug(&mut self.module, builder.func);
                                builder.ins().call(int_debug, &[value, fmt])
                            }

                            RowType::F32 => {
                                let f32_debug =
                                    imports.dataflow_jit_f32_debug(&mut self.module, builder.func);
                                builder.ins().call(f32_debug, &[value, fmt])
                            }
                            RowType::F64 => {
                                let f64_debug =
                                    imports.dataflow_jit_f64_debug(&mut self.module, builder.func);
                                builder.ins().call(f64_debug, &[value, fmt])
                            }

                            RowType::String => {
                                let string_debug = imports
                                    .dataflow_jit_string_debug(&mut self.module, builder.func);
                                builder.ins().call(string_debug, &[value, fmt])
                            }

                            RowType::Unit => unreachable!(),
                        };
                        builder.func.dfg.first_result(call)
                    };

                    // If writing the value failed, return an error
                    let debug_failed = builder.ins().iconst(types::I8, false as i64);
                    builder.ins().brz(result, return_block, &[debug_failed]);
                    builder.ins().jump(after_debug, &[]);
                    builder.seal_block(builder.current_block().unwrap());
                    builder.switch_to_block(after_debug);

                    // If this is the last column in the row, finish it off with ` }`
                    if idx == row_layout.len() - 1 {
                        // TODO: Should be a static within the jit code
                        static END: &str = " }";

                        // Write the end of a row to the output
                        let end_ptr = builder.ins().iconst(ptr_ty, END.as_ptr() as i64);
                        let end_len = builder.ins().iconst(ptr_ty, END.len() as i64);
                        let call = builder.ins().call(str_debug, &[end_ptr, end_len, fmt]);
                        let result = builder.func.dfg.first_result(call);

                        let debug_failed = builder.ins().iconst(types::I8, false as i64);
                        let debug_success = builder.ins().iconst(types::I8, true as i64);
                        builder.ins().brz(result, return_block, &[debug_failed]);
                        builder.ins().jump(return_block, &[debug_success]);

                    // Otherwise comma-separate each column
                    } else {
                        // TODO: Should be a static within the jit code
                        static COMMA: &str = ", ";

                        let next_debug = builder.create_block();
                        let comma_ptr = builder.ins().iconst(ptr_ty, COMMA.as_ptr() as i64);
                        let comma_len = builder.ins().iconst(ptr_ty, COMMA.len() as i64);
                        let call = builder.ins().call(str_debug, &[comma_ptr, comma_len, fmt]);
                        let result = builder.func.dfg.first_result(call);

                        let debug_failed = builder.ins().iconst(types::I8, false as i64);
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

        println!("{}", self.module_ctx.func.display());
        self.finalize_function(func_id);

        func_id
    }
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

        let mut imports = self.intrinsics.import();

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

            let (layout, row_layout) = self.layout_cache.get_layouts(layout_id);

            if row_layout.needs_drop() {
                let ptr = builder.block_params(entry_block)[0];
                drop_layout(
                    ptr,
                    layout,
                    &row_layout,
                    &mut builder,
                    &mut imports,
                    &mut self.module,
                    &self.config,
                );
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

        let mut imports = self.intrinsics.import();

        {
            let mut builder =
                FunctionBuilder::new(&mut self.module_ctx.func, &mut self.function_ctx);

            // Create the entry block
            let entry_block = builder.create_block();
            builder.switch_to_block(entry_block);

            // Add the function params as block params
            builder.append_block_params_for_function_params(entry_block);

            let (layout, row_layout) = self.layout_cache.get_layouts(layout_id);

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
                drop_layout(
                    ptr,
                    layout,
                    &row_layout,
                    &mut builder,
                    &mut imports,
                    &mut self.module,
                    &self.config,
                );

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

/// Checks if the given row is currently null, returns zero for non-null and
/// non-zero for null
fn column_non_null(
    column: usize,
    row_ptr: ClifValue,
    layout: &Layout,
    builder: &mut FunctionBuilder,
    config: &CodegenConfig,
    module: &JITModule,
    readonly: bool,
) -> ClifValue {
    debug_assert!(layout.is_nullable(column));

    let (bitset_ty, bitset_offset, bit_idx) = layout.row_nullability(column);
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
    if config.null_sigil.is_one() {
        // x & (1 << bit)
        builder.ins().band_imm(bitset, 1i64 << bit_idx)
    } else {
        // !x & (1 << bit)
        let bitset = builder.ins().bnot(bitset);
        builder.ins().band_imm(bitset, 1i64 << bit_idx)
    }
}

fn drop_layout(
    ptr: ClifValue,
    layout: &Layout,
    row_layout: &RowLayout,
    builder: &mut FunctionBuilder,
    imports: &mut ImportIntrinsics,
    module: &mut JITModule,
    config: &CodegenConfig,
) {
    for (idx, (ty, nullable)) in row_layout
        .iter()
        .enumerate()
        // Strings are the only thing that need dropping right now
        .filter(|(_, (ty, _))| ty.is_string())
    {
        debug_assert_eq!(ty, RowType::String);

        let next_drop = if nullable {
            // Zero = string isn't null, non-zero = string is null
            let string_non_null = column_non_null(idx, ptr, layout, builder, config, module, true);

            // If the string is null, jump to the `next_drop` block and don't drop
            // the current string. Otherwise (if the string isn't null) drop it and
            // then continue dropping any other fields
            let drop_string = builder.create_block();
            let next_drop = builder.create_block();
            builder.ins().brnz(string_non_null, next_drop, &[]);
            builder.ins().jump(drop_string, &[]);

            builder.switch_to_block(drop_string);

            Some(next_drop)
        } else {
            None
        };

        // Load the string
        let offset = layout.row_offset(idx) as i32;
        let native_ty = layout
            .row_type(idx)
            .native_type(&module.isa().frontend_config());
        // Readonly isn't transitive and doesn't apply to the data pointed to
        // by the pointer we're loading
        let flags = MemFlags::trusted().with_readonly();
        let string = builder.ins().load(native_ty, flags, ptr, offset);

        // Drop the string
        let string_drop_in_place = imports.dataflow_jit_string_drop_in_place(module, builder.func);
        builder.ins().call(string_drop_in_place, &[string]);

        if let Some(next_drop) = next_drop {
            builder.ins().jump(next_drop, &[]);
            builder.switch_to_block(next_drop);
        }
    }
}
