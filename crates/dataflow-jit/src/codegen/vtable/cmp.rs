use crate::{
    codegen::{utils::FunctionBuilderExt, vtable::column_non_null, Codegen, TRAP_NULL_PTR},
    ir::{ColumnType, LayoutId},
};
use cranelift::prelude::{types, FloatCC, FunctionBuilder, InstBuilder, IntCC, MemFlags, Value};
use cranelift_module::{FuncId, Module};
use std::{cmp::Ordering, num::NonZeroU8};

impl Codegen {
    /// Generates a function comparing two of the given layout for equality
    // FIXME: I took the really lazy route here and pretend that uninit data
    //        either doesn't exist or is always zeroed, if padding bytes
    //        or otherwise uninitialized data doesn't conform to that this'll
    //        spuriously error
    // FIXME: Need to implement total_cmp for floats
    // https://doc.rust-lang.org/std/primitive.f32.html#method.total_cmp
    // https://doc.rust-lang.org/std/primitive.f64.html#method.total_cmp
    pub(super) fn codegen_layout_eq(&mut self, layout_id: LayoutId) -> FuncId {
        // fn(*const u8, *const u8) -> bool
        let func_id = self.new_function([self.module.isa().pointer_type(); 2], Some(types::I8));
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
            let params = builder.block_params(entry_block);
            let (lhs, rhs) = (params[0], params[1]);

            if self.config.debug_assertions {
                builder.ins().trapz(lhs, TRAP_NULL_PTR);
                builder.ins().trapz(rhs, TRAP_NULL_PTR);
            }

            // Zero sized types are always equal
            let are_equal = if layout.is_zero_sized() || row_layout.is_empty() {
                builder.true_byte()

            // If there's any strings then comparisons are non-trivial
            } else if row_layout.columns().iter().any(ColumnType::is_string) {
                let return_block = builder.create_block();
                builder.append_block_params_for_function_returns(return_block);

                // We compare the fields of the struct in an order determined by three criteria:
                // - Whether or not it has a non-trivial comparison function (strings)
                // - Whether or not it's nullable
                // - Where it lies within the struct
                // This allows us to do the trivial work (like comparing integers) before we
                // compare the more heavyweight things like strings as well as being marginally
                // more data-local and giving the code generator more flexibility to reduce the
                // number of loads performed
                let mut fields: Vec<_> = (0..row_layout.len()).collect();
                fields.sort_by_key(|&idx| {
                    (
                        row_layout.columns()[idx].is_string(),
                        row_layout.column_nullable(idx),
                        layout.offset_of(idx),
                    )
                });

                // Iterate over each field within the struct, comparing them
                for idx in fields {
                    let row_ty = row_layout.columns()[idx];
                    if row_ty.is_unit() && !layout.is_nullable(idx) {
                        continue;
                    }

                    let next_compare = builder.create_block();

                    if layout.is_nullable(idx) {
                        // Zero = value isn't null, non-zero = value is null
                        let lhs_non_null =
                            column_non_null(idx, lhs, layout, &mut builder, &self.module, true);
                        let rhs_non_null =
                            column_non_null(idx, rhs, layout, &mut builder, &self.module, true);

                        let check_nulls = builder.create_block();
                        let null_eq = builder.ins().icmp(IntCC::Equal, lhs_non_null, rhs_non_null);
                        // If one is null and one is false, return inequal (`null_eq` will always
                        // hold `false` in this case)
                        builder.ins().brz(null_eq, return_block, &[null_eq]);
                        builder.ins().jump(check_nulls, &[]);
                        builder.seal_block(builder.current_block().unwrap());
                        builder.switch_to_block(check_nulls);

                        // For nullable unit values, we're done
                        if row_ty.is_unit() {
                            continue;

                        // For non-unit values we have to compare the inner
                        // value
                        } else {
                            let compare_innards = builder.create_block();

                            // At this point we know both `lhs_non_null` and `rhs_non_null` are
                            // equal, so we just test if the inner value
                            // is null or not
                            builder.ins().brnz(lhs_non_null, next_compare, &[]);
                            builder.ins().jump(compare_innards, &[]);

                            builder.seal_block(check_nulls);
                            builder.switch_to_block(compare_innards);
                        }
                    }

                    debug_assert!(!row_ty.is_unit());

                    // Load both values
                    let (lhs, rhs) = {
                        let offset = layout.offset_of(idx) as i32;
                        let native_ty = layout
                            .type_of(idx)
                            .native_type(&self.module.isa().frontend_config());
                        let flags = MemFlags::trusted().with_readonly();

                        let lhs = builder.ins().load(native_ty, flags, lhs, offset);
                        let rhs = builder.ins().load(native_ty, flags, rhs, offset);

                        (lhs, rhs)
                    };

                    let are_equal = match row_ty {
                        // Compare integers
                        ColumnType::Bool
                        | ColumnType::U16
                        | ColumnType::U32
                        | ColumnType::U64
                        | ColumnType::I16
                        | ColumnType::I32
                        | ColumnType::I64 => builder.ins().icmp(IntCC::Equal, lhs, rhs),

                        // Compare floats
                        ColumnType::F32 | ColumnType::F64 => {
                            // total comparison, see
                            // `f32::total_cmp()`/`f64::total_cmp()`
                            if self.config.total_float_comparisons {
                                let lhs = normalize_float(lhs, &mut builder);
                                let rhs = normalize_float(rhs, &mut builder);
                                builder.ins().icmp(IntCC::Equal, lhs, rhs)
                            } else {
                                builder.ins().fcmp(FloatCC::Equal, lhs, rhs)
                            }
                        }

                        // Compare strings
                        // TODO: If we ever intern or reference count strings, we can partially
                        // inline their comparison by comparing the string's
                        // pointers within jit code. This this could
                        // potentially let us skip function calls for the happy path of
                        // comparing two of the same (either via deduplication or cloning) string
                        ColumnType::String => {
                            let string_eq = imports.string_eq(&mut self.module, builder.func);
                            builder.call_fn(string_eq, &[lhs, rhs])
                        }

                        // Unit values have already been handled
                        ColumnType::Unit => unreachable!(),
                    };

                    // If the values aren't equal, return false (`are_equal` should contain `false`)
                    builder.ins().brz(are_equal, return_block, &[are_equal]);
                    builder.ins().jump(next_compare, &[]);
                    builder.seal_block(builder.current_block().unwrap());

                    builder.switch_to_block(next_compare);
                }

                // If all fields were equal, return `true`
                let true_val = builder.true_byte();
                builder.ins().jump(return_block, &[true_val]);
                builder.seal_block(builder.current_block().unwrap());

                builder.switch_to_block(return_block);
                builder.block_params(return_block)[0]

            // Otherwise we emit a memcmp
            // TODO: Is this valid in the presence of padding?
            } else {
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

    pub(super) fn codegen_layout_lt(&mut self, layout_id: LayoutId) -> FuncId {
        // fn(*const u8, *const u8) -> bool
        let func_id = self.new_function([self.module.isa().pointer_type(); 2], Some(types::I8));
        let mut imports = self.intrinsics.import();

        {
            let mut builder =
                FunctionBuilder::new(&mut self.module_ctx.func, &mut self.function_ctx);

            // Create the entry block
            let entry_block = builder.create_entry_block();

            let (layout, row_layout) = self.layout_cache.get_layouts(layout_id);

            // ZSTs always return `false`, they're always equal
            if !layout.is_zero_sized() {
                let return_block = builder.create_block();
                builder.append_block_params_for_function_returns(return_block);

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
                        let lhs_non_null =
                            column_non_null(idx, lhs, layout, &mut builder, &self.module, true);
                        let rhs_non_null =
                            column_non_null(idx, rhs, layout, &mut builder, &self.module, true);

                        if row_type.is_unit() {
                            // `lhs_non_null < rhs_non_null` gives us our proper ordering, making
                            // `non_null(0) > non_null(0) = false`, `null(1) > null(1) = false`,
                            // `non_null(0) > null(1) = false` and `null(1) > non_null(0) = true`
                            let isnt_less = builder.ins().icmp(
                                IntCC::UnsignedLessThan,
                                lhs_non_null,
                                rhs_non_null,
                            );

                            // if isnt_less { continue }
                            let next = builder.create_block();
                            let true_val = builder.true_byte();
                            builder.ins().brz(isnt_less, next, &[]);

                            // else { return true }
                            builder.ins().jump(return_block, &[true_val]);

                            builder.seal_current();
                            builder.switch_to_block(next);
                            continue;
                        }

                        // if lhs_non_null && rhs_non_null { compare inner values }
                        let neither_null = builder.create_block();
                        let secondary_branch = builder.create_block();
                        let both_non_null = builder.ins().band(lhs_non_null, rhs_non_null);
                        builder.ins().brz(both_non_null, neither_null, &[]);
                        builder.ins().jump(secondary_branch, &[]);

                        builder.seal_current();
                        builder.switch_to_block(secondary_branch);

                        let true_val = builder.true_byte();
                        let false_val = builder.false_byte();

                        // if lhs_non_null && !rhs_non_null { return true }
                        let non_null_and_null =
                            builder
                                .ins()
                                .icmp(IntCC::UnsignedLessThan, lhs_non_null, rhs_non_null);
                        builder
                            .ins()
                            .brnz(non_null_and_null, return_block, &[true_val]);

                        // if (!lhs_non_null && rhs_non_null) || (!lhs_non_null && !rhs_non_null) {
                        // return false }
                        builder.ins().jump(return_block, &[false_val]);

                        // Switch to the neither null block so that we can compare both values
                        builder.seal_block(secondary_branch);
                        builder.switch_to_block(neither_null);
                    }

                    debug_assert!(!row_type.is_unit());

                    // Load each row's value
                    let (lhs, rhs) = {
                        let offset = layout.offset_of(idx) as i32;
                        let native_ty = layout
                            .type_of(idx)
                            .native_type(&self.module.isa().frontend_config());
                        let flags = MemFlags::trusted().with_readonly();

                        let lhs = builder.ins().load(native_ty, flags, lhs, offset);
                        let rhs = builder.ins().load(native_ty, flags, rhs, offset);

                        (lhs, rhs)
                    };

                    let is_less = match row_type {
                        ColumnType::Bool | ColumnType::U16 | ColumnType::U32 | ColumnType::U64 => {
                            builder.ins().icmp(IntCC::UnsignedLessThan, lhs, rhs)
                        }

                        ColumnType::I16 | ColumnType::I32 | ColumnType::I64 => {
                            builder.ins().icmp(IntCC::SignedLessThan, lhs, rhs)
                        }

                        ColumnType::F32 | ColumnType::F64 => {
                            // Uses total comparison, see `f32::total_cmp()`/`f64::total_cmp()`
                            if self.config.total_float_comparisons {
                                let lhs = normalize_float(lhs, &mut builder);
                                let rhs = normalize_float(rhs, &mut builder);
                                builder.ins().icmp(IntCC::SignedLessThan, lhs, rhs)
                            } else {
                                builder.ins().fcmp(FloatCC::LessThan, lhs, rhs)
                            }
                        }

                        ColumnType::Unit => unreachable!(),

                        ColumnType::String => {
                            let string_lt = imports.string_lt(&mut self.module, builder.func);
                            builder.call_fn(string_lt, &[lhs, rhs])
                        }
                    };

                    let next = builder.create_block();
                    let true_val = builder.true_byte();
                    builder.ins().brnz(is_less, return_block, &[true_val]);
                    builder.ins().jump(next, &[]);

                    builder.seal_current();
                    builder.switch_to_block(next);
                }

                // If control flow reaches this point then either all fields are >= and
                // therefore not less than
                let false_val = builder.false_byte();
                builder.ins().jump(return_block, &[false_val]);
                builder.seal_current();

                // Build our return block
                builder.switch_to_block(return_block);
                let is_less = builder.block_params(return_block)[0];
                builder.ins().return_(&[is_less]);
                builder.seal_block(return_block);

            // For zsts we always return false, they're always equal
            } else {
                let false_val = builder.false_byte();
                builder.ins().return_(&[false_val]);
                builder.seal_block(entry_block);
            }

            // Finish building the function
            builder.seal_all_blocks();
            builder.finalize();
        }

        self.finalize_function(func_id);

        func_id
    }

    pub(super) fn codegen_layout_cmp(&mut self, layout_id: LayoutId) -> FuncId {
        // fn(*const u8, *const u8) -> Ordering
        // Ordering is represented as an i8 where -1 = Less, 0 = Equal and 1 = Greater
        let func_id = self.new_function([self.module.isa().pointer_type(); 2], Some(types::I8));
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
                        let lhs_non_null =
                            column_non_null(idx, lhs, layout, &mut builder, &self.module, true);
                        let rhs_non_null =
                            column_non_null(idx, rhs, layout, &mut builder, &self.module, true);

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
                        .type_of(idx)
                        .native_type(&self.module.isa().frontend_config());

                    // Load the column's values
                    let (lhs, rhs) = {
                        let offset = layout.offset_of(idx) as i32;
                        let flags = MemFlags::trusted().with_readonly();

                        let lhs = builder.ins().load(native_ty, flags, lhs, offset);
                        let rhs = builder.ins().load(native_ty, flags, rhs, offset);

                        (lhs, rhs)
                    };

                    match row_type {
                        // Unsigned integers
                        ColumnType::Bool | ColumnType::U16 | ColumnType::U32 | ColumnType::U64 => {
                            let eq = builder.ins().icmp(IntCC::Equal, lhs, rhs);
                            let less = builder.ins().icmp(IntCC::UnsignedLessThan, lhs, rhs);
                            let ordering = builder.ins().select(less, less_than, greater_than);

                            builder.ins().brz(eq, return_block, &[ordering]);
                        }

                        // Signed integers
                        ColumnType::I16 | ColumnType::I32 | ColumnType::I64 => {
                            let eq = builder.ins().icmp(IntCC::Equal, lhs, rhs);
                            let less = builder.ins().icmp(IntCC::SignedLessThan, lhs, rhs);
                            let ordering = builder.ins().select(less, less_than, greater_than);

                            builder.ins().brz(eq, return_block, &[ordering]);
                        }

                        // Floats
                        ColumnType::F32 | ColumnType::F64 => {
                            // Total comparison, see `f32::total_cmp()`/`f64::total_cmp()`
                            let (eq, less) = if self.config.total_float_comparisons {
                                let (lhs, rhs) = (
                                    normalize_float(lhs, &mut builder),
                                    normalize_float(rhs, &mut builder),
                                );

                                let eq = builder.ins().icmp(IntCC::Equal, lhs, rhs);
                                let less = builder.ins().icmp(IntCC::SignedLessThan, lhs, rhs);
                                (eq, less)
                            } else {
                                let eq = builder.ins().fcmp(FloatCC::Equal, lhs, rhs);
                                let less = builder.ins().fcmp(FloatCC::LessThan, lhs, rhs);
                                (eq, less)
                            };

                            let ordering = builder.ins().select(less, less_than, greater_than);
                            builder.ins().brz(eq, return_block, &[ordering]);
                        }

                        ColumnType::Unit => {}

                        ColumnType::String => {
                            let string_cmp = imports.string_cmp(&mut self.module, builder.func);

                            // -1 for less, 0 for equal, 1 for greater
                            let cmp = builder.call_fn(string_cmp, &[lhs, rhs]);

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
fn normalize_float(float: Value, builder: &mut FunctionBuilder<'_>) -> Value {
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

    // left >> {31, 63}
    let shifted = builder.ins().sshr_imm(int, first_shift);
    // ((left >> {31, 63}) as {u32, u64}) >> 1
    let shifted = builder.ins().ushr_imm(shifted, 1);

    // left ^= shifted
    builder.ins().bxor(int, shifted)
}
