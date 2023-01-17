mod clone;
mod drop;
mod tests;

use crate::{
    codegen::{utils::FunctionBuilderExt, Codegen, CodegenConfig, Layout, TRAP_NULL_PTR},
    ir::{LayoutId, RowType},
};
use cranelift::{
    codegen::ir::UserFuncName,
    prelude::{
        types, AbiParam, FloatCC, FunctionBuilder, InstBuilder, IntCC, MemFlags, Type as ClifType,
        Value as ClifValue,
    },
};
use cranelift_jit::JITModule;
use cranelift_module::{DataContext, DataId, FuncId, Module};
use dbsp::trace::layers::erased::ErasedVTable;
use std::{
    any::TypeId,
    cmp::Ordering,
    fmt::{self, Debug},
    num::{NonZeroU8, NonZeroUsize},
};

// TODO: The unwinding issues can be solved by creating unwind table entries for
// our generated functions, this'll also make our code more debug-able
// https://github.com/bytecodealliance/wasmtime/issues/5574

macro_rules! vtable {
    ($($func:ident: $ty:ty),+ $(,)?) => {
        #[derive(Debug, Clone, Copy)]
        pub struct LayoutVTable {
            size_of: usize,
            align_of: NonZeroUsize,
            $(pub $func: FuncId,)+
        }

        impl LayoutVTable {
            pub fn marshall(&self, jit: &JITModule) -> ErasedVTable {
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
                        let layout = self.layout_cache.compute(layout_id);
                        (
                            layout.size() as usize,
                            NonZeroUsize::new(layout.align() as usize).unwrap(),
                        )
                    };

                    LayoutVTable {
                        size_of,
                        align_of,
                        $($func: self.[<codegen_layout_ $func>](layout_id),)+
                    }
                }
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
}

fn create_data<D>(data: D, module: &mut JITModule, data_ctx: &mut DataContext) -> DataId
where
    D: Into<Box<[u8]>>,
{
    data_ctx.define(data.into());
    let data_id = module.declare_anonymous_data(false, false).unwrap();
    module.define_data(data_id, data_ctx).unwrap();
    data_ctx.clear();
    data_id
}

// TODO: Memoize these functions so we don't create multiple instances of them
impl Codegen {
    fn new_function<P>(&mut self, params: P, ret: Option<ClifType>) -> FuncId
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

    /// Generates a function comparing two of the given layout for equality
    // FIXME: I took the really lazy route here and pretend that uninit data
    //        either doesn't exist or is always zeroed, if padding bytes
    //        or otherwise uninitialized data doesn't conform to that this'll
    //        spuriously error
    pub fn codegen_layout_eq(&mut self, layout_id: LayoutId) -> FuncId {
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
            } else if row_layout.rows().iter().any(RowType::is_string) {
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
                        row_layout.rows()[idx].is_string(),
                        row_layout.row_is_nullable(idx),
                        layout.offset_of(idx),
                    )
                });

                // Iterate over each field within the struct, comparing them
                for idx in fields {
                    let row_ty = row_layout.rows()[idx];
                    if row_ty.is_unit() && !layout.is_nullable(idx) {
                        continue;
                    }

                    let next_compare = builder.create_block();

                    if layout.is_nullable(idx) {
                        // Zero = value isn't null, non-zero = value is null
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
                        RowType::Bool
                        | RowType::U16
                        | RowType::U32
                        | RowType::U64
                        | RowType::I16
                        | RowType::I32
                        | RowType::I64 => builder.ins().icmp(IntCC::Equal, lhs, rhs),

                        // Compare floats
                        RowType::F32 | RowType::F64 => builder.ins().fcmp(FloatCC::Equal, lhs, rhs),

                        // Compare strings
                        // TODO: If we ever intern or reference count strings, we can partially
                        // inline their comparison by comparing the string's
                        // pointers within jit code. This this could
                        // potentially let us skip function calls for the happy path of
                        // comparing two of the same (either via deduplication or cloning) string
                        RowType::String => {
                            let string_eq = imports.string_eq(&mut self.module, builder.func);
                            builder.call_fn(string_eq, &[lhs, rhs])
                        }

                        // Unit values have already been handled
                        RowType::Unit => unreachable!(),
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

    pub fn codegen_layout_lt(&mut self, layout_id: LayoutId) -> FuncId {
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
                        builder.ins().brnz(both_non_null, neither_null, &[]);
                        builder.ins().jump(secondary_branch, &[]);

                        builder.seal_current();
                        builder.switch_to_block(secondary_branch);

                        let true_val = builder.true_byte();
                        let false_val = builder.false_byte();

                        // if lhs_non_null && !rhs_non_null { return true }
                        let non_null_and_null = builder.ins().icmp(
                            IntCC::UnsignedGreaterThan,
                            lhs_non_null,
                            rhs_non_null,
                        );
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

    pub fn codegen_layout_cmp(&mut self, layout_id: LayoutId) -> FuncId {
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

    pub fn codegen_layout_type_name(&mut self, layout_id: LayoutId) -> FuncId {
        // fn(*mut usize) -> *const u8
        let ptr_ty = self.module.isa().pointer_type();
        let func_id = self.new_function([ptr_ty], Some(ptr_ty));

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

            let type_name = format!("DataflowJitRow({layout:?})").into_bytes();
            let type_name_len = type_name.len();
            let name_id = create_data(type_name, &mut self.module, &mut self.data_ctx);
            let name = self.module.declare_data_in_func(name_id, builder.func);

            // Get the length and pointer to the type name
            let type_name_ptr = builder.ins().symbol_value(ptr_ty, name);
            let type_name_len = builder.ins().iconst(ptr_ty, type_name_len as i64);

            // Write the string's length to the out param
            let length_out = builder.block_params(entry_block)[0];

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

    pub fn codegen_layout_size_of_children(&mut self, layout_id: LayoutId) -> FuncId {
        // fn(*const u8, *mut Context)
        let func_id = self.new_function([self.module.isa().pointer_type(); 2], None);
        let mut imports = self.intrinsics.import();

        {
            let mut builder =
                FunctionBuilder::new(&mut self.module_ctx.func, &mut self.function_ctx);

            // Create the entry block
            let entry_block = builder.create_block();
            builder.switch_to_block(entry_block);

            // Add the function params as block params
            builder.append_block_params_for_function_params(entry_block);

            let params = builder.block_params(entry_block);
            let (ptr, context) = (params[0], params[1]);

            if self.config.debug_assertions {
                builder.ins().trapz(ptr, TRAP_NULL_PTR);
                builder.ins().trapz(context, TRAP_NULL_PTR);
            }

            let (layout, row_layout) = self.layout_cache.get_layouts(layout_id);

            if row_layout.rows().iter().any(RowType::is_string) {
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
                    let offset = layout.offset_of(idx) as i32;
                    let native_ty = layout
                        .type_of(idx)
                        .native_type(&self.module.isa().frontend_config());
                    let flags = MemFlags::trusted().with_readonly();
                    let string = builder.ins().load(native_ty, flags, ptr, offset);

                    // Get the size of the string's children
                    let string_size_of_children =
                        imports.string_size_of_children(&mut self.module, builder.func);
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
        let ptr_ty = self.module.isa().pointer_type();
        let func_id = self.new_function([ptr_ty; 2], Some(types::I8));
        let mut imports = self.intrinsics.import();

        {
            let mut builder =
                FunctionBuilder::new(&mut self.module_ctx.func, &mut self.function_ctx);
            let str_debug = imports.str_debug(&mut self.module, builder.func);

            // Create the entry block
            let entry_block = builder.create_block();
            builder.switch_to_block(entry_block);

            // Add the function params as block params
            builder.append_block_params_for_function_params(entry_block);

            let params = builder.block_params(entry_block);
            let (ptr, fmt) = (params[0], params[1]);

            if self.config.debug_assertions {
                builder.ins().trapz(ptr, TRAP_NULL_PTR);
                builder.ins().trapz(fmt, TRAP_NULL_PTR);
            }

            let return_block = builder.create_block();
            builder.append_block_params_for_function_returns(return_block);

            let (layout, row_layout) = self.layout_cache.get_layouts(layout_id);

            // If the row is empty we can just write an empty row
            if row_layout.is_empty() {
                // Declare the data within the function
                // TODO: Deduplicate data entries within the module
                let empty_data = b"{}";
                let empty_id =
                    create_data(empty_data.to_owned(), &mut self.module, &mut self.data_ctx);
                let empty = self.module.declare_data_in_func(empty_id, builder.func);

                // Write an empty row to the output
                let empty_ptr = builder.ins().symbol_value(ptr_ty, empty);
                let empty_len = builder.ins().iconst(ptr_ty, empty_data.len() as i64);
                let result = builder.call_fn(str_debug, &[empty_ptr, empty_len, fmt]);

                builder.ins().jump(return_block, &[result]);
                builder.seal_block(entry_block);

            // Otherwise, write each row to the output
            } else {
                let start_data = b"{ ";
                let start_id =
                    create_data(start_data.to_owned(), &mut self.module, &mut self.data_ctx);
                let start = self.module.declare_data_in_func(start_id, builder.func);

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
                        builder.seal_current();

                        builder.switch_to_block(write_null);

                        let null_data = b"null";
                        let null_id =
                            create_data(null_data.to_owned(), &mut self.module, &mut self.data_ctx);
                        let null = self.module.declare_data_in_func(null_id, builder.func);

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
                        let unit_id =
                            create_data(unit_data.to_owned(), &mut self.module, &mut self.data_ctx);
                        let unit = self.module.declare_data_in_func(unit_id, builder.func);

                        // Write `()` to the output
                        let unit_ptr = builder.ins().symbol_value(ptr_ty, unit);
                        let unit_len = builder.ins().iconst(ptr_ty, unit_data.len() as i64);
                        builder.call_fn(str_debug, &[unit_ptr, unit_len, fmt])
                    } else {
                        // Load the value
                        let offset = layout.offset_of(idx) as i32;
                        let native_ty = layout
                            .type_of(idx)
                            .native_type(&self.module.isa().frontend_config());
                        let flags = MemFlags::trusted().with_readonly();
                        let value = builder.ins().load(native_ty, flags, ptr, offset);

                        match ty {
                            RowType::Bool => {
                                let bool_debug = imports.bool_debug(&mut self.module, builder.func);
                                builder.call_fn(bool_debug, &[value, fmt])
                            }

                            RowType::U16 | RowType::U32 => {
                                let uint_debug = imports.uint_debug(&mut self.module, builder.func);
                                let extended = builder.ins().uextend(types::I64, value);
                                builder.call_fn(uint_debug, &[extended, fmt])
                            }
                            RowType::U64 => {
                                let uint_debug = imports.uint_debug(&mut self.module, builder.func);
                                builder.call_fn(uint_debug, &[value, fmt])
                            }

                            RowType::I16 | RowType::I32 => {
                                let int_debug = imports.int_debug(&mut self.module, builder.func);
                                let extended = builder.ins().sextend(types::I64, value);
                                builder.call_fn(int_debug, &[extended, fmt])
                            }
                            RowType::I64 => {
                                let int_debug = imports.int_debug(&mut self.module, builder.func);
                                builder.call_fn(int_debug, &[value, fmt])
                            }

                            RowType::F32 => {
                                let f32_debug = imports.f32_debug(&mut self.module, builder.func);
                                builder.call_fn(f32_debug, &[value, fmt])
                            }
                            RowType::F64 => {
                                let f64_debug = imports.f64_debug(&mut self.module, builder.func);
                                builder.call_fn(f64_debug, &[value, fmt])
                            }

                            RowType::String => {
                                let string_debug =
                                    imports.string_debug(&mut self.module, builder.func);
                                builder.call_fn(string_debug, &[value, fmt])
                            }

                            RowType::Unit => unreachable!(),
                        }
                    };

                    // If writing the value failed, return an error
                    let debug_failed = builder.false_byte();
                    builder.ins().brz(result, return_block, &[debug_failed]);
                    builder.ins().jump(after_debug, &[]);
                    builder.seal_block(builder.current_block().unwrap());
                    builder.switch_to_block(after_debug);

                    // If this is the last column in the row, finish it off with ` }`
                    if idx == row_layout.len() - 1 {
                        let end_data = b" }";
                        let end_id =
                            create_data(end_data.to_owned(), &mut self.module, &mut self.data_ctx);
                        let end = self.module.declare_data_in_func(end_id, builder.func);

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
                        let comma_id = create_data(
                            comma_data.to_owned(),
                            &mut self.module,
                            &mut self.data_ctx,
                        );
                        let comma = self.module.declare_data_in_func(comma_id, builder.func);

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

        println!("{}", self.module_ctx.func.display());
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
    layout: &Layout,
    builder: &mut FunctionBuilder,
    config: &CodegenConfig,
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
    if config.null_sigil.is_one() {
        // x & (1 << bit)
        builder.ins().band_imm(bitset, 1i64 << bit_idx)
    } else {
        // !x & (1 << bit)
        let bitset = builder.ins().bnot(bitset);
        builder.ins().band_imm(bitset, 1i64 << bit_idx)
    }
}
