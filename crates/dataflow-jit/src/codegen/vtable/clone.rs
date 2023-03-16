use std::cmp::Ordering;

use crate::{
    codegen::{
        intrinsics::ImportIntrinsics, utils::FunctionBuilderExt, vtable::column_non_null, Codegen,
        NativeLayout, TRAP_NULL_PTR,
    },
    ir::{ColumnType, LayoutId, RowLayout},
};
use cranelift::prelude::{FunctionBuilder, InstBuilder, IntCC, MemFlags, TrapCode, Value};
use cranelift_jit::JITModule;
use cranelift_module::{FuncId, Module};

// FIXME: For non-trivial layouts we could potentially encounter leaks if
// cloning panics part of the way through. For example, if while cloning a `{
// string, string }` we clone the first string successfully and then panic while
// cloning the second string (due to a failed allocation, for example), the
// first successfully cloned string would be leaked. The same effect happens
// with `clone_into_slice`, except with all successfully cloned elements instead
// of just with successfully cloned fields. We probably want to fix that
// sometime by integrating panic handling into our clone routines even though
// this is a fairly minimal consequence of an edge case.

impl Codegen {
    /// Generates a function cloning the given layout
    #[tracing::instrument(skip(self))]
    pub(super) fn codegen_layout_clone(&mut self, layout_id: LayoutId) -> FuncId {
        tracing::info!("creating clone vtable function for {layout_id}");

        // fn(*const u8, *mut u8)
        let func_id = self.new_vtable_fn([self.module.isa().pointer_type(); 2], None);
        let mut imports = self.intrinsics.import(self.comment_writer.clone());

        self.set_comment_writer(
            &format!("{layout_id}_vtable_clone"),
            &format!(
                "fn(*const {0:?}, *mut {0:?})",
                self.layout_cache.row_layout(layout_id),
            ),
        );

        {
            let mut builder =
                FunctionBuilder::new(&mut self.module_ctx.func, &mut self.function_ctx);

            // Create the entry block
            let entry_block = builder.create_entry_block();
            let params = builder.block_params(entry_block);
            let (src, dest) = (params[0], params[1]);

            let (layout, row_layout) = self.layout_cache.get_layouts(layout_id);

            // Zero sized types have nothing to clone
            if !layout.is_zero_sized() {
                // If debug assertions are enabled, trap if `src` or `dest` are null or if
                // `src == dest`
                if self.config.debug_assertions {
                    builder.ins().trapz(src, TRAP_NULL_PTR);
                    builder.ins().trapz(dest, TRAP_NULL_PTR);

                    let src_eq_dest = builder.ins().icmp(IntCC::Equal, src, dest);
                    builder
                        .ins()
                        .trapnz(src_eq_dest, TrapCode::UnreachableCodeReached);

                    if let Some(writer) = self.comment_writer.as_deref() {
                        writer.borrow_mut().add_comment(
                            builder.func.dfg.value_def(src_eq_dest).unwrap_inst(),
                            format!("trap if {src} is equal to {dest}"),
                        );
                    }
                }

                // If the row contains types that require non-trivial cloning (e.g. strings)
                // we have to manually clone each field
                if row_layout.requires_nontrivial_clone() {
                    clone_layout(
                        src,
                        dest,
                        &layout,
                        &row_layout,
                        &mut builder,
                        &mut imports,
                        &mut self.module,
                    );

                // If the row is just scalar types we can simply memcpy it
                } else {
                    let align = layout.align().try_into().unwrap();

                    // TODO: We can make our own more efficient memcpy here, the one that ships with
                    // cranelift is eh
                    builder.emit_small_memory_copy(
                        self.module.isa().frontend_config(),
                        dest,
                        src,
                        layout.size() as u64,
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
    #[tracing::instrument(skip(self))]
    pub(super) fn codegen_layout_clone_into_slice(&mut self, layout_id: LayoutId) -> FuncId {
        tracing::info!("creating clone_into_slice vtable function for {layout_id}");

        // fn(*const u8, *mut u8, usize)
        let ptr_ty = self.module.isa().pointer_type();
        let func_id = self.new_vtable_fn([ptr_ty; 3], None);
        let mut imports = self.intrinsics.import(self.comment_writer.clone());

        self.set_comment_writer(
            &format!("{layout_id}_vtable_clone_into_slice"),
            &format!(
                "fn(*const {0:?}, *mut {0:?}, usize)",
                self.layout_cache.row_layout(layout_id),
            ),
        );

        {
            let mut builder =
                FunctionBuilder::new(&mut self.module_ctx.func, &mut self.function_ctx);

            let entry_block = builder.create_entry_block();
            let (layout, row_layout) = self.layout_cache.get_layouts(layout_id);

            // Zero sized types have nothing to clone
            if !layout.is_zero_sized() {
                let params = builder.block_params(entry_block);
                let (src, dest, length) = (params[0], params[1], params[2]);

                // If debug assertions are enabled, trap if `src` or `dest` are null or if
                // `src == dest`
                if self.config.debug_assertions {
                    builder.ins().trapz(src, TRAP_NULL_PTR);
                    builder.ins().trapz(dest, TRAP_NULL_PTR);

                    let src_eq_dest = builder.ins().icmp(IntCC::Equal, src, dest);
                    builder
                        .ins()
                        .trapnz(src_eq_dest, TrapCode::UnreachableCodeReached);
                }

                // For non-trivial layouts we have to manually clone things
                if row_layout.requires_nontrivial_clone() {
                    // TODO: I wonder if it wouldn't be more efficient to memcpy the entire source
                    // slice into the destination slice and then iterate over
                    // the destination slice while cloning strings in-place, e.g.
                    //
                    // ```
                    // // layout is a `{ string, u32 }`
                    // memcpy(src, dest, sizeof(layout) * length);
                    //
                    // let (mut current_src, mut current_dest) = (src, dest);
                    // let src_end = src.add(length);
                    // while current_src < src_end {
                    //     let current_val = current_src.add(offsetof(layout.0)).read();
                    //     let cloned = clone_string(current_val);
                    //     current_dest.add(offsetof(layout.0)).write(cloned);
                    //
                    //     current_src = current_src.add(sizeof(layout));
                    //     current_dest = current_dest.add(sizeof(layout));
                    // }
                    // ```
                    //
                    // For layouts like `{ string }` this is objectively worse, but there's likely a
                    // tipping point of string vs. scalar memory that makes this
                    // profitable, probably needs experimentation

                    // Build a tail-controlled loop to clone all elements
                    // ```
                    // entry(src, dest, length):
                    //   bytes = imul length, sizeof(layout)
                    //   src_end = iadd src, bytes
                    //
                    //   // Check if `length` is zero and if so, skip cloning
                    //   brz length, tail
                    //   jump body(src, dest)
                    //
                    // body(src, dest):
                    //   // clone columns...
                    //
                    //   src_inc = iadd src, sizeof(layout)
                    //   dest_inc = iadd dest, sizeof(layout)
                    //   inbounds = icmp ult src_inc, src_end
                    //   brnz inbounds, body(src_inc, dest_inc)
                    //   jump tail
                    //
                    // tail:
                    //   return
                    // ```

                    let tail = builder.create_block();
                    let body = builder.create_block();
                    // TODO: Is there a meaningful difference between phi-ing over an offset vs.
                    // phi-ing over the two incremented pointers?
                    builder.append_block_param(body, ptr_ty);
                    builder.append_block_param(body, ptr_ty);

                    // Calculate the slice's end pointer
                    let length_bytes = builder.ins().imul_imm(length, layout.size() as i64);
                    let src_end = builder.ins().iadd(src, length_bytes);

                    // Check that `length` isn't zero and if so jump to the end
                    builder.ins().brif(length, body, &[src, dest], tail, &[]);

                    builder.seal_block(entry_block);
                    builder.switch_to_block(body);

                    let params = builder.block_params(body);
                    let (src, dest) = (params[0], params[1]);
                    clone_layout(
                        src,
                        dest,
                        &layout,
                        &row_layout,
                        &mut builder,
                        &mut imports,
                        &mut self.module,
                    );

                    // Increment both pointers
                    let src_inc = builder.ins().iadd_imm(src, layout.size() as i64);
                    let dest_inc = builder.ins().iadd_imm(dest, layout.size() as i64);

                    // Check if we should continue iterating
                    let ptr_inbounds =
                        builder
                            .ins()
                            .icmp(IntCC::UnsignedLessThan, src_inc, src_end);
                    builder
                        .ins()
                        .brif(ptr_inbounds, body, &[src_inc, dest_inc], tail, &[]);

                    builder.seal_current();
                    builder.switch_to_block(tail);

                // For types consisting entirely of scalar values we can simply
                // emit a memcpy
                } else {
                    // The total size we need to copy is size_of(layout) * length
                    // TODO: Should we add a size/overflow assertion here? Just for
                    // `debug_assertions`?
                    let size = builder.ins().imul_imm(length, layout.size() as i64);
                    builder.call_memcpy(self.module.isa().frontend_config(), dest, src, size);
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

// TODO: We can copy over the bitflag bytes wholesale without doing the whole
// "check bit, set bit, write bit" thing
fn clone_layout(
    src: Value,
    dest: Value,
    layout: &NativeLayout,
    row_layout: &RowLayout,
    builder: &mut FunctionBuilder,
    imports: &mut ImportIntrinsics,
    module: &mut JITModule,
) {
    debug_assert!(row_layout.requires_nontrivial_clone());

    let src_flags = MemFlags::trusted().with_readonly();
    let dest_flags = MemFlags::trusted();

    // TODO: We should do this in layout order instead of field order so we can
    // potentially fuse loads/stores. Even better would be to clone in layout order
    // with padding bytes interspersed (also in layout order) for maximal
    // optimization potential
    for (idx, (ty, nullable)) in row_layout.iter().enumerate() {
        if ty.is_unit() && !nullable {
            continue;
        }

        // TODO: For nullable scalar values we can unconditionally copy them over, we
        // only need to branch for non-trivial clones
        let next_clone = if nullable {
            // Zero = value isn't null, non-zero = value is null
            let value_non_null = column_non_null(idx, src, layout, builder, true);

            // If the value is null, set the cloned value to null
            let (bitset_ty, bitset_offset, bit_idx) = layout.nullability_of(idx);
            let bitset_ty = bitset_ty.native_type();

            let bitset = if layout.bitset_occupants(idx) == 1 {
                let null_ty = builder.func.dfg.value_type(value_non_null);
                match bitset_ty.bytes().cmp(&null_ty.bytes()) {
                    Ordering::Less => builder.ins().ireduce(bitset_ty, value_non_null),
                    Ordering::Equal => value_non_null,
                    Ordering::Greater => builder.ins().uextend(bitset_ty, value_non_null),
                }
            } else {
                // Load the bitset's current value
                let current_bitset =
                    builder
                        .ins()
                        .load(bitset_ty, dest_flags, dest, bitset_offset as i32);

                let mask = 1 << bit_idx;
                let bitset_with_null = builder.ins().bor_imm(current_bitset, mask);
                let bitset_with_non_null = builder.ins().band_imm(current_bitset, !mask);

                builder
                    .ins()
                    .select(value_non_null, bitset_with_null, bitset_with_non_null)
            };

            // Store the newly modified bitset back into the row
            builder
                .ins()
                .store(dest_flags, bitset, dest, bitset_offset as i32);

            // For nullable unit types we don't need to do anything else
            if ty.is_unit() {
                continue;

            // For non-scalar values we need to conditionally clone their
            // innards, we can't clone uninit data without causing UB
            } else if ty.requires_nontrivial_clone() {
                let clone_innards = builder.create_block();
                let next_clone = builder.create_block();
                builder
                    .ins()
                    .brif(value_non_null, next_clone, &[], clone_innards, &[]);

                builder.switch_to_block(clone_innards);
                Some(next_clone)

            // For scalar values we can unconditionally copy over the inner
            // value, it doesn't matter if it's uninit or not since we'll never
            // observe it
            } else {
                None
            }
        } else {
            None
        };

        debug_assert!(!ty.is_unit());

        let offset = layout.offset_of(idx) as i32;
        let native_ty = layout
            .type_of(idx)
            .native_type(&module.isa().frontend_config());

        // Load the source value
        let src_value = builder.ins().load(native_ty, src_flags, src, offset);

        // Clone the source value
        let cloned = match ty {
            // For scalar types we just copy the value directly
            ColumnType::Bool
            | ColumnType::U8
            | ColumnType::I8
            | ColumnType::U16
            | ColumnType::U32
            | ColumnType::U64
            | ColumnType::I16
            | ColumnType::I32
            | ColumnType::I64
            | ColumnType::F32
            | ColumnType::F64
            | ColumnType::Date
            | ColumnType::Timestamp => src_value,

            // Strings need their clone function called
            ColumnType::String => {
                let clone_string = imports.string_clone(module, builder.func);
                builder.call_fn(clone_string, &[src_value])
            }

            // Unit types have been handled
            ColumnType::Ptr | ColumnType::Unit => unreachable!(),
        };

        // Store the cloned value
        builder.ins().store(dest_flags, cloned, dest, offset);

        if let Some(next_clone) = next_clone {
            builder.ins().jump(next_clone, &[]);
            builder.switch_to_block(next_clone);
        }
    }
}
