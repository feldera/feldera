use crate::{
    codegen::{
        intrinsics::ImportIntrinsics, utils::FunctionBuilderExt, vtable::column_non_null, Codegen,
        NativeLayout, TRAP_NULL_PTR,
    },
    ir::{ColumnType, LayoutId, RowLayout},
};
use cranelift::prelude::{FunctionBuilder, InstBuilder, IntCC, MemFlags, Value};
use cranelift_jit::JITModule;
use cranelift_module::{FuncId, Module};

impl Codegen {
    #[tracing::instrument(skip(self))]
    pub(super) fn codegen_layout_drop_in_place(&mut self, layout_id: LayoutId) -> FuncId {
        tracing::info!("creating drop_in_place vtable function for {layout_id}");

        // fn(*mut u8)
        let func_id = self.new_vtable_fn([self.module.isa().pointer_type()], None);
        let mut imports = self.intrinsics.import(self.comment_writer.clone());

        self.set_comment_writer(
            &format!("{layout_id}_vtable_drop_in_place"),
            &format!("fn(*mut {:?})", self.layout_cache.row_layout(layout_id)),
        );

        {
            let mut builder =
                FunctionBuilder::new(&mut self.module_ctx.func, &mut self.function_ctx);
            let (layout, row_layout) = self.layout_cache.get_layouts(layout_id);

            let entry_block = builder.create_entry_block();

            if row_layout.needs_drop() {
                let ptr = builder.block_params(entry_block)[0];
                drop_layout(
                    ptr,
                    &layout,
                    &row_layout,
                    &mut builder,
                    &mut imports,
                    &mut self.module,
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

    #[tracing::instrument(skip(self))]
    pub(super) fn codegen_layout_drop_slice_in_place(&mut self, layout_id: LayoutId) -> FuncId {
        tracing::info!("creating drop_slice_in_place vtable function for {layout_id}");

        // fn(*mut u8, usize)
        let ptr_ty = self.module.isa().pointer_type();
        let func_id = self.new_vtable_fn([ptr_ty; 2], None);
        let mut imports = self.intrinsics.import(self.comment_writer.clone());

        self.set_comment_writer(
            &format!("{layout_id}_vtable_drop_slice_in_place"),
            &format!(
                "fn(*mut {:?}, usize)",
                self.layout_cache.row_layout(layout_id),
            ),
        );

        {
            let mut builder =
                FunctionBuilder::new(&mut self.module_ctx.func, &mut self.function_ctx);
            let (layout, row_layout) = self.layout_cache.get_layouts(layout_id);

            let entry_block = builder.create_entry_block();

            if row_layout.needs_drop() {
                // Build a tail-controlled loop to drop all elements
                // ```
                // entry(ptr, length):
                //   bytes = imul length, sizeof(layout)
                //   end = iadd ptr, bytes
                //
                //   // Check if `length` is zero and if so, skip dropping
                //   brz length, tail
                //   jump body(ptr)
                //
                // body(ptr):
                //   // drop columns...
                //
                //   ptr_inc = iadd ptr, sizeof(layout)
                //   inbounds = icmp ult ptr_inc, end_ptr
                //   brnz inbounds, body(ptr_inc)
                //   jump tail
                //
                // tail:
                //   return
                // ```

                let params = builder.block_params(entry_block);
                let (ptr, length) = (params[0], params[1]);

                let tail = builder.create_block();
                let body = builder.create_block();
                builder.append_block_param(body, ptr_ty);

                // If debug assertions are enabled, trap if `ptr` is null
                if self.config.debug_assertions {
                    builder.ins().trapz(ptr, TRAP_NULL_PTR);
                }

                // Calculate the slice's end pointer
                let length_bytes = builder.ins().imul_imm(length, layout.size() as i64);
                let end_ptr = builder.ins().iadd(ptr, length_bytes);

                // Check that `length` isn't zero and if so jump to the end
                builder.ins().brif(length, body, &[ptr], tail, &[]);

                builder.seal_block(entry_block);
                builder.switch_to_block(body);

                let ptr = builder.block_params(body)[0];
                drop_layout(
                    ptr,
                    &layout,
                    &row_layout,
                    &mut builder,
                    &mut imports,
                    &mut self.module,
                );

                let ptr = builder.ins().iadd_imm(ptr, layout.size() as i64);
                let ptr_inbounds = builder.ins().icmp(IntCC::UnsignedLessThan, ptr, end_ptr);
                builder.ins().brif(ptr_inbounds, body, &[ptr], tail, &[]);

                builder.seal_current();
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

fn drop_layout(
    ptr: Value,
    layout: &NativeLayout,
    row_layout: &RowLayout,
    builder: &mut FunctionBuilder,
    imports: &mut ImportIntrinsics,
    module: &mut JITModule,
) {
    for (idx, (ty, nullable)) in row_layout
        .iter()
        .enumerate()
        .filter(|(_, (ty, _))| ty.needs_drop())
    {
        // Strings are the only thing that need dropping right now
        debug_assert_eq!(ty, ColumnType::String);

        let next_drop = if nullable {
            // Zero = string isn't null, non-zero = string is null
            let string_null = column_non_null(idx, ptr, layout, builder, false);

            // If the string is null, jump to the `next_drop` block and don't drop
            // the current string. Otherwise (if the string isn't null) drop it and
            // then continue dropping any other fields
            let drop_string = builder.create_block();
            let next_drop = builder.create_block();
            builder
                .ins()
                .brif(string_null, next_drop, &[], drop_string, &[]);

            builder.switch_to_block(drop_string);

            Some(next_drop)
        } else {
            None
        };

        // Load the string
        let offset = layout.offset_of(idx) as i32;
        let native_ty = layout
            .type_of(idx)
            .native_type(&module.isa().frontend_config());
        let flags = MemFlags::trusted();
        let string = builder.ins().load(native_ty, flags, ptr, offset);

        // Drop the string
        let string_drop_in_place = imports.string_drop_in_place(module, builder.func);
        builder.ins().call(string_drop_in_place, &[string]);

        if let Some(next_drop) = next_drop {
            builder.ins().jump(next_drop, &[]);
            builder.switch_to_block(next_drop);
        }
    }
}
