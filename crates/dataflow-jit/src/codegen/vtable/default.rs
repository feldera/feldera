use crate::{
    codegen::{
        layout::MemoryEntry, utils::FunctionBuilderExt, BitSetType, Codegen, CodegenCtx, NativeType,
    },
    ir::LayoutId,
    ThinStr,
};
use cranelift::prelude::{FunctionBuilder, InstBuilder, MemFlags};
use cranelift_module::{FuncId, Module};

impl Codegen {
    pub(super) fn codegen_layout_default(&mut self, layout_id: LayoutId) -> FuncId {
        tracing::info!("creating default vtable function for {layout_id}");

        // fn(*mut u8)
        let func_id = self.new_vtable_fn([self.module.isa().pointer_type()], None);

        self.set_comment_writer(
            &format!("{layout_id}_vtable_default"),
            &format!("fn(*mut {:?})", self.layout_cache.row_layout(layout_id),),
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
            let place = params[0];

            let (layout, row_layout) = ctx.layout_cache.get_layouts(layout_id);

            ctx.debug_assert_ptr_valid(place, layout.align(), &mut builder);

            // Zero sized types are already initialized to their default
            if !layout.is_zero_sized() {
                // If all fields are non-null and trivially zeroable, we can emit a memset
                if !row_layout.has_nullable_columns()
                    && row_layout.columns().iter().all(|ty| !ty.is_string())
                {
                    tracing::trace!("{layout_id} is trivially initializable, emitting zeroed memset for default");

                    builder.emit_small_memset(
                        ctx.frontend_config(),
                        place,
                        0x00,
                        layout.size() as u64,
                        layout.align() as u8,
                        MemFlags::trusted(),
                    );
                } else {
                    // TODO: Could emit memsets for zeroable head and tail of type

                    for entry in layout.memory_order() {
                        match *entry {
                            // Set non-null columns to their default value
                            MemoryEntry::Column {
                                offset,
                                ty,
                                column,
                                nullable: false,
                            } => {
                                // For strings initialize to the empty string
                                let default = if row_layout.column_type(column as usize).is_string()
                                {
                                    builder
                                        .ins()
                                        .iconst(ctx.pointer_type(), ThinStr::sigil_addr() as i64)

                                // For other scalars, initialize to zero
                                } else {
                                    let native = ty.native_type(&ctx.frontend_config());
                                    match ty {
                                        NativeType::U8
                                        | NativeType::I8
                                        | NativeType::U16
                                        | NativeType::I16
                                        | NativeType::U32
                                        | NativeType::I32
                                        | NativeType::U64
                                        | NativeType::I64
                                        | NativeType::Ptr
                                        // Zero is false for bools
                                        | NativeType::Bool
                                        | NativeType::Usize
                                        | NativeType::Isize => builder.ins().iconst(native, 0),
                                        NativeType::F32 => builder.ins().f32const(0.0),
                                        NativeType::F64 => builder.ins().f64const(0.0),
                                    }
                                };

                                builder.ins().store(
                                    MemFlags::trusted(),
                                    default,
                                    place,
                                    offset as i32,
                                );
                            }

                            // Do nothing for nullable columns
                            MemoryEntry::Column { .. } => {}

                            // Set all bitsets to null
                            MemoryEntry::BitSet { offset, ty, .. } => {
                                let all_ones = builder.ins().iconst(
                                    ty.native_type(),
                                    match ty {
                                        BitSetType::U8 => u8::MAX as i64,
                                        BitSetType::U16 => u16::MAX as i64,
                                        BitSetType::U32 => u32::MAX as i64,
                                        BitSetType::U64 => u64::MAX as i64,
                                    },
                                );

                                builder.ins().store(
                                    MemFlags::trusted(),
                                    all_ones,
                                    place,
                                    offset as i32,
                                );
                            }

                            // Do nothing to padding bytes
                            MemoryEntry::Padding { .. } => {}
                        }
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
