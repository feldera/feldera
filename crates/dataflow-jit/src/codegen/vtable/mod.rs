mod clone;
mod cmp;
mod debug;
mod drop;
mod hash;
mod tests;

use crate::{
    codegen::{Codegen, CodegenCtx, NativeLayout, NativeType},
    ir::{ColumnType, LayoutId},
};
use cranelift::{
    codegen::ir::UserFuncName,
    prelude::{
        AbiParam, FunctionBuilder, InstBuilder, MemFlags, Type as ClifType, Value as ClifValue,
    },
};
use cranelift_jit::JITModule;
use cranelift_module::{FuncId, Module};
use dbsp::trace::layers::erased::ErasedVTable;
use std::{
    any::TypeId,
    cmp::Ordering,
    fmt::{self, Debug},
    hash::Hasher,
    mem::align_of,
    num::NonZeroUsize,
};

// TODO: The unwinding issues can be solved by creating unwind table entries for
// our generated functions, this'll also make our code more debug-able
// https://github.com/bytecodealliance/wasmtime/issues/5574
// TODO: bincode Encode/Decode impls
// TODO: serde Serialize/Deserialize impls?
// TODO: rkyv Archive impls

macro_rules! vtable {
    ($($func:ident: $ty:ty),+ $(,)?) => {
        #[derive(Debug, Clone, Copy)]
        pub struct LayoutVTable {
            size_of: usize,
            align_of: NonZeroUsize,
            layout_id: LayoutId,
            $(pub $func: FuncId,)+
        }

        impl LayoutVTable {
            pub fn erased(&self, jit: &JITModule) -> ErasedVTable {
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

            pub fn marshalled(&self, jit: &JITModule) -> VTable {
                unsafe {
                    VTable {
                        size_of: self.size_of,
                        align_of: self.align_of,
                        layout_id: self.layout_id,
                        $(
                            $func: std::mem::transmute::<*const u8, $ty>(
                                jit.get_finalized_function(self.$func),
                            ),
                        )+
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
                        let layout = self.layout_cache.layout_of(layout_id);
                        (
                            layout.size() as usize,
                            NonZeroUsize::new(layout.align() as usize).unwrap(),
                        )
                    };

                    LayoutVTable {
                        size_of,
                        align_of,
                        layout_id,
                        $($func: {
                            // println!(concat!("vtable function for ", stringify!($func), ":"));
                            self.[<codegen_layout_ $func>](layout_id)
                        },)+
                    }
                }
            }
        }

        #[derive(Clone, Copy)]
        pub struct VTable {
            pub size_of: usize,
            pub align_of: NonZeroUsize,
            pub layout_id: LayoutId,
            $(pub $func: $ty,)+
        }

        impl VTable {
            pub fn type_name(&self) -> &str {
                unsafe {
                    let mut len = 0;
                    let ptr = (self.type_name)(&mut len);

                    let bytes = std::slice::from_raw_parts(ptr, len);
                    debug_assert!(std::str::from_utf8(bytes).is_ok());
                    std::str::from_utf8_unchecked(bytes)
                }
            }
        }

        impl Debug for VTable {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.debug_struct("VTable")
                    .field("size_of", &self.size_of)
                    .field("align_of", &self.align_of)
                    .field("layout_id", &self.layout_id)
                    $(.field(stringify!($func), &(self.$func as *const u8)))+
                    .finish()
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
    hash: unsafe extern "C" fn(&mut &mut dyn Hasher, *const u8),
}

// TODO: Move these functions onto `CodegenCtx`
impl Codegen {
    fn new_vtable_fn<P>(&mut self, params: P, ret: Option<ClifType>) -> FuncId
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

    #[tracing::instrument(skip(self))]
    fn codegen_layout_type_name(&mut self, layout_id: LayoutId) -> FuncId {
        tracing::info!("creating type_name vtable function for {layout_id}");

        // fn(*mut usize) -> *const u8
        let ptr_ty = self.module.isa().pointer_type();
        let func_id = self.new_vtable_fn([ptr_ty], Some(ptr_ty));

        self.set_comment_writer(
            &format!("{layout_id}_vtable_type_name"),
            "fn(*mut usize) -> *const u8",
        );

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

            // Create the entry block
            let entry_block = builder.create_block();
            builder.append_block_params_for_function_params(entry_block);
            builder.switch_to_block(entry_block);

            // Write the string's length to the out param
            let length_out = builder.block_params(entry_block)[0];
            ctx.debug_assert_ptr_valid(
                length_out,
                NativeType::Usize.align(&ctx.frontend_config()),
                &mut builder,
            );

            // Get the length and pointer to the type name
            let type_name = format!("{:?}", ctx.layout_cache.row_layout(layout_id));
            let (type_name_ptr, type_name_len) = ctx.import_string(type_name, &mut builder);

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

    #[tracing::instrument(skip(self))]
    fn codegen_layout_size_of_children(&mut self, layout_id: LayoutId) -> FuncId {
        tracing::info!("creating size_of_children vtable function for {layout_id}");

        // fn(*const u8, *mut Context)
        let func_id = self.new_vtable_fn([self.module.isa().pointer_type(); 2], None);

        self.set_comment_writer(
            &format!("{layout_id}_vtable_size_of_children"),
            &format!(
                "fn(*const {:?}, *mut size_of::Context)",
                self.layout_cache.row_layout(layout_id),
            ),
        );

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

            // Create the entry block
            let entry_block = builder.create_block();
            builder.switch_to_block(entry_block);

            // Add the function params as block params
            builder.append_block_params_for_function_params(entry_block);

            let (layout, row_layout) = self.layout_cache.get_layouts(layout_id);

            let params = builder.block_params(entry_block);
            let (ptr, context) = (params[0], params[1]);
            ctx.debug_assert_ptr_valid(ptr, layout.align(), &mut builder);
            ctx.debug_assert_ptr_valid(
                context,
                align_of::<size_of::Context>() as u32,
                &mut builder,
            );

            if row_layout.columns().iter().any(ColumnType::is_string) {
                for (idx, (ty, nullable)) in row_layout
                    .iter()
                    .enumerate()
                    // Strings are the only thing that have children sizes right now
                    .filter(|(_, (ty, _))| ty.is_string())
                {
                    debug_assert_eq!(ty, ColumnType::String);

                    let next_size_of = if nullable {
                        // Zero = string isn't null, non-zero = string is null
                        let string_non_null =
                            column_non_null(idx, ptr, &layout, &mut builder, true);

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
                    let native_ty = layout.type_of(idx).native_type(&ctx.frontend_config());
                    let flags = MemFlags::trusted().with_readonly();
                    let string = builder.ins().load(native_ty, flags, ptr, offset);

                    // Get the size of the string's children
                    let string_size_of_children = ctx
                        .imports
                        .string_size_of_children(ctx.module, builder.func);
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
    layout: &NativeLayout,
    builder: &mut FunctionBuilder,
    readonly: bool,
) -> ClifValue {
    debug_assert!(layout.is_nullable(column));

    let (bitset_ty, bitset_offset, bit_idx) = layout.nullability_of(column);
    let bitset_ty = bitset_ty.native_type();

    // Create the flags for the load
    let mut flags = MemFlags::trusted();
    if readonly {
        flags.set_readonly();
    }

    // Load the bitset containing the given column's nullability
    let bitset = builder
        .ins()
        .load(bitset_ty, flags, row_ptr, bitset_offset as i32);

    // Zero is true (the value isn't null), non-zero is false
    // (the value is null)
    if layout.bitset_occupants(column) == 1 {
        // If there's only a single occupant of the bitset, it's already in the proper
        // format
        bitset
    } else {
        // Otherwise we mask all bits other than the one we're interested in
        builder.ins().band_imm(bitset, 1i64 << bit_idx)
    }
}
