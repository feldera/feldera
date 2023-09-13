use crate::{
    codegen::{
        json::ColumnIdx,
        utils::{set_column_null, FunctionBuilderExt},
        Codegen, CodegenCtx,
    },
    ir::{ColumnType, LayoutId},
    utils::HashMap,
};
use cranelift::prelude::FunctionBuilder;
use cranelift_codegen::ir::{types, Block, InstBuilder, MemFlags, Type, Value};
use cranelift_module::{FuncId, Module};
use serde::Deserialize;
use std::mem::{self, align_of};

/// The function signature used for json deserialization functions
pub type DeserializeJsonFn =
    unsafe extern "C" fn(*mut u8, &serde_json::Value, &mut String) -> DeserializeResult;

/// A utility function for invoking jit generated deserialization functions
///
/// Takes the deserialization function, a mutable "place" (usually an [`UninitRow`],
/// a properly sized & aligned stack slot or a properly sized and aligned element
/// slot within a vector) and the json value being deserialized.
/// Returns a result, if the result is [`Ok`] then `row_place` will be fully initialized
/// (meaning that calling [`UninitRow::assume_init()`] or a similar function is sound).
/// If the result is [`Err`] then it will contain a formatted error containing best-effort
/// diagnostics
///
/// # Safety
///
/// `row_place` must be properly sized, aligned and mutable for the row type that
/// the deserialization function was created for. Any values residing in the place
/// will not be dropped
///
/// [`UninitRow`]: crate::row::UninitRow
/// [`UninitRow::assume_init()`]: crate::row::UninitRow::assume_init
pub unsafe fn call_deserialize_fn(
    deserialize_fn: DeserializeJsonFn,
    row_place: *mut u8,
    value: &serde_json::Value,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut error = String::new();
    let result = deserialize_fn(row_place, value, &mut error);

    if result.is_ok() {
        // The error string will always be empty so we don't need to drop it
        mem::forget(error);

        Ok(())
    } else {
        #[inline(never)]
        #[cold]
        fn deserialize_error(error: String) -> Box<dyn std::error::Error> {
            format!("an error occurred while parsing the key {error}").into()
        }

        Err(deserialize_error(error))
    }
}

// TODO: Try to incorporate more error codes into this
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
#[repr(u8)]
pub enum DeserializeResult {
    Ok = 0,
    Err,
}

impl DeserializeResult {
    /// Returns `true` if the deserialize result is [`Ok`].
    ///
    /// [`Ok`]: DeserializeResult::Ok
    #[must_use]
    pub const fn is_ok(&self) -> bool {
        matches!(self, Self::Ok)
    }

    /// Returns `true` if the deserialize result is [`Err`].
    ///
    /// [`Err`]: DeserializeResult::Err
    #[must_use]
    pub const fn is_err(&self) -> bool {
        matches!(self, Self::Err)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct JsonDeserConfig {
    pub layout: LayoutId,
    /// A map between column indices and the json pointer used to access them
    // TODO: We probably want a way for users to specify how flexible we are
    // with parsing, e.g. whether we allow parsing an `f64` from a float,
    // an integer, a string or a combination of them
    // TODO: Allow specifying date & timestamp formats
    pub mappings: HashMap<ColumnIdx, String>,
}

impl Codegen {
    pub(crate) fn deserialize_json(&mut self, mappings: &JsonDeserConfig) -> FuncId {
        let layout_id = mappings.layout;
        tracing::trace!(
            "creating json deserializer for {}",
            self.layout_cache.row_layout(layout_id),
        );

        // fn(*mut u8, *const serde_json::Value)
        let ptr_ty = self.module.isa().pointer_type();
        let func_id = self.create_function([ptr_ty; 3], Some(types::I8));

        self.set_comment_writer(
            &format!("deserialize_json_{layout_id}"),
            &format!(
                "fn(*mut {}, *const serde_json::Value, error: &mut String) -> DeserializeResult",
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
            let entry_block = builder.create_entry_block();
            let [place, json_map, error_string]: [_; 3] =
                builder.block_params(entry_block).try_into().unwrap();

            let layout_cache = ctx.layout_cache.clone();
            let (layout, row_layout) = layout_cache.get_layouts(layout_id);

            ctx.debug_assert_ptr_valid(place, layout.align(), &mut builder);
            ctx.debug_assert_ptr_valid(
                json_map,
                align_of::<serde_json::Value>() as u32,
                &mut builder,
            );
            ctx.debug_assert_ptr_valid(error_string, align_of::<String>() as u32, &mut builder);

            let return_error = builder.create_block();
            builder.append_block_param(return_error, ptr_ty);
            builder.append_block_param(return_error, ptr_ty);

            for (column_idx, (column_ty, nullable)) in row_layout.iter().enumerate() {
                // TODO: Json pointers include `/`s to delimit each token, so
                // if a "pointer" has only one leading `/` then we can index
                // directly with that single ident, potentially saving work since
                // we don't have to do path traversal
                // TODO: We can also pre-process path traversals, splitting at `/`s
                // during compile time
                let json_pointer = &*mappings.mappings[&column_idx];
                assert!(
                    !json_pointer.is_empty(),
                    "json pointers cannot be empty (column {column_idx} of {layout_id})",
                );
                assert!(
                    json_pointer.starts_with('/'),
                    "json pointers must start with `/` (this restriction may be loosened in the future)",
                );

                // Add the json pointer to the function's data
                let (json_pointer, json_pointer_len) =
                    ctx.import_string(json_pointer, &mut builder);

                // Get a pointer to the column
                let column_offset = builder
                    .ins()
                    .iconst(ptr_ty, layout.offset_of(column_idx) as i64);
                let column_place = builder.ins().iadd(place, column_offset);

                match column_ty {
                    ColumnType::String => deserialize_string_from_json(
                        &mut ctx,
                        &mut builder,
                        column_place,
                        json_pointer,
                        json_pointer_len,
                        json_map,
                        nullable,
                        ptr_ty,
                        return_error,
                    ),

                    ty @ (ColumnType::Bool
                    | ColumnType::I64
                    | ColumnType::I32
                    | ColumnType::F64
                    | ColumnType::F32) => {
                        let intrinsic = match ty {
                            ColumnType::Bool => "deserialize_json_bool",
                            ColumnType::I64 => "deserialize_json_i64",
                            ColumnType::I32 => "deserialize_json_i32",
                            ColumnType::F64 => "deserialize_json_f64",
                            ColumnType::F32 => "deserialize_json_f32",
                            ty => unreachable!("unhandled type in json deserialization: {ty}"),
                        };

                        // Call the deserialization function
                        let deserialize = ctx.imports.get(intrinsic, ctx.module, builder.func);
                        let value_is_null = builder.call_fn(
                            deserialize,
                            &[column_place, json_pointer, json_pointer_len, json_map],
                        );

                        // If the column is nullable, set its nullness
                        if nullable {
                            set_column_null(
                                value_is_null,
                                column_idx,
                                place,
                                MemFlags::trusted(),
                                &layout,
                                &mut builder,
                            );

                        // Otherwise return an error if deserialization fails or the field is null
                        } else {
                            let after = builder.create_block();
                            builder.ins().brif(
                                value_is_null,
                                return_error,
                                &[json_pointer, json_pointer_len],
                                after,
                                &[],
                            );

                            builder.switch_to_block(after);
                        }
                    }

                    ty => unreachable!("unhandled type in json deserialization: {ty}"),
                }
            }

            // If we reach this everything went smoothly
            let ok = builder
                .ins()
                .iconst(types::I8, DeserializeResult::Ok as i64);
            builder.ins().return_(&[ok]);

            // Build the error block
            // FIXME: Need to drop all previously written values when returning an error
            {
                builder.switch_to_block(return_error);
                builder.set_cold_block(return_error);

                let [key_ptr, key_len]: [_; 2] =
                    builder.block_params(return_error).try_into().unwrap();

                // Write the name of the key to the error output
                let write_str = ctx.imports.get(
                    "write_escaped_string_to_std_string",
                    ctx.module,
                    builder.func,
                );
                builder
                    .ins()
                    .call(write_str, &[error_string, key_ptr, key_len]);

                // Return an error
                let err = builder
                    .ins()
                    .iconst(types::I8, DeserializeResult::Err as i64);
                builder.ins().return_(&[err]);
            }

            builder.seal_all_blocks();
            builder.finalize();
        }

        self.finalize_function(func_id)
    }
}

#[allow(clippy::too_many_arguments)]
fn deserialize_string_from_json(
    ctx: &mut CodegenCtx<'_>,
    builder: &mut FunctionBuilder<'_>,
    column_place: Value,
    json_pointer: Value,
    json_pointer_len: Value,
    json_map: Value,
    nullable: bool,
    ptr_ty: Type,
    return_error: Block,
) {
    // Call the deserialization function
    let deserialize_string = ctx
        .imports
        .get("deserialize_json_string", ctx.module, builder.func);
    let value_is_null = builder.call_fn(
        deserialize_string,
        &[column_place, json_pointer, json_pointer_len, json_map],
    );

    // If the column is nullable, set its nullness
    if nullable {
        let set_null = builder.create_block();
        let after = builder.create_block();

        builder.ins().brif(value_is_null, set_null, &[], after, &[]);
        builder.switch_to_block(set_null);

        let null = builder.ins().iconst(ptr_ty, 0);
        builder
            .ins()
            .store(MemFlags::trusted(), null, column_place, 0);

        builder.ins().jump(after, &[]);
        builder.switch_to_block(after);
        builder.seal_block(set_null);

    // Otherwise return an error if deserialization fails or the field is null
    } else {
        let after = builder.create_block();
        builder.ins().brif(
            value_is_null,
            return_error,
            &[json_pointer, json_pointer_len],
            after,
            &[],
        );

        builder.switch_to_block(after);
    }
}
