use crate::{
    codegen::{
        json::{ColumnIdx, JsonColumn},
        utils::{column_non_null, FunctionBuilderExt},
        Codegen, CodegenCtx,
    },
    ir::{ColumnType, LayoutId},
    utils::HashMap,
};
use cranelift::prelude::FunctionBuilder;
use cranelift_codegen::ir::{InstBuilder, MemFlags};
use cranelift_module::{FuncId, Module};
use serde::Deserialize;
use std::{mem::align_of, ops::Not};

pub type SerializeFn = unsafe extern "C" fn(*const u8, &mut Vec<u8>);

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct JsonSerConfig {
    #[serde(default)]
    pub layout: LayoutId,
    // TODO: Allow specifying how to serialize things, like skipping nulls
    // TODO: Allow serializing into nested structures
    // TODO: Allow specifying date & timestamp formats
    pub mappings: HashMap<ColumnIdx, JsonColumn>,
}

impl Codegen {
    #[allow(dead_code)]
    pub(crate) fn serialize_json(&mut self, mappings: &JsonSerConfig) -> FuncId {
        let layout_id = mappings.layout;
        tracing::trace!(
            "creating json serializer for {}",
            self.layout_cache.row_layout(layout_id),
        );

        // fn(row: *mut u8, buffer: &mut Vec<u8>)
        let ptr_ty = self.module.isa().pointer_type();
        let func_id = self.create_function([ptr_ty; 2], None);

        self.set_comment_writer(
            &format!("serialize_json_{layout_id}"),
            &format!(
                "fn(row: *mut {}, buffer: &mut Vec<u8>)",
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
            let [place, buffer]: [_; 2] = builder.block_params(entry_block).try_into().unwrap();

            let layout_cache = ctx.layout_cache.clone();
            let (layout, row_layout) = layout_cache.get_layouts(layout_id);

            // Ensure that the row pointer is well formed
            ctx.debug_assert_ptr_valid(place, layout.align(), &mut builder);
            // Ensure that the string pointer is well formed
            ctx.debug_assert_ptr_valid(buffer, align_of::<Vec<u8>>() as u32, &mut builder);

            // TODO: We can infer/calculate the structure of the json from the information
            // avaliable, like if a column is stored in `/foo/bar/baz` and
            // another in `/foo/bar/bing` then we know that the structure looks
            // like `{ "foo": { "bar": { "baz": ..., "bing": ... }}}` and we can
            // adapt our serialization ordering to allow us to create nested structures

            // Pre-allocate some of the json object
            {
                let mut estimated_capacity = 2; // The start and end brackets of the object `{}`

                // Length of all keys plus their quotes, `:` and trailing comma
                estimated_capacity += mappings
                    .mappings
                    .values()
                    .map(|column| column.key().len() + 4)
                    .sum::<usize>();
                // TODO: We could look at all the column types in the row and estimate their
                // sizes, e.g. `log10()` for integers or `"false".len()` for booleans

                let estimated_capacity = builder.ins().iconst(ptr_ty, estimated_capacity as i64);

                let reserve = ctx
                    .imports
                    .get("byte_vec_reserve", ctx.module, builder.func);
                builder.ins().call(reserve, &[buffer, estimated_capacity]);
            }

            let push_bytes = ctx.imports.get("byte_vec_push", ctx.module, builder.func);

            // Push the starting bracket to the buffer
            let (bracket_ptr, bracket_len) = ctx.import_string("{", &mut builder);
            builder
                .ins()
                .call(push_bytes, &[buffer, bracket_ptr, bracket_len]);

            let last_idx = row_layout
                .iter()
                .enumerate()
                .filter_map(|(idx, (ty, _))| ty.is_unit().not().then_some(idx))
                .max()
                // TODO: Handle serializing rows without non-unit columns
                .unwrap();

            // Serialize all columns in the row
            for (column_idx, (column_ty, nullable)) in row_layout.iter().enumerate() {
                // We can't serialize unit types
                if column_ty.is_unit() {
                    tracing::warn!(
                        %layout_id,
                        layout = %self.layout_cache.row_layout(layout_id),
                        column_index = column_idx,
                        "serializing row containing a unit column, skipping unit column",
                    );
                    continue;
                }

                let mut after_serialize = None;

                let json_column = if let Some(column) = mappings.mappings.get(&column_idx) {
                    column

                // We don't have to serialize every column in the row
                } else {
                    continue;
                };
                let json_key = json_column.key();
                assert!(
                    !json_key.is_empty(),
                    "json pointers cannot be empty (column {column_idx} of {layout_id})",
                );

                let (key_ptr, key_len) =
                    ctx.import_string(format!("\"{json_key}\":"), &mut builder);

                // Push the key to the buffer
                builder.ins().call(push_bytes, &[buffer, key_ptr, key_len]);

                if nullable {
                    let non_null = column_non_null(column_idx, place, &layout, &mut builder, true);

                    let write_value = builder.create_block();
                    let write_null = builder.create_block();
                    let after_serialize = *after_serialize.insert(builder.create_block());

                    builder
                        .ins()
                        .brif(non_null, write_null, &[], write_value, &[]);
                    builder.seal_current();

                    // Write null to the output
                    {
                        builder.switch_to_block(write_null);

                        let (null_ptr, null_len) = ctx.import_string("null", &mut builder);
                        builder
                            .ins()
                            .call(push_bytes, &[buffer, null_ptr, null_len]);

                        builder.ins().jump(after_serialize, &[]);
                        builder.seal_current();
                    }

                    builder.switch_to_block(write_value);
                }

                let offset = layout.offset_of(column_idx) as i32;
                let native_ty = layout
                    .type_of(column_idx)
                    .native_type(&ctx.module.isa().frontend_config());
                let flags = MemFlags::trusted().with_readonly();
                // TODO: We could take advantage of uload16/uload32/sload16/sload32 here
                // instead of uext/sext later on
                let value = builder.ins().load(native_ty, flags, place, offset);

                match column_ty {
                    ColumnType::Bool => {
                        let (true_ptr, true_len) = ctx.import_string("true", &mut builder);
                        let (false_ptr, false_len) = ctx.import_string("false", &mut builder);

                        let ptr = builder.ins().select(value, true_ptr, false_ptr);
                        let len = builder.ins().select(value, true_len, false_len);

                        builder.ins().call(push_bytes, &[buffer, ptr, len]);
                    }

                    ColumnType::String => {
                        let intrinsic = ctx.imports.get(
                            "write_escaped_string_to_std_string",
                            ctx.module,
                            builder.func,
                        );

                        let ptr = ctx.string_ptr(value, &mut builder);
                        let len = ctx.string_length(value, true, &mut builder);
                        builder.ins().call(intrinsic, &[buffer, ptr, len]);
                    }

                    ty if ty.is_int() || ty.is_float() => {
                        let intrinsic = match ty {
                            ColumnType::I8 => "write_i8_to_byte_vec",
                            ColumnType::U8 => "write_u8_to_byte_vec",
                            ColumnType::U16 => "write_u16_to_byte_vec",
                            ColumnType::I16 => "write_i16_to_byte_vec",
                            ColumnType::U32 => "write_u32_to_byte_vec",
                            ColumnType::I32 => "write_i32_to_byte_vec",
                            ColumnType::U64 => "write_u64_to_byte_vec",
                            ColumnType::I64 => "write_i64_to_byte_vec",
                            ColumnType::F32 => "write_f32_to_byte_vec",
                            ColumnType::F64 => "write_f64_to_byte_vec",
                            _ => unreachable!(),
                        };
                        let intrinsic = ctx.imports.get(intrinsic, ctx.module, builder.func);

                        builder.ins().call(intrinsic, &[buffer, value]);
                    }

                    ColumnType::Decimal => {
                        let intrinsic =
                            ctx.imports
                                .get("write_decimal_to_byte_vec", ctx.module, builder.func);

                        let (lo, hi) = builder.ins().isplit(value);
                        builder.ins().call(intrinsic, &[buffer, lo, hi]);
                    }

                    ty @ (ColumnType::Date | ColumnType::Timestamp) => {
                        let intrinsic = match ty {
                            ColumnType::Date => "write_date_to_byte_vec",
                            ColumnType::Timestamp => "write_timestamp_to_byte_vec",
                            _ => unreachable!(),
                        };
                        let intrinsic = ctx.imports.get(intrinsic, ctx.module, builder.func);

                        let format = json_column
                            .format()
                            .expect("dates and timestamps are required to specify a parse format");
                        let (format_ptr, format_len) = ctx.import_string(format, &mut builder);

                        builder
                            .ins()
                            .call(intrinsic, &[buffer, format_ptr, format_len, value]);
                    }

                    ColumnType::Unit => unreachable!("unit values shouldn't reach here"),
                    unreachable => {
                        unreachable!("unreachable json serialization type: {unreachable:?}");
                    }
                }

                if let Some(after_serialize) = after_serialize {
                    builder.ins().jump(after_serialize, &[]);
                    builder.seal_current();
                    builder.switch_to_block(after_serialize);
                }

                // If there's a column after this one, separate them with a comma
                if column_idx != last_idx {
                    let (comma_ptr, comma_len) = ctx.import_string(",", &mut builder);
                    builder
                        .ins()
                        .call(push_bytes, &[buffer, comma_ptr, comma_len]);
                }
            }

            // Push the end bracket to the buffer
            let (bracket_ptr, bracket_len) = ctx.import_string("}", &mut builder);
            builder
                .ins()
                .call(push_bytes, &[buffer, bracket_ptr, bracket_len]);

            builder.ins().return_(&[]);

            builder.seal_all_blocks();
            builder.finalize();
        }

        self.finalize_function(func_id)
    }
}
