use crate::{
    codegen::{
        utils::{column_non_null, set_column_null, FunctionBuilderExt},
        Codegen, CodegenCtx,
    },
    ir::{ColumnType, LayoutId},
    utils::HashMap,
};
use cranelift::prelude::FunctionBuilder;
use cranelift_codegen::ir::{InstBuilder, MemFlags};
use cranelift_module::{FuncId, Module};
use std::{mem::align_of, ops::Not};

// The index of a column within a row
// TODO: Newtyping for column indices within the layout interfaces
type ColumnIdx = usize;

/// The function signature used for json deserialization functions
pub type DeserializeJsonFn = unsafe extern "C" fn(*mut u8, &serde_json::Value);

#[derive(Debug, Clone, PartialEq, Eq)]
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
        let func_id = self.create_function([ptr_ty; 2], None);

        self.set_comment_writer(
            &format!("deserialize_json_{layout_id}"),
            &format!(
                "fn(*mut {}, *const serde_json::Value)",
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
            let [place, json_map]: [_; 2] = builder.block_params(entry_block).try_into().unwrap();

            let layout_cache = ctx.layout_cache.clone();
            let (layout, row_layout) = layout_cache.get_layouts(layout_id);

            ctx.debug_assert_ptr_valid(place, layout.align(), &mut builder);
            ctx.debug_assert_ptr_valid(
                json_map,
                align_of::<serde_json::Value>() as u32,
                &mut builder,
            );

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
                    ColumnType::String => {
                        // Call the deserialization function
                        let deserialize_string =
                            ctx.imports
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

                        // Otherwise trap if deserialization fails or the field
                        // is null
                        // FIXME: This probably shouldn't be a debug assertion,
                        // and we probably want more graceful error handling in
                        // general
                        } else {
                            ctx.debug_assert_false(value_is_null, &mut builder);
                        }
                    }

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

                        // Otherwise trap if deserialization fails or the field
                        // is null
                        // FIXME: This probably shouldn't be a debug assertion,
                        // and we probably want more graceful error handling in
                        // general
                        } else {
                            ctx.debug_assert_false(value_is_null, &mut builder);
                        }
                    }

                    ty => unreachable!("unhandled type in json deserialization: {ty}"),
                }
            }

            builder.ins().return_(&[]);

            builder.seal_all_blocks();
            builder.finalize();
        }

        self.finalize_function(func_id)
    }
}

pub type SerializeJsonFn = unsafe extern "C" fn(*const u8, &mut String);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsonSerConfig {
    pub layout: LayoutId,
    // TODO: Allow specifying how to serialize things, like skipping nulls
    // TODO: Allow serializing into nested structures
    // TODO: Allow specifying date & timestamp formats
    pub mappings: HashMap<ColumnIdx, String>,
}

impl Codegen {
    #[allow(dead_code)]
    pub(crate) fn serialize_json(&mut self, mappings: &JsonSerConfig) -> FuncId {
        let layout_id = mappings.layout;
        tracing::trace!(
            "creating json serializer for {}",
            self.layout_cache.row_layout(layout_id),
        );

        // fn(row: *mut u8, buffer: &mut String)
        let ptr_ty = self.module.isa().pointer_type();
        let func_id = self.create_function([ptr_ty; 2], None);

        self.set_comment_writer(
            &format!("serialize_json_{layout_id}"),
            &format!(
                "fn(row: *mut {}, buffer: &mut String)",
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
            ctx.debug_assert_ptr_valid(buffer, align_of::<String>() as u32, &mut builder);

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
                    .map(|key| key.len() + 4)
                    .sum::<usize>();
                // TODO: We could look at all the column types in the row and estimate their
                // sizes, e.g. `log10()` for integers or `"false".len()` for booleans

                let estimated_capacity = builder.ins().iconst(ptr_ty, estimated_capacity as i64);

                let reserve = ctx
                    .imports
                    .get("std_string_reserve", ctx.module, builder.func);
                builder.ins().call(reserve, &[buffer, estimated_capacity]);
            }

            let push_str = ctx.imports.get("std_string_push", ctx.module, builder.func);

            // Push the starting bracket to the buffer
            let (bracket_ptr, bracket_len) = ctx.import_string("{", &mut builder);
            builder
                .ins()
                .call(push_str, &[buffer, bracket_ptr, bracket_len]);

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

                let json_key = &*mappings.mappings[&column_idx];
                assert!(
                    !json_key.is_empty(),
                    "json pointers cannot be empty (column {column_idx} of {layout_id})",
                );

                let (key_ptr, key_len) =
                    ctx.import_string(format!("\"{json_key}\":"), &mut builder);

                // Push the key to the buffer
                builder.ins().call(push_str, &[buffer, key_ptr, key_len]);

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
                        builder.ins().call(push_str, &[buffer, null_ptr, null_len]);

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

                        builder.ins().call(push_str, &[buffer, ptr, len]);
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

                    ty if ty.is_int() || ty.is_float() || ty.is_date() || ty.is_timestamp() => {
                        let intrinsic = match ty {
                            ColumnType::I8 => "write_i8_to_std_string",
                            ColumnType::U8 => "write_u8_to_std_string",
                            ColumnType::U16 => "write_u16_to_std_string",
                            ColumnType::I16 => "write_i16_to_std_string",
                            ColumnType::U32 => "write_u32_to_std_string",
                            ColumnType::I32 => "write_i32_to_std_string",
                            ColumnType::U64 => "write_u64_to_std_string",
                            ColumnType::I64 => "write_i64_to_std_string",
                            ColumnType::F32 => "write_f32_to_std_string",
                            ColumnType::F64 => "write_f64_to_std_string",
                            // TODO: Allow specifying date & timestamp formats
                            ColumnType::Date => "write_date_to_std_string",
                            ColumnType::Timestamp => "write_timestamp_to_std_string",
                            _ => unreachable!(),
                        };
                        let intrinsic = ctx.imports.get(intrinsic, ctx.module, builder.func);

                        builder.ins().call(intrinsic, &[buffer, value]);
                    }

                    ColumnType::Decimal => {
                        let intrinsic = ctx.imports.get(
                            "write_decimal_to_std_string",
                            ctx.module,
                            builder.func,
                        );

                        let (lo, hi) = builder.ins().isplit(value);
                        builder.ins().call(intrinsic, &[buffer, lo, hi]);
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
                        .call(push_str, &[buffer, comma_ptr, comma_len]);
                }
            }

            // Push the end bracket to the buffer
            let (bracket_ptr, bracket_len) = ctx.import_string("}", &mut builder);
            builder
                .ins()
                .call(push_str, &[buffer, bracket_ptr, bracket_len]);

            builder.ins().return_(&[]);

            builder.seal_all_blocks();
            builder.finalize();
        }

        self.finalize_function(func_id)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        codegen::{
            json::{DeserializeJsonFn, JsonDeserConfig, JsonSerConfig, SerializeJsonFn},
            Codegen, CodegenConfig,
        },
        ir::{ColumnType, RowLayoutBuilder, RowLayoutCache},
        row::{row_from_literal, UninitRow},
        utils::{self, HashMap},
    };
    use std::mem::transmute;

    #[test]
    fn deserialize_json_smoke() {
        utils::test_logger();

        let layout_cache = RowLayoutCache::new();
        let layout = layout_cache.add(
            RowLayoutBuilder::new()
                .with_column(ColumnType::String, false)
                .with_column(ColumnType::String, true)
                .with_column(ColumnType::I64, false)
                .with_column(ColumnType::I64, true)
                .with_column(ColumnType::F64, false)
                .with_column(ColumnType::F64, true)
                .build(),
        );

        let mut codegen = Codegen::new(layout_cache, CodegenConfig::debug());

        let deserialize = JsonDeserConfig {
            layout,
            mappings: {
                let mut mappings = HashMap::default();
                mappings.insert(0, "/foo".to_owned());
                mappings.insert(1, "/bar".to_owned());
                mappings.insert(2, "/baz".to_owned());
                mappings.insert(3, "/bing".to_owned());
                mappings.insert(4, "/bop".to_owned());
                mappings.insert(5, "/boop".to_owned());
                mappings
            },
        };
        let serialize = JsonSerConfig {
            layout,
            mappings: {
                let mut mappings = HashMap::default();
                mappings.insert(0, "foo".to_owned());
                mappings.insert(1, "bar".to_owned());
                mappings.insert(2, "baz".to_owned());
                mappings.insert(3, "bing".to_owned());
                mappings.insert(4, "bop".to_owned());
                mappings.insert(5, "boop".to_owned());
                mappings
            },
        };

        let deserialize_json = codegen.deserialize_json(&deserialize);
        let serialize_json = codegen.serialize_json(&serialize);
        let vtable = codegen.vtable_for(layout);

        let json_snippets = &[
            r#"{ "foo": "foo data string", "bar": "bar data string", "baz": 10, "bing": 100, "bop": 96.542, "boop": -1245.53 }"#,
            r#"{ "foo": "second foo data string", "bar": null, "baz": -10000, "bing": null, "bop": -0.0, "boop": null }"#,
            r#"{ "baz": -32, "bar": null, "foo": "woah, now we switched the field orderings", "bop": 0.3 }"#,
            r#"{ "baz": 0, "bar": null, "foo": "", "bop": "NaN", "boop": "Inf" }"#,
        ];

        #[rustfmt::skip]
        let expected = &[
            row!["foo data string", ?"bar data string", 10i64, ?100i64, 96.542f64, ?-1245.53f64],
            row!["second foo data string", null, -10000i64, null, -0.0, null],
            row!["woah, now we switched the field orderings", null, -32i64, null, 0.3, null],
            row!["", null, 0i64, null, f64::NAN, ?f64::INFINITY],
        ];

        let (jit, layout_cache) = codegen.finalize_definitions();
        let vtable = Box::into_raw(Box::new(vtable.marshalled(&jit)));

        {
            let (deserialize_json, serialize_json) = unsafe {
                (
                    transmute::<_, DeserializeJsonFn>(jit.get_finalized_function(deserialize_json)),
                    transmute::<_, SerializeJsonFn>(jit.get_finalized_function(serialize_json)),
                )
            };

            let mut serialize_buffer = String::new();
            for (&json, expected) in json_snippets.iter().zip(expected) {
                let json_value = serde_json::from_str(json).unwrap();
                let mut uninit = UninitRow::new(unsafe { &*vtable });

                unsafe { deserialize_json(uninit.as_mut_ptr(), &json_value) }

                let row = unsafe { uninit.assume_init() };
                let expected = unsafe {
                    row_from_literal(expected, &*vtable, &layout_cache.layout_of(layout))
                };
                assert_eq!(
                    row,
                    expected,
                    "input json: {json:?}\nrow value for {}: {row:?}",
                    layout_cache.row_layout(layout),
                );

                unsafe { serialize_json(row.as_ptr(), &mut serialize_buffer) }
                // assert_eq!(
                //     json_value,
                //     serde_json::from_str::<serde_json::Value>(&serialize_buffer).unwrap(),
                // );
                println!("{serialize_buffer}");
                serialize_buffer.clear();
            }
        }

        unsafe {
            drop(Box::from_raw(vtable));
            jit.free_memory();
        }
    }
}
