use crate::{
    codegen::{
        utils::{set_column_null, FunctionBuilderExt},
        Codegen, CodegenCtx,
    },
    ir::{ColumnType, LayoutId},
};
use cranelift::prelude::{FunctionBuilder, InstBuilder, MemFlags};
use cranelift_module::{FuncId, Module};
use csv::StringRecord;
use std::mem::align_of;

type CsvIndex = usize;
type ColumnIndex = usize;

impl Codegen {
    // TODO: Null values for strings is kinda hard, `,,` could be an empty string
    // and `,null,` could be the string "null" but there's no way for us to
    // distinguish them. We could force the user to choose, force strings to be
    // quoted, etc.
    // See https://docs.snowflake.com/en/user-guide/data-unload-considerations#empty-strings-and-null-values
    // TODO: Pre-parse format strings via `StrftimeItems`
    pub(crate) fn codegen_layout_from_csv(
        &mut self,
        layout_id: LayoutId,
        csv_layout: &[(CsvIndex, ColumnIndex, Option<&str>)],
    ) -> FuncId {
        tracing::info!("creating from csv vtable function for {layout_id}");

        // fn(*mut u8, *const StringRecord)
        let ptr_ty = self.module.isa().pointer_type();
        let func_id = self.new_vtable_fn([ptr_ty; 2], None);

        self.set_comment_writer(
            &format!("{layout_id}_vtable_from_csv"),
            &format!(
                "fn(*mut {}, *mut StringRecord)",
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
            let [place, byte_record]: [_; 2] =
                builder.block_params(entry_block).try_into().unwrap();

            let layout_cache = ctx.layout_cache.clone();
            let (layout, row_layout) = layout_cache.get_layouts(layout_id);

            ctx.debug_assert_ptr_valid(
                byte_record,
                align_of::<StringRecord>() as u32,
                &mut builder,
            );
            ctx.debug_assert_ptr_valid(place, layout.align(), &mut builder);

            for &(csv_column, row_column, format) in csv_layout {
                let column_ty = row_layout.column_type(row_column);
                let nullable = row_layout.column_nullable(row_column);

                if column_ty.is_unit() {
                    if nullable {
                        todo!("nullable unit values from csv?")
                    } else {
                        continue;
                    }
                }

                let csv_column = builder.ins().iconst(ptr_ty, csv_column as i64);
                let column_ptr = builder
                    .ins()
                    .iadd_imm(place, layout.offset_of(row_column) as i64);

                if nullable {
                    // Strings
                    if column_ty.is_string() {
                        // Parse the value from the csv
                        let func =
                            ctx.imports
                                .get("csv_get_nullable_str", ctx.module, builder.func);
                        let parsed = builder.call_fn(func, &[byte_record, csv_column]);

                        // Store the value to the row
                        builder.ins().store(
                            MemFlags::trusted(),
                            parsed,
                            place,
                            layout.offset_of(row_column) as i32,
                        );

                    // Date
                    } else if column_ty.is_date() {
                        let format = format.unwrap();
                        let (format_ptr, format_len) = ctx.import_string(format, &mut builder);

                        // Parse the value from the csv
                        let func =
                            ctx.imports
                                .get("csv_get_nullable_date", ctx.module, builder.func);
                        let is_null = builder.call_fn(
                            func,
                            &[byte_record, csv_column, format_ptr, format_len, column_ptr],
                        );

                        // Set the nullness of the column
                        set_column_null(
                            is_null,
                            row_column,
                            place,
                            MemFlags::trusted(),
                            &layout,
                            &mut builder,
                        );

                    // Timestamp
                    } else if column_ty.is_timestamp() {
                        let format = format.unwrap();
                        let (format_ptr, format_len) = ctx.import_string(format, &mut builder);

                        // Parse the value from the csv
                        let func =
                            ctx.imports
                                .get("csv_get_nullable_timestamp", ctx.module, builder.func);
                        let is_null = builder.call_fn(
                            func,
                            &[byte_record, csv_column, format_ptr, format_len, column_ptr],
                        );

                        // Set the nullness of the column
                        set_column_null(
                            is_null,
                            row_column,
                            place,
                            MemFlags::trusted(),
                            &layout,
                            &mut builder,
                        );

                    // Scalars
                    } else {
                        let intrinsic = match column_ty {
                            ColumnType::Bool => "csv_get_nullable_bool",
                            ColumnType::U8 => "csv_get_nullable_u8",
                            ColumnType::I8 => "csv_get_nullable_i8",
                            ColumnType::U16 => "csv_get_nullable_u16",
                            ColumnType::I16 => "csv_get_nullable_i16",
                            ColumnType::U32 => "csv_get_nullable_u32",
                            ColumnType::I32 => "csv_get_nullable_i32",
                            ColumnType::U64 => "csv_get_nullable_u64",
                            ColumnType::I64 => "csv_get_nullable_i64",
                            ColumnType::Usize => {
                                if ptr_ty.bits() == 32 {
                                    "csv_get_nullable_u32"
                                } else {
                                    "csv_get_nullable_u64"
                                }
                            }
                            ColumnType::Isize => {
                                if ptr_ty.bits() == 32 {
                                    "csv_get_nullable_i32"
                                } else {
                                    "csv_get_nullable_i64"
                                }
                            }
                            ColumnType::F32 => "csv_get_nullable_f32",
                            ColumnType::F64 => "csv_get_nullable_f64",

                            ColumnType::Timestamp
                            | ColumnType::Date
                            | ColumnType::String
                            | ColumnType::Unit
                            | ColumnType::Ptr => {
                                unreachable!()
                            }
                        };

                        // Parse the value from the csv
                        let func = ctx.imports.get(intrinsic, ctx.module, builder.func);
                        let is_null = builder.call_fn(func, &[byte_record, csv_column, column_ptr]);

                        // Set the nullness of the column
                        set_column_null(
                            is_null,
                            row_column,
                            place,
                            MemFlags::trusted(),
                            &layout,
                            &mut builder,
                        );
                    }
                } else {
                    let intrinsic = match column_ty {
                        ColumnType::Bool => "csv_get_bool",
                        ColumnType::U8 => "csv_get_u8",
                        ColumnType::I8 => "csv_get_i8",
                        ColumnType::U16 => "csv_get_u16",
                        ColumnType::I16 => "csv_get_i16",
                        ColumnType::U32 => "csv_get_u32",
                        ColumnType::I32 => "csv_get_i32",
                        ColumnType::U64 => "csv_get_u64",
                        ColumnType::I64 => "csv_get_i64",
                        ColumnType::Usize => {
                            if ptr_ty.bits() == 32 {
                                "csv_get_u32"
                            } else {
                                "csv_get_u64"
                            }
                        }
                        ColumnType::Isize => {
                            if ptr_ty.bits() == 32 {
                                "csv_get_i32"
                            } else {
                                "csv_get_i64"
                            }
                        }
                        ColumnType::F32 => "csv_get_f32",
                        ColumnType::F64 => "csv_get_f64",
                        ColumnType::Date => "csv_get_date",
                        ColumnType::Timestamp => "csv_get_timestamp",
                        ColumnType::String => "csv_get_str",
                        ColumnType::Unit | ColumnType::Ptr => unreachable!(),
                    };

                    // Parse the value from the csv
                    let func = ctx.imports.get(intrinsic, ctx.module, builder.func);
                    let parsed = if column_ty.is_date() || column_ty.is_timestamp() {
                        let format = format.unwrap();
                        let (format_ptr, format_len) = ctx.import_string(format, &mut builder);
                        builder.call_fn(func, &[byte_record, csv_column, format_ptr, format_len])
                    } else {
                        builder.call_fn(func, &[byte_record, csv_column])
                    };

                    // Store the value to the row
                    builder.ins().store(
                        MemFlags::trusted(),
                        parsed,
                        place,
                        layout.offset_of(row_column) as i32,
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
}

#[cfg(test)]
mod tests {
    use crate::{
        codegen::{Codegen, CodegenConfig},
        ir::{ColumnType, RowLayoutBuilder, RowLayoutCache},
        row::UninitRow,
        utils,
    };
    use csv::{ReaderBuilder, StringRecord};
    use std::mem::transmute;

    #[test]
    fn csv_smoke() {
        utils::test_logger();

        let layout_cache = RowLayoutCache::new();
        let layout = layout_cache.add(
            RowLayoutBuilder::new()
                .with_column(ColumnType::Bool, false)
                .with_column(ColumnType::String, false)
                .with_column(ColumnType::I32, false)
                .with_column(ColumnType::Bool, true)
                .with_column(ColumnType::String, true)
                .with_column(ColumnType::I32, true)
                .build(),
        );

        let mut codegen = Codegen::new(layout_cache, CodegenConfig::debug());
        let from_csv = codegen.codegen_layout_from_csv(
            layout,
            &[
                (0, 0, None),
                (1, 1, None),
                (2, 2, None),
                (3, 3, None),
                (4, 4, None),
                (5, 5, None),
            ],
        );
        let vtable = codegen.vtable_for(layout);

        let csv = "true,foo bar baz,-1000,null,null,null\nfalse, bung ,105345453,true,\"\",453";
        let reader = ReaderBuilder::new()
            .has_headers(false)
            .from_reader(csv.as_bytes());

        let (jit, layout_cache) = codegen.finalize_definitions();
        let vtable = Box::into_raw(Box::new(vtable.marshalled(&jit)));

        {
            let from_csv = unsafe {
                transmute::<_, unsafe extern "C" fn(*mut u8, *const u8)>(
                    jit.get_finalized_function(from_csv),
                )
            };

            for record in reader.into_records() {
                let mut uninit = UninitRow::new(unsafe { &*vtable });
                let record = record.unwrap();

                unsafe {
                    from_csv(
                        uninit.as_mut_ptr(),
                        &record as *const StringRecord as *const u8,
                    );
                }

                let row = unsafe { uninit.assume_init() };
                println!(
                    "input csv: {record:?}\nrow value for {}: {row:?}",
                    layout_cache.row_layout(layout),
                );
            }
        }

        unsafe {
            drop(Box::from_raw(vtable));
            jit.free_memory();
        }
    }
}
