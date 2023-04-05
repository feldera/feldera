use crate::codegen::{utils::FunctionBuilderExt, CodegenCtx};
use cranelift::prelude::{types, FunctionBuilder, InstBuilder, IntCC, MemFlags, Value};

const TIMESTAMP_YEAR_ERROR: i64 = i64::MIN;

impl CodegenCtx<'_> {
    pub(super) fn timestamp_year(
        &mut self,
        millis: Value,
        builder: &mut FunctionBuilder<'_>,
    ) -> Value {
        let ptr_type = self.pointer_type();

        let year_deltas = self.create_data(YEAR_DELTAS);
        let year_deltas = self.import_data(year_deltas, builder);

        let year_to_flags = self.create_data(YEAR_TO_FLAGS);
        let year_to_flags = self.import_data(year_to_flags, builder);

        let result_block = builder.create_block();
        builder.append_block_param(result_block, types::I64);

        let error_year = builder.ins().iconst(types::I64, TIMESTAMP_YEAR_ERROR);

        // let mut secs = millis / 1000;
        // if millis % 1000 < 0 {
        //     secs -= 1;
        // }
        let secs = {
            let one = builder.ins().iconst(types::I64, 1);
            let one_thousand = builder.ins().iconst(types::I64, 1000);

            let secs = builder.ins().sdiv(millis, one_thousand);
            let secs_sub_one = builder.ins().isub(secs, one);

            let millis_mod_1000 = builder.ins().srem(millis, one_thousand);

            let millis_lt_zero = builder
                .ins()
                .icmp_imm(IntCC::SignedLessThan, millis_mod_1000, 0);

            builder.ins().select(millis_lt_zero, secs_sub_one, secs)
        };

        // let days = div_floor(secs, 86_400);
        let days = {
            let secs_in_day = builder.ins().iconst(types::I64, 86_400);
            self.div_floor(true, secs, secs_in_day, builder)
        };

        // let days = if (i32::MIN as i64) <= days && days <= (i32::MAX as i64) {
        //     days as i32
        // } else {
        //     return TIMESTAMP_YEAR_ERROR;
        // };
        let days = {
            let days_ge_min =
                builder
                    .ins()
                    .icmp_imm(IntCC::SignedGreaterThanOrEqual, days, i32::MIN as i64);
            let days_le_max =
                builder
                    .ins()
                    .icmp_imm(IntCC::SignedLessThanOrEqual, days, i32::MAX as i64);
            let days_inbounds = builder.ins().band(days_ge_min, days_le_max);

            let days_valid = builder.create_block();
            builder
                .ins()
                .brif(days_inbounds, days_valid, &[], result_block, &[error_year]);

            builder.seal_current();
            builder.switch_to_block(days_valid);

            builder.ins().ireduce(types::I32, days)
        };

        // let days = if let Some(days) = days.checked_add(719_163) {
        //     days
        // } else {
        //     return TIMESTAMP_YEAR_ERROR;
        // };
        let days = {
            let offset = builder.ins().iconst(types::I32, 719_163);
            let (days, add_overflowed) = builder.ins().iadd_cout(days, offset);

            let days_valid = builder.create_block();
            builder
                .ins()
                .brif(add_overflowed, result_block, &[error_year], days_valid, &[]);

            builder.seal_current();
            builder.switch_to_block(days_valid);

            days
        };

        // let days = days + 365;
        let days_in_year = builder.ins().iconst(types::I32, 365);
        let days = builder.ins().iadd(days, days_in_year);

        // let (year_div_400, cycle) = div_mod_floor(days, 146_097);
        let cycle_const = builder.ins().iconst(types::I32, 146_097);
        let (year_div_400, cycle) = self.sdiv_mod_floor(days, cycle_const, builder);

        // let (mut year_mod_400, mut ordinal0) = div_rem(cycle as u32, 365);
        let (year_mod_400, ordinal0) = self.div_rem(false, cycle, days_in_year, builder);

        // let delta = YEAR_DELTAS[year_mod_400 as usize] as u32;
        let year_deltas_ptr = builder.ins().symbol_value(ptr_type, year_deltas);
        let delta = {
            let year_mod_400_usize = self.cast_to_ptr_size(year_mod_400, builder);
            let delta_ptr = builder.ins().iadd(year_deltas_ptr, year_mod_400_usize);
            let delta =
                builder
                    .ins()
                    .load(types::I8, MemFlags::trusted().with_readonly(), delta_ptr, 0);
            builder.ins().uextend(types::I32, delta)
        };

        // if ordinal0 < delta {
        //     year_mod_400 -= 1;
        //     ordinal0 += 365 - YEAR_DELTAS[year_mod_400 as usize] as u32;
        // } else {
        //     ordinal0 -= delta;
        // }
        let (year_mod_400, ordinal0) = {
            let ordinal0_is_lt = builder.create_block();
            let ordinal0_is_ge = builder.create_block();

            let with_ordinal0 = builder.create_block();
            builder.append_block_param(with_ordinal0, types::I32);
            builder.append_block_param(with_ordinal0, types::I32);

            let ordinal0_lt_delta = builder.ins().icmp(IntCC::UnsignedLessThan, ordinal0, delta);
            builder
                .ins()
                .brif(ordinal0_lt_delta, ordinal0_is_lt, &[], ordinal0_is_ge, &[]);

            builder.seal_current();
            builder.switch_to_block(ordinal0_is_lt);

            {
                //     year_mod_400 -= 1;
                let one = builder.ins().iconst(types::I32, 1);
                let year_mod_400 = builder.ins().isub(year_mod_400, one);

                //     ordinal0 += 365 - YEAR_DELTAS[year_mod_400 as usize] as u32;
                let year_mod_400_usize = self.cast_to_ptr_size(year_mod_400, builder);
                let delta_ptr = builder.ins().iadd(year_deltas_ptr, year_mod_400_usize);
                let delta = builder.ins().load(
                    types::I8,
                    MemFlags::trusted().with_readonly(),
                    delta_ptr,
                    0,
                );
                let delta = builder.ins().uextend(types::I32, delta);

                let days_in_year_sub_delta = builder.ins().isub(days_in_year, delta);
                let ordinal0 = builder.ins().iadd(ordinal0, days_in_year_sub_delta);

                builder.ins().jump(with_ordinal0, &[year_mod_400, ordinal0]);
            }

            builder.seal_current();
            builder.switch_to_block(ordinal0_is_ge);

            {
                //     ordinal0 -= delta;
                let ordinal0 = builder.ins().isub(ordinal0, delta);
                builder.ins().jump(with_ordinal0, &[year_mod_400, ordinal0]);
            }

            builder.seal_current();
            builder.switch_to_block(with_ordinal0);

            let params = builder.block_params(with_ordinal0);
            (params[0], params[1])
        };

        // let ordinal = ordinal0 + 1;
        let ordinal = builder.ins().iadd_imm(ordinal0, 1);

        // let flags = YEAR_TO_FLAGS[year_mod_400 as usize];
        let flags = {
            let year_mod_400_usize = self.cast_to_ptr_size(year_mod_400, builder);
            let year_to_flags_ptr = builder.ins().symbol_value(ptr_type, year_to_flags);
            let flags_ptr = builder.ins().iadd(year_to_flags_ptr, year_mod_400_usize);

            let flags =
                builder
                    .ins()
                    .load(types::I8, MemFlags::trusted().with_readonly(), flags_ptr, 0);
            builder.ins().uextend(types::I32, flags)
        };

        // let ol = if ordinal <= 366 {
        //     ((ordinal << 4) | flags as u32) >> 3
        // } else {
        //     return TIMESTAMP_YEAR_ERROR;
        // };
        let ordinal_valid = builder.create_block();

        let ordinal_le_366 = builder
            .ins()
            .icmp_imm(IntCC::UnsignedLessThanOrEqual, ordinal, 366);
        builder.ins().brif(
            ordinal_le_366,
            ordinal_valid,
            &[],
            result_block,
            &[error_year],
        );

        builder.seal_current();
        builder.switch_to_block(ordinal_valid);

        let of = builder.ins().ishl_imm(ordinal, 4);
        let of = builder.ins().bor(of, flags);
        let ol = builder.ins().ushr_imm(of, 3);

        // let year = year_div_400 * 400 + year_mod_400 as i32;
        let year = builder.ins().imul_imm(year_div_400, 400);
        let year = builder.ins().iadd(year, year_mod_400);

        // if year >= MIN_YEAR && year <= MAX_YEAR && ol >= MIN_OL && ol <= MAX_OL {
        //     year as i64
        // } else {
        //     TIMESTAMP_YEAR_ERROR
        // }
        let year_ge_min =
            builder
                .ins()
                .icmp_imm(IntCC::SignedGreaterThanOrEqual, year, MIN_YEAR as i64);
        let year_le_max =
            builder
                .ins()
                .icmp_imm(IntCC::SignedLessThanOrEqual, year, MAX_YEAR as i64);
        let ol_ge_min =
            builder
                .ins()
                .icmp_imm(IntCC::UnsignedGreaterThanOrEqual, ol, MIN_OL as i64);
        let ol_le_max = builder
            .ins()
            .icmp_imm(IntCC::UnsignedLessThanOrEqual, ol, MAX_OL as i64);

        let year_valid = builder.ins().band(year_ge_min, year_le_max);
        let ol_valid = builder.ins().band(ol_ge_min, ol_le_max);
        let is_valid = builder.ins().band(year_valid, ol_valid);

        let wide_year = builder.ins().sextend(types::I64, year);
        builder.ins().brif(
            is_valid,
            result_block,
            &[wide_year],
            result_block,
            &[error_year],
        );

        builder.seal_current();
        builder.switch_to_block(result_block);
        builder.block_params(result_block)[0]
    }
}

const MAX_YEAR: i32 = i32::MAX >> 13;
const MIN_YEAR: i32 = i32::MIN >> 13;

const MIN_OL: u32 = 1 << 1;
const MAX_OL: u32 = 366 << 1; // larger than the non-leap last day `(365 << 1) | 1`

static YEAR_TO_FLAGS: [u8; 400] = {
    const A: u8 = 0o15;
    const AG: u8 = 0o05;
    const B: u8 = 0o14;
    const BA: u8 = 0o04;
    const C: u8 = 0o13;
    const CB: u8 = 0o03;
    const D: u8 = 0o12;
    const DC: u8 = 0o02;
    const E: u8 = 0o11;
    const ED: u8 = 0o01;
    const F: u8 = 0o17;
    const FE: u8 = 0o07;
    const G: u8 = 0o16;
    const GF: u8 = 0o06;

    [
        BA, G, F, E, DC, B, A, G, FE, D, C, B, AG, F, E, D, CB, A, G, F, ED, C, B, A, GF, E, D, C,
        BA, G, F, E, DC, B, A, G, FE, D, C, B, AG, F, E, D, CB, A, G, F, ED, C, B, A, GF, E, D, C,
        BA, G, F, E, DC, B, A, G, FE, D, C, B, AG, F, E, D, CB, A, G, F, ED, C, B, A, GF, E, D, C,
        BA, G, F, E, DC, B, A, G, FE, D, C, B, AG, F, E, D, // 100
        C, B, A, G, FE, D, C, B, AG, F, E, D, CB, A, G, F, ED, C, B, A, GF, E, D, C, BA, G, F, E,
        DC, B, A, G, FE, D, C, B, AG, F, E, D, CB, A, G, F, ED, C, B, A, GF, E, D, C, BA, G, F, E,
        DC, B, A, G, FE, D, C, B, AG, F, E, D, CB, A, G, F, ED, C, B, A, GF, E, D, C, BA, G, F, E,
        DC, B, A, G, FE, D, C, B, AG, F, E, D, CB, A, G, F, // 200
        E, D, C, B, AG, F, E, D, CB, A, G, F, ED, C, B, A, GF, E, D, C, BA, G, F, E, DC, B, A, G,
        FE, D, C, B, AG, F, E, D, CB, A, G, F, ED, C, B, A, GF, E, D, C, BA, G, F, E, DC, B, A, G,
        FE, D, C, B, AG, F, E, D, CB, A, G, F, ED, C, B, A, GF, E, D, C, BA, G, F, E, DC, B, A, G,
        FE, D, C, B, AG, F, E, D, CB, A, G, F, ED, C, B, A, // 300
        G, F, E, D, CB, A, G, F, ED, C, B, A, GF, E, D, C, BA, G, F, E, DC, B, A, G, FE, D, C, B,
        AG, F, E, D, CB, A, G, F, ED, C, B, A, GF, E, D, C, BA, G, F, E, DC, B, A, G, FE, D, C, B,
        AG, F, E, D, CB, A, G, F, ED, C, B, A, GF, E, D, C, BA, G, F, E, DC, B, A, G, FE, D, C, B,
        AG, F, E, D, CB, A, G, F, ED, C, B, A, GF, E, D, C, // 400
    ]
};

static YEAR_DELTAS: [u8; 401] = [
    0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 6, 6, 6, 6, 7, 7, 7, 7, 8, 8, 8,
    8, 9, 9, 9, 9, 10, 10, 10, 10, 11, 11, 11, 11, 12, 12, 12, 12, 13, 13, 13, 13, 14, 14, 14, 14,
    15, 15, 15, 15, 16, 16, 16, 16, 17, 17, 17, 17, 18, 18, 18, 18, 19, 19, 19, 19, 20, 20, 20, 20,
    21, 21, 21, 21, 22, 22, 22, 22, 23, 23, 23, 23, 24, 24, 24, 24, 25, 25, 25, 25, 25, 25, 25, 25,
    26, 26, 26, 26, 27, 27, 27, 27, 28, 28, 28, 28, 29, 29, 29, 29, 30, 30, 30, 30, 31, 31, 31, 31,
    32, 32, 32, 32, 33, 33, 33, 33, 34, 34, 34, 34, 35, 35, 35, 35, 36, 36, 36, 36, 37, 37, 37, 37,
    38, 38, 38, 38, 39, 39, 39, 39, 40, 40, 40, 40, 41, 41, 41, 41, 42, 42, 42, 42, 43, 43, 43, 43,
    44, 44, 44, 44, 45, 45, 45, 45, 46, 46, 46, 46, 47, 47, 47, 47, 48, 48, 48, 48, 49, 49, 49, 49,
    49, 49, 49, 49, 50, 50, 50, 50, 51, 51, 51, 51, 52, 52, 52, 52, 53, 53, 53, 53, 54, 54, 54, 54,
    55, 55, 55, 55, 56, 56, 56, 56, 57, 57, 57, 57, 58, 58, 58, 58, 59, 59, 59, 59, 60, 60, 60, 60,
    61, 61, 61, 61, 62, 62, 62, 62, 63, 63, 63, 63, 64, 64, 64, 64, 65, 65, 65, 65, 66, 66, 66, 66,
    67, 67, 67, 67, 68, 68, 68, 68, 69, 69, 69, 69, 70, 70, 70, 70, 71, 71, 71, 71, 72, 72, 72, 72,
    73, 73, 73, 73, 73, 73, 73, 73, 74, 74, 74, 74, 75, 75, 75, 75, 76, 76, 76, 76, 77, 77, 77, 77,
    78, 78, 78, 78, 79, 79, 79, 79, 80, 80, 80, 80, 81, 81, 81, 81, 82, 82, 82, 82, 83, 83, 83, 83,
    84, 84, 84, 84, 85, 85, 85, 85, 86, 86, 86, 86, 87, 87, 87, 87, 88, 88, 88, 88, 89, 89, 89, 89,
    90, 90, 90, 90, 91, 91, 91, 91, 92, 92, 92, 92, 93, 93, 93, 93, 94, 94, 94, 94, 95, 95, 95, 95,
    96, 96, 96, 96, 97, 97, 97, 97,
];

#[cfg(test)]
mod tests {
    use crate::{
        codegen::{
            timestamp::{
                MAX_OL, MAX_YEAR, MIN_OL, MIN_YEAR, TIMESTAMP_YEAR_ERROR, YEAR_DELTAS,
                YEAR_TO_FLAGS,
            },
            Codegen, CodegenConfig,
        },
        ir::{
            exprs::{ArgType, Call},
            ColumnType, FunctionBuilder, RowLayoutBuilder, RowLayoutCache,
        },
        utils,
    };
    use chrono::{Datelike, TimeZone, Utc};
    use num_integer::{div_floor, div_mod_floor, div_rem};
    use proptest::{
        prelude::any,
        prop_assert_eq, proptest,
        test_runner::{Config, TestRunner},
    };
    use std::mem::transmute;

    fn year_of_timestamp(millis: i64) -> i64 {
        Utc.timestamp_millis_opt(millis)
            .single()
            .map_or(TIMESTAMP_YEAR_ERROR, |time| time.year() as i64)
    }

    #[allow(clippy::manual_range_contains)]
    fn inlined_year_of_timestamp(millis: i64) -> i64 {
        let mut secs = millis / 1000;
        if millis % 1000 < 0 {
            secs -= 1;
        }

        let days = div_floor(secs, 86_400);

        let days = if (i32::MIN as i64) <= days && days <= (i32::MAX as i64) {
            days as i32
        } else {
            return TIMESTAMP_YEAR_ERROR;
        };

        let days = if let Some(days) = days.checked_add(719_163) {
            days
        } else {
            return TIMESTAMP_YEAR_ERROR;
        };

        let days = days + 365; // make December 31, 1 BCE equal to day 0
        let (year_div_400, cycle) = div_mod_floor(days, 146_097);

        let (mut year_mod_400, mut ordinal0) = div_rem(cycle as u32, 365);
        let delta = YEAR_DELTAS[year_mod_400 as usize] as u32;
        if ordinal0 < delta {
            year_mod_400 -= 1;
            ordinal0 += 365 - YEAR_DELTAS[year_mod_400 as usize] as u32;
        } else {
            ordinal0 -= delta;
        }

        let ordinal = ordinal0 + 1;

        let flags = YEAR_TO_FLAGS[year_mod_400 as usize];

        let ol = if ordinal <= 366 {
            ((ordinal << 4) | flags as u32) >> 3
        } else {
            return TIMESTAMP_YEAR_ERROR;
        };

        let year = year_div_400 * 400 + year_mod_400 as i32;

        if year >= MIN_YEAR && year <= MAX_YEAR && ol >= MIN_OL && ol <= MAX_OL {
            year as i64
        } else {
            TIMESTAMP_YEAR_ERROR
        }
    }

    proptest! {
        #[test]
        fn reference_timestamp_year(millis in any::<i64>()) {
            let result = inlined_year_of_timestamp(millis);
            let expected = year_of_timestamp(millis);
            prop_assert_eq!(result, expected);
        }
    }

    #[test]
    fn timestamp_year() {
        utils::test_logger();

        let layout_cache = RowLayoutCache::new();
        let timestamp = layout_cache.add(
            RowLayoutBuilder::new()
                .with_column(ColumnType::Timestamp, false)
                .build(),
        );
        let i64 = layout_cache.add(
            RowLayoutBuilder::new()
                .with_column(ColumnType::I64, false)
                .build(),
        );

        let function = {
            let mut builder = FunctionBuilder::new(layout_cache.clone());
            let input = builder.add_input(timestamp);
            let output = builder.add_output(i64);

            let timestamp = builder.load(input, 0);
            let div = builder.add_expr(Call::new(
                "dbsp.timestamp.year".into(),
                vec![timestamp],
                vec![ArgType::Scalar(ColumnType::Timestamp)],
                ColumnType::I64,
            ));
            builder.store(output, 0, div);
            builder.ret_unit();

            builder.build()
        };

        let mut codegen = Codegen::new(layout_cache, CodegenConfig::debug());
        let function = codegen.codegen_func("timestamp_year", &function);

        let mut runner = TestRunner::new(Config {
            test_name: Some(concat!(module_path!(), "::timestamp_year")),
            source_file: Some(file!()),
            ..Config::default()
        });

        let (jit, _) = codegen.finalize_definitions();
        let result = {
            let timestamp_year_jit = unsafe {
                transmute::<*const u8, extern "C" fn(*const u8, *mut u8)>(
                    jit.get_finalized_function(function),
                )
            };

            runner.run(&any::<i64>(), |millis| {
                let mut result = 0;
                timestamp_year_jit(
                    &millis as *const i64 as *const u8,
                    &mut result as *mut i64 as *mut u8,
                );

                let expected = year_of_timestamp(millis);
                prop_assert_eq!(result, expected);

                Ok(())
            })
        };

        unsafe { jit.free_memory() }

        if let Err(err) = result {
            panic!("{}\n{}", err, runner);
        }
    }

    #[test]
    fn timestamp_year_corpus() {
        utils::test_logger();
        let corpus = &[-62167219200001];

        let layout_cache = RowLayoutCache::new();
        let timestamp = layout_cache.add(
            RowLayoutBuilder::new()
                .with_column(ColumnType::Timestamp, false)
                .build(),
        );
        let i64 = layout_cache.add(
            RowLayoutBuilder::new()
                .with_column(ColumnType::I64, false)
                .build(),
        );

        let function = {
            let mut builder = FunctionBuilder::new(layout_cache.clone());
            let input = builder.add_input(timestamp);
            let output = builder.add_output(i64);

            let timestamp = builder.load(input, 0);
            let div = builder.add_expr(Call::new(
                "dbsp.timestamp.year".into(),
                vec![timestamp],
                vec![ArgType::Scalar(ColumnType::Timestamp)],
                ColumnType::I64,
            ));
            builder.store(output, 0, div);
            builder.ret_unit();

            builder.build()
        };

        let mut codegen = Codegen::new(layout_cache, CodegenConfig::debug());
        let function = codegen.codegen_func("timestamp_year", &function);

        let (jit, _) = codegen.finalize_definitions();
        {
            let timestamp_year_jit = unsafe {
                transmute::<*const u8, extern "C" fn(*const u8, *mut u8)>(
                    jit.get_finalized_function(function),
                )
            };

            for &millis in corpus {
                let expected = year_of_timestamp(millis);
                let reference = inlined_year_of_timestamp(millis);
                assert_eq!(reference, expected);

                let mut result = 0;
                timestamp_year_jit(
                    &millis as *const i64 as *const u8,
                    &mut result as *mut i64 as *mut u8,
                );

                assert_eq!(result, expected);
            }
        }

        unsafe { jit.free_memory() }
    }
}
