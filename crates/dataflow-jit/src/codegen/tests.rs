#![cfg(test)]

use crate::{
    codegen::{Codegen, CodegenConfig},
    ir::{
        exprs::{ArgType, Call},
        ColumnType, Constant, FunctionBuilder, RowLayoutBuilder, RowLayoutCache,
    },
    row::UninitRow,
    thin_str::ThinStrRef,
    utils, ThinStr,
};
use chrono::{Datelike, Utc};
use std::mem::transmute;

#[test]
fn block_param_phi() {
    utils::test_logger();

    let layout_cache = RowLayoutCache::new();
    let timestamp = layout_cache.add(
        RowLayoutBuilder::new()
            .with_column(ColumnType::Timestamp, true)
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

        let return_block = builder.create_block();
        builder.add_block_param(return_block, ColumnType::I64);
        let timestamp_non_null = builder.create_block();

        let timestamp_is_null = builder.is_null(input, 0);
        let i64_min = builder.constant(Constant::I64(i64::MIN));
        builder.branch(
            timestamp_is_null,
            return_block,
            [i64_min],
            timestamp_non_null,
            [],
        );

        builder.move_to(timestamp_non_null);
        let timestamp = builder.load(input, 0);
        let year = builder.add_expr(Call::new(
            "dbsp.timestamp.year".into(),
            vec![timestamp],
            vec![ArgType::Scalar(ColumnType::Timestamp)],
            ColumnType::I64,
        ));
        builder.jump(return_block, [year]);

        builder.move_to(return_block);
        let year = builder.block_params(return_block)[0].0;
        builder.store(output, 0, year);
        builder.ret_unit();

        builder.build()
    };

    let mut codegen = Codegen::new(layout_cache, CodegenConfig::debug());
    let function = codegen.codegen_func("timestamp_year", &function);
    let timestamp_vtable = codegen.vtable_for(timestamp);
    let i64_vtable = codegen.vtable_for(i64);

    let (jit, layout_cache) = codegen.finalize_definitions();
    {
        let timestamp_layout = layout_cache.layout_of(timestamp);
        let i64_layout = layout_cache.layout_of(i64);

        let timestamp_vtable = Box::into_raw(Box::new(timestamp_vtable.marshalled(&jit)));
        let i64_vtable = Box::into_raw(Box::new(i64_vtable.marshalled(&jit)));

        let timestamp_year = unsafe {
            transmute::<*const u8, extern "C" fn(*const u8, *mut u8)>(
                jit.get_finalized_function(function),
            )
        };

        let current_timestamp = Utc::now();

        // Call with non-null input
        let mut input = UninitRow::new(unsafe { &*timestamp_vtable });
        unsafe {
            input
                .as_mut_ptr()
                .add(timestamp_layout.offset_of(0) as usize)
                .cast::<i64>()
                .write(current_timestamp.timestamp_millis());
            input.set_column_null(0, &timestamp_layout, false);
        }
        let mut input = unsafe { input.assume_init() };

        let mut output = UninitRow::new(unsafe { &*i64_vtable });
        timestamp_year(input.as_ptr(), output.as_mut_ptr());

        let mut output = unsafe { output.assume_init() };
        let year = unsafe {
            output
                .as_ptr()
                .add(i64_layout.offset_of(0) as usize)
                .cast::<i64>()
                .read()
        };
        assert_eq!(year, current_timestamp.year() as i64);

        // Call with null input
        input.set_column_null(0, &timestamp_layout, true);
        timestamp_year(input.as_ptr(), output.as_mut_ptr());

        let year = unsafe {
            output
                .as_ptr()
                .add(i64_layout.offset_of(0) as usize)
                .cast::<i64>()
                .read()
        };
        assert_eq!(year, i64::MIN);

        drop(input);
        drop(output);

        unsafe {
            drop(Box::from_raw(timestamp_vtable));
            drop(Box::from_raw(i64_vtable));
        }
    }

    unsafe { jit.free_memory() };
}

#[test]
fn string_length() {
    utils::test_logger();

    let layout_cache = RowLayoutCache::new();
    let string = layout_cache.add(
        RowLayoutBuilder::new()
            .with_column(ColumnType::String, false)
            .build(),
    );
    let u64 = layout_cache.add(
        RowLayoutBuilder::new()
            .with_column(ColumnType::U64, false)
            .build(),
    );

    let function = {
        let mut builder = FunctionBuilder::new(layout_cache.clone());
        let string_input = builder.add_input(string);
        let length_output = builder.add_output(u64);

        let string = builder.load(string_input, 0);
        let length = builder.string_len(string);
        builder.store(length_output, 0, length);
        builder.ret_unit();

        builder.build()
    };

    let mut codegen = Codegen::new(layout_cache, CodegenConfig::debug());
    let function = codegen.codegen_func("string_length", &function);
    let string_vtable = codegen.vtable_for(string);
    let u64_vtable = codegen.vtable_for(u64);

    let (jit, layout_cache) = codegen.finalize_definitions();
    {
        let string_vtable = Box::into_raw(Box::new(string_vtable.marshalled(&jit)));
        let u64_vtable = Box::into_raw(Box::new(u64_vtable.marshalled(&jit)));

        let string_length = unsafe {
            transmute::<*const u8, extern "C" fn(*const u8, *mut u8)>(
                jit.get_finalized_function(function),
            )
        };

        let mut input = UninitRow::new(unsafe { &*string_vtable });
        unsafe {
            input
                .as_mut_ptr()
                .add(layout_cache.layout_of(string).offset_of(0) as usize)
                .cast::<ThinStr>()
                .write(ThinStr::from("foobarbaz"));
        }
        let input = unsafe { input.assume_init() };

        let mut length = UninitRow::new(unsafe { &*u64_vtable });
        string_length(input.as_ptr(), length.as_mut_ptr());
        drop(input);

        let length_row = unsafe { length.assume_init() };
        let length = unsafe {
            length_row
                .as_ptr()
                .add(layout_cache.layout_of(u64).offset_of(0) as usize)
                .cast::<u64>()
                .read()
        };
        drop(length_row);

        unsafe {
            drop(Box::from_raw(string_vtable));
            drop(Box::from_raw(u64_vtable));
        }

        assert_eq!(length, 9);
    }
    unsafe { jit.free_memory() };
}

#[test]
fn concat_clone_string() {
    utils::test_logger();

    let layout_cache = RowLayoutCache::new();
    let strings = layout_cache.add(
        RowLayoutBuilder::new()
            .with_column(ColumnType::String, false)
            .with_column(ColumnType::String, false)
            .build(),
    );
    let string = layout_cache.add(
        RowLayoutBuilder::new()
            .with_column(ColumnType::String, false)
            .build(),
    );

    let function = {
        let mut builder = FunctionBuilder::new(layout_cache.clone());
        let string_input = builder.add_input(strings);
        let string_output = builder.add_output(string);

        let first = builder.load(string_input, 0);
        let second = builder.load(string_input, 1);
        let concatenated = builder.add_expr(Call::new(
            "dbsp.str.concat_clone".into(),
            vec![first, second],
            vec![ArgType::Scalar(ColumnType::String); 2],
            ColumnType::String,
        ));
        builder.store(string_output, 0, concatenated);
        builder.ret_unit();

        builder.build()
    };

    let mut codegen = Codegen::new(layout_cache, CodegenConfig::debug());
    let function = codegen.codegen_func("concat_clone_string", &function);
    let strings_vtable = codegen.vtable_for(strings);
    let string_vtable = codegen.vtable_for(string);

    let (jit, layout_cache) = codegen.finalize_definitions();
    {
        let strings_vtable = Box::into_raw(Box::new(strings_vtable.marshalled(&jit)));
        let string_vtable = Box::into_raw(Box::new(string_vtable.marshalled(&jit)));

        let string_length = unsafe {
            transmute::<*const u8, extern "C" fn(*const u8, *mut u8)>(
                jit.get_finalized_function(function),
            )
        };

        let mut input = UninitRow::new(unsafe { &*strings_vtable });
        unsafe {
            input
                .as_mut_ptr()
                .add(layout_cache.layout_of(strings).offset_of(0) as usize)
                .cast::<ThinStr>()
                .write(ThinStr::from("foobar"));
            input
                .as_mut_ptr()
                .add(layout_cache.layout_of(strings).offset_of(1) as usize)
                .cast::<ThinStr>()
                .write(ThinStr::from("bazbing"));
        }
        let input = unsafe { input.assume_init() };

        let mut concatenated = UninitRow::new(unsafe { &*string_vtable });
        string_length(input.as_ptr(), concatenated.as_mut_ptr());
        drop(input);

        let concatenated_row = unsafe { concatenated.assume_init() };
        {
            let concatenated = unsafe {
                concatenated_row
                    .as_ptr()
                    .add(layout_cache.layout_of(string).offset_of(0) as usize)
                    .cast::<ThinStrRef>()
                    .read()
            };
            assert_eq!(&*concatenated, "foobarbazbing");
        }
        drop(concatenated_row);

        unsafe {
            drop(Box::from_raw(strings_vtable));
            drop(Box::from_raw(string_vtable));
        }
    }
    unsafe { jit.free_memory() };
}

#[test]
fn clear_string() {
    utils::test_logger();

    let layout_cache = RowLayoutCache::new();
    let string = layout_cache.add(
        RowLayoutBuilder::new()
            .with_column(ColumnType::String, false)
            .build(),
    );

    let function = {
        let mut builder = FunctionBuilder::new(layout_cache.clone());
        let string_input = builder.add_input_output(string);

        let string = builder.load(string_input, 0);
        builder.add_expr(Call::new(
            "dbsp.str.clear".into(),
            vec![string],
            vec![ArgType::Scalar(ColumnType::String)],
            ColumnType::Unit,
        ));
        builder.ret_unit();

        builder.build()
    };

    let mut codegen = Codegen::new(layout_cache, CodegenConfig::release());
    let function = codegen.codegen_func("string_clear", &function);
    let string_vtable = codegen.vtable_for(string);

    let (jit, layout_cache) = codegen.finalize_definitions();
    {
        let string_vtable = Box::into_raw(Box::new(string_vtable.marshalled(&jit)));

        let string_length = unsafe {
            transmute::<*const u8, extern "C" fn(*mut u8)>(jit.get_finalized_function(function))
        };

        let value = ThinStr::from("foobarbaz");
        let capacity = value.capacity();

        let mut input = UninitRow::new(unsafe { &*string_vtable });
        unsafe {
            input
                .as_mut_ptr()
                .add(layout_cache.layout_of(string).offset_of(0) as usize)
                .cast::<ThinStr>()
                .write(value);
        }
        let mut input = unsafe { input.assume_init() };

        string_length(input.as_mut_ptr());

        {
            let cleared = unsafe {
                input
                    .as_ptr()
                    .add(layout_cache.layout_of(string).offset_of(0) as usize)
                    .cast::<ThinStrRef>()
                    .read()
            };
            assert_eq!(cleared.len(), 0);
            assert_eq!(cleared.capacity(), capacity);
            assert_eq!(&*cleared, "");
        }
        drop(input);

        unsafe { drop(Box::from_raw(string_vtable)) }
    }
    unsafe { jit.free_memory() };
}

#[test]
fn unwrap_optional_bool() {
    utils::test_logger();

    let layout_cache = RowLayoutCache::new();
    let optional_bool = layout_cache.add(
        RowLayoutBuilder::new()
            .with_column(ColumnType::Bool, true)
            .build(),
    );
    let boolean = layout_cache.add(
        RowLayoutBuilder::new()
            .with_column(ColumnType::Bool, false)
            .build(),
    );

    let function = {
        let mut builder = FunctionBuilder::new(layout_cache.clone());
        let input = builder.add_input(optional_bool);
        let output = builder.add_output(boolean);

        let is_null = builder.is_null(input, 0);
        let bool = builder.load(input, 0);
        let false_val = builder.constant(Constant::Bool(false));
        let unwrapped = builder.select(is_null, false_val, bool);
        builder.store(output, 0, unwrapped);
        builder.ret_unit();

        builder.build()
    };

    let mut codegen = Codegen::new(layout_cache, CodegenConfig::debug());
    let function = codegen.codegen_func("unwrap_bool", &function);
    let optional_bool_vtable = codegen.vtable_for(optional_bool);
    let boolean_vtable = codegen.vtable_for(boolean);

    let (jit, layout_cache) = codegen.finalize_definitions();
    {
        let optional_bool_vtable = Box::into_raw(Box::new(optional_bool_vtable.marshalled(&jit)));
        let boolean_vtable = Box::into_raw(Box::new(boolean_vtable.marshalled(&jit)));

        let optional_bool_layout = layout_cache.layout_of(optional_bool);
        let boolean_layout = layout_cache.layout_of(boolean);

        let unwrap_bool = unsafe {
            transmute::<*const u8, extern "C" fn(*const u8, *mut u8)>(
                jit.get_finalized_function(function),
            )
        };

        let mut input = UninitRow::new(unsafe { &*optional_bool_vtable });
        unsafe {
            input.set_column_null(0, &optional_bool_layout, true);
            input
                .as_mut_ptr()
                .add(optional_bool_layout.offset_of(0) as usize)
                .cast::<u8>()
                .write(u8::MAX);
        }
        let mut input = unsafe { input.assume_init() };

        let mut output = UninitRow::new(unsafe { &*boolean_vtable });
        unwrap_bool(input.as_ptr(), output.as_mut_ptr());

        let mut output = unsafe { output.assume_init() };
        let result = unsafe {
            output
                .as_ptr()
                .add(boolean_layout.offset_of(0) as usize)
                .cast::<u8>()
                .read()
        };
        assert_eq!(result, false as u8);

        unsafe {
            input.set_column_null(0, &optional_bool_layout, false);
            input
                .as_mut_ptr()
                .add(optional_bool_layout.offset_of(0) as usize)
                .cast::<bool>()
                .write(true);
        }
        unwrap_bool(input.as_ptr(), output.as_mut_ptr());
        let result = unsafe {
            output
                .as_ptr()
                .add(boolean_layout.offset_of(0) as usize)
                .cast::<u8>()
                .read()
        };
        assert_eq!(result, true as u8);

        unsafe {
            input.set_column_null(0, &optional_bool_layout, false);
            input
                .as_mut_ptr()
                .add(optional_bool_layout.offset_of(0) as usize)
                .cast::<bool>()
                .write(false);
        }
        unwrap_bool(input.as_ptr(), output.as_mut_ptr());
        let result = unsafe {
            output
                .as_ptr()
                .add(boolean_layout.offset_of(0) as usize)
                .cast::<u8>()
                .read()
        };
        assert_eq!(result, false as u8);

        drop((input, output));
        unsafe {
            drop(Box::from_raw(optional_bool_vtable));
            drop(Box::from_raw(boolean_vtable));
        }
    }
    unsafe { jit.free_memory() };
}

// TODO: Min/max with and without normalization
// TODO: More binops
// TODO: Test different codegen options
mod proptests {
    use crate::{
        codegen::{Codegen, CodegenConfig},
        ir::{BinaryOpKind, ColumnType, FunctionBuilder, RowLayoutBuilder, RowLayoutCache},
        utils,
    };
    use num_integer::{div_floor, mod_floor};
    use proptest::{
        prelude::any,
        prop_assert_eq, prop_assume,
        test_runner::{Config, TestRunner},
    };
    use std::mem::transmute;

    macro_rules! tests {
        ($test:tt, $op:ident, $ty:ident, $col:ident, $guard:expr, $expected:expr $(,)?) => {
            paste::paste! {
                #[test]
                fn [<$test _ $ty>]() {
                    utils::test_logger();

                    let layout_cache = RowLayoutCache::new();
                    let int = layout_cache.add(
                        RowLayoutBuilder::new()
                            .with_column(ColumnType::$col, false)
                            .build(),
                    );
                    let intx2 = layout_cache.add(
                        RowLayoutBuilder::new()
                            .with_column(ColumnType::$col, false)
                            .with_column(ColumnType::$col, false)
                            .build(),
                    );

                    let func = {
                        let mut builder = FunctionBuilder::new(layout_cache.clone());
                        let input = builder.add_input(intx2);
                        let output = builder.add_output(int);

                        let lhs = builder.load(input, 0);
                        let rhs = builder.load(input, 1);
                        let div = builder.binary_op(lhs, rhs, BinaryOpKind::$op);
                        builder.store(output, 0, div);
                        builder.ret_unit();

                        builder.build()
                    };

                    let test_name = concat!(module_path!(), "::", stringify!($test), "_", stringify!($ty));

                    let mut codegen = Codegen::new(layout_cache, CodegenConfig::debug());
                    let func = codegen.codegen_func(test_name, &func);

                    let (jit, _) = codegen.finalize_definitions();

                    let mut runner = TestRunner::new(Config {
                        test_name: Some(test_name),
                        source_file: Some(file!()),
                        ..Config::default()
                    });

                    let result = {
                        let jit_fn = unsafe {
                            transmute::<*const u8, extern "C" fn(*const u8, *mut u8)>(
                                jit.get_finalized_function(func),
                            )
                        };

                        runner.run(&(any::<$ty>(), any::<$ty>()), |(lhs, rhs)| {
                            ($guard)(lhs, rhs)?;

                            let expected: fn($ty, $ty) -> $ty = $expected;
                            let expected = expected(lhs, rhs);

                            let input: [$ty; 2] = [lhs, rhs];
                            let mut result: $ty = 0 as $ty;
                            jit_fn(
                                &input as *const [$ty; 2] as *const u8,
                                &mut result as *mut $ty as *mut u8,
                            );

                            prop_assert_eq!(result, expected);

                            Ok(())
                        })
                    };

                    unsafe { jit.free_memory() }

                    if let Err(error) = result {
                        panic!("{error}\n{runner}");
                    }
                }
            }
        };
    }

    macro_rules! proptest_int_binops {
        ($($ty:ident = $col:ident),+ $(,)?) => {
            $(
                tests!(
                    div, Div, $ty, $col,
                    |lhs, rhs| {
                        prop_assume!(rhs != 0 as $ty);
                        prop_assume!(lhs != <$ty>::MIN && rhs != -1 as $ty);
                        Ok(())
                    },
                    |lhs, rhs| lhs / rhs,
                );
                tests!(
                    div_floor, DivFloor, $ty, $col,
                    |lhs, rhs| {
                        prop_assume!(rhs != 0 as $ty);
                        prop_assume!(lhs != <$ty>::MIN && rhs != -1 as $ty);
                        Ok(())
                    },
                    div_floor,
                );

                tests!(mod, Mod, $ty, $col,
                    |lhs, rhs| {
                        prop_assume!(rhs != 0 as $ty);
                        prop_assume!(lhs != <$ty>::MIN && rhs != -1 as $ty);
                        Ok(())
                    },
                    |lhs, rhs| lhs.rem_euclid(rhs),
                );
                tests!(
                    mod_floor, ModFloor, $ty, $col,
                    |lhs, rhs| {
                        prop_assume!(rhs != 0 as $ty);
                        prop_assume!(lhs != <$ty>::MIN && rhs != -1 as $ty);
                        Ok(())
                    },
                    mod_floor,
                );

                tests!(
                    rem, Rem, $ty, $col,
                    |_, rhs| {
                        prop_assume!(rhs != 0 as $ty);
                        Ok(())
                    },
                    |lhs, rhs| lhs.wrapping_rem(rhs),
                );
            )+
        }
    }

    // Binops that just apply to integers
    proptest_int_binops! {
        i8 = I8,
        i16 = I16,
        i32 = I32,
        i64 = I64,
    }

    macro_rules! proptest_uint_binops {
        ($($ty:ident = $col:ident),+ $(,)?) => {
            $(
                tests!(
                    div, Div, $ty, $col,
                    |_, rhs| {
                        prop_assume!(rhs != 0 as $ty);
                        Ok(())
                    },
                    |lhs, rhs| lhs / rhs,
                );
                tests!(
                    div_floor, DivFloor, $ty, $col,
                    |_, rhs| {
                        prop_assume!(rhs != 0 as $ty);
                        Ok(())
                    },
                    div_floor,
                );

                tests!(mod, Mod, $ty, $col,
                    |_, rhs| {
                        prop_assume!(rhs != 0 as $ty);
                        Ok(())
                    },
                    |lhs, rhs| lhs.rem_euclid(rhs),
                );
                tests!(
                    mod_floor, ModFloor, $ty, $col,
                    |_, rhs| {
                        prop_assume!(rhs != 0 as $ty);
                        Ok(())
                    },
                    mod_floor,
                );

                tests!(
                    rem, Rem, $ty, $col,
                    |_, rhs| {
                        prop_assume!(rhs != 0 as $ty);
                        Ok(())
                    },
                    |lhs, rhs| lhs.wrapping_rem(rhs),
                );
            )+
        }
    }

    proptest_uint_binops! {
        u8 = U8,
        u16 = U16,
        u32 = U32,
        u64 = U64,
    }

    macro_rules! proptest_float_binops {
        ($($ty:ident = $col:ident),+ $(,)?) => {
            $(
                tests!(
                    div, Div, $ty, $col,
                    |lhs, rhs| {
                        prop_assume!(rhs != 0 as $ty);
                        prop_assume!(lhs != <$ty>::MIN && rhs != -1 as $ty);
                        Ok(())
                    },
                    |lhs, rhs| lhs / rhs,
                );

                tests!(mod, Mod, $ty, $col,
                    |_, rhs| {
                        prop_assume!(rhs != 0 as $ty);
                        Ok(())
                    },
                    |lhs, rhs| lhs.rem_euclid(rhs),
                );

                tests!(
                    rem, Rem, $ty, $col,
                    |_, rhs| {
                        prop_assume!(rhs != 0 as $ty);
                        Ok(())
                    },
                    |lhs, rhs| lhs % rhs,
                );
            )+
        }
    }

    proptest_float_binops! {
        f32 = F32,
        f64 = F64,
    }
}
