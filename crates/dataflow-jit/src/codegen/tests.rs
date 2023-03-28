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
use std::mem::transmute;

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
