#![cfg(test)]

use crate::{
    codegen::{
        json::{
            call_deserialize_fn, DeserializeJsonFn, JsonDeserConfig, JsonSerConfig, SerializeFn,
        },
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
                transmute::<_, SerializeFn>(jit.get_finalized_function(serialize_json)),
            )
        };

        let mut serialize_buffer = String::new();
        for (&json, expected) in json_snippets.iter().zip(expected) {
            let json_value = serde_json::from_str(json).unwrap();
            let mut uninit = UninitRow::new(unsafe { &*vtable });

            let row = unsafe {
                call_deserialize_fn(deserialize_json, uninit.as_mut_ptr(), &json_value).unwrap();
                uninit.assume_init()
            };

            let expected =
                unsafe { row_from_literal(expected, &*vtable, &layout_cache.layout_of(layout)) };
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

#[test]
#[should_panic = "an error occurred while parsing the key \"/foo\""]
fn deserialize_invalid_json() {
    utils::test_logger();

    let layout_cache = RowLayoutCache::new();
    let layout = layout_cache.add(
        RowLayoutBuilder::new()
            .with_column(ColumnType::String, false)
            .build(),
    );

    let mut codegen = Codegen::new(layout_cache, CodegenConfig::debug());

    let deserialize = JsonDeserConfig {
        layout,
        mappings: {
            let mut mappings = HashMap::default();
            mappings.insert(0, "/foo".to_owned());
            mappings
        },
    };

    let deserialize_json = codegen.deserialize_json(&deserialize);
    let vtable = codegen.vtable_for(layout);

    let (jit, _layout_cache) = codegen.finalize_definitions();
    let vtable = Box::into_raw(Box::new(vtable.marshalled(&jit)));

    {
        let deserialize_json = unsafe {
            transmute::<_, DeserializeJsonFn>(jit.get_finalized_function(deserialize_json))
        };

        let json_value = serde_json::from_str(r#"{ "foo": 10 }"#).unwrap();
        let mut uninit = UninitRow::new(unsafe { &*vtable });

        unsafe {
            match call_deserialize_fn(deserialize_json, uninit.as_mut_ptr(), &json_value) {
                // This shouldn't ever be ok
                Ok(()) => {}
                Err(error) => panic!("{error}"),
            }
        }
    }

    unsafe {
        drop(Box::from_raw(vtable));
        jit.free_memory();
    }
}
