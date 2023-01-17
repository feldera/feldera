#![cfg(test)]

use size_of::{Context, SizeOf, TotalSize};

use crate::{
    codegen::{Codegen, CodegenConfig},
    ir::{LayoutCache, RowLayoutBuilder, RowType},
    ThinStr,
};
use std::{
    cmp::Ordering,
    fmt::{self, Debug, Write},
    mem::size_of,
};

#[test]
fn empty() {
    let layout_cache = LayoutCache::new();
    let empty_layout = layout_cache.add(RowLayoutBuilder::new().build());

    {
        let mut codegen = Codegen::new(layout_cache.clone(), CodegenConfig::debug());
        let vtable = codegen.vtable_for(empty_layout);

        let (module, mut layouts) = codegen.finalize_definitions();
        let vtable = vtable.marshall(&module);

        let layout = layouts.compute(empty_layout);
        assert_eq!(layout.size(), 0);
        assert!(layout.is_zero_sized());

        let (lhs, rhs) = (
            layout.alloc().unwrap().as_ptr(),
            layout.alloc().unwrap().as_ptr(),
        );

        // clone
        // clone_into_slice
        // size_of_children
        // debug
        // drop_in_place
        // drop_slice_in_place
        // type_name

        unsafe {
            assert!((vtable.eq)(lhs, rhs));
            assert!(!(vtable.lt)(lhs, rhs));
            assert_eq!((vtable.cmp)(lhs, rhs), Ordering::Equal);

            let clone = layout.alloc().unwrap().as_ptr();
            (vtable.clone)(lhs, clone);
            assert!((vtable.eq)(lhs, clone));

            let debug = DebugRow(lhs, vtable.debug).debug();
            assert_eq!(debug, "{}");

            let type_name = vtable.type_name();
            assert_eq!(type_name, "DataflowJitRow({})");

            let mut ctx = Context::new();
            (vtable.size_of_children)(lhs, &mut ctx);
            assert_eq!(ctx.total_size(), TotalSize::zero());

            (vtable.drop_slice_in_place)(lhs, 1);
            (vtable.drop_in_place)(rhs);
            (vtable.drop_in_place)(clone);
            layout.dealloc(lhs);
            layout.dealloc(rhs);
            layout.dealloc(clone);

            module.free_memory();
        }
    }
}

#[test]
fn string() {
    let layout_cache = LayoutCache::new();
    let string_layout = layout_cache.add(
        RowLayoutBuilder::new()
            .with_row(RowType::String, false)
            .build(),
    );

    {
        let mut codegen = Codegen::new(layout_cache.clone(), CodegenConfig::debug());
        let vtable = codegen.vtable_for(string_layout);

        let (module, mut layouts) = codegen.finalize_definitions();
        let vtable = vtable.marshall(&module);

        let layout = layouts.compute(string_layout);
        let (lhs, rhs) = (
            layout.alloc().unwrap().as_ptr(),
            layout.alloc().unwrap().as_ptr(),
        );

        unsafe {
            let offset = layout.offset_of(0) as usize;
            lhs.add(offset)
                .cast::<ThinStr>()
                .write(ThinStr::from("foobar"));
            rhs.add(offset)
                .cast::<ThinStr>()
                .write(ThinStr::from("foobar"));

            assert!((vtable.eq)(lhs, rhs));
            assert!(!(vtable.lt)(lhs, rhs));
            assert_eq!((vtable.cmp)(lhs, rhs), Ordering::Equal);

            let clone = layout.alloc().unwrap().as_ptr();
            (vtable.clone)(lhs, clone);
            assert!((vtable.eq)(lhs, clone));

            for ptr in [lhs, rhs, clone] {
                let debug = DebugRow(ptr, vtable.debug).debug();
                assert_eq!(debug, r#"{ "foobar" }"#);
            }

            assert_eq!(vtable.type_name(), "DataflowJitRow({str})");

            // Ensure a slice drop of length zero doesn't do anything
            (vtable.drop_slice_in_place)(lhs, 0);

            let mut ctx = Context::new();
            (vtable.size_of_children)(lhs, &mut ctx);
            let expected = {
                let mut ctx = Context::new();
                ThinStr::from("foobar").size_of_children(&mut ctx);
                ctx.total_size()
            };
            assert_eq!(ctx.total_size(), expected);

            (vtable.drop_slice_in_place)(lhs, 1);
            (vtable.drop_in_place)(rhs);
            (vtable.drop_in_place)(clone);

            layout.dealloc(lhs);
            layout.dealloc(rhs);
            layout.dealloc(clone);

            module.free_memory();
        }
    }
}

struct DebugRow(
    *const u8,
    unsafe extern "C" fn(*const u8, *mut fmt::Formatter) -> bool,
);

impl DebugRow {
    pub fn debug(self) -> String {
        format!("{self:?}")
    }
}

impl Debug for DebugRow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if unsafe { (self.1)(self.0, f) } {
            Ok(())
        } else {
            Err(fmt::Error)
        }
    }
}

struct FailWriter;

impl Write for FailWriter {
    fn write_str(&mut self, _: &str) -> fmt::Result {
        Err(fmt::Error)
    }
}
