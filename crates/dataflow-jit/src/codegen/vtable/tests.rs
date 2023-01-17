#![cfg(test)]

use crate::{
    codegen::{Codegen, CodegenConfig, Type},
    ir::{LayoutCache, RowLayoutBuilder, RowType},
    thin_str::ThinStrRef,
    ThinStr,
};
use dbsp::{trace::layers::erased::DataVTable, utils::DynVec};
use size_of::{Context, SizeOf, TotalSize};
use std::{
    cmp::Ordering,
    fmt::{self, Debug, Write},
};

// TODO: Test nullable fields
// TODO: Proptesting

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
        let mut codegen = Codegen::new(layout_cache, CodegenConfig::debug());
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

            let values = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"];

            let src = layout.alloc_array(values.len()).unwrap().as_ptr();
            let dest = layout.alloc_array(values.len()).unwrap().as_ptr();

            // Initialize src
            for (idx, &val) in values.iter().enumerate() {
                src.add(idx * layout.size() as usize)
                    .cast::<ThinStr>()
                    .write(ThinStr::from(val));
            }
            for (idx, &val) in values.iter().enumerate() {
                let string = *src.add(idx * layout.size() as usize).cast::<ThinStrRef>();
                assert_eq!(string.as_str(), val);
            }

            // Clone src into dest
            (vtable.clone_into_slice)(src, dest, values.len());

            // Ensure dest contains the correct values
            for (idx, &val) in values.iter().enumerate() {
                let string = *dest.add(idx * layout.size() as usize).cast::<ThinStrRef>();
                assert_eq!(string.as_str(), val);
            }

            // Drop all values within src and dest
            (vtable.drop_slice_in_place)(src, values.len());
            (vtable.drop_slice_in_place)(dest, values.len());

            // Deallocate the arrays
            layout.dealloc_array(src, values.len());
            layout.dealloc_array(dest, values.len());

            module.free_memory();
        }
    }
}

#[test]
fn dyn_vec() {
    let types = [
        RowType::Bool,
        RowType::U16,
        RowType::U32,
        RowType::U64,
        RowType::I16,
        RowType::I32,
        RowType::I64,
        RowType::F32,
        RowType::F64,
        RowType::Unit,
        RowType::String,
    ];

    // TODO: Proptest data generation
    let data = &[
        (
            (true, Some(false)),
            (53u16, Some(1255u16)),
            (u32::MAX, Some(u32::MAX / 2)),
            (u64::MAX / 5, Some(u64::MAX / 4)),
            (i16::MAX, Some(i16::MIN)),
            (i32::MAX, Some(i32::MIN)),
            (i64::MAX, Some(i64::MIN)),
            (std::f32::consts::PI, Some(std::f32::consts::FRAC_1_SQRT_2)),
            (std::f64::consts::LN_2, Some(std::f64::consts::TAU)),
            ((), Some(())),
            (
                ThinStr::from("foobarbazbop"),
                Some(ThinStr::from("lorem ipsum")),
            ),
        ),
        (
            (false, None),
            (95u16, None),
            (u32::MAX / 2, None),
            (u64::MAX / 10, None),
            (i16::MAX, None),
            (i32::MAX, None),
            (i64::MAX, None),
            (std::f32::consts::LOG2_10, None),
            (std::f64::consts::TAU, None),
            ((), None),
            (ThinStr::from("foobarbaz"), None),
        ),
    ];

    let layout_cache = LayoutCache::new();

    let mut builder = RowLayoutBuilder::new();
    for ty in types {
        builder.add_row(ty, false).add_row(ty, true);
    }
    let layout_id = layout_cache.add(builder.build());

    {
        let mut codegen = Codegen::new(layout_cache, CodegenConfig::debug());
        let vtable = codegen.vtable_for(layout_id);
        let (jit, mut layout_cache) = codegen.finalize_definitions();

        let layout = layout_cache.compute(layout_id);
        let vtable = Box::into_raw(Box::new(DataVTable {
            common: vtable.marshall(&jit),
        }));

        {
            // Create a dynamic vector
            let mut vec = DynVec::new(unsafe { &*vtable });

            // Initialize the dynvec
            {
                // We don't statically know the layout of the value we're creating, so we
                // allocate a place for it on the heap, write to it and then push to our DynVec
                // from that place
                let place = layout.alloc().unwrap().as_ptr();

                macro_rules! emplace {
                    ($($value:ident: $ty:ty),+ $(,)?) => {
                        let mut offset = 0;

                        $(
                            place
                                .add(layout.offset_of(offset) as usize)
                                .cast::<$ty>()
                                .write($value.0.clone());
                            offset += 1;

                            let (ty, bit_offset, bit) = layout.nullability_of(offset);
                            let bitset = place.add(bit_offset as usize);
                            let mut mask = match ty {
                                Type::U8 | Type::I8 => bitset.read() as u64,
                                Type::U16 | Type::I16 => bitset.cast::<u16>().read() as u64,
                                Type::U32 | Type::I32 => bitset.cast::<u32>().read() as u64,
                                Type::U64 | Type::I64 => bitset.cast::<u64>().read() as u64,
                                Type::Usize => bitset.cast::<usize>().read() as u64,
                                Type::F32 | Type::F64 | Type::Ptr | Type::Bool => unreachable!(),
                            };
                            if $value.1.is_some() {
                                mask &= !(1 << bit);
                            } else {
                                mask |= 1 << bit;
                            }
                            match ty {
                                Type::U8 | Type::I8 => bitset.write(mask as u8),
                                Type::U16 | Type::I16 => bitset.cast::<u16>().write(mask as u16),
                                Type::U32 | Type::I32 => bitset.cast::<u32>().write(mask as u32),
                                Type::U64 | Type::I64 => bitset.cast::<u64>().write(mask as u64),
                                Type::Usize => bitset.cast::<usize>().write(mask as usize),
                                Type::F32 | Type::F64 | Type::Ptr | Type::Bool => unreachable!(),
                            }

                            if let Some(val) = $value.1.clone() {
                                place
                                    .add(layout.offset_of(offset) as usize)
                                    .cast::<$ty>()
                                    .write(val);
                            }

                            #[allow(unused_assignments)]
                            { offset += 1 };
                        )+
                    };
                }

                for (bools, u16s, u32s, u64s, i16s, i32s, i64s, f32s, f64s, units, strs) in data {
                    unsafe {
                        // Fill `place` with our data
                        emplace! {
                            bools: bool,
                            u16s: u16,
                            u32s: u32,
                            u64s: u64,
                            i16s: i16,
                            i32s: i32,
                            i64s: i64,
                            f32s: f32,
                            f64s: f64,
                            units: (),
                            strs: ThinStr,
                        }

                        // Push the value we created to the vec
                        vec.push_raw(place)
                    }
                }

                // Deallocate our place
                unsafe { layout.dealloc(place) }
            }

            let clone = vec.clone();
            assert_eq!(vec, clone);
            drop(clone);

            println!("{vec:#?}");
        }

        unsafe {
            drop(Box::from_raw(vtable));
            jit.free_memory();
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
