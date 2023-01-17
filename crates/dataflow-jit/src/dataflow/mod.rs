mod map;

#[cfg(test)]
mod tests {
    use crate::{
        codegen::{Codegen, CodegenConfig},
        ir::{LayoutCache, RowLayoutBuilder, RowType},
    };
    use dbsp::trace::layers::erased::{DataVTable, ErasedVTable};
    use std::{
        any::TypeId,
        cmp::Ordering,
        fmt::{self, Debug},
        mem::transmute,
        num::NonZeroUsize,
    };

    #[test]
    fn dynamic_erased_sets() {
        let layout_cache = LayoutCache::new();
        let layout_id = layout_cache.add(
            RowLayoutBuilder::new()
                .with_row(RowType::U32, false)
                .with_row(RowType::U32, false)
                .with_row(RowType::Unit, false)
                .build(),
        );

        let nullable_layout_id = layout_cache.add(
            RowLayoutBuilder::new()
                .with_row(RowType::U32, false)
                .with_row(RowType::U32, true)
                .with_row(RowType::Unit, false)
                .with_row(RowType::Unit, true)
                .build(),
        );
        let string_layout_id = layout_cache.add(
            RowLayoutBuilder::new()
                .with_row(RowType::U32, false)
                .with_row(RowType::U32, true)
                .with_row(RowType::Unit, false)
                .with_row(RowType::Unit, true)
                .with_row(RowType::String, false)
                .with_row(RowType::String, true)
                .build(),
        );

        let mut codegen = Codegen::new(layout_cache, CodegenConfig::debug());

        let (size_of, align_of) = {
            let layout = codegen.layout_for(layout_id);
            (
                layout.size() as usize,
                NonZeroUsize::new(layout.align() as usize).unwrap(),
            )
        };

        let eq = codegen.codegen_layout_eq(layout_id);
        let clone = codegen.codegen_layout_clone(layout_id);
        let clone_into_slice = codegen.codegen_layout_clone_into_slice(layout_id);
        let lt = codegen.codegen_layout_lt(layout_id);
        let cmp = codegen.codegen_layout_cmp(layout_id);
        let drop_in_place = codegen.codegen_layout_drop_in_place(layout_id);
        let drop_slice_in_place = codegen.codegen_layout_drop_slice_in_place(layout_id);
        let type_name = codegen.codegen_layout_type_name(layout_id);
        let size_of_children = codegen.codegen_layout_size_of_children(layout_id);
        let debug = codegen.codegen_layout_debug(layout_id);

        let _nullable_lt = codegen.codegen_layout_lt(nullable_layout_id);
        let _string_drop_in_place = codegen.codegen_layout_drop_in_place(string_layout_id);
        let _string_drop_slice_in_place =
            codegen.codegen_layout_drop_slice_in_place(string_layout_id);
        let _string_size_of_children = codegen.codegen_layout_size_of_children(string_layout_id);
        let _string_eq = codegen.codegen_layout_eq(string_layout_id);

        let (jit_module, mut layout_cache) = codegen.finalize_definitions();

        let eq = unsafe {
            transmute::<*const u8, unsafe extern "C" fn(*const u8, *const u8) -> bool>(
                jit_module.get_finalized_function(eq),
            )
        };
        let lt = unsafe {
            transmute::<*const u8, unsafe extern "C" fn(*const u8, *const u8) -> bool>(
                jit_module.get_finalized_function(lt),
            )
        };
        let cmp = unsafe {
            transmute::<*const u8, unsafe extern "C" fn(*const u8, *const u8) -> Ordering>(
                jit_module.get_finalized_function(cmp),
            )
        };
        let clone = unsafe {
            transmute::<*const u8, unsafe extern "C" fn(*const u8, *mut u8)>(
                jit_module.get_finalized_function(clone),
            )
        };
        let clone_into_slice = unsafe {
            transmute::<*const u8, unsafe extern "C" fn(*const u8, *mut u8, usize)>(
                jit_module.get_finalized_function(clone_into_slice),
            )
        };
        let drop_in_place = unsafe {
            transmute::<*const u8, unsafe extern "C" fn(*mut u8)>(
                jit_module.get_finalized_function(drop_in_place),
            )
        };
        let drop_slice_in_place = unsafe {
            transmute::<*const u8, unsafe extern "C" fn(*mut u8, usize)>(
                jit_module.get_finalized_function(drop_slice_in_place),
            )
        };
        let size_of_children = unsafe {
            transmute::<*const u8, unsafe extern "C" fn(*const u8, &mut size_of::Context)>(
                jit_module.get_finalized_function(size_of_children),
            )
        };
        let debug = unsafe {
            transmute::<*const u8, unsafe extern "C" fn(*const u8, *mut fmt::Formatter) -> bool>(
                jit_module.get_finalized_function(debug),
            )
        };
        let type_name = unsafe {
            transmute::<*const u8, unsafe extern "C" fn(*mut usize) -> *const u8>(
                jit_module.get_finalized_function(type_name),
            )
        };

        struct DebugRow(
            *const u8,
            unsafe extern "C" fn(*const u8, *mut fmt::Formatter) -> bool,
        );
        impl Debug for DebugRow {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                if unsafe { (self.1)(self.0, f) } {
                    Ok(())
                } else {
                    Err(fmt::Error)
                }
            }
        }

        let layout = layout_cache.compute(layout_id);

        unsafe {
            let lhs = layout.alloc().unwrap().as_ptr();
            lhs.add(layout.offset_of(0) as usize).cast::<u32>().write(0);
            lhs.add(layout.offset_of(1) as usize).cast::<u32>().write(1);

            let rhs = layout.alloc().unwrap().as_ptr();
            rhs.add(layout.offset_of(0) as usize).cast::<u32>().write(2);
            rhs.add(layout.offset_of(1) as usize).cast::<u32>().write(3);

            println!("lhs: {:?}", DebugRow(lhs, debug));
            println!("rhs: {:?}", DebugRow(rhs, debug));

            assert!(!eq(lhs, rhs));
            assert!(lt(lhs, rhs));
            assert_eq!(cmp(lhs, rhs), Ordering::Less);
            assert!(!lt(rhs, lhs));
            assert_eq!(cmp(rhs, lhs), Ordering::Greater);

            rhs.add(layout.offset_of(0) as usize).cast::<u32>().write(0);
            rhs.add(layout.offset_of(1) as usize).cast::<u32>().write(1);

            println!("lhs: {:?}", DebugRow(lhs, debug));
            println!("rhs: {:?}", DebugRow(rhs, debug));

            assert!(eq(lhs, rhs));
            assert!(!lt(rhs, lhs));
            assert_eq!(cmp(lhs, rhs), Ordering::Equal);
            assert_eq!(cmp(rhs, lhs), Ordering::Equal);

            rhs.add(layout.offset_of(0) as usize).cast::<u32>().write(0);
            rhs.add(layout.offset_of(1) as usize).cast::<u32>().write(0);

            println!("lhs: {:?}", DebugRow(lhs, debug));
            println!("rhs: {:?}", DebugRow(rhs, debug));

            assert!(!eq(lhs, rhs));
            assert!(!lt(lhs, rhs));
            assert!(lt(rhs, lhs));
            assert_eq!(cmp(lhs, rhs), Ordering::Greater);
            assert_eq!(cmp(rhs, lhs), Ordering::Less);

            layout.dealloc(lhs);
            layout.dealloc(rhs);
        }

        // This is just a dummy function since we can't meaningfully create type ids at
        // runtime (we could technically ignore the existence of other types and hope
        // they never cross paths with the unholy abominations created here so that we
        // could make our own TypeIds that tell which layout a row originated from,
        // allowing us to check that two rows are of the same layout)
        fn type_id() -> TypeId {
            struct DataflowJitRow;
            TypeId::of::<DataflowJitRow>()
        }

        let vtable = DataVTable {
            common: ErasedVTable {
                size_of,
                align_of,
                eq,
                lt,
                cmp,
                clone,
                clone_into_slice,
                size_of_children,
                debug,
                drop_in_place,
                drop_slice_in_place,
                type_id,
                type_name,
            },
        };

        println!("type name: {}", vtable.common.type_name());
    }
}
