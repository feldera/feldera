mod map;

#[cfg(test)]
mod tests {
    use crate::{
        codegen::{Codegen, CodegenConfig},
        ir::{LayoutCache, RowLayoutBuilder, RowType},
    };
    use dbsp::trace::layers::erased::{DataVTable, ErasedVTable};
    use std::{mem::transmute, num::NonZeroUsize};

    #[test]
    fn dynamic_erased_sets() {
        let layout_cache = LayoutCache::new();
        let layout_id = layout_cache.add(
            RowLayoutBuilder::new()
                .with_row(RowType::U32, false)
                .with_row(RowType::U32, false)
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

        let _nullable_lt = codegen.codegen_layout_lt(nullable_layout_id);

        let (jit_module, mut layout_cache) = codegen.finalize_definitions();

        let eq = unsafe {
            transmute::<*const u8, unsafe extern "C" fn(*const u8, *const u8) -> bool>(
                jit_module.get_finalized_function(eq),
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
        let lt = unsafe {
            transmute::<*const u8, unsafe extern "C" fn(*const u8, *const u8) -> bool>(
                jit_module.get_finalized_function(lt),
            )
        };

        let layout = layout_cache.compute(layout_id);

        unsafe {
            let lhs = layout.alloc().unwrap().as_ptr();
            lhs.add(layout.row_offset(0) as usize)
                .cast::<u32>()
                .write(0);
            lhs.add(layout.row_offset(1) as usize)
                .cast::<u32>()
                .write(1);

            let rhs = layout.alloc().unwrap().as_ptr();
            rhs.add(layout.row_offset(0) as usize)
                .cast::<u32>()
                .write(2);
            rhs.add(layout.row_offset(1) as usize)
                .cast::<u32>()
                .write(3);

            assert!(!eq(lhs, rhs));
            assert!(lt(lhs, rhs));
            assert!(!lt(rhs, lhs));

            rhs.add(layout.row_offset(0) as usize)
                .cast::<u32>()
                .write(0);
            rhs.add(layout.row_offset(1) as usize)
                .cast::<u32>()
                .write(1);

            assert!(eq(lhs, rhs));
            assert!(!lt(rhs, lhs));

            layout.dealloc(lhs);
            layout.dealloc(rhs);
        }

        let vtable = DataVTable {
            common: ErasedVTable {
                size_of,
                align_of,
                eq,
                lt,
                cmp: todo!(),
                clone,
                clone_into_slice,
                size_of_children: todo!(),
                debug: todo!(),
                drop_in_place: todo!(),
                drop_slice_in_place: todo!(),
                type_id: todo!(),
                type_name: todo!(),
            },
        };
    }
}
