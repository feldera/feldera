use dbsp::trace::layers::erased::ErasedVTable;

pub struct Map {
    map: unsafe extern "C" fn(*const u8, *mut u8),
    output_vtable: ErasedVTable,
}
