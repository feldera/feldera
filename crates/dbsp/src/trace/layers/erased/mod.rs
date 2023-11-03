mod erased_leaf;
mod vtable;

pub use erased_leaf::{ErasedLeaf, TypedLayer};
pub use vtable::{DataVTable, DiffVTable, ErasedVTable, IntoErasedData, IntoErasedDiff};
