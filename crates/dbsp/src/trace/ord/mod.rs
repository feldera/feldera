pub mod fallback;
pub mod file;
pub mod merge_batcher;
pub mod vec;

pub use fallback::{
    indexed_wset::{
        FallbackIndexedWSet, FallbackIndexedWSet as OrdIndexedWSet, FallbackIndexedWSetBuilder,
        FallbackIndexedWSetBuilder as OrdIndexedWSetBuilder, FallbackIndexedWSetFactories,
        FallbackIndexedWSetFactories as OrdIndexedWSetFactories, FallbackIndexedWSetMerger,
        FallbackIndexedWSetMerger as OrdIndexedWSetMerger,
    },
    key_batch::{FallbackKeyBatch, FallbackKeyBatchFactories},
    val_batch::{FallbackValBatch, FallbackValBatchFactories},
    wset::{
        FallbackWSet, FallbackWSet as OrdWSet, FallbackWSetBuilder,
        FallbackWSetBuilder as OrdWSetBuilder, FallbackWSetFactories,
        FallbackWSetFactories as OrdWSetFactories, FallbackWSetMerger,
        FallbackWSetMerger as OrdWSetMerger,
    },
};
pub use file::{
    FileIndexedWSet, FileIndexedWSetFactories, FileKeyBatch, FileKeyBatchFactories, FileValBatch,
    FileValBatchFactories, FileWSet, FileWSetFactories,
};
pub use vec::{
    VecIndexedWSet, VecIndexedWSetFactories, VecKeyBatch, VecKeyBatch as OrdKeyBatch,
    VecKeyBatchFactories, VecKeyBatchFactories as OrdKeyBatchFactories, VecValBatch,
    VecValBatch as OrdValBatch, VecValBatchFactories, VecValBatchFactories as OrdValBatchFactories,
    VecWSet, VecWSetFactories,
};

use super::Filter;

fn filter<T>(f: &Option<Filter<T>>, t: &T) -> bool
where
    T: ?Sized,
{
    f.as_ref().map_or(true, |f| f(t))
}
