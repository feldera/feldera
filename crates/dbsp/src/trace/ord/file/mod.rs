//! Trace and batch implementations based on files.

pub mod indexed_zset_batch;
pub mod key_batch;
pub mod val_batch;
pub mod zset_batch;

pub use indexed_zset_batch::{FileIndexedZSet, FileIndexedZSetFactories};
pub use key_batch::{FileKeyBatch, FileKeyBatchFactories};
pub use val_batch::{FileValBatch, FileValBatchFactories};
pub use zset_batch::{FileZSet, FileZSetFactories};

pub type StorageBackend =
    crate::storage::buffer_cache::BufferCache<crate::storage::backend::DefaultBackend>;
