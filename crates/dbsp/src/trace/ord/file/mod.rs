//! Trace and batch implementations based on files.

pub mod indexed_zset_batch;
pub mod key_batch;
pub mod val_batch;
pub mod zset_batch;

pub use indexed_zset_batch::FileIndexedZSet;
pub use key_batch::FileKeyBatch;
pub use val_batch::FileValBatch;
pub use zset_batch::FileZSet;

pub type StorageBackend = feldera_storage::backend::DefaultBackend;
