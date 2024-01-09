//! Trace and batch implementations based on sorted vectors.
//!
//! These can be used as the standard implementations for traces and batches in
//! [`crate::trace::ord`].

pub mod indexed_zset_batch;
pub mod key_batch;
pub mod val_batch;
pub mod zset_batch;

use crate::trace::Spine;

pub use indexed_zset_batch::VecIndexedZSet;
pub use key_batch::VecKeyBatch;
pub use val_batch::VecValBatch;
pub use zset_batch::VecZSet;

/// A trace implementation using a spine of ordered lists.
pub type VecValSpine<K, V, T, R> = Spine<VecValBatch<K, V, T, R>>;

/// A trace implementation for empty values using a spine of ordered lists.
pub type VecKeySpine<K, T, R> = Spine<VecKeyBatch<K, T, R>>;

/// A trace implementation using a [`Spine`] of [`VecZSet`].
pub type VecZSetSpine<K, R> = Spine<VecZSet<K, R>>;

/// A trace implementation using a [`Spine`] of [`VecIndexedZSet`].
pub type VecIndexedZSetSpine<K, V, R> = Spine<VecIndexedZSet<K, V, R>>;
