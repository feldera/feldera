//! Trace and batch implementations based on sorted ranges.
//!
//! The types and type aliases in this module start with one of:
//!
//! * `OrdVal`: Collections whose data have the form `(key, val)` where `key`
//!   and `val` are ordered.
//! * `OrdKey`: Collections whose data have the form `key` where `key` is
//!   ordered.
//! * `OrdIndexedZSet`:  Collections whose data have the form `(key, val)` where
//!   `key` and `val` are ordered and whose timestamp type is `()`.
//!   Semantically, such collections store `(key, val, weight)` tuples without
//!   timing information, and implement the indexed ZSet abstraction of DBSP.
//! * `OrdZSet`:  Collections whose data have the form `key` where `key` is
//!   ordered and whose timestamp type is `()`.  Semantically, such collections
//!   store `(key, weight)` tuples without timing information, and implement the
//!   ZSet abstraction of DBSP.
//!
//! Although `OrdVal` is more general than `OrdKey`, the latter has a simpler
//! representation and should consume fewer resources (computation and memory)
//! when it applies.
//!
//! Likewise, `OrdIndexedZSet` and `OrdZSet` are less general than `OrdVal` and
//! `OrdKey` respectively, but are more light-weight.

pub mod indexed_zset_batch;
pub mod key_batch;
pub mod val_batch;
pub mod zset_batch;

mod merge_batcher;

pub use indexed_zset_batch::OrdIndexedZSet;
pub use key_batch::OrdKeyBatch;
pub use val_batch::OrdValBatch;
pub use zset_batch::OrdZSet;

use crate::trace::Spine;

/// A trace implementation using a spine of ordered lists.
pub type OrdValSpine<K, V, T, R, O = usize> = Spine<OrdValBatch<K, V, T, R, O>>;

/// A trace implementation for empty values using a spine of ordered lists.
pub type OrdKeySpine<K, T, R, O = usize> = Spine<OrdKeyBatch<K, T, R, O>>;

/// A trace implementation using a [`Spine`] of [`OrdZSet`].
pub type OrdZSetSpine<K, R> = Spine<OrdZSet<K, R>>;

pub type OrdIndexedZSetSpine<K, V, R, O = usize> = Spine<OrdIndexedZSet<K, V, R, O>>;
