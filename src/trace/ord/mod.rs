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

use crate::trace::spine_fueled::Spine;

mod merge_batcher;

pub mod val_batch;
pub use val_batch::OrdValBatch;

/// A trace implementation using a spine of ordered lists.
pub type OrdValSpine<K, V, T, R, O = usize> = Spine<OrdValBatch<K, V, T, R, O>>;

pub mod key_batch;
pub use key_batch::OrdKeyBatch;

/// A trace implementation for empty values using a spine of ordered lists.
pub type OrdKeySpine<K, T, R, O = usize> = Spine<OrdKeyBatch<K, T, R, O>>;

pub mod zset_batch;
pub use zset_batch::OrdZSet;

/// A trace implementation using a [`Spine`] of [`OrdZSet`].
pub type OrdZSetSpine<K, R> = Spine<OrdZSet<K, R>>;

pub mod indexed_zset_batch;
pub use indexed_zset_batch::OrdIndexedZSet;

pub type OrdIndexedZSetSpine<K, V, R, O = usize> = Spine<OrdIndexedZSet<K, V, R, O>>;
