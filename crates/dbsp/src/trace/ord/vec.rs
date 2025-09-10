//! Trace and batch implementations based on sorted ranges.
//!
//! The types and type aliases in this module start with one of:
//!
//! * `VecVal`: Collections whose data have the form `(key, val)` where `key`
//!   and `val` are ordered.
//! * `VecKey`: Collections whose data have the form `key` where `key` is
//!   ordered.
//! * `VecIndexedWSet`:  Collections whose data have the form `(key, val)` where
//!   `key` and `val` are ordered and whose timestamp type is `()`.
//!   Semantically, such collections store `(key, val, weight)` tuples without
//!   timing information, and implement the indexed ZSet abstraction of DBSP.
//! * `VecWSet`:  Collections whose data have the form `key` where `key` is
//!   ordered and whose timestamp type is `()`.  Semantically, such collections
//!   store `(key, weight)` tuples without timing information, and implement the
//!   ZSet abstraction of DBSP.
//!
//! Although `VecVal` is more general than `VecKey`, the latter has a simpler
//! representation and should consume fewer resources (computation and memory)
//! when it applies.
//!
//! Likewise, `VecIndexedWSet` and `VecWSet` are less general than `VecVal` and
//! `VecKey` respectively, but are more light-weight.

pub mod indexed_wset_batch;
pub mod key_batch;
pub mod val_batch;
pub mod wset_batch;

pub use super::merge_batcher;

pub use indexed_wset_batch::{VecIndexedWSet, VecIndexedWSetFactories};
pub use key_batch::{VecKeyBatch, VecKeyBatchFactories};
pub use val_batch::{VecValBatch, VecValBatchFactories};
pub use wset_batch::{VecWSet, VecWSetFactories};
