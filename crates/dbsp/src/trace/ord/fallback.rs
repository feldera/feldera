//! Batch implementations that fall back from memory to disk.

#![allow(clippy::large_enum_variant)]

pub mod indexed_wset;
pub mod key_batch;
mod utils;
pub mod val_batch;
pub mod wset;
pub use utils::pick_merge_destination;
