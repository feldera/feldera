#![cfg_attr(docsrs, feature(doc_cfg))]
#![doc = include_str!("../../../README.md")]

mod error;
mod hash;
mod num_entries;
mod ref_pair;

#[macro_use]
pub mod circuit;
pub mod algebra;
pub mod mimalloc;
pub mod monitor;
pub mod operator;
pub mod profile;
pub mod time;
pub mod trace;
pub mod utils;

pub use crate::error::Error;
pub use crate::hash::default_hash;
pub use crate::num_entries::NumEntries;
pub use crate::ref_pair::RefPair;
pub use crate::time::Timestamp;

pub use algebra::{IndexedZSet, ZSet};
pub use circuit::{
    ChildCircuit, Circuit, CircuitHandle, DBSPHandle, RootCircuit, Runtime, RuntimeError,
    SchedulerError, Stream,
};
pub use operator::{CollectionHandle, InputHandle, OutputHandle, UpsertHandle};
pub use trace::ord::{OrdIndexedZSet, OrdZSet};
pub use trace::{DBData, DBTimestamp, DBWeight};
