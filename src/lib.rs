#![feature(generic_associated_types)]
#![cfg_attr(feature = "with-nexmark", feature(is_some_with))]

mod num_entries;
mod ref_pair;
mod shared_ref;
mod utils;

pub mod algebra;
pub mod circuit;
pub mod lattice;
pub mod monitor;
pub mod operator;
pub mod profile;
pub mod time;
pub mod trace;

#[cfg(feature = "with-nexmark")]
pub mod nexmark;

pub use num_entries::NumEntries;
pub use ref_pair::RefPair;
pub use shared_ref::SharedRef;
pub use time::Timestamp;

pub use circuit::{Circuit, CircuitHandle, Runtime, Stream};
pub use trace::ord::{OrdIndexedZSet, OrdZSet};
