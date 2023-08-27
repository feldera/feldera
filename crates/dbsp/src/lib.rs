//! The `dbsp` crate implements a computational engine for continuous analysis
//! of changing data.  With DBSP, a programmer writes code in terms of
//! computations on a complete data set, but DBSP implements it incrementally,
//! meaning that changes to the data set run in time proportional to the size of
//! the change rather than the size of the data set.  This is a major advantage
//! for applications that work with large data sets that change frequently in
//! small ways.
//!
//! The [`tutorial`] is a good place to start for a guided tour.  After that, if
//! you want to look through the API on your own, the [`circuit`] module is a
//! reasonable starting point.  For complete examples, visit the [`examples`][1]
//! directory in the DBSP repository.
//!
//! [1]: https://github.com/feldera/feldera/tree/main/crates/dbsp/examples
//!
//! # Theory
//!
//! DBSP is underpinned by a formal theory:
//!
//! - [Budiu, Chajed, McSherry, Ryzhyk, Tannen. DBSP: Automatic Incremental View
//!   Maintenance for Rich Query Languages, Conference on Very Large Databases, August
//!   2023, Vancouver, Canada](https://www.feldera.com/vldb23.pdf)
//!
//! - Here is [a presentation about DBSP](https://www.youtube.com/watch?v=iT4k5DCnvPU)
//!   at the 2023
//! Apache Calcite Meetup.
//!
//! The model provides two things:
//!
//! 1. **Semantics.** DBSP defines a formal language of streaming operators and
//! queries built out of these operators, and precisely specifies how these
//! queries must transform input streams to output streams.
//!
//! 2. **Algorithm.** DBSP also gives an algorithm that takes an arbitrary query
//! and generates an incremental dataflow program that implements this query
//! correctly (in accordance with its formal semantics) and efficiently.
//! Efficiency here means, in a nutshell, that the cost of processing a set of
//! input events is proportional to the size of the input rather than the entire
//! state of the database.

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

pub use crate::error::{DetailedError, Error};
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
pub use trace::{DBData, DBTimestamp, DBWeight, Rkyv};

#[cfg(doc)]
pub mod tutorial;
