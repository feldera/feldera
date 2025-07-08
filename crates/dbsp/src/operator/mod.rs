//! DBSP stream operator implementations.
//!
//! Operators are implemented in two layers. An outer layer, in most of the
//! submodules of this one, implements a statically typed API. An inner layer,
//! in the [dynamic] sub-module, is dynamically typed, unsafe, and wrapped by
//! the outer layer in a safe way.
//!
//! Code that uses DBSP hardly needs to work directly with this module. Instead,
//! use [Stream](crate::Stream) methods to instantiate operators.
pub(crate) mod apply;
pub mod apply2;
pub mod apply3;
pub mod apply_n;
mod async_stream_operators;
pub mod communication;
pub(crate) mod inspect;

mod accumulator;
mod condition;
mod count;
mod csv;
mod delta0;
mod differentiate;
mod generator;
mod integrate;
mod macrostep_z1;
mod neg;
mod output;
mod plus;
mod stream_fold;
mod sum;
mod z1;

mod aggregate;
mod asof_join;
mod average;
pub mod chain_aggregate;
mod consolidate;
pub mod controlled_filter;
mod distinct;
pub mod dynamic;
#[cfg(not(feature = "backend-mode"))]
pub mod filter_map;
pub mod group;
pub mod input;
pub mod join;
mod join_range;
pub mod neighborhood;
mod non_incremental;
mod recursive;
pub mod sample;
mod semijoin;
pub mod time_series;
mod trace;

use crate::circuit::GlobalNodeId;
use crate::storage::backend::StorageError;
use crate::Error;

pub use self::csv::CsvSource;
pub use apply::Apply;
pub use condition::Condition;
pub use delta0::Delta0;
pub use dynamic::aggregate::{
    Aggregator, Avg, Fold, Max, MaxSemigroup, Min, MinSemigroup, Postprocess,
};
pub use dynamic::neighborhood::DynNeighborhood;
pub use generator::{Generator, GeneratorNested};
// // //pub use index::Index;
pub use group::CmpFunc;
use input::Mailbox;
pub use input::{
    IndexedZSetHandle, Input, InputHandle, MapHandle, SetHandle, StagedBuffers, Update, ZSetHandle,
};
pub use inspect::Inspect;

pub use dynamic::join_range::StreamJoinRange;
// // //pub use neg::UnaryMinus;
pub use dynamic::{neighborhood::NeighborhoodDescr, trace::TraceBound};
#[cfg(not(feature = "backend-mode"))]
pub use filter_map::FilterMap;
pub use macrostep_z1::MacrostepZ1;
pub use neighborhood::{NeighborhoodDescrBox, NeighborhoodDescrStream};
pub use output::OutputHandle;
pub use plus::{Minus, Plus};
pub use recursive::RecursiveStreams;
pub use sample::{MAX_QUANTILES, MAX_SAMPLE_SIZE};
pub use sum::Sum;
pub use time_series::OrdPartitionedIndexedZSet;
pub use z1::{DelayedFeedback, DelayedNestedFeedback, Z1Nested, Z1};

/// Returns a `NoPersistentId` error if `persistent_id` is `None`.
fn require_persistent_id<'a>(
    persistent_id: Option<&'a str>,
    global_id: &GlobalNodeId,
) -> Result<&'a str, Error> {
    persistent_id.ok_or_else(|| Error::Storage(StorageError::NoPersistentId(global_id.to_string())))
}
