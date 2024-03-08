//! DBSP stream operators.
pub mod apply2;
pub mod apply3;
pub mod communication;

pub(crate) mod apply;
pub(crate) mod inspect;

mod condition;
mod count;
#[cfg(feature = "with-csv")]
mod csv;
mod delta0;
mod differentiate;
mod generator;
mod integrate;
mod neg;
mod output;
mod plus;
mod stream_fold;
mod sum;
mod z1;

mod aggregate;
mod average;
mod consolidate;
mod distinct;
pub mod dynamic;
pub mod filter_map;
pub mod group;
pub mod input;
mod join;
mod join_range;
pub mod neighborhood;
mod recursive;
pub mod sample;
mod semijoin;
pub mod time_series;
mod trace;

#[cfg(feature = "with-csv")]
pub use self::csv::CsvSource;
pub use apply::Apply;
pub use condition::Condition;
pub use delta0::Delta0;
pub use dynamic::aggregate::{Aggregator, Avg, Fold, Max, MaxSemigroup, Min, MinSemigroup};
pub use dynamic::neighborhood::DynNeighborhood;
pub use generator::{Generator, GeneratorNested};
// // //pub use index::Index;
pub use group::CmpFunc;
use input::Mailbox;
pub use input::{IndexedZSetHandle, Input, InputHandle, MapHandle, SetHandle, Update, ZSetHandle};
pub use inspect::Inspect;

pub use dynamic::join_range::StreamJoinRange;
// // //pub use neg::UnaryMinus;
pub use dynamic::{neighborhood::NeighborhoodDescr, trace::TraceBound};
pub use filter_map::FilterMap;
pub use neighborhood::{NeighborhoodDescrBox, NeighborhoodDescrStream};
pub use output::OutputHandle;
pub use plus::{Minus, Plus};
pub use recursive::RecursiveStreams;
pub use sample::{MAX_QUANTILES, MAX_SAMPLE_SIZE};
pub use sum::Sum;
pub use time_series::OrdPartitionedIndexedZSet;
pub use z1::{DelayedFeedback, DelayedNestedFeedback, Z1Nested, Z1};
