//! DBSP stream operators.

pub mod apply2;
pub mod apply3;
pub mod communication;
pub mod recursive;

pub(crate) mod apply;
pub(crate) mod inspect;
pub(crate) mod upsert;

mod aggregate;
mod condition;
mod consolidate;
mod count;
#[cfg(feature = "with-csv")]
mod csv;
mod delta0;
mod differentiate;
mod distinct;
mod filter_map;
mod generator;
mod group;
mod index;
mod input;
mod integrate;
mod join;
pub mod join_range;
mod neg;
mod neighborhood;
mod output;
mod plus;
pub mod sample;
mod semijoin;
mod stream_fold;
mod sum;
pub mod time_series;
mod trace;
mod z1;

#[cfg(feature = "with-csv")]
pub use self::csv::CsvSource;
pub use aggregate::{Aggregator, Avg, Fold, Max, MaxSemigroup, Min, MinSemigroup};
pub use apply::Apply;
pub use condition::Condition;
pub use delta0::Delta0;
pub use distinct::Distinct;
pub use filter_map::{FilterKeys, FilterMap, FilterVals, FlatMap, Map, MapKeys};
pub use generator::{Generator, GeneratorNested};
pub use index::Index;
use input::Mailbox;
pub use input::{CollectionHandle, InputHandle, UpsertHandle};
pub use inspect::Inspect;
pub use join::Join;
pub use join_range::StreamJoinRange;
pub use neg::UnaryMinus;
pub use neighborhood::{Neighborhood, NeighborhoodDescr};
pub use output::OutputHandle;
pub use plus::{Minus, Plus};
pub use sum::Sum;
pub use z1::{DelayedFeedback, DelayedNestedFeedback, Z1Nested, Z1};
