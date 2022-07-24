//! Some basic operators.

pub mod adapter;
pub mod apply2;
pub mod communication;
pub mod recursive;

pub(crate) mod apply;
pub(crate) mod inspect;

mod aggregate;
mod condition;
mod consolidate;
#[cfg(feature = "with-csv")]
mod csv;
mod delta0;
mod differentiate;
mod distinct;
mod filter_map;
mod generator;
mod index;
mod integrate;
mod join;
mod join_range;
mod neg;
mod plus;
mod semijoin;
mod sum;
mod trace;
mod window;
mod z1;

#[cfg(feature = "with-csv")]
pub use self::csv::CsvSource;
pub use adapter::{BinaryOperatorAdapter, UnaryOperatorAdapter};
pub use aggregate::Aggregate;
pub use apply::Apply;
pub use condition::Condition;
pub use delta0::Delta0;
pub use distinct::Distinct;
pub use filter_map::{FilterKeys, FilterMap, FilterVals, FlatMap, Map, MapKeys};
pub use generator::{Generator, GeneratorNested};
pub use index::Index;
pub use inspect::Inspect;
pub use join::Join;
pub use join_range::StreamJoinRange;
pub use neg::UnaryMinus;
pub use plus::{Minus, Plus};
pub use sum::Sum;
pub use z1::{DelayedFeedback, DelayedNestedFeedback, Z1Nested, Z1};
