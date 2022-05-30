//! Some basic operators.

pub mod adapter;
pub use adapter::{BinaryOperatorAdapter, UnaryOperatorAdapter};

pub(crate) mod inspect;
pub use inspect::Inspect;

pub(crate) mod apply;
pub use apply::Apply;

mod apply2;
pub use apply2::Apply2;

mod plus;
pub use plus::{Minus, Plus};

mod z1;
pub use z1::{DelayedFeedback, DelayedNestedFeedback, Z1Nested, Z1};

mod generator;
pub use generator::{Generator, GeneratorNested};

mod integrate;
mod trace;
mod consolidate;

pub mod communication;

mod differentiate;

mod filter;
pub use filter::FilterKeys;

mod delta0;
pub use delta0::Delta0;

mod condition;
pub use condition::Condition;

mod index;
pub use index::Index;

//mod join;
//pub use join::Join;

mod sum;
pub use sum::Sum;

mod distinct;
pub use distinct::Distinct;

mod map;
pub use map::{MapKeys, MapValues};

mod filter_map;
pub use filter_map::FilterMapKeys;

mod aggregate;
pub use aggregate::Aggregate;

#[cfg(feature = "with-csv")]
mod csv;
#[cfg(feature = "with-csv")]
pub use self::csv::CsvSource;
