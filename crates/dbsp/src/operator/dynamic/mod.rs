use crate::{
    algebra::{OrdIndexedZSet, OrdIndexedZSetFactories, OrdZSet, OrdZSetFactories},
    dynamic::DynData,
};

pub mod aggregate;
pub mod asof_join;
mod communication;
mod consolidate;
pub mod controlled_filter;
pub mod count;
pub mod distinct;
pub mod filter_map;
pub mod group;
pub mod index;
pub mod input;
pub(crate) mod input_upsert;
pub mod join;
pub mod join_range;
pub mod neighborhood;
mod output;
pub mod recursive;
pub mod sample;
pub mod semijoin;
pub mod time_series;
pub mod trace;
pub(crate) mod upsert;

/// The "standard" indexed Z-set type used by monomorphic
/// versions of operators.
pub type MonoIndexedZSet = OrdIndexedZSet<DynData, DynData>;

/// The "standard" Z-set type used by monomorphic
/// versions of operators.
pub type MonoZSet = OrdZSet<DynData>;

pub type MonoIndexedZSetFactories = OrdIndexedZSetFactories<DynData, DynData>;
pub type MonoZSetFactories = OrdZSetFactories<DynData>;
