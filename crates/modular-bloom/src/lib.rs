//! Bloom filters split into independently droppable modules.
//!
//! Implements the modular Bloom filter design described in ElasticBF and SHaMBa papers.
//!
//! A [`ModularBloomFilter`] is `D` independent Bloom filters over the same key
//! set. Membership is the conjunction across modules, so
//! dropping a trailing module raises the false positive rate but never produces
//! a false negative. That makes filter accuracy a dial an owner can turn at any
//! time, in either direction, instead of a property fixed when the filter was
//! built.
//!
//! Dropping a module removes `1/D` of the bits and `1/D` of the hashes at the
//! same time, so the ratio of hashes to bits stays at its optimum and every
//! prefix of the modules is as accurate as a filter purpose-built at that size.
//!
//! # Scope
//!
//! This crate builds and probes filters in memory. It defines
//! no serialization format. A caller that persists filters is responsible for
//! storing [`ModuleLayout`] and the module words, and for handing them back
//! to [`ModularBloomFilter::from_modules`]. Module words are exposed as plain
//! `&[u64]` so the caller chooses the encoding, the framing and the checksums.
//!
//! # Interoperating with monolithic filters
//!
//! A monolithic `fastbloom` filter is exactly a modular filter with one module:
//! module 0 sets and tests precisely the bits a bare `fastbloom::BloomFilter`
//! would. [`ModuleLayout::monolithic`] describes such a filter, so one can be
//! loaded and probed with no conversion and no separate code path. It has a
//! single module, so there is nothing in it to truncate.
//!
//! # Building and probing
//!
//! Insertion and truncation are separated by type. [`ModularBloomFilterBuilder`]
//! accepts keys, [`ModularBloomFilter`] answers queries and resizes.
//!
//! Only a filter holding every module may be inserted into. A key inserted
//! while some modules are dropped is recorded in the modules present and
//! missing from the ones absent. Membership is the conjunction over the
//! modules held, so the key is found while they stay dropped, and lost as soon
//! as any of them is read back from storage. Splitting the two roles by type
//! means no such filter can be built.
//!
//! # Hashing
//!
//! Callers supply the key hash. The crate never sees the key itself, which keeps
//! hashing policy with the caller and lets one hash serve every filter consulted
//! during a lookup.
//!
//! One hash chain is threaded across all the modules, so **the caller must
//! supply a hash that is well distributed across all 64 bits**. The chain seeds
//! itself from the top half of the hash, so a caller whose high bits carry
//! little entropy degrades every module at once, rather than just the first.
//! xxh3 is a good choice. Measured with the top 32 bits pinned to zero, a
//! four-module filter admits 4.3% of absent keys against a nominal 0.01%, so
//! this is a real precondition and not a formality.

mod bits;
mod density;
mod error;
mod filter;
mod layout;

pub use density::ModuleDensity;
pub use error::{LayoutError, LoadError};
pub use filter::{ModularBloomFilter, ModularBloomFilterBuilder};
pub use layout::ModuleLayout;
