//! Hashing utilities.

use std::hash::{Hash, Hasher};
use xxhash_rust::xxh3::Xxh3;

const SEED: u64 = 0x7f95_ef85_be33_c337u64;

/// Default hashing function used to shard records across workers.
pub fn default_hash<T: Hash>(x: &T) -> u64 {
    let mut hasher = Xxh3::with_seed(SEED);
    x.hash(&mut hasher);
    hasher.finish()
}
