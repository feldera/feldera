//! Hashing utilities.

use std::hash::{Hash, Hasher};
use xxhash_rust::xxh3::Xxh3Default;

/// Default hashing function used to shard records across workers.
pub fn default_hash<T: Hash + ?Sized>(x: &T) -> u64 {
    let mut hasher = Xxh3Default::new();
    x.hash(&mut hasher);
    hasher.finish()
}
