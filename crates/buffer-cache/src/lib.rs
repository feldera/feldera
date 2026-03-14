//! Weighted in-memory buffer caches with LRU and S3-FIFO eviction.

mod builder;
mod lru;
mod s3_fifo;
mod thread_type;

use std::any::Any;
use std::sync::Arc;

pub use builder::{BufferCacheAllocationStrategy, BufferCacheBuilder, BufferCacheStrategy};
pub use lru::LruCache;
pub use s3_fifo::S3FifoCache;
pub use thread_type::ThreadType;

/// Cached values expose their memory cost directly to the cache backends.
pub trait CacheEntry: Any + Send + Sync {
    /// Returns the cost of this value in bytes.
    fn cost(&self) -> usize;
}

impl<T> CacheEntry for Arc<T>
where
    T: CacheEntry + ?Sized + 'static,
{
    fn cost(&self) -> usize {
        (**self).cost()
    }
}

impl dyn CacheEntry {
    /// Attempts to downcast an `Arc<dyn CacheEntry>` to a concrete entry type.
    pub fn downcast<T>(self: Arc<Self>) -> Option<Arc<T>>
    where
        T: Send + Sync + 'static,
    {
        (self as Arc<dyn Any + Send + Sync>).downcast().ok()
    }
}

/// Shared trait object used to pass buffer-cache backends around.
pub type SharedBufferCache<K, V> = Arc<dyn BufferCache<K, V>>;

/// Common object-safe API implemented by all buffer-cache backends.
pub trait BufferCache<K, V>: Any + Send + Sync {
    /// Returns this backend as [`Any`] for backend-specific downcasts.
    fn as_any(&self) -> &dyn Any;

    /// Returns the eviction strategy used by this cache.
    fn strategy(&self) -> BufferCacheStrategy;

    /// Inserts or replaces `key` with `value`.
    ///
    /// When a key, value is inserted with a the cost that exceeds
    /// the capacity of a shard in the cache, it is up to the
    /// implementation to decided if it wants to store/accept
    /// the key, value pair.
    fn insert(&self, key: K, value: V);

    /// Looks up `key` and returns a clone of the stored value.
    fn get(&self, key: K) -> Option<V>;

    /// Removes `key` if it is present and returns the removed value.
    fn remove(&self, key: &K) -> Option<V>;

    /// Removes every entry whose key matches `predicate`.
    fn remove_if(&self, predicate: &dyn Fn(&K) -> bool);

    /// Returns `true` if `key` is currently resident.
    fn contains_key(&self, key: &K) -> bool;

    /// Returns the number of resident entries.
    fn len(&self) -> usize;

    /// Returns `true` if the cache has no resident entries.
    fn is_empty(&self) -> bool;

    /// Returns the total resident weight.
    fn total_charge(&self) -> usize;

    /// Returns the configured total weight capacity.
    fn total_capacity(&self) -> usize;

    /// Returns the number of shards used by this backend.
    fn shard_count(&self) -> usize;

    /// Returns `(used_charge, capacity)` for shard `idx`.
    #[cfg(test)]
    fn shard_usage(&self, idx: usize) -> (usize, usize);
}

#[cfg(test)]
mod tests;
