//! Buffer cache that can be used to cache reads.

/// A buffer-cache based on LRU eviction.
mod cache;

pub use feldera_storage::fbuf::{FBuf, FBufSerializer, LimitExceeded};

pub use cache::{
    AtomicCacheCounts, AtomicCacheStats, BufferCache, CacheAccess, CacheCounts, CacheEntry,
    CacheStats,
};
