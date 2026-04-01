//! Buffer cache that can be used to cache reads.

/// Buffer-cache implementations and cache statistics.
mod cache;

pub use feldera_storage::fbuf::{FBuf, FBufSerializer, LimitExceeded};

pub use cache::{
    AtomicCacheCounts, AtomicCacheStats, BufferCache, CacheAccess, CacheCounts, CacheStats,
};

pub(crate) use cache::build_buffer_caches;
