//! Buffer cache that can be used to cache reads.

/// A buffer-cache based on LRU eviction.
mod cache;
/// A file-backed buffer.
mod fbuf;

pub use cache::{BufferCache, CacheEntry};
pub use fbuf::{FBuf, FBufSerializer};
