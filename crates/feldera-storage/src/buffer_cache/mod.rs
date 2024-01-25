//! Buffer cache that can be used to cache reads.

/// A buffer-cache based on LRU eviction.
mod cache;
/// A file-backed buffer.
mod fbuf;

#[cfg(test)]
mod tests;

pub use cache::{BufferCache, TinyLfuCache};
pub use fbuf::{FBuf, FBufSerializer};
