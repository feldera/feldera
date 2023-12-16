/// A buffer-cache based on LRU eviction.
mod cache;
/// A file-backed buffer.
mod fbuf;

#[cfg(test)]
mod tests;

pub use cache::BufferCache;
pub use fbuf::FBuf;
