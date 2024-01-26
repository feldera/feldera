//! The buffer cache implementation, just implements the same storage traits
//! as the backends ([`StorageControl`], [`StorageRead`], [`StorageWrite`]).
//!
//! It forwards requests to a storage backend, but it also adds a buffer cache
//! in front of the backend to serve reads faster if they are cached.

use metrics::counter;
use std::cell::RefCell;
use std::collections::HashMap;
use std::future::Future;
use std::ops::Range;
use std::rc::Rc;
use wtinylfu::WTinyLfuCache;

use crate::backend::{
    FileHandle, ImmutableFileHandle, StorageControl, StorageError, StorageExecutor, StorageRead,
    StorageWrite,
};
use crate::buffer_cache::FBuf;

/// The key for the cache is a tuple of (file_handle, offset, size), and
/// identifies a slice in a file.
type CacheKey = (i64, u64, usize);

/// CachedBuffer (the values of the cache) are reference-counted buffers.
type CachedBuffer = Rc<FBuf>;

/// Adds a buffer-cache to a storage backend which is supposed to contain
/// the most recently used buffers and evict least recently used ones to save
/// memory.
///
/// The strategy is to cache on (successful) writes, since it is likely that
/// we will read the same data again soon. However, due to this we need to
/// ensure that we do not put the cache in an inconsistent state.
/// e.g., if we have two writes to overlapping regions the cache needs
/// to still be correct (or at least reject the write, which is easier and
/// what we do since our storage format is append-only).
pub struct BufferCache<B> {
    cache: RefCell<WTinyLfuCache<CacheKey, CachedBuffer>>,
    /// A list of written ranges per file. It's important that the vector is
    /// sorted by the start of the range, since we binary search it.
    blocks: RefCell<HashMap<i64, Vec<Range<u64>>>>,
    /// The IO backend that handles reads/writes from disk.
    backend: B,
}

impl<B> BufferCache<B> {
    /// How many entries the LFU cache holds.
    ///
    /// Likely to become a configurable parameter in the future.
    const LFU_CACHE_SIZE: usize = 4096;

    /// Sample size for the LFU cache.
    ///
    /// Not quite sure what this parameter is for yet.
    /// Likely to become a configurable parameter in the future.
    const TINY_LFU_SAMPLE_SIZE: usize = 42;

    pub fn with_backend(backend: B) -> Self {
        Self {
            cache: RefCell::new(WTinyLfuCache::new(
                BufferCache::<B>::LFU_CACHE_SIZE,
                BufferCache::<B>::TINY_LFU_SAMPLE_SIZE,
            )),
            blocks: Default::default(),
            backend,
        }
    }

    fn get(&self, key: &CacheKey) -> Option<CachedBuffer> {
        self.cache.borrow_mut().get(key).cloned()
    }

    fn insert(&self, key: CacheKey, value: CachedBuffer) {
        self.cache.borrow_mut().put(key, value);
    }

    fn overlaps_with_previous_write(&self, fd: &FileHandle, range: Range<u64>) -> bool {
        fn overlaps(r1: &Range<u64>, r2: &Range<u64>) -> bool {
            r1.start < r2.end && r2.start < r1.end
        }

        let blocks_ht = self.blocks.borrow();
        let blocks = blocks_ht.get(&fd.into()).unwrap();
        if blocks.is_empty() {
            return false;
        }

        match blocks.binary_search_by(|probe| probe.start.cmp(&range.start)) {
            Ok(_) => {
                // If for whatever reason this function changes in the future,
                // it's important to return true if the range is already in the list.
                // (or change the logic that assumes it in [`write_block`] accordingly).
                true
            }
            Err(i) if i > 0 => {
                overlaps(&blocks[i - 1], &range)
                    || blocks.len() > i && overlaps(&blocks[i], &range)
                    || blocks.len() > i + 1 && overlaps(&blocks[i + 1], &blocks[i])
            }
            Err(_) => {
                // i == 0
                overlaps(&blocks[0], &range)
            }
        }
    }
}

#[test]
fn overlaps_with_previous_write_check() {
    use crate::backend::tests::InMemoryBackend;

    let mut blocks = HashMap::new();
    blocks.insert(1, vec![0..10, 20..30, 40..50]);
    let blocks = RefCell::new(blocks);
    let mut cache = BufferCache::with_backend(InMemoryBackend::<true>::default());
    cache.blocks = blocks;

    let fd = &FileHandle::new(1);
    assert!(cache.overlaps_with_previous_write(fd, 5..15));
    assert!(cache.overlaps_with_previous_write(fd, 0..10));
    assert!(cache.overlaps_with_previous_write(fd, 0..30));
    assert!(cache.overlaps_with_previous_write(fd, 20..30));
    assert!(cache.overlaps_with_previous_write(fd, 39..51));
    assert!(cache.overlaps_with_previous_write(fd, 49..51));
    assert!(!cache.overlaps_with_previous_write(fd, 10..20));
    assert!(!cache.overlaps_with_previous_write(fd, 30..40));
    assert!(!cache.overlaps_with_previous_write(fd, 50..60));

    let mut blocks = HashMap::new();
    #[allow(clippy::single_range_in_vec_init)]
    blocks.insert(1, vec![1024..(1024 + 2048)]);
    let blocks = RefCell::new(blocks);
    let mut cache = BufferCache::with_backend(InMemoryBackend::<true>::default());
    cache.blocks = blocks;
    let fd = &FileHandle::new(1);
    assert!(cache.overlaps_with_previous_write(fd, 512..(1024 + 512)));
}

impl<B: StorageControl> StorageControl for BufferCache<B> {
    async fn create(&self) -> Result<FileHandle, StorageError> {
        let fd = self.backend.create().await?;
        let fid = (&fd).into();
        self.blocks.borrow_mut().insert(fid, Vec::new());
        Ok(fd)
    }

    async fn delete(&self, fd: ImmutableFileHandle) -> Result<(), StorageError> {
        self.backend.delete(fd).await
    }

    async fn delete_mut(&self, fd: FileHandle) -> Result<(), StorageError> {
        let fid = (&fd).into();
        self.backend.delete_mut(fd).await?;
        self.blocks.borrow_mut().remove(&fid);
        Ok(())
    }
}

impl<B: StorageRead> StorageRead for BufferCache<B> {
    async fn prefetch(&self, _fd: &ImmutableFileHandle, _offset: u64, _size: usize) {}

    async fn read_block(
        &self,
        fd: &ImmutableFileHandle,
        offset: u64,
        size: usize,
    ) -> Result<Rc<FBuf>, StorageError> {
        if let Some(buf) = self.get(&(fd.into(), offset, size)) {
            counter!("disk.buffer_cache_hit").increment(1);
            Ok(buf)
        } else {
            counter!("disk.buffer_cache_miss").increment(1);
            match self.backend.read_block(fd, offset, size).await {
                Ok(buf) => {
                    self.insert((fd.into(), offset, size), buf.clone());
                    Ok(buf)
                }
                Err(e) => Err(e),
            }
        }
    }

    async fn get_size(&self, fd: &ImmutableFileHandle) -> Result<u64, StorageError> {
        self.backend.get_size(fd).await
    }
}

impl<B: StorageWrite> StorageWrite for BufferCache<B> {
    /// The BufferCache `write_block` function is more restrictive than the
    /// trait definition. It does not allow overlapping writes.
    /// This is to allow to fill the cache on writes.
    async fn write_block(
        &self,
        fd: &FileHandle,
        offset: u64,
        data: FBuf,
    ) -> Result<Rc<FBuf>, StorageError> {
        if self.overlaps_with_previous_write(fd, offset..offset + data.len() as u64) {
            return Err(StorageError::OverlappingWrites);
        }
        let res = self.backend.write_block(fd, offset, data).await;
        match res {
            Ok(buf) => {
                self.insert((fd.into(), offset, buf.len()), buf.clone());

                // !overlaps_with_previous_write => range not in the list yet
                let mut blocks = self.blocks.borrow_mut();
                let block_vector = blocks.get_mut(&fd.into()).unwrap();
                let new_range = offset..offset + buf.len() as u64;
                let pos = block_vector
                    .binary_search_by(|probe| probe.start.cmp(&new_range.start))
                    .unwrap_or_else(|e| e);
                block_vector.insert(pos, new_range);
                Ok(buf)
            }
            Err(e) => Err(e),
        }
    }

    async fn complete(&self, fd: FileHandle) -> Result<ImmutableFileHandle, StorageError> {
        let fid = (&fd).into();
        let fd = self.backend.complete(fd).await?;
        self.blocks.borrow_mut().remove(&fid);
        Ok(fd)
    }
}

impl<B: StorageExecutor> StorageExecutor for BufferCache<B>
where
    B: StorageExecutor,
{
    fn block_on<F>(&self, future: F) -> F::Output
    where
        F: Future,
    {
        self.backend.block_on(future)
    }
}
