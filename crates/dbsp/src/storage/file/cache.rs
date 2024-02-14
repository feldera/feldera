//! Caching layer for layer files.
//!
//! This implements an approximately LRU cache for layer files.  The
//! [`Reader`](super::reader::Reader) and [writer](super::writer) use it.
use std::{mem::size_of, rc::Rc};

use crc32c::crc32c;

use binrw::{
    io::{self},
    BinRead,
};

use crate::storage::{
    backend::{
        DefaultBackend, ImmutableFileHandle, StorageControl, StorageExecutor, StorageRead,
        StorageWrite,
    },
    buffer_cache::{BufferCache, CacheEntry, FBuf},
    file::BlockLocation,
};

use super::{
    format::{
        BlockHeader, FileTrailer, DATA_BLOCK_MAGIC, FILE_TRAILER_BLOCK_MAGIC, INDEX_BLOCK_MAGIC,
    },
    reader::{CorruptionError, Error, InnerDataBlock, InnerIndexBlock},
};

/// Buffer cache type for [`Reader`](super::reader::Reader) and
/// [`Writer`](super::writer::Writer1).
pub type FileCache<B> = BufferCache<B, FileCacheEntry>;

/// A cached interpretation of a particular block.
#[derive(Clone)]
pub enum FileCacheEntry {
    /// File trailer block.
    FileTrailer(Rc<FileTrailer>),

    /// Index block.
    Index(Rc<InnerIndexBlock>),

    /// Data block.
    Data(Rc<InnerDataBlock>),
}

impl CacheEntry for FileCacheEntry {
    fn cost(&self) -> usize {
        match self {
            Self::FileTrailer(_) => size_of::<FileTrailer>(),
            Self::Index(index_block) => index_block.cost(),
            Self::Data(data_block) => data_block.cost(),
        }
    }
    fn from_read(raw: Rc<FBuf>, offset: u64, size: usize) -> Result<Self, Error> {
        let computed_checksum = crc32c(&raw[4..]);
        let checksum = u32::from_le_bytes(raw[..4].try_into().unwrap());
        if checksum != computed_checksum {
            return Err(CorruptionError::InvalidChecksum {
                size,
                offset,
                checksum,
                computed_checksum,
            }
            .into());
        }

        Self::from_write(raw, offset, size)
    }
    fn from_write(raw: Rc<FBuf>, offset: u64, size: usize) -> Result<Self, Error> {
        let block_header = BlockHeader::read_le(&mut io::Cursor::new(raw.as_slice()))?;
        match block_header.magic {
            DATA_BLOCK_MAGIC => Ok(Self::Data(Rc::new(InnerDataBlock::from_raw(
                raw,
                BlockLocation { offset, size },
            )?))),
            INDEX_BLOCK_MAGIC => Ok(Self::Index(Rc::new(InnerIndexBlock::from_raw(
                raw,
                BlockLocation { offset, size },
            )?))),
            FILE_TRAILER_BLOCK_MAGIC => Ok(Self::FileTrailer(Rc::new(FileTrailer::read_le(
                &mut io::Cursor::new(raw.as_slice()),
            )?))),
            _ => Err(
                Error::Corruption(CorruptionError::BadBlockType { offset, size }), /* XXX */
            ),
        }
    }
}

impl FileCacheEntry {
    fn as_file_trailer(&self) -> Result<Rc<FileTrailer>, ()> {
        match self {
            Self::FileTrailer(inner) => Ok(inner.clone()),
            _ => Err(()),
        }
    }

    fn as_data_block(&self) -> Result<Rc<InnerDataBlock>, ()> {
        match self {
            Self::Data(inner) => Ok(inner.clone()),
            _ => Err(()),
        }
    }

    fn as_index_block(&self) -> Result<Rc<InnerIndexBlock>, ()> {
        match self {
            Self::Index(inner) => Ok(inner.clone()),
            _ => Err(()),
        }
    }
}

impl FileCache<DefaultBackend> {
    fn new_default_for_thread() -> Rc<FileCache<DefaultBackend>> {
        Rc::new(BufferCache::new(DefaultBackend::default_for_thread()))
    }

    /// Returns a thread-local default backend.
    pub fn default_for_thread() -> Rc<FileCache<DefaultBackend>> {
        thread_local! {
            pub static DEFAULT_CACHE: Rc<FileCache<DefaultBackend>> = BufferCache::<DefaultBackend, FileCacheEntry>::new_default_for_thread();
        }
        DEFAULT_CACHE.with(|rc| rc.clone())
    }
}

impl<B> BufferCache<B, FileCacheEntry>
where
    B: StorageRead + StorageWrite + StorageControl + StorageExecutor,
{
    /// Reads a `size`-byte block at `offset` in `fd` and returns it converted
    /// to `InnerDataBlock`.
    pub(super) async fn read_data_block(
        &self,
        fd: &ImmutableFileHandle,
        offset: u64,
        size: usize,
    ) -> Result<Rc<InnerDataBlock>, Error> {
        self.read(fd, offset, size, FileCacheEntry::as_data_block)
            .await
    }

    /// Reads a `size`-byte block at `offset` in `fd` and returns it converted
    /// to `InnerIndexBlock`.
    pub(super) async fn read_index_block(
        &self,
        fd: &ImmutableFileHandle,
        offset: u64,
        size: usize,
    ) -> Result<Rc<InnerIndexBlock>, Error> {
        self.read(fd, offset, size, FileCacheEntry::as_index_block)
            .await
    }

    /// Reads a `size`-byte file trailer block at `offset` in `fd` and returns
    /// it converted to `FileTrailer`.
    pub(super) async fn read_file_trailer_block(
        &self,
        fd: &ImmutableFileHandle,
        offset: u64,
        size: usize,
    ) -> Result<Rc<FileTrailer>, Error> {
        self.read(fd, offset, size, FileCacheEntry::as_file_trailer)
            .await
    }
}
