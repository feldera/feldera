//! Caching layer for layer files.
//!
//! This implements an approximately LRU cache for layer files.  The
//! [`Reader`](super::reader::Reader) and [writer](super::writer) use it.
use std::mem::size_of;
use std::sync::Arc;

use crc32c::crc32c;

use binrw::{
    io::{self},
    BinRead,
};
use lazy_static::lazy_static;

use crate::storage::{
    backend::{BlockLocation, FileReader},
    buffer_cache::{BufferCache, CacheEntry, FBuf},
};

use super::{
    format::{
        BlockHeader, FileTrailer, DATA_BLOCK_MAGIC, FILE_TRAILER_BLOCK_MAGIC, INDEX_BLOCK_MAGIC,
    },
    reader::{CorruptionError, Error, InnerDataBlock, InnerIndexBlock},
};

/// Buffer cache type for [`Reader`](super::reader::Reader) and
/// [`Writer`](super::writer::Writer1).
pub type FileCache = BufferCache<FileCacheEntry>;

/// A cached interpretation of a particular block.
#[derive(Clone)]
pub enum FileCacheEntry {
    /// File trailer block.
    FileTrailer(Arc<FileTrailer>),

    /// Index block.
    Index(Arc<InnerIndexBlock>),

    /// Data block.
    Data(Arc<InnerDataBlock>),
}

impl CacheEntry for FileCacheEntry {
    fn cost(&self) -> usize {
        match self {
            Self::FileTrailer(_) => size_of::<FileTrailer>(),
            Self::Index(index_block) => index_block.cost(),
            Self::Data(data_block) => data_block.cost(),
        }
    }
    fn from_read(raw: Arc<FBuf>, location: BlockLocation) -> Result<Self, Error> {
        let computed_checksum = crc32c(&raw[4..]);
        let checksum = u32::from_le_bytes(raw[..4].try_into().unwrap());
        if checksum != computed_checksum {
            return Err(CorruptionError::InvalidChecksum {
                location,
                checksum,
                computed_checksum,
            }
            .into());
        }

        Self::from_write(raw, location)
    }
    fn from_write(raw: Arc<FBuf>, location: BlockLocation) -> Result<Self, Error> {
        let block_header = BlockHeader::read_le(&mut io::Cursor::new(raw.as_slice()))?;
        match block_header.magic {
            DATA_BLOCK_MAGIC => Ok(Self::Data(Arc::new(InnerDataBlock::from_raw(
                raw, location,
            )?))),
            INDEX_BLOCK_MAGIC => Ok(Self::Index(Arc::new(InnerIndexBlock::from_raw(
                raw, location,
            )?))),
            FILE_TRAILER_BLOCK_MAGIC => Ok(Self::FileTrailer(Arc::new(FileTrailer::read_le(
                &mut io::Cursor::new(raw.as_slice()),
            )?))),
            _ => Err(Error::Corruption(CorruptionError::BadBlockType(location))),
        }
    }
}

impl FileCacheEntry {
    fn as_file_trailer(&self) -> Result<Arc<FileTrailer>, ()> {
        match self {
            Self::FileTrailer(inner) => Ok(inner.clone()),
            _ => Err(()),
        }
    }

    fn as_data_block(&self) -> Result<Arc<InnerDataBlock>, ()> {
        match self {
            Self::Data(inner) => Ok(inner.clone()),
            _ => Err(()),
        }
    }

    fn as_index_block(&self) -> Result<Arc<InnerIndexBlock>, ()> {
        match self {
            Self::Index(inner) => Ok(inner.clone()),
            _ => Err(()),
        }
    }
}

lazy_static! {
    static ref DEFAULT_CACHE: Arc<FileCache> = Arc::new(FileCache::new());
}

/// Returns a global `FileCache` suitable for examples, tests, and other
/// programs that don't need a specific backend configuration.
pub fn default_cache() -> Arc<FileCache> {
    DEFAULT_CACHE.clone()
}

impl BufferCache<FileCacheEntry> {
    /// Reads `location` from `file` and returns it converted to
    /// `InnerDataBlock`.
    pub(super) fn read_data_block(
        &self,
        file: &dyn FileReader,
        location: BlockLocation,
    ) -> Result<Arc<InnerDataBlock>, Error> {
        self.read(file, location, FileCacheEntry::as_data_block)
    }

    /// Reads `location` from `file` and returns it converted to
    /// `InnerIndexBlock`.
    pub(super) fn read_index_block(
        &self,
        file: &dyn FileReader,
        location: BlockLocation,
    ) -> Result<Arc<InnerIndexBlock>, Error> {
        self.read(file, location, FileCacheEntry::as_index_block)
    }

    /// Reads `location` from `file` and returns it converted to `FileTrailer`.
    pub(super) fn read_file_trailer_block(
        &self,
        file: &dyn FileReader,
        location: BlockLocation,
    ) -> Result<Arc<FileTrailer>, Error> {
        self.read(file, location, FileCacheEntry::as_file_trailer)
    }
}
