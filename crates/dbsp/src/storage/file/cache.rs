//! Caching layer for layer files.
//!
//! This implements an approximately LRU cache for layer files.  The
//! [`Reader`](super::reader::Reader) and [writer](super::writer) use it.
use std::sync::Arc;
use std::{mem::size_of, time::Instant};

use crc32c::crc32c;

use binrw::{
    io::{self},
    BinRead,
};
use snap::raw::{decompress_len, Decoder};

use crate::storage::buffer_cache::{AtomicCacheStats, CacheAccess};
use crate::storage::{
    backend::{BlockLocation, FileReader},
    buffer_cache::{BufferCache, CacheEntry, FBuf},
    file::format::Compression,
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
    /// Returns the data size in bytes.
    fn cost(&self) -> usize {
        match self {
            Self::FileTrailer(_) => size_of::<FileTrailer>(),
            Self::Index(index_block) => index_block.cost(),
            Self::Data(data_block) => data_block.cost(),
        }
    }
}
impl FileCacheEntry {
    pub(super) fn from_read(
        raw: Arc<FBuf>,
        location: BlockLocation,
        compression: Option<Compression>,
    ) -> Result<Self, Error> {
        let raw = if let Some(compression) = compression {
            let compressed_len = u32::from_le_bytes(raw[..4].try_into().unwrap()) as usize;
            let Some(compressed) = raw[4..].get(..compressed_len) else {
                return Err(CorruptionError::BadCompressedLen {
                    location,
                    compressed_len,
                    max_compressed_len: raw.len() - 4,
                }
                .into());
            };
            match compression {
                Compression::Snappy => {
                    let decompressed_len = decompress_len(compressed).map_err(|error| {
                        Error::Corruption(CorruptionError::Snappy { location, error })
                    })?;
                    let mut decompressed = FBuf::with_capacity(decompressed_len);
                    decompressed.resize(decompressed_len, 0);
                    match Decoder::new().decompress(compressed, decompressed.as_mut_slice()) {
                        Ok(n) if n == decompressed_len => {}
                        Ok(n) => {
                            return Err(CorruptionError::UnexpectedDecompressionLength {
                                location,
                                length: n,
                                expected_length: decompressed_len,
                            }
                            .into())
                        }
                        Err(error) => {
                            return Err(CorruptionError::Snappy { location, error }.into())
                        }
                    }
                    Arc::new(decompressed)
                }
            }
        } else {
            raw
        };
        let computed_checksum = crc32c(&raw[4..]);
        let checksum = u32::from_le_bytes(raw[..4].try_into().unwrap());
        if checksum != computed_checksum {
            return Err(CorruptionError::InvalidChecksum {
                location,
                magic: raw[4..8].try_into().unwrap(),
                checksum,
                computed_checksum,
            }
            .into());
        }

        Self::from_write(raw, location)
    }

    pub(super) fn from_write(raw: Arc<FBuf>, location: BlockLocation) -> Result<Self, Error> {
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

impl BufferCache<FileCacheEntry> {
    fn get_entry(
        &self,
        file: &dyn FileReader,
        location: BlockLocation,
        compression: Option<Compression>,
        stats: &AtomicCacheStats,
    ) -> Result<FileCacheEntry, Error> {
        let start = Instant::now();
        let (access, entry) = match self.get(file, location) {
            Some(entry) => (CacheAccess::Hit, entry),
            None => {
                let block = file.read_block(location)?;
                let entry = FileCacheEntry::from_read(block, location, compression)?;
                self.insert(file.file_id(), location.offset, entry.clone());
                (CacheAccess::Miss, entry)
            }
        };
        stats.record(access, start.elapsed(), location);
        Ok(entry)
    }

    /// Reads `location` from `file` and returns it converted to
    /// `InnerDataBlock`.
    pub(super) fn read_data_block(
        &self,
        file: &dyn FileReader,
        location: BlockLocation,
        compression: Option<Compression>,
        stats: &AtomicCacheStats,
    ) -> Result<Arc<InnerDataBlock>, Error> {
        match self.get_entry(file, location, compression, stats)? {
            FileCacheEntry::Data(inner) => Ok(inner),
            _ => Err(Error::Corruption(CorruptionError::BadBlockType(location))),
        }
    }

    /// Reads `location` from `file` and returns it converted to
    /// `InnerIndexBlock`.
    pub(super) fn read_index_block(
        &self,
        file: &dyn FileReader,
        location: BlockLocation,
        compression: Option<Compression>,
        stats: &AtomicCacheStats,
    ) -> Result<Arc<InnerIndexBlock>, Error> {
        match self.get_entry(file, location, compression, stats)? {
            FileCacheEntry::Index(inner) => Ok(inner),
            _ => Err(Error::Corruption(CorruptionError::BadBlockType(location))),
        }
    }

    /// Reads `location` from `file` and returns it converted to `FileTrailer`.
    pub(super) fn read_file_trailer_block(
        &self,
        file: &dyn FileReader,
        location: BlockLocation,
        stats: &AtomicCacheStats,
    ) -> Result<Arc<FileTrailer>, Error> {
        match self.get_entry(file, location, None, stats)? {
            FileCacheEntry::FileTrailer(inner) => Ok(inner),
            _ => Err(Error::Corruption(CorruptionError::BadBlockType(location))),
        }
    }
}
