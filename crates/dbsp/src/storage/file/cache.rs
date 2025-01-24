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
use snap::raw::{decompress_len, Decoder};

use crate::storage::buffer_cache::AtomicCacheStats;
use crate::storage::{
    backend::{BlockLocation, FileReader},
    buffer_cache::{BufferCache, CacheEntry, FBuf},
    file::format::Compression,
};

use super::{
    format::{
        BlockHeader, FileTrailer, DATA_BLOCK_MAGIC, FILE_TRAILER_BLOCK_MAGIC, INDEX_BLOCK_MAGIC,
    },
    reader::{CorruptionError, Error, InnerDataBlock, InnerFileTrailer, InnerIndexBlock},
};

/// Buffer cache type for [`Reader`](super::reader::Reader) and
/// [`Writer`](super::writer::Writer1).
pub type FileCache = BufferCache<FileCacheEntry>;

/// A cached interpretation of a particular block.
#[derive(Clone, Debug)]
pub enum FileCacheEntry {
    /// File trailer block.
    FileTrailer(Arc<InnerFileTrailer>),

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
    pub fn from_read(
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
            FILE_TRAILER_BLOCK_MAGIC => Ok(Self::FileTrailer(Arc::new(
                InnerFileTrailer::from_raw(raw, location)?,
            ))),
            _ => Err(Error::Corruption(CorruptionError::BadBlockType(location))),
        }
    }

    fn location(&self) -> BlockLocation {
        match self {
            FileCacheEntry::FileTrailer(inner) => inner.location,
            FileCacheEntry::Index(inner) => inner.location(),
            FileCacheEntry::Data(inner) => inner.location(),
        }
    }

    fn bad_type_error(&self) -> Error {
        Error::Corruption(CorruptionError::BadBlockType(self.location()))
    }

    pub(super) fn into_data_block(self) -> Result<Arc<InnerDataBlock>, Error> {
        match self {
            FileCacheEntry::Data(inner) => Ok(inner),
            _ => Err(self.bad_type_error()),
        }
    }

    pub(super) fn into_index_block(self) -> Result<Arc<InnerIndexBlock>, Error> {
        match self {
            FileCacheEntry::Index(inner) => Ok(inner),
            _ => Err(self.bad_type_error()),
        }
    }

    pub(super) fn into_file_trailer_block(self) -> Result<Arc<InnerFileTrailer>, Error> {
        match self {
            FileCacheEntry::FileTrailer(inner) => Ok(inner),
            _ => Err(self.bad_type_error()),
        }
    }
}

impl BufferCache<FileCacheEntry> {
    pub(super) fn read_blocking(
        &self,
        file: &dyn FileReader,
        location: BlockLocation,
        compression: Option<Compression>,
        stats: &AtomicCacheStats,
    ) -> Result<FileCacheEntry, Error> {
        match self.get(file, location, stats) {
            Some(entry) => Ok(entry),
            None => {
                let block = file.read_block(location)?;
                let entry = FileCacheEntry::from_read(block, location, compression)?;
                self.insert(file.file_id(), location.offset, entry.clone());
                Ok(entry)
            }
        }
    }
}
