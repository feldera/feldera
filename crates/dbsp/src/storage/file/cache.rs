//! Caching layer for layer files.
//!
//! This implements an approximately LRU cache for layer files.  The
//! [`Reader`](super::reader::Reader) and [writer](super::writer) use it.
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
    format::{BlockHeader, FileTrailer, FILE_TRAILER_BLOCK_MAGIC},
    reader::{CorruptionError, Error},
};

/// Buffer cache type for [`Reader`](super::reader::Reader) and
/// [`Writer`](super::writer::Writer1).
pub type FileCache = BufferCache<dyn CacheEntry>;

pub(super) fn decompress(
    raw: Arc<FBuf>,
    location: BlockLocation,
    compression: Option<Compression>,
) -> Result<Arc<FBuf>, Error> {
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
                    Err(error) => return Err(CorruptionError::Snappy { location, error }.into()),
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
    Ok(raw)
}

pub(super) fn from_read(
    raw: Arc<FBuf>,
    location: BlockLocation,
    compression: Option<Compression>,
) -> Result<Arc<dyn CacheEntry>, Error> {
    let decompressed = decompress(raw, location, compression)?;
    from_write(decompressed, location)
}

pub(super) fn from_write(
    raw: Arc<FBuf>,
    location: BlockLocation,
) -> Result<Arc<dyn CacheEntry>, Error> {
    let block_header = BlockHeader::read_le(&mut io::Cursor::new(raw.as_slice()))?;
    match block_header.magic {
        //DATA_BLOCK_MAGIC => Ok(Arc::new(InnerDataBlock::from_raw(raw, location)?)),
        //INDEX_BLOCK_MAGIC => Ok(Arc::new(InnerIndexBlock::from_raw(raw, location)?)),
        FILE_TRAILER_BLOCK_MAGIC => Ok(Arc::new(FileTrailer::read_le(&mut io::Cursor::new(
            raw.as_slice(),
        ))?)),
        _ => Err(Error::Corruption(CorruptionError::BadBlockType(location))),
    }
}

impl BufferCache<dyn CacheEntry> {
    fn get_entry(
        &self,
        file: &dyn FileReader,
        location: BlockLocation,
        compression: Option<Compression>,
        stats: &AtomicCacheStats,
    ) -> Result<Arc<dyn CacheEntry>, Error> {
        match self.get(file, location, stats) {
            Some(entry) => Ok(entry),
            None => {
                let block = file.read_block(location)?;
                let entry = from_read(block, location, compression)?;
                self.insert(file.file_id(), location.offset, entry.clone());
                Ok(entry)
            }
        }
    }

    /// Reads `location` from `file` and returns it converted to `FileTrailer`.
    pub(super) fn read_file_trailer_block(
        &self,
        file: &dyn FileReader,
        location: BlockLocation,
        stats: &AtomicCacheStats,
    ) -> Result<Arc<FileTrailer>, Error> {
        let entry = self.get_entry(file, location, None, stats)?.as_any();
        Arc::downcast(entry).map_err(|_| Error::Corruption(CorruptionError::BadBlockType(location)))
    }
}
