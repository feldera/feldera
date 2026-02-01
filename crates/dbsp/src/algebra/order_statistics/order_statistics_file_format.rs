//! Block-based file format for OrderStatisticsMultiset leaf storage.
//!
//! This module defines the on-disk format for spilled leaf nodes. The format follows
//! DBSP conventions:
//! - 512-byte block alignment
//! - CRC32C checksums for data integrity
//! - rkyv serialization for efficient storage
//!
//! # File Structure
//!
//! ```text
//! ┌────────────────────────────────────────────────────────────┐
//! │ File Header (512 bytes)                                    │
//! │   checksum: u32 (CRC32C of bytes [4..])                   │
//! │   magic: [u8; 4] = "OSML"                                 │
//! │   version: u32                                             │
//! │   num_leaves: u64                                          │
//! │   index_offset: u64                                        │
//! │   reserved: [u8; ...]                                      │
//! ├────────────────────────────────────────────────────────────┤
//! │ Data Blocks (512-byte aligned)                             │
//! │   Block 0: checksum(4) + magic(4) + leaf_id(8) + data...  │
//! │   Block 1: ...                                             │
//! │   ...                                                      │
//! ├────────────────────────────────────────────────────────────┤
//! │ Index Block (at index_offset)                              │
//! │   checksum: u32                                            │
//! │   magic: [u8; 4] = "OSMI"                                 │
//! │   num_entries: u64                                         │
//! │   Array of (leaf_id: u64, offset: u64, size: u32)         │
//! └────────────────────────────────────────────────────────────┘
//! ```
//!
//! See `order_statistics_node_storage_plan.md` for the full design.

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use size_of::SizeOf;

/// Block alignment for all disk I/O (512 bytes).
pub const BLOCK_ALIGNMENT: usize = 512;

/// File header size (one block).
pub const FILE_HEADER_SIZE: usize = BLOCK_ALIGNMENT;

/// Data block header size (checksum + magic + leaf_id + data_len).
/// Layout: [checksum:4][magic:4][leaf_id:8][data_len:8] = 24 bytes
pub const DATA_BLOCK_HEADER_SIZE: usize = 4 + 4 + 8 + 8; // 24 bytes

/// Index entry size (leaf_id + offset + size).
pub const INDEX_ENTRY_SIZE: usize = 8 + 8 + 4; // 20 bytes

/// File format version.
pub const FORMAT_VERSION: u32 = 1;

// =============================================================================
// Magic Numbers
// =============================================================================

/// Magic number for file header: "OSML" (Order Statistics Multiset Leaf file)
pub const MAGIC_FILE_HEADER: [u8; 4] = *b"OSML";

/// Magic number for data blocks: "OSMD" (OSM Data block)
pub const MAGIC_DATA_BLOCK: [u8; 4] = *b"OSMD";

/// Magic number for index block: "OSMI" (OSM Index block)
pub const MAGIC_INDEX_BLOCK: [u8; 4] = *b"OSMI";

// =============================================================================
// Block Location
// =============================================================================

/// Location of a block within the leaf file.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    Hash,
    SizeOf,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[archive(check_bytes)]
pub struct BlockLocation {
    /// Byte offset from start of file
    pub offset: u64,
    /// Size of the block in bytes (always 512-byte aligned)
    pub size: u32,
}

impl BlockLocation {
    /// Create a new block location.
    pub fn new(offset: u64, size: u32) -> Self {
        Self { offset, size }
    }

    /// Get the offset after this block.
    pub fn after(&self) -> u64 {
        self.offset + self.size as u64
    }
}

// =============================================================================
// File Header
// =============================================================================

/// File header structure (512 bytes).
///
/// Layout:
/// - bytes [0..4]: CRC32C checksum of bytes [4..512]
/// - bytes [4..8]: magic "OSML"
/// - bytes [8..12]: version (u32 LE)
/// - bytes [12..20]: num_leaves (u64 LE)
/// - bytes [20..28]: index_offset (u64 LE)
/// - bytes [28..36]: total_entries (u64 LE)
/// - bytes [36..44]: total_weight (i64 LE)
/// - bytes [44..512]: reserved (zeros)
#[derive(Clone, Debug)]
pub struct FileHeader {
    /// Number of leaves stored in this file
    pub num_leaves: u64,
    /// Byte offset of the index block
    pub index_offset: u64,
    /// Total entries across all leaves
    pub total_entries: u64,
    /// Total weight across all leaves
    pub total_weight: i64,
}

impl Default for FileHeader {
    fn default() -> Self {
        Self {
            num_leaves: 0,
            index_offset: 0,
            total_entries: 0,
            total_weight: 0,
        }
    }
}

impl FileHeader {
    /// Serialize the header to a 512-byte block with checksum.
    pub fn to_bytes(&self) -> [u8; FILE_HEADER_SIZE] {
        let mut block = [0u8; FILE_HEADER_SIZE];

        // Leave bytes [0..4] for checksum (computed at end)
        // Magic
        block[4..8].copy_from_slice(&MAGIC_FILE_HEADER);
        // Version
        block[8..12].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
        // num_leaves
        block[12..20].copy_from_slice(&self.num_leaves.to_le_bytes());
        // index_offset
        block[20..28].copy_from_slice(&self.index_offset.to_le_bytes());
        // total_entries
        block[28..36].copy_from_slice(&self.total_entries.to_le_bytes());
        // total_weight
        block[36..44].copy_from_slice(&self.total_weight.to_le_bytes());

        // Compute checksum of bytes [4..512]
        let checksum = crc32c::crc32c(&block[4..]);
        block[0..4].copy_from_slice(&checksum.to_le_bytes());

        block
    }

    /// Parse a header from a 512-byte block, verifying the checksum.
    pub fn from_bytes(block: &[u8; FILE_HEADER_SIZE]) -> Result<Self, FileFormatError> {
        // Verify checksum
        let stored_checksum = u32::from_le_bytes(block[0..4].try_into().unwrap());
        let computed_checksum = crc32c::crc32c(&block[4..]);
        if stored_checksum != computed_checksum {
            return Err(FileFormatError::ChecksumMismatch {
                expected: computed_checksum,
                found: stored_checksum,
            });
        }

        // Verify magic
        if &block[4..8] != &MAGIC_FILE_HEADER {
            return Err(FileFormatError::InvalidMagic {
                expected: MAGIC_FILE_HEADER,
                found: [block[4], block[5], block[6], block[7]],
            });
        }

        // Verify version
        let version = u32::from_le_bytes(block[8..12].try_into().unwrap());
        if version != FORMAT_VERSION {
            return Err(FileFormatError::UnsupportedVersion {
                expected: FORMAT_VERSION,
                found: version,
            });
        }

        // Parse fields
        let num_leaves = u64::from_le_bytes(block[12..20].try_into().unwrap());
        let index_offset = u64::from_le_bytes(block[20..28].try_into().unwrap());
        let total_entries = u64::from_le_bytes(block[28..36].try_into().unwrap());
        let total_weight = i64::from_le_bytes(block[36..44].try_into().unwrap());

        Ok(Self {
            num_leaves,
            index_offset,
            total_entries,
            total_weight,
        })
    }
}

// =============================================================================
// Index Entry
// =============================================================================

/// Entry in the leaf index.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Archive, RkyvSerialize, RkyvDeserialize)]
#[archive(check_bytes)]
pub struct IndexEntry {
    /// Unique leaf ID
    pub leaf_id: u64,
    /// Location of the data block
    pub location: BlockLocation,
}

impl IndexEntry {
    /// Create a new index entry.
    pub fn new(leaf_id: u64, offset: u64, size: u32) -> Self {
        Self {
            leaf_id,
            location: BlockLocation::new(offset, size),
        }
    }

    /// Serialize to bytes (20 bytes).
    pub fn to_bytes(&self) -> [u8; INDEX_ENTRY_SIZE] {
        let mut bytes = [0u8; INDEX_ENTRY_SIZE];
        bytes[0..8].copy_from_slice(&self.leaf_id.to_le_bytes());
        bytes[8..16].copy_from_slice(&self.location.offset.to_le_bytes());
        bytes[16..20].copy_from_slice(&self.location.size.to_le_bytes());
        bytes
    }

    /// Parse from bytes.
    pub fn from_bytes(bytes: &[u8]) -> Self {
        let leaf_id = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let offset = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        let size = u32::from_le_bytes(bytes[16..20].try_into().unwrap());
        Self::new(leaf_id, offset, size)
    }
}

// =============================================================================
// Errors
// =============================================================================

/// Errors that can occur during file format operations.
#[derive(Debug, Clone)]
pub enum FileFormatError {
    /// CRC32C checksum verification failed
    ChecksumMismatch { expected: u32, found: u32 },
    /// Invalid magic number
    InvalidMagic { expected: [u8; 4], found: [u8; 4] },
    /// Unsupported file format version
    UnsupportedVersion { expected: u32, found: u32 },
    /// I/O error
    Io(String),
    /// Serialization error
    Serialization(String),
    /// Block not found in index
    BlockNotFound { leaf_id: u64 },
}

impl std::fmt::Display for FileFormatError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FileFormatError::ChecksumMismatch { expected, found } => {
                write!(
                    f,
                    "checksum mismatch: expected {:#x}, found {:#x}",
                    expected, found
                )
            }
            FileFormatError::InvalidMagic { expected, found } => {
                write!(
                    f,
                    "invalid magic: expected {:?}, found {:?}",
                    expected, found
                )
            }
            FileFormatError::UnsupportedVersion { expected, found } => {
                write!(
                    f,
                    "unsupported version: expected {}, found {}",
                    expected, found
                )
            }
            FileFormatError::Io(msg) => write!(f, "I/O error: {}", msg),
            FileFormatError::Serialization(msg) => write!(f, "serialization error: {}", msg),
            FileFormatError::BlockNotFound { leaf_id } => {
                write!(f, "block not found for leaf_id {}", leaf_id)
            }
        }
    }
}

impl std::error::Error for FileFormatError {}

// =============================================================================
// Utility Functions
// =============================================================================

/// Round a size up to the next 512-byte boundary.
#[inline]
pub fn align_to_block(size: usize) -> usize {
    (size + BLOCK_ALIGNMENT - 1) & !(BLOCK_ALIGNMENT - 1)
}

/// Create a data block header.
///
/// Returns a 16-byte header: checksum(4) + magic(4) + leaf_id(8)
/// The checksum should be computed after the full block is assembled.
pub fn create_data_block_header(leaf_id: u64, data_len: u64) -> [u8; DATA_BLOCK_HEADER_SIZE] {
    let mut header = [0u8; DATA_BLOCK_HEADER_SIZE];
    // bytes [0..4] reserved for checksum (computed later over entire block)
    header[4..8].copy_from_slice(&MAGIC_DATA_BLOCK);
    header[8..16].copy_from_slice(&leaf_id.to_le_bytes());
    header[16..24].copy_from_slice(&data_len.to_le_bytes());
    header
}

/// Verify a data block header and extract the leaf_id and data_len.
///
/// Returns `(leaf_id, data_len)` where `data_len` is the actual serialized data length
/// (before 512-byte alignment padding).
pub fn verify_data_block_header(block: &[u8]) -> Result<(u64, u64), FileFormatError> {
    if block.len() < DATA_BLOCK_HEADER_SIZE {
        return Err(FileFormatError::Io("block too small".to_string()));
    }

    // Verify checksum
    let stored_checksum = u32::from_le_bytes(block[0..4].try_into().unwrap());
    let computed_checksum = crc32c::crc32c(&block[4..]);
    if stored_checksum != computed_checksum {
        return Err(FileFormatError::ChecksumMismatch {
            expected: computed_checksum,
            found: stored_checksum,
        });
    }

    // Verify magic
    if &block[4..8] != &MAGIC_DATA_BLOCK {
        return Err(FileFormatError::InvalidMagic {
            expected: MAGIC_DATA_BLOCK,
            found: [block[4], block[5], block[6], block[7]],
        });
    }

    // Extract leaf_id and data_len
    let leaf_id = u64::from_le_bytes(block[8..16].try_into().unwrap());
    let data_len = u64::from_le_bytes(block[16..24].try_into().unwrap());
    Ok((leaf_id, data_len))
}

/// Set the checksum in a block (first 4 bytes).
pub fn set_block_checksum(block: &mut [u8]) {
    let checksum = crc32c::crc32c(&block[4..]);
    block[0..4].copy_from_slice(&checksum.to_le_bytes());
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_alignment() {
        assert_eq!(align_to_block(0), 0);
        assert_eq!(align_to_block(1), 512);
        assert_eq!(align_to_block(511), 512);
        assert_eq!(align_to_block(512), 512);
        assert_eq!(align_to_block(513), 1024);
        assert_eq!(align_to_block(1024), 1024);
    }

    #[test]
    fn test_file_header_roundtrip() {
        let header = FileHeader {
            num_leaves: 42,
            index_offset: 8192,
            total_entries: 1000,
            total_weight: -500,
        };

        let bytes = header.to_bytes();
        assert_eq!(bytes.len(), FILE_HEADER_SIZE);

        let parsed = FileHeader::from_bytes(&bytes).unwrap();
        assert_eq!(parsed.num_leaves, header.num_leaves);
        assert_eq!(parsed.index_offset, header.index_offset);
        assert_eq!(parsed.total_entries, header.total_entries);
        assert_eq!(parsed.total_weight, header.total_weight);
    }

    #[test]
    fn test_file_header_checksum_validation() {
        let header = FileHeader::default();
        let mut bytes = header.to_bytes();

        // Corrupt a byte
        bytes[20] ^= 0xFF;

        let result = FileHeader::from_bytes(&bytes);
        assert!(matches!(
            result,
            Err(FileFormatError::ChecksumMismatch { .. })
        ));
    }

    #[test]
    fn test_file_header_magic_validation() {
        let header = FileHeader::default();
        let mut bytes = header.to_bytes();

        // Corrupt magic
        bytes[4] = b'X';
        // Recompute checksum so it passes checksum validation
        set_block_checksum(&mut bytes);

        let result = FileHeader::from_bytes(&bytes);
        assert!(matches!(result, Err(FileFormatError::InvalidMagic { .. })));
    }

    #[test]
    fn test_index_entry_roundtrip() {
        let entry = IndexEntry::new(123, 4096, 1024);
        let bytes = entry.to_bytes();
        assert_eq!(bytes.len(), INDEX_ENTRY_SIZE);

        let parsed = IndexEntry::from_bytes(&bytes);
        assert_eq!(parsed.leaf_id, entry.leaf_id);
        assert_eq!(parsed.location.offset, entry.location.offset);
        assert_eq!(parsed.location.size, entry.location.size);
    }

    #[test]
    fn test_block_location() {
        let loc = BlockLocation::new(1024, 512);
        assert_eq!(loc.offset, 1024);
        assert_eq!(loc.size, 512);
        assert_eq!(loc.after(), 1536);
    }

    #[test]
    fn test_data_block_header() {
        let leaf_id = 42u64;
        let data_len = 128u64;
        let header = create_data_block_header(leaf_id, data_len);

        // Simulate adding data and computing checksum
        let mut block = vec![0u8; 512];
        block[..DATA_BLOCK_HEADER_SIZE].copy_from_slice(&header);
        set_block_checksum(&mut block);

        // Verify
        let (parsed_leaf_id, parsed_data_len) = verify_data_block_header(&block).unwrap();
        assert_eq!(parsed_leaf_id, leaf_id);
        assert_eq!(parsed_data_len, data_len);
    }

    #[test]
    fn test_data_block_checksum_validation() {
        let leaf_id = 42u64;
        let data_len = 64u64;
        let header = create_data_block_header(leaf_id, data_len);

        let mut block = vec![0u8; 512];
        block[..DATA_BLOCK_HEADER_SIZE].copy_from_slice(&header);
        set_block_checksum(&mut block);

        // Corrupt data
        block[100] ^= 0xFF;

        let result = verify_data_block_header(&block);
        assert!(matches!(
            result,
            Err(FileFormatError::ChecksumMismatch { .. })
        ));
    }
}
