//! Layer file reader.
//!
//! [`Reader`] is the top-level interface for reading layer files.

use super::format::{Compression, FileTrailer};
use super::{AnyFactories, Factories};
use crate::dynamic::{DynVec, WeightTrait};
use crate::storage::buffer_cache::{CacheAccess, CacheEntry};
use crate::storage::file::format::FilterBlock;
use crate::storage::{
    backend::StorageError,
    buffer_cache::{BufferCache, FBuf},
    file::format::{
        DataBlockHeader, FileTrailerColumn, IndexBlockHeader, NodeType, Varint, VERSION_NUMBER,
    },
    file::item::ArchivedItem,
};
use crate::{
    dynamic::{DataTrait, DeserializeDyn, Factory},
    storage::{
        backend::{BlockLocation, FileReader, InvalidBlockLocation, StorageBackend},
        buffer_cache::{AtomicCacheStats, CacheStats},
    },
};
use binrw::{
    io::{self},
    BinRead,
};
use crc32c::crc32c;
use fastbloom::BloomFilter;
use feldera_storage::file::FileId;
use feldera_storage::StoragePath;
use smallvec::SmallVec;
use snap::raw::{decompress_len, Decoder};
use std::mem::replace;
use std::ops::Index;
use std::time::Instant;
use std::{
    cmp::{
        max, min,
        Ordering::{self, *},
    },
    fmt::{Debug, Formatter, Result as FmtResult},
    marker::PhantomData,
    mem::size_of,
    ops::{Bound, Range, RangeBounds},
    sync::Arc,
};
use thiserror::Error as ThisError;
use tracing::{info, warn};

mod bulk_rows;
pub use bulk_rows::BulkRows;

mod fetch_zset;
pub use fetch_zset::FetchZSet;

mod fetch_indexed_zset;
pub use fetch_indexed_zset::FetchIndexedZSet;
/// Any kind of error encountered reading a layer file.
#[derive(ThisError, Debug)]
pub enum Error {
    /// Errors that indicate a problem with the layer file contents.
    #[error("Corrupt layer file: {0}")]
    Corruption(#[from] CorruptionError),

    /// Errors reading the layer file.
    #[error("Error accessing storage: {0}")]
    Storage(#[from] StorageError),

    /// File has unexpected number of columns.
    #[error("File has {actual} column(s) but should have {expected}.")]
    WrongNumberOfColumns {
        /// Number of columns in file.
        actual: usize,
        /// Expected number of columns in file.
        expected: usize,
    },

    /// The invocation is not supported.
    #[error("The requested operation is not supported.")]
    Unsupported,
}

impl From<io::Error> for Error {
    fn from(source: io::Error) -> Self {
        Error::Storage(StorageError::StdIo(source.kind()))
    }
}

/// Errors that indicate a problem with the layer file contents.
#[derive(ThisError, Clone, Debug)]
pub enum CorruptionError {
    /// File size must be a positive multiple of 512.
    #[error("File size {0} must be a positive multiple of 512")]
    InvalidFileSize(
        /// Actual file size.
        u64,
    ),

    /// Block has invalid checksum.
    #[error(
        "Block ({location}) with magic {magic:?} has invalid checksum {checksum:#x} (expected {computed_checksum:#x})"
    )]
    InvalidChecksum {
        /// Block location
        location: BlockLocation,
        /// Block magic,
        magic: [u8; 4],
        /// Checksum in block.
        checksum: u32,
        /// Checksum that block should have.
        computed_checksum: u32,
    },

    /// Invalid version number in file trailer.
    #[error("File has invalid version {version} (expected {expected_version})")]
    InvalidVersion {
        /// Version in file.
        version: u32,
        /// Expected version ([`VERSION_NUMBER`]).
        expected_version: u32,
    },

    /// Invalid version number in file trailer.
    #[error("File uses unsupported incompatible features {0:#x}")]
    UnsupportedIncompatibleFeatures(
        /// Unsupported incompatible features
        u64,
    ),

    /// [`mod@binrw`] reported a format violation.
    #[error("Binary read/write error reading {block_type} block ({location}): {inner}")]
    Binrw {
        /// Block location.
        location: BlockLocation,

        /// Block type.
        block_type: &'static str,

        /// Underlying error.
        inner: String,
    },

    /// Array overflows block bounds.
    #[error("{count}-element array of {each}-byte elements starting at offset {offset} within block overflows {block_size}-byte block")]
    InvalidArray {
        /// Block size.
        block_size: usize,
        /// Starting byte offset in block.
        offset: usize,
        /// Number of array elements.
        count: usize,
        /// Array element size.
        each: usize,
    },

    /// Strides overflow block bounds.
    #[error("{count} strides of {stride} bytes each starting at offset {start} overflows {block_size}-byte block")]
    InvalidStride {
        /// Block size.
        block_size: usize,
        /// Starting byte offset in block.
        start: usize,
        /// Size of each stride in bytes.
        stride: usize,
        /// Number of strides.
        count: usize,
    },

    /// Index is too deep.
    #[error("Index nesting depth {depth} exceeds maximum ({max_depth}).")]
    TooDeep {
        /// Depth.
        depth: usize,
        /// Maximum depth.
        max_depth: usize,
    },

    /// File has no columns.
    #[error("File has no columns.")]
    NoColumns,

    /// Index block has no children.
    #[error("Index block ({0}) is empty")]
    EmptyIndex(BlockLocation),

    /// Data block contains unexpected rows.
    #[error("Data block ({location}) contains rows {rows:?} but {expected_rows:?} were expected.")]
    DataBlockWrongRows {
        /// Block location.
        location: BlockLocation,
        /// Rows actually in block.
        rows: Range<u64>,
        /// Expected rows in block.
        expected_rows: Range<u64>,
    },

    /// Index block requires unexpected number of rows.
    #[error("Index block ({location}) contains {n_rows} rows but {expected_rows} were expected.")]
    IndexBlockWrongNumberOfRows {
        /// Block location.
        location: BlockLocation,
        /// Number of rows in block.
        n_rows: u64,
        /// Expected number of rows in block.
        expected_rows: u64,
    },

    /// Index row totals aren't strictly increasing.
    #[error("Index block ({location}) has nonmonotonic row totals ({prev} then {next}).")]
    NonmonotonicIndex {
        /// Block location.
        location: BlockLocation,
        /// Previous row total.
        prev: u64,
        /// Next row total (which should be bigger than `prev`).
        next: u64,
    },

    /// Each column must have at least at many rows as the previous, that is, we
    /// should have `prev_n_rows <= this_n_rows`.
    #[error(
        "Column {column} has fewer rows ({this_n_rows}) than the previous column ({prev_n_rows})."
    )]
    DecreasingRowCount {
        /// 0-based column index.
        column: usize,
        /// Number of rows in `column`.
        this_n_rows: u64,
        /// Number of rows in `column - 1`.
        prev_n_rows: u64,
    },

    /// Row should be present but isn't.
    #[error("Unexpectedly missing row {0} in column 1 (or later)")]
    MissingRow(
        /// Row number of missing row.
        u64,
    ),

    /// Invalid child in index block.  At least one of `child_offset` or
    /// `child_size` is invalid.
    #[error("Index block ({location}) has child {index} with invalid offset {child_offset} or size {child_size}.")]
    InvalidChild {
        /// Block location.
        location: BlockLocation,
        /// Index of child within block.
        index: usize,
        /// Child offset.
        child_offset: u64,
        /// Child size.
        child_size: usize,
    },

    /// Invalid node pointer in file trailer block.
    #[error("File trailer column specification has invalid node offset {node_offset} or size {node_size}.")]
    InvalidColumnRoot {
        /// Block offset in bytes.
        node_offset: u64,
        /// Block size in bytes.
        node_size: u32,
    },

    /// Invalid row group in data block.
    #[error("Row group {index} in data block ({location}) has invalid row range {start}..{end}.")]
    InvalidRowGroup {
        /// Block location.
        location: BlockLocation,
        /// Row group index inside block.
        index: usize,
        /// Row number of start of range.
        start: u64,
        /// Row number of end of range (exclusive).
        end: u64,
    },

    /// Bad block type.
    #[error("Block ({0}) is wrong type of block.")]
    BadBlockType(BlockLocation),

    /// Bad compressed length.
    #[error("Compressed block ({location}) claims compressed length {compressed_len} but at most {max_compressed_len} would fit.")]
    BadCompressedLen {
        /// Block location.
        location: BlockLocation,
        /// Compressed length.
        compressed_len: usize,
        /// Maximum compressed length.
        max_compressed_len: usize,
    },

    /// Unexpected decompressed length.
    #[error("Compressed block ({location}) decompressed to {length} bytes instead of the expected {expected_length} bytes")]
    UnexpectedDecompressionLength {
        /// Block location.
        location: BlockLocation,
        /// Actual length.
        length: usize,
        /// Expected length.
        expected_length: usize,
    },

    /// Snappy decompression failed.
    #[error("Compressed block ({location}) failed Snappy decompression: {error}.")]
    Snappy {
        /// Block location.
        location: BlockLocation,
        /// Snappy error.
        error: snap::Error,
    },

    /// Multiple paths to block.
    #[error("Multiple paths to block ({0}).")]
    MultiplePaths(BlockLocation),

    /// Invalid filter block location.
    #[error("Invalid file block location ({0}).")]
    InvalidFilterLocation(InvalidBlockLocation),
}

#[derive(Clone)]
struct VarintReader {
    varint: Varint,
    start: usize,
    count: usize,
}

impl VarintReader {
    fn new(buf: &FBuf, varint: Varint, start: usize, count: usize) -> Result<Self, Error> {
        let block_size = buf.len();
        match varint
            .len()
            .checked_mul(count)
            .and_then(|len| len.checked_add(start))
        {
            Some(end) if end <= block_size => Ok(Self {
                varint,
                start,
                count,
            }),
            _ => Err(CorruptionError::InvalidArray {
                block_size,
                offset: start,
                count,
                each: varint.len(),
            }
            .into()),
        }
    }
    fn new_opt(
        buf: &FBuf,
        varint: Option<Varint>,
        start: usize,
        count: usize,
    ) -> Result<Option<Self>, Error> {
        varint
            .map(|varint| VarintReader::new(buf, varint, start, count))
            .transpose()
    }
    fn get(&self, src: &FBuf, index: usize) -> u64 {
        debug_assert!(index < self.count);
        self.varint.get(src, self.start + self.varint.len() * index)
    }
}

#[derive(Clone)]
struct StrideReader {
    start: usize,
    stride: usize,
    count: usize,
}

impl StrideReader {
    fn new(raw: &FBuf, start: usize, stride: usize, count: usize) -> Result<Self, Error> {
        let block_size = raw.len();
        if count > 0 {
            if let Some(last) = stride
                .checked_mul(count - 1)
                .and_then(|len| len.checked_add(start))
            {
                if last < block_size {
                    return Ok(Self {
                        start,
                        stride,
                        count,
                    });
                }
            }
        }
        Err(CorruptionError::InvalidStride {
            block_size,
            start,
            stride,
            count,
        }
        .into())
    }
    fn get(&self, index: usize) -> usize {
        debug_assert!(index < self.count);
        self.start + index * self.stride
    }
}

#[derive(Clone)]
enum ValueMapReader {
    VarintMap(VarintReader),
    StrideMap(StrideReader),
}

impl ValueMapReader {
    fn new(raw: &FBuf, varint: Option<Varint>, offset: u32, n_values: u32) -> Result<Self, Error> {
        let offset = offset as usize;
        let n_values = n_values as usize;
        if let Some(varint) = varint {
            Ok(Self::VarintMap(VarintReader::new(
                raw, varint, offset, n_values,
            )?))
        } else {
            let stride_map = VarintReader::new(raw, Varint::B32, offset, 2)?;
            let start = stride_map.get(raw, 0) as usize;
            let stride = stride_map.get(raw, 1) as usize;
            Ok(Self::StrideMap(StrideReader::new(
                raw, start, stride, n_values,
            )?))
        }
    }
    fn len(&self) -> usize {
        match self {
            ValueMapReader::VarintMap(ref varint_reader) => varint_reader.count,
            ValueMapReader::StrideMap(ref stride_reader) => stride_reader.count,
        }
    }
    fn get(&self, raw: &FBuf, index: usize) -> usize {
        match self {
            ValueMapReader::VarintMap(ref varint_reader) => varint_reader.get(raw, index) as usize,
            ValueMapReader::StrideMap(ref stride_reader) => stride_reader.get(index),
        }
    }
}

pub(super) struct DataBlock<K, A>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    location: BlockLocation,
    raw: Arc<FBuf>,
    value_map: ValueMapReader,
    row_groups: Option<VarintReader>,
    first_row: u64,
    _phantom: PhantomData<fn(&K, &A)>,
}

impl<K, A> CacheEntry for DataBlock<K, A>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    fn cost(&self) -> usize {
        size_of::<Self>() + self.raw.capacity()
    }
}

impl<K, A> DataBlock<K, A>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    pub(super) fn from_raw(
        raw: Arc<FBuf>,
        location: BlockLocation,
        first_row: u64,
    ) -> Result<Self, Error> {
        let header =
            DataBlockHeader::read_le(&mut io::Cursor::new(raw.as_slice())).map_err(|e| {
                Error::Corruption(CorruptionError::Binrw {
                    location,
                    block_type: "data",
                    inner: e.to_string(),
                })
            })?;
        Ok(Self {
            location,
            value_map: ValueMapReader::new(
                &raw,
                header.value_map_varint,
                header.value_map_ofs,
                header.n_values,
            )?,
            row_groups: VarintReader::new_opt(
                &raw,
                header.row_group_varint,
                header.row_groups_ofs as usize,
                header.n_values as usize + 1,
            )?,
            raw,
            first_row,
            _phantom: PhantomData,
        })
    }

    pub(super) fn from_raw_with_cache(
        raw: Arc<FBuf>,
        node: &TreeNode,
        cache: &BufferCache,
        file_id: FileId,
    ) -> Result<Arc<Self>, Error> {
        let block = Arc::new(Self::from_raw(raw, node.location, node.rows.start)?);
        cache.insert(file_id, node.location.offset, block.clone());
        Ok(block)
    }

    fn from_cache_entry(
        cache_entry: Arc<dyn CacheEntry>,
        location: BlockLocation,
    ) -> Result<Arc<Self>, Error> {
        cache_entry
            .downcast()
            .ok_or(Error::Corruption(CorruptionError::BadBlockType(location)))
    }

    fn new(file: &ImmutableFileRef, node: &TreeNode) -> Result<Arc<Self>, Error> {
        let start = Instant::now();
        let cache = (file.cache)();
        #[allow(clippy::borrow_deref_ref)]
        let (access, entry) = match cache.get(&*file.file_handle, node.location) {
            Some(entry) => (
                CacheAccess::Hit,
                Self::from_cache_entry(entry, node.location)?,
            ),
            None => {
                let block = file.read_block(node.location)?;
                let entry =
                    Self::from_raw_with_cache(block, node, &cache, file.file_handle.file_id())?;
                (CacheAccess::Miss, entry)
            }
        };
        file.stats.record(access, start.elapsed(), node.location);

        if entry.rows() != node.rows {
            return Err(CorruptionError::DataBlockWrongRows {
                location: node.location,
                rows: entry.rows(),
                expected_rows: node.rows.clone(),
            }
            .into());
        }

        Ok(entry)
    }

    fn n_values(&self) -> usize {
        self.value_map.len()
    }

    fn rows(&self) -> Range<u64> {
        self.first_row..(self.first_row + self.n_values() as u64)
    }

    fn row_group(&self, index: usize) -> Result<Range<u64>, Error> {
        let row_groups = self.row_groups.as_ref().unwrap();
        let start = row_groups.get(&self.raw, index);
        let end = row_groups.get(&self.raw, index + 1);
        if start < end {
            Ok(start..end)
        } else {
            Err(CorruptionError::InvalidRowGroup {
                location: self.location,
                index,
                start,
                end,
            }
            .into())
        }
    }

    fn row_group_for_row(&self, row: u64) -> Result<Range<u64>, Error> {
        let index = (row - self.first_row) as usize;
        self.row_group(index)
    }

    unsafe fn archived_item(
        &self,
        factories: &Factories<K, A>,
        index: usize,
    ) -> &dyn ArchivedItem<'_, K, A> {
        factories
            .item_factory
            .archived_value(&self.raw, self.value_map.get(&self.raw, index))
    }
    unsafe fn archived_item_for_row(
        &self,
        factories: &Factories<K, A>,
        row: u64,
    ) -> &dyn ArchivedItem<'_, K, A> {
        let index = (row - self.first_row) as usize;

        self.archived_item(factories, index)
    }

    unsafe fn item(&self, factories: &Factories<K, A>, index: usize, item: (&mut K, &mut A)) {
        let archived_item = self.archived_item(factories, index);
        DeserializeDyn::deserialize(archived_item.fst(), item.0);
        DeserializeDyn::deserialize(archived_item.snd(), item.1);
    }
    unsafe fn item_for_row(&self, factories: &Factories<K, A>, row: u64, item: (&mut K, &mut A)) {
        let index = (row - self.first_row) as usize;
        self.item(factories, index, item)
    }
    unsafe fn key(&self, factories: &Factories<K, A>, index: usize, key: &mut K) {
        let item = self.archived_item(factories, index);
        DeserializeDyn::deserialize(item.fst(), key)
    }
    unsafe fn aux(&self, factories: &Factories<K, A>, index: usize, aux: &mut A) {
        let item = self.archived_item(factories, index);
        DeserializeDyn::deserialize(item.snd(), aux)
    }
    unsafe fn key_for_row(&self, factories: &Factories<K, A>, row: u64, key: &mut K) {
        let index = (row - self.first_row) as usize;
        self.key(factories, index, key)
    }
    unsafe fn aux_for_row(&self, factories: &Factories<K, A>, row: u64, aux: &mut A) {
        let index = (row - self.first_row) as usize;
        self.aux(factories, index, aux)
    }

    unsafe fn find_best_match<C>(
        &self,
        factories: &Factories<K, A>,
        target_rows: &Range<u64>,
        compare: &C,
        bias: Ordering,
    ) -> Option<usize>
    where
        C: Fn(&K) -> Ordering,
    {
        let block_rows = self.rows();
        if block_rows.start >= target_rows.end || block_rows.end <= target_rows.start {
            return None;
        }
        let mut best = None;
        factories.key_factory.with(&mut |key| {
            let mut start = (max(block_rows.start, target_rows.start) - self.first_row) as usize;
            let mut end = (min(block_rows.end, target_rows.end) - self.first_row) as usize;
            while start < end {
                let mid = start.midpoint(end);
                self.key(factories, mid, key);
                let cmp = compare(key);

                match cmp {
                    Less => end = mid,
                    Equal => {
                        best = Some(mid);
                        break;
                    }
                    Greater => start = mid + 1,
                };
                if cmp == bias {
                    best = Some(mid);
                }
            }
        });
        best
    }

    /// Searches this data block for `target` and returns whether it was
    /// successful.
    ///
    /// Maintains `key_stack` and `index_stack` as a cache of the keys along a
    /// binary search through the data block.  When successful, the index of the
    /// child found and its key are at the top of `index_stack` and `key_stack`,
    /// respectively.
    unsafe fn find_with_cache<const N: usize>(
        &self,
        factories: &Factories<K, A>,
        key_stack: &mut DynVec<K>,
        index_stack: &mut SmallVec<[usize; N]>,
        target: &K,
    ) -> bool {
        let mut start = 0;
        let mut end = self.n_values();
        let mut i = 0;
        while start < end {
            let mid = start.midpoint(end);
            if index_stack.get(i) != Some(&mid) {
                index_stack.truncate(i);
                index_stack.push(mid);
                key_stack.truncate(i);
                key_stack.push_with(&mut |key| self.key(factories, mid, key));
            };
            match target.cmp(&key_stack[i]) {
                Less => end = mid,
                Equal => {
                    index_stack.truncate(i + 1);
                    key_stack.truncate(i + 1);
                    return true;
                }
                Greater => start = mid + 1,
            };
            i += 1;
        }
        false
    }

    /// Returns the comparison of the key in `row` using `compare`.
    unsafe fn compare_row<C>(&self, factories: &Factories<K, A>, row: u64, compare: &C) -> Ordering
    where
        C: Fn(&K) -> Ordering,
    {
        let mut ordering = Equal;
        factories.key_factory.with(&mut |key| {
            self.key_for_row(factories, row, key);
            ordering = compare(key);
        });
        ordering
    }
}

fn range_compare<T>(range: &Range<T>, target: T) -> Ordering
where
    T: Ord,
{
    if target < range.start {
        Greater
    } else if target >= range.end {
        Less
    } else {
        Equal
    }
}

/// Metadata for reading an index or data node.
#[derive(Clone, Debug)]
pub(super) struct TreeNode {
    pub location: BlockLocation,
    pub node_type: NodeType,
    pub rows: Range<u64>,
}

impl TreeNode {
    fn read<K, A>(&self, file: &ImmutableFileRef) -> Result<TreeBlock<K, A>, Error>
    where
        K: DataTrait + ?Sized,
        A: DataTrait + ?Sized,
    {
        match self.node_type {
            NodeType::Data => Ok(TreeBlock::Data(DataBlock::new(file, self)?)),
            NodeType::Index => Ok(TreeBlock::Index(IndexBlock::new(file, self)?)),
        }
    }
}

enum TreeBlock<K: DataTrait + ?Sized, A: DataTrait + ?Sized> {
    Data(Arc<DataBlock<K, A>>),
    Index(Arc<IndexBlock<K>>),
}

impl<K, A> TreeBlock<K, A>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    fn from_cache(
        node: &TreeNode,
        cache: &BufferCache,
        file: &dyn FileReader,
    ) -> Result<Option<Self>, Error> {
        match cache.get(file, node.location) {
            Some(cache_entry) => match node.node_type {
                NodeType::Data => Ok(Some(Self::Data(DataBlock::from_cache_entry(
                    cache_entry,
                    node.location,
                )?))),
                NodeType::Index => Ok(Some(Self::Index(IndexBlock::from_cache_entry(
                    cache_entry,
                    node.location,
                )?))),
            },
            None => Ok(None),
        }
    }

    pub(super) fn from_raw_with_cache(
        raw: Arc<FBuf>,
        node: &TreeNode,
        cache: &BufferCache,
        file_id: FileId,
    ) -> Result<Self, Error> {
        match node.node_type {
            NodeType::Data => Ok(Self::Data(DataBlock::from_raw_with_cache(
                raw, node, cache, file_id,
            )?)),
            NodeType::Index => Ok(Self::Index(IndexBlock::from_raw_with_cache(
                raw, node, cache, file_id,
            )?)),
        }
    }

    fn lookup_row(&self, row: u64) -> Result<Option<TreeNode>, Error> {
        match self {
            Self::Data(data_block) => {
                if data_block.rows().contains(&row) {
                    return Ok(None);
                }
            }
            Self::Index(index_block) => {
                if index_block.rows().contains(&row) {
                    return Ok(Some(index_block.get_child_by_row(row)?));
                }
            }
        }
        Err(CorruptionError::MissingRow(row).into())
    }
}

/// Index block.
pub(super) struct IndexBlock<K>
where
    K: DataTrait + ?Sized,
{
    location: BlockLocation,
    raw: Arc<FBuf>,
    child_type: NodeType,
    bounds: VarintReader,
    row_totals: VarintReader,
    child_offsets: VarintReader,
    child_sizes: VarintReader,
    first_row: u64,
    _phantom: PhantomData<K>,
}

impl<K> CacheEntry for IndexBlock<K>
where
    K: DataTrait + ?Sized,
{
    fn cost(&self) -> usize {
        size_of::<Self>() + self.raw.capacity()
    }
}

impl<K> IndexBlock<K>
where
    K: DataTrait + ?Sized,
{
    pub(super) fn from_raw(
        raw: Arc<FBuf>,
        location: BlockLocation,
        first_row: u64,
    ) -> Result<Self, Error> {
        let header =
            IndexBlockHeader::read_le(&mut io::Cursor::new(raw.as_slice())).map_err(|e| {
                Error::Corruption(CorruptionError::Binrw {
                    location,
                    block_type: "index",
                    inner: e.to_string(),
                })
            })?;
        if header.n_children == 0 {
            return Err(CorruptionError::EmptyIndex(location).into());
        }

        let row_totals = VarintReader::new(
            &raw,
            header.row_total_varint,
            header.row_totals_offset as usize,
            header.n_children as usize,
        )?;
        for i in 1..header.n_children as usize {
            let prev = row_totals.get(&raw, i - 1);
            let next = row_totals.get(&raw, i);
            if prev >= next {
                return Err(CorruptionError::NonmonotonicIndex {
                    location,
                    prev,
                    next,
                }
                .into());
            }
        }

        Ok(Self {
            location,
            child_type: header.child_type,
            bounds: VarintReader::new(
                &raw,
                header.bound_map_varint,
                header.bound_map_offset as usize,
                header.n_children as usize * 2,
            )?,
            row_totals,
            child_offsets: VarintReader::new(
                &raw,
                header.child_offset_varint,
                header.child_offsets_offset as usize,
                header.n_children as usize,
            )?,
            child_sizes: VarintReader::new(
                &raw,
                header.child_size_varint,
                header.child_sizes_offset as usize,
                header.n_children as usize,
            )?,
            raw,
            first_row,
            _phantom: PhantomData,
        })
    }

    pub(super) fn from_raw_with_cache(
        raw: Arc<FBuf>,
        node: &TreeNode,
        cache: &BufferCache,
        file_id: FileId,
    ) -> Result<Arc<Self>, Error> {
        let block = Arc::new(Self::from_raw(raw, node.location, node.rows.start)?);
        cache.insert(file_id, node.location.offset, block.clone());
        Ok(block)
    }

    fn from_cache_entry(
        cache_entry: Arc<dyn CacheEntry>,
        location: BlockLocation,
    ) -> Result<Arc<Self>, Error> {
        cache_entry
            .downcast()
            .ok_or(Error::Corruption(CorruptionError::BadBlockType(location)))
    }

    fn new(file: &ImmutableFileRef, node: &TreeNode) -> Result<Arc<Self>, Error> {
        let start = Instant::now();
        let cache = (file.cache)();
        let first_row = node.rows.start;
        #[allow(clippy::borrow_deref_ref)]
        let (access, entry) = match cache.get(&*file.file_handle, node.location) {
            Some(entry) => {
                let entry = Self::from_cache_entry(entry, node.location)?;
                if entry.first_row != first_row {
                    return Err(Error::Corruption(CorruptionError::MultiplePaths(
                        node.location,
                    )));
                }
                (CacheAccess::Hit, entry)
            }
            None => {
                let block = file.read_block(node.location)?;
                let entry =
                    Self::from_raw_with_cache(block, node, &cache, file.file_handle.file_id())?;
                (CacheAccess::Miss, entry)
            }
        };
        file.stats.record(access, start.elapsed(), node.location);

        let expected_rows = node.rows.end - node.rows.start;
        let n_rows = entry.row_totals.get(&entry.raw, entry.row_totals.count - 1);
        if n_rows != expected_rows {
            return Err(CorruptionError::IndexBlockWrongNumberOfRows {
                location: node.location,
                n_rows,
                expected_rows,
            }
            .into());
        }

        Ok(entry)
    }

    /// Returns the range of rows covered by this index block.
    fn rows(&self) -> Range<u64> {
        self.first_row..self.first_row + self.row_totals.get(&self.raw, self.row_totals.count - 1)
    }

    fn get_child_location(&self, index: usize) -> Result<BlockLocation, Error> {
        let offset = self.child_offsets.get(&self.raw, index) << 9;
        let size = self.child_sizes.get(&self.raw, index) << 9;
        BlockLocation::new(offset, size as usize).map_err(|error: InvalidBlockLocation| {
            Error::Corruption(CorruptionError::InvalidChild {
                location: self.location,
                index,
                child_offset: error.offset,
                child_size: error.size,
            })
        })
    }

    fn get_child(&self, index: usize) -> Result<TreeNode, Error> {
        Ok(TreeNode {
            location: self.get_child_location(index)?,
            node_type: self.child_type,
            rows: self.get_rows(index),
        })
    }

    fn get_child_by_row(&self, row: u64) -> Result<TreeNode, Error> {
        self.get_child(self.find_row(row)?)
    }

    fn get_rows(&self, index: usize) -> Range<u64> {
        let low = if index == 0 {
            0
        } else {
            self.row_totals.get(&self.raw, index - 1)
        };
        let high = self.row_totals.get(&self.raw, index);
        (self.first_row + low)..(self.first_row + high)
    }

    fn get_row_bound(&self, index: usize) -> u64 {
        if index == 0 {
            0
        } else if index % 2 == 1 {
            self.row_totals.get(&self.raw, index / 2) - 1
        } else {
            self.row_totals.get(&self.raw, index / 2 - 1)
        }
    }

    fn find_row(&self, row: u64) -> Result<usize, Error> {
        let mut indexes = 0..self.n_children();
        while !indexes.is_empty() {
            let mid = indexes.start.midpoint(indexes.end);
            let rows = self.get_rows(mid);
            if row < rows.start {
                indexes.end = mid;
            } else if row >= rows.end {
                indexes.start = mid + 1;
            } else {
                return Ok(mid);
            }
        }
        Err(CorruptionError::MissingRow(row).into())
    }

    unsafe fn get_bound(&self, index: usize, bound: &mut K) {
        let offset = self.bounds.get(&self.raw, index) as usize;
        bound.deserialize_from_bytes(&self.raw, offset)
    }

    unsafe fn find_best_match<C>(
        &self,
        key_factory: &dyn Factory<K>,
        target_rows: &Range<u64>,
        compare: &C,
        bias: Ordering,
    ) -> Option<usize>
    where
        C: Fn(&K) -> Ordering,
    {
        let mut result: Option<usize> = None;

        key_factory.with(&mut |bound| {
            let mut start = 0;
            let mut end = self.n_children() * 2;
            result = None;
            while start < end {
                let mid = start.midpoint(end);
                let row = self.get_row_bound(mid) + self.first_row;
                let cmp = match range_compare(target_rows, row) {
                    Equal => {
                        self.get_bound(mid, bound);
                        let cmp = compare(bound);
                        if cmp == Equal {
                            result = Some(mid / 2);
                            return;
                        }
                        cmp
                    }
                    cmp => cmp,
                };
                if cmp == Less {
                    end = mid
                } else {
                    start = mid + 1
                };
                if bias == cmp {
                    result = Some(mid / 2);
                }
            }
        });

        result
    }

    /// Finds the child `child_index` that contains
    /// `targets[target_indexes.start]`.  If found, some of the following
    /// indexes within `target_indexes` might also be in the same child; this
    /// function counts them as `n_targets` (which is at least 1, counting the
    /// initial target).  Returns `Some((child_index, n_targets))`.
    ///
    /// `tmp_key` and `start` are temporary storage.
    unsafe fn find_next<I>(
        &self,
        targets: &I,
        mut target_indexes: Range<usize>,
        tmp_key: &mut K,
        start: &mut usize,
    ) -> Option<(usize, usize)>
    where
        I: Index<usize, Output = K> + ?Sized,
    {
        let start_index = target_indexes.next().unwrap();
        let mut end = self.n_children();
        while *start < end {
            let mid = start.midpoint(end);
            self.get_bound(mid * 2, tmp_key);
            if &targets[start_index] < tmp_key {
                end = mid;
            } else {
                *start = mid + 1;
                self.get_bound(mid * 2 + 1, tmp_key);
                if &targets[start_index] <= tmp_key {
                    let n = 1 + target_indexes
                        .take_while(|i| &targets[*i] <= tmp_key)
                        .count();
                    return Some((mid, n));
                }
            }
        }
        None
    }

    fn n_children(&self) -> usize {
        self.child_offsets.count
    }

    /// Returns the comparison of the largest bound key` using `compare`.
    unsafe fn compare_max<C>(&self, key_factory: &dyn Factory<K>, compare: &C) -> Ordering
    where
        C: Fn(&K) -> Ordering,
    {
        let mut ordering = Equal;
        key_factory.with(&mut |key| {
            self.get_bound(self.n_children() * 2 - 1, key);
            ordering = compare(key);
        });
        ordering
    }
}

impl<K> Debug for IndexBlock<K>
where
    K: DataTrait + ?Sized + Debug,
{
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(
            f,
            "IndexBlock {{ first_row: {}, child_type: {:?}, children = {{",
            self.first_row, self.child_type
        )?;
        for i in 0..self.n_children() {
            if i > 0 {
                write!(f, ",")?;
            }
            write!(
                f,
                " [{i}] = {{ rows: {:?}, location: {:?} }}",
                self.get_rows(i),
                self.get_child_location(i),
            )?;
        }
        write!(f, " }}")
    }
}

impl CacheEntry for FileTrailer {
    fn cost(&self) -> usize {
        size_of::<FileTrailer>()
    }
}

impl FileTrailer {
    fn from_raw(raw: Arc<FBuf>, location: BlockLocation) -> Result<Self, Error> {
        Self::read_le(&mut io::Cursor::new(raw.as_slice())).map_err(|e| {
            Error::Corruption(CorruptionError::Binrw {
                location,
                block_type: "trailer",
                inner: e.to_string(),
            })
        })
    }
    fn new(
        cache: fn() -> Arc<BufferCache>,
        file_handle: &dyn FileReader,
        location: BlockLocation,
        stats: &AtomicCacheStats,
    ) -> Result<Arc<FileTrailer>, Error> {
        let start = Instant::now();
        let cache = cache();
        #[allow(clippy::borrow_deref_ref)]
        let (access, entry) = match cache.get(&*file_handle, location) {
            Some(entry) => {
                let entry = entry
                    .downcast()
                    .ok_or(Error::Corruption(CorruptionError::BadBlockType(location)))?;
                (CacheAccess::Hit, entry)
            }
            None => {
                let block = file_handle.read_block(location)?;
                let entry = Arc::new(Self::from_raw(block, location)?);
                cache.insert(file_handle.file_id(), location.offset, entry.clone());
                (CacheAccess::Miss, entry)
            }
        };
        stats.record(access, start.elapsed(), location);
        Ok(entry)
    }
}

#[derive(Debug)]
struct Column {
    root: Option<TreeNode>,
    factories: AnyFactories,
    n_rows: u64,
}

impl FilterBlock {
    fn new(file_handle: &dyn FileReader, location: BlockLocation) -> Result<Self, Error> {
        let block = file_handle.read_block(location)?;
        Self::read_le(&mut io::Cursor::new(block.as_slice())).map_err(|e| {
            Error::Corruption(CorruptionError::Binrw {
                location,
                block_type: "filter",
                inner: e.to_string(),
            })
        })
    }
}

impl Column {
    fn new(factories: &AnyFactories, info: &FileTrailerColumn) -> Result<Self, Error> {
        let FileTrailerColumn {
            node_offset,
            node_size,
            node_type,
            n_rows,
        } = *info;
        let root = if n_rows != 0 {
            let location = match BlockLocation::new(node_offset, node_size as usize) {
                Ok(location) => location,
                Err(_) => {
                    return Err(Error::Corruption(CorruptionError::InvalidColumnRoot {
                        node_offset,
                        node_size,
                    }));
                }
            };
            Some(TreeNode {
                location,
                node_type,
                rows: 0..n_rows,
            })
        } else {
            None
        };
        Ok(Self {
            root,
            n_rows,
            factories: factories.clone(),
        })
    }
}

/// Encapsulates storage and a file handle.
struct ImmutableFileRef {
    path: StoragePath,
    cache: fn() -> Arc<BufferCache>,
    file_handle: Arc<dyn FileReader>,
    compression: Option<Compression>,
    stats: AtomicCacheStats,
}

impl Debug for ImmutableFileRef {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("ImmutableFileRef")
            .field("path", &self.path)
            .finish()
    }
}
impl Drop for ImmutableFileRef {
    fn drop(&mut self) {
        if Arc::strong_count(&self.file_handle) == 1 {
            self.evict();
        }
    }
}

impl ImmutableFileRef {
    fn new(
        cache: fn() -> Arc<BufferCache>,
        file_handle: Arc<dyn FileReader>,
        path: StoragePath,
        compression: Option<Compression>,
        stats: AtomicCacheStats,
    ) -> Self {
        Self {
            cache,
            path,
            file_handle,
            compression,
            stats,
        }
    }

    pub fn evict(&self) {
        (self.cache)().evict(&*self.file_handle);
    }

    pub fn read_block(&self, location: BlockLocation) -> Result<Arc<FBuf>, Error> {
        decompress(
            self.compression,
            location,
            self.file_handle.read_block(location)?,
        )
    }
}

fn decompress(
    compression: Option<Compression>,
    location: BlockLocation,
    raw: Arc<FBuf>,
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

/// Layer file column specification.
///
/// A column specification must take the form `K0, A0, N0`, where `(K0, A0)` is
/// the first column's key and auxiliary data types.  If there is only one
/// column, `N0` is `()`; otherwise, it is `(K1, A1, N1)`, where `(K1, A1)` is
/// the second column's key and auxiliary data types.  If there are only two
/// columns, `N1` is `()`, otherwise it is `(K2, A2, N2)`; and so on.  Thus:
///
/// * For one column, `T` is `(K0, A0, ())`.
///
/// * For two columns, `T` is `(K0, A0, (K1, A1, ()))`.
///
/// * For three columns, `T` is `(K0, A0, (K1, A1, (K2, A2, ())))`.
pub trait ColumnSpec {
    /// Returns the number of columns in this `ColumnSpec`.
    fn n_columns() -> usize;
}

impl ColumnSpec for () {
    fn n_columns() -> usize {
        0
    }
}

impl<K, A, N> ColumnSpec for (&'static K, &'static A, N)
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
    N: ColumnSpec,
{
    fn n_columns() -> usize {
        1 + N::n_columns()
    }
}

/// Layer file reader.
///
/// `T` in `Reader<T>` must be a [`ColumnSpec`] that specifies the key and
/// auxiliary data types for all of the columns in the file to be read.
///
/// Use [Reader::rows] to read data.
#[derive(Debug)]
pub struct Reader<T> {
    file: ImmutableFileRef,
    bloom_filter: Option<BloomFilter>,
    columns: Vec<Column>,

    /// `fn() -> T` is `Send` and `Sync` regardless of `T`.  See
    /// <https://doc.rust-lang.org/nomicon/phantom-data.html>.
    _phantom: PhantomData<fn() -> T>,
}

impl<T> Reader<T>
where
    T: ColumnSpec,
{
    /// Creates and returns a new `Reader` for `file`.
    pub(crate) fn new(
        factories: &[&AnyFactories],
        path: StoragePath,
        cache: fn() -> Arc<BufferCache>,
        file_handle: Arc<dyn FileReader>,
        bloom_filter: Option<BloomFilter>,
    ) -> Result<Self, Error> {
        let file_size = file_handle.get_size()?;
        if file_size < 512 || (file_size % 512) != 0 {
            return Err(CorruptionError::InvalidFileSize(file_size).into());
        }

        let stats = AtomicCacheStats::default();
        let file_trailer = FileTrailer::new(
            cache,
            &*file_handle,
            BlockLocation::new(file_size - 512, 512).unwrap(),
            &stats,
        )?;
        let has_compatible_bloom_filter = match file_trailer.version {
            1 => {
                // Old, incompatible version.
                None
            }
            2 => {
                // Version before [fastbloom] crate was upgraded to one with an incompatible format.
                warn!("{path}: reading old format storage file, performance may be reduced due to incompatible Bloom filters");
                Some(false)
            }
            VERSION_NUMBER => Some(true),
            _ => None,
        }
        .ok_or_else(|| CorruptionError::InvalidVersion {
            version: file_trailer.version,
            expected_version: VERSION_NUMBER,
        })?;

        if file_trailer.compatible_features != 0 {
            info!(
                "{path}: storage file uses unsupported compatible features {:#x}",
                file_trailer.compatible_features
            );
        }

        if file_trailer.incompatible_features != 0 {
            return Err(CorruptionError::UnsupportedIncompatibleFeatures(
                file_trailer.incompatible_features,
            )
            .into());
        }

        assert_eq!(factories.len(), file_trailer.columns.len());

        let columns: Vec<_> = file_trailer
            .columns
            .iter()
            .zip(factories.iter())
            .map(|(info, factories)| Column::new(factories, info))
            .collect::<Result<_, _>>()?;
        if columns.is_empty() {
            return Err(CorruptionError::NoColumns.into());
        }
        if columns.len() != T::n_columns() {
            return Err(Error::WrongNumberOfColumns {
                actual: columns.len(),
                expected: T::n_columns(),
            });
        }
        for i in 1..columns.len() {
            let prev_n_rows = columns[i - 1].n_rows;
            let this_n_rows = columns[i].n_rows;
            if this_n_rows < prev_n_rows {
                return Err(CorruptionError::DecreasingRowCount {
                    column: i,
                    prev_n_rows,
                    this_n_rows,
                }
                .into());
            }
        }

        let bloom_filter = match bloom_filter {
            Some(bloom_filter) => Some(bloom_filter),
            None if has_compatible_bloom_filter && file_trailer.filter_offset != 0 => Some(
                FilterBlock::new(
                    &*file_handle,
                    BlockLocation::new(
                        file_trailer.filter_offset,
                        file_trailer.filter_size as usize,
                    )
                    .map_err(|error: InvalidBlockLocation| {
                        Error::Corruption(CorruptionError::InvalidFilterLocation(error))
                    })?,
                )?
                .into(),
            ),
            None => None,
        };

        Ok(Self {
            file: ImmutableFileRef::new(cache, file_handle, path, file_trailer.compression, stats),
            columns,
            bloom_filter,
            _phantom: PhantomData,
        })
    }

    /// Marks the file of the reader as being part of a checkpoint.
    pub fn mark_for_checkpoint(&self) {
        self.file.file_handle.mark_for_checkpoint();
    }

    /// Instantiates a reader given an existing path.
    pub fn open(
        factories: &[&AnyFactories],
        cache: fn() -> Arc<BufferCache>,
        storage_backend: &dyn StorageBackend,
        path: &StoragePath,
    ) -> Result<Self, Error> {
        Self::new(
            factories,
            path.clone(),
            cache,
            storage_backend.open(path)?,
            None,
        )
    }

    /// The number of columns in the layer file.
    ///
    /// This is a fixed value for any given `Reader`.
    pub fn n_columns(&self) -> usize {
        T::n_columns()
    }

    /// The number of rows in the given `column`.
    ///
    /// For column 0, this is the number of rows that may be visited with
    /// [`rows`](Self::rows).  In other columns, it is the number of rows that
    /// may be visited in total by calling `next_column()` on each of the rows
    /// in the previous column.
    pub fn n_rows(&self, column: usize) -> u64 {
        self.columns[column].n_rows
    }

    /// Returns the storage path for the underlying object.
    pub fn path(&self) -> StoragePath {
        self.file.path.clone()
    }

    /// Returns the size of the underlying file in bytes.
    pub fn byte_size(&self) -> Result<u64, Error> {
        Ok(self.file.file_handle.get_size()?)
    }

    /// Evict this file from the cache.
    #[cfg(test)]
    pub fn evict(&self) {
        self.file.evict();
    }

    /// Returns the cache statistics for this file.  The statistics are specific
    /// to this file's cache behavior for reads.
    pub fn cache_stats(&self) -> CacheStats {
        self.file.stats.read()
    }

    /// Returns the `FileReader` embedded in this `Reader`.
    pub fn file_handle(&self) -> &dyn FileReader {
        &*self.file.file_handle
    }
}

impl<K, A, N> Reader<(&'static K, &'static A, N)>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
    (&'static K, &'static A, N): ColumnSpec,
{
    /// Asks the bloom filter of the reader if we have the key.
    pub fn maybe_contains_key(&self, hash: u64) -> bool {
        self.bloom_filter
            .as_ref()
            .is_none_or(|b| b.contains_hash(hash))
    }

    /// Returns a [`RowGroup`] for all of the rows in column 0.
    pub fn rows(&self) -> RowGroup<'_, K, A, N, (&'static K, &'static A, N)> {
        RowGroup::new(self, 0, 0..self.columns[0].n_rows)
    }

    /// Returns a [`BulkRows`] for column 0.
    pub fn bulk_rows(&self) -> Result<BulkRows<'_, K, A, N, (&'static K, &'static A, N)>, Error> {
        BulkRows::new(self, 0)
    }
}

impl<K, A> Reader<(&'static K, &'static A, ())>
where
    K: DataTrait + ?Sized,
    A: WeightTrait + ?Sized,
{
    /// Returns a [`FetchZSet`], which will subset this reader to just the rows
    /// in colunn 0 whose keys are in `keys` (which must be sorted) and return
    /// it as a Z-set, treating auxiliary values as weights.
    pub fn fetch_zset<'a, 'b>(
        &'a self,
        keys: &'b DynVec<K>,
    ) -> Result<FetchZSet<'a, 'b, K, A>, Error> {
        FetchZSet::new(self, keys)
    }
}

impl<K0, A0, K1, A1> Reader<(&'static K0, &'static A0, (&'static K1, &'static A1, ()))>
where
    K0: DataTrait + ?Sized,
    A0: DataTrait + ?Sized,
    K1: DataTrait + ?Sized,
    A1: WeightTrait + ?Sized,
{
    /// Returns a [`FetchIndexedZSet`], which will build an indexed Z-set from
    /// this reader containing just the rows whose keys are in `keys` (which
    /// must be sorted).
    pub fn fetch_indexed_zset<'a, 'b>(
        &'a self,
        keys: &'b DynVec<K0>,
    ) -> Result<FetchIndexedZSet<'a, 'b, K0, A0, K1, A1>, Error> {
        FetchIndexedZSet::new(self, keys)
    }
}

/// A sorted, indexed group of unique rows in a [`Reader`].
///
/// Column 0 in a layer file has a single [`RowGroup`] that includes all of the
/// rows in column 0.  This row group, obtained with [`Reader::rows`], is empty
/// if the layer file is empty.
///
/// Row groups for other columns are obtained by first obtaining a [`Cursor`]
/// for a row in column 0 and calling [`Cursor::next_column`] to get its row
/// group in column 1, and then repeating as many times as necessary to get to
/// the desired column.
pub struct RowGroup<'a, K, A, N, T>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    reader: &'a Reader<T>,
    factories: Factories<K, A>,
    column: usize,
    rows: Range<u64>,
    _phantom: PhantomData<fn(&K, &A, N)>,
}

impl<K, A, N, T> Clone for RowGroup<'_, K, A, N, T>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            reader: self.reader,
            factories: self.factories.clone(),
            column: self.column,
            rows: self.rows.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<K, A, N, T> Debug for RowGroup<'_, K, A, N, T>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "RowGroup(column={}, rows={:?})", self.column, self.rows)
    }
}

impl<'a, K, A, N, T> RowGroup<'a, K, A, N, T>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
    T: ColumnSpec,
{
    fn new(reader: &'a Reader<T>, column: usize, rows: Range<u64>) -> Self {
        Self {
            reader,
            factories: reader.columns[column].factories.factories(),
            column,
            rows,
            _phantom: PhantomData,
        }
    }

    fn cursor(&self, position: Position<K, A>) -> Cursor<'a, K, A, N, T> {
        Cursor {
            row_group: self.clone(),
            position,
        }
    }

    /// Returns `true` if the row group contains no rows.
    ///
    /// The row group for column 0 is empty if and only if the layer file is
    /// empty.  A row group obtained from [`Cursor::next_column`] is never
    /// empty.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Returns the number of rows in the row group.
    pub fn len(&self) -> u64 {
        self.rows.end - self.rows.start
    }

    /// Returns a cursor for just before the row group.
    pub fn before(&self) -> Cursor<'a, K, A, N, T> {
        self.cursor(Position::Before)
    }

    /// Return a cursor for just after the row group.
    pub fn after(&self) -> Cursor<'a, K, A, N, T> {
        self.cursor(Position::After { hint: None })
    }

    /// Return a cursor for the first row in the row group, or just after the
    /// row group if it is empty.
    pub fn first(&self) -> Result<Cursor<'a, K, A, N, T>, Error> {
        let position = if self.is_empty() {
            Position::After { hint: None }
        } else {
            Position::for_row(self, self.rows.start)?
        };
        Ok(self.cursor(position))
    }

    /// Return a cursor for the first row in the row group, or just after the
    /// row group if it is empty, using `hint` as an internal starting point for
    /// searching the B-tree. For best performance, use a `hint` near the first
    /// row in the row group (but the result will be correct regardless of
    /// `hint`).
    pub fn first_with_hint(
        &self,
        hint: &Cursor<'a, K, A, N, T>,
    ) -> Result<Cursor<'a, K, A, N, T>, Error> {
        let position = if self.is_empty() {
            Position::After { hint: None }
        } else {
            Position::for_row_from_hint(self, &hint.position, self.rows.start)?
        };
        Ok(self.cursor(position))
    }

    /// Return a cursor for the last row in the row group, or just after the
    /// row group if it is empty.
    pub fn last(&self) -> Result<Cursor<'a, K, A, N, T>, Error> {
        let position = if self.is_empty() {
            Position::After { hint: None }
        } else {
            Position::for_row(self, self.rows.end - 1)?
        };
        Ok(self.cursor(position))
    }

    /// If `row` is less than the number of rows in the row group, returns a
    /// cursor for that row; otherwise, returns a cursor for just after the row
    /// group.
    pub fn nth(&self, row: u64) -> Result<Cursor<'a, K, A, N, T>, Error> {
        let position = if row < self.len() {
            Position::for_row(self, self.rows.start + row)?
        } else {
            Position::After { hint: None }
        };
        Ok(self.cursor(position))
    }

    /// Returns a row group for a subset of the rows in this one.
    pub fn subset<B>(&self, range: B) -> Self
    where
        B: RangeBounds<u64>,
    {
        let start = match range.start_bound() {
            Bound::Included(&index) => index,
            Bound::Excluded(&index) => index + 1,
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(&index) => index + 1,
            Bound::Excluded(&index) => index,
            Bound::Unbounded => self.len(),
        };
        let subset = start..end;

        let start = self.rows.start + subset.start;
        let end = start + (subset.end - subset.start);
        Self {
            rows: start..end,
            factories: self.factories.clone(),
            ..*self
        }
    }
}

/// Trait for equality comparisons that might fail due to an I/O error.
pub trait FallibleEq {
    /// Compares `self` to `other` and returns whether they are equal, with
    /// the possibility of failure due to an I/O error.
    fn equals(&self, other: &Self) -> Result<bool, Error>;
}

impl<K, A, N> FallibleEq for Reader<(&'static K, &'static A, N)>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
    (&'static K, &'static A, N): ColumnSpec,
    for<'a> RowGroup<'a, K, A, N, (&'static K, &'static A, N)>: FallibleEq,
{
    fn equals(&self, other: &Self) -> Result<bool, Error> {
        self.rows().equals(&other.rows())
    }
}

impl<'a, K, A, T> FallibleEq for RowGroup<'a, K, A, (), T>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
    T: ColumnSpec,
{
    fn equals(&self, other: &Self) -> Result<bool, Error> {
        if self.len() != other.len() {
            return Ok(false);
        }
        let mut sc: Cursor<'a, _, _, _, _> = self.clone().first()?;
        let mut oc: Cursor<'a, _, _, _, _> = other.clone().first()?;

        while sc.has_value() {
            if unsafe { sc.archived_item() != oc.archived_item() } {
                return Ok(false);
            }
            sc.move_next()?;
            oc.move_next()?;
        }
        Ok(true)
    }
}

impl<'a, K, A, NK, NA, NN, T> FallibleEq for RowGroup<'a, K, A, (&'static NK, &'static NA, NN), T>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
    NK: DataTrait + ?Sized,
    NA: DataTrait + ?Sized,
    T: ColumnSpec,
    RowGroup<'a, NK, NA, NN, T>: FallibleEq,
{
    fn equals(&self, other: &Self) -> Result<bool, Error> {
        if self.len() != other.len() {
            return Ok(false);
        }
        let mut sc = self.clone().first()?;
        let mut oc = other.clone().first()?;
        while sc.has_value() {
            if unsafe { sc.archived_item() != oc.archived_item() } {
                return Ok(false);
            }
            if !sc.next_column()?.equals(&oc.next_column()?)? {
                return Ok(false);
            }
            sc.move_next()?;
            oc.move_next()?;
        }
        Ok(true)
    }
}

/// A cursor for a layer file.
///
/// A cursor traverses a [`RowGroup`].  It can be positioned on a particular row
/// or before or after the row group.  (If the row group is empty, then the
/// cursor can only be before or after the row group.)
pub struct Cursor<'a, K, A, N, T>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    row_group: RowGroup<'a, K, A, N, T>,
    position: Position<K, A>,
}

impl<K, A, N, T> Clone for Cursor<'_, K, A, N, T>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            row_group: self.row_group.clone(),
            position: self.position.clone(),
        }
    }
}

impl<K, A, N, T> Debug for Cursor<'_, K, A, N, T>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "Cursor({:?}, {:?})", self.row_group, self.position)
    }
}

impl<'a, K, A, N, T> Cursor<'a, K, A, N, T>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
    T: ColumnSpec,
{
    /// Moves to the next row in the row group.  If the cursor was previously
    /// before the row group, it moves to the first row; if it was on the last
    /// row, it moves after the row group.
    pub fn move_next(&mut self) -> Result<(), Error> {
        self.position.next(&self.row_group)?;
        Ok(())
    }

    /// Moves to the previous row in the row group.  If the cursor was
    /// previously after the row group, it moves to the last row; if it was
    /// on the first row, it moves before the row group.
    pub fn move_prev(&mut self) -> Result<(), Error> {
        self.position.prev(&self.row_group)?;
        Ok(())
    }

    /// Moves to the first row in the row group.  If the row group is empty,
    /// this has no effect.
    pub fn move_first(&mut self) -> Result<(), Error> {
        self.position
            .move_to_row(&self.row_group, self.row_group.rows.start)
    }

    /// Moves to the last row in the row group.  If the row group is empty,
    /// this has no effect.
    pub fn move_last(&mut self) -> Result<(), Error> {
        if !self.row_group.is_empty() {
            self.position
                .move_to_row(&self.row_group, self.row_group.rows.end - 1)
        } else {
            self.position = Position::After { hint: None };
            Ok(())
        }
    }

    /// Moves to row `row`.  If `row >= self.len()`, moves after the row group.
    pub fn move_to_row(&mut self, row: u64) -> Result<(), Error> {
        if row < self.row_group.rows.end - self.row_group.rows.start {
            self.position
                .move_to_row(&self.row_group, self.row_group.rows.start + row)
        } else {
            self.position.move_after();
            Ok(())
        }
    }

    /// Returns the key in the current row, or `None` if the cursor is before or
    /// after the row group.
    ///
    /// # Safety
    ///
    /// Unsafe because `rkyv` deserialization is unsafe.
    pub unsafe fn key(&self, key: &'a mut K) -> Option<&'a mut K> {
        self.position.key(&self.row_group.factories, key)
    }

    /// Returns the auxiliary data in the current row, or `None` if the cursor
    /// is before or after the row group.
    ///
    /// # Safety
    ///
    /// Unsafe because `rkyv` deserialization is unsafe.
    pub unsafe fn aux<'b>(&self, aux: &'b mut A) -> Option<&'b mut A> {
        self.position.aux(&self.row_group.factories, aux)
    }

    /// Returns the key and auxiliary data in the current row, or `None` if the
    /// cursor is before or after the row group.
    ///
    /// # Safety
    ///
    /// Unsafe because `rkyv` deserialization is unsafe.
    pub unsafe fn item<'b>(&self, item: (&'b mut K, &'b mut A)) -> Option<(&'b mut K, &'b mut A)> {
        self.position.item(&self.row_group.factories, item)
    }

    /// Returns archived representation of the key and auxiliary data in the
    /// current row, or `None` if the cursor is before or after the row
    /// group.
    ///
    /// # Safety
    ///
    /// Unsafe because `rkyv` deserialization is unsafe.
    pub unsafe fn archived_item(&self) -> Option<&dyn ArchivedItem<'_, K, A>> {
        self.position.archived_item(&self.row_group.factories)
    }

    /// Returns `true` if the cursor is on a row.
    pub fn has_value(&self) -> bool {
        self.position.has_value()
    }

    /// Returns the number of rows in the cursor's row group.
    pub fn len(&self) -> u64 {
        self.row_group.len()
    }

    /// Returns true if this cursor's row group has no rows.
    pub fn is_empty(&self) -> bool {
        self.row_group.is_empty()
    }

    /// Returns the row number of the current row, as an absolute number
    /// relative to the top of the column rather than the top of the row group.
    /// If the cursor is before the row group or on the first row, returns the
    /// row number of the first row in the row group; if the cursor is after the
    /// row group, returns the row number of the row just after the row group.
    pub fn absolute_position(&self) -> u64 {
        self.position.absolute_position(&self.row_group)
    }

    /// Returns the number of times [`move_next`](Self::move_next) may be called
    /// before the cursor is after the row group.
    pub fn remaining_rows(&self) -> u64 {
        self.position.remaining_rows(&self.row_group)
    }

    /// Moves the cursor forward past rows for which `predicate` returns false,
    /// where `predicate` is a function such that if it is true for a given key,
    /// it is also true for all larger keys.
    ///
    /// This function does not move the cursor if `predicate` is true for the
    /// current row or a previous row.
    ///
    /// # Safety
    ///
    /// Unsafe because `rkyv` deserialization is unsafe.
    pub unsafe fn seek_forward_until<P>(&mut self, predicate: P) -> Result<(), Error>
    where
        P: Fn(&K) -> bool + Clone,
    {
        self.advance_to_first_ge(&|key| {
            if predicate(key) {
                Less
            } else {
                Greater
            }
        })
    }

    /// Moves the cursor forward past rows whose keys are less than `target`.
    /// This function does not move the cursor if the current row's key is
    /// greater than or equal to `target`.
    ///
    /// # Safety
    ///
    /// Unsafe because `rkyv` deserialization is unsafe.
    pub unsafe fn advance_to_value_or_larger(&mut self, target: &K) -> Result<(), Error> {
        self.advance_to_first_ge(&|key| target.cmp(key))
    }

    /// Moves the cursor forward past rows for which `compare` returns [`Less`],
    /// where `compare` is a function such that if it returns [`Equal`] or
    /// [`Greater`] for a given key, it returns [`Greater`] for all larger keys.
    ///
    /// This function does not move the cursor if `compare` returns [`Equal`] or
    /// [`Greater`] for the current row or a previous row.
    ///
    /// # Error handling
    ///
    /// If this returns an error, then the cursor's position might be lost. If
    /// so, then its position is advanced past the end of the row group.
    ///
    /// # Safety
    ///
    /// Unsafe because `rkyv` deserialization is unsafe.
    pub unsafe fn advance_to_first_ge<C>(&mut self, compare: &C) -> Result<(), Error>
    where
        C: Fn(&K) -> Ordering,
    {
        self.position.advance_to_first_ge(&self.row_group, compare)
    }

    /// Moves the cursor backward past rows for which `predicate` returns false,
    /// where `predicate` is a function such that if it is true for a given key,
    /// it is also true for all lesser keys.
    ///
    /// This function does not move the cursor if `predicate` is true for the
    /// current row or a previous row.
    ///
    /// # Safety
    ///
    /// Unsafe because `rkyv` deserialization is unsafe.
    pub unsafe fn seek_backward_until<P>(&mut self, predicate: P) -> Result<(), Error>
    where
        P: Fn(&K) -> bool + Clone,
    {
        self.rewind_to_last_le(&|key| {
            if !predicate(key) {
                Less
            } else {
                Greater
            }
        })
    }

    /// Moves the cursor backward past rows whose keys are greater than
    /// `target`.  This function does not move the cursor if the current row's
    /// key is less than or equal to `target`.
    ///
    /// # Safety
    ///
    /// Unsafe because `rkyv` deserialization is unsafe.
    pub unsafe fn rewind_to_value_or_smaller(&mut self, target: &K) -> Result<(), Error>
    where
        K: Ord,
    {
        self.rewind_to_last_le(&|key| target.cmp(key))
    }

    /// Moves the cursor backward past rows for which `compare` returns
    /// [`Greater`], where `compare` is a function such that if it returns
    /// [`Equal`] or [`Less`] for a given key, it returns [`Less`] for all
    /// lesser keys.
    ///
    /// This function does not move the cursor if `compare` returns [`Equal`] or
    /// [`Less`] for the current row or a previous row.
    ///
    /// # Safety
    ///
    /// Unsafe because `rkyv` deserialization is unsafe.
    pub unsafe fn rewind_to_last_le<C>(&mut self, compare: &C) -> Result<(), Error>
    where
        C: Fn(&K) -> Ordering,
    {
        let position = Position::best_match::<N, T, _>(&self.row_group, compare, Greater)?;
        if position < self.position {
            self.position = position;
        }
        Ok(())
    }
}

impl<'a, K, A, NK, NA, NN, T> Cursor<'a, K, A, (&'static NK, &'static NA, NN), T>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
    NK: DataTrait + ?Sized,
    NA: DataTrait + ?Sized,
    T: ColumnSpec,
{
    /// Obtains the row group in the next column associated with the current
    /// row.  If the cursor is on a row, the returned row group will contain at
    /// least one row.  If the cursor is before or after the row group, the
    /// returned row group will be empty.
    ///
    /// This method does not do I/O, but it can report [Error::Corruption].
    pub fn next_column<'b>(&'b self) -> Result<RowGroup<'a, NK, NA, NN, T>, Error> {
        Ok(RowGroup::new(
            self.row_group.reader,
            self.row_group.column + 1,
            self.position.row_group()?,
        ))
    }
}

/// A path from the root of a column to a data block.
struct Path<K: DataTrait + ?Sized, A: DataTrait + ?Sized> {
    row: u64,
    indexes: Vec<Arc<IndexBlock<K>>>,
    data: Arc<DataBlock<K, A>>,
}

impl<K: DataTrait + ?Sized, A: DataTrait + ?Sized> PartialEq for Path<K, A> {
    fn eq(&self, other: &Self) -> bool {
        self.row == other.row
    }
}

impl<K: DataTrait + ?Sized, A: DataTrait + ?Sized> Eq for Path<K, A> {}

impl<K: DataTrait + ?Sized, A: DataTrait + ?Sized> PartialOrd for Path<K, A> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<K: DataTrait + ?Sized, A: DataTrait + ?Sized> Ord for Path<K, A> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.row.cmp(&other.row)
    }
}

impl<K: DataTrait + ?Sized, A: DataTrait + ?Sized> Clone for Path<K, A> {
    fn clone(&self) -> Self {
        Self {
            row: self.row,
            indexes: self.indexes.clone(),
            data: self.data.clone(),
        }
    }
}

fn push_index_block<K>(
    indexes: &mut Vec<Arc<IndexBlock<K>>>,
    index_block: Arc<IndexBlock<K>>,
) -> Result<(), Error>
where
    K: DataTrait + ?Sized,
{
    const MAX_DEPTH: usize = 64;
    if indexes.len() > MAX_DEPTH {
        // A depth of 64 (very deep) with a branching factor of 2 (very
        // small) would allow for over `2**64` items.  A deeper file is a
        // bug or a memory exhaustion attack.
        return Err(CorruptionError::TooDeep {
            depth: indexes.len(),
            max_depth: MAX_DEPTH,
        }
        .into());
    }

    indexes.push(index_block);
    Ok(())
}

impl<K, A> Path<K, A>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    fn for_row<N, T>(row_group: &RowGroup<'_, K, A, N, T>, row: u64) -> Result<Self, Error>
    where
        T: ColumnSpec,
    {
        Self::for_row_from_ancestor(
            row_group,
            Vec::new(),
            row_group.reader.columns[row_group.column]
                .root
                .clone()
                .unwrap(),
            row,
        )
    }
    fn for_row_from_ancestor<N, T>(
        row_group: &RowGroup<'_, K, A, N, T>,
        mut indexes: Vec<Arc<IndexBlock<K>>>,
        mut node: TreeNode,
        row: u64,
    ) -> Result<Self, Error>
    where
        T: ColumnSpec,
    {
        loop {
            let block = node.read(&row_group.reader.file)?;
            let next = block.lookup_row(row)?;
            match block {
                TreeBlock::Data(data) => {
                    return Ok(Self { row, indexes, data });
                }
                TreeBlock::Index(index) => {
                    push_index_block(&mut indexes, index)?;
                }
            };
            node = next.unwrap();
        }
    }
    fn for_row_from_hint<N, T>(
        row_group: &RowGroup<'_, K, A, N, T>,
        hint: Option<&Self>,
        row: u64,
    ) -> Result<Self, Error>
    where
        T: ColumnSpec,
    {
        let Some(hint) = hint else {
            return Self::for_row(row_group, row);
        };
        if hint.data.rows().contains(&row) {
            return Ok(Self {
                row,
                ..hint.clone()
            });
        }
        for (idx, index_block) in hint.indexes.iter().enumerate().rev() {
            if index_block.rows().contains(&row) {
                let node = index_block.get_child_by_row(row)?;
                return Self::for_row_from_ancestor(
                    row_group,
                    hint.indexes[0..=idx].to_vec(),
                    node,
                    row,
                );
            }
        }
        Err(CorruptionError::MissingRow(row).into())
    }
    unsafe fn key(&self, factories: &Factories<K, A>, key: &mut K) {
        self.data.key_for_row(factories, self.row, key)
    }
    unsafe fn aux(&self, factories: &Factories<K, A>, aux: &mut A) {
        self.data.aux_for_row(factories, self.row, aux)
    }
    unsafe fn item(&self, factories: &Factories<K, A>, item: (&mut K, &mut A)) {
        self.data.item_for_row(factories, self.row, item)
    }
    unsafe fn archived_item(&self, factories: &Factories<K, A>) -> &dyn ArchivedItem<'_, K, A> {
        self.data.archived_item_for_row(factories, self.row)
    }

    fn row_group(&self) -> Result<Range<u64>, Error> {
        self.data.row_group_for_row(self.row)
    }

    fn move_to_row<N, T>(
        &mut self,
        row_group: &RowGroup<'_, K, A, N, T>,
        row: u64,
    ) -> Result<(), Error>
    where
        T: ColumnSpec,
    {
        if self.data.rows().contains(&row) {
            self.row = row;
        } else {
            *self = Self::for_row_from_hint(row_group, Some(self), row)?;
        }
        Ok(())
    }
    unsafe fn best_match<N, T, C>(
        row_group: &RowGroup<'_, K, A, N, T>,
        compare: &C,
        bias: Ordering,
    ) -> Result<Option<Self>, Error>
    where
        T: ColumnSpec,
        C: Fn(&K) -> Ordering,
    {
        let mut indexes = Vec::new();
        let Some(mut node) = row_group.reader.columns[row_group.column].root.clone() else {
            return Ok(None);
        };
        loop {
            match node.read(&row_group.reader.file)? {
                TreeBlock::Index(index_block) => {
                    let Some(child_idx) = index_block.find_best_match(
                        row_group.factories.key_factory,
                        &row_group.rows,
                        compare,
                        bias,
                    ) else {
                        return Ok(None);
                    };
                    node = index_block.get_child(child_idx)?;
                    push_index_block(&mut indexes, index_block)?;
                }
                TreeBlock::Data(data_block) => {
                    return Ok(data_block
                        .find_best_match(&row_group.factories, &row_group.rows, compare, bias)
                        .map(|child_idx| Self {
                            row: data_block.first_row + child_idx as u64,
                            indexes,
                            data: data_block,
                        }));
                }
            }
        }
    }

    /// This implements an equivalent of the following snippet, but it performs
    /// much better because it searches from the current path, reusing the data
    /// block and index blocks already in the path, instead of starting from the
    /// root node.
    ///
    /// ````text
    /// match Self::best_match(row_group, compare, Less)? {
    ///     Some(path) => {
    ///         *self = path;
    ///         return Ok(true);
    ///     }
    ///     None => return Ok(false),
    /// }
    /// ```
    ///
    /// If this returns `Ok(false)` or `Err(_)`, then the resulting `Path` can
    /// violate the invariant that `self.data` is not a direct child of the last
    /// element in `self.indexes`. The caller should not use this `Path` again.
    ///
    /// The same optimization would apply to backward seeks, but they haven't
    /// been important in practice yet.
    unsafe fn advance_to_first_ge<N, T, C>(
        &mut self,
        row_group: &RowGroup<'_, K, A, N, T>,
        compare: &C,
    ) -> Result<bool, Error>
    where
        T: ColumnSpec,
        C: Fn(&K) -> Ordering,
    {
        let rows = self.row..row_group.rows.end;

        // Check the current position first. We might already be done.
        let mut ordering = Equal;
        row_group.factories.key_factory.with(&mut |key| {
            self.key(&row_group.factories, key);
            ordering = compare(key);
        });
        if ordering != Greater {
            return Ok(true);
        }

        // If the last item in `rows` in the current data block is greater than
        // or equal to the target, then the position must be in the current data
        // block.
        if self.data.compare_row(
            &row_group.factories,
            min(self.data.rows().end, rows.end) - 1,
            compare,
        ) != Greater
        {
            let child_idx = self
                .data
                .find_best_match(&row_group.factories, &rows, compare, Less)
                .unwrap();
            self.row = self.data.first_row + child_idx as u64;
            return Ok(true);
        }

        while let Some(index_block) = self.indexes.pop() {
            // We need to go up another level if `rows.end` is beyond the end of
            // `index_block` and the greatest value under `index_block` is less
            // than the target.
            if rows.end > index_block.rows().end
                && index_block.compare_max(row_group.factories.key_factory, compare) == Greater
            {
                continue;
            }

            // Otherwise, our target (if any) must be below `index_block`.
            let Some(child_idx) = index_block.find_best_match(
                row_group.factories.key_factory,
                &row_group.rows,
                compare,
                Less,
            ) else {
                // `rows.end` is inside `index_block` but the largest key is
                // less than the target.
                return Ok(false);
            };
            let mut node = index_block.get_child(child_idx)?;
            push_index_block(&mut self.indexes, index_block)?;

            loop {
                match node.read::<K, A>(&row_group.reader.file)? {
                    TreeBlock::Index(index_block) => {
                        let Some(child_idx) = index_block.find_best_match(
                            row_group.factories.key_factory,
                            &row_group.rows,
                            compare,
                            Less,
                        ) else {
                            return Ok(false);
                        };
                        node = index_block.get_child(child_idx)?;
                        push_index_block(&mut self.indexes, index_block)?;
                    }
                    TreeBlock::Data(data_block) => {
                        let Some(child_idx) = data_block.find_best_match(
                            &row_group.factories,
                            &row_group.rows,
                            compare,
                            Less,
                        ) else {
                            return Ok(false);
                        };
                        self.row = child_idx as u64 + data_block.first_row;
                        self.data = data_block;
                        return Ok(true);
                    }
                }
            }
        }

        // Every value in `rows` is less than the target.
        Ok(false)
    }
}

impl<K, A> Debug for Path<K, A>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "Path {{ row: {}, indexes:", self.row)?;
        for index in &self.indexes {
            let n = index.n_children();
            match index.find_row(self.row) {
                Ok(i) => {
                    let min_row = index.get_row_bound(i * 2);
                    let max_row = index.get_row_bound(i * 2 + 1);
                    write!(f, "\n[child {i} of {n}: rows {min_row}..={max_row}]",)?;
                }
                Err(_) => {
                    // This should not be possible because it indicates an
                    // invariant violation.  Possibly we should panic.
                    write!(f, " [unknown child of {n}]")?
                }
            }
        }
        write!(
            f,
            ", data: [row {} of {}] }}",
            self.row - self.data.first_row,
            self.data.n_values()
        )
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Position<K, A>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    /// Before a row group.
    Before,

    /// Within a row group.
    Row(Path<K, A>),

    /// After a row group.
    ///
    /// `hint` is optionally some position in this column. It is useful for
    /// optimizing finding another (presumably nearby) position in the same
    /// column. This optimizes the common case of iterating through one column
    /// (e.g. a key in an indexed wset) and visiting all of the corresponding
    /// values in the next column (e.g. the key's values in the indexed wset),
    /// using [RowGroup::first_with_hint] to visit the first value of each key
    /// after the first.
    ///
    /// We could add a hint to [Position::Before] but reverse iteration is
    /// uncommon so it probably wouldn't help with much.
    After { hint: Option<Path<K, A>> },
}

impl<K, A> Clone for Position<K, A>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    fn clone(&self) -> Self {
        match self {
            Position::Before => Position::Before,
            Position::Row(path) => Position::Row(path.clone()),
            Position::After { hint } => Position::After { hint: hint.clone() },
        }
    }
}

impl<K, A> Position<K, A>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    fn for_row<N, T>(row_group: &RowGroup<'_, K, A, N, T>, row: u64) -> Result<Self, Error>
    where
        T: ColumnSpec,
    {
        Ok(Self::Row(Path::for_row(row_group, row)?))
    }
    fn for_row_from_hint<N, T>(
        row_group: &RowGroup<'_, K, A, N, T>,
        hint: &Self,
        row: u64,
    ) -> Result<Self, Error>
    where
        T: ColumnSpec,
    {
        Ok(Self::Row(Path::for_row_from_hint(
            row_group,
            hint.hint(),
            row,
        )?))
    }
    fn next<N, T>(&mut self, row_group: &RowGroup<'_, K, A, N, T>) -> Result<(), Error>
    where
        T: ColumnSpec,
    {
        let row = match self {
            Self::Before => row_group.rows.start,
            Self::Row(path) => path.row + 1,
            Self::After { .. } => return Ok(()),
        };
        if row < row_group.rows.end {
            self.move_to_row(row_group, row)
        } else {
            self.move_after();
            Ok(())
        }
    }
    fn prev<N, T>(&mut self, row_group: &RowGroup<'_, K, A, N, T>) -> Result<(), Error>
    where
        T: ColumnSpec,
    {
        match self {
            Self::Before => (),
            Self::Row(path) => {
                if path.row > row_group.rows.start {
                    path.move_to_row(row_group, path.row - 1)?;
                } else {
                    *self = Self::Before;
                }
            }
            Self::After { hint } => {
                *self = if !row_group.is_empty() {
                    Self::Row(Path::for_row_from_hint(
                        row_group,
                        hint.as_ref(),
                        row_group.rows.end - 1,
                    )?)
                } else {
                    Self::Before
                }
            }
        }
        Ok(())
    }
    fn move_to_row<N, T>(
        &mut self,
        row_group: &RowGroup<'_, K, A, N, T>,
        row: u64,
    ) -> Result<(), Error>
    where
        T: ColumnSpec,
    {
        if !row_group.rows.is_empty() {
            match self {
                Position::Before => *self = Self::Row(Path::for_row(row_group, row)?),
                Position::After { hint } => {
                    *self = Self::Row(Path::for_row_from_hint(row_group, hint.as_ref(), row)?)
                }
                Position::Row(path) => path.move_to_row(row_group, row)?,
            }
        }
        Ok(())
    }
    fn move_after(&mut self) {
        *self = Position::After {
            hint: self.take_hint(),
        };
    }
    /// Replaces `self` by an arbitrary value and returns its prevous [Path]
    /// (whether the current row or a hint).
    fn take_hint(&mut self) -> Option<Path<K, A>> {
        match replace(self, Position::Before) {
            Position::Before => None,
            Position::Row(hint) => Some(hint),
            Position::After { hint } => hint,
        }
    }
    /// Returns the current row or a hint for one.
    fn hint(&self) -> Option<&Path<K, A>> {
        match self {
            Position::Before => None,
            Position::Row(path) => Some(path),
            Position::After { hint } => hint.as_ref(),
        }
    }

    fn path(&self) -> Option<&Path<K, A>> {
        match self {
            Position::Before => None,
            Position::Row(path) => Some(path),
            Position::After { .. } => None,
        }
    }
    pub unsafe fn key<'k>(&self, factories: &Factories<K, A>, key: &'k mut K) -> Option<&'k mut K> {
        self.path().map(|path| {
            path.key(factories, key);
            key
        })
    }
    pub unsafe fn aux<'a>(&self, factories: &Factories<K, A>, aux: &'a mut A) -> Option<&'a mut A> {
        self.path().map(|path| {
            path.aux(factories, aux);
            aux
        })
    }
    pub unsafe fn item<'a>(
        &self,
        factories: &Factories<K, A>,
        item: (&'a mut K, &'a mut A),
    ) -> Option<(&'a mut K, &'a mut A)> {
        self.path().map(|path| {
            path.item(factories, (item.0, item.1));
            item
        })
    }
    pub unsafe fn archived_item(
        &self,
        factories: &Factories<K, A>,
    ) -> Option<&dyn ArchivedItem<'_, K, A>> {
        self.path().map(|path| path.archived_item(factories))
    }

    pub fn row_group(&self) -> Result<Range<u64>, Error> {
        match self.path() {
            Some(path) => path.row_group(),
            None => Ok(0..0),
        }
    }
    fn has_value(&self) -> bool {
        self.path().is_some()
    }
    unsafe fn best_match<N, T, C>(
        row_group: &RowGroup<'_, K, A, N, T>,
        compare: &C,
        bias: Ordering,
    ) -> Result<Self, Error>
    where
        T: ColumnSpec,
        C: Fn(&K) -> Ordering,
    {
        match Path::best_match(row_group, compare, bias)? {
            Some(path) => Ok(Position::Row(path)),
            None => Ok(if bias == Less {
                Position::After { hint: None }
            } else {
                Position::Before
            }),
        }
    }
    fn absolute_position<N, T>(&self, row_group: &RowGroup<K, A, N, T>) -> u64
    where
        T: ColumnSpec,
    {
        match self {
            Position::Before => row_group.rows.start,
            Position::Row(path) => path.row,
            Position::After { .. } => row_group.rows.end,
        }
    }
    fn remaining_rows<N, T>(&self, row_group: &RowGroup<K, A, N, T>) -> u64
    where
        T: ColumnSpec,
    {
        match self {
            Position::Before => row_group.len(),
            Position::Row(path) => row_group.rows.end - path.row,
            Position::After { .. } => 0,
        }
    }

    /// If this returns an I/O error, then the position might be lost (and set
    /// to `Position::After`).
    unsafe fn advance_to_first_ge<N, T, C>(
        &mut self,
        row_group: &RowGroup<'_, K, A, N, T>,
        compare: &C,
    ) -> Result<(), Error>
    where
        T: ColumnSpec,
        C: Fn(&K) -> Ordering,
    {
        match self {
            Position::Before => {
                *self = Self::best_match::<N, T, _>(row_group, compare, Less)?;
            }
            Position::After { .. } => (),
            Position::Row(path) => {
                match path.advance_to_first_ge(row_group, compare) {
                    Ok(false) => {
                        // Discard `path`, which might now violate its internal
                        // invariants (so don't even try to use it as a hint).
                        *self = Position::After { hint: None };
                    }
                    Ok(true) => (),
                    Err(error) => {
                        // Discard `path`, which might now violate its internal
                        // invariants (so don't even try to use it as a hint).
                        *self = Position::After { hint: None };
                        return Err(error);
                    }
                }
            }
        }
        Ok(())
    }
}

/// A `DynVec`, possibly filtered by a Bloom filter.
struct FilteredKeys<'b, K>
where
    K: ?Sized,
{
    /// Sorted array to keys to retrieve.
    queried_keys: &'b DynVec<K>,

    /// Indexes into `queried_keys` of the keys that pass the Bloom filter.  If
    /// this is `None`, then enough of the keys passed the Bloom filter that we
    /// just take all of them.
    bloom_keys: Option<Vec<usize>>,
}

impl<'b, K> FilteredKeys<'b, K>
where
    K: DataTrait + ?Sized,
{
    /// Returns `keys`, filtered using `reader.maybe_contains_key()`.
    fn new<'a, A, N>(reader: &'a Reader<(&'static K, &'static A, N)>, keys: &'b DynVec<K>) -> Self
    where
        A: DataTrait + ?Sized,
        N: ColumnSpec,
    {
        debug_assert!(keys.is_sorted_by(&|a, b| a.cmp(b)));

        // Pass keys into the Bloom filter until 1/300th of them pass the Bloom
        // filter.  Empirically, this seems to good enough for the common case
        // where the data passed into a "distinct" operator is actually distinct
        // but we get some false positives from the Bloom filter.  Because the
        // keys that go into a "distinct" operator are often large, we don't
        // want to pass all of them into the Bloom filter if we're going to have
        // to deserialize them anyhow later.
        let mut bloom_keys = SmallVec::<[_; 50]>::new();
        for (index, key) in keys.dyn_iter().enumerate() {
            if reader.maybe_contains_key(key.default_hash()) {
                bloom_keys.push(index);
                if bloom_keys.len() >= keys.len() / 300 {
                    return Self {
                        queried_keys: keys,
                        bloom_keys: None,
                    };
                }
            }
        }
        Self {
            queried_keys: keys,
            bloom_keys: Some(bloom_keys.into_vec()),
        }
    }

    fn len(&self) -> usize {
        match &self.bloom_keys {
            Some(bloom_keys) => bloom_keys.len(),
            None => self.queried_keys.len(),
        }
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<'b, K> Index<usize> for FilteredKeys<'b, K>
where
    K: DataTrait + ?Sized,
{
    type Output = K;

    fn index(&self, index: usize) -> &Self::Output {
        match &self.bloom_keys {
            Some(bloom_keys) => &self.queried_keys[bloom_keys[index]],
            None => &self.queried_keys[index],
        }
    }
}
