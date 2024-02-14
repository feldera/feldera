//! Layer file reader.
//!
//! [`Reader`] is the top-level interface for reading layer files.

use std::{
    cmp::Ordering::{self, *},
    fmt::{Debug, Formatter, Result as FmtResult},
    marker::PhantomData,
    mem::size_of,
    ops::{Bound, Range, RangeBounds},
    path::PathBuf,
    rc::Rc,
};

use crate::dynamic::{DataTrait, DeserializeDyn, Factory};
use binrw::{
    io::{self, Error as IoError},
    BinRead, Error as BinError,
};
use thiserror::Error as ThisError;

use crate::storage::{
    backend::{
        ImmutableFileHandle, StorageControl, StorageError, StorageExecutor, StorageRead,
        StorageWrite,
    },
    buffer_cache::{BufferCache, FBuf},
    file::format::{
        DataBlockHeader, FileTrailerColumn, IndexBlockHeader, NodeType, Varint, VERSION_NUMBER,
    },
    file::item::ArchivedItem,
};

use super::{cache::FileCacheEntry, AnyFactories, BlockLocation, Factories, InvalidBlockLocation};

/// Any kind of error encountered reading a layer file.
#[derive(ThisError, Debug)]
pub enum Error {
    /// Errors that indicate a problem with the layer file contents.
    #[error("Corrupt layer file: {0}")]
    Corruption(#[from] CorruptionError),

    /// Errors reading the layer file.
    #[error("Error accessing storage: {0}")]
    Storage(#[from] StorageError),

    /// Errors reading the layer file.
    #[error("Error accessing storage: {0}")]
    Io(#[from] IoError),

    /// File has unexpected number of columns.
    #[error("File has {actual} column(s) but should have {expected}.")]
    WrongNumberOfColumns {
        /// Number of columns in file.
        actual: usize,
        /// Expected number of columns in file.
        expected: usize,
    },
}

impl From<BinError> for Error {
    fn from(source: BinError) -> Self {
        Error::Corruption(source.into())
    }
}

/// Errors that indicate a problem with the layer file contents.
#[derive(ThisError, Debug)]
pub enum CorruptionError {
    /// File size must be a positive multiple of 4096.
    #[error("File size {0} must be a positive multiple of 4096")]
    InvalidFileSize(
        /// Actual file size.
        u64,
    ),

    /// Block has invalid checksum.
    #[error("{size}-byte block at offset {offset} has invalid checksum {checksum:#x} (expected {computed_checksum:#})")]
    InvalidChecksum {
        /// Block offset.
        offset: u64,
        /// Block size.
        size: usize,
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

    /// [`mod@binrw`] reported a format violation.
    #[error("Binary read/write error: {0}")]
    Binrw(
        /// Underlying error.
        #[from]
        BinError,
    ),

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
    #[error("{size}-byte index block at offset {offset} is empty")]
    EmptyIndex {
        /// Block offset.
        offset: u64,
        /// Block size.
        size: usize,
    },

    /// Data block contains unexpected number of rows.
    #[error("{size}-byte data block at offset {offset} contains {n_rows} rows but {expected_rows} were expected.")]
    DataBlockWrongNumberOfRows {
        /// Block offset in bytes.
        offset: u64,
        /// Block size in bytes.
        size: usize,
        /// Number of rows in block.
        n_rows: u64,
        /// Expected number of rows in block.
        expected_rows: u64,
    },

    /// Index block requires unexpected number of rows.
    #[error("{size}-byte index block at offset {offset} contains {n_rows} rows but {expected_rows} were expected.")]
    IndexBlockWrongNumberOfRows {
        /// Block offset in bytes.
        offset: u64,
        /// Block size in bytes.
        size: usize,
        /// Number of rows in block.
        n_rows: u64,
        /// Expected number of rows in block.
        expected_rows: u64,
    },

    /// Index row totals aren't strictly increasing.
    #[error("{size}-byte index block at offset {offset} has nonmonotonic row totals ({prev} then {next}).")]
    NonmonotonicIndex {
        /// Block offset in bytes.
        offset: u64,
        /// Block size in bytes.
        size: usize,
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

    /// Invalid child pointer in index block.  At least one of `child_offset` or
    /// `child_size` is invalid.
    #[error("{index_size}-byte index block at offset {index_offset} has child pointer {index} with invalid offset {child_offset} or size {child_size}.")]
    InvalidChildPointer {
        /// Block offset in bytes.
        index_offset: u64,
        /// Block size in bytes.
        index_size: usize,
        /// Index of child pointer within block.
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
    #[error("Row group {index} in {size}-byte data block at offset {offset} has invalid row range {start}..{end}.")]
    InvalidRowGroup {
        /// Block offset in bytes.
        offset: u64,
        /// Block size in bytes.
        size: usize,
        /// Row group index inside block.
        index: usize,
        /// Row number of start of range.
        start: u64,
        /// Row number of end of range (exclusive).
        end: u64,
    },

    /// Bad block type.
    #[error("{size}-byte block at offset {offset} is wrong type of block.")]
    BadBlockType {
        /// Block offset in bytes.
        offset: u64,
        /// Block size in bytes.
        size: usize,
    },
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

/// Cached data block details.
pub struct InnerDataBlock {
    location: BlockLocation,
    raw: Rc<FBuf>,
    value_map: ValueMapReader,
    row_groups: Option<VarintReader>,
}

impl InnerDataBlock {
    pub(super) fn cost(&self) -> usize {
        size_of::<Self>() + self.raw.len()
    }
    pub(super) fn from_raw(raw: Rc<FBuf>, location: BlockLocation) -> Result<Self, Error> {
        let header = DataBlockHeader::read_le(&mut io::Cursor::new(raw.as_slice()))?;
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
        })
    }

    fn new<S>(file: &ImmutableFileRef<S>, node: &TreeNode) -> Result<Rc<Self>, Error>
    where
        S: StorageRead + StorageWrite + StorageControl + StorageExecutor,
    {
        file.cache.block_on(file.cache.read_data_block(
            file.file_handle.as_ref().unwrap(),
            node.location.offset,
            node.location.size,
        ))
    }
    fn n_values(&self) -> usize {
        self.value_map.len()
    }
    fn row_group(&self, index: usize) -> Result<Range<u64>, Error> {
        let row_groups = self.row_groups.as_ref().unwrap();
        let start = row_groups.get(&self.raw, index);
        let end = row_groups.get(&self.raw, index + 1);
        if start < end {
            Ok(start..end)
        } else {
            Err(CorruptionError::InvalidRowGroup {
                offset: self.location.offset,
                size: self.location.size,
                index,
                start,
                end,
            }
            .into())
        }
    }
}

struct DataBlock<K, A>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    inner: Rc<InnerDataBlock>,
    first_row: u64,
    factories: Factories<K, A>,
    _phantom: PhantomData<fn(&K, &A)>,
}

impl<K, A> Clone for DataBlock<K, A>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            first_row: self.first_row,
            factories: self.factories.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<K, A> DataBlock<K, A>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    fn new<S>(
        factories: &Factories<K, A>,
        file: &ImmutableFileRef<S>,
        node: &TreeNode,
    ) -> Result<Self, Error>
    where
        S: StorageRead + StorageWrite + StorageControl + StorageExecutor,
    {
        let inner = InnerDataBlock::new(file, node)?;

        let expected_rows = node.rows.end - node.rows.start;
        if inner.n_values() as u64 != expected_rows {
            let BlockLocation { size, offset } = node.location;
            return Err(CorruptionError::DataBlockWrongNumberOfRows {
                offset,
                size,
                n_rows: inner.n_values() as u64,
                expected_rows,
            }
            .into());
        }

        Ok(Self {
            inner,
            first_row: node.rows.start,
            factories: factories.clone(),
            _phantom: PhantomData,
        })
    }
    fn n_values(&self) -> usize {
        self.inner.n_values()
    }
    fn rows(&self) -> Range<u64> {
        self.first_row..(self.first_row + self.n_values() as u64)
    }
    fn row_group(&self, row: u64) -> Result<Range<u64>, Error> {
        self.inner.row_group((row - self.first_row) as usize)
    }
    unsafe fn archived_item(&self, index: usize) -> &dyn ArchivedItem<K, A> {
        self.factories.item_factory.archived_value(
            &self.inner.raw,
            self.inner.value_map.get(&self.inner.raw, index),
        )
    }
    unsafe fn archived_item_for_row(&self, row: u64) -> &dyn ArchivedItem<K, A> {
        let index = (row - self.first_row) as usize;

        self.archived_item(index)
    }

    unsafe fn item(&self, index: usize, item: (&mut K, &mut A)) {
        let archived_item = self.archived_item(index);
        DeserializeDyn::deserialize(archived_item.fst(), item.0);
        DeserializeDyn::deserialize(archived_item.snd(), item.1);
    }
    unsafe fn item_for_row(&self, row: u64, item: (&mut K, &mut A)) {
        let index = (row - self.first_row) as usize;
        self.item(index, item)
    }
    unsafe fn key(&self, index: usize, key: &mut K) {
        let item = self.archived_item(index);
        DeserializeDyn::deserialize(item.fst(), key)
    }
    unsafe fn aux(&self, index: usize, aux: &mut A) {
        let item = self.archived_item(index);
        DeserializeDyn::deserialize(item.snd(), aux)
    }
    unsafe fn key_for_row(&self, row: u64, key: &mut K) {
        let index = (row - self.first_row) as usize;
        self.key(index, key)
    }
    unsafe fn aux_for_row(&self, row: u64, aux: &mut A) {
        let index = (row - self.first_row) as usize;
        self.aux(index, aux)
    }

    unsafe fn find_best_match<C>(
        &self,
        target_rows: &Range<u64>,
        compare: &C,
        bias: Ordering,
    ) -> Option<usize>
    where
        C: Fn(&K) -> Ordering,
    {
        let mut start = 0;
        let mut end = self.n_values();
        let mut best = None;

        self.factories.key_factory.with(&mut |key| {
            while start < end {
                let mid = (start + end) / 2;
                let row = self.first_row + mid as u64;
                let cmp = range_compare(target_rows, row);
                match cmp {
                    Equal => {
                        self.key(mid, key);
                        let cmp = compare(key);

                        match cmp {
                            Less => end = mid,
                            Equal => {
                                best = Some(mid);
                                return;
                            }
                            Greater => start = mid + 1,
                        };
                        if cmp == bias {
                            best = Some(mid);
                        }
                    }
                    Less => end = mid,
                    Greater => start = mid + 1,
                };
            }
        });
        best
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

#[derive(Clone, Debug)]
struct TreeNode {
    location: BlockLocation,
    node_type: NodeType,
    depth: usize,
    rows: Range<u64>,
    factories: AnyFactories,
}

impl TreeNode {
    fn read<S, K, A>(self, file: &ImmutableFileRef<S>) -> Result<TreeBlock<K, A>, Error>
    where
        S: StorageRead + StorageWrite + StorageControl + StorageExecutor,
        K: DataTrait + ?Sized,
        A: DataTrait + ?Sized,
    {
        match self.node_type {
            NodeType::Data => Ok(TreeBlock::Data(DataBlock::new(
                &self.factories.factories(),
                file,
                &self,
            )?)),
            NodeType::Index => Ok(TreeBlock::Index(IndexBlock::new(
                &self.factories,
                file,
                &self,
            )?)),
        }
    }
}

enum TreeBlock<K: DataTrait + ?Sized, A: DataTrait + ?Sized> {
    Data(DataBlock<K, A>),
    Index(IndexBlock<K>),
}

impl<K, A> TreeBlock<K, A>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    fn lookup_row(&self, row: u64) -> Result<Option<TreeNode>, Error> {
        match self {
            Self::Data(data_block) => {
                if data_block.rows().contains(&row) {
                    return Ok(None);
                }
            }
            Self::Index(index_block) => {
                if let Some(child_node) = index_block.get_child_by_row(row)? {
                    return Ok(Some(child_node));
                }
            }
        }
        Err(CorruptionError::MissingRow(row).into())
    }
}

/// Cached index block details.
pub struct InnerIndexBlock {
    location: BlockLocation,
    raw: Rc<FBuf>,
    child_type: NodeType,
    bounds: VarintReader,
    row_totals: VarintReader,
    child_pointers: VarintReader,
}

impl InnerIndexBlock {
    pub(super) fn cost(&self) -> usize {
        size_of::<Self>() + self.raw.len()
    }
    pub(super) fn from_raw(raw: Rc<FBuf>, location: BlockLocation) -> Result<Self, Error> {
        let header = IndexBlockHeader::read_le(&mut io::Cursor::new(raw.as_slice()))?;
        if header.n_children == 0 {
            return Err(CorruptionError::EmptyIndex {
                size: location.size,
                offset: location.offset,
            }
            .into());
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
                    size: location.size,
                    offset: location.offset,
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
            child_pointers: VarintReader::new(
                &raw,
                header.child_pointer_varint,
                header.child_pointers_offset as usize,
                header.n_children as usize,
            )?,
            raw,
        })
    }

    fn new<S>(file: &ImmutableFileRef<S>, node: &TreeNode) -> Result<Rc<Self>, Error>
    where
        S: StorageRead + StorageWrite + StorageControl + StorageExecutor,
    {
        file.cache.block_on(file.cache.read_index_block(
            file.file_handle.as_ref().unwrap(),
            node.location.offset,
            node.location.size,
        ))
    }
}

struct IndexBlock<K>
where
    K: DataTrait + ?Sized,
{
    inner: Rc<InnerIndexBlock>,
    first_row: u64,
    depth: usize,
    key_factory: &'static dyn Factory<K>,
    factories: AnyFactories,
    _phantom: PhantomData<K>,
}

impl<K> Clone for IndexBlock<K>
where
    K: DataTrait + ?Sized,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            first_row: self.first_row,
            depth: self.depth,
            key_factory: self.key_factory,
            factories: self.factories.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<K> IndexBlock<K>
where
    K: DataTrait + ?Sized,
{
    fn new<S>(
        factories: &AnyFactories,
        file: &ImmutableFileRef<S>,
        node: &TreeNode,
    ) -> Result<Self, Error>
    where
        S: StorageRead + StorageWrite + StorageControl + StorageExecutor,
    {
        const MAX_DEPTH: usize = 64;
        if node.depth > MAX_DEPTH {
            // A depth of 64 (very deep) with a branching factor of 2 (very
            // small) would allow for over `2**64` items.  A deeper file is a
            // bug or a memory exhaustion attack.
            return Err(CorruptionError::TooDeep {
                depth: node.depth,
                max_depth: MAX_DEPTH,
            }
            .into());
        }

        let inner = InnerIndexBlock::new(file, node)?;

        let expected_rows = node.rows.end - node.rows.start;
        let n_rows = inner.row_totals.get(&inner.raw, inner.row_totals.count - 1);
        if n_rows != expected_rows {
            return Err(CorruptionError::IndexBlockWrongNumberOfRows {
                offset: node.location.offset,
                size: node.location.size,
                n_rows,
                expected_rows,
            }
            .into());
        }

        Ok(Self {
            inner,
            first_row: node.rows.start,
            depth: node.depth,
            factories: factories.clone(),
            key_factory: factories.key_factory(),
            _phantom: PhantomData,
        })
    }

    fn get_child_location(&self, index: usize) -> Result<BlockLocation, Error> {
        self.inner
            .child_pointers
            .get(&self.inner.raw, index)
            .try_into()
            .map_err(|error: InvalidBlockLocation| {
                Error::Corruption(CorruptionError::InvalidChildPointer {
                    index_offset: self.inner.location.offset,
                    index_size: self.inner.location.size,
                    index,
                    child_offset: error.offset,
                    child_size: error.size,
                })
            })
    }

    fn get_child(&self, index: usize) -> Result<TreeNode, Error> {
        Ok(TreeNode {
            factories: self.factories.clone(),
            location: self.get_child_location(index)?,
            node_type: self.inner.child_type,
            depth: self.depth + 1,
            rows: self.get_rows(index),
        })
    }

    fn get_child_by_row(&self, row: u64) -> Result<Option<TreeNode>, Error> {
        self.find_row(row)
            .map(|child_idx| self.get_child(child_idx))
            .transpose()
    }

    fn get_rows(&self, index: usize) -> Range<u64> {
        let low = if index == 0 {
            0
        } else {
            self.inner.row_totals.get(&self.inner.raw, index - 1)
        };
        let high = self.inner.row_totals.get(&self.inner.raw, index);
        (self.first_row + low)..(self.first_row + high)
    }

    fn get_row_bound(&self, index: usize) -> u64 {
        if index == 0 {
            0
        } else if index % 2 == 1 {
            self.inner.row_totals.get(&self.inner.raw, index / 2) - 1
        } else {
            self.inner.row_totals.get(&self.inner.raw, index / 2 - 1)
        }
    }

    fn find_row(&self, row: u64) -> Option<usize> {
        let mut indexes = 0..self.n_children();
        while !indexes.is_empty() {
            let mid = (indexes.start + indexes.end) / 2;
            let rows = self.get_rows(mid);
            if row < rows.start {
                indexes.end = mid;
            } else if row >= rows.end {
                indexes.start = mid + 1;
            } else {
                return Some(mid);
            }
        }
        None
    }

    unsafe fn get_bound(&self, index: usize, bound: &mut K) {
        let offset = self.inner.bounds.get(&self.inner.raw, index) as usize;
        bound.deserialize_from_bytes(&self.inner.raw, offset)
    }

    unsafe fn find_best_match<C>(
        &self,
        target_rows: &Range<u64>,
        compare: &C,
        bias: Ordering,
    ) -> Option<usize>
    where
        C: Fn(&K) -> Ordering,
    {
        let mut result: Option<usize> = None;

        self.key_factory.with(&mut |bound| {
            let mut start = 0;
            let mut end = self.n_children() * 2;
            result = None;
            while start < end {
                let mid = (start + end) / 2;
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

    fn n_children(&self) -> usize {
        self.inner.child_pointers.count
    }
}

impl<K> Debug for IndexBlock<K>
where
    K: DataTrait + ?Sized + Debug,
{
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(
            f,
            "IndexBlock {{ depth: {}, first_row: {}, child_type: {:?}, children = {{",
            self.depth, self.first_row, self.inner.child_type
        )?;
        for i in 0..self.n_children() {
            if i > 0 {
                write!(f, ",")?;
            }
            let mut bound1 = self.factories.key_factory::<K>().default_box();
            let mut bound2 = self.factories.key_factory::<K>().default_box();
            unsafe { self.get_bound(i * 2, &mut bound1) };
            unsafe { self.get_bound(i * 2 + 1, &mut bound2) };
            write!(
                f,
                " [{i}] = {{ rows: {:?}, bounds: {:?}..={:?}, location: {:?} }}",
                self.get_rows(i),
                bound1,
                bound2,
                self.get_child_location(i),
            )?;
        }
        write!(f, " }}")
    }
}

struct Column {
    root: Option<TreeNode>,
    n_rows: u64,
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
                factories: factories.clone(),
                location,
                node_type,
                depth: 0,
                rows: 0..n_rows,
            })
        } else {
            None
        };
        Ok(Self { root, n_rows })
    }
    fn empty() -> Self {
        Self {
            root: None,
            n_rows: 0,
        }
    }
}

/// Encapsulates storage and a file handle, and deletes the file handle when
/// dropped.
pub(crate) struct ImmutableFileRef<S>
where
    S: StorageRead + StorageWrite + StorageControl + StorageExecutor,
{
    path: PathBuf,
    cache: Rc<BufferCache<S, FileCacheEntry>>,
    file_handle: Option<ImmutableFileHandle>,
}

impl<S> ImmutableFileRef<S>
where
    S: StorageRead + StorageWrite + StorageControl + StorageExecutor,
{
    pub(crate) fn new(
        cache: &Rc<BufferCache<S, FileCacheEntry>>,
        file_handle: ImmutableFileHandle,
        path: PathBuf,
    ) -> Self {
        Self {
            cache: Rc::clone(cache),
            path,
            file_handle: Some(file_handle),
        }
    }
}

impl<S> Drop for ImmutableFileRef<S>
where
    S: StorageRead + StorageWrite + StorageControl + StorageExecutor,
{
    fn drop(&mut self) {
        let _ = self
            .cache
            .block_on(self.cache.delete(self.file_handle.take().unwrap()));
    }
}

struct ReaderInner<S, T>
where
    S: StorageRead + StorageWrite + StorageControl + StorageExecutor,
{
    file: Rc<ImmutableFileRef<S>>,
    columns: Vec<Column>,

    /// `fn() -> T` is `Send` and `Sync` regardless of `T`.  See
    /// <https://doc.rust-lang.org/nomicon/phantom-data.html>.
    _phantom: PhantomData<fn() -> T>,
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
/// `T` in `Reader<S, T>` must be a [`ColumnSpec`] that specifies the key and
/// auxiliary data types for all of the columns in the file to be read.
pub struct Reader<S, T>(Rc<ReaderInner<S, T>>)
where
    S: StorageRead + StorageWrite + StorageControl + StorageExecutor;

impl<S, T> Reader<S, T>
where
    S: StorageRead + StorageWrite + StorageControl + StorageExecutor,
    T: ColumnSpec,
{
    /// Creates and returns a new `Reader` for `file`.
    pub(crate) fn new(
        factories: &[&AnyFactories],
        file: Rc<ImmutableFileRef<S>>,
    ) -> Result<Self, Error> {
        let file_size = file
            .cache
            .block_on(file.cache.get_size(file.file_handle.as_ref().unwrap()))?;

        let file_trailer = file.cache.block_on(file.cache.read_file_trailer_block(
            file.file_handle.as_ref().unwrap(),
            file_size - 4096,
            4096,
        ))?;
        if file_trailer.version != VERSION_NUMBER {
            return Err(CorruptionError::InvalidVersion {
                version: file_trailer.version,
                expected_version: VERSION_NUMBER,
            }
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

        Ok(Self(Rc::new(ReaderInner {
            file,
            columns,
            _phantom: PhantomData,
        })))
    }

    /// Create and returns a new `Reader` that has no rows.
    ///
    /// This internally creates an empty temporary file, which means that it can
    /// fail with an I/O error.
    pub fn empty(cache: &Rc<BufferCache<S, FileCacheEntry>>) -> Result<Self, Error>
    where
        S: StorageRead + StorageWrite + StorageControl + StorageExecutor,
    {
        let file_handle = cache.block_on(cache.create())?;
        let (file_handle, path) = cache.block_on(cache.complete(file_handle))?;
        Ok(Self(Rc::new(ReaderInner {
            file: Rc::new(ImmutableFileRef::new(cache, file_handle, path)),
            columns: (0..T::n_columns()).map(|_| Column::empty()).collect(),
            _phantom: PhantomData,
        })))
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
        self.0.columns[column].n_rows
    }

    /// Returns the path on persistent storage for the file of the underlying
    /// reader.
    pub fn path(&self) -> PathBuf {
        self.0.file.as_ref().path.clone()
    }
}

impl<S, T> Clone for Reader<S, T>
where
    S: StorageRead + StorageWrite + StorageControl + StorageExecutor,
{
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<S, K, A, N> Reader<S, (&'static K, &'static A, N)>
where
    S: StorageRead + StorageWrite + StorageControl + StorageExecutor,
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
    (&'static K, &'static A, N): ColumnSpec,
{
    /// Returns a [`RowGroup`] for all of the rows in column 0.
    pub fn rows(&self) -> RowGroup<S, K, A, N, (&'static K, &'static A, N)> {
        RowGroup::new(self, 0, 0..self.0.columns[0].n_rows)
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
pub struct RowGroup<'a, S, K, A, N, T>
where
    S: StorageRead + StorageWrite + StorageControl + StorageExecutor,
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
    T: ColumnSpec,
{
    reader: &'a Reader<S, T>,
    column: usize,
    rows: Range<u64>,
    _phantom: PhantomData<fn(&K, &A, N)>,
}

impl<'a, S, K, A, N, T> Clone for RowGroup<'a, S, K, A, N, T>
where
    S: StorageRead + StorageWrite + StorageControl + StorageExecutor,
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
    T: ColumnSpec,
{
    fn clone(&self) -> Self {
        Self {
            reader: self.reader,
            column: self.column,
            rows: self.rows.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<'a, S, K, A, N, T> Debug for RowGroup<'a, S, K, A, N, T>
where
    S: StorageRead + StorageWrite + StorageControl + StorageExecutor,
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
    T: ColumnSpec,
{
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "RowGroup(column={}, rows={:?})", self.column, self.rows)
    }
}

impl<'a, S, K, A, N, T> RowGroup<'a, S, K, A, N, T>
where
    S: StorageRead + StorageWrite + StorageControl + StorageExecutor,
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
    T: ColumnSpec,
{
    fn new(reader: &'a Reader<S, T>, column: usize, rows: Range<u64>) -> Self {
        Self {
            reader,
            column,
            rows,
            _phantom: PhantomData,
        }
    }

    fn cursor(&self, position: Position<K, A>) -> Cursor<'a, S, K, A, N, T> {
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
    pub fn before(&self) -> Cursor<'a, S, K, A, N, T> {
        self.cursor(Position::Before)
    }

    /// Return a cursor for just after the row group.
    pub fn after(&self) -> Cursor<'a, S, K, A, N, T> {
        self.cursor(Position::After)
    }

    /// Return a cursor for the first row in the row group, or just after the
    /// row group if it is empty.
    pub fn first(&self) -> Result<Cursor<'a, S, K, A, N, T>, Error>
    where
        S: StorageRead + StorageControl + StorageExecutor,
    {
        let position = if self.is_empty() {
            Position::After
        } else {
            Position::for_row(self, self.rows.start)?
        };
        Ok(self.cursor(position))
    }

    /// Return a cursor for the last row in the row group, or just after the
    /// row group if it is empty.
    pub fn last(&self) -> Result<Cursor<'a, S, K, A, N, T>, Error>
    where
        S: StorageRead + StorageExecutor,
    {
        let position = if self.is_empty() {
            Position::After
        } else {
            Position::for_row(self, self.rows.end - 1)?
        };
        Ok(self.cursor(position))
    }

    /// If `row` is less than the number of rows in the row group, returns a
    /// cursor for that row; otherwise, returns a cursor for just after the row
    /// group.
    pub fn nth(&self, row: u64) -> Result<Cursor<'a, S, K, A, N, T>, Error>
    where
        S: StorageRead + StorageExecutor,
    {
        let position = if row < self.len() {
            Position::for_row(self, self.rows.start + row)?
        } else {
            Position::After
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

impl<S, K, A, N> FallibleEq for Reader<S, (&'static K, &'static A, N)>
where
    S: StorageRead + StorageWrite + StorageControl + StorageExecutor,
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
    (&'static K, &'static A, N): ColumnSpec,
    for<'a> RowGroup<'a, S, K, A, N, (&'static K, &'static A, N)>: FallibleEq,
{
    fn equals(&self, other: &Self) -> Result<bool, Error> {
        self.rows().equals(&other.rows())
    }
}

impl<'a, S, K, A, T> FallibleEq for RowGroup<'a, S, K, A, (), T>
where
    S: StorageRead + StorageWrite + StorageControl + StorageExecutor,
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
    T: ColumnSpec,
{
    fn equals(&self, other: &Self) -> Result<bool, Error> {
        if self.len() != other.len() {
            return Ok(false);
        }
        let mut sc: Cursor<'a, _, _, _, _, _> = self.clone().first()?;
        let mut oc: Cursor<'a, _, _, _, _, _> = other.clone().first()?;

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

impl<'a, S, K, A, NK, NA, NN, T> FallibleEq
    for RowGroup<'a, S, K, A, (&'static NK, &'static NA, NN), T>
where
    S: StorageRead + StorageWrite + StorageControl + StorageExecutor,
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
    NK: DataTrait + ?Sized,
    NA: DataTrait + ?Sized,
    T: ColumnSpec,
    RowGroup<'a, S, NK, NA, NN, T>: FallibleEq,
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
pub struct Cursor<'a, S, K, A, N, T>
where
    S: StorageRead + StorageWrite + StorageControl + StorageExecutor,
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
    T: ColumnSpec,
{
    row_group: RowGroup<'a, S, K, A, N, T>,
    position: Position<K, A>,
}

impl<'a, S, K, A, N, T> Clone for Cursor<'a, S, K, A, N, T>
where
    S: StorageRead + StorageWrite + StorageControl + StorageExecutor,
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
    T: ColumnSpec,
{
    fn clone(&self) -> Self {
        Self {
            row_group: self.row_group.clone(),
            position: self.position.clone(),
        }
    }
}

impl<'a, S, K, A, N, T> Debug for Cursor<'a, S, K, A, N, T>
where
    S: StorageRead + StorageWrite + StorageControl + StorageExecutor,
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
    T: ColumnSpec,
{
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "Cursor({:?}, {:?})", self.row_group, self.position)
    }
}

impl<'a, S, K, A, N, T> Cursor<'a, S, K, A, N, T>
where
    S: StorageRead + StorageWrite + StorageControl + StorageExecutor,
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
            self.position = Position::After;
            Ok(())
        }
    }

    /// Moves to row `row`.  If `row >= self.len()`, moves after the row group.
    pub fn move_to_row(&mut self, row: u64) -> Result<(), Error> {
        if row < self.row_group.rows.end - self.row_group.rows.start {
            self.position
                .move_to_row(&self.row_group, self.row_group.rows.start + row)
        } else {
            self.position = Position::After;
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
        self.position.key(key)
    }

    /// Returns the auxiliary data in the current row, or `None` if the cursor
    /// is before or after the row group.
    ///
    /// # Safety
    ///
    /// Unsafe because `rkyv` deserialization is unsafe.
    pub unsafe fn aux<'b>(&self, aux: &'b mut A) -> Option<&'b mut A> {
        self.position.aux(aux)
    }

    /// Returns the key and auxiliary data in the current row, or `None` if the
    /// cursor is before or after the row group.
    ///
    /// # Safety
    ///
    /// Unsafe because `rkyv` deserialization is unsafe.
    pub unsafe fn item<'b>(&self, item: (&'b mut K, &'b mut A)) -> Option<(&'b mut K, &'b mut A)> {
        self.position.item(item)
    }

    /// Returns archived representation of the key and auxiliary data in the
    /// current row, or `None` if the cursor is before or after the row
    /// group.
    ///
    /// # Safety
    ///
    /// Unsafe because `rkyv` deserialization is unsafe.
    pub unsafe fn archived_item(&self) -> Option<&dyn ArchivedItem<'_, K, A>> {
        self.position.archived_item()
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
    /// # Safety
    ///
    /// Unsafe because `rkyv` deserialization is unsafe.
    pub unsafe fn advance_to_first_ge<C>(&mut self, compare: &C) -> Result<(), Error>
    where
        C: Fn(&K) -> Ordering,
    {
        let position = Position::best_match::<S, N, T, _>(&self.row_group, compare, Less)?;
        if position > self.position {
            self.position = position;
        }
        Ok(())
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
        let position = Position::best_match::<S, N, T, _>(&self.row_group, compare, Greater)?;
        if position < self.position {
            self.position = position;
        }
        Ok(())
    }
}

impl<'a, S, K, A, NK, NA, NN, T> Cursor<'a, S, K, A, (&'static NK, &'static NA, NN), T>
where
    S: StorageRead + StorageWrite + StorageControl + StorageExecutor,
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
    pub fn next_column<'b>(&'b self) -> Result<RowGroup<'a, S, NK, NA, NN, T>, Error> {
        Ok(RowGroup::new(
            self.row_group.reader,
            self.row_group.column + 1,
            self.position.row_group()?,
        ))
    }
}

struct Path<K: DataTrait + ?Sized, A: DataTrait + ?Sized> {
    row: u64,
    indexes: Vec<IndexBlock<K>>,
    data: DataBlock<K, A>,
    factories: Factories<K, A>,
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
            factories: self.factories.clone(),
        }
    }
}

impl<K, A> Path<K, A>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    fn for_row<S, N, T>(row_group: &RowGroup<'_, S, K, A, N, T>, row: u64) -> Result<Self, Error>
    where
        S: StorageRead + StorageWrite + StorageControl + StorageExecutor,
        T: ColumnSpec,
    {
        Self::for_row_from_ancestor(
            row_group.reader,
            Vec::new(),
            row_group.reader.0.columns[row_group.column]
                .root
                .clone()
                .unwrap(),
            row,
        )
    }
    fn for_row_from_ancestor<S, T>(
        reader: &Reader<S, T>,
        mut indexes: Vec<IndexBlock<K>>,
        mut node: TreeNode,
        row: u64,
    ) -> Result<Self, Error>
    where
        S: StorageRead + StorageWrite + StorageControl + StorageExecutor,
    {
        loop {
            let block = node.read(&reader.0.file)?;
            let next = block.lookup_row(row)?;
            match block {
                TreeBlock::Data(data) => {
                    let factories = data.factories.clone();
                    return Ok(Self {
                        row,
                        indexes,
                        data,
                        factories,
                    });
                }
                TreeBlock::Index(index) => indexes.push(index),
            };
            node = next.unwrap();
        }
    }
    fn for_row_from_hint<S, T>(reader: &Reader<S, T>, hint: &Self, row: u64) -> Result<Self, Error>
    where
        S: StorageRead + StorageWrite + StorageControl + StorageExecutor,
    {
        if hint.data.rows().contains(&row) {
            return Ok(Self {
                row,
                ..hint.clone()
            });
        }
        for (idx, index_block) in hint.indexes.iter().enumerate().rev() {
            if let Some(node) = index_block.get_child_by_row(row)? {
                return Self::for_row_from_ancestor(
                    reader,
                    hint.indexes[0..=idx].to_vec(),
                    node,
                    row,
                );
            }
        }
        Err(CorruptionError::MissingRow(row).into())
    }
    unsafe fn key(&self, key: &mut K) {
        self.data.key_for_row(self.row, key)
    }
    unsafe fn aux(&self, aux: &mut A) {
        self.data.aux_for_row(self.row, aux)
    }
    unsafe fn item(&self, item: (&mut K, &mut A)) {
        self.data.item_for_row(self.row, item)
    }
    unsafe fn archived_item(&self) -> &dyn ArchivedItem<K, A> {
        self.data.archived_item_for_row(self.row)
    }

    fn row_group(&self) -> Result<Range<u64>, Error> {
        self.data.row_group(self.row)
    }
    fn move_to_row<S, T>(&mut self, reader: &Reader<S, T>, row: u64) -> Result<(), Error>
    where
        S: StorageRead + StorageWrite + StorageControl + StorageExecutor,
    {
        if self.data.rows().contains(&row) {
            self.row = row;
        } else {
            *self = Self::for_row_from_hint(reader, self, row)?;
        }
        Ok(())
    }
    unsafe fn best_match<S, N, T, C>(
        row_group: &RowGroup<'_, S, K, A, N, T>,
        compare: &C,
        bias: Ordering,
    ) -> Result<Option<Self>, Error>
    where
        S: StorageRead + StorageWrite + StorageControl + StorageExecutor,
        T: ColumnSpec,
        C: Fn(&K) -> Ordering,
    {
        let mut indexes = Vec::new();
        let Some(mut node) = row_group.reader.0.columns[row_group.column].root.clone() else {
            return Ok(None);
        };
        loop {
            match node.read(&row_group.reader.0.file)? {
                TreeBlock::Index(index_block) => {
                    let Some(child_idx) =
                        index_block.find_best_match(&row_group.rows, compare, bias)
                    else {
                        return Ok(None);
                    };
                    node = index_block.get_child(child_idx)?;
                    indexes.push(index_block);
                }
                TreeBlock::Data(data_block) => {
                    let factories = data_block.factories.clone();
                    return Ok(data_block
                        .find_best_match(&row_group.rows, compare, bias)
                        .map(|child_idx| Self {
                            row: data_block.first_row + child_idx as u64,
                            indexes,
                            data: data_block,
                            factories,
                        }));
                }
            }
        }
    }
}

impl<K, A> Debug for Path<K, A>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        let mut min = self.factories.key_factory.default_box();
        let mut max = self.factories.key_factory.default_box();

        write!(f, "Path {{ row: {}, indexes:", self.row)?;
        for index in &self.indexes {
            let n = index.n_children();
            match index.find_row(self.row) {
                Some(i) => {
                    unsafe { index.get_bound(i * 2, &mut min) };
                    unsafe { index.get_bound(i * 2 + 1, &mut max) };
                    let min_row = index.get_row_bound(i * 2);
                    let max_row = index.get_row_bound(i * 2 + 1);
                    write!(
                        f,
                        "\n[child {i} of {n}: keys {min:?}..={max:?}, rows {min_row}..={max_row}]",
                    )?;
                }
                None => {
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

#[derive(Debug)]
enum Position<K, A>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    Before,
    Row(Path<K, A>),
    After,
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
            Position::After => Position::After,
        }
    }
}

impl<K, A> PartialEq for Position<K, A>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Before, Self::Before) => true,
            (Self::Row(a), Self::Row(b)) => a.eq(b),
            (Self::After, Self::After) => true,
            _ => false,
        }
    }
}

impl<K, A> Eq for Position<K, A>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
}

impl<K, A> PartialOrd for Position<K, A>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<K, A> Ord for Position<K, A>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::Before, Self::Before) => Equal,
            (Self::Before, Self::Row(_)) => Less,
            (Self::Before, Self::After) => Less,

            (Self::Row(_), Self::Before) => Greater,
            (Self::Row(a), Self::Row(b)) => a.cmp(b),
            (Self::Row(_), Self::After) => Less,

            (Self::After, Self::Before) => Greater,
            (Self::After, Self::Row(_)) => Greater,
            (Self::After, Self::After) => Equal,
        }
    }
}

impl<K, A> Position<K, A>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    fn for_row<S, N, T>(row_group: &RowGroup<'_, S, K, A, N, T>, row: u64) -> Result<Self, Error>
    where
        S: StorageRead + StorageWrite + StorageControl + StorageExecutor,

        T: ColumnSpec,
    {
        Ok(Self::Row(Path::for_row(row_group, row)?))
    }
    fn next<S, N, T>(&mut self, row_group: &RowGroup<'_, S, K, A, N, T>) -> Result<(), Error>
    where
        S: StorageRead + StorageWrite + StorageControl + StorageExecutor,
        T: ColumnSpec,
    {
        let row = match self {
            Self::Before => row_group.rows.start,
            Self::Row(path) => path.row + 1,
            Self::After => return Ok(()),
        };
        if row < row_group.rows.end {
            self.move_to_row(row_group, row)
        } else {
            *self = Self::After;
            Ok(())
        }
    }
    fn prev<S, N, T>(&mut self, row_group: &RowGroup<'_, S, K, A, N, T>) -> Result<(), Error>
    where
        S: StorageRead + StorageWrite + StorageControl + StorageExecutor,
        T: ColumnSpec,
    {
        match self {
            Self::Before => (),
            Self::Row(path) => {
                if path.row > row_group.rows.start {
                    path.move_to_row(row_group.reader, path.row - 1)?;
                } else {
                    *self = Self::Before;
                }
            }
            Self::After => {
                *self = if !row_group.is_empty() {
                    Self::Row(Path::for_row(row_group, row_group.rows.end - 1)?)
                } else {
                    Self::Before
                }
            }
        }
        Ok(())
    }
    fn move_to_row<S, N, T>(
        &mut self,
        row_group: &RowGroup<'_, S, K, A, N, T>,
        row: u64,
    ) -> Result<(), Error>
    where
        S: StorageRead + StorageWrite + StorageControl + StorageExecutor,

        T: ColumnSpec,
    {
        if !row_group.rows.is_empty() {
            match self {
                Position::Before | Position::After => {
                    *self = Self::Row(Path::for_row(row_group, row)?)
                }
                Position::Row(path) => path.move_to_row(row_group.reader, row)?,
            }
        }
        Ok(())
    }
    fn path(&self) -> Option<&Path<K, A>> {
        match self {
            Position::Before => None,
            Position::Row(path) => Some(path),
            Position::After => None,
        }
    }
    pub unsafe fn key<'k>(&self, key: &'k mut K) -> Option<&'k mut K> {
        self.path().map(|path| {
            path.key(key);
            key
        })
    }
    pub unsafe fn aux<'a>(&self, aux: &'a mut A) -> Option<&'a mut A> {
        self.path().map(|path| {
            path.aux(aux);
            aux
        })
    }
    pub unsafe fn item<'a>(&self, item: (&'a mut K, &'a mut A)) -> Option<(&'a mut K, &'a mut A)> {
        self.path().map(|path| {
            path.item((item.0, item.1));
            item
        })
    }
    pub unsafe fn archived_item(&self) -> Option<&dyn ArchivedItem<'_, K, A>> {
        self.path().map(|path| path.archived_item())
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
    unsafe fn best_match<S, N, T, C>(
        row_group: &RowGroup<'_, S, K, A, N, T>,
        compare: &C,
        bias: Ordering,
    ) -> Result<Self, Error>
    where
        S: StorageRead + StorageWrite + StorageControl + StorageExecutor,

        T: ColumnSpec,
        C: Fn(&K) -> Ordering,
    {
        match Path::best_match(row_group, compare, bias)? {
            Some(path) => Ok(Position::Row(path)),
            None => Ok(if bias == Less {
                Position::After
            } else {
                Position::Before
            }),
        }
    }
    fn absolute_position<S, N, T>(&self, row_group: &RowGroup<S, K, A, N, T>) -> u64
    where
        S: StorageRead + StorageWrite + StorageControl + StorageExecutor,
        T: ColumnSpec,
    {
        match self {
            Position::Before => row_group.rows.start,
            Position::Row(path) => path.row,
            Position::After => row_group.rows.end,
        }
    }
    fn remaining_rows<S, N, T>(&self, row_group: &RowGroup<S, K, A, N, T>) -> u64
    where
        S: StorageRead + StorageWrite + StorageControl + StorageExecutor,
        T: ColumnSpec,
    {
        match self {
            Position::Before => row_group.len(),
            Position::Row(path) => row_group.rows.end - path.row,
            Position::After => 0,
        }
    }
}
