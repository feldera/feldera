//! Layer file reader.
//!
//! [`Reader`] is the top-level interface for reading layer files.

use std::{
    cmp::Ordering::{self, *},
    fmt::{Debug, Formatter, Result as FmtResult},
    marker::PhantomData,
    ops::{Bound, Range, RangeBounds},
    rc::Rc,
    sync::Arc,
};

use binrw::{
    io::{self, Error as IoError},
    BinRead, Error as BinError,
};
use crc32c::crc32c;
use rkyv::{archived_value, Deserialize, Infallible};
use thiserror::Error as ThisError;

use crate::{
    backend::{
        ImmutableFileHandle, StorageControl, StorageError, StorageExecutor, StorageRead,
        StorageWrite,
    },
    buffer_cache::FBuf,
    file::format::{
        ArchivedItem, DataBlockHeader, FileTrailer, FileTrailerColumn, IndexBlockHeader, Item,
        NodeType, Varint, VERSION_NUMBER,
    },
};

use super::{BlockLocation, InvalidBlockLocation, Rkyv};

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

struct DataBlock<K, A> {
    location: BlockLocation,
    raw: Arc<FBuf>,
    value_map: ValueMapReader,
    row_groups: Option<VarintReader>,
    first_row: u64,
    _phantom: PhantomData<(K, A)>,
}

impl<K, A> Clone for DataBlock<K, A> {
    fn clone(&self) -> Self {
        Self {
            location: self.location,
            raw: self.raw.clone(),
            value_map: self.value_map.clone(),
            row_groups: self.row_groups.clone(),
            first_row: self.first_row,
            _phantom: PhantomData,
        }
    }
}

impl<K, A> DataBlock<K, A>
where
    K: Rkyv,
    A: Rkyv,
{
    fn new<S>(file: &ImmutableFileRef<S>, node: &TreeNode) -> Result<Self, Error>
    where
        S: StorageRead + StorageControl + StorageExecutor,
    {
        let raw = read_block(file, node.location)?;
        let header = DataBlockHeader::read_le(&mut io::Cursor::new(raw.as_slice()))?;
        let expected_rows = node.rows.end - node.rows.start;
        if header.n_values as u64 != expected_rows {
            let BlockLocation { size, offset } = node.location;
            return Err(CorruptionError::DataBlockWrongNumberOfRows {
                offset,
                size,
                n_rows: header.n_values as u64,
                expected_rows,
            }
            .into());
        }
        Ok(Self {
            location: node.location,
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
            first_row: node.rows.start,
            _phantom: PhantomData,
        })
    }
    fn n_values(&self) -> usize {
        self.value_map.len()
    }
    fn rows(&self) -> Range<u64> {
        self.first_row..(self.first_row + self.n_values() as u64)
    }
    fn row_group(&self, row: u64) -> Result<Range<u64>, Error> {
        let row_groups = self.row_groups.as_ref().unwrap();
        let index = (row - self.first_row) as usize;
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
    unsafe fn archived_item(&self, index: usize) -> &ArchivedItem<K, A> {
        archived_value::<Item<K, A>>(&self.raw, self.value_map.get(&self.raw, index))
    }
    unsafe fn item(&self, index: usize) -> (K, A) {
        let item = self.archived_item(index);
        let key = item.0.deserialize(&mut Infallible).unwrap();
        let aux = item.1.deserialize(&mut Infallible).unwrap();
        (key, aux)
    }
    unsafe fn item_for_row(&self, row: u64) -> (K, A) {
        let index = (row - self.first_row) as usize;
        self.item(index)
    }
    unsafe fn key(&self, index: usize) -> K {
        let item = self.archived_item(index);
        item.0.deserialize(&mut Infallible).unwrap()
    }
    unsafe fn aux(&self, index: usize) -> A {
        let item = self.archived_item(index);
        item.1.deserialize(&mut Infallible).unwrap()
    }
    unsafe fn key_for_row(&self, row: u64) -> K {
        let index = (row - self.first_row) as usize;
        self.key(index)
    }
    unsafe fn aux_for_row(&self, row: u64) -> A {
        let index = (row - self.first_row) as usize;
        self.aux(index)
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
        while start < end {
            let mid = (start + end) / 2;
            let row = self.first_row + mid as u64;
            let cmp = range_compare(target_rows, row);
            match cmp {
                Equal => {
                    let key = self.key(mid);
                    let cmp = compare(&key);
                    match cmp {
                        Less => end = mid,
                        Equal => return Some(mid),
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
}

impl TreeNode {
    fn read<S, K, A>(self, file: &ImmutableFileRef<S>) -> Result<TreeBlock<K, A>, Error>
    where
        S: StorageRead + StorageControl + StorageExecutor,
        K: Rkyv + Debug,
        A: Rkyv,
    {
        match self.node_type {
            NodeType::Data => Ok(TreeBlock::Data(DataBlock::new(file, &self)?)),
            NodeType::Index => Ok(TreeBlock::Index(IndexBlock::new(file, &self)?)),
        }
    }
}

enum TreeBlock<K, A> {
    Data(DataBlock<K, A>),
    Index(IndexBlock<K>),
}

impl<K, A> TreeBlock<K, A>
where
    K: Rkyv,
    A: Rkyv,
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

struct IndexBlock<K> {
    location: BlockLocation,
    raw: Arc<FBuf>,
    child_type: NodeType,
    bounds: VarintReader,
    row_totals: VarintReader,
    child_pointers: VarintReader,
    depth: usize,
    first_row: u64,
    _phantom: PhantomData<K>,
}

impl<K> Clone for IndexBlock<K> {
    fn clone(&self) -> Self {
        Self {
            location: self.location,
            raw: self.raw.clone(),
            child_type: self.child_type,
            bounds: self.bounds.clone(),
            row_totals: self.row_totals.clone(),
            child_pointers: self.child_pointers.clone(),
            depth: self.depth,
            first_row: self.first_row,
            _phantom: PhantomData,
        }
    }
}

impl<K> IndexBlock<K>
where
    K: Rkyv,
{
    fn new<S>(file: &ImmutableFileRef<S>, node: &TreeNode) -> Result<Self, Error>
    where
        S: StorageRead + StorageControl + StorageExecutor,
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

        let raw = read_block(file, node.location)?;
        let header = IndexBlockHeader::read_le(&mut io::Cursor::new(raw.as_slice()))?;
        let BlockLocation { size, offset } = node.location;
        if header.n_children == 0 {
            return Err(CorruptionError::EmptyIndex { size, offset }.into());
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
                    size,
                    offset,
                    prev,
                    next,
                }
                .into());
            }
        }

        let expected_rows = node.rows.end - node.rows.start;
        let n_rows = row_totals.get(&raw, header.n_children as usize - 1);
        if n_rows != expected_rows {
            return Err(CorruptionError::IndexBlockWrongNumberOfRows {
                offset,
                size,
                n_rows,
                expected_rows,
            }
            .into());
        }

        Ok(Self {
            location: node.location,
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
            depth: node.depth,
            first_row: node.rows.start,
            _phantom: PhantomData,
        })
    }

    fn get_child_location(&self, index: usize) -> Result<BlockLocation, Error> {
        self.child_pointers
            .get(&self.raw, index)
            .try_into()
            .map_err(|error: InvalidBlockLocation| {
                Error::Corruption(CorruptionError::InvalidChildPointer {
                    index_offset: self.location.offset,
                    index_size: self.location.size,
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

    unsafe fn get_bound(&self, index: usize) -> K {
        let offset = self.bounds.get(&self.raw, index) as usize;
        let archived = archived_value::<K>(&self.raw, offset);
        archived.deserialize(&mut Infallible).unwrap()
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
        let mut end = self.n_children() * 2;
        let mut best = None;
        while start < end {
            let mid = (start + end) / 2;
            let row = self.get_row_bound(mid) + self.first_row;
            let cmp = match range_compare(target_rows, row) {
                Equal => {
                    let bound = self.get_bound(mid);
                    let cmp = compare(&bound);
                    if cmp == Equal {
                        return Some(mid / 2);
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
                best = Some(mid / 2);
            }
        }
        best
    }

    fn n_children(&self) -> usize {
        self.child_pointers.count
    }
}

impl<K> Debug for IndexBlock<K>
where
    K: Rkyv + Debug,
{
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(
            f,
            "IndexBlock {{ depth: {}, first_row: {}, child_type: {:?}, children = {{",
            self.depth, self.first_row, self.child_type
        )?;
        for i in 0..self.n_children() {
            if i > 0 {
                write!(f, ",")?;
            }
            write!(
                f,
                " [{i}] = {{ rows: {:?}, bounds: {:?}..={:?}, location: {:?} }}",
                self.get_rows(i),
                unsafe { self.get_bound(i * 2) },
                unsafe { self.get_bound(i * 2 + 1) },
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
    fn new(info: &FileTrailerColumn) -> Result<Self, Error> {
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
                    }))
                }
            };
            Some(TreeNode {
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
    S: StorageControl + StorageExecutor,
{
    storage: Rc<S>,
    file_handle: Option<ImmutableFileHandle>,
}

impl<S> ImmutableFileRef<S>
where
    S: StorageControl + StorageExecutor,
{
    pub(crate) fn new(storage: &Rc<S>, file_handle: ImmutableFileHandle) -> Self {
        Self {
            storage: Rc::clone(storage),
            file_handle: Some(file_handle),
        }
    }
}

impl<S> Drop for ImmutableFileRef<S>
where
    S: StorageControl + StorageExecutor,
{
    fn drop(&mut self) {
        let _ = self
            .storage
            .block_on(self.storage.delete(self.file_handle.take().unwrap()));
    }
}

struct ReaderInner<S, T>
where
    S: StorageControl + StorageExecutor,
{
    file: Rc<ImmutableFileRef<S>>,
    columns: Vec<Column>,

    /// `fn() -> T` is `Send` and `Sync` regardless of `T`.  See
    /// <https://doc.rust-lang.org/nomicon/phantom-data.html>.
    _phantom: PhantomData<fn() -> T>,
}

fn read_block<S>(file: &ImmutableFileRef<S>, location: BlockLocation) -> Result<Arc<FBuf>, Error>
where
    S: StorageRead + StorageControl + StorageExecutor,
{
    let block = file.storage.block_on(file.storage.read_block(
        file.file_handle.as_ref().unwrap(),
        location.offset,
        location.size,
    ))?;
    let computed_checksum = crc32c(&block[4..]);
    let checksum = u32::from_le_bytes(block[..4].try_into().unwrap());
    if checksum != computed_checksum {
        let BlockLocation { size, offset } = location;
        return Err(CorruptionError::InvalidChecksum {
            size,
            offset,
            checksum,
            computed_checksum,
        }
        .into());
    }
    Ok(block)
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
impl<K, A, N> ColumnSpec for (K, A, N)
where
    K: Rkyv,
    A: Rkyv,
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
pub struct Reader<S, T>(Arc<ReaderInner<S, T>>)
where
    S: StorageControl + StorageExecutor;

impl<S, T> Reader<S, T>
where
    S: StorageRead + StorageControl + StorageExecutor,
    T: ColumnSpec,
{
    /// Creates and returns a new `Reader` for `file`.
    pub(crate) fn new(file: Rc<ImmutableFileRef<S>>) -> Result<Self, Error> {
        let file_size = file
            .storage
            .block_on(file.storage.get_size(file.file_handle.as_ref().unwrap()))?;

        let file_trailer_block =
            read_block(&file, BlockLocation::new(file_size - 4096, 4096).unwrap())?;
        let file_trailer =
            FileTrailer::read_le(&mut io::Cursor::new(file_trailer_block.as_slice()))?;
        if file_trailer.version != VERSION_NUMBER {
            return Err(CorruptionError::InvalidVersion {
                version: file_trailer.version,
                expected_version: VERSION_NUMBER,
            }
            .into());
        }

        let columns: Vec<_> = file_trailer
            .columns
            .iter()
            .map(Column::new)
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

        Ok(Self(Arc::new(ReaderInner {
            file,
            columns,
            _phantom: PhantomData,
        })))
    }

    /// Create and returns a new `Reader` that has no rows.
    ///
    /// This internally creates an empty temporary file, which means that it can
    /// fail with an I/O error.
    pub fn empty(storage: &Rc<S>) -> Result<Self, Error>
    where
        S: StorageControl + StorageWrite + StorageExecutor,
    {
        let file_handle = storage.block_on(storage.create())?;
        let file_handle = storage.block_on(storage.complete(file_handle))?;
        Ok(Self(Arc::new(ReaderInner {
            file: Rc::new(ImmutableFileRef::new(storage, file_handle)),
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
}

impl<S, T> Clone for Reader<S, T>
where
    S: StorageControl + StorageExecutor,
{
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<S, K, A, N> Reader<S, (K, A, N)>
where
    S: StorageControl + StorageExecutor,
    K: Rkyv + Debug,
    A: Rkyv,
    (K, A, N): ColumnSpec,
{
    /// Returns a [`RowGroup`] for all of the rows in column 0.
    pub fn rows(&self) -> RowGroup<S, K, A, N, (K, A, N)> {
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
    S: StorageControl + StorageExecutor,
    K: Rkyv,
    A: Rkyv,
    T: ColumnSpec,
{
    reader: &'a Reader<S, T>,
    column: usize,
    rows: Range<u64>,
    _phantom: PhantomData<(K, A, N)>,
}

impl<'a, S, K, A, N, T> Clone for RowGroup<'a, S, K, A, N, T>
where
    S: StorageControl + StorageExecutor,
    K: Rkyv,
    A: Rkyv,
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
    S: StorageControl + StorageExecutor,
    K: Rkyv,
    A: Rkyv,
    T: ColumnSpec,
{
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "RowGroup(column={}, rows={:?})", self.column, self.rows)
    }
}

impl<'a, S, K, A, N, T> RowGroup<'a, S, K, A, N, T>
where
    S: StorageControl + StorageExecutor,
    K: Rkyv + Debug,
    A: Rkyv,
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

impl<S, K, A, N> FallibleEq for Reader<S, (K, A, N)>
where
    S: StorageControl + StorageExecutor,
    K: Rkyv + Debug,
    A: Rkyv,
    (K, A, N): ColumnSpec,
    for<'a> RowGroup<'a, S, K, A, N, (K, A, N)>: FallibleEq,
{
    fn equals(&self, other: &Self) -> Result<bool, Error> {
        self.rows().equals(&other.rows())
    }
}

impl<'a, S, K, A, T> FallibleEq for RowGroup<'a, S, K, A, (), T>
where
    S: StorageRead + StorageControl + StorageExecutor,
    K: Rkyv + Debug + Eq,
    A: Rkyv + Eq,
    T: ColumnSpec,
{
    fn equals(&self, other: &Self) -> Result<bool, Error> {
        if self.len() != other.len() {
            return Ok(false);
        }
        let mut sc = self.clone().first()?;
        let mut oc = other.clone().first()?;
        while sc.has_value() {
            if unsafe { sc.item() != oc.item() } {
                return Ok(false);
            }
            sc.move_next()?;
            oc.move_next()?;
        }
        Ok(true)
    }
}

impl<'a, S, K, A, NK, NA, NN, T> FallibleEq for RowGroup<'a, S, K, A, (NK, NA, NN), T>
where
    S: StorageRead + StorageControl + StorageExecutor,
    K: Rkyv + Eq + Debug,
    A: Rkyv + Eq,
    NK: Rkyv + Eq + Debug,
    NA: Rkyv + Eq,
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
            if unsafe { sc.item() != oc.item() } {
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
    S: StorageControl + StorageExecutor,
    K: Rkyv,
    A: Rkyv,
    T: ColumnSpec,
{
    row_group: RowGroup<'a, S, K, A, N, T>,
    position: Position<K, A>,
}

impl<'a, S, K, A, N, T> Clone for Cursor<'a, S, K, A, N, T>
where
    S: StorageRead + StorageControl + StorageExecutor,
    K: Rkyv + Clone,
    A: Rkyv + Clone,
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
    S: StorageRead + StorageControl + StorageExecutor,
    K: Rkyv + Debug,
    A: Rkyv + Debug,
    T: ColumnSpec,
{
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "Cursor({:?}, {:?})", self.row_group, self.position)
    }
}

impl<'a, S, K, A, N, T> Cursor<'a, S, K, A, N, T>
where
    S: StorageRead + StorageControl + StorageExecutor,
    K: Rkyv + Debug,
    A: Rkyv,
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
    pub unsafe fn key(&self) -> Option<K> {
        self.position.key()
    }

    /// Returns the auxiliary data in the current row, or `None` if the cursor
    /// is before or after the row group.
    ///
    /// # Safety
    ///
    /// Unsafe because `rkyv` deserialization is unsafe.
    pub unsafe fn aux(&self) -> Option<A> {
        self.position.aux()
    }

    /// Returns the key and auxiliary data in the current row, or `None` if the
    /// cursor is before or after the row group.
    ///
    /// # Safety
    ///
    /// Unsafe because `rkyv` deserialization is unsafe.
    pub unsafe fn item(&self) -> Option<(K, A)> {
        self.position.item()
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
    pub unsafe fn advance_to_value_or_larger(&mut self, target: &K) -> Result<(), Error>
    where
        K: Ord,
    {
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

impl<'a, S, K, A, NK, NA, NN, T> Cursor<'a, S, K, A, (NK, NA, NN), T>
where
    S: StorageControl + StorageExecutor,
    K: Rkyv + Debug,
    A: Rkyv,
    NK: Rkyv + Debug,
    NA: Rkyv,
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

struct Path<K, A> {
    row: u64,
    indexes: Vec<IndexBlock<K>>,
    data: DataBlock<K, A>,
}

impl<K, A> PartialEq for Path<K, A> {
    fn eq(&self, other: &Self) -> bool {
        self.row == other.row
    }
}

impl<K, A> Eq for Path<K, A> {}

impl<K, A> PartialOrd for Path<K, A> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<K, A> Ord for Path<K, A> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.row.cmp(&other.row)
    }
}

impl<K, A> Clone for Path<K, A> {
    fn clone(&self) -> Self {
        Self {
            row: self.row,
            indexes: self.indexes.clone(),
            data: self.data.clone(),
        }
    }
}

impl<K, A> Path<K, A>
where
    K: Rkyv + Debug,
    A: Rkyv,
{
    fn for_row<S, N, T>(row_group: &RowGroup<'_, S, K, A, N, T>, row: u64) -> Result<Self, Error>
    where
        S: StorageRead + StorageControl + StorageExecutor,
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
        S: StorageRead + StorageControl + StorageExecutor,
    {
        loop {
            let block = node.read(&reader.0.file)?;
            let next = block.lookup_row(row)?;
            match block {
                TreeBlock::Data(data) => return Ok(Self { row, indexes, data }),
                TreeBlock::Index(index) => indexes.push(index),
            };
            node = next.unwrap();
        }
    }
    fn for_row_from_hint<S, T>(reader: &Reader<S, T>, hint: &Self, row: u64) -> Result<Self, Error>
    where
        S: StorageRead + StorageControl + StorageExecutor,
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
    unsafe fn key(&self) -> K {
        self.data.key_for_row(self.row)
    }
    unsafe fn aux(&self) -> A {
        self.data.aux_for_row(self.row)
    }
    unsafe fn item(&self) -> (K, A) {
        self.data.item_for_row(self.row)
    }
    fn row_group(&self) -> Result<Range<u64>, Error> {
        self.data.row_group(self.row)
    }
    fn move_to_row<S, T>(&mut self, reader: &Reader<S, T>, row: u64) -> Result<(), Error>
    where
        S: StorageRead + StorageControl + StorageExecutor,
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
        S: StorageRead + StorageControl + StorageExecutor,
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
                    return Ok(data_block
                        .find_best_match(&row_group.rows, compare, bias)
                        .map(|child_idx| Self {
                            row: data_block.first_row + child_idx as u64,
                            indexes,
                            data: data_block,
                        }))
                }
            }
        }
    }
}

impl<K, A> Debug for Path<K, A>
where
    K: Rkyv + Debug,
    A: Rkyv,
{
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "Path {{ row: {}, indexes:", self.row)?;
        for index in &self.indexes {
            let n = index.n_children();
            match index.find_row(self.row) {
                Some(i) => {
                    let min = unsafe { index.get_bound(i * 2) };
                    let max = unsafe { index.get_bound(i * 2 + 1) };
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

#[derive(Clone, Debug)]
enum Position<K, A>
where
    K: Rkyv,
    A: Rkyv,
{
    Before,
    Row(Path<K, A>),
    After,
}

impl<K, A> PartialEq for Position<K, A>
where
    K: Rkyv,
    A: Rkyv,
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
    K: Rkyv,
    A: Rkyv,
{
}

impl<K, A> PartialOrd for Position<K, A>
where
    K: Rkyv,
    A: Rkyv,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<K, A> Ord for Position<K, A>
where
    K: Rkyv,
    A: Rkyv,
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
    K: Rkyv + Debug,
    A: Rkyv,
{
    fn for_row<S, N, T>(row_group: &RowGroup<'_, S, K, A, N, T>, row: u64) -> Result<Self, Error>
    where
        S: StorageRead + StorageControl + StorageExecutor,
        T: ColumnSpec,
    {
        Ok(Self::Row(Path::for_row(row_group, row)?))
    }
    fn next<S, N, T>(&mut self, row_group: &RowGroup<'_, S, K, A, N, T>) -> Result<(), Error>
    where
        S: StorageRead + StorageControl + StorageExecutor,
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
        S: StorageRead + StorageControl + StorageExecutor,
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
        S: StorageRead + StorageControl + StorageExecutor,
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
    pub unsafe fn key(&self) -> Option<K> {
        self.path().map(|path| path.key())
    }
    pub unsafe fn aux(&self) -> Option<A> {
        self.path().map(|path| path.aux())
    }
    pub unsafe fn item(&self) -> Option<(K, A)> {
        self.path().map(|path| path.item())
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
        S: StorageRead + StorageControl + StorageExecutor,
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
        S: StorageControl + StorageExecutor,
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
        S: StorageControl + StorageExecutor,
        T: ColumnSpec,
    {
        match self {
            Position::Before => row_group.len(),
            Position::Row(path) => row_group.rows.end - path.row,
            Position::After => 0,
        }
    }
}
