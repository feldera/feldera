//! Layer file writer.
//!
//! Use [`Writer1`] to write a 1-column layer file and [`Writer2`] to write a
//! 2-column layer file.  To write more columns, either add another `Writer<N>`
//! struct, which is easily done, or mark the currently private `Writer` as
//! `pub`.
use std::{
    marker::PhantomData,
    mem::{replace, take},
    ops::Range,
    rc::Rc,
};

use binrw::{
    io::{Cursor, NoSeek},
    BinWrite,
};
use crc32c::crc32c;
use rkyv::{archived_value, Archive, Deserialize, Infallible, Serialize};

use crate::{
    backend::{
        FileHandle, ImmutableFileHandle, StorageControl, StorageError, StorageExecutor,
        StorageRead, StorageWrite,
    },
    buffer_cache::{FBuf, FBufSerializer},
    file::{
        format::{
            BlockHeader, DataBlockHeader, FileTrailer, FileTrailerColumn, FixedLen,
            IndexBlockHeader, Item, NodeType, Varint, VERSION_NUMBER,
        },
        BlockLocation,
    },
};

use rkyv::ser::Serializer as RkyvSerializer;

use super::{
    reader::{ImmutableFileRef, Reader},
    Rkyv, Serializer,
};

struct VarintWriter {
    varint: Varint,
    start: usize,
    count: usize,
}
impl VarintWriter {
    fn new(varint: Varint, start: usize, count: usize) -> Self {
        Self {
            start: varint.align(start),
            varint,
            count,
        }
    }
    fn offset_after(&self) -> usize {
        self.start + self.varint.len() * self.count
    }
    fn offset_after_or(opt_array: &Option<VarintWriter>, otherwise: usize) -> usize {
        match opt_array {
            Some(array) => array.offset_after(),
            None => otherwise,
        }
    }
    fn put<V>(&self, dst: &mut FBuf, values: V)
    where
        V: Iterator<Item = u64>,
    {
        dst.resize(self.start, 0);
        let mut count = 0;
        for value in values {
            self.varint.put(dst, value);
            count += 1;
        }
        debug_assert_eq!(count, self.count);
    }
}

/// Configuration parameters for writing a layer file.
///
/// The default parameters should usually be good enough.
pub struct Parameters {
    /// Minimum size of a data block, in bytes.  Must be a power of 2 and at
    /// least 4096.
    ///
    /// Larger data blocks reduce the size of the indexes (by allowing an
    /// individual index entry to span a wider range of data, reducing the
    /// number of index entries) and they allow a single IOP to retrieve more
    /// data, but they also fill up more of the cache.
    ///
    /// An individual data block will be bigger than the minimum if necessary
    /// for at least [`min_branch`](Self::min_branch) data and auxiliary data
    /// values to fit.
    pub min_data_block: usize,

    /// Minimum size of an index block, in bytes.  Must be a power of 2 and at
    /// least 4096.
    ///
    /// Larger index blocks have similar advantages and disadvantages to larger
    /// data blocks, but with less of an effect.
    ///
    /// An individual data block will be bigger than the minimum if necessary
    /// for at least [`2 * min_branch`](Self::min_branch) data values to fit.
    pub min_index_block: usize,

    /// Minimum branching factor.  This controls the minimum number of data
    /// items in a data block and the minimum number of child nodes in an index
    /// block.
    ///
    /// Increasing the branching factor reduces the number of nodes that must be
    /// read to find a particular value.  It also increases the memory required
    /// to read each block.
    ///
    /// The branching factor is not an important consideration for small data
    /// values, because our 4-kB (or larger) minimum data and index block size
    /// means that the minimum branching factor will be high.  For example, over
    /// 100 32-byte values fit in a 4-kB block, even considering overhead.
    ///
    /// The branching factor is more important for large values.  Suppose only
    /// a single value fits into a data block.  The index block that refers to
    /// it reproduces the first value in each data block, which in turn makes
    /// it likely that index block only fits a single child, which is
    /// pathological and silly.
    pub min_branch: usize,

    #[cfg(test)]
    pub max_branch: usize,
}

impl Parameters {
    #[cfg(test)]
    pub fn max_branch(&self) -> usize {
        self.max_branch
    }

    /// Returns the maximum branching factor.  It only makes sense to limit this
    /// for testing purposes, so the non-test version always returns
    /// `usize::MAX`.
    #[doc(hidden)]
    #[cfg(not(test))]
    pub fn max_branch(&self) -> usize {
        usize::MAX
    }

    #[cfg(test)]
    pub fn with_max_branch(max_branch: usize) -> Self {
        Self {
            max_branch,
            ..Self::default()
        }
    }
}

impl Default for Parameters {
    fn default() -> Self {
        Self {
            min_data_block: 8192,
            min_index_block: 8192,
            min_branch: 32,
            #[cfg(test)]
            max_branch: usize::MAX,
        }
    }
}

trait IntoBlock {
    fn into_block(self) -> FBuf;
    fn overwrite_head(&self, dst: &mut FBuf)
    where
        Self: FixedLen;
}

impl<B> IntoBlock for B
where
    B: for<'a> BinWrite<Args<'a> = ()>,
{
    fn into_block(self) -> FBuf {
        let mut block = NoSeek::new(FBuf::with_capacity(4096));
        self.write_le(&mut block).unwrap();
        block.into_inner()
    }

    fn overwrite_head(&self, dst: &mut FBuf)
    where
        Self: FixedLen,
    {
        let mut writer = Cursor::new(dst.as_mut_slice());
        self.write_le(&mut writer).unwrap();
        debug_assert_eq!(writer.position(), <Self as FixedLen>::LEN as u64);
    }
}

struct ColumnWriter {
    parameters: Rc<Parameters>,
    column_index: usize,
    rows: Range<u64>,
    data_block: DataBlockBuilder,
    index_blocks: Vec<IndexBlockBuilder>,
}

impl ColumnWriter {
    fn new(parameters: &Rc<Parameters>, column_index: usize) -> Self {
        ColumnWriter {
            parameters: parameters.clone(),
            column_index,
            rows: 0..0,
            data_block: DataBlockBuilder::new(parameters),
            index_blocks: Vec::new(),
        }
    }

    fn take_rows(&mut self) -> Range<u64> {
        let end = self.rows.end;
        replace(&mut self.rows, end..end)
    }

    fn finish<W, K, A>(
        &mut self,
        block_writer: &mut BlockWriter<W>,
    ) -> Result<FileTrailerColumn, StorageError>
    where
        W: StorageWrite + StorageControl + StorageExecutor,
        K: Rkyv,
        A: Rkyv,
    {
        // Flush data.
        if !self.data_block.is_empty() {
            let data_block = self.data_block.take().build::<K, A>();
            self.write_data_block(block_writer, data_block)?;
        }

        // Flush index.
        let mut level = 0;
        while level < self.index_blocks.len() {
            if level == self.index_blocks.len() - 1 && self.index_blocks[level].entries.len() == 1 {
                let builder = &self.index_blocks[level];
                let entry = &builder.entries[0];
                return Ok(FileTrailerColumn {
                    node_type: builder.child_type,
                    node_offset: entry.child.offset,
                    node_size: entry.child.size as u32,
                    n_rows: entry.row_total,
                });
            } else if !self.index_blocks[level].is_empty() {
                let index_block = self.index_blocks[level].take().build();
                self.write_index_block::<W, K>(block_writer, index_block, level)?;
            }
            level += 1;
        }
        Ok(FileTrailerColumn {
            node_type: NodeType::Data,
            node_offset: 0,
            node_size: 0,
            n_rows: 0,
        })
    }

    fn get_index_block(&mut self, level: usize) -> &mut IndexBlockBuilder {
        if level >= self.index_blocks.len() {
            debug_assert_eq!(level, self.index_blocks.len());
            self.index_blocks.push(IndexBlockBuilder::new(
                &self.parameters,
                self.column_index,
                if level == 0 {
                    NodeType::Data
                } else {
                    NodeType::Index
                },
            ));
        }
        &mut self.index_blocks[level]
    }

    fn write_data_block<W, K>(
        &mut self,
        block_writer: &mut BlockWriter<W>,
        data_block: DataBlock<K>,
    ) -> Result<(), StorageError>
    where
        W: StorageWrite + StorageControl + StorageExecutor,
        K: Rkyv,
    {
        let location = block_writer.write_block(data_block.raw)?;

        if let Some(index_block) = self.get_index_block(0).add_entry(
            location,
            &data_block.min_max,
            data_block.n_values as u64,
        ) {
            self.write_index_block::<W, K>(block_writer, index_block, 0)?;
        }
        Ok(())
    }

    fn write_index_block<W, K>(
        &mut self,
        block_writer: &mut BlockWriter<W>,
        mut index_block: IndexBlock<K>,
        mut level: usize,
    ) -> Result<(BlockLocation, u64), StorageError>
    where
        W: StorageWrite + StorageControl + StorageExecutor,
        K: Rkyv,
    {
        loop {
            let location = block_writer.write_block(index_block.raw)?;

            level += 1;
            let opt_index_block = self.get_index_block(level).add_entry(
                location,
                &index_block.min_max,
                index_block.n_rows,
            );
            index_block = match opt_index_block {
                None => return Ok((location, index_block.n_rows)),
                Some(index_block) => index_block,
            };
        }
    }

    fn add_item<W, K, A>(
        &mut self,
        block_writer: &mut BlockWriter<W>,
        item: (&K, &A),
        row_group: &Option<Range<u64>>,
    ) -> Result<(), StorageError>
    where
        W: StorageWrite + StorageControl + StorageExecutor,
        K: Rkyv,
        A: Rkyv,
    {
        if let Some(data_block) = self.data_block.add_item(item, row_group) {
            self.write_data_block(block_writer, data_block)?;
        }
        Ok(())
    }
}

#[derive(Copy, Clone)]
enum StrideBuilder {
    NoValues,
    OneValue { first: usize },
    Constant { delta: usize, prev: usize },
    Variable,
}

impl StrideBuilder {
    fn new() -> Self {
        Self::NoValues
    }
    fn push(&mut self, value: usize) {
        *self = match *self {
            StrideBuilder::NoValues => StrideBuilder::OneValue { first: value },
            StrideBuilder::OneValue { first } => StrideBuilder::Constant {
                delta: value - first,
                prev: value,
            },
            StrideBuilder::Constant { delta, prev } => {
                if value - prev == delta {
                    StrideBuilder::Constant { delta, prev: value }
                } else {
                    StrideBuilder::Variable
                }
            }
            StrideBuilder::Variable => StrideBuilder::Variable,
        };
    }
    fn get_stride(&self) -> Option<usize> {
        if let StrideBuilder::Constant { delta, .. } = self {
            Some(*delta)
        } else {
            None
        }
    }
}

struct DataBlockBuilder {
    parameters: Rc<Parameters>,
    raw: FBuf,
    value_offsets: Vec<usize>,
    value_offset_stride: StrideBuilder,
    row_groups: ContiguousRanges,
    size_target: Option<usize>,
}

struct DataBuildSpecs {
    value_map: VarintWriter,
    row_groups: Option<VarintWriter>,
    len: usize,
}

struct DataBlock<K> {
    raw: FBuf,
    min_max: (K, K),
    n_values: usize,
}

impl DataBlockBuilder {
    fn new(parameters: &Rc<Parameters>) -> Self {
        let mut raw = FBuf::with_capacity(parameters.min_data_block);
        raw.resize(DataBlockHeader::LEN, 0);
        Self {
            parameters: parameters.clone(),
            raw,
            row_groups: ContiguousRanges::with_capacity(parameters.min_branch),
            value_offsets: Vec::with_capacity(parameters.min_branch),
            value_offset_stride: StrideBuilder::new(),
            size_target: None,
        }
    }
    fn is_empty(&self) -> bool {
        self.value_offsets.is_empty()
    }
    fn take(&mut self) -> DataBlockBuilder {
        replace(self, Self::new(&self.parameters))
    }
    fn try_add_item<K, A>(&mut self, item: (&K, &A), row_group: &Option<Range<u64>>) -> bool
    where
        K: Rkyv,
        A: Rkyv,
    {
        if self.value_offsets.len() >= self.parameters.max_branch() {
            return false;
        }

        let old_len = self.raw.len();
        let old_stride = self.value_offset_stride;

        let offset = rkyv_serialize(&mut self.raw, &Item(item.0, item.1));
        self.value_offsets.push(offset);
        self.value_offset_stride.push(offset);
        if let Some(row_group) = row_group.as_ref() {
            self.row_groups.push(row_group);
        }

        if let Some(size_target) = self.size_target {
            if self.specs().len > size_target {
                self.raw.resize(old_len, 0);
                self.value_offsets.pop();
                self.value_offset_stride = old_stride;
                if row_group.is_some() {
                    self.row_groups.pop();
                }
                return false;
            }
        } else if self.value_offsets.len() >= self.parameters.min_branch {
            self.size_target = Some(
                self.specs()
                    .len
                    .next_power_of_two()
                    .max(self.parameters.min_data_block),
            );
        }

        true
    }
    fn add_item<K, A>(
        &mut self,
        item: (&K, &A),
        row_group: &Option<Range<u64>>,
    ) -> Option<DataBlock<K>>
    where
        K: Rkyv,
        A: Rkyv,
    {
        if self.try_add_item(item, row_group) {
            None
        } else {
            let retval = self.take().build::<K, A>();
            assert!(self.try_add_item(item, row_group));
            Some(retval)
        }
    }

    fn specs(&self) -> DataBuildSpecs {
        debug_assert!(!self.is_empty());
        let len = self.raw.len();

        let value_map = match self.value_offset_stride.get_stride() {
            // General case.
            None => VarintWriter::new(
                Varint::from_len(self.raw.len()),
                len,
                self.value_offsets.len(),
            ),

            // Optimization for constant stride.  We need a starting offset and
            // a stride, both 32 bits.
            Some(_) => VarintWriter::new(Varint::B32, len, 2),
        };
        let len = value_map.offset_after();

        let row_groups = self.row_groups.max().map(|max| {
            debug_assert_eq!(self.row_groups.0.len(), self.value_offsets.len() + 1);
            VarintWriter::new(Varint::from_max_value(max), len, self.row_groups.0.len())
        });
        let len = VarintWriter::offset_after_or(&row_groups, len);

        DataBuildSpecs {
            value_map,
            row_groups,
            len,
        }
    }
    fn build<K, A>(mut self) -> DataBlock<K>
    where
        K: Rkyv,
        A: Rkyv,
    {
        let specs = self.specs();

        self.raw
            .reserve(specs.len.saturating_sub(self.raw.capacity()));

        let value_map_varint = if let Some(stride) = self.value_offset_stride.get_stride() {
            specs.value_map.put(
                &mut self.raw,
                [self.value_offsets[0] as u64, stride as u64].into_iter(),
            );
            None
        } else {
            specs.value_map.put(
                &mut self.raw,
                self.value_offsets.iter().map(|offset| *offset as u64),
            );
            Some(specs.value_map.varint)
        };

        let (row_group_varint, row_groups_ofs) = if let Some(row_groups) = specs.row_groups.as_ref()
        {
            row_groups.put(&mut self.raw, self.row_groups.0.iter().copied());
            (Some(row_groups.varint), row_groups.start as u32)
        } else {
            (None, 0)
        };

        let n_values = self.value_offsets.len();
        let header = DataBlockHeader {
            header: BlockHeader::new(b"LFDB"),
            n_values: n_values as u32,
            value_map_varint,
            row_group_varint,
            value_map_ofs: specs.value_map.start as u32,
            row_groups_ofs,
        };
        header.overwrite_head(&mut self.raw);

        let min_offset = *self.value_offsets.first().unwrap();
        let min = rkyv_deserialize_key::<K, A>(&self.raw, min_offset);

        let max_offset = *self.value_offsets.last().unwrap();
        let max = rkyv_deserialize_key::<K, A>(&self.raw, max_offset);

        DataBlock {
            raw: self.raw,
            min_max: (min, max),
            n_values,
        }
    }
}

struct IndexEntry {
    child: BlockLocation,
    min_offset: usize,
    max_offset: usize,
    row_total: u64,
}

struct ContiguousRanges(Vec<u64>);

impl ContiguousRanges {
    fn with_capacity(capacity: usize) -> Self {
        Self(Vec::with_capacity(capacity.saturating_add(1)))
    }
    fn push(&mut self, range: &Range<u64>) {
        match self.0.last() {
            Some(&last) => {
                debug_assert_eq!(last, range.start);
            }
            None => self.0.push(range.start),
        };
        self.0.push(range.end);
    }
    fn pop(&mut self) {
        match self.0.len() {
            0 | 1 => unreachable!(),
            2 => self.0.clear(),
            _ => {
                self.0.pop();
            }
        }
    }
    fn max(&self) -> Option<u64> {
        self.0.last().copied()
    }
}

struct IndexBlockBuilder {
    parameters: Rc<Parameters>,
    column_index: usize,
    raw: FBuf,
    entries: Vec<IndexEntry>,
    child_type: NodeType,
    size_target: Option<usize>,
}

struct IndexBuildSpecs {
    bound_map: VarintWriter,
    row_totals: VarintWriter,
    child_pointers: VarintWriter,
    len: usize,
}

struct IndexBlock<K> {
    raw: FBuf,
    min_max: (K, K),
    n_rows: u64,
}

fn rkyv_deserialize<K>(src: &FBuf, offset: usize) -> K
where
    K: Rkyv,
{
    let archived = unsafe { archived_value::<K>(src.as_slice(), offset) };
    archived.deserialize(&mut Infallible).unwrap()
}

fn rkyv_deserialize_key<K, A>(src: &FBuf, offset: usize) -> K
where
    K: Rkyv,
    A: Rkyv,
{
    let archived = unsafe { archived_value::<Item<K, A>>(src.as_slice(), offset) };
    archived.0.deserialize(&mut Infallible).unwrap()
}

fn rkyv_serialize<T>(dst: &mut FBuf, value: &T) -> usize
where
    T: Archive + Serialize<Serializer>,
{
    let old_len = dst.len();

    let mut serializer = Serializer::new(
        FBufSerializer::new(take(dst)),
        Default::default(),
        Default::default(),
    );
    let offset = serializer.serialize_value(value).unwrap();
    *dst = serializer.into_components().0.into_inner();

    if dst.len() == old_len {
        // Ensure that a value takes up at least one byte.  Otherwise, we'll
        // have to think hard about how fitting an unbounded number of values in
        // a block works with our other assumptions.
        dst.push(0);
    }
    offset
}

impl IndexBlockBuilder {
    fn new(parameters: &Rc<Parameters>, column_index: usize, child_type: NodeType) -> Self {
        let mut raw = FBuf::with_capacity(parameters.min_index_block);
        raw.resize(IndexBlockHeader::LEN, 0);

        Self {
            parameters: parameters.clone(),
            column_index,
            raw,
            entries: Vec::with_capacity(parameters.min_branch),
            child_type,
            size_target: None,
        }
    }
    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
    fn take(&mut self) -> IndexBlockBuilder {
        replace(
            self,
            Self::new(&self.parameters, self.column_index, self.child_type),
        )
    }
    fn try_add_entry<K>(&mut self, child: BlockLocation, min_max: &(K, K), n_rows: u64) -> bool
    where
        K: Rkyv,
    {
        if self.entries.len() >= self.parameters.max_branch() {
            return false;
        }
        let saved_len = self.raw.len();

        let min_offset = rkyv_serialize(&mut self.raw, &min_max.0);
        let max_offset = rkyv_serialize(&mut self.raw, &min_max.1);
        self.entries.push(IndexEntry {
            child,
            min_offset,
            max_offset,
            row_total: self.entries.last().map_or(0, |entry| entry.row_total) + n_rows,
        });

        if let Some(size_target) = self.size_target {
            if self.specs().len > size_target {
                self.raw.resize(saved_len, 0);
                self.entries.pop();
                return false;
            }
        } else if self.entries.len() >= self.parameters.min_branch {
            self.size_target = Some(
                self.specs()
                    .len
                    .next_power_of_two()
                    .max(self.parameters.min_index_block),
            );
        }

        true
    }
    fn add_entry<K>(
        &mut self,
        child: BlockLocation,
        min_max: &(K, K),
        n_rows: u64,
    ) -> Option<IndexBlock<K>>
    where
        K: Rkyv,
    {
        let f = |t: &mut Self| t.try_add_entry(child, min_max, n_rows);
        if f(self) {
            None
        } else {
            let retval = self.take().build();
            assert!(f(self));
            Some(retval)
        }
    }
    fn specs(&self) -> IndexBuildSpecs {
        debug_assert!(!self.entries.is_empty());
        let len = self.raw.len();

        let bound_map = VarintWriter::new(
            Varint::from_len(self.raw.len()),
            len,
            self.entries.len() * 2,
        );
        let len = bound_map.offset_after();

        let row_totals = {
            let max_row_total = self.entries.last().unwrap().row_total;
            VarintWriter::new(
                Varint::from_max_value(max_row_total),
                len,
                self.entries.len(),
            )
        };
        let len = row_totals.offset_after();

        let child_pointers = VarintWriter::new(
            Varint::from_max_value(self.entries.last().unwrap().child.offset),
            len,
            self.entries.len(),
        );
        let len = child_pointers.offset_after();

        IndexBuildSpecs {
            bound_map,
            row_totals,
            child_pointers,
            len,
        }
    }
    fn build<K>(mut self) -> IndexBlock<K>
    where
        K: Rkyv,
    {
        let specs = self.specs();

        self.raw
            .reserve(specs.len.saturating_sub(self.raw.capacity()));

        specs.bound_map.put(
            &mut self.raw,
            self.entries
                .iter()
                .flat_map(|entry| [entry.min_offset as u64, entry.max_offset as u64]),
        );

        specs.row_totals.put(
            &mut self.raw,
            self.entries.iter().map(|entry| entry.row_total),
        );

        specs.child_pointers.put(
            &mut self.raw,
            self.entries.iter().map(|entry| entry.child.into()),
        );

        let header = IndexBlockHeader {
            header: BlockHeader::new(b"LFIB"),
            bound_map_offset: specs.bound_map.start as u32,
            row_totals_offset: specs.row_totals.start as u32,
            child_pointers_offset: specs.child_pointers.start as u32,
            n_children: self.entries.len() as u16,
            child_type: self.child_type,
            bound_map_varint: specs.bound_map.varint,
            row_total_varint: specs.row_totals.varint,
            child_pointer_varint: specs.child_pointers.varint,
        };
        header.overwrite_head(&mut self.raw);

        let entry_0 = self.entries.first().unwrap();
        let min = rkyv_deserialize(&self.raw, entry_0.min_offset);

        let entry_n = self.entries.last().unwrap();
        let max = rkyv_deserialize(&self.raw, entry_n.max_offset);

        IndexBlock {
            raw: self.raw,
            min_max: (min, max),
            n_rows: entry_n.row_total,
        }
    }
}

struct BlockWriter<W>
where
    W: StorageWrite + StorageControl + StorageExecutor,
{
    storage: Rc<W>,
    file_handle: Option<FileHandle>,
    offset: u64,
}

impl<W> BlockWriter<W>
where
    W: StorageWrite + StorageControl + StorageExecutor,
{
    fn new(storage: &Rc<W>, file_handle: FileHandle) -> Self {
        Self {
            storage: storage.clone(),
            file_handle: Some(file_handle),
            offset: 0,
        }
    }

    fn complete(mut self) -> Result<ImmutableFileHandle, StorageError> {
        self.storage
            .block_on(self.storage.complete(self.file_handle.take().unwrap()))
    }

    fn write_block(&mut self, mut block: FBuf) -> Result<BlockLocation, StorageError> {
        block.resize(block.len().max(4096).next_power_of_two(), 0);
        let checksum = crc32c(&block[4..]).to_le_bytes();
        block[..4].copy_from_slice(checksum.as_slice());

        let location = BlockLocation::new(self.offset, block.len()).unwrap();
        self.offset += block.len() as u64;
        self.storage.block_on(self.storage.write_block(
            self.file_handle.as_ref().unwrap(),
            location.offset,
            block,
        ))?;
        Ok(location)
    }
}

impl<W> Drop for BlockWriter<W>
where
    W: StorageWrite + StorageControl + StorageExecutor,
{
    fn drop(&mut self) {
        self.file_handle
            .take()
            .map(|file_handle| self.storage.block_on(self.storage.delete_mut(file_handle)));
    }
}

/// General-purpose layer file writer.
///
/// A `Writer` can write a layer file with any number of columns.  It lacks type
/// safety to ensure that the data and auxiliary values written to columns are
/// all the same type.  Thus, [`Writer1`] and [`Writer2`] exist for writing
/// 1-column and 2-column layer files, respectively, with added type safety.
struct Writer<W>
where
    W: StorageWrite + StorageControl + StorageExecutor,
{
    writer: BlockWriter<W>,
    cws: Vec<ColumnWriter>,
    finished_columns: Vec<FileTrailerColumn>,
}

impl<W> Writer<W>
where
    W: StorageControl + StorageWrite + StorageExecutor,
{
    pub fn new(
        writer: &Rc<W>,
        parameters: Parameters,
        n_columns: usize,
    ) -> Result<Self, StorageError> {
        let parameters = Rc::new(parameters);
        let cws = (0..n_columns)
            .map(|column| ColumnWriter::new(&parameters, column))
            .collect();
        let finished_columns = Vec::with_capacity(n_columns);
        let writer = Self {
            writer: BlockWriter::new(writer, writer.block_on(writer.create())?),
            cws,
            finished_columns,
        };
        Ok(writer)
    }

    pub fn write<K, A>(&mut self, column: usize, item: (&K, &A)) -> Result<(), StorageError>
    where
        K: Rkyv + Ord,
        A: Rkyv,
    {
        let row_group = if column + 1 < self.n_columns() {
            let row_group = self.cws[column + 1].take_rows();
            assert!(!row_group.is_empty());
            Some(row_group)
        } else {
            None
        };

        // Add `value` to row group for column.
        self.cws[column].rows.end += 1;
        self.cws[column].add_item(&mut self.writer, item, &row_group)
    }

    pub fn finish_column<K, A>(&mut self, column: usize) -> Result<(), StorageError>
    where
        K: Rkyv + Ord,
        A: Rkyv,
    {
        debug_assert_eq!(column, self.finished_columns.len());
        for cw in self.cws.iter().skip(1) {
            assert!(cw.rows.is_empty());
        }

        self.finished_columns
            .push(self.cws[column].finish::<W, K, A>(&mut self.writer)?);
        Ok(())
    }

    pub fn close(mut self) -> Result<ImmutableFileHandle, StorageError> {
        debug_assert_eq!(self.cws.len(), self.finished_columns.len());

        // Write the file trailer block.
        let file_trailer = FileTrailer {
            header: BlockHeader::new(b"LFFT"),
            version: VERSION_NUMBER,
            columns: take(&mut self.finished_columns),
        };
        self.writer.write_block(file_trailer.into_block())?;

        self.writer.complete()
    }

    pub fn n_columns(&self) -> usize {
        self.cws.len()
    }

    pub fn n_rows(&self) -> u64 {
        self.cws[0].rows.end
    }

    pub fn storage(&self) -> &Rc<W> {
        &self.writer.storage
    }
}

/// 1-column layer file writer.
///
/// `Writer1<W, K0, A0>` writes a new 1-column layer file in which column 0 has
/// key and auxiliary data types `(K0, A0)`.
///
/// # Example
///
/// The following code writes 1000 rows in column 0 with values `(0, ())`
/// through `(999, ())`.
///
/// ```
/// # use feldera_storage::file::writer::{Parameters, Writer1};
/// # use feldera_storage::backend::DefaultBackend;
/// let mut file =
///     Writer1::new(&DefaultBackend::default_for_thread(), Parameters::default()).unwrap();
/// for i in 0..1000_u32 {
///     file.write0((&i, &())).unwrap();
/// }
/// file.close().unwrap();
/// ```
pub struct Writer1<W, K0, A0>
where
    W: StorageWrite + StorageControl + StorageExecutor,
    K0: Rkyv,
    A0: Rkyv,
{
    inner: Writer<W>,
    _phantom: PhantomData<(K0, A0)>,
}

impl<S, K0, A0> Writer1<S, K0, A0>
where
    S: StorageControl + StorageWrite + StorageExecutor,
    K0: Rkyv + Ord,
    A0: Rkyv,
{
    /// Creates a new writer with the given parameters.
    pub fn new(storage: &Rc<S>, parameters: Parameters) -> Result<Self, StorageError> {
        Ok(Self {
            inner: Writer::new(storage, parameters, 1)?,
            _phantom: PhantomData,
        })
    }
    /// Writes `item` to column 0.  `item.0` must be greater than passed in the
    /// previous call to this function (if any).
    pub fn write0(&mut self, item: (&K0, &A0)) -> Result<(), StorageError> {
        self.inner.write(0, item)
    }

    /// Returns the number of calls to [`write0`](Self::write0) so far.
    pub fn n_rows(&self) -> u64 {
        self.inner.n_rows()
    }

    /// Finishes writing the layer file and returns the writer passed to
    /// [`new`](Self::new).
    pub fn close(mut self) -> Result<ImmutableFileHandle, StorageError> {
        self.inner.finish_column::<K0, A0>(0)?;
        self.inner.close()
    }

    /// Returns the storage used for this writer.
    pub fn storage(&self) -> &Rc<S> {
        self.inner.storage()
    }

    /// Finishes writing the layer file and returns a reader for it.
    pub fn into_reader(self) -> Result<Reader<S, (K0, A0, ())>, super::reader::Error>
    where
        S: StorageRead,
    {
        let storage = self.storage().clone();
        let file_handle = self.close()?;
        Reader::new(Rc::new(ImmutableFileRef::new(&storage, file_handle)))
    }
}

/// 2-column layer file writer.
///
/// `Writer2<W, K0, A0, K1, A1>` writes a new 2-column layer file in which
/// column 0 has key and auxiliary data types `(K0, A0)` and column 1 has `(K1,
/// A1)`.
///
/// Each row in column 0 must be associated with a group of one or more rows in
/// column 1.  To form the association, first write the rows to column 1 using
/// [`write1`](Self::write1) then the row to column 0 with
/// [`write0`](Self::write0).
///
/// # Example
///
/// The following code writes 1000 rows in column 0 with values `(0, ())`
/// through `(999, ())`, each associated with 10 rows in column 1 with values
/// `(0, ())` through `(9, ())`.
///
/// ```
/// # use feldera_storage::file::writer::{Parameters, Writer2};
/// # use feldera_storage::backend::DefaultBackend;
/// let mut file =
///     Writer2::new(&DefaultBackend::default_for_thread(), Parameters::default()).unwrap();
/// for i in 0..1000_u32 {
///     for j in 0..10_u32 {
///         file.write1((&j, &())).unwrap();
///     }
///     file.write0((&i, &())).unwrap();
/// }
/// file.close().unwrap();
/// ```
pub struct Writer2<W, K0, A0, K1, A1>
where
    W: StorageWrite + StorageControl + StorageExecutor,
    K0: Rkyv + Ord,
    A0: Rkyv,
    K1: Rkyv + Ord,
    A1: Rkyv,
{
    inner: Writer<W>,
    _phantom: PhantomData<(K0, A0, K1, A1)>,
}

impl<S, K0, A0, K1, A1> Writer2<S, K0, A0, K1, A1>
where
    S: StorageControl + StorageWrite + StorageExecutor,
    K0: Rkyv + Ord,
    A0: Rkyv,
    K1: Rkyv + Ord,
    A1: Rkyv,
{
    /// Creates a new writer with the given parameters.
    pub fn new(storage: &Rc<S>, parameters: Parameters) -> Result<Self, StorageError> {
        Ok(Self {
            inner: Writer::new(storage, parameters, 2)?,
            _phantom: PhantomData,
        })
    }
    /// Writes `item` to column 0.  All of the items previously written to
    /// column 1 since the last call to this function (if any) become the row
    /// group associated with `item`.  There must be at least one such item.
    ///
    /// `item.0` must be greater than passed in the previous call to this
    /// function (if any).
    pub fn write0(&mut self, item: (&K0, &A0)) -> Result<(), StorageError> {
        self.inner.write(0, item)
    }

    /// Writes `item` to column 1.  `item.0` must be greater than passed in the
    /// previous call to this function (if any) since the last call to
    /// [`write0`](Self::write0) (if any).
    pub fn write1(&mut self, item: (&K1, &A1)) -> Result<(), StorageError> {
        self.inner.write(1, item)
    }

    /// Returns the number of calls to [`write0`](Self::write0) so far.
    pub fn n_rows(&self) -> u64 {
        self.inner.n_rows()
    }

    /// Finishes writing the layer file and returns the writer passed to
    /// [`new`](Self::new).
    ///
    /// This function will panic if [`write1`](Self::write1) has been called
    /// without a subsequent call to [`write0`](Self::write0).
    pub fn close(mut self) -> Result<ImmutableFileHandle, StorageError> {
        self.inner.finish_column::<K0, A0>(0)?;
        self.inner.finish_column::<K1, A1>(1)?;
        self.inner.close()
    }

    /// Returns the storage used for this writer.
    pub fn storage(&self) -> &Rc<S> {
        self.inner.storage()
    }

    /// Finishes writing the layer file and returns a reader for it.
    #[allow(clippy::type_complexity)]
    pub fn into_reader(self) -> Result<Reader<S, (K0, A0, (K1, A1, ()))>, super::reader::Error>
    where
        S: StorageRead,
    {
        let storage = self.storage().clone();
        let file_handle = self.close()?;
        Reader::new(Rc::new(ImmutableFileRef::new(&storage, file_handle)))
    }
}
