//! Layer file writer.
//!
//! Use [`Writer1`] to write a 1-column layer file and [`Writer2`] to write a
//! 2-column layer file.  To write more columns, either add another `Writer<N>`
//! struct, which is easily done, or mark the currently private `Writer` as
//! `pub`.
use crate::storage::{
    backend::{BlockLocation, FileReader, FileWriter, StorageBackend, StorageError},
    buffer_cache::{BufferCache, CacheEntry, FBuf, FBufSerializer, LimitExceeded},
    file::{
        format::{
            BlockHeader, DataBlockHeader, FileTrailer, FileTrailerColumn, FilterBlockRef, FixedLen,
            IndexBlockHeader, NodeType, Varint, DATA_BLOCK_MAGIC, FILE_TRAILER_BLOCK_MAGIC,
            INDEX_BLOCK_MAGIC, VERSION_NUMBER,
        },
        reader::TreeNode,
        with_serializer, BLOOM_FILTER_SEED,
    },
};
use binrw::{
    io::{Cursor, NoSeek},
    BinWrite,
};
use crc32c::crc32c;
#[cfg(debug_assertions)]
use dyn_clone::clone_box;
use fastbloom::BloomFilter;
use feldera_storage::StoragePath;
use snap::raw::{max_compress_len, Encoder};
use std::{cell::RefCell, sync::Arc};
use std::{
    marker::PhantomData,
    mem::{replace, take},
    ops::Range,
};

use crate::{
    dynamic::{DataTrait, DeserializeDyn, SerializeDyn},
    storage::file::ItemFactory,
    Runtime,
};

use super::format::Compression;
use super::{reader::Reader, AnyFactories, Factories, BLOOM_FILTER_FALSE_POSITIVE_RATE};

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
#[derive(Clone, Debug)]
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

    /// How to compress input and data blocks in the output file.
    pub compression: Option<Compression>,
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
    pub fn with_max_branch(self, max_branch: usize) -> Self {
        Self { max_branch, ..self }
    }

    /// Returns these parameters with `compression` updated.
    pub fn with_compression(self, compression: Option<Compression>) -> Self {
        Self {
            compression,
            ..self
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
            compression: Some(Compression::Snappy),
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
    parameters: Arc<Parameters>,
    rows: Range<u64>,
    data_block: DataBlockBuilder,
    index_blocks: Vec<IndexBlockBuilder>,
    factories: AnyFactories,
}

impl ColumnWriter {
    fn new(factories: &AnyFactories, parameters: &Arc<Parameters>) -> Self {
        ColumnWriter {
            parameters: parameters.clone(),
            rows: 0..0,
            data_block: DataBlockBuilder::new(factories, parameters),
            index_blocks: Vec::new(),
            factories: factories.clone(),
        }
    }

    fn take_rows(&mut self) -> Range<u64> {
        let end = self.rows.end;
        replace(&mut self.rows, end..end)
    }

    fn finish<K, A>(
        &mut self,
        block_writer: &mut BlockWriter,
    ) -> Result<FileTrailerColumn, StorageError>
    where
        K: DataTrait + ?Sized,
        A: DataTrait + ?Sized,
    {
        // Flush data.
        if !self.data_block.is_empty() {
            let data_block = self.data_block.build::<K, A>();
            self.write_data_block::<K, A>(block_writer, data_block)?;
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
                let index_block = self.index_blocks[level].build();
                self.write_index_block::<K>(block_writer, index_block, level)?;
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
                &self.factories,
                &self.parameters,
                if level == 0 {
                    NodeType::Data
                } else {
                    NodeType::Index
                },
            ));
        }
        &mut self.index_blocks[level]
    }

    fn write_data_block<K, A>(
        &mut self,
        block_writer: &mut BlockWriter,
        data_block: DataBlock<K>,
    ) -> Result<(), StorageError>
    where
        K: DataTrait + ?Sized,
        A: DataTrait + ?Sized,
    {
        let rows = data_block.rows();
        let (block, location) =
            block_writer.write_block(data_block.raw, self.parameters.compression)?;

        super::reader::DataBlock::<K, A>::from_raw_with_cache(
            block,
            &TreeNode {
                location,
                node_type: NodeType::Data,
                rows,
            },
            &block_writer.cache,
            block_writer.file_handle.as_ref().unwrap().file_id(),
        )
        .unwrap();

        if let Some(index_block) = self.get_index_block(0).add_entry(
            location,
            &data_block.min_max,
            data_block.n_rows as u64,
        ) {
            self.write_index_block::<K>(block_writer, index_block, 0)?;
        }
        Ok(())
    }

    fn write_index_block<K>(
        &mut self,
        block_writer: &mut BlockWriter,
        mut index_block: IndexBlock<K>,
        mut level: usize,
    ) -> Result<(), StorageError>
    where
        K: DataTrait + ?Sized,
    {
        loop {
            let rows = index_block.rows.clone();
            let n_rows = index_block.n_rows();
            let (block, location) =
                block_writer.write_block(index_block.raw, self.parameters.compression)?;
            super::reader::IndexBlock::<K>::from_raw_with_cache(
                block,
                &TreeNode {
                    location,
                    node_type: NodeType::Index,
                    rows,
                },
                &block_writer.cache,
                block_writer.file_handle.as_ref().unwrap().file_id(),
            )
            .unwrap();

            level += 1;
            let opt_index_block =
                self.get_index_block(level)
                    .add_entry(location, &index_block.min_max, n_rows);
            index_block = match opt_index_block {
                None => return Ok(()),
                Some(index_block) => index_block,
            };
        }
    }

    fn add_item<K, A>(
        &mut self,
        block_writer: &mut BlockWriter,
        item: (&K, &A),
        row_group: &Option<Range<u64>>,
    ) -> Result<(), StorageError>
    where
        K: DataTrait + ?Sized,
        A: DataTrait + ?Sized,
    {
        if let Some(data_block) = self.data_block.add_item(item, row_group) {
            self.write_data_block::<K, A>(block_writer, data_block)?;
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
    fn clear(&mut self) {
        *self = Self::NoValues;
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
    parameters: Arc<Parameters>,
    raw: FBuf,
    value_offsets: Vec<usize>,
    value_offset_stride: StrideBuilder,
    row_groups: ContiguousRanges,
    size_target: Option<usize>,
    factories: AnyFactories,
    first_row: u64,
}

struct DataBuildSpecs {
    value_map: VarintWriter,
    row_groups: Option<VarintWriter>,
    len: usize,
}

struct DataBlock<K: ?Sized> {
    raw: FBuf,
    min_max: (Box<K>, Box<K>),
    n_rows: usize,
    first_row: u64,
}

impl<K> DataBlock<K>
where
    K: ?Sized,
{
    fn rows(&self) -> Range<u64> {
        self.first_row..self.first_row + self.n_rows as u64
    }
}

impl DataBlockBuilder {
    fn new(factories: &AnyFactories, parameters: &Arc<Parameters>) -> Self {
        let mut raw = FBuf::with_capacity(parameters.min_data_block);
        raw.resize(DataBlockHeader::LEN, 0);
        Self {
            parameters: parameters.clone(),
            raw,
            row_groups: ContiguousRanges::with_capacity(parameters.min_branch),
            value_offsets: Vec::with_capacity(parameters.min_branch),
            value_offset_stride: StrideBuilder::new(),
            size_target: None,
            factories: factories.clone(),
            first_row: 0,
        }
    }
    fn clear(&mut self) {
        self.raw.clear();
        self.raw.resize(DataBlockHeader::LEN, 0);
        self.row_groups.clear();
        self.value_offsets.clear();
        self.value_offset_stride.clear();
        self.size_target = None;
    }
    fn is_empty(&self) -> bool {
        self.value_offsets.is_empty()
    }
    fn try_add_item<K, A>(
        &mut self,
        item: (&K, &A),
        row_group: &Option<Range<u64>>,
    ) -> Result<(), LimitExceeded>
    where
        K: DataTrait + ?Sized,
        A: DataTrait + ?Sized,
    {
        if self.value_offsets.len() >= self.parameters.max_branch() {
            return Err(LimitExceeded);
        }

        let old_len = self.raw.len();
        let old_stride = self.value_offset_stride;

        let mut result = Ok(0);
        self.factories
            .item_factory()
            .with(item.0, item.1, &mut |item| {
                result =
                    rkyv_serialize(&mut self.raw, item, self.size_target.unwrap_or(usize::MAX));
            });
        let offset = result.inspect_err(|_| self.raw.resize(old_len, 0))?;

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
                return Err(LimitExceeded);
            }
        } else if self.value_offsets.len() >= self.parameters.min_branch {
            self.size_target = Some(
                self.specs()
                    .len
                    .next_multiple_of(512)
                    .max(self.parameters.min_data_block),
            );
        }

        Ok(())
    }
    fn add_item<K, A>(
        &mut self,
        item: (&K, &A),
        row_group: &Option<Range<u64>>,
    ) -> Option<DataBlock<K>>
    where
        K: DataTrait + ?Sized,
        A: DataTrait + ?Sized,
    {
        if self.try_add_item(item, row_group).is_ok() {
            None
        } else {
            let retval = self.build::<K, A>();
            assert!(self.try_add_item(item, row_group).is_ok());
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
    fn build<K, A>(&mut self) -> DataBlock<K>
    where
        K: DataTrait + ?Sized,
        A: DataTrait + ?Sized,
    {
        let key_factory = self.factories.key_factory::<K>();
        let item_factory = self.factories.item_factory::<K, A>();

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
            header: BlockHeader::new(&DATA_BLOCK_MAGIC),
            n_values: n_values as u32,
            value_map_varint,
            row_group_varint,
            value_map_ofs: specs.value_map.start as u32,
            row_groups_ofs,
        };
        header.overwrite_head(&mut self.raw);

        let mut min = key_factory.default_box();
        let min_offset = *self.value_offsets.first().unwrap();
        rkyv_deserialize_key::<K, A>(item_factory, &self.raw, min_offset, min.as_mut());

        let mut max = key_factory.default_box();
        let max_offset = *self.value_offsets.last().unwrap();
        rkyv_deserialize_key::<K, A>(item_factory, &self.raw, max_offset, max.as_mut());

        // Take our data buffer, replacing it by a new one with a capacity big
        // enough for the data in the current one.  We round up to a multiple of
        // 512 because our caller will have to do that anyhow to write the data
        // to a storage object (unless the data is being compressed, but
        // rounding up a bit is harmless for that case).
        let new_capacity = self.raw.len().next_multiple_of(512);
        let raw = replace(&mut self.raw, FBuf::with_capacity(new_capacity));

        let data_block = DataBlock {
            raw,
            min_max: (min, max),
            n_rows: n_values,
            first_row: self.first_row,
        };
        self.first_row += self.value_offsets.len() as u64;
        self.clear();
        data_block
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
    fn clear(&mut self) {
        self.0.clear();
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
    parameters: Arc<Parameters>,
    raw: FBuf,
    entries: Vec<IndexEntry>,
    child_type: NodeType,
    size_target: Option<usize>,
    factories: AnyFactories,
    max_child_size: usize,
    first_row: u64,
}

struct IndexBuildSpecs {
    bound_map: VarintWriter,
    row_totals: VarintWriter,
    child_offsets: VarintWriter,
    child_sizes: VarintWriter,
    len: usize,
}

struct IndexBlock<K: ?Sized> {
    raw: FBuf,
    min_max: (Box<K>, Box<K>),
    rows: Range<u64>,
}

impl<K: ?Sized> IndexBlock<K> {
    fn n_rows(&self) -> u64 {
        self.rows.end - self.rows.start
    }
}

fn rkyv_deserialize<K>(src: &FBuf, offset: usize, key: &mut K)
where
    K: DataTrait + ?Sized,
{
    unsafe { key.deserialize_from_bytes(src.as_slice(), offset) };
}

fn rkyv_deserialize_key<K, A>(
    factory: &'static dyn ItemFactory<K, A>,
    src: &FBuf,
    offset: usize,
    key: &mut K,
) where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    DeserializeDyn::deserialize(
        unsafe { factory.archived_value(src.as_slice(), offset).fst() },
        key,
    )
}

fn rkyv_serialize<T>(dst: &mut FBuf, value: &T, limit: usize) -> Result<usize, LimitExceeded>
where
    T: SerializeDyn + ?Sized,
{
    let old_len = dst.len();

    let result;
    (*dst, result) = with_serializer(FBufSerializer::new(take(dst), limit), |serializer| {
        value.serialize(serializer)
    });
    let offset = result.map_err(|_| LimitExceeded)?;

    if dst.len() == old_len {
        // Ensure that a value takes up at least one byte.  Otherwise, we'll
        // have to think hard about how fitting an unbounded number of values in
        // a block works with our other assumptions.
        dst.push(0);
    }
    Ok(offset)
}

impl IndexBlockBuilder {
    fn new(factories: &AnyFactories, parameters: &Arc<Parameters>, child_type: NodeType) -> Self {
        let mut raw = FBuf::with_capacity(parameters.min_index_block);
        raw.resize(IndexBlockHeader::LEN, 0);

        Self {
            parameters: parameters.clone(),
            raw,
            entries: Vec::with_capacity(parameters.min_branch),
            child_type,
            size_target: None,
            factories: factories.clone(),
            max_child_size: 0,
            first_row: 0,
        }
    }
    fn clear(&mut self) {
        self.raw.clear();
        self.raw.resize(IndexBlockHeader::LEN, 0);
        self.entries.clear();
        self.size_target = None;
        self.max_child_size = 0;
    }
    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
    fn inner_try_add_entry<K>(
        &mut self,
        child: BlockLocation,
        min_max: &(Box<K>, Box<K>),
        n_rows: u64,
    ) -> Result<(), LimitExceeded>
    where
        K: DataTrait + ?Sized,
    {
        if self.entries.len() >= self.parameters.max_branch() {
            return Err(LimitExceeded);
        }
        self.max_child_size = self.max_child_size.max(child.size);
        let limit = self.size_target.unwrap_or(usize::MAX);
        let min_offset = rkyv_serialize(&mut self.raw, min_max.0.as_ref(), limit)?;
        let max_offset = rkyv_serialize(&mut self.raw, min_max.1.as_ref(), limit)?;
        self.entries.push(IndexEntry {
            child,
            min_offset,
            max_offset,
            row_total: self.entries.last().map_or(0, |entry| entry.row_total) + n_rows,
        });

        if let Some(size_target) = self.size_target {
            if self.specs().len > size_target {
                return Err(LimitExceeded);
            }
        } else if self.entries.len() >= self.parameters.min_branch {
            self.size_target = Some(
                self.specs()
                    .len
                    .next_multiple_of(512)
                    .max(self.parameters.min_index_block),
            );
        }
        Ok(())
    }
    fn try_add_entry<K>(
        &mut self,
        child: BlockLocation,
        min_max: &(Box<K>, Box<K>),
        n_rows: u64,
    ) -> Result<(), LimitExceeded>
    where
        K: DataTrait + ?Sized,
    {
        let saved_len = self.raw.len();
        let saved_max_child_size = self.max_child_size;
        let n_entries = self.entries.len();
        self.inner_try_add_entry(child, min_max, n_rows)
            .inspect_err(|_| {
                self.max_child_size = saved_max_child_size;
                self.raw.resize(saved_len, 0);
                if self.entries.len() > n_entries {
                    self.entries.pop();
                }
            })
    }
    fn add_entry<K>(
        &mut self,
        child: BlockLocation,
        min_max: &(Box<K>, Box<K>),
        n_rows: u64,
    ) -> Option<IndexBlock<K>>
    where
        K: DataTrait + ?Sized,
    {
        let f = |t: &mut Self| t.try_add_entry(child, min_max, n_rows);
        if f(self).is_ok() {
            None
        } else {
            let retval = self.build();
            assert!(f(self).is_ok());
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

        let child_offsets = VarintWriter::new(
            Varint::from_max_value(self.entries.last().unwrap().child.offset),
            len,
            self.entries.len(),
        );
        let len = child_offsets.offset_after();

        let child_sizes = VarintWriter::new(
            Varint::from_max_value((self.max_child_size >> 9) as u64),
            len,
            self.entries.len(),
        );
        let len = child_sizes.offset_after();

        IndexBuildSpecs {
            bound_map,
            row_totals,
            child_offsets,
            child_sizes,
            len,
        }
    }
    fn build<K>(&mut self) -> IndexBlock<K>
    where
        K: DataTrait + ?Sized,
    {
        let key_factory = self.factories.key_factory::<K>();

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

        specs.child_offsets.put(
            &mut self.raw,
            self.entries.iter().map(|entry| entry.child.offset >> 9),
        );

        specs.child_sizes.put(
            &mut self.raw,
            self.entries
                .iter()
                .map(|entry| (entry.child.size >> 9) as u64),
        );

        let header = IndexBlockHeader {
            header: BlockHeader::new(&INDEX_BLOCK_MAGIC),
            bound_map_offset: specs.bound_map.start as u32,
            row_totals_offset: specs.row_totals.start as u32,
            child_offsets_offset: specs.child_offsets.start as u32,
            child_sizes_offset: specs.child_sizes.start as u32,
            n_children: self.entries.len() as u16,
            child_type: self.child_type,
            bound_map_varint: specs.bound_map.varint,
            row_total_varint: specs.row_totals.varint,
            child_offset_varint: specs.child_offsets.varint,
            child_size_varint: specs.child_sizes.varint,
        };
        header.overwrite_head(&mut self.raw);

        let mut min = key_factory.default_box();
        let entry_0 = self.entries.first().unwrap();
        rkyv_deserialize(&self.raw, entry_0.min_offset, min.as_mut());

        let mut max = key_factory.default_box();
        let entry_n = self.entries.last().unwrap();
        rkyv_deserialize(&self.raw, entry_n.max_offset, max.as_mut());

        let capacity = self.raw.capacity();
        let raw = replace(&mut self.raw, FBuf::with_capacity(capacity));
        let index_block = IndexBlock {
            raw,
            min_max: (min, max),
            rows: self.first_row..self.first_row + entry_n.row_total,
        };
        self.first_row += entry_n.row_total;
        self.clear();
        index_block
    }
}

struct BlockWriter {
    cache: Arc<BufferCache>,
    file_handle: Option<Box<dyn FileWriter>>,
    encoder: Encoder,
    offset: u64,
}

impl BlockWriter {
    fn new(cache: Arc<BufferCache>, file_handle: Box<dyn FileWriter>) -> Self {
        Self {
            cache,
            file_handle: Some(file_handle),
            encoder: Encoder::new(),
            offset: 0,
        }
    }

    fn complete(mut self) -> Result<(Arc<dyn FileReader>, StoragePath), StorageError> {
        self.file_handle.take().unwrap().complete()
    }

    fn write_block(
        &mut self,
        mut block: FBuf,
        compression: Option<Compression>,
    ) -> Result<(Arc<FBuf>, BlockLocation), StorageError> {
        // `block` is the uncompressed version.
        // We need to write the compressed version.
        let (uncompressed, location) = if let Some(compression) = compression {
            // Checksum the uncompressed data.
            let checksum = crc32c(&block[4..]).to_le_bytes();
            block[..4].copy_from_slice(checksum.as_slice());

            // Use a thread-local bounce buffer to create an appropriately sized
            // compressed buffer.
            //
            // We could avoid a copy here, at a memory cost, by allocating a
            // maximum-size compressed buffer and compressing directly into
            // that.
            thread_local! { static BOUNCE: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) }};
            let (padded_len, compressed) = BOUNCE.with_borrow_mut(|bounce| {
                // Compress the data into a bounce buffer.
                let compressed_len = match compression {
                    Compression::Snappy => {
                        let max_len = max_compress_len(block.len());
                        if max_len > bounce.len() {
                            bounce.resize(max_len, 0);
                        }
                        self.encoder
                            .compress(block.as_slice(), bounce.as_mut_slice())
                            .unwrap()
                    }
                };

                // Construct compressed buffer as:
                //
                // - `compressed_len` as a 32-bit little-endian integer
                // - compressed data (`compressed_len` bytes)
                // - padding to `padded_len`, which is a multiple of 512 bytes
                let padded_len = (compressed_len + 4).next_multiple_of(512);
                let mut compressed = FBuf::with_capacity(padded_len);
                compressed.extend_from_slice((compressed_len as u32).to_le_bytes().as_slice());
                compressed.extend_from_slice(&bounce[..compressed_len]);
                compressed.resize(padded_len, 0);
                (padded_len, compressed)
            });

            // Write the compressed data (and discard it).
            let location = BlockLocation::new(self.offset, padded_len).unwrap();
            self.file_handle
                .as_mut()
                .unwrap()
                .as_mut()
                .write_block(compressed)?;

            (Arc::new(block), location)
        } else {
            // Pad and checksum the block.
            block.resize(block.len().next_multiple_of(512), 0);
            let checksum = crc32c(&block[4..]).to_le_bytes();
            block[..4].copy_from_slice(checksum.as_slice());

            // Write the block.
            let location = BlockLocation::new(self.offset, block.len()).unwrap();
            let block = self
                .file_handle
                .as_mut()
                .unwrap()
                .as_mut()
                .write_block(block)?;
            (block, location)
        };

        // Construct a cache entry from the uncompressed data.
        self.offset = location.after();
        Ok((uncompressed, location))
    }

    fn insert_cache_entry(&self, location: BlockLocation, entry: Arc<dyn CacheEntry>) {
        self.cache.insert(
            self.file_handle.as_ref().unwrap().file_id(),
            location.offset,
            entry,
        );
    }
}

/// General-purpose layer file writer.
///
/// A `Writer` can write a layer file with any number of columns.  It lacks type
/// safety to ensure that the data and auxiliary values written to columns are
/// all the same type.  Thus, [`Writer1`] and [`Writer2`] exist for writing
/// 1-column and 2-column layer files, respectively, with added type safety.
struct Writer {
    cache: fn() -> Arc<BufferCache>,
    writer: BlockWriter,
    bloom_filter: BloomFilter,
    cws: Vec<ColumnWriter>,
    finished_columns: Vec<FileTrailerColumn>,
}

impl Writer {
    pub fn new(
        factories: &[&AnyFactories],
        cache: fn() -> Arc<BufferCache>,
        storage_backend: &dyn StorageBackend,
        parameters: Parameters,
        n_columns: usize,
        estimated_keys: usize,
    ) -> Result<Self, StorageError> {
        assert_eq!(factories.len(), n_columns);

        let parameters = Arc::new(parameters);
        let cws = factories
            .iter()
            .map(|factories| ColumnWriter::new(factories, &parameters))
            .collect();
        let finished_columns = Vec::with_capacity(n_columns);
        let worker = format!("w{}-", Runtime::worker_index());
        let writer = Self {
            cache,
            writer: BlockWriter::new(cache(), storage_backend.create_with_prefix(&worker.into())?),
            bloom_filter: BloomFilter::with_false_pos(BLOOM_FILTER_FALSE_POSITIVE_RATE)
                .seed(&BLOOM_FILTER_SEED)
                .expected_items(estimated_keys),
            cws,
            finished_columns,
        };
        Ok(writer)
    }

    pub fn write<K, A>(&mut self, column: usize, item: (&K, &A)) -> Result<(), StorageError>
    where
        K: DataTrait + ?Sized,
        A: DataTrait + ?Sized,
    {
        let row_group = if column + 1 < self.n_columns() {
            let row_group = self.cws[column + 1].take_rows();
            assert!(!row_group.is_empty());
            Some(row_group)
        } else {
            None
        };

        if column == 0 {
            // Add `key` to bloom filter.
            self.bloom_filter
                .insert(&item.0.default_hash().to_le_bytes());
        }

        // Add `value` to row group for column.
        self.cws[column].rows.end += 1;
        self.cws[column].add_item(&mut self.writer, item, &row_group)
    }

    pub fn finish_column<K, A>(&mut self, column: usize) -> Result<(), StorageError>
    where
        K: DataTrait + ?Sized,
        A: DataTrait + ?Sized,
    {
        debug_assert_eq!(column, self.finished_columns.len());
        for cw in self.cws.iter().skip(1) {
            assert!(cw.rows.is_empty());
        }

        self.finished_columns
            .push(self.cws[column].finish::<K, A>(&mut self.writer)?);
        Ok(())
    }

    pub fn close(
        mut self,
    ) -> Result<(Arc<dyn FileReader>, StoragePath, BloomFilter), StorageError> {
        debug_assert_eq!(self.cws.len(), self.finished_columns.len());

        // Write the Bloom filter.
        let (_block, filter_location) = self
            .writer
            .write_block(FilterBlockRef::from(&self.bloom_filter).into_block(), None)?;

        // Write the file trailer block.
        let file_trailer = FileTrailer {
            header: BlockHeader::new(&FILE_TRAILER_BLOCK_MAGIC),
            version: VERSION_NUMBER,
            columns: take(&mut self.finished_columns),
            compression: self.cws[0].parameters.compression,
            filter_offset: filter_location.offset,
            filter_size: filter_location.size.try_into().unwrap(),
        };
        let (_block, location) = self
            .writer
            .write_block(file_trailer.clone().into_block(), None)?;
        self.writer
            .insert_cache_entry(location, Arc::new(file_trailer));

        let (reader, path) = self.writer.complete()?;

        Ok((reader, path, self.bloom_filter))
    }

    pub fn n_columns(&self) -> usize {
        self.cws.len()
    }

    pub fn n_rows(&self) -> u64 {
        self.cws[0].rows.end
    }

    pub fn storage(&self) -> &Arc<BufferCache> {
        &self.writer.cache
    }
}

/// 1-column layer file writer.
///
/// `Writer1<K0, A0>` writes a new 1-column layer file in which column 0 has
/// key and auxiliary data types `(K0, A0)`.
///
/// # Example
///
/// The following code writes 1000 rows in column 0 with values `(0, ())`
/// through `(999, ())`.
///
/// ```
/// # use dbsp::dynamic::{DynData, Erase, DynUnit};
/// # use dbsp::storage::file::{writer::{Parameters, Writer1}};
/// use feldera_types::config::{StorageConfig, StorageOptions};
/// # use std::sync::Arc;
/// use dbsp::storage::{
///     backend::StorageBackend,
///     file::Factories,
///     buffer_cache::BufferCache,
/// };
/// let factories = Factories::<DynData, DynUnit>::new::<u32, ()>();
/// let tempdir = tempfile::tempdir().unwrap();
/// let storage_backend = <dyn StorageBackend>::new(&StorageConfig {
///     path: tempdir.path().to_string_lossy().to_string(),
///    cache: Default::default(),
/// }, &StorageOptions::default()).unwrap();
/// let cache = Arc::new(BufferCache::new(1024 * 1024));
/// let parameters = Parameters::default();
/// let mut file =
///     Writer1::new(&factories, cache, &*storage_backend, parameters, 1_000_000).unwrap();
/// for i in 0..1000_u32 {
///     file.write0((i.erase(), ().erase())).unwrap();
/// }
/// file.close().unwrap();
/// ```
pub struct Writer1<K0, A0>
where
    K0: DataTrait + ?Sized,
    A0: DataTrait + ?Sized,
{
    inner: Writer,
    pub(crate) factories: Factories<K0, A0>,
    _phantom: PhantomData<fn(&K0, &A0)>,
    #[cfg(debug_assertions)]
    prev0: Option<Box<K0>>,
}

impl<K0, A0> Writer1<K0, A0>
where
    K0: DataTrait + ?Sized,
    A0: DataTrait + ?Sized,
{
    /// Creates a new writer with the given parameters.
    pub fn new(
        factories: &Factories<K0, A0>,
        cache: fn() -> Arc<BufferCache>,
        storage_backend: &dyn StorageBackend,
        parameters: Parameters,
        estimated_keys: usize,
    ) -> Result<Self, StorageError> {
        Ok(Self {
            factories: factories.clone(),
            inner: Writer::new(
                &[&factories.any_factories()],
                cache,
                storage_backend,
                parameters,
                1,
                estimated_keys,
            )?,
            _phantom: PhantomData,
            #[cfg(debug_assertions)]
            prev0: None,
        })
    }
    /// Writes `item` to column 0.  `item.0` must be greater than passed in the
    /// previous call to this function (if any).
    pub fn write0(&mut self, item: (&K0, &A0)) -> Result<(), StorageError> {
        #[cfg(debug_assertions)]
        {
            let key0 = item.0;
            if let Some(prev0) = &self.prev0 {
                debug_assert!(
                    &**prev0 < key0,
                    "can't write {prev0:?} then {key0:?} to column 0",
                );
            }
            self.prev0 = Some(clone_box(key0));
        }
        self.inner.write(0, item)
    }

    /// Returns the number of calls to [`write0`](Self::write0) so far.
    pub fn n_rows(&self) -> u64 {
        self.inner.n_rows()
    }

    /// Finishes writing the layer file and returns the writer passed to
    /// [`new`](Self::new).
    pub fn close(
        mut self,
    ) -> Result<(Arc<dyn FileReader>, StoragePath, BloomFilter), StorageError> {
        self.inner.finish_column::<K0, A0>(0)?;
        self.inner.close()
    }

    /// Returns the storage used for this writer.
    pub fn storage(&self) -> &Arc<BufferCache> {
        self.inner.storage()
    }

    /// Finishes writing the layer file and returns a reader for it.
    pub fn into_reader(
        self,
    ) -> Result<Reader<(&'static K0, &'static A0, ())>, super::reader::Error> {
        let any_factories = self.factories.any_factories();

        let cache = self.inner.cache;
        let (file_handle, path, bloom_filter) = self.close()?;

        Reader::new(
            &[&any_factories],
            path,
            cache,
            file_handle,
            Some(bloom_filter),
        )
    }
}

/// 2-column layer file writer.
///
/// `Writer2<K0, A0, K1, A1>` writes a new 2-column layer file in which
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
/// # use dbsp::dynamic::{DynData, DynUnit};
/// # use dbsp::storage::file::{writer::{Parameters, Writer2}};
/// # use std::sync::Arc;
/// use feldera_types::config::{StorageConfig, StorageOptions};
/// use dbsp::storage::{
///     backend::StorageBackend,
///     file::Factories,
///     buffer_cache::BufferCache,
/// };
/// let factories = Factories::<DynData, DynUnit>::new::<u32, ()>();
/// let tempdir = tempfile::tempdir().unwrap();
/// let storage_backend = <dyn StorageBackend>::new(&StorageConfig {
///     path: tempdir.path().to_string_lossy().to_string(),
///    cache: Default::default(),
/// }, &StorageOptions::default()).unwrap();
/// let cache = Arc::new(BufferCache::new(1024 * 1024));
/// let parameters = Parameters::default();
/// let mut file =
///     Writer2::new(&factories, &factories, cache, &*storage_backend, parameters, 1_000_000).unwrap();
/// for i in 0..1000_u32 {
///     for j in 0..10_u32 {
///         file.write1((&j, &())).unwrap();
///     }
///     file.write0((&i, &())).unwrap();
/// }
/// file.close().unwrap();
/// ```
pub struct Writer2<K0, A0, K1, A1>
where
    K0: DataTrait + ?Sized,
    A0: DataTrait + ?Sized,
    K1: DataTrait + ?Sized,
    A1: DataTrait + ?Sized,
{
    inner: Writer,
    pub(crate) factories0: Factories<K0, A0>,
    pub(crate) factories1: Factories<K1, A1>,
    #[cfg(debug_assertions)]
    prev0: Option<Box<K0>>,
    #[cfg(debug_assertions)]
    prev1: Option<Box<K1>>,
    _phantom: PhantomData<fn(&K0, &A0, &K1, &A1)>,
}

impl<K0, A0, K1, A1> Writer2<K0, A0, K1, A1>
where
    K0: DataTrait + ?Sized,
    A0: DataTrait + ?Sized,
    K1: DataTrait + ?Sized,
    A1: DataTrait + ?Sized,
{
    /// Creates a new writer with the given parameters.
    pub fn new(
        factories0: &Factories<K0, A0>,
        factories1: &Factories<K1, A1>,
        cache: fn() -> Arc<BufferCache>,
        storage_backend: &dyn StorageBackend,
        parameters: Parameters,
        estimated_keys: usize,
    ) -> Result<Self, StorageError> {
        Ok(Self {
            factories0: factories0.clone(),
            factories1: factories1.clone(),
            inner: Writer::new(
                &[&factories0.any_factories(), &factories1.any_factories()],
                cache,
                storage_backend,
                parameters,
                2,
                estimated_keys,
            )?,
            #[cfg(debug_assertions)]
            prev0: None,
            #[cfg(debug_assertions)]
            prev1: None,
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
        #[cfg(debug_assertions)]
        {
            let key0 = item.0;
            if let Some(prev0) = &self.prev0 {
                debug_assert!(
                    &**prev0 < key0,
                    "can't write {prev0:?} then {key0:?} to column 0",
                );
            }
            self.prev0 = Some(clone_box(key0));
            self.prev1 = None;
        }

        self.inner.write(0, item)
    }

    /// Writes `item` to column 1.  `item.0` must be greater than passed in the
    /// previous call to this function (if any) since the last call to
    /// [`write0`](Self::write0) (if any).
    pub fn write1(&mut self, item: (&K1, &A1)) -> Result<(), StorageError> {
        #[cfg(debug_assertions)]
        {
            let key1 = item.0;
            if let Some(prev1) = &self.prev1 {
                debug_assert!(
                    &**prev1 < key1,
                    "can't write {prev1:?} then {key1:?} to column 1",
                );
            }
            self.prev1 = Some(clone_box(key1));
        }

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
    pub fn close(
        mut self,
    ) -> Result<(Arc<dyn FileReader>, StoragePath, BloomFilter), StorageError> {
        self.inner.finish_column::<K0, A0>(0)?;
        self.inner.finish_column::<K1, A1>(1)?;
        self.inner.close()
    }

    /// Returns the storage used for this writer.
    pub fn storage(&self) -> &Arc<BufferCache> {
        self.inner.storage()
    }

    /// Finishes writing the layer file and returns a reader for it.
    #[allow(clippy::type_complexity)]
    pub fn into_reader(
        self,
    ) -> Result<
        Reader<(&'static K0, &'static A0, (&'static K1, &'static A1, ()))>,
        super::reader::Error,
    > {
        let any_factories0 = self.factories0.any_factories();
        let any_factories1 = self.factories1.any_factories();
        let cache = self.inner.cache;
        let (file_handle, path, bloom_filter) = self.close()?;
        Reader::new(
            &[&any_factories0, &any_factories1],
            path,
            cache,
            file_handle,
            Some(bloom_filter),
        )
    }
}
