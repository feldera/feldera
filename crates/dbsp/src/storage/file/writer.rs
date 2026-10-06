//! Layer file writer.
//!
//! Use [`Writer1`] to write a 1-column layer file and [`Writer2`] to write a
//! 2-column layer file.  To write more columns, either add another `Writer<N>`
//! struct, which is easily done, or mark the currently private `Writer` as
//! `pub`.
use super::format::Compression;
use super::{
    AnyFactories, BatchKeyFilter, Factories,
    reader::{RawItem, RawItems, Reader},
};
use crate::storage::{
    backend::{BlockLocation, FileReader, FileWriter, StorageBackend, StorageError},
    buffer_cache::{BufferCache, FBuf, FBufSerializer, LimitExceeded},
    file::{
        SerializerInner,
        format::{
            BatchMetadata, BlockHeader, BloomFilterBlockRef, COMPATIBLE_FEATURE_FILTER64,
            COMPATIBLE_FEATURE_NEGATIVE_WEIGHT_COUNT, DATA_BLOCK_MAGIC, DataBlockHeader,
            FILE_TRAILER_BLOCK_MAGIC, FileTrailer, FileTrailerColumn, FixedLen,
            INCOMPATIBLE_FEATURE_MODULAR_FILTERS, INCOMPATIBLE_FEATURE_ROARING_FILTERS,
            INDEX_BLOCK_MAGIC, IndexBlockHeader, MODULAR_BLOOM_FILTER_BLOCK_MAGIC,
            ModularBloomFilterBlockRef, NodeType, ROARING_BITMAP_FILTER_BLOCK_MAGIC,
            RoaringBitmapFilterBlockRef, VERSION_NUMBER, Varint,
        },
        reader::TreeNode,
    },
};
use crate::{
    Runtime,
    dynamic::{DataTrait, DeserializeDyn, SerializeDyn},
    storage::file::ItemFactory,
    trace::filter::{BatchFilters, key_range::KeyRange},
};
use binrw::{
    BinWrite,
    io::{Cursor, NoSeek},
};
use crc32c::crc32c;
#[cfg(debug_assertions)]
use dyn_clone::clone_box;
use feldera_buffer_cache::CacheEntry;
use feldera_storage::StoragePath;
use lz4_flex::block::{compress_into, get_maximum_output_size};
use snap::raw::{Encoder, max_compress_len};
use std::{cell::RefCell, sync::Arc};
use std::{
    marker::PhantomData,
    mem::{replace, take},
    ops::Range,
};
use zstd::bulk::Compressor as ZstdCompressor;

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

    /// Compression level, for codecs that have one.
    pub compression_level: Option<i32>,
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

    /// Returns these parameters with `compression_level` updated.
    pub fn with_compression_level(self, compression_level: Option<i32>) -> Self {
        Self {
            compression_level,
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
            compression: Some(Compression::Lz4),
            compression_level: None,
        }
    }
}

trait IntoBlock {
    fn into_block(self, expected_capacity: usize) -> FBuf;
    fn overwrite_head(&self, dst: &mut FBuf)
    where
        Self: FixedLen;
}

impl<B> IntoBlock for B
where
    B: for<'a> BinWrite<Args<'a> = ()>,
{
    fn into_block(self, expected_capacity: usize) -> FBuf {
        let mut block = NoSeek::new(FBuf::with_capacity(expected_capacity));
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
        serializer: &mut SerializerInner,
    ) -> Result<(FileTrailerColumn, Option<(Box<K>, Box<K>)>), StorageError>
    where
        K: DataTrait + ?Sized,
        A: DataTrait + ?Sized,
    {
        // Flush data.
        if !self.data_block.is_empty() {
            let data_block = self.data_block.build::<K, A>();
            self.write_data_block::<K, A>(block_writer, data_block, serializer)?;
        }

        // Flush index.
        let mut level = 0;
        while level < self.index_blocks.len() {
            if level == self.index_blocks.len() - 1 && self.index_blocks[level].entries.len() == 1 {
                let builder = &self.index_blocks[level];
                let entry = &builder.entries[0];
                return Ok((
                    FileTrailerColumn {
                        node_type: builder.child_type,
                        node_offset: entry.child.offset,
                        node_size: entry.child.size.try_into().unwrap_or_else(|_| {
                            unreachable!(
                                "Individual blocks should be much less than 4 GiB, tried to write {:?}",
                                &entry.child
                            )
                        }),
                        n_rows: entry.row_total,
                    },
                    Some(self.key_bounds::<K>(&builder.raw, entry)),
                ));
            } else if !self.index_blocks[level].is_empty() {
                let index_block = self.index_blocks[level].build();
                self.write_index_block::<K>(block_writer, index_block, level, serializer)?;
            }
            level += 1;
        }
        Ok((
            FileTrailerColumn {
                node_type: NodeType::Data,
                node_offset: 0,
                node_size: 0,
                n_rows: 0,
            },
            None,
        ))
    }

    fn key_bounds<K>(&self, raw: &FBuf, entry: &IndexEntry) -> (Box<K>, Box<K>)
    where
        K: DataTrait + ?Sized,
    {
        let key_factory = self.factories.key_factory::<K>();

        let mut min = key_factory.default_box();
        rkyv_deserialize(raw, entry.min_offset, min.as_mut());

        let mut max = key_factory.default_box();
        rkyv_deserialize(raw, entry.max_offset, max.as_mut());

        (min, max)
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
        serializer: &mut SerializerInner,
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
            block_writer.file_handle.file_id(),
            VERSION_NUMBER,
        )
        .unwrap();

        if let Some(index_block) = self.get_index_block(0).add_entry(
            location,
            &data_block.min_max,
            data_block.n_rows as u64,
            serializer,
        ) {
            self.write_index_block::<K>(block_writer, index_block, 0, serializer)?;
        }
        Ok(())
    }

    fn write_index_block<K>(
        &mut self,
        block_writer: &mut BlockWriter,
        mut index_block: IndexBlock<K>,
        mut level: usize,
        serializer: &mut SerializerInner,
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
                block_writer.file_handle.file_id(),
                VERSION_NUMBER,
            )
            .unwrap();

            level += 1;
            let opt_index_block = self.get_index_block(level).add_entry(
                location,
                &index_block.min_max,
                n_rows,
                serializer,
            );
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
        serializer: &mut SerializerInner,
    ) -> Result<(), StorageError>
    where
        K: DataTrait + ?Sized,
        A: DataTrait + ?Sized,
    {
        if let Some(data_block) = self.data_block.add_item(item, row_group, serializer) {
            self.write_data_block::<K, A>(block_writer, data_block, serializer)?;
        }
        Ok(())
    }

    /// Appends one encoded item to this column, writing the open data block
    /// out first if the item does not fit in it.
    ///
    /// # Arguments
    ///
    /// * `block_writer` - where a full data block goes.
    /// * `item` - the item, encoded as the source file stored it.
    /// * `row_group` - for every column but the last, the rows of the next
    ///   column that belong to the item; `None` for the last column.
    /// * `serializer` - for the index entry of a data block written out.
    fn add_raw_item<K, A>(
        &mut self,
        block_writer: &mut BlockWriter,
        item: &RawItem<'_>,
        row_group: Option<Range<u64>>,
        serializer: &mut SerializerInner,
    ) -> Result<(), StorageError>
    where
        K: DataTrait + ?Sized,
        A: DataTrait + ?Sized,
    {
        if !self.data_block.try_add_raw_item(item, row_group.clone()) {
            let data_block = self.data_block.build::<K, A>();
            self.write_data_block::<K, A>(block_writer, data_block, serializer)?;
            let taken = self.data_block.try_add_raw_item(item, row_group);
            assert!(taken, "an empty data block refused an item");
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
        serializer: &mut SerializerInner,
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
                result = rkyv_serialize(
                    serializer,
                    &mut self.raw,
                    item,
                    self.size_target.unwrap_or(usize::MAX),
                );
            });
        let offset = result.inspect_err(|_| self.raw.resize(old_len, 0))?;
        // A merge may copy this item's bytes later, keeping its position only
        // modulo the alignment its types declare; anything in it aligned more
        // strictly than that could land misaligned.
        debug_assert!(
            serializer.max_align() <= self.factories.item_factory::<K, A>().max_align(),
            "archiving an item asked for alignment {}, more than the {} its key and \
             auxiliary types declare through `ArchivedRepr::MAX_ALIGN`",
            serializer.max_align(),
            self.factories.item_factory::<K, A>().max_align(),
        );

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
    /// Copies one encoded item into this block, without decoding it, if the
    /// block has room for it, and returns whether it did.
    ///
    /// The bytes are moved verbatim, so every relative pointer inside them
    /// still points where it did; what has to be re-established is alignment,
    /// which the leading pad does by putting the item back in the phase it was
    /// written in.  An item from the middle of a run starts where the previous
    /// one ended, so a run appended item by item lands exactly as it lay in the
    /// source, with no pad between its items.
    ///
    /// The block takes the item only if it would still build within its size
    /// target with it: the test [`try_add_item`](Self::try_add_item) makes after
    /// encoding an item, made here before anything is written, so nothing ever
    /// has to be given back.  A block without a size target yet, which is one
    /// short of `min_branch` items, takes every item.
    ///
    /// # Arguments
    ///
    /// * `item` - the item, encoded as the source block stored it.
    /// * `row_group` - for every column but the last, the rows of this file's
    ///   next column that belong to the item; `None` for the last column.
    ///
    /// # Returns
    ///
    /// Whether the block took the item.
    fn try_add_raw_item(&mut self, item: &RawItem<'_>, row_group: Option<Range<u64>>) -> bool {
        debug_assert!(item.align.is_power_of_two());
        debug_assert!(
            row_group.is_some() || self.row_groups.is_empty(),
            "a block that holds row groups needs one for every item"
        );
        if self.value_offsets.len() >= self.parameters.max_branch() {
            return false;
        }

        // Zero byte padding, so the root lands as aligned as it was in the
        // source block.
        let pad = item.phase.wrapping_sub(self.raw.len()) & (item.align - 1);
        // Where the item's root lands.
        let root = self.raw.len() + pad + item.root;

        if let Some(size_target) = self.size_target {
            // The size the block would build to with the item in it.  A
            // block's first row group brings both of its boundaries.
            let mut stride = self.value_offset_stride;
            stride.push(root);
            let row_groups = row_group
                .as_ref()
                .map(|rows| (rows.end, self.row_groups.0.len().max(1) + 1));
            let len = Self::specs_for(
                root + (item.bytes.len() - item.root),
                self.value_offsets.len() + 1,
                &stride,
                row_groups,
            )
            .len;
            if len > size_target {
                return false;
            }
        }

        self.raw.resize(self.raw.len() + pad, 0);
        self.raw.extend_from_slice(item.bytes);
        self.value_offsets.push(root);
        self.value_offset_stride.push(root);
        if let Some(rows) = row_group {
            self.row_groups.push(&rows);
        }

        if self.size_target.is_none() && self.value_offsets.len() >= self.parameters.min_branch {
            // The block now holds its first `min_branch` items, so it sets its
            // size target the way `try_add_item` does.
            self.size_target = Some(
                self.specs()
                    .len
                    .next_multiple_of(512)
                    .max(self.parameters.min_data_block),
            );
        }
        true
    }

    fn add_item<K, A>(
        &mut self,
        item: (&K, &A),
        row_group: &Option<Range<u64>>,
        serializer: &mut SerializerInner,
    ) -> Option<DataBlock<K>>
    where
        K: DataTrait + ?Sized,
        A: DataTrait + ?Sized,
    {
        if self.try_add_item(item, row_group, serializer).is_ok() {
            None
        } else {
            let retval = self.build::<K, A>();
            assert!(self.try_add_item(item, row_group, serializer).is_ok());
            Some(retval)
        }
    }

    fn specs(&self) -> DataBuildSpecs {
        debug_assert!(!self.is_empty());
        debug_assert!(
            self.row_groups.is_empty() || self.row_groups.0.len() == self.value_offsets.len() + 1
        );
        Self::specs_for(
            self.raw.len(),
            self.value_offsets.len(),
            &self.value_offset_stride,
            self.row_groups
                .max()
                .map(|max| (max, self.row_groups.0.len())),
        )
    }

    /// Lays out a block's value map and row groups after `raw_len` bytes of
    /// items.
    ///
    /// The layout depends on nothing else, so a caller can ask how big a block
    /// would build to with an item it has not added yet.
    ///
    /// # Arguments
    ///
    /// * `raw_len` - the bytes the header and the items take.
    /// * `n_values` - how many items the block holds.
    /// * `stride` - the stride builder that has seen every item's offset.
    /// * `row_groups` - for a column with row groups, the largest boundary and
    ///   how many boundaries there are; `None` otherwise.
    ///
    /// # Returns
    ///
    /// Where the value map and the row groups go, and the block's length.
    fn specs_for(
        raw_len: usize,
        n_values: usize,
        stride: &StrideBuilder,
        row_groups: Option<(u64, usize)>,
    ) -> DataBuildSpecs {
        let value_map = match stride.get_stride() {
            // General case.
            None => VarintWriter::new(Varint::from_len(raw_len), raw_len, n_values),

            // Optimization for constant stride.  We need a starting offset and
            // a stride, both 32 bits.
            Some(_) => VarintWriter::new(Varint::B32, raw_len, 2),
        };
        let len = value_map.offset_after();
        let row_groups = row_groups
            .map(|(max, count)| VarintWriter::new(Varint::from_max_value(max), len, count));
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
    fn is_empty(&self) -> bool {
        self.0.is_empty()
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

/// Decodes the key of an encoded item, for the ordering checks that a debug
/// build makes on items that are copied rather than written decoded.
///
/// # Arguments
///
/// * `factories` - the factories of the column the item came from.
/// * `bytes` - the bytes that hold the item, as a reader handed them out.
/// * `root` - where the item's root starts in `bytes`.
///
/// # Returns
///
/// The item's key.
#[cfg(debug_assertions)]
fn decode_raw_key<K, A>(factories: &Factories<K, A>, bytes: &[u8], root: usize) -> Box<K>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    let mut key = factories.key_factory.default_box();
    // SAFETY: a reader hands out an item only from a block of the column it
    // reads, with the root of the archived item at `root` in `bytes`, which
    // the write that follows relies on too.
    let archived = unsafe { factories.item_factory.archived_value(bytes, root) }.fst();
    DeserializeDyn::deserialize(archived, &mut *key);
    key
}

fn rkyv_serialize<T>(
    serializer: &mut SerializerInner,
    dst: &mut FBuf,
    value: &T,
    limit: usize,
) -> Result<usize, LimitExceeded>
where
    T: SerializeDyn + ?Sized,
{
    let old_len = dst.len();

    let offset = serializer
        .with(
            FBufSerializer::new(&mut *dst).with_limit(limit),
            |serializer| value.serialize(serializer),
        )
        .map_err(|_| LimitExceeded)?;

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
        serializer: &mut SerializerInner,
    ) -> Result<(), LimitExceeded>
    where
        K: DataTrait + ?Sized,
    {
        if self.entries.len() >= self.parameters.max_branch() {
            return Err(LimitExceeded);
        }
        self.max_child_size = self.max_child_size.max(child.size);
        let limit = self.size_target.unwrap_or(usize::MAX);
        let min_offset = rkyv_serialize(serializer, &mut self.raw, min_max.0.as_ref(), limit)?;
        let max_offset = rkyv_serialize(serializer, &mut self.raw, min_max.1.as_ref(), limit)?;
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
        serializer: &mut SerializerInner,
    ) -> Result<(), LimitExceeded>
    where
        K: DataTrait + ?Sized,
    {
        let saved_len = self.raw.len();
        let saved_max_child_size = self.max_child_size;
        let n_entries = self.entries.len();
        self.inner_try_add_entry(child, min_max, n_rows, serializer)
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
        serializer: &mut SerializerInner,
    ) -> Option<IndexBlock<K>>
    where
        K: DataTrait + ?Sized,
    {
        let mut f = |t: &mut Self| t.try_add_entry(child, min_max, n_rows, serializer);
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
    file_handle: Box<dyn FileWriter>,
    encoder: Encoder,
    /// Created on the first Zstd block, so that files using another codec
    /// never allocate a zstd context. Reused afterwards, which is the whole
    /// reason to hold it rather than call the free function per block.
    zstd: Option<ZstdCompressor<'static>>,
    zstd_level: i32,
    offset: u64,
}

impl BlockWriter {
    fn new(
        cache: Arc<BufferCache>,
        file_handle: Box<dyn FileWriter>,
        compression_level: Option<i32>,
    ) -> Self {
        Self {
            cache,
            file_handle,
            encoder: Encoder::new(),
            zstd: None,
            // zstd treats 0 as "use the default level". A level outside what
            // this zstd build accepts would make `Compressor::new` fail, and
            // this one comes from user configuration, so clamp rather than
            // panic on it later.
            zstd_level: compression_level.map_or(0, |level| {
                let range = zstd::compression_level_range();
                level.clamp(*range.start(), *range.end())
            }),
            offset: 0,
        }
    }

    fn complete(self) -> Result<Arc<dyn FileReader>, StorageError> {
        // Not committed here. A layer file only has to be durable once a
        // checkpoint references it, and the checkpoint commit phase syncs every
        // batch it captures. Syncing on the writing thread instead would stall
        // that thread on one fsync per file, including throughout a merge.
        self.file_handle.complete()
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
                    Compression::Lz4 => {
                        let max_len = 4 + get_maximum_output_size(block.len());
                        if max_len > bounce.len() {
                            bounce.resize(max_len, 0);
                        }
                        bounce[..4].copy_from_slice(&(block.len() as u32).to_le_bytes());
                        4 + compress_into(block.as_slice(), &mut bounce[4..]).unwrap()
                    }
                    Compression::Zstd => {
                        let max_len = 4 + zstd::zstd_safe::compress_bound(block.len());
                        if max_len > bounce.len() {
                            bounce.resize(max_len, 0);
                        }
                        bounce[..4].copy_from_slice(&(block.len() as u32).to_le_bytes());
                        let level = self.zstd_level;
                        let zstd = self.zstd.get_or_insert_with(|| {
                            ZstdCompressor::new(level).expect("failed to create zstd compressor")
                        });
                        4 + zstd
                            .compress_to_buffer(block.as_slice(), &mut bounce[4..])
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
            self.file_handle.write_block(compressed)?;

            (Arc::new(block), location)
        } else {
            // Pad and checksum the block.
            block.resize(block.len().next_multiple_of(512), 0);
            let checksum = crc32c(&block[4..]).to_le_bytes();
            block[..4].copy_from_slice(checksum.as_slice());

            // Write the block.
            let location = BlockLocation::new(self.offset, block.len()).unwrap();
            let block = self.file_handle.write_block(block)?;
            (block, location)
        };

        // Construct a cache entry from the uncompressed data.
        self.offset = location.after();
        Ok((uncompressed, location))
    }

    fn insert_cache_entry(&self, location: BlockLocation, entry: Arc<dyn CacheEntry>) {
        self.cache
            .insert(self.file_handle.file_id(), location.offset, entry);
    }
}

/// General-purpose layer file writer.
///
/// A `Writer` can write a layer file with any number of columns.  It lacks type
/// safety to ensure that the data and auxiliary values written to columns are
/// all the same type.  Thus, [`Writer1`] and [`Writer2`] exist for writing
/// 1-column and 2-column layer files, respectively, with added type safety.
struct Writer {
    cache: fn() -> Option<Arc<BufferCache>>,
    writer: BlockWriter,
    key_filter: Option<BatchKeyFilter>,
    /// Whether keys can go in without being decoded, which is fixed for the
    /// file; see [`can_hash_raw_keys`](Self::can_hash_raw_keys).
    raw_keys_hashable: Option<bool>,

    cws: Vec<ColumnWriter>,
    finished_columns: Vec<FileTrailerColumn>,
    serializer: SerializerInner,
}

impl Writer {
    pub fn new(
        factories: &[&AnyFactories],
        cache: fn() -> Option<Arc<BufferCache>>,
        storage_backend: &dyn StorageBackend,
        parameters: Parameters,
        n_columns: usize,
        key_filter: Option<BatchKeyFilter>,
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
            writer: BlockWriter::new(
                cache().expect("Should have a buffer cache"),
                storage_backend.create_with_prefix(&worker.into())?,
                parameters.compression_level,
            ),
            key_filter,
            raw_keys_hashable: None,
            cws,
            finished_columns,
            serializer: SerializerInner::new(),
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

        if column == 0
            && let Some(key_filter) = &mut self.key_filter
        {
            key_filter.push_key(item.0);
        }

        // Add `value` to row group for column.
        self.cws[column].rows.end += 1;
        self.cws[column].add_item(&mut self.writer, item, &row_group, &mut self.serializer)
    }

    /// Whether keys written to `column` can be recorded in this file's
    /// membership filter without decoding them.
    ///
    /// Both halves of the answer are properties of the file and the key type,
    /// not of any key: a filter that wants the key itself rather than a hash
    /// of it can never be fed this way, and whether an archived key
    /// reproduces the decoded key's hash is fixed for the type.  So the
    /// question is answered once and the answer kept.
    fn can_hash_raw_keys<K>(&mut self, column: usize) -> bool
    where
        K: DataTrait + ?Sized,
    {
        if let Some(answer) = self.raw_keys_hashable {
            return answer;
        }
        let answer = match &self.key_filter {
            None => true,
            Some(filter) => {
                filter.takes_hashes()
                    && self.cws[column]
                        .factories
                        .key_factory::<K>()
                        .supports_archived_hash()
            }
        };
        self.raw_keys_hashable = Some(answer);
        answer
    }

    /// Appends already-encoded items to the last column, `column`, one after
    /// another.
    ///
    /// The last column has no row groups, so the items go in exactly as they
    /// lay in the source; a data block that fills up is written out and the
    /// rest go into the next one.
    ///
    /// # Arguments
    ///
    /// * `column` - the last column.
    /// * `items` - the run, encoded as the source file stored it.
    pub fn write_raw_values<K, A>(
        &mut self,
        column: usize,
        items: &RawItems<'_>,
    ) -> Result<(), StorageError>
    where
        K: DataTrait + ?Sized,
        A: DataTrait + ?Sized,
    {
        debug_assert_eq!(
            column + 1,
            self.n_columns(),
            "only the last column has no row groups"
        );
        for index in 0..items.len() {
            self.cws[column].add_raw_item::<K, A>(
                &mut self.writer,
                &items.item(index),
                None,
                &mut self.serializer,
            )?;
        }
        self.cws[column].rows.end += items.len() as u64;
        Ok(())
    }

    /// Appends one already-encoded item to `column`, a column with row
    /// groups, as the owner of the rows of the next column written since the
    /// previous item went in.
    ///
    /// That is how a merge copies a key: it writes the key's values first, so
    /// the rows they took are the key's row group in this file, however the
    /// source numbered them and however the values got here.  A key this
    /// file's membership filter could not record without decoding it is
    /// refused, and nothing is written.
    ///
    /// # Arguments
    ///
    /// * `column` - a column other than the last.
    /// * `item` - the item, encoded as the source file stored it.
    ///
    /// # Returns
    ///
    /// Whether the item went in.
    pub fn write_raw_key<K, A>(
        &mut self,
        column: usize,
        item: &RawItem<'_>,
    ) -> Result<bool, StorageError>
    where
        K: DataTrait + ?Sized,
        A: DataTrait + ?Sized,
    {
        debug_assert!(
            column + 1 < self.n_columns(),
            "the last column has no row groups"
        );
        // The filter must record every key, so refuse one that cannot be
        // hashed without decoding.
        let feeds_filter = column == 0 && self.key_filter.is_some();
        if feeds_filter && !self.can_hash_raw_keys::<K>(column) {
            return Ok(false);
        }

        let rows = self.cws[column + 1].take_rows();
        assert!(
            !rows.is_empty(),
            "a key needs at least one row of the next column"
        );
        self.cws[column].rows.end += 1;
        self.cws[column].add_raw_item::<K, A>(
            &mut self.writer,
            item,
            Some(rows),
            &mut self.serializer,
        )?;

        if feeds_filter {
            let item_factory = self.cws[column].factories.item_factory::<K, A>();
            let filter = self.key_filter.as_mut().expect("the file has a filter");
            // SAFETY: `item.root` is where the root of the archived item, the
            // key together with its auxiliary data, sits in `item.bytes`, and
            // the source block's reader produced the two together.  The key
            // is the item's first half, which is not at the root's start
            // unless the auxiliary data takes no space.
            let archived = unsafe { item_factory.archived_value(item.bytes, item.root) }.fst();
            let hash = archived
                .archived_hash()
                .expect("the key type answered before the key was written");
            let recorded = filter.push_hash(hash);
            debug_assert!(
                recorded,
                "the filter took hashes before the key was written"
            );
        }
        Ok(true)
    }

    pub fn finish_column<K, A>(
        &mut self,
        column: usize,
    ) -> Result<Option<(Box<K>, Box<K>)>, StorageError>
    where
        K: DataTrait + ?Sized,
        A: DataTrait + ?Sized,
    {
        debug_assert_eq!(column, self.finished_columns.len());
        for cw in self.cws.iter().skip(1) {
            assert!(cw.rows.is_empty());
        }

        let (trailer, key_bounds) =
            self.cws[column].finish::<K, A>(&mut self.writer, &mut self.serializer)?;
        self.finished_columns.push(trailer);
        Ok(key_bounds)
    }

    pub fn close(
        mut self,
        metadata: BatchMetadata,
    ) -> Result<(Arc<dyn FileReader>, Option<BatchKeyFilter>), StorageError> {
        debug_assert_eq!(self.cws.len(), self.finished_columns.len());

        if let Some(key_filter) = &mut self.key_filter {
            key_filter.finalize();
        }

        // Write the batch key filter.
        let mut incompatible_features = 0;
        let filter_location = if let Some(key_filter) = &self.key_filter {
            match key_filter {
                BatchKeyFilter::Bloom(filter) => {
                    let layout = *filter.layout();
                    let data: Vec<u64> = filter.module_words().concat();
                    if layout.total_modules() == 1 {
                        // A one-module filter is a plain fastbloom filter, so
                        // writing it in the original encoding keeps the file
                        // readable by binaries that predate modular filters.
                        let filter_block = BloomFilterBlockRef {
                            header: BlockHeader::new(
                                &crate::storage::file::format::BLOOM_FILTER_BLOCK_MAGIC,
                            ),
                            num_hashes: layout.hashes_per_module(),
                            data: &data,
                        };
                        let estimated_block_size = (std::mem::size_of::<BloomFilterBlockRef>()
                            + std::mem::size_of_val(filter_block.data))
                        .next_multiple_of(512);
                        self.writer
                            .write_block(filter_block.into_block(estimated_block_size), None)?
                            .1
                    } else {
                        incompatible_features |= INCOMPATIBLE_FEATURE_MODULAR_FILTERS;
                        let set_bits = filter.density().set_bits().to_vec();
                        let filter_block = ModularBloomFilterBlockRef {
                            header: BlockHeader::new(&MODULAR_BLOOM_FILTER_BLOCK_MAGIC),
                            total_modules: layout.total_modules(),
                            hashes_per_module: layout.hashes_per_module(),
                            words_per_module: u64::from(layout.words_per_module()),
                            set_bits: &set_bits,
                            data: &data,
                        };
                        let estimated_block_size =
                            (std::mem::size_of::<ModularBloomFilterBlockRef>()
                                + std::mem::size_of_val(filter_block.data))
                            .next_multiple_of(512);
                        self.writer
                            .write_block(filter_block.into_block(estimated_block_size), None)?
                            .1
                    }
                }
                BatchKeyFilter::RoaringU32(filter) => {
                    incompatible_features |= INCOMPATIBLE_FEATURE_ROARING_FILTERS;
                    let mut data = Vec::with_capacity(filter.serialized_size());
                    filter
                        .serialize_into(&mut data)
                        .map_err(|_| StorageError::RoaringBitmapFilter)?;
                    let filter_block = RoaringBitmapFilterBlockRef {
                        header: BlockHeader::new(&ROARING_BITMAP_FILTER_BLOCK_MAGIC),
                        data: &data,
                    };
                    let estimated_block_size = (std::mem::size_of::<RoaringBitmapFilterBlockRef>()
                        + data.len())
                    .next_multiple_of(512);
                    self.writer
                        .write_block(filter_block.into_block(estimated_block_size), None)?
                        .1
                }
            }
        } else {
            BlockLocation { offset: 0, size: 0 }
        };

        // Write the file trailer block.

        let mut file_trailer = FileTrailer {
            header: BlockHeader::new(&FILE_TRAILER_BLOCK_MAGIC),
            version: VERSION_NUMBER,
            columns: take(&mut self.finished_columns),
            compression: self.cws[0].parameters.compression,
            filter_offset: 0,
            filter_size: 0,
            compatible_features: COMPATIBLE_FEATURE_NEGATIVE_WEIGHT_COUNT,
            incompatible_features,
            filter_offset64: 0,
            filter_size64: 0,
            metadata,
        };
        if filter_location.size > 0 {
            if let Ok(size) = u32::try_from(filter_location.size)
                && size < i32::MAX as u32
            {
                file_trailer.filter_offset = filter_location.offset;
                file_trailer.filter_size = size;
            } else {
                file_trailer.compatible_features |= COMPATIBLE_FEATURE_FILTER64;
                file_trailer.filter_offset64 = filter_location.offset;
                file_trailer.filter_size64 = filter_location.size as u64;
            }
        }
        let (_block, location) = self
            .writer
            .write_block(file_trailer.clone().into_block(4096), None)?;
        self.writer
            .insert_cache_entry(location, Arc::new(file_trailer));

        Ok((self.writer.complete()?, self.key_filter))
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

    /// Returns the path for the file being written.
    pub fn path(&self) -> &StoragePath {
        self.writer.file_handle.path()
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
///     file::{Factories, format::BatchMetadata},
///     buffer_cache::BufferCache,
/// };
/// let factories = Factories::<DynData, DynUnit>::new::<u32, ()>();
/// let tempdir = tempfile::tempdir().unwrap();
/// let storage_backend = <dyn StorageBackend>::new(&StorageConfig {
///     path: tempdir.path().to_string_lossy().to_string(),
///    cache: Default::default(),
/// }, &StorageOptions::default()).unwrap();
/// let parameters = Parameters::default();
/// let mut file =
///     Writer1::new(&factories, || Some(Arc::new(BufferCache::new(1024 * 1024))), &*storage_backend, parameters, None).unwrap();
/// for i in 0..1000_u32 {
///     file.write0((i.erase(), ().erase())).unwrap();
/// }
/// file.close(BatchMetadata::default()).unwrap();
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
        cache: fn() -> Option<Arc<BufferCache>>,
        storage_backend: &dyn StorageBackend,
        parameters: Parameters,
        key_filter: Option<BatchKeyFilter>,
    ) -> Result<Self, StorageError> {
        Ok(Self {
            factories: factories.clone(),
            inner: Writer::new(
                &[&factories.any_factories()],
                cache,
                storage_backend,
                parameters,
                1,
                key_filter,
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
                    "can't write {prev0:?} >= {key0:?} to column 0",
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

    /// Finishes writing the layer file and returns the file handle, optional
    /// bloom filter, and column-0 key bounds.
    ///
    /// # Arguments
    ///
    /// * `metadata` - Batch metadata to include in the trailer.
    pub fn close(
        mut self,
        metadata: BatchMetadata,
    ) -> Result<
        (
            Arc<dyn FileReader>,
            Option<BatchKeyFilter>,
            Option<(Box<K0>, Box<K0>)>,
        ),
        StorageError,
    > {
        let key_bounds = self.inner.finish_column::<K0, A0>(0)?;
        let (file_handle, bloom_filter) = self.inner.close(metadata)?;
        Ok((file_handle, bloom_filter, key_bounds))
    }

    /// Returns the path for the file being written.
    pub fn path(&self) -> &StoragePath {
        self.inner.path()
    }

    /// Returns the storage used for this writer.
    pub fn storage(&self) -> &Arc<BufferCache> {
        self.inner.storage()
    }

    fn into_reader_impl(
        self,
        metadata: BatchMetadata,
    ) -> Result<(Reader<(&'static K0, &'static A0, ())>, BatchFilters<K0>), super::reader::Error>
    {
        let any_factories = self.factories.any_factories();

        let cache = self.inner.cache;
        let (file_handle, key_filter, key_bounds) = self.close(metadata)?;
        let key_range = key_bounds
            .as_ref()
            .map(|(min, max)| KeyRange::from_refs(min.as_ref(), max.as_ref()));
        let (reader, membership_filter) =
            Reader::new_with_filter(&[&any_factories], cache, file_handle, key_filter)?;
        let filters = BatchFilters::from_file(key_range, membership_filter);
        Ok((reader, filters))
    }

    /// Finishes writing the layer file and returns a reader for it together
    /// with exact-seek filters.
    pub fn into_reader(
        self,
        metadata: BatchMetadata,
    ) -> Result<(Reader<(&'static K0, &'static A0, ())>, BatchFilters<K0>), super::reader::Error>
    {
        self.into_reader_impl(metadata)
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
///     file::{Factories, format::BatchMetadata},
///     buffer_cache::BufferCache,
/// };
/// let factories = Factories::<DynData, DynUnit>::new::<u32, ()>();
/// let tempdir = tempfile::tempdir().unwrap();
/// let storage_backend = <dyn StorageBackend>::new(&StorageConfig {
///     path: tempdir.path().to_string_lossy().to_string(),
///    cache: Default::default(),
/// }, &StorageOptions::default()).unwrap();
/// let parameters = Parameters::default();
/// let mut file =
///     Writer2::new(&factories, &factories, || Some(Arc::new(BufferCache::new(1024 * 1024))), &*storage_backend, parameters, None).unwrap();
/// for i in 0..1000_u32 {
///     for j in 0..10_u32 {
///         file.write1((&j, &())).unwrap();
///     }
///     file.write0((&i, &())).unwrap();
/// }
/// file.close(BatchMetadata::default()).unwrap();
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
        cache: fn() -> Option<Arc<BufferCache>>,
        storage_backend: &dyn StorageBackend,
        parameters: Parameters,
        key_filter: Option<BatchKeyFilter>,
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
                key_filter,
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

    /// Appends a run of already-encoded column-1 items, all of them.
    ///
    /// The run's first key must be greater than the column-1 key written
    /// before it under the same column-0 key, as for [`write1`](Self::write1).
    /// A debug build decodes the run's first and last keys to check that.  The
    /// order within the run is the order of the file it came from, whose
    /// writer checked it.
    pub fn write1_raw(&mut self, items: &RawItems<'_>) -> Result<(), StorageError> {
        #[cfg(debug_assertions)]
        if let (Some(&first), Some(&last)) = (items.roots.first(), items.roots.last()) {
            let key1 = decode_raw_key(&self.factories1, items.bytes, first);
            if let Some(prev1) = &self.prev1 {
                debug_assert!(
                    **prev1 < *key1,
                    "can't write {prev1:?} then {key1:?} to column 1",
                );
            }
            self.prev1 = Some(decode_raw_key(&self.factories1, items.bytes, last));
        }

        self.inner.write_raw_values::<K1, A1>(1, items)
    }

    /// Appends one already-encoded column-0 item, whose column-1 rows must
    /// all have been written since the previous column-0 item, and returns
    /// whether it went in.
    ///
    /// The key must be greater than the previous column-0 key, as for
    /// [`write0`](Self::write0); a debug build decodes it to check.
    ///
    /// Refuses a key for a file whose key filter needs the keys themselves
    /// rather than hashes of them, or whose key type cannot be hashed from
    /// its archived form: such a filter cannot be fed from a key that is
    /// never decoded, and a file whose filter is missing keys answers a query
    /// wrongly.  The caller writes the key decoded instead.
    ///
    /// # Arguments
    ///
    /// * `item` - the key, encoded as the source file stored it.
    ///
    /// # Returns
    ///
    /// Whether the key went in.
    pub fn write0_raw(&mut self, item: &RawItem<'_>) -> Result<bool, StorageError> {
        let written = self.inner.write_raw_key::<K0, A0>(0, item)?;
        // A refused key comes back decoded through `write0`, which checks it.
        #[cfg(debug_assertions)]
        if written {
            let key0 = decode_raw_key(&self.factories0, item.bytes, item.root);
            if let Some(prev0) = &self.prev0 {
                debug_assert!(
                    **prev0 < *key0,
                    "can't write {prev0:?} then {key0:?} to column 0",
                );
            }
            self.prev0 = Some(key0);
            self.prev1 = None;
        }
        Ok(written)
    }

    /// Returns the number of calls to [`write0`](Self::write0) so far.
    pub fn n_rows(&self) -> u64 {
        self.inner.n_rows()
    }

    /// Finishes writing the layer file and returns the file handle, optional
    /// bloom filter, and column-0 key bounds.
    ///
    /// This function will panic if [`write1`](Self::write1) has been called
    /// without a subsequent call to [`write0`](Self::write0).
    ///
    /// # Arguments
    ///
    /// * `metadata` - Batch metadata to include in the trailer.
    pub fn close(
        mut self,
        metadata: BatchMetadata,
    ) -> Result<
        (
            Arc<dyn FileReader>,
            Option<BatchKeyFilter>,
            Option<(Box<K0>, Box<K0>)>,
        ),
        StorageError,
    > {
        let key_bounds = self.inner.finish_column::<K0, A0>(0)?;
        let _ = self.inner.finish_column::<K1, A1>(1)?;
        let (file_handle, bloom_filter) = self.inner.close(metadata)?;
        Ok((file_handle, bloom_filter, key_bounds))
    }

    /// Returns the storage used for this writer.
    pub fn storage(&self) -> &Arc<BufferCache> {
        self.inner.storage()
    }

    /// Returns the path for the file being written.
    pub fn path(&self) -> &StoragePath {
        self.inner.path()
    }

    fn into_reader_impl(
        self,
        metadata: BatchMetadata,
    ) -> Result<
        (
            Reader<(&'static K0, &'static A0, (&'static K1, &'static A1, ()))>,
            BatchFilters<K0>,
        ),
        super::reader::Error,
    > {
        let any_factories0 = self.factories0.any_factories();
        let any_factories1 = self.factories1.any_factories();
        let cache = self.inner.cache;
        let (file_handle, key_filter, key_bounds) = self.close(metadata)?;
        let key_range = key_bounds
            .as_ref()
            .map(|(min, max)| KeyRange::from_refs(min.as_ref(), max.as_ref()));
        let (reader, membership_filter) = Reader::new_with_filter(
            &[&any_factories0, &any_factories1],
            cache,
            file_handle,
            key_filter,
        )?;
        let filters = BatchFilters::from_file(key_range, membership_filter);
        Ok((reader, filters))
    }

    /// Finishes writing the layer file and returns a reader for it together
    /// with exact-seek filters.
    #[allow(clippy::type_complexity)]
    pub fn into_reader(
        self,
        metadata: BatchMetadata,
    ) -> Result<
        (
            Reader<(&'static K0, &'static A0, (&'static K1, &'static A1, ()))>,
            BatchFilters<K0>,
        ),
        super::reader::Error,
    > {
        self.into_reader_impl(metadata)
    }
}

#[cfg(test)]
mod splice_test {
    //! Does copying a run of encoded items from one block to another preserve
    //! them exactly?
    //!
    //! The copy rewrites nothing, so the question is whether rkyv's relative
    //! pointers and alignment survive the move.  These tests build a block the
    //! ordinary way, splice runs out of it into a second block, and read the
    //! second block back, which is the only check that matters: if a pointer
    //! or an alignment were wrong, the values would come back wrong or the
    //! read would fault.

    use std::{ops::RangeInclusive, sync::Arc};

    use feldera_storage::fbuf::FBuf;

    use super::{DataBlockBuilder, DataBlockHeader, Parameters, RawItems};
    use crate::storage::file::{
        SerializerInner,
        item::{ArchivedRefTup2, RefTup2},
    };
    use crate::{
        dynamic::{DynData, Erase},
        storage::{
            backend::BlockLocation,
            file::{
                Factories,
                format::{FixedLen, VERSION_NUMBER, Varint},
                reader::DataBlock as ReadBlock,
            },
        },
    };

    type K = String;
    type A = i64;

    fn factories() -> Factories<DynData, DynData> {
        Factories::<DynData, DynData>::new::<K, A>()
    }

    /// A key whose encoded length varies with `i`, so the run being copied is
    /// not a uniform stride and every item lands at its own alignment.
    fn key(i: usize) -> K {
        format!("key-{i:04}-{}", "x".repeat(i % 17))
    }

    fn aux(i: usize) -> A {
        (i as i64) * 1_000 - 7
    }

    /// Builds one data block holding items `0..n`.
    fn build_raw(n: usize, parameters: &Arc<Parameters>) -> FBuf {
        let factories = factories();
        let mut builder = DataBlockBuilder::new(&factories.any_factories(), parameters);
        let mut serializer = SerializerInner::new();
        for i in 0..n {
            let (mut k, mut a) = (key(i), aux(i));
            builder
                .try_add_item::<DynData, DynData>(
                    (k.erase_mut(), a.erase_mut()),
                    &None,
                    &mut serializer,
                )
                .expect("the test block is sized to hold every item");
        }
        builder.build::<DynData, DynData>().raw
    }

    fn one_block(n: usize, parameters: &Arc<Parameters>) -> ReadBlock<DynData, DynData> {
        read_back(build_raw(n, parameters))
    }

    fn read_back(raw: FBuf) -> ReadBlock<DynData, DynData> {
        read_back_as(raw, VERSION_NUMBER)
    }

    fn read_back_as(raw: FBuf, version: u32) -> ReadBlock<DynData, DynData> {
        let location = BlockLocation {
            offset: 0,
            size: raw.len(),
        };
        ReadBlock::from_raw(Arc::new(raw), location, 0, version)
            .expect("a block this writer just built should read back")
    }

    fn items_of(block: &ReadBlock<DynData, DynData>, n: usize) -> Vec<(K, A)> {
        let factories = factories();
        (0..n)
            .map(|i| {
                let (mut k, mut a) = (K::default(), A::default());
                // SAFETY: this test built the block from items of these
                // factories, and `i` is less than its item count.
                unsafe { block.item(&factories, i, (k.erase_mut(), a.erase_mut())) };
                (k, a)
            })
            .collect()
    }

    /// Offers the items of `items` to `builder` one at a time, as a column
    /// writer does, and returns how many it took before it refused one.
    fn add_run(builder: &mut DataBlockBuilder, items: &RawItems<'_>) -> usize {
        (0..items.len())
            .take_while(|&index| builder.try_add_raw_item(&items.item(index), None))
            .count()
    }

    /// Copies `first..=last` of `source` into a fresh block and reads it back.
    fn splice(
        source: &ReadBlock<DynData, DynData>,
        first: usize,
        last: usize,
        parameters: &Arc<Parameters>,
    ) -> (ReadBlock<DynData, DynData>, usize) {
        let factories = factories();
        let mut builder = DataBlockBuilder::new(&factories.any_factories(), parameters);
        let mut taken = 0;
        while taken <= last - first {
            let rest = source
                .raw_items(&factories, first + taken..=last)
                .expect("a block this version wrote can be spliced");
            let more = add_run(&mut builder, &rest);
            if more == 0 {
                break;
            }
            taken += more;
        }
        (read_back(builder.build::<DynData, DynData>().raw), taken)
    }

    fn parameters() -> Arc<Parameters> {
        Arc::new(Parameters {
            min_data_block: 1 << 20,
            ..Parameters::default()
        })
    }

    #[test]
    fn a_spliced_run_reads_back_unchanged() {
        let parameters = parameters();
        let source = one_block(64, &parameters);
        for (first, last) in [(0, 0), (0, 63), (1, 1), (5, 20), (63, 63), (31, 32)] {
            let (spliced, taken) = splice(&source, first, last, &parameters);
            assert_eq!(
                taken,
                last - first + 1,
                "run {first}..={last} was cut short"
            );
            let expected: Vec<_> = (first..=last).map(|i| (key(i), aux(i))).collect();
            assert_eq!(
                items_of(&spliced, taken),
                expected,
                "run {first}..={last} came back changed"
            );
        }
    }

    /// A short key, which `rkyv` keeps inside the item's root, or for every
    /// fourth `i` a long one, which it writes out of line, ahead of the root.
    ///
    /// # Arguments
    ///
    /// * `i` - which key; the keys sort in the order of `i`.
    ///
    /// # Returns
    ///
    /// Key `i`.
    fn short_or_long_key(i: usize) -> K {
        if i % 4 == 3 {
            format!("k{i:02}-{}", "l".repeat(16 + i % 5))
        } else {
            format!("k{i:02}")
        }
    }

    /// An item taken alone is the same bytes as the same item taken in any
    /// run that holds it, with its root in the same place, and keeps the same
    /// alignment as the run.
    ///
    /// A merge copies keys an item at a time and values a run at a time, so
    /// the two have to agree on where every item starts and ends.  The
    /// alignment a copy keeps is the one the item type declares, so it is the
    /// same for every item and every run: every fourth key here is held out
    /// of line and the others inline, and none of them asks for anything
    /// different.
    #[test]
    fn an_item_taken_alone_matches_the_same_item_in_any_run() {
        const N: usize = 40;
        let parameters = parameters();
        let factories = factories();
        let mut builder = DataBlockBuilder::new(&factories.any_factories(), &parameters);
        let mut serializer = SerializerInner::new();
        for i in 0..N {
            let (mut k, mut a) = (short_or_long_key(i), aux(i));
            builder
                .try_add_item::<DynData, DynData>(
                    (k.erase_mut(), a.erase_mut()),
                    &None,
                    &mut serializer,
                )
                .unwrap();
        }
        let roots = builder.value_offsets.clone();
        let block = read_back(builder.build::<DynData, DynData>().raw);

        // Each item runs from the end of the previous item's root through the
        // end of its own, wherever the encoder put that root.
        let root_size = factories.item_factory.archived_layout().size();
        for (i, &root) in roots.iter().enumerate() {
            let item = block.raw_item(&factories, i).unwrap();
            let start = match i.checked_sub(1) {
                None => DataBlockHeader::LEN,
                Some(previous) => roots[previous] + root_size,
            };
            assert_eq!(item.phase, start, "item {i} starts in the wrong place");
            assert_eq!(
                item.phase + item.root,
                root,
                "item {i} has its root misplaced"
            );
            assert_eq!(
                item.bytes.len() - item.root,
                root_size,
                "item {i} does not end with its root",
            );
        }

        let declared = factories.item_factory.max_align();
        for first in 0..N {
            for last in first..N {
                let run = block.raw_items(&factories, first..=last).unwrap();
                assert_eq!(run.len(), last - first + 1, "run {first}..={last}");
                assert_eq!(run.align, declared, "run {first}..={last}");
                for (n, i) in (first..=last).enumerate() {
                    let in_run = run.item(n);
                    let alone = block.raw_item(&factories, i).unwrap();
                    assert_eq!(
                        in_run.bytes, alone.bytes,
                        "item {i} of run {first}..={last}"
                    );
                    assert_eq!(
                        (in_run.root, in_run.phase),
                        (alone.root, alone.phase),
                        "item {i} of run {first}..={last}",
                    );
                    assert_eq!(
                        (in_run.align, alone.align),
                        (declared, declared),
                        "item {i} of run {first}..={last}",
                    );
                }
            }
        }

        // A range that names no item, a run past the end, and an item past
        // the end get nothing.
        let empty = RangeInclusive::new(1, 0);
        assert!(block.raw_items(&factories, empty).is_none());
        assert!(block.raw_items(&factories, 0..=N).is_none());
        assert!(block.raw_items(&factories, N..=N).is_none());
        assert!(block.raw_item(&factories, N).is_none());
    }

    #[test]
    fn a_splice_survives_every_starting_phase() {
        // The destination pads itself into the source's alignment phase.  Vary
        // how much is already in the destination so that every phase is tried,
        // which is the case a wrong pad would get wrong.
        let parameters = parameters();
        let source = one_block(32, &parameters);
        let factories = factories();
        for prefix in 0..16 {
            let items = source.raw_items(&factories, 8..=23).unwrap();
            let mut builder = DataBlockBuilder::new(&factories.any_factories(), &parameters);
            let mut serializer = SerializerInner::new();
            for i in 0..prefix {
                let (mut k, mut a) = (format!("pre{i}"), -(i as i64));
                builder
                    .try_add_item::<DynData, DynData>(
                        (k.erase_mut(), a.erase_mut()),
                        &None,
                        &mut serializer,
                    )
                    .unwrap();
            }
            let taken = add_run(&mut builder, &items);
            assert_eq!(taken, 16, "prefix {prefix}: the splice was cut short");

            // Reading a misaligned integer gives the right answer on this
            // hardware, so comparing values back cannot catch a bad pad.  The
            // invariant itself is what must hold: every root has to sit at the
            // same offset modulo its alignment as it did where it was written.
            //
            // These keys hold their text out of line, but text needs no
            // alignment, so the copy keeps them in phase only modulo their
            // roots' alignment, and pads by less than that.
            assert_eq!(
                items.align,
                factories.item_factory.archived_layout().align(),
                "a copy of string-keyed items keeps more alignment than their roots need",
            );
            for (n, &root) in items.roots.iter().enumerate() {
                assert_eq!(
                    builder.value_offsets[prefix + n] % items.align,
                    (items.phase + root) % items.align,
                    "prefix {prefix}: root {n} moved to a different alignment"
                );
            }
            let block = read_back(builder.build::<DynData, DynData>().raw);
            let got = items_of(&block, prefix + taken);
            for (n, i) in (8..24).enumerate() {
                assert_eq!(
                    got[prefix + n],
                    (key(i), aux(i)),
                    "prefix {prefix}: item {i} came back changed"
                );
            }
        }
    }

    #[test]
    fn a_run_bigger_than_the_block_is_taken_in_part() {
        // A block that can hold only a few items must take a prefix of the run
        // and say so, rather than overrun its size target or refuse outright.
        let small = Arc::new(Parameters {
            min_data_block: 4096,
            min_branch: 2,
            ..Parameters::default()
        });
        let source = one_block(200, &parameters());
        let (spliced, taken) = splice(&source, 0, 199, &small);
        assert!(taken > 0, "nothing was taken");
        assert!(taken < 200, "a 4 KiB block should not hold all 200 items");
        let expected: Vec<_> = (0..taken).map(|i| (key(i), aux(i))).collect();
        assert_eq!(items_of(&spliced, taken), expected);
    }

    /// An aux type aligned to sixteen bytes, which makes the item's root
    /// aligned that coarsely too.
    type WideA = u128;

    fn wide_factories() -> Factories<DynData, DynData> {
        Factories::<DynData, DynData>::new::<K, WideA>()
    }

    fn wide_aux(i: usize) -> WideA {
        ((i as u128) << 64) | 0x0123_4567_89AB_CDEF
    }

    /// One block of `n` coarsely aligned items, and the same read back.
    fn one_wide_block(n: usize, parameters: &Arc<Parameters>) -> ReadBlock<DynData, DynData> {
        let factories = wide_factories();
        let mut builder = DataBlockBuilder::new(&factories.any_factories(), parameters);
        let mut serializer = SerializerInner::new();
        for i in 0..n {
            let (mut k, mut a) = (key(i), wide_aux(i));
            builder
                .try_add_item::<DynData, DynData>(
                    (k.erase_mut(), a.erase_mut()),
                    &None,
                    &mut serializer,
                )
                .expect("the test block is sized to hold every item");
        }
        read_back(builder.build::<DynData, DynData>().raw)
    }

    fn wide_items_of(block: &ReadBlock<DynData, DynData>, n: usize) -> Vec<(K, WideA)> {
        let factories = wide_factories();
        (0..n)
            .map(|i| {
                let (mut k, mut a) = (K::default(), WideA::default());
                // SAFETY: this test built the block from items of these
                // factories, and `i` is less than its item count.
                unsafe { block.item(&factories, i, (k.erase_mut(), a.erase_mut())) };
                (k, a)
            })
            .collect()
    }

    /// A key whose archived root is aligned to eight bytes but which holds
    /// sixteen-byte-aligned data out of line, ahead of the root.
    type DeepKey = Vec<u128>;

    fn deep_factories() -> Factories<DynData, DynData> {
        Factories::<DynData, DynData>::new::<DeepKey, A>()
    }

    fn deep_key(i: usize) -> DeepKey {
        vec![i as u128 + 1; 1 + i]
    }

    /// Where the `u128`s of the key of the item rooted at `root` start, as an
    /// offset into `raw`, found without decoding anything.
    ///
    /// # Arguments
    ///
    /// * `raw` - a block of items of [`deep_factories`].
    /// * `root` - where one of those items is rooted in `raw`.
    fn deep_payload_offset(raw: &[u8], root: usize) -> usize {
        // SAFETY: the caller passes the root of an item of `deep_factories`.
        let archived: &ArchivedRefTup2<'static, DeepKey, A> =
            unsafe { rkyv::archived_value::<RefTup2<'static, DeepKey, A>>(raw, root) };
        archived.0.as_ptr() as usize - raw.as_ptr() as usize
    }

    /// A copy keeps what an item holds out of line aligned, however loosely
    /// the item's root is aligned.
    ///
    /// `rkyv` writes what an item holds out of line ahead of its root, each
    /// object aligned to its own alignment, so a `Vec<u128>` key keeps its
    /// elements sixteen-aligned behind a root aligned only to eight.  A copy
    /// that kept the item in phase with its root alone could land those
    /// elements eight bytes off, and reading them would be undefined behavior,
    /// which a debug build aborts on.  Every item is copied alone into an
    /// empty block, and after another item, the way a merge copies keys.
    #[test]
    fn a_copy_keeps_what_an_item_holds_out_of_line_aligned() {
        let parameters = parameters();
        let factories = deep_factories();
        let mut serializer = SerializerInner::new();
        let mut builder = DataBlockBuilder::new(&factories.any_factories(), &parameters);
        for i in 0..4 {
            let (mut k, mut a) = (deep_key(i), aux(i));
            builder
                .try_add_item::<DynData, DynData>(
                    (k.erase_mut(), a.erase_mut()),
                    &None,
                    &mut serializer,
                )
                .unwrap();
        }
        let source = read_back(builder.build::<DynData, DynData>().raw);

        for i in 0..4 {
            for copied in [vec![i], vec![(i + 1) % 4, i]] {
                let mut copy = DataBlockBuilder::new(&factories.any_factories(), &parameters);
                for &j in &copied {
                    let item = source.raw_item(&factories, j).unwrap();
                    assert!(copy.try_add_raw_item(&item, None));
                }
                for (&j, &root) in copied.iter().zip(&copy.value_offsets) {
                    let offset = deep_payload_offset(copy.raw.as_slice(), root);
                    assert_eq!(
                        offset % 16,
                        0,
                        "copying items {copied:?} put item {j}'s u128s at offset {offset}"
                    );
                }
                let block = read_back(copy.build::<DynData, DynData>().raw);
                for (n, &j) in copied.iter().enumerate() {
                    let (mut k, mut a) = (DeepKey::default(), A::default());
                    // SAFETY: the block holds `copied.len()` items of these
                    // factories.
                    unsafe { block.item(&factories, n, (k.erase_mut(), a.erase_mut())) };
                    assert_eq!((k, a), (deep_key(j), aux(j)), "item {j} came back changed");
                }
            }
        }
    }

    /// The same invariant as [`a_splice_survives_every_starting_phase`], over
    /// a root aligned to sixteen rather than eight.
    ///
    /// Sixteen is the strictest alignment a primitive needs, so this pins
    /// down that the arithmetic holds there too.  An item whose root is aligned
    /// less strictly than what it holds out of line is
    /// [`a_copy_keeps_what_an_item_holds_out_of_line_aligned`].
    #[test]
    fn a_coarsely_aligned_splice_keeps_its_phase() {
        let parameters = parameters();
        let source = one_wide_block(32, &parameters);
        let factories = wide_factories();
        let mut pads = Vec::new();

        for prefix in 0..16 {
            let items = source.raw_items(&factories, 8..=23).unwrap();
            assert!(
                items.align >= 16,
                "this test needs a root aligned more coarsely than eight; got {}",
                items.align,
            );

            let mut builder = DataBlockBuilder::new(&factories.any_factories(), &parameters);
            let mut serializer = SerializerInner::new();
            for i in 0..prefix {
                let (mut k, mut a) = (format!("pre{i}"), wide_aux(i + 900));
                builder
                    .try_add_item::<DynData, DynData>(
                        (k.erase_mut(), a.erase_mut()),
                        &None,
                        &mut serializer,
                    )
                    .unwrap();
            }

            let before = builder.raw.len();
            let taken = add_run(&mut builder, &items);
            assert_eq!(taken, 16, "prefix {prefix}: the splice was cut short");
            pads.push((items.phase + items.align - before % items.align) % items.align);

            for (n, &root) in items.roots.iter().enumerate() {
                assert_eq!(
                    builder.value_offsets[prefix + n] % items.align,
                    (items.phase + root) % items.align,
                    "prefix {prefix}: root {n} moved to a different alignment",
                );
            }

            let block = read_back(builder.build::<DynData, DynData>().raw);
            let got = wide_items_of(&block, prefix + taken);
            for (n, i) in (8..24).enumerate() {
                assert_eq!(
                    got[prefix + n],
                    (key(i), wide_aux(i)),
                    "prefix {prefix}: item {i} came back changed",
                );
            }
        }

        // Recorded rather than asserted non-zero: see the note above on why
        // nothing in the tree can produce a pad today.
        assert!(
            pads.iter().all(|&pad| pad % 16 == 0),
            "a pad left the run out of phase: {pads:?}",
        );
    }

    /// A key that holds sixteen-byte-aligned data out of line, like
    /// [`DeepKey`], but whose `ArchivedRepr` declares only eight: the mistake
    /// the writer checks every item it encodes for.
    #[derive(
        Clone,
        Debug,
        Default,
        PartialEq,
        Eq,
        PartialOrd,
        Ord,
        Hash,
        size_of::SizeOf,
        rkyv::Archive,
        rkyv::Serialize,
        rkyv::Deserialize,
        feldera_macros::IsNone,
        feldera_macros::OrdRepr,
        feldera_macros::HashRepr,
    )]
    #[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
    #[archive(compare(PartialEq, PartialOrd))]
    struct Understated(Vec<u128>);

    impl crate::dynamic::ArchivedRepr<Understated> for ArchivedUnderstated {
        const MAX_ALIGN: usize = 8;
    }

    /// A type that declares less alignment than archiving it asks for is
    /// caught when its first item is encoded, in a debug build, rather than
    /// when a merge copies the item and lands its `u128`s misaligned.
    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "asked for alignment 16, more than the 8")]
    fn an_item_whose_type_understates_its_alignment_is_refused() {
        let factories = Factories::<DynData, DynData>::new::<Understated, A>();
        let mut builder = DataBlockBuilder::new(&factories.any_factories(), &parameters());
        let mut serializer = SerializerInner::new();
        let (mut k, mut a) = (Understated(vec![1]), aux(0));
        let _ = builder.try_add_item::<DynData, DynData>(
            (k.erase_mut(), a.erase_mut()),
            &None,
            &mut serializer,
        );
    }

    /// A block that has refused an item refuses it again, rather than
    /// overrunning its target.
    ///
    /// A column writer that is refused writes the block out and offers the
    /// item to the next one, so a block that took items after refusing one
    /// would hold them out of order.
    #[test]
    fn a_filled_block_takes_nothing_more_of_a_run() {
        let small = Arc::new(Parameters {
            min_data_block: 4096,
            min_branch: 2,
            ..Parameters::default()
        });
        let factories = factories();
        let source = one_block(200, &parameters());

        let mut builder = DataBlockBuilder::new(&factories.any_factories(), &small);
        let mut taken = 0;
        let mut calls = 0;
        loop {
            let rest = source.raw_items(&factories, taken..=199).unwrap();
            let more = add_run(&mut builder, &rest);
            calls += 1;
            if more == 0 {
                break;
            }
            taken += more;
            assert!(calls < 200, "the block never stopped taking items");
        }

        assert!(taken > 0, "nothing was taken at all");
        assert!(taken < 200, "a 4 KiB block should not hold all 200 items");
        // Asking once more changes nothing: the answer stays zero.
        let rest = source.raw_items(&factories, taken..=199).unwrap();
        assert_eq!(
            add_run(&mut builder, &rest),
            0,
            "a full block took more when asked a second time",
        );

        let block = read_back(builder.build::<DynData, DynData>().raw);
        let expected: Vec<_> = (0..taken).map(|i| (key(i), aux(i))).collect();
        assert_eq!(items_of(&block, taken), expected);
    }

    /// A block filled by copying items stops where one filled by encoding them
    /// does, and builds to the same bytes when the copy starts in the phase
    /// the encoder writes at.
    ///
    /// Both ask whether the block, with the item in it, would still build
    /// within its size target -- the encoding path after writing the item, the
    /// copy before.  A copy keeps the items in phase modulo the alignment
    /// their type declares, and a run taken from the middle of the source may
    /// start out of phase with a fresh block; the copy then opens with a pad
    /// the encoder has no reason to write, which can displace at most one
    /// item.
    #[test]
    fn a_copied_block_fills_like_an_encoded_one() {
        const N: usize = 400;
        let small = Arc::new(Parameters {
            min_data_block: 4096,
            min_branch: 2,
            ..Parameters::default()
        });
        let factories = factories();
        let source = one_block(N, &parameters());
        let mut serializer = SerializerInner::new();

        let mut first = 0;
        while first < N {
            let mut encoded = DataBlockBuilder::new(&factories.any_factories(), &small);
            let mut n_encoded = 0;
            while first + n_encoded < N {
                let i = first + n_encoded;
                let (mut k, mut a) = (key(i), aux(i));
                if encoded
                    .try_add_item::<DynData, DynData>(
                        (k.erase_mut(), a.erase_mut()),
                        &None,
                        &mut serializer,
                    )
                    .is_err()
                {
                    break;
                }
                n_encoded += 1;
            }

            let mut copied = DataBlockBuilder::new(&factories.any_factories(), &small);
            let rest = source.raw_items(&factories, first..=N - 1).unwrap();
            let n_copied = add_run(&mut copied, &rest);

            if (rest.phase - DataBlockHeader::LEN).is_multiple_of(rest.align) {
                assert_eq!(
                    n_copied, n_encoded,
                    "the blocks starting at item {first} took different numbers of items",
                );
                assert_eq!(
                    copied.build::<DynData, DynData>().raw.as_slice(),
                    encoded.build::<DynData, DynData>().raw.as_slice(),
                    "the blocks starting at item {first} came out different",
                );
            } else {
                assert!(
                    n_copied <= n_encoded && n_encoded <= n_copied + 1,
                    "the block copied from item {first} took {n_copied} items, \
                     the encoded one {n_encoded}",
                );
                let block = read_back(copied.build::<DynData, DynData>().raw);
                let expected: Vec<_> = (first..first + n_copied)
                    .map(|i| (key(i), aux(i)))
                    .collect();
                assert_eq!(items_of(&block, n_copied), expected);
            }
            first += n_copied;
        }
    }

    /// Offers the items of `items` to `builder` one at a time, each with its
    /// row group, as a column writer does for a column other than the last.
    ///
    /// # Arguments
    ///
    /// * `builder` - the block to fill.
    /// * `items` - the run to offer.
    /// * `bounds` - the row groups' boundaries: item `n` of the run owns rows
    ///   `bounds[n]..bounds[n + 1]`.
    ///
    /// # Returns
    ///
    /// How many items `builder` took before it refused one.
    fn add_run_with_row_groups(
        builder: &mut DataBlockBuilder,
        items: &RawItems<'_>,
        bounds: &[u64],
    ) -> usize {
        (0..items.len())
            .take_while(|&n| {
                builder.try_add_raw_item(&items.item(n), Some(bounds[n]..bounds[n + 1]))
            })
            .count()
    }

    /// The same as [`a_copied_block_fills_like_an_encoded_one`], for items
    /// that own row groups.
    ///
    /// A block stores its row-group boundaries in the narrowest width that
    /// holds the largest of them, so the first boundary past a width's limit
    /// widens every boundary in the block at once.  The encoder sees that
    /// after it adds an item and the copy has to foresee it, or the two fill
    /// their blocks differently.  The boundaries here start at zero and just
    /// below the limits of one, two and three bytes, so that blocks cross
    /// those limits as they fill.  A block starts at every item of the
    /// source, as a copy may, which leaves it a different amount of room when
    /// it fills, and the blocks set their size targets after different
    /// numbers of items.
    #[test]
    fn a_copied_block_with_row_groups_fills_like_an_encoded_one() {
        const N: usize = 600;
        let factories = factories();
        let source = one_block(N, &parameters());
        let mut serializer = SerializerInner::new();
        // Blocks that started in phase and crossed a width's limit as they
        // filled, which are the ones this test is about.
        let mut widened = 0;

        for base in [0, 200, 65_400, (1 << 24) - 300] {
            // Item `i` owns rows `bounds[i]..bounds[i + 1]`, one to four of
            // them.
            let bounds: Vec<u64> = (0..=N as u64)
                .scan(base, |next, i| {
                    let bound = *next;
                    *next += 1 + i % 4;
                    Some(bound)
                })
                .collect();
            for first in 0..N {
                let min_branch = [1, 2, 3, 5, 32][first % 5];
                let small = Arc::new(Parameters {
                    min_data_block: 4096,
                    min_branch,
                    ..Parameters::default()
                });

                let mut encoded = DataBlockBuilder::new(&factories.any_factories(), &small);
                let n_encoded = (first..N)
                    .take_while(|&i| {
                        let (mut k, mut a) = (key(i), aux(i));
                        encoded
                            .try_add_item::<DynData, DynData>(
                                (k.erase_mut(), a.erase_mut()),
                                &Some(bounds[i]..bounds[i + 1]),
                                &mut serializer,
                            )
                            .is_ok()
                    })
                    .count();

                let mut copied = DataBlockBuilder::new(&factories.any_factories(), &small);
                let rest = source.raw_items(&factories, first..=N - 1).unwrap();
                let n_copied = add_run_with_row_groups(&mut copied, &rest, &bounds[first..]);

                let block = format!(
                    "the block from item {first}, with rows from {base} and \
                     min_branch {min_branch},"
                );
                if (rest.phase - DataBlockHeader::LEN).is_multiple_of(rest.align) {
                    assert_eq!(
                        n_copied, n_encoded,
                        "{block} took different numbers of items copied and encoded",
                    );
                    assert_eq!(
                        copied.build::<DynData, DynData>().raw.as_slice(),
                        encoded.build::<DynData, DynData>().raw.as_slice(),
                        "{block} came out different copied and encoded",
                    );
                    widened += usize::from(
                        Varint::from_max_value(bounds[first + 1])
                            != Varint::from_max_value(bounds[first + n_copied]),
                    );
                } else {
                    assert!(
                        n_copied <= n_encoded && n_encoded <= n_copied + 1,
                        "{block} took {n_copied} items copied and {n_encoded} encoded",
                    );
                    let spliced = read_back(copied.build::<DynData, DynData>().raw);
                    let expected: Vec<_> = (first..first + n_copied)
                        .map(|i| (key(i), aux(i)))
                        .collect();
                    assert_eq!(items_of(&spliced, n_copied), expected, "{block}");
                }
            }
        }
        assert!(
            widened > 0,
            "no block that started in phase crossed a width's limit",
        );
    }

    /// A value too big for the block size still goes in, by growing the block
    /// around it; one offered to a block that has already set its target is
    /// refused instead, so the caller can finish that block and start another.
    #[test]
    fn a_value_larger_than_the_block_size_is_taken_only_by_an_empty_block() {
        // Much larger than `min_data_block`, so no ordinary block holds it.
        const HUGE: usize = 200_000;
        let small = Arc::new(Parameters {
            min_data_block: 4096,
            min_branch: 2,
            ..Parameters::default()
        });
        let factories = factories();

        let roomy = Arc::new(Parameters {
            min_data_block: 1 << 20,
            ..Parameters::default()
        });
        let mut builder = DataBlockBuilder::new(&factories.any_factories(), &roomy);
        let mut serializer = SerializerInner::new();
        for i in 0..3 {
            let (mut k, mut a) = (format!("{i:04}{}", "y".repeat(HUGE)), aux(i));
            builder
                .try_add_item::<DynData, DynData>(
                    (k.erase_mut(), a.erase_mut()),
                    &None,
                    &mut serializer,
                )
                .unwrap();
        }
        let source = read_back(builder.build::<DynData, DynData>().raw);

        // A fresh block has no size target yet -- it sets one from its first
        // `min_branch` items -- so it takes them however big they are and
        // sizes itself around them.
        let items = source.raw_items(&factories, 0..=2).unwrap();
        let mut fresh = DataBlockBuilder::new(&factories.any_factories(), &small);
        let taken = add_run(&mut fresh, &items);
        assert_eq!(
            taken, small.min_branch,
            "an empty block must take its first `min_branch` values however big",
        );
        let raw = fresh.build::<DynData, DynData>().raw;
        assert!(
            raw.len() > small.min_data_block,
            "the block did not grow past its minimum to fit the value: {} bytes",
            raw.len(),
        );
        let block = read_back(raw);
        let expected: Vec<_> = (0..taken)
            .map(|i| (format!("{i:04}{}", "y".repeat(HUGE)), aux(i)))
            .collect();
        assert_eq!(items_of(&block, taken), expected);

        // A block that has already sized itself refuses it, rather than
        // overrunning; the merge then writes the value the slow way.
        let mut started = DataBlockBuilder::new(&factories.any_factories(), &small);
        let ordinary = one_block(8, &parameters());
        let head = ordinary.raw_items(&factories, 0..=7).unwrap();
        assert!(add_run(&mut started, &head) > 0);
        assert_eq!(
            add_run(&mut started, &items),
            0,
            "a block with a size target took a value far beyond it",
        );
    }

    #[test]
    fn a_block_from_a_future_version_is_refused() {
        // The encoding may change between versions, so bytes from one the
        // reader does not know must not be copied blindly, a run or an item
        // at a time.  The same bytes read as this version's are offered, so
        // the version is the only reason for the refusal.
        let parameters = parameters();
        let factories = factories();
        let alien = read_back_as(build_raw(8, &parameters), VERSION_NUMBER + 1);
        let native = read_back(build_raw(8, &parameters));
        assert!(alien.raw_items(&factories, 0..=7).is_none());
        assert!(native.raw_items(&factories, 0..=7).is_some());
        for i in 0..8 {
            assert!(
                alien.raw_item(&factories, i).is_none(),
                "item {i} of a future version's block was offered",
            );
            assert!(
                native.raw_item(&factories, i).is_some(),
                "item {i} of this version's block was refused",
            );
        }
    }
}
