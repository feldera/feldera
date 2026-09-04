//! Layer file writer.
//!
//! Use [`Writer1`] to write a 1-column layer file and [`Writer2`] to write a
//! 2-column layer file.  To write more columns, either add another `Writer<N>`
//! struct, which is easily done, or mark the currently private `Writer` as
//! `pub`.
use super::format::Compression;
use super::{AnyFactories, BatchKeyFilter, Factories, reader::RawItems, reader::Reader};
use crate::storage::{
    backend::{BlockLocation, FileReader, FileWriter, StorageBackend, StorageError},
    buffer_cache::{BufferCache, FBuf, FBufSerializer, LimitExceeded},
    file::{
        SerializerInner,
        format::{
            BatchMetadata, BlockHeader, BloomFilterBlockRef, COMPATIBLE_FEATURE_FILTER64,
            COMPATIBLE_FEATURE_NEGATIVE_WEIGHT_COUNT, DATA_BLOCK_MAGIC, DataBlockHeader,
            FILE_TRAILER_BLOCK_MAGIC, FileTrailer, FileTrailerColumn, FixedLen,
            INCOMPATIBLE_FEATURE_HIDDEN_VALUE_COLUMN, INCOMPATIBLE_FEATURE_MODULAR_FILTERS,
            INCOMPATIBLE_FEATURE_ROARING_FILTERS, INDEX_BLOCK_MAGIC, IndexBlockHeader,
            MODULAR_BLOOM_FILTER_BLOCK_MAGIC, ModularBloomFilterBlockRef, NodeType,
            ROARING_BITMAP_FILTER_BLOCK_MAGIC, RoaringBitmapFilterBlockRef, VERSION_NUMBER, Varint,
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
            compression: Some(Compression::Snappy),
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

    /// Takes as much of `items` as the open block will hold, and returns how
    /// many it took.  A caller with more to give calls again: a short return
    /// means the block was finished and a fresh one is waiting.
    fn add_raw_items<K, A>(
        &mut self,
        block_writer: &mut BlockWriter,
        items: &RawItems<'_>,
        row_groups: Option<(&[u64], i64)>,
        serializer: &mut SerializerInner,
    ) -> Result<usize, StorageError>
    where
        K: DataTrait + ?Sized,
        A: DataTrait + ?Sized,
    {
        let mut taken = self.data_block.try_add_raw_items::<K, A>(items, row_groups);
        if taken < items.roots.len() && !self.data_block.is_empty() {
            let data_block = self.data_block.build::<K, A>();
            self.write_data_block::<K, A>(block_writer, data_block, serializer)?;
            if taken == 0 {
                taken = self.data_block.try_add_raw_items::<K, A>(items, row_groups);
            }
        }
        Ok(taken)
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
    /// Copies as many of `items` into this block as will fit, without decoding
    /// any of them, and returns how many it took.
    ///
    /// The bytes are moved verbatim, so every relative pointer inside them
    /// still points where it did; what has to be re-established is alignment,
    /// which the leading pad does by putting the run back in the phase it was
    /// written in.  A column whose rows have row groups can be spliced only
    /// where the caller brings them, since a row group names rows in the next
    /// column by absolute number and those numbers change when a run moves.
    fn try_add_raw_items<K, A>(
        &mut self,
        items: &RawItems<'_>,
        row_groups: Option<(&[u64], i64)>,
    ) -> usize
    where
        K: DataTrait + ?Sized,
        A: DataTrait + ?Sized,
    {
        if items.roots.is_empty() {
            return 0;
        }
        match row_groups {
            // A column whose rows have row groups can only be spliced if the
            // caller brings them; one whose rows have none must not be given
            // any.
            Some((boundaries, _)) if boundaries.len() != items.roots.len() + 1 => return 0,
            None if !self.row_groups.is_empty() => return 0,
            _ => {}
        }
        let root_size = self
            .factories
            .item_factory::<K, A>()
            .archived_layout()
            .size();
        let room = self.parameters.max_branch()
            - self.value_offsets.len().min(self.parameters.max_branch());
        if room == 0 {
            return 0;
        }

        let pad = (items.phase + items.align - self.raw.len() % items.align) % items.align;
        let start = self.raw.len() + pad;

        // Take only what the block has room for, so that the check below, which
        // has to build the block's layout to be exact, rarely has to give
        // anything back.
        let mut count = items.roots.len().min(room);
        if self.size_target.is_none() {
            // A block sets its size target from its first `min_branch` items,
            // so until it has them there is no budget to measure a run
            // against.  Take it that far and no further; the caller asks again
            // with the rest, by which time the target is set.
            count = count.min(
                self.parameters
                    .min_branch
                    .saturating_sub(self.value_offsets.len())
                    .max(1),
            );
        } else if let Some(size_target) = self.size_target {
            let budget = size_target.saturating_sub(self.specs().len + pad);
            count = count.min(
                items
                    .roots
                    .partition_point(|&offset| offset + root_size <= budget),
            );
        }
        if count == 0 {
            return 0;
        }

        let old_len = self.raw.len();
        let old_stride = self.value_offset_stride;
        self.raw.resize(start, 0);
        self.raw
            .extend_from_slice(&items.bytes[..items.roots[count - 1] + root_size]);
        for &offset in &items.roots[..count] {
            self.value_offsets.push(start + offset);
            self.value_offset_stride.push(start + offset);
        }
        let old_row_groups = self.row_groups.0.len();
        if let Some((boundaries, delta)) = row_groups {
            // The run's rows now sit `delta` further along the next column, so
            // every boundary moves by the same amount and the ranges stay
            // consecutive.
            let shift = |row: u64| (row as i64 + delta) as u64;
            for pair in boundaries[..=count].windows(2) {
                self.row_groups.push(&(shift(pair[0])..shift(pair[1])));
            }
        }

        // The estimate above ignores how much the value map itself grows, so
        // confirm the block still fits and hand back the tail if it does not.
        if let Some(size_target) = self.size_target {
            let wanted = count;
            while count > 0 && self.specs().len > size_target {
                count -= 1;
                self.value_offsets.pop();
                if row_groups.is_some() {
                    self.row_groups.pop();
                }
                self.raw.resize(
                    if count == 0 {
                        old_len
                    } else {
                        start + items.roots[count - 1] + root_size
                    },
                    0,
                );
            }
            if count == 0 {
                self.raw.resize(old_len, 0);
                self.value_offset_stride = old_stride;
                self.row_groups.0.truncate(old_row_groups);
                return 0;
            }
            if count < wanted {
                // Popping cannot be undone on the stride builder, so rebuild
                // it -- but only where something was popped.  Rebuilding on
                // every call costs the block's items over again each time,
                // which is a run's length squared for a caller that splices
                // one item at a time.
                self.value_offset_stride = StrideBuilder::new();
                for &offset in &self.value_offsets {
                    self.value_offset_stride.push(offset);
                }
            }
        } else if self.value_offsets.len() >= self.parameters.min_branch {
            self.size_target = Some(
                self.specs()
                    .len
                    .next_multiple_of(512)
                    .max(self.parameters.min_data_block),
            );
        }
        count
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

    /// Whether an archived key reproduces the hash its decoded form would
    /// have, asked of the first key a splice offers and remembered.
    ///
    /// The answer is a property of the key type, and asking it means taking a
    /// hash: a filter that recorded every key twice, once to find out and
    /// once for real, would spend more on the second hash than a splice
    /// saves on the key.
    key_hashable: Option<bool>,

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
            key_hashable: None,
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

    /// Whether the keys of `items` can be recorded in this file's membership
    /// filter without decoding them.
    ///
    /// Neither half of the answer varies from one item to the next, so one
    /// look settles the whole run: a filter that wants the key itself rather
    /// than a hash of it can never be fed this way, and whether an archived
    /// key reproduces the decoded key's hash is a property of the key type.
    fn can_hash_run<K>(&mut self, column: usize, items: &RawItems<'_>) -> bool
    where
        K: DataTrait + ?Sized,
    {
        let Some(filter) = &self.key_filter else {
            return true;
        };
        if !filter.takes_hashes() {
            return false;
        }
        if let Some(known) = self.key_hashable {
            return known;
        }
        let Some(&root) = items.roots.first() else {
            // Nothing to answer for, and nothing to remember: an empty run
            // says nothing about the key type.
            return true;
        };
        let key_factory = self.cws[column].factories.key_factory::<K>();
        // SAFETY: `root` is where the source block put this item, and
        // `items.bytes` is that block's bytes; `raw_items` produced the two
        // together.
        let archived = unsafe { key_factory.archived_value(items.bytes, root) };
        let hashable = archived.archived_hash().is_some();
        self.key_hashable = Some(hashable);
        hashable
    }

    /// Appends a run of already-encoded items to `column`, returning how many
    /// were taken.  A short return leaves the rest for another call.
    ///
    /// Only the last column can be spliced.  Every other column stores, with
    /// each of its items, the range of rows in the next column that belong to
    /// it, and those row numbers change when a run is moved.
    pub fn write_raw<K, A>(
        &mut self,
        column: usize,
        items: &RawItems<'_>,
        boundaries: Option<&[u64]>,
    ) -> Result<usize, StorageError>
    where
        K: DataTrait + ?Sized,
        A: DataTrait + ?Sized,
    {
        // A membership filter has to record every key, and a splice never
        // decodes one.  The keys are here all the same, as the bytes about to
        // be copied, and `HashRepr` promises that a hash taken from an
        // archived key is the one the decoded key would have had, which is
        // what the filter is queried with later.  Where the promise is not
        // available this refuses, and the caller rewrites the run instead: a
        // file whose filter is missing a key answers a query for it wrongly.
        let feeds_filter = column == 0 && self.key_filter.is_some();
        if feeds_filter && !self.can_hash_run::<K>(column, items) {
            return Ok(0);
        }
        let last = column + 1 == self.n_columns();
        assert_eq!(
            last,
            boundaries.is_none(),
            "column {column} of {} needs row groups iff it is not the last",
            self.n_columns(),
        );

        // The run's rows have already been written to the next column, where
        // they sit at that column's pending range; the whole run therefore
        // moves by the distance between there and where it sat in the file it
        // came from.
        let pending = (!last).then(|| self.cws[column + 1].take_rows());
        let row_groups = boundaries.map(|boundaries| {
            let pending = pending.as_ref().unwrap();
            assert!(
                pending.end - pending.start >= boundaries[boundaries.len() - 1] - boundaries[0],
                "the run's rows were not written to the next column first",
            );
            (boundaries, pending.start as i64 - boundaries[0] as i64)
        });

        let taken = self.cws[column].add_raw_items::<K, A>(
            &mut self.writer,
            items,
            row_groups,
            &mut self.serializer,
        )?;
        self.cws[column].rows.end += taken as u64;
        if feeds_filter {
            // Only what was taken: the rest is offered again next call, and
            // is hashed then rather than now.
            let key_factory = self.cws[column].factories.key_factory::<K>();
            let filter = self.key_filter.as_mut().expect("the file has a filter");
            for &root in &items.roots[..taken] {
                // SAFETY: `root` is where the source block put this item, and
                // `items.bytes` is that block's bytes; `raw_items` produced
                // the two together.
                let archived = unsafe { key_factory.archived_value(items.bytes, root) };
                let hash = archived
                    .archived_hash()
                    .expect("the key type answered before the run was written");
                let recorded = filter.push_hash(hash);
                debug_assert!(
                    recorded,
                    "the filter took hashes before the run was written"
                );
            }
        }
        if let (Some(boundaries), Some(pending)) = (boundaries, pending) {
            // What the run did not claim stays pending for the next call,
            // counted in this file's rows rather than the source's.  That is
            // both the rows of whatever items were not taken and any rows
            // written ahead of this run.
            let consumed = boundaries[taken] - boundaries[0];
            self.cws[column + 1].rows = (pending.start + consumed)..pending.end;
        }
        Ok(taken)
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

        // A stamped value column is unreadable to a binary that does not know to
        // hide the trailing column, so advertise it as incompatible.
        if metadata.value_stamp.is_stamped() {
            incompatible_features |= INCOMPATIBLE_FEATURE_HIDDEN_VALUE_COLUMN;
        }

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

    /// Appends a run of already-encoded column-1 items and returns how many
    /// were taken, which may be fewer than offered; the caller asks again with
    /// the rest.
    ///
    /// The caller keeps the ordering that [`write1`](Self::write1) checks for
    /// itself, because these items are never decoded.
    pub fn write1_raw(&mut self, items: &RawItems<'_>) -> Result<usize, StorageError> {
        self.inner.write_raw::<K1, A1>(1, items, None)
    }

    /// Appends a run of already-encoded column-0 items, whose column-1 rows
    /// must already have been written, and returns how many were taken.
    ///
    /// `boundaries` are the run's row groups as the `n + 1` rows between them,
    /// straight from
    /// [`Cursor::raw_run_with_row_groups`](super::reader::Cursor::raw_run_with_row_groups).
    ///
    /// Returns zero for a file whose key filter needs the keys themselves
    /// rather than hashes of them, or whose key type cannot be hashed from
    /// its archived form: such a filter cannot be fed from a run that is
    /// never decoded, and a file whose filter is missing keys answers a query
    /// wrongly.  The caller rewrites the run instead.
    pub fn write0_raw(
        &mut self,
        items: &RawItems<'_>,
        boundaries: &[u64],
    ) -> Result<usize, StorageError> {
        // Column 1's order restarts under each key, which is why
        // [`write0`](Self::write0) forgets the last value it saw.  A key that
        // goes in this way is never decoded, so there is no key to remember
        // either; both checks resume with the next decoded item.
        #[cfg(debug_assertions)]
        {
            self.prev0 = None;
            self.prev1 = None;
        }
        self.inner.write_raw::<K0, A0>(0, items, Some(boundaries))
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

    use std::sync::Arc;

    use feldera_storage::fbuf::FBuf;

    use super::{DataBlockBuilder, Parameters};
    use crate::storage::file::SerializerInner;
    use crate::{
        dynamic::{DynData, Erase},
        storage::{
            backend::BlockLocation,
            file::{Factories, format::VERSION_NUMBER, reader::DataBlock as ReadBlock},
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
                unsafe { block.item(&factories, i, (k.erase_mut(), a.erase_mut())) };
                (k, a)
            })
            .collect()
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
                .raw_items(&factories, first + taken, last)
                .expect("a block this version wrote can be spliced");
            let more = builder.try_add_raw_items::<DynData, DynData>(&rest, None);
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

    #[test]
    fn a_splice_survives_every_starting_phase() {
        // The destination pads itself into the source's alignment phase.  Vary
        // how much is already in the destination so that every phase is tried,
        // which is the case a wrong pad would get wrong.
        let parameters = parameters();
        let source = one_block(32, &parameters);
        let factories = factories();
        for prefix in 0..16 {
            let items = source.raw_items(&factories, 8, 23).unwrap();
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
            let taken = builder.try_add_raw_items::<DynData, DynData>(&items, None);
            assert_eq!(taken, 16, "prefix {prefix}: the splice was cut short");

            // Reading a misaligned integer gives the right answer on this
            // hardware, so comparing values back cannot catch a bad pad.  The
            // invariant itself is what must hold: every root has to sit at the
            // same offset modulo its alignment as it did where it was written.
            //
            // For an ordinary item the pad this asserts on comes out zero, and
            // not by luck: a root's size is a multiple of its alignment, so a
            // block's length after any item is a multiple of it too, and both
            // ends of the copy are therefore already in phase.  The pad earns
            // its keep only for a root aligned more coarsely than the block
            // header is long.
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

    #[test]
    fn a_block_from_a_future_version_is_refused() {
        // The encoding may change between versions, so bytes from one the
        // reader does not know must not be copied blindly.
        let parameters = parameters();
        let alien = read_back_as(build_raw(8, &parameters), VERSION_NUMBER + 1);
        assert!(alien.raw_items(&factories(), 0, 7).is_none());
    }
}
