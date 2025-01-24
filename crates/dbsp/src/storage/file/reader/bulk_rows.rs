use std::{
    collections::{BTreeMap, VecDeque},
    fmt::{Debug, Formatter},
    marker::PhantomData,
    ops::Range,
    sync::{
        mpsc::{channel, Receiver, Sender},
        Arc,
    },
};

use feldera_storage::{error::StorageError, fbuf::FBuf};

use crate::{
    dynamic::DataTrait,
    storage::{
        buffer_cache::BufferCache,
        file::{
            format::NodeType,
            reader::{
                decompress, ColumnSpec, DataBlock, Error, IndexBlock, Reader, TreeBlock, TreeNode,
            },
            Factories,
        },
    },
};

struct BulkRead {
    node: TreeNode,
    level: usize,
}

struct BulkReadResults {
    reads: Vec<BulkRead>,
    results: Vec<Result<Arc<FBuf>, StorageError>>,
}

/// Reads all of the data in a column in a [Reader].
///
/// `BulkRows` provides non-blocking access to all of the data in a [Reader]
/// column.  It does all of the I/O asynchronously with heavy readahead.
pub struct BulkRows<'a, K, A, N, T>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    /// The underlying [Reader].
    reader: &'a Reader<T>,

    /// Cache for reading data and index blocks.
    cache: Arc<BufferCache>,

    /// Factories for constructing keys and aux data.
    factories: Factories<K, A>,

    /// The column we're reading.
    column: usize,

    /// The row number in the column that we're currently reading.
    row: u64,

    /// The number of rows in our column.
    n_rows: u64,

    /// Receives asynchronous read results.
    receiver: Receiver<BulkReadResults>,

    /// For cloning and passing to asynchronous read callback handler.
    sender: Sender<BulkReadResults>,

    /// Number of data blocks that are scheduled for reading but not yet read.
    data_pending: usize,

    /// If nonempty, then:
    ///
    /// - `data_blocks[0]` contains `row`.
    ///
    /// - `data_blocks[1..]` sequentially follow, without gaps in row numbering.
    data_blocks: VecDeque<Arc<DataBlock<K, A>>>,

    /// First row in the next block to be added to `data_blocks`.  If
    /// `data_blocks` is nonempty, then this is
    /// `data_blocks.last().unwrap().first_row`.
    next_data: u64,

    /// Data blocks that have been received out of order (that is, for each of
    /// them, their first row is greater than `next_data`).  They will be moved
    /// to `blocks` when `next_data` catches up to their starting row.
    out_of_order_data: BTreeMap<u64, Arc<DataBlock<K, A>>>,

    /// Tracks reading of index blocks.  `indexes[0]` is the root level,
    /// `indexes[1]` the level below that, and so on.
    indexes: Vec<IndexLevel<K>>,

    /// Phantom data.
    _phantom: PhantomData<fn(N)>,
}

impl<'a, K, A, N, T> Debug for BulkRows<'a, K, A, N, T>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
    T: ColumnSpec,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "BulkRows {{ row: {}, n_rows: {}, n_readable: {} }}",
            self.row,
            self.n_rows,
            self.n_readable()
        )
    }
}

impl<'a, K, A, NK, NA, NN, T> BulkRows<'a, K, A, (&'static NK, &'static NA, NN), T>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
    NK: DataTrait + ?Sized,
    NA: DataTrait + ?Sized,
    T: ColumnSpec,
{
    /// Returns a [BulkRows] for the next column.
    pub fn next_column<'b>(&'b self) -> Result<BulkRows<'a, NK, NA, NN, T>, Error> {
        BulkRows::new(self.reader, self.column + 1)
    }
}

impl<'a, K, A, N, T> BulkRows<'a, K, A, N, T>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
    T: ColumnSpec,
{
    pub(super) fn new(reader: &'a Reader<T>, column: usize) -> Result<Self, Error> {
        let (sender, receiver) = channel();
        let mut this = Self {
            reader,
            cache: (reader.file.cache)(),
            factories: reader.columns[column].factories.factories(),
            column,
            row: 0,
            sender,
            receiver,
            n_rows: reader.columns[column].n_rows,
            indexes: Vec::new(),
            data_blocks: VecDeque::new(),
            next_data: 0,
            out_of_order_data: BTreeMap::new(),
            data_pending: 0,
            _phantom: PhantomData,
        };
        if let Some(node) = &reader.columns[column].root {
            let reads = this.start_block_read(node, 0)?.into_iter().collect();
            this.work_(reads)?;
        }
        Ok(this)
    }

    fn start_index_read(
        &mut self,
        node: &TreeNode,
        level: usize,
    ) -> Result<Option<BulkRead>, Error> {
        if level >= self.indexes.len() {
            self.indexes.push(IndexLevel::new());
        }
        self.indexes[level].pending += 1;
        if let Some(cache_entry) = self
            .cache
            .get(&*self.reader.file.file_handle, node.location)
        {
            self.indexes[level].received(IndexBlock::from_cache_entry(cache_entry, node.location)?);
            Ok(None)
        } else {
            Ok(Some(BulkRead {
                node: node.clone(),
                level,
            }))
        }
    }

    fn start_data_read(&mut self, node: &TreeNode) -> Result<Option<BulkRead>, Error> {
        self.data_pending += 1;
        if let Some(cache_entry) = self
            .cache
            .get(&*self.reader.file.file_handle, node.location)
        {
            self.received_data(DataBlock::from_cache_entry(cache_entry, node.location)?);
            Ok(None)
        } else {
            Ok(Some(BulkRead {
                node: node.clone(),
                level: 0,
            }))
        }
    }

    fn start_block_read(
        &mut self,
        node: &TreeNode,
        level: usize,
    ) -> Result<Option<BulkRead>, Error> {
        match node.node_type {
            NodeType::Data => self.start_data_read(node),
            NodeType::Index => self.start_index_read(node, level),
        }
    }

    fn process_read_results(&mut self, read_results: BulkReadResults) -> Result<(), Error> {
        for (BulkRead { node, level }, result) in read_results
            .reads
            .into_iter()
            .zip(read_results.results.into_iter())
        {
            let raw = decompress(self.reader.file.compression, node.location, result?)?;
            let file_id = self.reader.file.file_handle.file_id();
            let tree_block = TreeBlock::from_raw_with_cache(raw, &node, &self.cache, file_id)?;
            match tree_block {
                TreeBlock::Data(data_block) => self.received_data(data_block),
                TreeBlock::Index(index_block) => self.indexes[level].received(index_block),
            }
        }
        Ok(())
    }

    /// Initiates and continues background work for reading data in this column.
    /// This must be called periodically to keep data flowing.  It limits the
    /// amount of data buffered beyond the current read point.
    pub fn work(&mut self) -> Result<(), Error> {
        self.work_(Vec::new())
    }

    fn work_(&mut self, mut reads: Vec<BulkRead>) -> Result<(), Error> {
        // First, catch up on all completed reads.
        while let Ok(read_results) = self.receiver.try_recv() {
            self.process_read_results(read_results)?;
        }

        // Then schedule more reads.
        let mut level = 0;
        while level < self.indexes.len() {
            while let Some(node) = self.indexes[level].child()? {
                if self.is_level_full(node.node_type, level + 1) {
                    break;
                }
                self.indexes[level].next_child();
                if let Some(read) = self.start_block_read(&node, level + 1)? {
                    reads.push(read);
                }
            }
            level += 1;
        }

        if !reads.is_empty() {
            self.reader.file.file_handle.read_async(
                reads.iter().map(|read| read.node.location).collect(),
                {
                    let sender = self.sender.clone();
                    Box::new(move |results| {
                        let _ = sender.send(BulkReadResults { reads, results });
                    })
                },
            );
        }

        Ok(())
    }

    fn is_level_full(&self, node_type: NodeType, level: usize) -> bool {
        match node_type {
            NodeType::Data => {
                self.data_pending + self.data_blocks.len() + self.out_of_order_data.len() >= 100
            }
            NodeType::Index => self
                .indexes
                .get(level)
                .is_some_and(|child| child.is_full(level)),
        }
    }

    /// Adds `block` to the collection of data blocks.
    fn received_data(&mut self, block: Arc<DataBlock<K, A>>) {
        self.data_pending -= 1;
        if block.first_row == self.next_data {
            self.next_data = block.rows().end;
            self.data_blocks.push_back(block);
            while let Some(entry) = self.out_of_order_data.first_entry() {
                if *entry.key() != self.next_data {
                    break;
                }
                let block = entry.remove();
                self.next_data = block.rows().end;
                self.data_blocks.push_back(block);
            }
        } else if block.first_row > self.next_data {
            self.out_of_order_data.insert(block.first_row, block);
        } else {
            // File corruption or (more likely) a bug.
            todo!()
        }
    }

    /// Returns the number of rows that can currently be read.  If this is 0,
    /// the current row isn't readable.
    pub fn n_readable(&self) -> usize {
        self.data_blocks
            .back()
            .map_or(0, |last| last.rows().end - self.row) as usize
    }

    /// Returns true if all the data has been read, false if there is at least
    /// one row yet to be read.
    pub fn at_eof(&self) -> bool {
        self.row >= self.n_rows
    }

    /// Returns true if the current row can be read, equivalent to
    /// `bulk_rows.n_readable() > 0` but slightly cheaper.
    pub fn is_readable(&self) -> bool {
        !self.data_blocks.is_empty()
    }

    /// Unless we're at end-of-file, blocks until the current row can be read.
    pub fn wait(&mut self) -> Result<(), Error> {
        if self.at_eof() {
            return Ok(());
        }

        while !self.is_readable() {
            // Process received blocks and schedule block reads.  The latter is
            // particularly important on the first loop iteration, since the
            // caller might have read all of the rows without ever calling
            // `work` to schedule more block reads.
            self.work()?;
            if self.is_readable() {
                break;
            }

            // Nothing is readable, and we're not at the end, so there must be
            // pending reads.  Wait until one completes, and process it.
            debug_assert!(self.pending());
            self.process_read_results(self.receiver.recv().unwrap())?;
        }
        Ok(())
    }

    /// Returns whether we're waiting on any block reads.
    fn pending(&self) -> bool {
        self.data_pending > 0 || self.indexes.iter().any(|level| level.pending > 0)
    }

    /// Returns the key in the current row, or `None` if we're at EOF or this
    /// row isn't readable yet.
    ///
    /// # Safety
    ///
    /// Unsafe because `rkyv` deserialization is unsafe.
    pub unsafe fn key<'b>(&self, key: &'b mut K) -> Option<&'b mut K> {
        self.data_blocks.front().map(|block| {
            block.key_for_row(&self.factories, self.row, key);
            key
        })
    }

    /// Returns the auxiliary data in the current row, or `None` if we're at EOF
    /// or this row isn't readable yet.
    ///
    /// # Safety
    ///
    /// Unsafe because `rkyv` deserialization is unsafe.
    pub unsafe fn aux<'b>(&self, aux: &'b mut A) -> Option<&'b mut A> {
        self.data_blocks.front().map(|block| {
            block.aux_for_row(&self.factories, self.row, aux);
            aux
        })
    }

    /// Returns the key and auxiliary data in the current row, or `None` if
    /// we're at EOF or this row isn't readable yet.
    ///
    /// # Safety
    ///
    /// Unsafe because `rkyv` deserialization is unsafe.
    pub unsafe fn item<'b>(&self, item: (&'b mut K, &'b mut A)) -> Option<(&'b mut K, &'b mut A)> {
        self.data_blocks.front().map(|block| {
            block.item_for_row(&self.factories, self.row, (item.0, item.1));
            item
        })
    }

    /// Returns the "row group', that is, the range of rows in the next column
    /// that corresponds to the current row.  Returns an [Error] if there is
    /// data corruption, or `None` if this row isn't readable yet.
    ///
    /// # Panic
    ///
    /// Panics if this is the last column in the [Reader], since the last column
    /// doesn't have row groups.
    pub fn row_group(&self) -> Result<Option<Range<u64>>, Error> {
        self.data_blocks
            .front()
            .map(|block| block.row_group_for_row(self.row))
            .transpose()
    }

    /// Returns the current row number.
    pub fn row(&self) -> u64 {
        self.row
    }

    /// Returns the number of rows in this column.
    pub fn n_rows(&self) -> u64 {
        self.n_rows
    }

    /// Advances to the next row.
    ///
    /// # Panic
    ///
    /// May panic if the current row isn't readable, or if we're already past
    /// the last row.
    pub fn step(&mut self) {
        debug_assert!(self.data_blocks[0].rows().contains(&self.row));
        self.row += 1;
        if self.row >= self.data_blocks[0].rows().end {
            self.data_blocks.pop_front();
        }
    }

    /// Advances to row `target_row`.
    ///
    /// Returns true if the row is readable, false if further I/O is required.
    ///
    /// # Panic
    ///
    /// May panic if `target_row` is less than the current row or greater than
    /// the number of rows.
    pub fn step_to(&mut self, target_row: u64) -> bool {
        debug_assert!(target_row >= self.row);
        debug_assert!(target_row <= self.n_rows);
        while target_row > self.row {
            let Some(end) = self.data_blocks.front().map(|block| block.rows().end) else {
                return false;
            };
            if target_row >= end {
                self.row = end;
                self.data_blocks.pop_front();
            } else {
                self.row = target_row;
            }
        }
        true
    }
}

struct IndexLevel<K>
where
    K: DataTrait + ?Sized,
{
    /// Number of outstanding block reads pending completion for this level.
    pending: usize,

    /// Sequential blocks whose children need to be loaded.
    blocks: VecDeque<Arc<IndexBlock<K>>>,

    index: usize,

    /// First row in the next block to be added to `blocks`.  If `blocks` is
    /// nonempty, then this is `blocks.last().unwrap().first_row`.
    next: u64,

    /// Blocks that have been received out of order.  They will be moved to
    /// `blocks` when `next` catches up to their starting row.
    out_of_order: BTreeMap<u64, Arc<IndexBlock<K>>>,
}

impl<K> IndexLevel<K>
where
    K: DataTrait + ?Sized,
{
    fn new() -> Self {
        Self {
            pending: 0,
            blocks: VecDeque::new(),
            index: 0,
            next: 0,
            out_of_order: BTreeMap::new(),
        }
    }

    /// Adds `block` to the collection of blocks in this level.
    fn received(&mut self, block: Arc<IndexBlock<K>>) {
        debug_assert!(self.pending > 0);
        self.pending -= 1;

        if block.first_row == self.next {
            self.next = block.rows().end;
            self.blocks.push_back(block);
            while let Some(entry) = self.out_of_order.first_entry() {
                if *entry.key() != self.next {
                    break;
                }
                let block = entry.remove();
                self.next = block.rows().end;
                self.blocks.push_back(block);
            }
        } else if block.first_row > self.next {
            self.out_of_order.insert(block.first_row, block);
        } else {
            // File corruption or (more likely) a bug.
            todo!()
        }
    }

    /// Returns the next [TreeNode] to read in the level below this one, or
    /// `None` if we've exhausted this level or there are none to read yet.
    /// (Use [eof](Self::eof) to distinguish the meanings of `None`.)
    fn child(&self) -> Result<Option<TreeNode>, Error> {
        self.blocks
            .front()
            .map(|block| block.get_child(self.index))
            .transpose()
    }

    fn next_child(&mut self) {
        let block = self.blocks.front().unwrap();
        self.index += 1;
        if self.index >= block.n_children() {
            self.blocks.pop_front();
            self.index = 0;
        }
    }

    fn is_full(&self, level: usize) -> bool {
        self.pending + self.blocks.len() + self.out_of_order.len() >= 1 << level
    }
}
