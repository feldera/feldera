use std::{collections::BTreeMap, marker::PhantomData, ops::Range, sync::Arc};

use feldera_storage::{error::StorageError, fbuf::FBuf};
use itertools::Itertools;
use smallvec::SmallVec;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::{
    dynamic::{DataTrait, DynVec, WeightTrait},
    storage::{
        buffer_cache::BufferCache,
        file::{
            reader::{
                decompress, ColumnSpec, DataBlock, Error, FilteredKeys, Reader, TreeBlock, TreeNode,
            },
            Factories,
        },
    },
    trace::{VecIndexedWSet, VecIndexedWSetFactories},
};

/// Reads a subset of a 2-column [Reader].
///
/// Builds an indexed Z-set containing just the rows from a 2-column reader with
/// a specified set of first-column keys.
pub struct FetchIndexedZSet<'a, 'b, K0, A0, K1, A1>(FetchIndexedZSetInner<'a, 'b, K0, A0, K1, A1>)
where
    K0: DataTrait + ?Sized,
    A0: DataTrait + ?Sized,
    K1: DataTrait + ?Sized,
    A1: WeightTrait + ?Sized;

enum FetchIndexedZSetInner<'a, 'b, K0, A0, K1, A1>
where
    K0: DataTrait + ?Sized,
    A0: DataTrait + ?Sized,
    K1: DataTrait + ?Sized,
    A1: WeightTrait + ?Sized,
{
    Column0(Option<Fetch0<'a, 'b, K0, A0, (&'static K1, &'static A1, ())>>),
    Column1(Fetch1<'a, K0, A0, K1, A1>),
}

impl<'a, 'b, K0, A0, K1, A1> FetchIndexedZSet<'a, 'b, K0, A0, K1, A1>
where
    K0: DataTrait + ?Sized,
    A0: DataTrait + ?Sized,
    K1: DataTrait + ?Sized,
    A1: WeightTrait + ?Sized,
{
    pub(super) fn new(
        reader: &'a Reader<(&'static K0, &'static A0, (&'static K1, &'static A1, ()))>,
        keys: &'b DynVec<K0>,
    ) -> Result<Self, Error> {
        Ok(Self(FetchIndexedZSetInner::Column0(Some(Fetch0::new(
            reader, keys,
        )?))))
    }

    /// Fetches the indexed Z-set asynchronously and returns the results.
    ///
    /// This is the main method to use in production to overlap I/O.  With this
    /// function, It's not necessary to call [Self::is_done] or [Self::run] or
    /// [Self::wait].
    pub async fn async_results(
        self,
        factories: VecIndexedWSetFactories<K0, K1, A1>,
    ) -> Result<VecIndexedWSet<K0, K1, A1>, Error> {
        let multifetch1 = match self.0 {
            FetchIndexedZSetInner::Column0(option0) => {
                let mut inner0 = option0.unwrap();
                inner0.async_run().await?;
                inner0.next_column()?
            }
            FetchIndexedZSetInner::Column1(multifetch1) => multifetch1,
        };
        multifetch1.async_results(factories).await
    }

    /// If the results aren't available yet, waits for an asynchronous
    /// background read to complete and then processes it.
    ///
    /// This is useful for testing purposes to avoid busy-waiting, but to
    /// overlap I/O with other code it's better to use [Self::async_results].
    pub fn wait(&mut self) -> Result<(), Error> {
        match &mut self.0 {
            FetchIndexedZSetInner::Column0(option0) => {
                let inner0 = option0.as_mut().unwrap();
                inner0.wait()?;
                if inner0.is_done() {
                    let inner0 = option0.take().unwrap();
                    *self = Self(FetchIndexedZSetInner::Column1(inner0.next_column()?));
                }
                Ok(())
            }
            FetchIndexedZSetInner::Column1(inner1) => inner1.wait(),
        }
    }

    /// Processes any asynchronous reads that have completed, and launches new
    /// ones.
    pub fn run(&mut self) -> Result<(), Error> {
        match &mut self.0 {
            FetchIndexedZSetInner::Column0(option0) => {
                let inner0 = option0.as_mut().unwrap();
                inner0.run()?;
                if inner0.is_done() {
                    let inner0 = option0.take().unwrap();
                    *self = Self(FetchIndexedZSetInner::Column1(inner0.next_column()?));
                }
                Ok(())
            }
            FetchIndexedZSetInner::Column1(inner1) => inner1.run(),
        }
    }

    /// Returns true if the results are available.
    ///
    /// If results aren't available yet, call [Self::wait] or [Self::run].
    pub fn is_done(&self) -> bool {
        match &self.0 {
            FetchIndexedZSetInner::Column0(option0) => option0.as_ref().unwrap().is_done(),
            FetchIndexedZSetInner::Column1(multifetch1) => multifetch1.is_done(),
        }
    }

    /// Constructs and returns a [VecIndexedWSet] with the results.
    ///
    /// # Panic
    ///
    /// Panics if results aren't available yet.
    pub fn results(
        self,
        factories: VecIndexedWSetFactories<K0, K1, A1>,
    ) -> VecIndexedWSet<K0, K1, A1> {
        match self.0 {
            FetchIndexedZSetInner::Column0(_) => {
                panic!("can't get results because FetchIndexedZSet is not done yet")
            }
            FetchIndexedZSetInner::Column1(multifetch1) => multifetch1.results(factories),
        }
    }
}

struct Fetch0<'a, 'b, K, A, N>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
{
    reader: &'a Reader<(&'static K, &'static A, N)>,
    keys: FilteredKeys<'b, K>,
    cache: Arc<BufferCache>,
    factories: Factories<K, A>,

    receiver: UnboundedReceiver<Fetch0ReadResults>,
    sender: UnboundedSender<Fetch0ReadResults>,

    tmp_key: Box<K>,
    key_stack: Box<DynVec<K>>,
    output: Box<DynVec<K>>,
    row_groups: Vec<Range<u64>>,

    pending: usize,
    _phantom: PhantomData<fn(&N)>,
}

impl<'a, 'b, K, A, N> Fetch0<'a, 'b, K, A, N>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
    N: ColumnSpec,
{
    fn new(
        reader: &'a Reader<(&'static K, &'static A, N)>,
        keys: &'b DynVec<K>,
    ) -> Result<Self, Error> {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let factories = reader.columns[0].factories.factories();
        let output = factories.keys_factory.default_box();
        let tmp_key = factories.key_factory.default_box();

        // Allocate a stack for `DataBlock::find_with_cache`.  10 levels should
        // be deeper than any normal file.
        let mut key_stack = factories.keys_factory.default_box();
        key_stack.reserve_exact(10);

        let mut this = Self {
            keys: FilteredKeys::new(reader, keys),
            reader,
            cache: (reader.file.cache)(),
            factories,
            sender,
            receiver,
            tmp_key,
            key_stack,
            output,
            row_groups: Vec::new(),
            pending: 0,
            _phantom: PhantomData,
        };
        if !this.keys.is_empty() {
            if let Some(node) = &reader.columns[0].root {
                let mut reads = Vec::new();
                this.try_read(
                    Fetch0Read::new(0..this.keys.len(), node.clone()),
                    &mut reads,
                )?;
                this.start_reads(reads);
            }
        }
        Ok(this)
    }

    fn is_done(&self) -> bool {
        self.pending == 0
    }

    fn finish(&mut self) {
        debug_assert!(self.is_done());
        self.output.sort_unstable();
        self.row_groups.sort_unstable_by_key(|rows| rows.start);
    }

    async fn async_run(&mut self) -> Result<(), Error> {
        while !self.is_done() {
            let mut reads = Vec::new();
            let msg = self.receiver.recv().await.unwrap();
            self.process_results(msg, &mut reads)?;
            self.run_(reads)?;
        }
        Ok(())
    }

    fn wait(&mut self) -> Result<(), Error> {
        if !self.is_done() {
            let mut reads = Vec::new();
            let msg = self.receiver.blocking_recv().unwrap();
            self.process_results(msg, &mut reads)?;
            self.run_(reads)?;
        }
        Ok(())
    }

    fn run(&mut self) -> Result<(), Error> {
        self.run_(Vec::new())
    }

    fn run_(&mut self, mut reads: Vec<Fetch0Read>) -> Result<(), Error> {
        while let Ok(results) = self.receiver.try_recv() {
            self.process_results(results, &mut reads)?;
        }
        self.start_reads(reads);
        Ok(())
    }

    fn process_results(
        &mut self,
        results: Fetch0ReadResults,
        reads: &mut Vec<Fetch0Read>,
    ) -> Result<(), Error> {
        self.pending -= 1;
        for (read, result) in results.reads.into_iter().zip(results.results.into_iter()) {
            let raw = result?;
            let tree_block = TreeBlock::from_raw_with_cache(
                decompress(self.reader.file.compression, read.node.location, raw)?,
                &read.node,
                &self.cache,
                self.reader.file_handle().file_id(),
            )
            .unwrap();
            self.process_read(&read.keys, tree_block, reads)?;
        }
        Ok(())
    }

    fn start_reads(&mut self, reads: Vec<Fetch0Read>) {
        if !reads.is_empty() {
            self.reader.file.file_handle.read_async(
                reads.iter().map(|read| read.node.location).collect(),
                {
                    let sender = self.sender.clone();
                    Box::new(move |results| {
                        let _ = sender.send(Fetch0ReadResults { reads, results });
                    })
                },
            );
            self.pending += 1;
        }
    }

    fn try_read(&mut self, read: Fetch0Read, reads: &mut Vec<Fetch0Read>) -> Result<(), Error> {
        if let Some(tree_block) =
            TreeBlock::from_cache(&read.node, &self.cache, &*self.reader.file.file_handle).unwrap()
        {
            self.process_read(&read.keys, tree_block, reads)?;
        } else {
            reads.push(read);
        }
        Ok(())
    }

    fn process_read(
        &mut self,
        key_range: &Range<usize>,
        tree_block: TreeBlock<K, A>,
        reads: &mut Vec<Fetch0Read>,
    ) -> Result<(), Error> {
        match tree_block {
            TreeBlock::Data(data_block) => {
                let mut index_stack = SmallVec::<[usize; 10]>::new();
                self.key_stack.clear();
                for i in key_range.clone() {
                    let key = &self.keys[i];
                    if unsafe {
                        data_block.find_with_cache(
                            &self.factories,
                            &mut *self.key_stack,
                            &mut index_stack,
                            key,
                        )
                    } {
                        let child_index = index_stack.pop().unwrap();
                        self.output.push_val(self.key_stack.last_mut().unwrap());
                        self.key_stack.truncate(index_stack.len());
                        if data_block.row_groups.is_some() {
                            self.row_groups.push(data_block.row_group(child_index)?);
                        }
                    }
                }
            }
            TreeBlock::Index(index_block) => {
                let mut child_idx = 0;
                let mut i = key_range.start;
                while i < key_range.end {
                    if let Some((child_index, n_keys)) = unsafe {
                        index_block.find_next(
                            &self.keys,
                            i..key_range.end,
                            &mut self.tmp_key,
                            &mut child_idx,
                        )
                    } {
                        let read =
                            Fetch0Read::new(i..i + n_keys, index_block.get_child(child_index)?);
                        self.try_read(read, reads)?;
                        i += n_keys;
                    } else {
                        i += 1;
                    }
                    if child_idx >= index_block.n_children() {
                        break;
                    }
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
struct Fetch0Read {
    keys: Range<usize>,
    node: TreeNode,
}

impl Fetch0Read {
    fn new(keys: Range<usize>, node: TreeNode) -> Self {
        Self { keys, node }
    }
}

struct Fetch0ReadResults {
    reads: Vec<Fetch0Read>,
    results: Vec<Result<Arc<FBuf>, StorageError>>,
}

#[derive(Debug)]
struct Fetch1Read {
    keys: Rows,
    node: TreeNode,
}

impl Fetch1Read {
    fn new(keys: Rows, node: TreeNode) -> Self {
        Self { keys, node }
    }
}

struct Fetch1ReadResults {
    reads: Vec<Fetch1Read>,
    results: Vec<Result<Arc<FBuf>, StorageError>>,
}

impl<'a, 'b, K, A, NK, NA> Fetch0<'a, 'b, K, A, (&'static NK, &'static NA, ())>
where
    K: DataTrait + ?Sized,
    A: DataTrait + ?Sized,
    NK: DataTrait + ?Sized,
    NA: WeightTrait + ?Sized,
{
    fn next_column(self) -> Result<Fetch1<'a, K, A, NK, NA>, Error> {
        Fetch1::new(self)
    }
}

struct Fetch1<'a, K0, A0, K1, A1>
where
    K0: DataTrait + ?Sized,
    A0: DataTrait + ?Sized,
    K1: DataTrait + ?Sized,
    A1: WeightTrait + ?Sized,
{
    reader: &'a Reader<(&'static K0, &'static A0, (&'static K1, &'static A1, ()))>,
    cache: Arc<BufferCache>,
    factories: Factories<K1, A1>,

    receiver: UnboundedReceiver<Fetch1ReadResults>,
    sender: UnboundedSender<Fetch1ReadResults>,

    keys: Box<DynVec<K0>>,
    offs: Vec<usize>,
    vals: Box<DynVec<K1>>,
    diffs: Box<DynVec<A1>>,

    rows: Vec<Range<u64>>,

    /// Next row to append to `vals` and `diffs`.
    next_row: u64,

    out_of_order: BTreeMap<u64, (Rows, Arc<DataBlock<K1, A1>>)>,

    pending: usize,
}

impl<'a, K0, A0, K1, A1> Fetch1<'a, K0, A0, K1, A1>
where
    K0: DataTrait + ?Sized,
    A0: DataTrait + ?Sized,
    K1: DataTrait + ?Sized,
    A1: WeightTrait + ?Sized,
{
    fn new<'b>(
        mut source: Fetch0<'a, 'b, K0, A0, (&'static K1, &'static A1, ())>,
    ) -> Result<Self, Error> {
        source.finish();

        let factories = source.reader.columns[1].factories.factories();

        let offs = {
            let mut offs = Vec::with_capacity(source.row_groups.len() + 1);
            offs.push(0);
            for row_group in &source.row_groups {
                offs.push(offs.last().unwrap() + (row_group.end - row_group.start) as usize);
            }
            offs
        };

        // Combine contiguous row groups.
        //
        // This could be done in-place with a little extra work.
        let rows = source
            .row_groups
            .into_iter()
            .coalesce(|x, y| {
                if x.end == y.start {
                    Ok(x.start..y.end)
                } else {
                    Err((x, y))
                }
            })
            .collect::<Vec<_>>();

        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let mut this = Self {
            reader: source.reader,
            cache: source.cache,
            keys: source.output,
            offs,
            vals: factories.keys_factory.default_box(),
            diffs: factories.auxes_factory.default_box(),
            next_row: rows.first().map_or(0, |range| range.start),
            rows,
            factories,
            receiver,
            sender,
            out_of_order: BTreeMap::new(),
            pending: 0,
        };
        if !this.rows.is_empty() {
            if let Some(node) = &source.reader.columns[1].root {
                let mut reads = Vec::new();
                this.try_read(
                    Fetch1Read::new(Rows::new(&this.rows), node.clone()),
                    &mut reads,
                )?;
                this.start_reads(reads);
            }
        }
        Ok(this)
    }

    fn start_reads(&mut self, reads: Vec<Fetch1Read>) {
        if !reads.is_empty() {
            self.reader.file.file_handle.read_async(
                reads.iter().map(|read| read.node.location).collect(),
                {
                    let sender = self.sender.clone();
                    Box::new(move |results| {
                        let _ = sender.send(Fetch1ReadResults { reads, results });
                    })
                },
            );
            self.pending += 1;
        }
    }
    fn try_read(&mut self, read: Fetch1Read, reads: &mut Vec<Fetch1Read>) -> Result<(), Error> {
        if let Some(tree_block) =
            TreeBlock::from_cache(&read.node, &self.cache, &*self.reader.file.file_handle).unwrap()
        {
            self.process_read(read.keys, tree_block, reads)?;
        } else {
            reads.push(read);
        }
        Ok(())
    }

    fn is_done(&self) -> bool {
        self.pending == 0
    }

    fn results(self, factories: VecIndexedWSetFactories<K0, K1, A1>) -> VecIndexedWSet<K0, K1, A1> {
        assert!(self.is_done());
        VecIndexedWSet::from_parts(factories, self.keys, self.offs, self.vals, self.diffs)
    }

    async fn async_results(
        mut self,
        factories: VecIndexedWSetFactories<K0, K1, A1>,
    ) -> Result<VecIndexedWSet<K0, K1, A1>, Error> {
        while !self.is_done() {
            let mut reads = Vec::new();
            let msg = self.receiver.recv().await.unwrap();
            self.process_results(msg, &mut reads)?;
            self.run_(reads)?;
        }
        Ok(self.results(factories))
    }

    fn wait(&mut self) -> Result<(), Error> {
        if !self.is_done() {
            let mut reads = Vec::new();
            let msg = self.receiver.blocking_recv().unwrap();
            self.process_results(msg, &mut reads)?;
            self.run_(reads)?;
        }
        Ok(())
    }

    fn run(&mut self) -> Result<(), Error> {
        self.run_(Vec::new())
    }

    fn run_(&mut self, mut reads: Vec<Fetch1Read>) -> Result<(), Error> {
        while let Ok(results) = self.receiver.try_recv() {
            self.process_results(results, &mut reads)?;
        }
        self.start_reads(reads);
        Ok(())
    }

    fn process_results(
        &mut self,
        results: Fetch1ReadResults,
        reads: &mut Vec<Fetch1Read>,
    ) -> Result<(), Error> {
        self.pending -= 1;
        for (read, result) in results.reads.into_iter().zip(results.results.into_iter()) {
            let raw = result?;
            let tree_block = TreeBlock::from_raw_with_cache(
                decompress(self.reader.file.compression, read.node.location, raw)?,
                &read.node,
                &self.cache,
                self.reader.file_handle().file_id(),
            )
            .unwrap();
            self.process_read(read.keys, tree_block, reads)?;
        }
        Ok(())
    }

    fn process_data_block(&mut self, rows: Rows, data_block: Arc<DataBlock<K1, A1>>) {
        for row in rows.iter(&self.rows) {
            self.vals
                .push_with(&mut |val| unsafe { data_block.key_for_row(&self.factories, row, val) });
            self.diffs.push_with(&mut |diff| unsafe {
                data_block.aux_for_row(&self.factories, row, diff)
            });
        }
        self.next_row = Rows::next(&self.rows, data_block.rows().end);
    }

    fn process_read(
        &mut self,
        mut rows: Rows,
        tree_block: TreeBlock<K1, A1>,
        reads: &mut Vec<Fetch1Read>,
    ) -> Result<(), Error> {
        match tree_block {
            TreeBlock::Data(data_block) => {
                let first_row = rows.first(&self.rows).unwrap();
                if first_row == self.next_row {
                    self.process_data_block(rows, data_block);
                    while let Some(first_entry) = self.out_of_order.first_entry() {
                        if &self.next_row != first_entry.key() {
                            break;
                        }
                        let (rows, data_block) = first_entry.remove();
                        self.process_data_block(rows, data_block);
                    }
                } else {
                    self.out_of_order.insert(first_row, (rows, data_block));
                }
            }
            TreeBlock::Index(index_block) => {
                while let Some(first_row) = rows.first(&self.rows) {
                    let child_idx = index_block.find_row(first_row)?;
                    let child_rows;
                    (child_rows, rows) =
                        rows.split(&self.rows, index_block.get_rows(child_idx).end);
                    let read = Fetch1Read::new(child_rows, index_block.get_child(child_idx)?);
                    self.try_read(read, reads)?;
                }
            }
        }
        Ok(())
    }
}

/// A subrange of `[Range<u64>]`.
///
/// Given a `[Range<u64>]`, this data structure represents some contiguous
/// subrange of it.  For example, given `[0..10, 20..30, 40..50]`:
///
/// * The full subrange `[0..10, 20..30, 40..50]` would be represented as `Rows
///   { before: None, middle: 0..3, after: None }`.
///
/// * The subrange `[5..10, 20..30, 40..50]` would be represented as `Rows {
///   before: Some(5..10), middle: 1..3, after: None }`.
///
/// * The subrange `[0..10, 20..30, 40..45]` would be represented as `Rows {
///   before: None, middle: 0..2, after: Some(40..45) }`.
///
/// * The subrange `[5..10, 20..30, 40..45]` would be represented as `Rows {
///   before: Some(5..10), middle: 1..2, after: Some(40..45) }`.
///
/// and so on.
#[derive(Clone, Debug, Default)]
struct Rows {
    before: Option<Range<u64>>,
    middle: Range<usize>,
    after: Option<Range<u64>>,
}

impl Rows {
    fn empty() -> Self {
        Self::default()
    }

    fn check_invariants(&self, _rows: &[Range<u64>]) {
        #[cfg(debug_assertions)]
        {
            if let Some(before) = &self.before {
                assert!(!before.is_empty());
                if !self.middle.is_empty() {
                    assert!(before.end < _rows[self.middle.start].start);
                }
                if let Some(after) = &self.after {
                    assert!(before.end < after.start);
                }
            }
            if let Some(after) = &self.after {
                assert!(!after.is_empty());
                if !self.middle.is_empty() {
                    assert!(_rows[self.middle.end - 1].end < after.start);
                }
            }
        }
    }

    /// Creates a new `Rows` that will be based on `rows`.
    ///
    /// The ranges in `rows` must be in strictly monotonically increasing order.
    /// They must not be adjacent, either: that is, `[1..2, 2..3]` is not
    /// allowed, instead the otherwise equivalent `[1..3]` must be used.
    fn new(rows: &[Range<u64>]) -> Self {
        // Check invariants on `rows` itself.  We only bother with this at
        // construction since the same `rows` has to be passed to every later
        // method.
        #[cfg(debug_assertions)]
        {
            for range in rows {
                assert!(!range.is_empty());
            }
            for i in 1..rows.len() {
                assert!(rows[i - 1].end < rows[i].start);
            }
        }

        let this = Self {
            before: None,
            middle: 0..rows.len(),
            after: None,
        };
        this.check_invariants(rows);
        this
    }

    #[allow(dead_code)]
    fn is_empty(&self) -> bool {
        self.before.is_none() && self.middle.is_empty() && self.after.is_none()
    }

    /// Equivalent to `self.iter(rows).next()`.
    fn first(&self, rows: &[Range<u64>]) -> Option<u64> {
        if let Some(before) = &self.before {
            Some(before.start)
        } else if !self.middle.is_empty() {
            Some(rows[self.middle.start].start)
        } else {
            self.after.as_ref().map(|after| after.start)
        }
    }

    /// Returns the smallest row within the ranges in `rows` that is greater
    /// than or equal to `row`, or `row` if `row` is greater than all of the
    /// rows in `rows`.
    fn next(rows: &[Range<u64>], row: u64) -> u64 {
        match rows.binary_search_by_key(&row, |range| range.start) {
            Ok(_) => row,
            Err(0) if rows.is_empty() => row,
            Err(0) => rows[0].start,
            Err(index) if row < rows[index - 1].end => row,
            Err(index) if index < rows.len() => rows[index].start,
            _ => row,
        }
    }

    /// Iterates through all of the represented rows.
    fn iter<'a>(&self, rows: &'a [Range<u64>]) -> RowsIter<'a> {
        RowsIter::new(self, rows)
    }

    /// Splits this set of rows into two at `row`.  Returns the rows before
    /// `row` and the rest as new `Rows`.
    fn split(self, rows: &[Range<u64>], row: u64) -> (Rows, Rows) {
        fn split_range(range: &Range<u64>, row: u64) -> (Option<Range<u64>>, Option<Range<u64>>) {
            if row == range.start {
                (None, Some(range.clone()))
            } else if row == range.end {
                (Some(range.clone()), None)
            } else {
                debug_assert!(range.contains(&row));
                (Some(range.start..row), Some(row..range.end))
            }
        }

        if let Some(before) = &self.before {
            if row < before.start {
                return (Self::empty(), self);
            } else if row <= before.end {
                let split = split_range(before, row);
                return (
                    Self {
                        before: split.0,
                        ..Self::empty()
                    },
                    Self {
                        before: split.1,
                        ..self
                    },
                );
            }
        }

        if let Some(after) = &self.after {
            if row >= after.end {
                return (self, Self::empty());
            } else if row >= after.start {
                let split = split_range(after, row);
                return (
                    Self {
                        after: split.0,
                        ..self
                    },
                    Self {
                        after: split.1,
                        ..Self::empty()
                    },
                );
            }
        }

        match rows.binary_search_by_key(&row, |range| range.start) {
            Ok(index) => (
                Self {
                    before: self.before,
                    middle: self.middle.start..index,
                    after: None,
                },
                Self {
                    before: None,
                    middle: index..self.middle.end,
                    after: self.after,
                },
            ),
            Err(0) => (
                Self {
                    before: self.before,
                    middle: 0..0,
                    after: None,
                },
                Self {
                    before: None,
                    ..self
                },
            ),
            Err(index) if row >= rows[index - 1].end => (
                Self {
                    before: self.before,
                    middle: self.middle.start..index,
                    after: None,
                },
                Self {
                    before: None,
                    middle: index..self.middle.end,
                    after: self.after,
                },
            ),
            Err(index) => (
                Self {
                    before: self.before,
                    middle: self.middle.start..index - 1,
                    after: Some(rows[index - 1].start..row),
                },
                Self {
                    before: Some(row..rows[index - 1].end),
                    middle: index..self.middle.end,
                    after: self.after,
                },
            ),
        }
    }
}

struct RowsIter<'a> {
    rows: Rows,
    range: Range<u64>,
    ranges: &'a [Range<u64>],
}

impl<'a> Iterator for RowsIter<'a> {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        if self.range.is_empty() {
            if let Some(before) = self.rows.before.take() {
                self.range = before;
            } else if !self.rows.middle.is_empty() {
                self.range = self.ranges[self.rows.middle.start].clone();
                self.rows.middle.start += 1;
            } else if let Some(after) = self.rows.after.take() {
                self.range = after;
            } else {
                return None;
            }
        }

        debug_assert!(!self.range.is_empty());
        let row = self.range.start;
        self.range.start += 1;
        Some(row)
    }
}

impl<'a> RowsIter<'a> {
    fn new(rows: &Rows, ranges: &'a [Range<u64>]) -> Self {
        Self {
            rows: rows.clone(),
            range: 0..0,
            ranges,
        }
    }
}

#[cfg(test)]
mod test {
    use std::ops::Range;

    use itertools::Itertools;

    use super::Rows;

    fn check_rows(rows: &Rows, ranges: &[Range<u64>], expected: u32) {
        rows.check_invariants(ranges);
        let mut actual = 0;
        for row in rows.iter(ranges) {
            assert!((0..32).contains(&row));
            actual |= 1 << row;
        }
        assert_eq!(actual, expected);
    }

    #[test]
    fn rows() {
        for pattern in 0..4096u32 {
            #[allow(clippy::filter_map_bool_then)]
            let ranges = (0..12)
                .map(|index| (pattern & (1 << index)) != 0)
                .enumerate()
                .dedup_by_with_count(|a, b| a.1 == b.1)
                .filter_map(|(count, (offset, value))| {
                    value.then(|| offset as u64..offset as u64 + count as u64)
                })
                .collect::<Vec<_>>();

            let rows = Rows::new(&ranges);
            check_rows(&rows, &ranges, pattern);
            assert_eq!(
                rows.first(&ranges),
                (pattern != 0).then(|| pattern.trailing_zeros() as u64)
            );

            check_rows(&rows, &ranges, pattern);

            for i in 0..=12 {
                let (a, b) = rows.clone().split(&ranges, i);
                check_rows(&a, &ranges, ((1 << i) - 1) & pattern);
                check_rows(&b, &ranges, !((1 << i) - 1) & pattern);
            }

            for i in 0..=12 {
                let remaining = !((1 << i) - 1) & pattern;
                let next = if remaining != 0 {
                    remaining.trailing_zeros()
                } else {
                    i
                };
                assert_eq!(Rows::next(&ranges, i as u64), next as u64);
            }
        }
    }
}
