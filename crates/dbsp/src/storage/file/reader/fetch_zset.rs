use super::super::Factories;
use crate::dynamic::{DataTrait, DynVec, WeightTrait};
use crate::storage::file::reader::{decompress, DataBlock, Error, Reader, TreeBlock, TreeNode};
use crate::storage::{
    backend::StorageError,
    buffer_cache::{BufferCache, FBuf},
};
use crate::trace::ord::vec::wset_batch::VecWSetBuilder;
use crate::trace::{BatchReaderFactories, Builder, VecWSet, VecWSetFactories};
use smallvec::SmallVec;
use std::{collections::BTreeMap, fmt::Debug, ops::Range, sync::Arc};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

/// Reads a subset of a 1-column [Reader].
///
/// Subsets a 1-column reader to just the rows whose keys are in a given array
/// and returns it as a Z-set, treating auxiliary values as weights.
pub struct FetchZSet<'a, 'b, K, A>
where
    K: DataTrait + ?Sized,
    A: WeightTrait + ?Sized,
{
    /// The reader.
    reader: &'a Reader<(&'static K, &'static A, ())>,

    /// Sorted keys specifying the subset.
    keys: &'b DynVec<K>,

    /// The buffer cache.
    cache: Arc<BufferCache>,

    /// Factories for constructing keys and aux data.
    factories: Factories<K, A>,

    /// Receives asynchronous read results.
    receiver: UnboundedReceiver<FetchZSetReadResults>,

    /// For cloning and passing to asynchronous read callback handler.
    sender: UnboundedSender<FetchZSetReadResults>,

    /// Number of asynchronous reads submitted that haven't yet had a response.
    pending: usize,

    /// Temporary storage for keys.
    tmp_key: Box<K>,

    /// Data blocks received for results.
    ///
    /// Maps from the first row in the received data block to the range in
    /// `keys` that the data block covers and the data block itself.
    output_blocks: BTreeMap<u64, (Range<usize>, Arc<DataBlock<K, A>>)>,
}

impl<'a, 'b, K, A> FetchZSet<'a, 'b, K, A>
where
    K: DataTrait + ?Sized,
    A: WeightTrait + ?Sized,
{
    pub(super) fn new(
        reader: &'a Reader<(&'static K, &'static A, ())>,
        keys: &'b DynVec<K>,
    ) -> Result<Self, Error> {
        debug_assert!(keys.is_sorted_by(&|a, b| a.cmp(b)));
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        let factories = reader.columns[0].factories.factories();
        let tmp_key = factories.key_factory.default_box();
        let mut this = Self {
            reader,
            keys,
            cache: (reader.file.cache)(),
            factories,
            sender,
            receiver,
            tmp_key,
            output_blocks: BTreeMap::new(),
            pending: 0,
        };
        if !keys.is_empty() {
            if let Some(node) = &reader.columns[0].root {
                let mut reads = Vec::new();
                this.try_read(FetchZSetRead::new(0..keys.len(), node.clone()), &mut reads)?;
                this.start_reads(reads);
            }
        }
        Ok(this)
    }

    /// Fetches the Z-set asynchronously and returns the results.
    ///
    /// This is the main method to use in production to overlap I/O.  With this
    /// function, It's not necessary to call [Self::is_done] or [Self::run] or
    /// [Self::wait].
    pub async fn async_results(
        mut self,
        factories: VecWSetFactories<K, A>,
    ) -> Result<VecWSet<K, A>, Error> {
        while !self.is_done() {
            let mut reads = Vec::new();
            let msg = self.receiver.recv().await.unwrap();
            self.process_results(msg, &mut reads)?;
            self.run_(reads)?;
        }
        Ok(self.results(factories))
    }

    /// Returns true if the results are available.
    ///
    /// If results aren't available yet, call [Self::wait] or [Self::run].
    pub fn is_done(&self) -> bool {
        self.pending == 0
    }

    /// Constructs and returns a [VecWSet] with the results.
    ///
    /// # Panic
    ///
    /// Panics if results aren't available yet.
    pub fn results(self, factories: VecWSetFactories<K, A>) -> VecWSet<K, A> {
        assert!(self.is_done());
        let mut builder = VecWSetBuilder::<K, A>::new_builder(&factories);
        let mut tmp_diff = factories.weight_factory().default_box();
        let mut index_stack = SmallVec::<[usize; 10]>::new();
        let mut key_stack = factories.layer_factories.keys.default_box();
        key_stack.reserve_exact(10);
        for (key_range, data_block) in self.output_blocks.into_values() {
            key_stack.clear();
            index_stack.clear();
            for i in key_range {
                let key = &self.keys[i];
                if unsafe {
                    data_block.find_with_cache(
                        &self.factories,
                        &mut *key_stack,
                        &mut index_stack,
                        key,
                    )
                } {
                    let child_index = index_stack.pop().unwrap();
                    unsafe { data_block.aux(&self.factories, child_index, &mut tmp_diff) };
                    builder.push_val_diff_mut(&mut (), &mut tmp_diff);

                    builder.push_key_mut(key_stack.last_mut().unwrap());
                    key_stack.truncate(index_stack.len());
                }
            }
        }
        builder.done()
    }

    /// If the results aren't available yet, waits for an asynchronous
    /// background read to complete and then processes it.
    ///
    /// This is useful for testing purposes to avoid busy-waiting, but to
    /// overlap I/O with other code it's better to use [Self::async_results].
    pub fn wait(&mut self) -> Result<(), Error> {
        if !self.is_done() {
            let mut reads = Vec::new();
            let msg = self.receiver.blocking_recv().unwrap();
            self.process_results(msg, &mut reads)?;
            self.run_(reads)?;
        }
        Ok(())
    }

    /// Processes any asynchronous reads that have completed, and launches new
    /// ones.
    pub fn run(&mut self) -> Result<(), Error> {
        self.run_(Vec::new())
    }

    fn run_(&mut self, mut reads: Vec<FetchZSetRead>) -> Result<(), Error> {
        while let Ok(results) = self.receiver.try_recv() {
            self.process_results(results, &mut reads)?;
        }
        self.start_reads(reads);
        Ok(())
    }

    fn process_results(
        &mut self,
        results: FetchZSetReadResults,
        reads: &mut Vec<FetchZSetRead>,
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

    fn start_reads(&mut self, reads: Vec<FetchZSetRead>) {
        if !reads.is_empty() {
            self.reader.file.file_handle.read_async(
                reads.iter().map(|read| read.node.location).collect(),
                {
                    let sender = self.sender.clone();
                    Box::new(move |results| {
                        let _ = sender.send(FetchZSetReadResults { reads, results });
                    })
                },
            );
            self.pending += 1;
        }
    }

    fn try_read(
        &mut self,
        read: FetchZSetRead,
        reads: &mut Vec<FetchZSetRead>,
    ) -> Result<(), Error> {
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
        reads: &mut Vec<FetchZSetRead>,
    ) -> Result<(), Error> {
        match tree_block {
            TreeBlock::Data(data_block) => {
                let _existing = self
                    .output_blocks
                    .insert(data_block.first_row, (key_range.clone(), data_block));
                debug_assert!(_existing.is_none());
            }
            TreeBlock::Index(index_block) => {
                let mut child_idx = 0;
                let mut i = key_range.start;
                while i < key_range.end {
                    if let Some((child_index, n_keys)) = unsafe {
                        index_block.find_next(
                            self.keys,
                            i..key_range.end,
                            &mut self.tmp_key,
                            &mut child_idx,
                        )
                    } {
                        let read =
                            FetchZSetRead::new(i..i + n_keys, index_block.get_child(child_index)?);
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
struct FetchZSetRead {
    keys: Range<usize>,
    node: TreeNode,
}

impl FetchZSetRead {
    fn new(keys: Range<usize>, node: TreeNode) -> Self {
        Self { keys, node }
    }
}

struct FetchZSetReadResults {
    reads: Vec<FetchZSetRead>,
    results: Vec<Result<Arc<FBuf>, StorageError>>,
}
