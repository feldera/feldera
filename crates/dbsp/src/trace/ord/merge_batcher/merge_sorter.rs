use std::{cmp::Ordering, collections::VecDeque, fmt, fmt::Debug, mem::swap};

use size_of::SizeOf;

use crate::dynamic::{pair::DynPair, DataTrait, DynWeightedPairs, Factory, WeightTrait};

#[derive(SizeOf)]
pub struct MergeSorter<D, R>
where
    D: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    /// Queue's invariant is not that every `Vec<Vec<(D, R)>>` is sorted
    /// relative to each other but instead that within each `Vec<Vec<(D, R)>>`
    /// every inner `Vec<(D, R)>` is sorted by `D` relative to the rest of the
    /// vecs within its current batch
    pub(crate) queue: Vec<Vec<Box<DynWeightedPairs<D, R>>>>,
    pub(crate) stash: Vec<Box<DynWeightedPairs<D, R>>>,
    buffer_elements: usize,
    #[size_of(skip)]
    pairs_factory: &'static dyn Factory<DynWeightedPairs<D, R>>,
}

impl<D: DataTrait + ?Sized, R: WeightTrait + ?Sized> Debug for MergeSorter<D, R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MergeSorter")
            .field("queue", &self.queue)
            .field("stash", &self.stash)
            .field("buffer_elements", &self.buffer_elements)
            .finish()
    }
}

impl<D, R> MergeSorter<D, R>
where
    D: DataTrait + ?Sized,
    R: WeightTrait + ?Sized,
{
    /// The maximum number of bytes we'd like our buffers to contain
    ///
    /// Note that this isn't a hard limit or something that can be
    /// depended on for correctness, it's simply a preference
    const BUFFER_BYTES: usize = 1 << 13;

    pub fn buffer_elements(element_size: usize) -> usize {
        // Capacity doesn't matter for ZSTs
        if element_size == 0 {
            usize::MAX

            // For everything that's smaller than our acceptable
            // memory usage, we figure out how many elements can
            // fit within that amount of memory
        } else if element_size <= Self::BUFFER_BYTES {
            Self::BUFFER_BYTES / element_size

            // For things larger than our acceptable memory
            // usage we just store one of them
            // FIXME: Does limiting buffers to one element
            //        prevent any merging from happening?
        } else {
            1
        }
    }

    #[inline]
    pub fn new(
        pair_factory: &'static dyn Factory<DynPair<D, R>>,
        pairs_factory: &'static dyn Factory<DynWeightedPairs<D, R>>,
    ) -> Self {
        let size = pair_factory.size_of();
        let buffer_elements = Self::buffer_elements(size);

        Self {
            queue: Vec::new(),
            stash: Vec::new(),
            pairs_factory,
            buffer_elements,
        }
    }

    #[inline]
    fn buffer(&mut self) -> Box<DynWeightedPairs<D, R>> {
        let empty = self.stash.pop().unwrap_or_else(|| {
            let mut vec = self.pairs_factory.default_box();
            vec.reserve(self.buffer_elements);
            vec
        });
        debug_assert!(
            empty.is_empty(),
            "popped a stashed buffer that wasn't empty",
        );
        debug_assert_ne!(
            empty.capacity(),
            0,
            "popped a stashed buffer with zero capacity",
        );

        empty
    }

    pub fn push_batch(&mut self, batch: &mut Box<DynWeightedPairs<D, R>>) {
        // If the batch we're given is empty, do nothing
        if !batch.is_empty() {
            // TODO: Reason about possible unbounded stash growth. How to / should we return
            // them? TODO: Reason about mis-sized vectors, from deserialized data;
            // should probably drop.
            // If we have at least three stashed vectors (one for swapping, two for merging)
            // then we replace the batch we were given with one of our stashed ones
            // so that the caller doesn't have to reallocate when reusing the buffer
            let mut batch = if self.stash.len() > 2 {
                let mut stashed = self.stash.pop().expect("there's at least 3 stashes");

                swap(batch, &mut stashed);
                stashed

            // Otherwise if we don't have at least three stashed buffers we just
            // replace the buffer we were given with an empty one
            } else {
                let mut empty = self.pairs_factory.default_box();
                swap(batch, &mut empty);
                empty
            };

            // Consolidate and push the batch we were given
            batch.consolidate();

            debug_assert!(batch.is_sorted_by(&|x, y| x.fst().cmp(y.fst())));

            if !batch.is_empty() {
                self.queue.push(vec![batch]);

                // While there's at least two elements in our queue and one
                // of them is much larger than the other
                while self.queue.len() > 1
                    && (self.queue[self.queue.len() - 1].len()
                        >= self.queue[self.queue.len() - 2].len() / 2)
                {
                    let (left, right) = (
                        self.queue.pop().expect("there's at least two batches"),
                        self.queue.pop().expect("there's at least two batches"),
                    );

                    // Merge the two batches together
                    let merged = self.merge_by(left, right);
                    if !merged.is_empty() {
                        self.queue.push(merged);
                    }
                }
            }
        }
    }

    pub fn push_consolidated_batch(&mut self, batch: &mut Box<DynWeightedPairs<D, R>>) {
        debug_assert!(batch.is_sorted_by(&|x, y| x.fst().cmp(y.fst())));

        // If the batch we're given is empty, do nothing
        if !batch.is_empty() {
            // TODO: Reason about possible unbounded stash growth. How to / should we return
            // them? TODO: Reason about mis-sized vectors, from deserialized data;
            // should probably drop.

            // If we have at least three stashed vectors (one for swapping, two for merging)
            // then we replace the batch we were given with one of our stashed ones
            // so that the caller doesn't have to reallocate when reusing the buffer
            let batch = if self.stash.len() > 2 {
                let mut stashed = self.stash.pop().expect("there's at least 3 stashes");

                swap(batch, &mut stashed);
                stashed

                // Otherwise if we don't have at least three stashed buffers we
                // just replace the buffer we were given with an
                // empty one
            } else {
                let mut empty = self.pairs_factory.default_box();
                swap(batch, &mut empty);
                empty
            };

            // Push the batch we were given, it's already consolidated
            if !batch.is_empty() {
                self.queue.push(vec![batch]);

                // While there's at least two elements in our queue and one
                // of them is much larger than the other
                while self.queue.len() > 1
                    && (self.queue[self.queue.len() - 1].len()
                        >= self.queue[self.queue.len() - 2].len() / 2)
                {
                    let (left, right) = (
                        self.queue.pop().expect("there's at least two batches"),
                        self.queue.pop().expect("there's at least two batches"),
                    );

                    // Merge the two batches together
                    let merged = self.merge_by(left, right);
                    if !merged.is_empty() {
                        self.queue.push(merged);
                    }
                }
            }
        }
    }

    #[inline(never)]
    pub fn finish_into(&mut self, target: &mut Vec<Box<DynWeightedPairs<D, R>>>) {
        while self.queue.len() >= 2 {
            let list1 = self.queue.pop().unwrap();
            let list2 = self.queue.pop().unwrap();

            let merged = self.merge_by(list1, list2);
            if !merged.is_empty() {
                self.queue.push(merged);
            }
        }

        if let Some(last) = self.queue.pop() {
            // TODO: Reuse the `target` buffer somehow
            *target = last;
        }
        debug_assert!(self.queue.is_empty());
    }

    // merges two sorted input lists into one sorted output list.
    // TODO: Split this into two functions:
    //       - `fn merge_batch(Vec<(D, R)>, Vec<(D, R)>) -> Vec<(D, R)>`
    //       - `fn merge_batches(Vec<Vec<(D, R)>>, Vec<Vec<(D, R)>>) -> Vec<Vec<(D,
    //         R)>>`
    // TODO: There's some things that need to be evaluated/tested/benchmarked here:
    //       - When getting a new head1/head2 after the inner merge happens, is it
    //         beneficial to pop from the opposite list (list1/list2) when one of
    //         them is exhausted?
    //       - Whenever we push a batch to `output` we lose the ability to merge
    //         anything else into it, this can somewhat restrict the amount of
    //         merging we can possibly do and can lead to missed opportunities
    pub(crate) fn merge_by(
        &mut self,
        mut list1: Vec<Box<DynWeightedPairs<D, R>>>,
        mut list2: Vec<Box<DynWeightedPairs<D, R>>>,
    ) -> Vec<Box<DynWeightedPairs<D, R>>> {
        // Remove all empty batches from the inputs
        list1.retain(|batch| !batch.is_empty());
        list2.retain(|batch| !batch.is_empty());

        let (mut list1, mut list2) = (VecDeque::from(list1), VecDeque::from(list2));

        // Ensure all the batches we've been given are in sorted order
        if cfg!(debug_assertions) {
            for batch in list1.iter().chain(&list2) {
                assert!(!batch.is_empty());
                assert!(batch.is_sorted_by(&|a, b| a.fst().cmp(b.fst())));
            }
        }

        // TODO: `list1` and `list2` get dropped; would be better to reuse?
        let mut output = Vec::with_capacity(list1.len() + list2.len());
        let mut result = self.buffer();

        let mut batch1 = list1
            .pop_front()
            .unwrap_or_else(|| self.pairs_factory.default_box());
        let mut index1 = 0;

        let mut batch2 = list2
            .pop_front()
            .unwrap_or_else(|| self.pairs_factory.default_box());
        let mut index2 = 0;

        // while we have valid data in each input, merge.
        while index1 < batch1.len() && index2 < batch2.len() {
            debug_assert!(result.is_sorted_by(&|a, b| a.fst().cmp(b.fst())));

            // Iterate while the result vec has spare capacity (and therefore can be pushed
            // to) and both `head1` and `head2` are non-empty
            while result.has_spare_capacity() && index1 < batch1.len() && index2 < batch2.len() {
                let pair1 = &mut batch1[index1];
                let pair2 = &mut batch2[index2];

                // Compare the data of each element
                let cmp = pair1.fst().cmp(pair2.fst());

                // Get the result of merging the two values
                match cmp {
                    Ordering::Less => {
                        result.push_val/*_unchecked*/(pair1);
                        index1 += 1;
                    }
                    Ordering::Greater => {
                        result.push_val/*_unchecked*/(pair2);
                        index2 += 1;
                    }

                    // If the two values are equal
                    Ordering::Equal => {
                        // Merge the diff values of both elements

                        let (data, diff1) = pair1.split_mut();
                        let (data2, diff2) = pair2.split();

                        debug_assert!(data == data2);
                        diff1.add_assign(diff2);

                        // If merging the two values returns zero, discard the element
                        if !diff1.is_zero() {
                            result.push_val/*_unchecked*/(pair1);
                        }

                        index1 += 1;
                        index2 += 1;
                    }
                };

                /*if cfg!(debug_assertions) {
                    if !result.is_empty() {
                        debug_assert!(&result.get(result.len() - 1).0 <= &merged.0);
                    }
                }*/

                // Safety: We've checked that the current vec has spare capacity
                // available         as part of the loop's
                // condition debug_assert!(result.
                // has_spare_capacity()); unsafe { result.
                // push_unchecked(merged) };
            }

            debug_assert!(result.is_sorted_by(&|a, b| a.fst().cmp(b.fst())));
            if !result.has_spare_capacity() {
                output.push(result);
                result = self.buffer();
            }

            if index1 == batch1.len() {
                let mut tmp_batch = list1
                    .pop_front()
                    .unwrap_or_else(|| self.pairs_factory.default_box());

                swap(&mut tmp_batch, &mut batch1);

                if tmp_batch.capacity() >= self.buffer_elements {
                    tmp_batch.clear();
                    self.stash.push(tmp_batch);
                }

                index1 = 0;
            }

            if index2 == batch2.len() {
                let mut tmp_batch = list2
                    .pop_front()
                    .unwrap_or_else(|| self.pairs_factory.default_box());
                swap(&mut tmp_batch, &mut batch2);

                if tmp_batch.capacity() >= self.buffer_elements {
                    tmp_batch.clear();
                    self.stash.push(tmp_batch);
                }

                index2 = 0;
            }
        }

        if !result.is_empty() {
            debug_assert!(result.is_sorted_by(&|a, b| a.fst().cmp(b.fst())));
            output.push(result);
        } else if result.capacity() >= self.buffer_elements {
            self.stash.push(result);
        }

        if index1 < batch1.len() {
            let mut batch = self.pairs_factory.default_box();
            batch.reserve(batch1.len() - index1);
            while index1 < batch1.len() {
                batch.push_val(&mut batch1[index1]);
                index1 += 1;
            }
            // Our result buffer should be in sorted order
            debug_assert!(batch.is_sorted_by(&|a, b| a.fst().cmp(b.fst())));

            output.push(batch);
        } else if batch1.capacity() >= self.buffer_elements {
            batch1.clear();
            self.stash.push(batch1);
        }
        output.extend(list1);

        if index2 < batch2.len() {
            let mut batch = self.pairs_factory.default_box();
            batch.reserve(batch2.len() - index2);
            while index2 < batch2.len() {
                batch.push_val(&mut batch2[index2]);
                index2 += 1;
            }

            // Our result buffer should be in sorted order
            debug_assert!(batch.is_sorted_by(&|a, b| a.fst().cmp(b.fst())));
            output.push(batch);
        } else if batch2.capacity() >= self.buffer_elements {
            batch2.clear();
            self.stash.push(batch2);
        }
        output.extend(list2);

        // None of the batches we output should be empty
        debug_assert!(output.iter().all(|batch| !batch.is_empty()));

        output
    }

    pub fn len(&self) -> usize {
        let queue_tuples: usize = self.queue.iter().flatten().map(|alloc| alloc.len()).sum();
        // TODO: Why do we count the stash lengths, all stashes should be empty
        let stash_tuples: usize = self.stash.iter().map(|stash| stash.len()).sum();
        queue_tuples + stash_tuples
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
