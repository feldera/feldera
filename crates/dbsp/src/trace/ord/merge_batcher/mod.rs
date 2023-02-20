//! A general purpose `Batcher` implementation based on radix sort.

use crate::{
    algebra::MonoidValue,
    trace::{consolidation, Batch, Batcher, Builder},
    utils::VecExt,
    DBTimestamp,
};
use size_of::SizeOf;
use std::{
    cmp::Ordering,
    collections::VecDeque,
    fmt::Debug,
    marker::PhantomData,
    mem::{replace, size_of, take},
};

mod tests;

/// Creates batches from unordered tuples.
#[derive(SizeOf)]
pub struct MergeBatcher<I: Ord, T: Ord, R: MonoidValue, B: Batch<Item = I, Time = T, R = R>> {
    sorter: MergeSorter<I, R>,
    time: T,
    phantom: PhantomData<B>,
}

impl<I, T, R, B> Batcher<I, T, R, B> for MergeBatcher<I, T, R, B>
where
    Self: SizeOf,
    I: Ord + Clone,
    T: DBTimestamp,
    R: MonoidValue,
    B: Batch<Item = I, Time = T, R = R>,
{
    fn new_batcher(time: T) -> Self {
        Self {
            sorter: MergeSorter::new(),
            time,
            phantom: PhantomData,
        }
    }

    fn push_batch(&mut self, batch: &mut Vec<(I, R)>) {
        self.sorter.push(batch);
    }

    fn push_consolidated_batch(&mut self, batch: &mut Vec<(I, R)>) {
        self.sorter.push_consolidated(batch);
    }

    fn tuples(&self) -> usize {
        self.sorter.tuples()
    }

    // Sealing a batch means finding those updates with times not greater or equal
    // to any time in `upper`. All updates must have time greater or equal to
    // the previously used `upper`, which we call `lower`, by assumption that
    // after sealing a batcher we receive no more updates with times not greater
    // or equal to `upper`.
    // TODO: Since sealing takes self by value all of the buffers we've collected
    //       are just discarded, which isn't ideal
    // TODO: Should we just merge batches until completion instead of having
    //       the inner builder do it?
    fn seal(mut self) -> B {
        let mut merged = Vec::new();
        self.sorter.finish_into(&mut merged);

        // Try and pre-allocate our builder a little bit
        let mut builder = B::Builder::with_capacity(
            self.time.clone(),
            merged.iter().map(|batch| batch.len()).sum(),
        );

        for buffer in merged.drain(..) {
            builder.extend(buffer.into_iter());
        }

        builder.done()
    }
}

#[derive(Debug, SizeOf)]
struct MergeSorter<D: Ord, R: MonoidValue> {
    /// Queue's invariant is not that every `Vec<Vec<(D, R)>>` is sorted
    /// relative to each other but instead that within each `Vec<Vec<(D, R)>>`
    /// every inner `Vec<(D, R)>` is sorted by `D` relative to the rest of the
    /// vecs within its current batch
    queue: Vec<Vec<Vec<(D, R)>>>,
    stash: Vec<Vec<(D, R)>>,
}

impl<D: Ord, R: MonoidValue> MergeSorter<D, R> {
    /// The maximum number of bytes we'd like our buffers to contain
    ///
    /// Note that this isn't a hard limit or something that can be
    /// depended on for correctness, it's simply a preference
    const BUFFER_BYTES: usize = 1 << 13;

    /// The maximum number of elements we'd like our buffers to contain,
    /// as calculated by the number of elements we can fit into
    /// [`Self::BUFFER_BYTES`]
    ///
    /// Note that this isn't a hard limit or something that can be
    /// depended on for correctness, it's simply a preference
    const BUFFER_ELEMENTS: usize = {
        let size = size_of::<(D, R)>();

        // Capacity doesn't matter for ZSTs
        if size == 0 {
            usize::MAX

        // For everything that's smaller than our acceptable
        // memory usage, we figure out how many elements can
        // fit within that amount of memory
        } else if size <= Self::BUFFER_BYTES {
            Self::BUFFER_BYTES / size

        // For things larger than our acceptable memory
        // usage we just store one of them
        // FIXME: Does limiting buffers to one element
        //        prevent any merging from happening?
        } else {
            1
        }
    };

    #[inline]
    pub fn new() -> Self {
        Self {
            queue: Vec::new(),
            stash: Vec::new(),
        }
    }

    #[inline]
    fn buffer(&mut self) -> Vec<(D, R)> {
        let empty = self
            .stash
            .pop()
            .unwrap_or_else(|| Vec::with_capacity(Self::BUFFER_ELEMENTS));
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

    pub fn push(&mut self, batch: &mut Vec<(D, R)>) {
        // If the batch we're given is empty, do nothing
        if !batch.is_empty() {
            // TODO: Reason about possible unbounded stash growth. How to / should we return
            // them? TODO: Reason about mis-sized vectors, from deserialized data;
            // should probably drop.
            // If we have at least three stashed vectors (one for swapping, two for merging)
            // then we replace the batch we were given with one of our stashed ones
            // so that the caller doesn't have to reallocate when reusing the buffer
            let mut batch = if self.stash.len() > 2 {
                replace(batch, self.stash.pop().expect("there's at least 3 stashes"))

            // Otherwise if we don't have at least three stashed buffers we just
            // replace the buffer we were given with an empty one
            } else {
                take(batch)
            };

            // Consolidate and push the batch we were given
            consolidation::consolidate(&mut batch);
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

    pub fn push_consolidated(&mut self, batch: &mut Vec<(D, R)>) {
        // If the batch we're given is empty, do nothing
        if !batch.is_empty() {
            // TODO: Reason about possible unbounded stash growth. How to / should we return
            // them? TODO: Reason about mis-sized vectors, from deserialized data;
            // should probably drop.
            // If we have at least three stashed vectors (one for swapping, two for merging)
            // then we replace the batch we were given with one of our stashed ones
            // so that the caller doesn't have to reallocate when reusing the buffer
            let batch = if self.stash.len() > 2 {
                replace(batch, self.stash.pop().expect("there's at least 3 stashes"))

            // Otherwise if we don't have at least three stashed buffers we just
            // replace the buffer we were given with an empty one
            } else {
                take(batch)
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
    pub fn finish_into(&mut self, target: &mut Vec<Vec<(D, R)>>) {
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
    fn merge_by(
        &mut self,
        mut list1: Vec<Vec<(D, R)>>,
        mut list2: Vec<Vec<(D, R)>>,
    ) -> Vec<Vec<(D, R)>> {
        // Remove all empty batches from the inputs
        list1.retain(|batch| !batch.is_empty());
        list2.retain(|batch| !batch.is_empty());

        let (mut list1, mut list2) = (VecDeque::from(list1), VecDeque::from(list2));

        // Ensure all the batches we've been given are in sorted order
        if cfg!(debug_assertions) {
            for batch in list1.iter().chain(&list2) {
                assert!(!batch.is_empty());
                assert!(batch.is_sorted_by(|(a, _), (b, _)| Some(a.cmp(b))));
            }
        }

        // TODO: `list1` and `list2` get dropped; would be better to reuse?
        let mut output = Vec::with_capacity(list1.len() + list2.len());
        let mut result = self.buffer();

        let mut head1 = list1.pop_front().map_or_else(VecDeque::new, VecDeque::from);
        let mut head2 = list2.pop_front().map_or_else(VecDeque::new, VecDeque::from);

        // while we have valid data in each input, merge.
        while !head1.is_empty() && !head2.is_empty() {
            debug_assert!(result.is_sorted_by(|(a, _), (b, _)| Some(a.cmp(b))));

            // Iterate while the result vec has spare capacity (and therefore can be pushed
            // to) and both `head1` and `head2` are non-empty
            while result.has_spare_capacity() && !head1.is_empty() && !head2.is_empty() {
                // Compare the data of each element
                let cmp = {
                    let (data1, _) = head1.front().expect("there's at least one element");
                    let (data2, _) = head2.front().expect("there's at least one element");
                    data1.cmp(data2)
                };

                // Get the result of merging the two values
                let merged = match cmp {
                    Ordering::Less => head1.pop_front().expect("there's at least one element"),
                    Ordering::Greater => head2.pop_front().expect("there's at least one element"),

                    // If the two values are equal
                    Ordering::Equal => {
                        // Merge the diff values of both elements
                        let (data, mut diff1) =
                            head1.pop_front().expect("there's at least one element");
                        let (data2, diff2) =
                            head2.pop_front().expect("there's at least one element");

                        debug_assert!(data == data2);
                        diff1.add_assign_by_ref(&diff2);

                        // If merging the two values returns zero, discard the element
                        if diff1.is_zero() {
                            continue;

                        // If the diff is non-zero, keep the element
                        } else {
                            (data, diff1)
                        }
                    }
                };

                if cfg!(debug_assertions) {
                    if let Some((last, _)) = result.last() {
                        debug_assert!(last <= &merged.0);
                    }
                }

                // Safety: We've checked that the current vec has spare capacity available
                //         as part of the loop's condition
                debug_assert!(result.has_spare_capacity());
                unsafe { result.push_unchecked(merged) };
            }

            debug_assert!(result.is_sorted_by(|(a, _), (b, _)| Some(a.cmp(b))));
            if !result.has_spare_capacity() {
                output.push(result);
                result = self.buffer();
            }

            if head1.is_empty() {
                if head1.capacity() >= Self::BUFFER_ELEMENTS {
                    self.stash.push(Vec::from(head1));
                }

                head1 = list1.pop_front().map_or_else(VecDeque::new, VecDeque::from);
            }

            if head2.is_empty() {
                if head2.capacity() >= Self::BUFFER_ELEMENTS {
                    self.stash.push(Vec::from(head2));
                }

                head2 = list2.pop_front().map_or_else(VecDeque::new, VecDeque::from);
            }
        }

        if !result.is_empty() {
            debug_assert!(result.is_sorted_by(|(a, _), (b, _)| Some(a.cmp(b))));
            output.push(result);
        } else if result.capacity() >= Self::BUFFER_ELEMENTS {
            self.stash.push(result);
        }

        let head1 = Vec::from(head1);
        if !head1.is_empty() {
            // Our result buffer should be in sorted order
            debug_assert!(head1.is_sorted_by(|(a, _), (b, _)| Some(a.cmp(b))));
            output.push(head1);
        } else if head1.capacity() >= Self::BUFFER_ELEMENTS {
            self.stash.push(head1);
        }
        output.extend(list1);

        let head2 = Vec::from(head2);
        if !head2.is_empty() {
            // Our result buffer should be in sorted order
            debug_assert!(head2.is_sorted_by(|(a, _), (b, _)| Some(a.cmp(b))));
            output.push(head2);
        } else if head2.capacity() >= Self::BUFFER_ELEMENTS {
            self.stash.push(head2);
        }
        output.extend(list2);

        // None of the batches we output should be empty
        debug_assert!(output.iter().all(|batch| !batch.is_empty()));

        output
    }

    fn tuples(&self) -> usize {
        let queue_tuples: usize = self.queue.iter().flatten().map(|alloc| alloc.len()).sum();
        // TODO: Why do we count the stash lengths, all stashes should be empty
        let stash_tuples: usize = self.stash.iter().map(|stash| stash.len()).sum();
        queue_tuples + stash_tuples
    }
}
