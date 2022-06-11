//! A general purpose `Batcher` implementation based on radix sort.

use crate::{
    algebra::MonoidValue,
    lattice::Lattice,
    trace::{Batch, Batcher, Builder},
    Timestamp,
};
use deepsize::DeepSizeOf;
use std::{
    marker::PhantomData,
    mem::{replace, size_of, swap, take},
    slice::from_raw_parts,
};

/// Creates batches from unordered tuples.
pub struct MergeBatcher<
    K: Ord,
    V: Ord,
    T: Ord,
    R: MonoidValue,
    B: Batch<Key = K, Val = V, Time = T, R = R>,
> {
    sorter: MergeSorter<(K, V), R>,
    time: T,
    phantom: PhantomData<B>,
}

impl<K, V, T, R, B> DeepSizeOf for MergeBatcher<K, V, T, R, B>
where
    K: DeepSizeOf + Ord,
    V: DeepSizeOf + Ord,
    T: Ord,
    R: DeepSizeOf + MonoidValue,
    B: Batch<Key = K, Val = V, Time = T, R = R>,
{
    fn deep_size_of_children(&self, _context: &mut deepsize::Context) -> usize {
        self.sorter.deep_size_of()
    }
}

impl<K, V, T, R, B> Batcher<K, V, T, R, B> for MergeBatcher<K, V, T, R, B>
where
    K: Ord + Clone,
    V: Ord + Clone,
    T: Lattice + Timestamp + Ord + Clone,
    R: MonoidValue,
    B: Batch<Key = K, Val = V, Time = T, R = R>,
{
    fn new(time: T) -> Self {
        MergeBatcher {
            sorter: MergeSorter::new(),
            time,
            phantom: PhantomData,
        }
    }

    #[inline(never)]
    fn push_batch(&mut self, batch: &mut Vec<((K, V), R)>) {
        self.sorter.push(batch);
    }

    fn tuples(&self) -> usize {
        self.sorter.tuples()
    }

    // Sealing a batch means finding those updates with times not greater or equal
    // to any time in `upper`. All updates must have time greater or equal to
    // the previously used `upper`, which we call `lower`, by assumption that
    // after sealing a batcher we receive no more updates with times not greater
    // or equal to `upper`.
    #[inline(never)]
    fn seal(mut self) -> B {
        let mut builder = B::Builder::new(self.time.clone());

        let mut merged = Vec::new();
        self.sorter.finish_into(&mut merged);

        // TODO: Re-use buffer, rather than dropping.
        for mut buffer in merged.drain(..) {
            for ((key, val), diff) in buffer.drain(..) {
                builder.push((key, val, diff));
            }
        }

        builder.done()
    }
}

pub struct VecQueue<T> {
    list: Vec<T>,
    head: usize,
    tail: usize,
}

impl<T> VecQueue<T> {
    #[inline]
    pub fn new() -> Self {
        VecQueue::from(Vec::new())
    }
    #[inline]
    pub fn pop(&mut self) -> T {
        debug_assert!(self.head < self.tail);
        self.head += 1;
        unsafe { ::std::ptr::read(self.list.as_mut_ptr().offset((self.head as isize) - 1)) }
    }
    #[inline]
    pub fn peek(&self) -> &T {
        debug_assert!(self.head < self.tail);
        unsafe { self.list.get_unchecked(self.head) }
    }
    #[inline]
    pub fn _peek_tail(&self) -> &T {
        debug_assert!(self.head < self.tail);
        unsafe { self.list.get_unchecked(self.tail - 1) }
    }
    #[inline]
    pub fn _slice(&self) -> &[T] {
        debug_assert!(self.head < self.tail);
        unsafe { from_raw_parts(self.list.get_unchecked(self.head), self.tail - self.head) }
    }
    #[inline]
    pub fn from(mut list: Vec<T>) -> Self {
        let tail = list.len();
        unsafe {
            list.set_len(0);
        }
        VecQueue {
            list,
            head: 0,
            tail,
        }
    }
    // could leak, if self.head != self.tail.
    #[inline]
    pub fn done(self) -> Vec<T> {
        debug_assert!(self.head == self.tail);
        self.list
    }
    #[inline]
    pub fn len(&self) -> usize {
        self.tail - self.head
    }
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.head == self.tail
    }
}

#[inline]
unsafe fn push_unchecked<T>(vec: &mut Vec<T>, element: T) {
    debug_assert!(vec.len() < vec.capacity());
    let len = vec.len();
    vec.spare_capacity_mut().get_unchecked_mut(0).write(element);
    vec.set_len(len + 1);
}

pub struct MergeSorter<D: Ord, R: MonoidValue> {
    queue: Vec<Vec<Vec<(D, R)>>>, // each power-of-two length list of allocations.
    stash: Vec<Vec<(D, R)>>,
}

impl<D, R> DeepSizeOf for MergeSorter<D, R>
where
    D: DeepSizeOf + Ord,
    R: DeepSizeOf + MonoidValue,
{
    fn deep_size_of_children(&self, _context: &mut deepsize::Context) -> usize {
        self.queue.deep_size_of() + self.stash.deep_size_of()
    }
}

impl<D: Ord, R: MonoidValue> MergeSorter<D, R> {
    const BUFFER_SIZE_BYTES: usize = 1 << 13;

    fn buffer_size() -> usize {
        let size = size_of::<(D, R)>();
        if size == 0 {
            Self::BUFFER_SIZE_BYTES
        } else if size <= Self::BUFFER_SIZE_BYTES {
            Self::BUFFER_SIZE_BYTES / size
        } else {
            1
        }
    }

    #[inline]
    pub fn new() -> Self {
        MergeSorter {
            queue: Vec::new(),
            stash: Vec::new(),
        }
    }

    #[inline]
    pub fn empty(&mut self) -> Vec<(D, R)> {
        self.stash
            .pop()
            .unwrap_or_else(|| Vec::with_capacity(Self::buffer_size()))
    }

    #[inline(never)]
    pub fn _sort(&mut self, list: &mut Vec<Vec<(D, R)>>) {
        for mut batch in list.drain(..) {
            self.push(&mut batch);
        }
        self.finish_into(list);
    }

    #[inline]
    pub fn push(&mut self, batch: &mut Vec<(D, R)>) {
        // TODO: Reason about possible unbounded stash growth. How to / should we return
        // them? TODO: Reason about mis-sized vectors, from deserialized data;
        // should probably drop.
        let mut batch = if self.stash.len() > 2 {
            replace(batch, self.stash.pop().unwrap())
        } else {
            take(batch)
        };

        if !batch.is_empty() {
            crate::trace::consolidation::consolidate(&mut batch);
            self.queue.push(vec![batch]);

            while self.queue.len() > 1
                && (self.queue[self.queue.len() - 1].len()
                    >= self.queue[self.queue.len() - 2].len() / 2)
            {
                let list1 = self.queue.pop().unwrap();
                let list2 = self.queue.pop().unwrap();
                let merged = self.merge_by(list1, list2);
                self.queue.push(merged);
            }
        }
    }

    #[inline(never)]
    pub fn finish_into(&mut self, target: &mut Vec<Vec<(D, R)>>) {
        while self.queue.len() > 1 {
            let list1 = self.queue.pop().unwrap();
            let list2 = self.queue.pop().unwrap();
            let merged = self.merge_by(list1, list2);
            self.queue.push(merged);
        }

        if let Some(mut last) = self.queue.pop() {
            swap(&mut last, target);
        }
    }

    // merges two sorted input lists into one sorted output list.
    #[inline(never)]
    fn merge_by(&mut self, list1: Vec<Vec<(D, R)>>, list2: Vec<Vec<(D, R)>>) -> Vec<Vec<(D, R)>> {
        use std::cmp::Ordering;

        // TODO: `list1` and `list2` get dropped; would be better to reuse?
        let mut output = Vec::with_capacity(list1.len() + list2.len());
        let mut result = self.empty();

        let mut list1 = VecQueue::from(list1);
        let mut list2 = VecQueue::from(list2);

        let mut head1 = if !list1.is_empty() {
            VecQueue::from(list1.pop())
        } else {
            VecQueue::new()
        };
        let mut head2 = if !list2.is_empty() {
            VecQueue::from(list2.pop())
        } else {
            VecQueue::new()
        };

        // while we have valid data in each input, merge.
        while !head1.is_empty() && !head2.is_empty() {
            while (result.capacity() - result.len()) > 0 && !head1.is_empty() && !head2.is_empty() {
                let cmp = {
                    let x = head1.peek();
                    let y = head2.peek();
                    (&x.0).cmp(&y.0)
                };
                match cmp {
                    Ordering::Less => unsafe {
                        push_unchecked(&mut result, head1.pop());
                    },
                    Ordering::Greater => unsafe {
                        push_unchecked(&mut result, head2.pop());
                    },
                    Ordering::Equal => {
                        let (data1, mut diff1) = head1.pop();
                        let (_data2, diff2) = head2.pop();
                        diff1.add_assign_by_ref(&diff2);
                        if !diff1.is_zero() {
                            unsafe {
                                push_unchecked(&mut result, (data1, diff1));
                            }
                        }
                    }
                }
            }

            if result.capacity() == result.len() {
                output.push(result);
                result = self.empty();
            }

            if head1.is_empty() {
                let done1 = head1.done();
                if done1.capacity() == Self::buffer_size() {
                    self.stash.push(done1);
                }
                head1 = if !list1.is_empty() {
                    VecQueue::from(list1.pop())
                } else {
                    VecQueue::new()
                };
            }
            if head2.is_empty() {
                let done2 = head2.done();
                if done2.capacity() == Self::buffer_size() {
                    self.stash.push(done2);
                }
                head2 = if !list2.is_empty() {
                    VecQueue::from(list2.pop())
                } else {
                    VecQueue::new()
                };
            }
        }

        if !result.is_empty() {
            output.push(result);
        } else if result.capacity() > 0 {
            self.stash.push(result);
        }

        if !head1.is_empty() {
            let mut result = self.empty();
            for _ in 0..head1.len() {
                result.push(head1.pop());
            }
            output.push(result);
        }
        while !list1.is_empty() {
            output.push(list1.pop());
        }

        if !head2.is_empty() {
            let mut result = self.empty();
            for _ in 0..head2.len() {
                result.push(head2.pop());
            }
            output.push(result);
        }
        while !list2.is_empty() {
            output.push(list2.pop());
        }

        output
    }

    fn tuples(&self) -> usize {
        let mut result = 0;

        for alloc in self.queue.iter() {
            for v in alloc.iter() {
                result += v.len();
            }
        }
        for v in self.stash.iter() {
            result += v.len();
        }

        result
    }
}

/// Reports the number of elements satisfing the predicate.
///
/// This methods *relies strongly* on the assumption that the predicate
/// stays false once it becomes false, a joint property of the predicate
/// and the slice. This allows `advance` to use exponential search to
/// count the number of elements in time logarithmic in the result.
#[inline]
pub fn _advance<T, F: Fn(&T) -> bool>(slice: &[T], function: F) -> usize {
    // start with no advance
    let mut index = 0;
    if index < slice.len() && function(&slice[index]) {
        // advance in exponentially growing steps.
        let mut step = 1;
        while index + step < slice.len() && function(&slice[index + step]) {
            index += step;
            step <<= 1;
        }

        // advance in exponentially shrinking steps.
        step >>= 1;
        while step > 0 {
            if index + step < slice.len() && function(&slice[index + step]) {
                index += step;
            }
            step >>= 1;
        }

        index += 1;
    }

    index
}
