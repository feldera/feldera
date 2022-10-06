use crate::{
    trace::{
        layers::{advance, column_leaf::OrderedColumnLeaf},
        Consumer, ValueConsumer,
    },
    utils::cursor_position_oob,
};
use size_of::SizeOf;
use std::mem::{size_of, MaybeUninit};

// TODO: Fuzz for correctness and equivalence to Cursor
// TODO: Fuzz `.seek_key()`
// TODO: Fuzz for panic and drop safety

#[derive(Debug)]
pub struct ColumnLeafConsumer<K, R> {
    // Invariant: `storage.len <= self.position`, if `storage.len == self.position` the cursor is
    // exhausted
    position: usize,
    // Invariant: `storage.keys[position..]` and `storage.diffs[position..]` are all valid
    storage: OrderedColumnLeaf<MaybeUninit<K>, MaybeUninit<R>>,
}

impl<K, R> ColumnLeafConsumer<K, R> {
    /// Get a reference to all valid keys in the leaf
    fn keys(&self) -> &[K] {
        // FIXME: MaybeUninit::slice_assume_init_ref()
        // Safety: We're just casting the valid part of the `MaybeUninit<K>` slice into
        // a slice of `K`
        unsafe { &*(&self.storage.keys[self.position..] as *const [MaybeUninit<K>] as *const [K]) }
    }

    /// Get a reference to all valid differences in the leaf
    fn diffs(&self) -> &[R] {
        // FIXME: MaybeUninit::slice_assume_init_ref()
        // Safety: We're just casting the valid part of the `MaybeUninit<R>` slice into
        // a slice of `R`
        unsafe { &*(&self.storage.diffs[self.position..] as *const [MaybeUninit<R>] as *const [R]) }
    }
}

impl<K, R> Consumer<K, (), R, ()> for ColumnLeafConsumer<K, R> {
    type ValueConsumer<'a> = ColumnLeafValues<'a, K, R>
    where
        Self: 'a;

    fn key_valid(&self) -> bool {
        self.position < self.storage.len()
    }

    fn peek_key(&self) -> &K {
        if !self.key_valid() {
            cursor_position_oob(self.position, self.storage.keys.len());
        }

        // Safety: The current key is valid
        unsafe { self.storage.keys[self.position].assume_init_ref() }
    }

    fn next_key(&mut self) -> (K, Self::ValueConsumer<'_>) {
        let idx = self.position;
        if !self.key_valid() {
            cursor_position_oob(idx, self.storage.len());
        }

        // We increment position before reading out the key and diff values
        self.position += 1;

        // Copy out the key and diff
        let key = unsafe { self.storage.keys[idx].assume_init_read() };

        (key, ColumnLeafValues::new(self))
    }

    fn seek_key(&mut self, key: &K)
    where
        K: Ord,
    {
        let start_position = self.position;

        // Search for the given key
        let offset = advance(&self.storage.keys[start_position..], |k| unsafe {
            k.assume_init_ref().lt(key)
        });

        // Increment the offset before we drop the elements for panic safety
        self.position += offset;

        // Drop the skipped elements
        unsafe {
            self.storage
                .drop_range(start_position..start_position + offset);
        }
    }
}

impl<K, R> From<OrderedColumnLeaf<K, R>> for ColumnLeafConsumer<K, R> {
    #[inline]
    fn from(leaf: OrderedColumnLeaf<K, R>) -> Self {
        Self {
            position: 0,
            storage: leaf.into_uninit(),
        }
    }
}

// We use a custom SizeOf impl to properly
// FIXME: Doesn't take into account excess capacity allocated by vecs
impl<K, R> SizeOf for ColumnLeafConsumer<K, R>
where
    K: SizeOf,
    R: SizeOf,
{
    fn size_of_children(&self, context: &mut size_of::Context) {
        // We incur two allocations from the two vecs within `OrderedColumnLeaf`
        context.add_distinct_allocations(2);

        // We count the invalid prefix as excess space
        context.add_excess((size_of::<K>() + size_of::<R>()) * self.position);

        // Then we get the sizes of all valid elements
        self.keys().size_of_children(context);
        self.diffs().size_of_children(context);
    }
}

impl<K, R> Drop for ColumnLeafConsumer<K, R> {
    fn drop(&mut self) {
        // Drop all remaining elements
        unsafe { self.storage.drop_range(self.position..) }
    }
}

#[derive(Debug, SizeOf)]
pub struct ColumnLeafValues<'a, K, R> {
    done: bool,
    consumer: &'a mut ColumnLeafConsumer<K, R>,
}

impl<'a, K, R> ColumnLeafValues<'a, K, R> {
    #[inline]
    fn new(consumer: &'a mut ColumnLeafConsumer<K, R>) -> Self {
        Self {
            done: false,
            consumer,
        }
    }
}

impl<'a, K, R> ValueConsumer<'a, (), R, ()> for ColumnLeafValues<'a, K, R> {
    fn value_valid(&self) -> bool {
        !self.done
    }

    fn next_value(&mut self) -> ((), R, ()) {
        if self.done {
            value_already_consumed();
        }
        self.done = true;

        // The consumer increments `position` before creating the value consumer
        let idx = self.consumer.position - 1;
        let diff = unsafe { self.consumer.storage.diffs[idx].assume_init_read() };

        ((), diff, ())
    }
}

impl<'a, K, R> Drop for ColumnLeafValues<'a, K, R> {
    fn drop(&mut self) {
        // If the value consumer was never used, drop the difference value
        if !self.done {
            // The consumer increments `position` before creating the value consumer
            let idx = self.consumer.position - 1;

            // Drop the unused difference value
            unsafe { self.consumer.storage.diffs[idx].assume_init_drop() };
        }
    }
}

#[cold]
#[inline(never)]
fn value_already_consumed() -> ! {
    panic!("attempted to consume a value that was already consumed")
}
