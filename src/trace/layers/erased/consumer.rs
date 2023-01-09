use crate::{
    trace::{
        layers::{
            advance_erased,
            erased::{ErasedLayer, TypedLayer},
        },
        Consumer, ValueConsumer,
    },
    utils::cursor_position_oob,
};
use std::{marker::PhantomData, mem::ManuallyDrop};

// TODO: Fuzz for correctness and equivalence to Cursor
// TODO: Fuzz `.seek_key()`
// TODO: Fuzz for panic and drop safety

#[derive(Debug)]
pub struct TypedLayerConsumer<K, R> {
    // Invariant: `storage.len <= self.position`, if `storage.len == self.position` the cursor is
    // exhausted
    position: usize,
    // Invariant: `storage.keys[position..]` and `storage.diffs[position..]` are all valid
    storage: ManuallyDrop<ErasedLayer>,
    __type: PhantomData<(K, R)>,
}

impl<K, R> Consumer<K, (), R, ()> for TypedLayerConsumer<K, R> {
    type ValueConsumer<'a> = TypedLayerValues<'a, K, R>
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
        unsafe { &*self.storage.keys.index(self.position).cast() }
    }

    fn next_key(&mut self) -> (K, Self::ValueConsumer<'_>) {
        let idx = self.position;
        if !self.key_valid() {
            cursor_position_oob(idx, self.storage.len());
        }

        // We increment position before reading out the key and diff values
        self.position += 1;

        // Copy out the key and diff
        let key = unsafe { self.storage.keys.index(idx).cast::<K>().read() };

        (key, TypedLayerValues::new(self))
    }

    fn seek_key(&mut self, key: &K)
    where
        K: Ord,
    {
        let key = key as *const K as *const u8;
        let start_position = self.position;

        // Search for the given key
        let offset = advance_erased(
            self.storage.keys.range(start_position..),
            self.storage.key_size(),
            |x| unsafe { (self.storage.keys.vtable().common.lt)(x, key) },
        );

        // Increment the offset before we drop the elements for panic safety
        self.position += offset;

        // Drop the skipped elements
        unsafe {
            self.storage
                .drop_range(start_position..start_position + offset);
        }
    }
}

impl<K, R> From<TypedLayer<K, R>> for TypedLayerConsumer<K, R> {
    #[inline]
    fn from(layer: TypedLayer<K, R>) -> Self {
        Self {
            position: 0,
            storage: ManuallyDrop::new(layer.layer),
            __type: PhantomData,
        }
    }
}

impl<K, R> Drop for TypedLayerConsumer<K, R> {
    fn drop(&mut self) {
        unsafe {
            // Drop all remaining elements
            self.storage.drop_range(self.position..);

            // Set the vec's lengths to zero, we just dropped all of their elements
            self.storage.keys.set_len(0);
            self.storage.diffs.set_len(0);

            // Drop the layer, deallocating the backing vecs
            ManuallyDrop::drop(&mut self.storage);
        }
    }
}

#[derive(Debug)]
pub struct TypedLayerValues<'a, K, R> {
    done: bool,
    consumer: &'a mut TypedLayerConsumer<K, R>,
}

impl<'a, K, R> TypedLayerValues<'a, K, R> {
    #[inline]
    fn new(consumer: &'a mut TypedLayerConsumer<K, R>) -> Self {
        Self {
            done: false,
            consumer,
        }
    }
}

impl<'a, K, R> ValueConsumer<'a, (), R, ()> for TypedLayerValues<'a, K, R> {
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
        let diff = unsafe { self.consumer.storage.diffs.index(idx).cast::<R>().read() };

        ((), diff, ())
    }

    fn remaining_values(&self) -> usize {
        !self.done as usize
    }
}

impl<'a, K, R> Drop for TypedLayerValues<'a, K, R> {
    fn drop(&mut self) {
        // If the value consumer was never used, drop the difference value
        if !self.done {
            // The consumer increments `position` before creating the value consumer
            let idx = self.consumer.position - 1;

            // Drop the unused difference value
            unsafe {
                (self.consumer.storage.diffs.vtable().common.drop_in_place)(
                    self.consumer.storage.diffs.index_mut(idx),
                );
            }
        }
    }
}

#[cold]
#[inline(never)]
fn value_already_consumed() -> ! {
    panic!("attempted to consume a value that was already consumed")
}
