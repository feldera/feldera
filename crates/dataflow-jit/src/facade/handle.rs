use crate::{
    codegen::{
        json::{call_deserialize_fn, DeserializeJsonFn},
        VTable,
    },
    row::{Row, UninitRow},
};
use anyhow::Result as AnyResult;
use dbsp::CollectionHandle;
use serde_json::Value;

/// Maximal buffer size reused across input batches.
const MAX_REUSABLE_CAPACITY: usize = 100_000;

/// This is a copy of `trait DeCollectionStream` from the adapters
/// crate (with small modifications), required to avoid circular
/// dependencies between crates.
pub trait DeCollectionStream: Send + Clone {
    /// Buffer a new update.
    ///
    /// Returns an error if deserialization fails, i.e., the serialized
    /// representation is corrupted or does not match the value type of
    /// the underlying input stream.
    fn push(&mut self, data: &[u8], weight: i32) -> AnyResult<()>;

    /// Reserve space for at least `reservation` more updates in the
    /// internal input buffer.
    fn reserve(&mut self, additional: usize);

    /// Push all buffered updates to the underlying input stream handle.
    ///
    /// Flushed updates will be pushed to the stream during the next call
    /// to [`DBSPHandle::step`](`dbsp::DBSPHandle::step`).  `flush` can
    /// be called multiple times between two subsequent `step`s.  Every
    /// `flush` call adds new updates to the previously flushed updates.
    ///
    /// Updates queued after the last `flush` remain buffered in the handle
    /// until the next `flush` or `clear_buffer` call or until the handle
    /// is destroyed.
    fn flush(&mut self);

    /// Clear all buffered updates.
    ///
    /// Clears updates pushed to the handle after the last `flush`.
    /// Flushed updates remain queued at the underlying input handle.
    fn clear_buffer(&mut self);
}

#[derive(Clone)]
pub struct JsonZSetHandle {
    handle: CollectionHandle<Row, i32>,
    deserialize_fn: DeserializeJsonFn,
    vtable: &'static VTable,
    updates: Vec<(Row, i32)>,
}

impl JsonZSetHandle {
    pub fn new(
        handle: CollectionHandle<Row, i32>,
        deserialize_fn: DeserializeJsonFn,
        vtable: &'static VTable,
    ) -> Self {
        Self {
            handle,
            deserialize_fn,
            vtable,
            updates: Vec::new(),
        }
    }

    fn clear(&mut self) {
        self.updates.clear();
        self.updates.shrink_to(MAX_REUSABLE_CAPACITY);
    }
}

impl DeCollectionStream for JsonZSetHandle {
    fn push(&mut self, key: &[u8], weight: i32) -> AnyResult<()> {
        let value: Value = serde_json::from_slice(key)?;
        let key = unsafe {
            let mut uninit = UninitRow::new(self.vtable);
            call_deserialize_fn(self.deserialize_fn, uninit.as_mut_ptr(), &value)?;
            uninit.assume_init()
        };

        self.updates.push((key, weight));

        Ok(())
    }

    fn reserve(&mut self, additional: usize) {
        self.updates.reserve(additional);
    }

    fn flush(&mut self) {
        self.handle.append(&mut self.updates);
        self.clear();
    }

    fn clear_buffer(&mut self) {
        self.clear();
    }
}
