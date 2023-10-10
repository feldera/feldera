use crate::{
    codegen::{
        json::{call_deserialize_fn, DeserializeJsonFn},
        VTable,
    },
    dataflow::relations::{
        IndexedZSetHandle, IndexedZSetUpsertHandle, ZSetHandle, ZSetUpsertHandle,
    },
    row::{Row, UninitRow},
};
use anyhow::Result as AnyResult;
use serde_json::Value;

/// Maximal buffer size reused across input batches.
const MAX_REUSABLE_CAPACITY: usize = 100_000;

fn clear_and_shrink<T>(buffer: &mut Vec<T>) {
    buffer.clear();
    buffer.shrink_to(MAX_REUSABLE_CAPACITY);
}

/// Convenience function for deserializing a row
///
/// # Safety
///
/// `deserialize_fn` must be for the type `vtable` contains
unsafe fn deserialize_row(
    deserialize_fn: DeserializeJsonFn,
    vtable: &'static VTable,
    json: &Value,
) -> AnyResult<Row> {
    let mut uninit = UninitRow::new(vtable);

    unsafe {
        call_deserialize_fn(deserialize_fn, uninit.as_mut_ptr(), json)?;
        Ok(uninit.assume_init())
    }
}

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
    handle: ZSetHandle,
    deserialize_fn: DeserializeJsonFn,
    vtable: &'static VTable,
    updates: Vec<(Row, i32)>,
}

impl JsonZSetHandle {
    pub fn new(
        handle: ZSetHandle,
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
        clear_and_shrink(&mut self.updates);
    }
}

impl DeCollectionStream for JsonZSetHandle {
    fn push(&mut self, key: &[u8], weight: i32) -> AnyResult<()> {
        let json: Value = serde_json::from_slice(key)?;
        let key = unsafe { deserialize_row(self.deserialize_fn, self.vtable, &json)? };

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

#[derive(Clone)]
pub struct JsonSetHandle {
    handle: ZSetUpsertHandle,
    deserialize_fn: DeserializeJsonFn,
    vtable: &'static VTable,
    updates: Vec<(Row, bool)>,
}

impl JsonSetHandle {
    pub fn new(
        handle: ZSetUpsertHandle,
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
        clear_and_shrink(&mut self.updates);
    }
}

impl DeCollectionStream for JsonSetHandle {
    fn push(&mut self, key: &[u8], weight: i32) -> AnyResult<()> {
        let json: Value = serde_json::from_slice(key)?;
        let key = unsafe { deserialize_row(self.deserialize_fn, self.vtable, &json)? };

        // FIXME: Is this correct? Do we need special handling?
        self.updates.push((key, weight.is_positive()));

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

#[derive(Clone)]
pub struct JsonIndexedZSetHandle {
    handle: IndexedZSetHandle,
    key_vtable: &'static VTable,
    deserialize_key: DeserializeJsonFn,
    value_vtable: &'static VTable,
    deserialize_value: DeserializeJsonFn,
    updates: Vec<(Row, (Row, i32))>,
}

impl JsonIndexedZSetHandle {
    pub fn new(
        handle: IndexedZSetHandle,
        key_vtable: &'static VTable,
        deserialize_key: DeserializeJsonFn,
        value_vtable: &'static VTable,
        deserialize_value: DeserializeJsonFn,
    ) -> Self {
        Self {
            handle,
            key_vtable,
            deserialize_key,
            value_vtable,
            deserialize_value,
            updates: Vec::new(),
        }
    }

    fn clear(&mut self) {
        clear_and_shrink(&mut self.updates);
    }
}

impl DeCollectionStream for JsonIndexedZSetHandle {
    fn push(&mut self, key: &[u8], weight: i32) -> AnyResult<()> {
        let json: Value = serde_json::from_slice(key)?;

        let key = unsafe { deserialize_row(self.deserialize_key, self.key_vtable, &json)? };
        let value = unsafe { deserialize_row(self.deserialize_value, self.value_vtable, &json)? };

        self.updates.push((key, (value, weight)));

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

#[derive(Clone)]
pub struct JsonMapHandle {
    handle: IndexedZSetUpsertHandle,
    key_vtable: &'static VTable,
    deserialize_key: DeserializeJsonFn,
    value_vtable: &'static VTable,
    deserialize_value: DeserializeJsonFn,
    updates: Vec<(Row, Option<Row>)>,
}

impl JsonMapHandle {
    pub fn new(
        handle: IndexedZSetUpsertHandle,
        key_vtable: &'static VTable,
        deserialize_key: DeserializeJsonFn,
        value_vtable: &'static VTable,
        deserialize_value: DeserializeJsonFn,
    ) -> Self {
        Self {
            handle,
            key_vtable,
            deserialize_key,
            value_vtable,
            deserialize_value,
            updates: Vec::new(),
        }
    }

    fn clear(&mut self) {
        clear_and_shrink(&mut self.updates);
    }
}

impl DeCollectionStream for JsonMapHandle {
    fn push(&mut self, key: &[u8], weight: i32) -> AnyResult<()> {
        let json: Value = serde_json::from_slice(key)?;

        let key = unsafe { deserialize_row(self.deserialize_key, self.key_vtable, &json)? };
        let value = weight
            .is_positive()
            .then(|| unsafe { deserialize_row(self.deserialize_value, self.value_vtable, &json) })
            .transpose()?;

        self.updates.push((key, value));

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
