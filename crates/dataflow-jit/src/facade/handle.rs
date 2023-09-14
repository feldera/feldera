use crate::{
    codegen::{
        json::{call_deserialize_fn, DeserializeJsonFn},
        VTable,
    },
    row::{Row, UninitRow},
};
use dbsp::CollectionHandle;
use serde_json::Value;
use std::error;

#[derive(Clone)]
pub struct JsonSetHandle {
    handle: CollectionHandle<Row, i32>,
    deserialize_fn: DeserializeJsonFn,
    vtable: &'static VTable,
}

impl JsonSetHandle {
    pub fn new(
        handle: CollectionHandle<Row, i32>,
        deserialize_fn: DeserializeJsonFn,
        vtable: &'static VTable,
    ) -> Self {
        Self {
            handle,
            deserialize_fn,
            vtable,
        }
    }

    pub fn push(&self, key: &[u8], weight: i32) -> Result<(), Box<dyn error::Error>> {
        let value: Value = serde_json::from_slice(key)?;
        let key = unsafe {
            let mut uninit = UninitRow::new(self.vtable);
            call_deserialize_fn(self.deserialize_fn, uninit.as_mut_ptr(), &value)?;
            uninit.assume_init()
        };

        self.handle.push(key, weight);

        Ok(())
    }

    pub fn clear_input(&self) {
        self.handle.clear_input();
    }
}
