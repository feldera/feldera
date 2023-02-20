use crate::{DeCollectionHandle, DeZSetHandle, SerOutputBatchHandle};
use dbsp::{algebra::ZRingValue, CollectionHandle, DBData, DBWeight};
use serde::Deserialize;
use std::collections::BTreeMap;

/// A catalog of input and output stream handles of a circuit.
///
/// An instance of this type is created by the user (or auto-generated code)
/// who constructs the circuit and is used by
/// [`Controller`](`crate::Controller`) to bind external data sources and sinks
/// to DBSP streams
/// (See [`InputFormat::new_parser()`](`crate::InputFormat::new_parser`)
/// method).
#[derive(Default)]
pub struct Catalog {
    input_collection_handles: BTreeMap<String, Box<dyn DeCollectionHandle>>,
    output_batch_handles: BTreeMap<String, Box<dyn SerOutputBatchHandle>>,
}

impl Catalog {
    /// Create an empty catalog.
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_input_zset_handle<K, R>(&mut self, name: &str, handle: CollectionHandle<K, R>)
    where
        K: DBData + for<'de> Deserialize<'de>,
        R: DBWeight + ZRingValue,
    {
        self.register_input_collection_handle(name, DeZSetHandle::new(handle));
    }

    /// Add a named input stream handle to the catalog.
    pub fn register_input_collection_handle<H>(&mut self, name: &str, handle: H)
    where
        H: DeCollectionHandle + 'static,
    {
        self.input_collection_handles
            .insert(name.to_owned(), Box::new(handle));
    }

    /// Add a named output stream handle to the catalog.
    pub fn register_output_batch_handle<H>(&mut self, name: &str, handle: H)
    where
        H: SerOutputBatchHandle + 'static,
    {
        self.output_batch_handles
            .insert(name.to_owned(), Box::new(handle));
    }

    /// Look up an input stream handle by name.
    pub fn input_collection_handle(&self, name: &str) -> Option<&dyn DeCollectionHandle> {
        self.input_collection_handles.get(name).map(|b| &**b)
    }

    /// Look up an output stream handle by name.
    pub fn output_batch_handle(&self, name: &str) -> Option<&dyn SerOutputBatchHandle> {
        self.output_batch_handles.get(name).map(|b| &**b)
    }
}
