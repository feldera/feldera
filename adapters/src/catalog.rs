use crate::{DeCollectionHandle, DeZSetHandle};
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
    input_collections: BTreeMap<String, Box<dyn DeCollectionHandle>>,
}

impl Catalog {
    /// Create an empty catalog.
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_input_zset<K, R>(&mut self, name: &str, handle: CollectionHandle<K, R>)
    where
        K: DBData + for<'de> Deserialize<'de>,
        R: DBWeight + ZRingValue,
    {
        self.register_input(name, DeZSetHandle::new(handle));
    }

    /// Add a named input stream handle to the catalog.
    pub fn register_input<H>(&mut self, name: &str, handle: H)
    where
        H: DeCollectionHandle + 'static,
    {
        self.input_collections
            .insert(name.to_owned(), Box::new(handle));
    }

    /// Look up an input stream handle by name.
    pub fn input_collection(&self, name: &str) -> Option<&dyn DeCollectionHandle> {
        self.input_collections.get(name).map(|b| &**b)
    }
}
