use feldera_adapterlib::errors::controller::ControllerError;
use feldera_types::program_schema::SqlIdentifier;
use std::collections::BTreeMap;

pub use feldera_adapterlib::catalog::*;

/// Circuit catalog implementation.
pub struct Catalog {
    input_collection_handles: BTreeMap<SqlIdentifier, InputCollectionHandle>,
    output_batch_handles: BTreeMap<SqlIdentifier, OutputCollectionHandles>,
}

impl Default for Catalog {
    fn default() -> Self {
        Self::new()
    }
}

impl Catalog {
    pub fn new() -> Self {
        Self {
            input_collection_handles: BTreeMap::new(),
            output_batch_handles: BTreeMap::new(),
        }
    }

    pub fn register_input_collection_handle(
        &mut self,
        handle: InputCollectionHandle,
    ) -> Result<(), ControllerError> {
        let name = &handle.schema.name;
        if self.input_collection_handles.contains_key(name) {
            return Err(ControllerError::duplicate_input_stream(&name.sql_name()));
        }
        self.input_collection_handles.insert(name.clone(), handle);

        Ok(())
    }

    pub fn register_output_batch_handles(
        &mut self,
        handles: OutputCollectionHandles,
    ) -> Result<(), ControllerError> {
        let name = &handles.schema.name;
        if self.output_batch_handles.contains_key(name) {
            return Err(ControllerError::duplicate_output_stream(&name.sql_name()));
        }
        self.output_batch_handles.insert(name.clone(), handles);

        Ok(())
    }
}

impl CircuitCatalog for Catalog {
    /// Look up an input stream handle by name.
    fn input_collection_handle(&self, name: &SqlIdentifier) -> Option<&InputCollectionHandle> {
        self.input_collection_handles.get(name)
    }

    /// Look up output stream handles by name.
    fn output_handles(&self, name: &SqlIdentifier) -> Option<&OutputCollectionHandles> {
        self.output_batch_handles.get(name)
    }

    fn output_handles_mut(&mut self, name: &SqlIdentifier) -> Option<&mut OutputCollectionHandles> {
        self.output_batch_handles.get_mut(name)
    }

    fn output_iter(
        &self,
    ) -> Box<dyn Iterator<Item = (&SqlIdentifier, &OutputCollectionHandles)> + '_> {
        Box::new(self.output_batch_handles.iter())
    }
}
