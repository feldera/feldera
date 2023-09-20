use crate::{
    catalog::{OutputCollectionHandles, SerCollectionHandle},
    Catalog,
};

impl Catalog {
    /// Register "naked" output collection handle without
    /// accompanying neighborhood/quantile handles.
    ///
    /// Used for JIT-compiled circuits, which don't yet support
    /// neighborhoods and quantiles.
    pub fn register_output_collection_handle(
        &mut self,
        name: &str,
        handle: Box<dyn SerCollectionHandle>,
    ) {
        self.output_batch_handles.insert(
            name.to_string(),
            OutputCollectionHandles {
                delta_handle: handle,
                neighborhood_descr_handle: None,
                neighborhood_handle: None,
                neighborhood_snapshot_handle: None,
                num_quantiles_handle: None,
                quantiles_handle: None,
            },
        );
    }
}
