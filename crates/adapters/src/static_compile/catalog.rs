use crate::{
    catalog::{
        DeCollectionHandle, NeighborhoodEntry, OutputCollectionHandles, SerCollectionHandle,
    },
    static_compile::{DeScalarHandleImpl, ErasedDeScalarHandle},
    CircuitCatalog, OutputQuery, OutputQueryHandles,
};
use dbsp::{
    algebra::ZRingValue,
    operator::{DelayedFeedback, NeighborhoodDescr},
    CollectionHandle, RootCircuit, Stream, UpsertHandle, ZSet,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use super::{DeSetHandle, DeZSetHandle, SerCollectionHandleImpl};

/// A catalog of input and output stream handles of a circuit.
///
/// An instance of this type is created by the user (or auto-generated code)
/// who constructs the circuit and is used by
/// [`Controller`](`crate::Controller`) to bind external data sources and sinks
/// to DBSP streams.
#[derive(Default)]
pub struct Catalog {
    input_collection_handles: BTreeMap<String, Box<dyn DeCollectionHandle>>,
    output_batch_handles: BTreeMap<String, OutputCollectionHandles>,
}

impl Catalog {
    /// Create an empty catalog.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add an input stream of Z-sets to the catalog.
    ///
    /// Adds a `DeCollectionHandle` to the catalog, which will deserialize
    /// input records into type `D` before converting them to `Z::Key` using
    /// the `From` trait.
    pub fn register_input_zset<Z, D>(
        &mut self,
        name: &str,
        stream: Stream<RootCircuit, Z>,
        handle: CollectionHandle<Z::Key, Z::R>,
    ) where
        D: for<'de> Deserialize<'de> + Serialize + From<Z::Key> + Clone + Send + 'static,
        Z: ZSet + Send + Sync,
        Z::R: ZRingValue + Into<i64> + Sync,
        Z::Key: Serialize + Sync + From<D>,
    {
        self.register_input_collection_handle(name, DeZSetHandle::new(handle));

        // Inputs are also outputs.
        self.register_output_zset(name, stream);
    }

    pub fn register_input_set<Z, D>(
        &mut self,
        name: &str,
        stream: Stream<RootCircuit, Z>,
        handle: UpsertHandle<Z::Key, bool>,
    ) where
        D: for<'de> Deserialize<'de> + Serialize + From<Z::Key> + Clone + Send + 'static,
        Z: ZSet + Send + Sync,
        Z::R: ZRingValue + Into<i64> + Sync,
        Z::Key: Serialize + Sync + From<D>,
    {
        self.register_input_collection_handle(name, DeSetHandle::new(handle));

        // Inputs are also outputs.
        self.register_output_zset(name, stream);
    }

    /// Add a named input stream handle to the catalog.
    fn register_input_collection_handle<H>(&mut self, name: &str, handle: H)
    where
        H: DeCollectionHandle + 'static,
    {
        self.input_collection_handles
            .insert(name.to_owned(), Box::new(handle));
    }

    /// Add an output stream of Z-sets to the catalog.
    pub fn register_output_zset<Z, D>(&mut self, name: &str, stream: Stream<RootCircuit, Z>)
    where
        D: for<'de> Deserialize<'de> + Serialize + From<Z::Key> + Clone + Send + 'static,
        Z: ZSet + Send + Sync,
        Z::R: ZRingValue + Into<i64> + Sync,
        Z::Key: Serialize + Sync + From<D>,
    {
        let circuit = stream.circuit();

        // Create handle for the stream itself.
        let delta_handle = stream.output();

        // Improve the odds that `integrate_trace` below reuses the trace of `stream`
        // if one exists.
        let stream = stream.try_sharded_version();

        // Create handles for the neighborhood query.
        let (neighborhood_descr_stream, neighborhood_descr_handle) =
            circuit.add_input_stream::<(bool, Option<NeighborhoodDescr<D, ()>>)>();
        let neighborhood_stream = {
            // Create a feedback loop to latch the latest neighborhood descriptor
            // when `reset=true`.
            let feedback =
                <DelayedFeedback<RootCircuit, Option<NeighborhoodDescr<Z::Key, ()>>>>::new(
                    stream.circuit(),
                );
            let new_neighborhood =
                feedback
                    .stream()
                    .apply2(&neighborhood_descr_stream, |old, (reset, new)| {
                        if *reset {
                            // Convert anchor of type `D` into `Z::Key`.
                            new.clone().map(|new| {
                                NeighborhoodDescr::new(
                                    new.anchor.map(From::from),
                                    (),
                                    new.before,
                                    new.after,
                                )
                            })
                        } else {
                            old.clone()
                        }
                    });
            feedback.connect(&new_neighborhood);
            stream.neighborhood(&new_neighborhood)
        };

        // Neighborhood delta stream.
        let neighborhood_handle = neighborhood_stream.output();

        // Neighborhood snapshot stream.  The integral computation
        // is essentially free thanks to stream caching.
        let neighborhood_snapshot_stream = neighborhood_stream.integrate();
        let neighborhood_snapshot_handle = neighborhood_snapshot_stream
            .output_guarded(&neighborhood_descr_stream.apply(|(reset, _descr)| *reset));

        // Handle for the quantiles query.
        let (num_quantiles_stream, num_quantiles_handle) = circuit.add_input_stream::<usize>();

        // Output of the quantiles query, only produced when `num_quantiles>0`.
        let quantiles_stream = stream
            .integrate_trace()
            .stream_key_quantiles(&num_quantiles_stream);
        let quantiles_handle = quantiles_stream
            .output_guarded(&num_quantiles_stream.apply(|num_quantiles| *num_quantiles > 0));

        let handles = OutputCollectionHandles {
            delta_handle: Box::new(<SerCollectionHandleImpl<_, D, ()>>::new(delta_handle))
                as Box<dyn SerCollectionHandle>,

            neighborhood_descr_handle: Box::new(DeScalarHandleImpl::new(neighborhood_descr_handle))
                as Box<dyn ErasedDeScalarHandle>,
            neighborhood_handle: Box::new(
                <SerCollectionHandleImpl<_, NeighborhoodEntry<D>, ()>>::new(neighborhood_handle),
            ) as Box<dyn SerCollectionHandle>,
            neighborhood_snapshot_handle: Box::new(<SerCollectionHandleImpl<
                _,
                NeighborhoodEntry<D>,
                (),
            >>::new(neighborhood_snapshot_handle))
                as Box<dyn SerCollectionHandle>,

            num_quantiles_handle,
            quantiles_handle: Box::new(<SerCollectionHandleImpl<_, D, ()>>::new(quantiles_handle))
                as Box<dyn SerCollectionHandle>,
        };

        self.output_batch_handles.insert(name.to_owned(), handles);
    }
}

impl CircuitCatalog for Catalog {
    /// Look up an input stream handle by name.
    fn input_collection_handle(&self, name: &str) -> Option<&dyn DeCollectionHandle> {
        self.input_collection_handles
            .get(name)
            .map(|b| &**b as &dyn DeCollectionHandle)
    }

    /// Look up output stream handles by name.
    fn output_handles(&self, name: &str) -> Option<&OutputCollectionHandles> {
        self.output_batch_handles.get(name)
    }

    /// Look up an output query handles by stream name and query type.
    fn output_query_handles(&self, name: &str, query: OutputQuery) -> Option<OutputQueryHandles> {
        self.output_batch_handles
            .get(name)
            .map(|handles| match query {
                OutputQuery::Table => OutputQueryHandles {
                    delta: Some(handles.delta_handle.fork()),
                    snapshot: None,
                },
                OutputQuery::Neighborhood => OutputQueryHandles {
                    delta: (Some(handles.neighborhood_handle.fork())),
                    snapshot: Some(handles.neighborhood_snapshot_handle.fork()),
                },
                OutputQuery::Quantiles => OutputQueryHandles {
                    delta: None,
                    snapshot: Some(handles.quantiles_handle.fork()),
                },
            })
    }
}
