use crate::{
    DeCollectionHandle, DeScalarHandle, DeScalarHandleImpl, DeZSetHandle, SerOutputBatchHandle,
};
use dbsp::{
    algebra::ZRingValue,
    operator::{DelayedFeedback, NeighborhoodDescr},
    CollectionHandle, DBData, DBWeight, InputHandle, RootCircuit, Stream, ZSet,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// A set of stream handles associated with each output collection.
pub struct OutputCollectionHandles {
    /// A stream of changes to the collection.
    pub delta_handle: Box<dyn SerOutputBatchHandle>,

    /// Input stream used to specify the [neighborhood](`Stream::neighborhood`)
    /// of the collection, which the circuit will monitor.
    ///
    /// The circuit monitors at most one neighborhood at any time.  The stream
    /// carries values of type `Option<NeighborhoodDescr<K, V>>`, where `K` and `V`
    /// are the key and value types of the collection.  Writing a `Some(..)` value
    /// to the stream changes the monitored neighborhood.
    pub neighborhood_descr_handle: Box<dyn DeScalarHandle>,

    /// A stream of changes to the neighborhood, computed using the
    /// [`Stream::neighborhood`] operator.
    pub neighborhood_handle: Box<dyn SerOutputBatchHandle>,

    /// A stream that contains the full snapshot of the neightborhood.  Only produces
    /// an output whenever the `neighborhood_descr_handle` input is set to `Some(..)`.  
    pub neighborhood_snapshot_handle: Box<dyn SerOutputBatchHandle>,

    /// Input stream used to specify the number of quantiles.
    pub num_quantiles_handle: InputHandle<usize>,

    /// Quantiles stream.
    ///
    /// When the `num_quantiles_handle` input is set to `N`, `N>0`, this stream contains
    /// up to `N` quantiles of the input collection, computed using the
    /// `Stream::stream_key_quantiles` operator.
    pub quantiles_handle: Box<dyn SerOutputBatchHandle>,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum OutputQuery {
    #[serde(rename = "table")]
    Table,
    #[serde(rename = "neighborhood")]
    Neighborhood,
    #[serde(rename = "quantiles")]
    Quantiles,
}

impl Default for OutputQuery {
    fn default() -> Self {
        Self::Table
    }
}

pub struct OutputQueryHandles {
    pub delta: Option<Box<dyn SerOutputBatchHandle>>,
    pub snapshot: Option<Box<dyn SerOutputBatchHandle>>,
}

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
    output_batch_handles: BTreeMap<String, OutputCollectionHandles>,
}

impl Catalog {
    /// Create an empty catalog.
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_input_zset<Z>(
        &mut self,
        name: &str,
        stream: Stream<RootCircuit, Z>,
        handle: CollectionHandle<Z::Key, Z::R>,
    ) where
        Z: ZSet + Send + Sync,
        Z::R: ZRingValue + Into<i64> + Sync,
        Z::Key: for<'de> Deserialize<'de> + Serialize + Sync,
    {
        self.register_input_collection_handle(name, DeZSetHandle::new(handle));

        // Inputs are also outputs.
        self.register_output_zset(name, stream);
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
    pub fn register_output_zset<Z>(&mut self, name: &str, stream: Stream<RootCircuit, Z>)
    where
        Z: ZSet + Send + Sync,
        Z::R: ZRingValue + Into<i64> + Sync,
        Z::Key: for<'de> Deserialize<'de> + Serialize + Sync,
    {
        let circuit = stream.circuit();

        let delta_handle = stream.output();

        let (neighborhood_descr_stream, neighborhood_descr_handle) =
            circuit.add_input_stream::<(bool, Option<NeighborhoodDescr<Z::Key, Z::Val>>)>();
        let neighborhood_stream = {
            let feedback =
                <DelayedFeedback<RootCircuit, Option<NeighborhoodDescr<Z::Key, Z::Val>>>>::new(
                    stream.circuit(),
                );
            let new_neighborhood =
                feedback
                    .stream()
                    .apply2(&neighborhood_descr_stream, |old, (reset, new)| {
                        if *reset {
                            new.clone()
                        } else {
                            old.clone()
                        }
                    });
            feedback.connect(&new_neighborhood);
            stream.neighborhood(&new_neighborhood)
        };

        let neighborhood_handle = neighborhood_stream.output();

        let neighborhood_snapshot_stream = neighborhood_stream.integrate();
        let neighborhood_snapshot_handle = neighborhood_snapshot_stream
            .output_guarded(&neighborhood_descr_stream.apply(|(reset, _descr)| *reset));

        let (num_quantiles_stream, num_quantiles_handle) = circuit.add_input_stream::<usize>();
        let quantiles_stream = stream
            .integrate_trace()
            .stream_key_quantiles(&num_quantiles_stream);
        let quantiles_handle = quantiles_stream
            .output_guarded(&num_quantiles_stream.apply(|num_quantiles| *num_quantiles > 0));

        let handles = OutputCollectionHandles {
            delta_handle: Box::new(delta_handle) as Box<dyn SerOutputBatchHandle>,

            neighborhood_descr_handle: Box::new(DeScalarHandleImpl::new(neighborhood_descr_handle))
                as Box<dyn DeScalarHandle>,
            neighborhood_handle: Box::new(neighborhood_handle) as Box<dyn SerOutputBatchHandle>,
            neighborhood_snapshot_handle: Box::new(neighborhood_snapshot_handle)
                as Box<dyn SerOutputBatchHandle>,

            num_quantiles_handle,
            quantiles_handle: Box::new(quantiles_handle) as Box<dyn SerOutputBatchHandle>,
        };

        self.output_batch_handles.insert(name.to_owned(), handles);
    }

    /// Look up an input stream handle by name.
    pub fn input_collection_handle(&self, name: &str) -> Option<&dyn DeCollectionHandle> {
        self.input_collection_handles.get(name).map(|b| &**b)
    }

    /// Look up an output stream handle by name&.
    pub fn output_handles(&self, name: &str) -> Option<&OutputCollectionHandles> {
        self.output_batch_handles.get(name)
    }

    /// Look up an output stream handle by name.
    pub fn output_query_handles(
        &self,
        name: &str,
        query: OutputQuery,
    ) -> Option<OutputQueryHandles> {
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
