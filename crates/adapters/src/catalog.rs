use std::sync::Arc;

use crate::{static_compile::ErasedDeScalarHandle, ControllerError};
use anyhow::Result as AnyResult;
use dbsp::InputHandle;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Descriptor that specifies the format in which records are received
/// or into which they should be encoded before sending.
///
// TODO: Currently we only allows choosing between JSON and CSV formats.
// In the future, in addition to adding mode formats we can make these two
// formats configurable (see below).
#[derive(Clone, Deserialize, Serialize, Debug, PartialEq, Eq)]
pub enum RecordFormat {
    // TODO: Support different JSON encodings:
    // * Map - the default encoding
    // * Array - allow the subset and the order of columns to be configurable
    // * Raw - Only applicable to single-column tables.  Input records contain
    // raw encoding of this column only.  This is particularly useful for
    // tables that store raw JSON or binary data to be parsed using SQL.
    Json,
    Csv,
}

// This is only here so we can derive `ToSchema` for it without adding
// a `utoipa` dependency to the `dbsp` crate to derive ToSchema for
// `NeighborhoodDescr`.
/// A request to output a specific neighborhood of a table or view.
/// The neighborhood is defined in terms of its central point (`anchor`)
/// and the number of rows preceding and following the anchor to output.
#[derive(Deserialize, ToSchema)]
pub struct NeighborhoodQuery {
    pub anchor: Option<utoipa::openapi::Object>,
    pub before: u32,
    pub after: u32,
}

// Helper type only used to serialize neighborhoods as a map vs tuple.
#[derive(Serialize)]
pub struct NeighborhoodEntry<KD> {
    index: isize,
    key: KD,
}

impl<K, KD> From<(isize, (K, ()))> for NeighborhoodEntry<KD>
where
    KD: From<K>,
{
    fn from((index, (key, ())): (isize, (K, ()))) -> Self {
        Self {
            index,
            key: KD::from(key),
        }
    }
}

/// An input handle that deserializes records before pushing them to a
/// stream.
///
/// A trait for a type that wraps a [`CollectionHandle`](`dbsp::CollectionHandle`) or
/// an [`UpsertHandle`](`dbsp::UpsertHandle`) and pushes serialized relational data
/// to the associated input stream record-by-record.  The client passes a
/// byte array with a serialized data record (e.g., in JSON or CSV format)
/// to [`insert`](`Self::insert`) and [`delete`](`Self::delete`) methods.
/// The record gets deserialized into the strongly typed representation
/// expected by the input stream and gets buffered inside the handle.
/// The [`flush`](`Self::flush`) method pushes all buffered data to the
/// underlying [`CollectionHandle`](`dbsp::CollectionHandle`) or
/// [`UpsertHandle`](`dbsp::UpsertHandle`).
///
/// Instances of this trait are created by calling
/// [`DeCollectionHandle::configure_deserializer`].
/// The data format accepted by the handle is determined
/// by the `record_format` argument passed to this method.
pub trait DeCollectionStream: Send {
    /// Buffer a new insert update.
    ///
    /// Returns an error if deserialization fails, i.e., the serialized
    /// representation is corrupted or does not match the value type of
    /// the underlying input stream.
    fn insert(&mut self, data: &[u8]) -> AnyResult<()>;

    /// Buffer a new delete update.
    ///
    /// The `data` argument contains a serialized record whose
    /// type depends on the underlying input stream: streams created by
    /// [`RootCircuit::add_input_zset`](`dbsp::RootCircuit::add_input_zset`)
    /// and [`RootCircuit::add_input_set`](`dbsp::RootCircuit::add_input_set`)
    /// methods support deletion by value, hence the serialized record must
    /// match the value type of the stream.  Streams created with
    /// [`RootCircuit::add_input_map`](`dbsp::RootCircuit::add_input_map`)
    /// support deletion by key, so the serialized record must match the key
    /// type of the stream.
    ///
    /// The record gets deserialized and pushed to the underlying input stream
    /// handle as a delete update.
    ///
    /// Returns an error if deserialization fails, i.e., the serialized
    /// representation is corrupted or does not match the value or key
    /// type of the underlying input stream.
    fn delete(&mut self, data: &[u8]) -> AnyResult<()>;

    /// Reserve space for at least `reservation` more updates in the
    /// internal input buffer.
    ///
    /// Reservations are not required but can be used when the number
    /// of inputs is known ahead of time to reduce reallocations.
    fn reserve(&mut self, reservation: usize);

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
    // TODO: add another method to invoke `CollectionHandle::clear_input`?
    fn clear_buffer(&mut self);

    /// Create a new deserializer with the same configuration connected to
    /// the same input stream.
    fn fork(&self) -> Box<dyn DeCollectionStream>;
}

/// A handle to an input collection that can be used to feed serialized data
/// to the collection.
pub trait DeCollectionHandle: Send {
    /// Create a [`DeCollectionStream`] object to parse input data encoded
    /// using the format specified in `RecordFormat`.
    fn configure_deserializer(
        &self,
        record_format: RecordFormat,
    ) -> Result<Box<dyn DeCollectionStream>, ControllerError>;
}

/// A type-erased batch whose contents can be serialized.
///
/// This is a wrapper around the DBSP `Batch` trait that returns a cursor that
/// yields `erased_serde::Serialize` trait objects that can be used to serialize
/// the contents of the batch without knowing its key and value types.
pub trait SerBatch: Send + Sync {
    /// Number of keys in the batch.
    fn key_count(&self) -> usize;

    /// Number of tuples in the batch.
    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Create a cursor over the batch that yields record
    /// formatted using the specified format.
    fn cursor<'a>(
        &'a self,
        record_format: RecordFormat,
    ) -> Result<Box<dyn SerCursor + 'a>, ControllerError>;
}

/// Cursor that allows serializing the contents of a type-erased batch.
///
/// This is a wrapper around the DBSP `Cursor` trait that yields keys and values
/// of the underlying batch as `erased_serde::Serialize` trait objects.
pub trait SerCursor {
    /// Indicates if the current key is valid.
    ///
    /// A value of `false` indicates that the cursor has exhausted all keys.
    fn key_valid(&self) -> bool;

    /// Indicates if the current value is valid.
    ///
    /// A value of `false` indicates that the cursor has exhausted all values
    /// for this key.
    fn val_valid(&self) -> bool;

    /// Serialize current key. Panics if invalid.
    fn serialize_key(&mut self, dst: &mut Vec<u8>) -> AnyResult<()>;

    /// Serialize the `(key, weight)` tuple.
    ///
    /// FIXME: This only exists to support the CSV serializer, which outputs
    /// key and weight in the same CSV record.  This should be eliminated once
    /// we have a better idea about the common denominator between JIT and
    /// serde serializers.
    fn serialize_key_weight(&mut self, dst: &mut Vec<u8>) -> AnyResult<()>;

    /// Serialize current value. Panics if invalid.
    fn serialize_val(&mut self, dst: &mut Vec<u8>) -> AnyResult<()>;

    /// Returns the weight associated with the current key/value pair.
    fn weight(&mut self) -> i64;

    /// Advances the cursor to the next key.
    fn step_key(&mut self);

    /// Advances the cursor to the next value.
    fn step_val(&mut self);

    /// Rewinds the cursor to the first key.
    fn rewind_keys(&mut self);

    /// Rewinds the cursor to the first value for current key.
    fn rewind_vals(&mut self);
}

/// A handle to an output stream of a circuit that yields type-erased
/// output batches.
///
/// A trait for a type that wraps around an [`OutputHandle<Batch>`] and
/// yields output batches produced by the circuit as [`SerBatch`]s.
pub trait SerCollectionHandle: Send + Sync {
    /// Like [`OutputHandle::take_from_worker`], but returns output batch as a
    /// [`SerBatch`] trait object.
    fn take_from_worker(&self, worker: usize) -> Option<Box<dyn SerBatch>>;

    /// Like [`OutputHandle::take_from_all`], but returns output batches as
    /// [`SerBatch`] trait objects.
    fn take_from_all(&self) -> Vec<Arc<dyn SerBatch>>;

    /// Like [`OutputHandle::consolidate`], but returns the output batch as a
    /// [`SerBatch`] trait object.
    fn consolidate(&self) -> Box<dyn SerBatch>;

    /// Returns an alias to `self`.
    fn fork(&self) -> Box<dyn SerCollectionHandle>;
}

/// A catalog of input and output stream handles of a circuit.
pub trait CircuitCatalog: Send {
    /// Look up an input stream handle by name.
    fn input_collection_handle(&self, name: &str) -> Option<&dyn DeCollectionHandle>;

    /// Look up output stream handles by name.
    fn output_handles(&self, name: &str) -> Option<&OutputCollectionHandles>;

    /// Look up output query handles by stream name and query type.
    fn output_query_handles(&self, name: &str, query: OutputQuery) -> Option<OutputQueryHandles>;
}

/// A set of stream handles associated with each output collection.
pub struct OutputCollectionHandles {
    /// A stream of changes to the collection.
    pub delta_handle: Box<dyn SerCollectionHandle>,

    /// Input stream used to submit neighborhood queries.
    ///
    /// The stream carries values of type `(bool, Option<NeighborhoodDescr<K,
    /// V>>)`, where `K` and `V` are the key and value types of the
    /// collection.
    ///
    /// The first component of the tuple is the `reset` flag, which instructs
    /// the circuit to start executing the neighborhood query specified in the
    /// second component of the tuple.  The outputs of the neighborhood query
    /// are emitted to [`neighborhood_handle`](`Self::neighborhood_handle`)
    /// and
    /// [`neighborhood_snapshot_handle`](`Self::neighborhood_snapshot_handle`)
    /// streams.  When the flag is `false`, this input is ignored.
    ///
    /// In more detail, the circuit handles inputs written to this stream as
    /// follows:
    ///
    /// * `(true, Some(descr))` - Start monitoring the specified descriptor. The
    ///   circuit will output a complete snapshot of the neighborhood to the
    ///   [`neighborhood_snapshot_handle`](`Self::neighborhood_snapshot_handle`)
    ///   sstream at the end of the current clock cycle.  The
    ///   [`neighborhood_handle`](`Self::neighborhood_handle`) stream will
    ///   output the difference between the previous and the new neighborhoods
    ///   at the end of the current clock cycle and will contain changes to the
    ///   new neighborhood going forward.
    ///
    /// * `(true, None)` - Stop executing the neighborhood query.  This is
    ///   equivalent to writing `(true, Some(descr))`, where `descr` specifies
    ///   an empty neighborhood.
    ///
    /// * `(false, _)` - This is a no-op. The circuit will continue monitoring
    ///   the previously specified neighborhood if any.  Nothing is written to
    ///   the [`neighborhood_snapshot_handle`](`Self::neighborhood_snapshot_handle`)
    ///   stream.
    pub neighborhood_descr_handle: Box<dyn ErasedDeScalarHandle>,

    /// A stream of changes to the neighborhood, computed using the
    /// [`Stream::neighborhood`] operator.
    pub neighborhood_handle: Box<dyn SerCollectionHandle>,

    /// A stream that contains the full snapshot of the neighborhood.  Only produces
    /// an output whenever the `neighborhood_descr_handle` input is set to `Some(..)`.
    pub neighborhood_snapshot_handle: Box<dyn SerCollectionHandle>,

    /// Input stream used to submit the quantiles query.
    ///
    /// The value in the stream specifies the number of quantiles to
    /// output.  When  greater than zero, it triggers the quantiles
    /// computation.  The result is output to the
    /// [`quantiles_handle`](`Self::quantiles_handle`) stream at the
    /// end of the current clock cycle.
    pub num_quantiles_handle: InputHandle<usize>,

    /// Quantiles stream.
    ///
    /// When the `num_quantiles_handle` input is set to `N`, `N>0`, this stream
    /// outputs up to `N` quantiles of the input collection, computed using
    /// the [`Stream::stream_key_quantiles`] operator.
    pub quantiles_handle: Box<dyn SerCollectionHandle>,
}

/// A query over an output stream.
///
/// We currently do not support ad hoc queries.  Instead the client can use
/// three pre-defined queries to inspect the contents of a table or view.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, PartialOrd, ToSchema, Ord)]
pub enum OutputQuery {
    /// Query the entire contents of the table (similar to `SELECT * FROM`).
    #[serde(rename = "table")]
    Table,
    /// Neighborhood query (see [`Stream::neighborhood`](`dbsp::Stream::neighborhood`)).
    #[serde(rename = "neighborhood")]
    Neighborhood,
    /// Quantiles query (see [`Stream::stream_key_quantiles`](`dbsp::Stream::stream_key_quantiles`)).
    #[serde(rename = "quantiles")]
    Quantiles,
}

impl Default for OutputQuery {
    fn default() -> Self {
        Self::Table
    }
}

/// Query result streams.
///
/// Stores the result of a a [query](`OutputQuery`) as a pair of streams:
/// a stream of changes and a snapshot, i.e., the integral, of all previous
/// changes.  Not all queries return both streams, e.g., the
/// [quantiles](`OutputQuery::Quantiles`) query only returns a snapshot,
/// while the [table](`OutputQuery::Table`) query currently only returns the
/// delta stream; therefore the stream handles are wrapped in `Option`s.
///
/// Whenever both streams are present, the client may consume the result in
/// a hybrid mode: read the initial snapshot containing a full answer to the
/// query based on previously received inputs, and then listen to the delta
/// stream for incremental updates triggered by new inputs.
pub struct OutputQueryHandles {
    pub delta: Option<Box<dyn SerCollectionHandle>>,
    pub snapshot: Option<Box<dyn SerCollectionHandle>>,
}
