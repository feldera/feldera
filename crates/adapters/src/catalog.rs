use std::{collections::BTreeMap, sync::Arc};

use crate::{static_compile::DeScalarHandle, ControllerError};
use anyhow::Result as AnyResult;
use dbsp::InputHandle;
use pipeline_types::format::json::JsonFlavor;
use pipeline_types::query::OutputQuery;
use serde::{Deserialize, Serialize};

/// Descriptor that specifies the format in which records are received
/// or into which they should be encoded before sending.
#[derive(Clone, Deserialize, Serialize, Debug, PartialEq, Eq)]
pub enum RecordFormat {
    // TODO: Support different JSON encodings:
    // * Map - the default encoding
    // * Array - allow the subset and the order of columns to be configurable
    // * Raw - Only applicable to single-column tables.  Input records contain
    // raw encoding of this column only.  This is particularly useful for
    // tables that store raw JSON or binary data to be parsed using SQL.
    Json(JsonFlavor),
    Csv,
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
/// A trait for a type that wraps a
/// [`CollectionHandle`](`dbsp::CollectionHandle`) or
/// an [`UpsertHandle`](`dbsp::UpsertHandle`) and pushes serialized relational
/// data to the associated input stream record-by-record.  The client passes a
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
/// A trait for a type that wraps around an
/// [`OutputHandle`](`dbsp::OutputHandle`) and yields output batches produced by
/// the circuit as [`SerBatch`]s.
pub trait SerCollectionHandle: Send + Sync {
    /// Like [`OutputHandle::take_from_worker`](`dbsp::OutputHandle::take_from_worker`),
    ///  but returns output batch as a [`SerBatch`] trait object.
    fn take_from_worker(&self, worker: usize) -> Option<Box<dyn SerBatch>>;

    /// Like [`OutputHandle::take_from_all`](`dbsp::OutputHandle::take_from_all`),
    /// but returns output batches as [`SerBatch`] trait objects.
    fn take_from_all(&self) -> Vec<Arc<dyn SerBatch>>;

    /// Like [`OutputHandle::consolidate`](`dbsp::OutputHandle::consolidate`),
    /// but returns the output batch as a [`SerBatch`] trait object.
    fn consolidate(&self) -> Box<dyn SerBatch>;

    /// Returns an alias to `self`.
    fn fork(&self) -> Box<dyn SerCollectionHandle>;
}

/// Cursor that iterates over deletions before insertions.
///
/// Most consumers don't understand Z-sets and expect a stream of upserts
/// instead, which means that the order of updates matters. For a table
/// with a primary key or unique constraint we must delete an existing record
/// before creating a new one with the same key.  DBSP may not know about
/// these constraints, so the safe thing to do is to output deletions before
/// insertions.  This cursor helps by iterating over all deletions in
/// the batch before insertions.
pub struct CursorWithPolarity<'a> {
    cursor: Box<dyn SerCursor + 'a>,
    second_pass: bool,
}

impl<'a> CursorWithPolarity<'a> {
    pub fn new(cursor: Box<dyn SerCursor + 'a>) -> Self {
        let mut result = Self {
            cursor,
            second_pass: false,
        };

        if result.key_valid() {
            result.advance_val();
        }

        result
    }

    fn advance_val(&mut self) {
        while self.cursor.val_valid()
            && ((!self.second_pass && self.cursor.weight() >= 0)
                || (self.second_pass && self.cursor.weight() <= 0))
        {
            self.step_val();
        }
    }
}

impl<'a> SerCursor for CursorWithPolarity<'a> {
    fn key_valid(&self) -> bool {
        self.cursor.key_valid()
    }

    fn val_valid(&self) -> bool {
        self.cursor.val_valid()
    }

    fn serialize_key(&mut self, dst: &mut Vec<u8>) -> AnyResult<()> {
        self.cursor.serialize_key(dst)
    }

    fn serialize_key_weight(&mut self, dst: &mut Vec<u8>) -> AnyResult<()> {
        self.cursor.serialize_key_weight(dst)
    }

    fn serialize_val(&mut self, dst: &mut Vec<u8>) -> AnyResult<()> {
        self.cursor.serialize_val(dst)
    }

    fn weight(&mut self) -> i64 {
        self.cursor.weight()
    }

    fn step_key(&mut self) {
        self.cursor.step_key();
        if !self.cursor.key_valid() && !self.second_pass {
            self.cursor.rewind_keys();
            self.second_pass = true;
        }

        if self.cursor.key_valid() {
            self.advance_val();
        }
    }

    fn step_val(&mut self) {
        self.cursor.step_val();
        self.advance_val();
    }

    fn rewind_keys(&mut self) {
        self.cursor.rewind_keys();
        self.second_pass = false;
        if self.cursor.key_valid() {
            self.advance_val();
        }
    }

    fn rewind_vals(&mut self) {
        self.cursor.rewind_vals();
        self.advance_val();
    }
}

/// A catalog of input and output stream handles of a circuit.
pub trait CircuitCatalog: Send {
    /// Look up an input stream handle by name.
    fn input_collection_handle(&self, name: &str) -> Option<&dyn DeCollectionHandle>;

    /// Look up output stream handles by name.
    fn output_handles(&self, name: &str) -> Option<&OutputCollectionHandles>;

    /// Look up output query handles by stream name and query type.
    fn output_query_handles(&self, name: &str, query: OutputQuery) -> Option<OutputQueryHandles> {
        self.output_handles(name).map(|handles| match query {
            OutputQuery::Table => OutputQueryHandles {
                delta: Some(handles.delta_handle.fork()),
                snapshot: None,
            },
            OutputQuery::Neighborhood => OutputQueryHandles {
                delta: handles
                    .neighborhood_handle
                    .as_ref()
                    .map(|handle| handle.fork()),
                snapshot: handles
                    .neighborhood_snapshot_handle
                    .as_ref()
                    .map(|handle| handle.fork()),
            },
            OutputQuery::Quantiles => OutputQueryHandles {
                delta: None,
                snapshot: handles
                    .quantiles_handle
                    .as_ref()
                    .map(|handle| handle.fork()),
            },
        })
    }
}

/// Circuit catalog implementation.
// For now, we use a common `CircuitCatalog` implementation for JIT and
// statically compiled circuits (with each module adding its own impls
// to this type).
pub struct Catalog {
    pub(crate) input_collection_handles: BTreeMap<String, Box<dyn DeCollectionHandle>>,
    pub(crate) output_batch_handles: BTreeMap<String, OutputCollectionHandles>,
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

    pub fn register_input_collection_handle<H>(&mut self, name: &str, handle: H)
    where
        H: DeCollectionHandle + 'static,
    {
        self.input_collection_handles
            .insert(name.to_owned(), Box::new(handle));
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
    pub neighborhood_descr_handle: Option<Box<dyn DeScalarHandle>>,

    /// A stream of changes to the neighborhood, computed using the
    /// [`dbsp::Stream::neighborhood`] operator.
    pub neighborhood_handle: Option<Box<dyn SerCollectionHandle>>,

    /// A stream that contains the full snapshot of the neighborhood.  Only
    /// produces an output whenever the `neighborhood_descr_handle` input is
    /// set to `Some(..)`.
    pub neighborhood_snapshot_handle: Option<Box<dyn SerCollectionHandle>>,

    /// Input stream used to submit the quantiles query.
    ///
    /// The value in the stream specifies the number of quantiles to
    /// output.  When  greater than zero, it triggers the quantiles
    /// computation.  The result is output to the
    /// `quantiles_handle` stream at the
    /// end of the current clock cycle.
    pub num_quantiles_handle: Option<InputHandle<usize>>,

    /// Quantiles stream.
    ///
    /// When the `num_quantiles_handle` input is set to `N`, `N>0`, this stream
    /// outputs up to `N` quantiles of the input collection, computed using
    /// the [`dbsp::Stream::stream_key_quantiles`] operator.
    pub quantiles_handle: Option<Box<dyn SerCollectionHandle>>,
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
