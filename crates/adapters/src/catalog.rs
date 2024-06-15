use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::{collections::BTreeMap, sync::Arc};

use crate::{static_compile::DeScalarHandle, ControllerError};
use anyhow::Result as AnyResult;
#[cfg(feature = "with-avro")]
use apache_avro::{types::Value as AvroValue, Schema as AvroSchema};
use arrow::record_batch::RecordBatch;
use dbsp::{utils::Tup2, InputHandle};
use pipeline_types::format::json::JsonFlavor;
use pipeline_types::program_schema::canonical_identifier;
use pipeline_types::program_schema::Relation;
use pipeline_types::query::OutputQuery;
use pipeline_types::serde_with_context::SqlSerdeConfig;
use pipeline_types::serialize_struct;
use serde::{Deserialize, Serialize};
use serde_arrow::schema::SerdeArrowSchema;
use serde_arrow::ArrayBuilder;

/// Descriptor that specifies the format in which records are received
/// or into which they should be encoded before sending.
#[derive(Clone, PartialEq)]
pub enum RecordFormat {
    // TODO: Support different JSON encodings:
    // * Map - the default encoding
    // * Array - allow the subset and the order of columns to be configurable
    // * Raw - Only applicable to single-column tables.  Input records contain
    // raw encoding of this column only.  This is particularly useful for
    // tables that store raw JSON or binary data to be parsed using SQL.
    Json(JsonFlavor),
    Csv,
    Parquet(SerdeArrowSchema),
    #[cfg(feature = "with-avro")]
    Avro,
}

// Helper type only used to serialize neighborhoods as a map vs tuple.
#[derive(Deserialize, Serialize, Debug)]
pub struct NeighborhoodEntry<KD> {
    index: i64,
    key: KD,
}

serialize_struct!(NeighborhoodEntry(KD)[2]{
    index["index"]: isize,
    key["key"]: KD
});

impl<K, KD> From<Tup2<i64, Tup2<K, ()>>> for NeighborhoodEntry<KD>
where
    KD: From<K>,
{
    fn from(Tup2(index, Tup2(key, ())): Tup2<i64, Tup2<K, ()>>) -> Self {
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
/// [`ZSetHandle`](`dbsp::ZSetHandle`) or
/// an [`MapHandle`](`dbsp::MapHandle`) and pushes serialized relational
/// data to the associated input stream record-by-record.  The client passes a
/// byte array with a serialized data record (e.g., in JSON or CSV format)
/// to [`insert`](`Self::insert`), [`delete`](`Self::delete`), and
/// [`update`](`Self::update`) methods. The record gets deserialized into the
/// strongly typed representation expected by the input stream and gets buffered
/// inside the handle. The [`flush`](`Self::flush`) method pushes all buffered
/// data to the underlying [`ZSetHandle`](`dbsp::ZSetHandle`) or
/// [`MapHandle`](`dbsp::MapHandle`).
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

    /// Buffer a new update that will modify an existing record.
    ///
    /// This method can only be called on streams created with
    /// [`RootCircuit::add_input_map`](`dbsp::RootCircuit::add_input_map`)
    /// and will fail on other streams.  The serialized record must match
    /// the update type of this stream, specified as a type argument to
    /// [`Catalog::register_input_map`].
    fn update(&mut self, data: &[u8]) -> AnyResult<()>;

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

pub trait ArrowStream: Send {
    fn insert(&mut self, data: &RecordBatch) -> AnyResult<()>;

    fn delete(&mut self, data: &RecordBatch) -> AnyResult<()>;

    /// Create a new deserializer with the same configuration connected to
    /// the same input stream.
    fn fork(&self) -> Box<dyn ArrowStream>;
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

    fn configure_arrow_deserializer(
        &self,
        config: SqlSerdeConfig,
    ) -> Result<Box<dyn ArrowStream>, ControllerError>;
}

/// A type-erased batch whose contents can be serialized.
///
/// This is a wrapper around the DBSP `BatchReader` trait that returns a cursor that
/// yields `erased_serde::Serialize` trait objects that can be used to serialize
/// the contents of the batch without knowing its key and value types.
// The reason we need the `Sync` trait below is so that we can wrap batches
// in `Arc` and send the same batch to multiple output endpoint threads.
pub trait SerBatchReader: 'static {
    /// Number of keys in the batch.
    fn key_count(&self) -> usize;

    /// Number of tuples in the batch.
    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Create a cursor over the batch that yields records
    /// formatted using the specified format.
    fn cursor<'a>(
        &'a self,
        record_format: RecordFormat,
    ) -> Result<Box<dyn SerCursor + 'a>, ControllerError>;
}

impl Debug for dyn SerBatchReader {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut cursor = self
            .cursor(RecordFormat::Json(Default::default()))
            .map_err(|_| std::fmt::Error)?;
        let mut key = Vec::new();
        let mut val = Vec::new();
        while cursor.key_valid() {
            cursor
                .serialize_key(&mut key)
                .map_err(|_| std::fmt::Error)?;
            write!(f, "{}=>{{", String::from_utf8_lossy(&key))?;

            while cursor.val_valid() {
                cursor
                    .serialize_val(&mut val)
                    .map_err(|_| std::fmt::Error)?;
                write!(
                    f,
                    "{}=>{}, ",
                    String::from_utf8_lossy(&val),
                    cursor.weight()
                )?;

                val.clear();
                cursor.step_val();
            }

            write!(f, "}}, ")?;
            key.clear();
            cursor.step_key();
        }

        Ok(())
    }
}

impl Debug for dyn SerBatch {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.as_batch_reader().fmt(f)
    }
}

/// A type-erased `Batch`.
pub trait SerBatch: SerBatchReader + Send + Sync {
    /// Convert to `Arc<Any>`, which can then be downcast to a reference
    /// to a concrete batch type.
    fn as_any(self: Arc<Self>) -> Arc<dyn Any + Sync + Send>;

    /// Merge `self` with all batches in `other`.
    fn merge(self: Arc<Self>, other: Vec<Arc<dyn SerBatch>>) -> Arc<dyn SerBatch>;

    /// Convert batch into a trace with identical contents.
    fn into_trace(self: Arc<Self>) -> Box<dyn SerTrace>;

    fn as_batch_reader(&self) -> &dyn SerBatchReader;
}

/// A type-erased `Trace`.
pub trait SerTrace: SerBatchReader {
    /// Insert a batch into the trace.
    fn insert(&mut self, batch: Arc<dyn SerBatch>);

    fn as_batch_reader(&self) -> &dyn SerBatchReader;
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

    /// Serialize current key into arrow format. Panics if invalid.
    fn serialize_key_to_arrow(&mut self, dst: &mut ArrayBuilder) -> AnyResult<()>;

    #[cfg(feature = "with-avro")]
    /// Convert current key to an Avro value.
    fn key_to_avro(&mut self, schema: &AvroSchema) -> AnyResult<AvroValue>;

    /// Serialize the `(key, weight)` tuple.
    ///
    /// FIXME: This only exists to support the CSV serializer, which outputs
    /// key and weight in the same CSV record.
    fn serialize_key_weight(&mut self, dst: &mut Vec<u8>) -> AnyResult<()>;

    /// Serialize current value. Panics if invalid.
    fn serialize_val(&mut self, dst: &mut Vec<u8>) -> AnyResult<()>;

    #[cfg(feature = "with-avro")]
    /// Convert current value to Avro.
    fn val_to_avro(&mut self, schema: &AvroSchema) -> AnyResult<AvroValue>;

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
    /// See [`OutputHandle::num_nonempty_mailboxes`](`dbsp::OutputHandle::num_nonempty_mailboxes`)
    fn num_nonempty_mailboxes(&self) -> usize;

    /// Like [`OutputHandle::take_from_worker`](`dbsp::OutputHandle::take_from_worker`),
    /// but returns output batch as a [`SerBatch`] trait object.
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

    #[cfg(feature = "with-avro")]
    fn key_to_avro(&mut self, schema: &AvroSchema) -> AnyResult<AvroValue> {
        self.cursor.key_to_avro(schema)
    }

    fn serialize_key_weight(&mut self, dst: &mut Vec<u8>) -> AnyResult<()> {
        self.cursor.serialize_key_weight(dst)
    }

    fn serialize_key_to_arrow(&mut self, dst: &mut ArrayBuilder) -> AnyResult<()> {
        self.cursor.serialize_key_to_arrow(dst)
    }

    fn serialize_val(&mut self, dst: &mut Vec<u8>) -> AnyResult<()> {
        self.cursor.serialize_val(dst)
    }

    #[cfg(feature = "with-avro")]
    fn val_to_avro(&mut self, schema: &AvroSchema) -> AnyResult<AvroValue> {
        self.cursor.val_to_avro(schema)
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
    fn input_collection_handle(&self, name: &str) -> Option<&InputCollectionHandle>;

    /// Look up output stream handles by name.
    fn output_handles(&self, name: &str) -> Option<&OutputCollectionHandles>;

    /// Look up output query handles by stream name and query type.
    fn output_query_handles(&self, name: &str, query: OutputQuery) -> Option<OutputQueryHandles> {
        self.output_handles(name).map(|handles| match query {
            OutputQuery::Table => OutputQueryHandles {
                schema: handles.schema.clone(),
                delta: Some(handles.delta_handle.fork()),
                snapshot: None,
            },
            OutputQuery::Neighborhood => OutputQueryHandles {
                schema: handles.schema.clone(),
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
                schema: handles.schema.clone(),
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
pub struct Catalog {
    input_collection_handles: BTreeMap<String, InputCollectionHandle>,
    output_batch_handles: BTreeMap<String, OutputCollectionHandles>,
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
        let name = handle.schema.name();
        if self.input_collection_handles.contains_key(&name) {
            return Err(ControllerError::duplicate_input_stream(&name));
        }
        self.input_collection_handles.insert(name, handle);

        Ok(())
    }

    pub fn register_output_batch_handles(
        &mut self,
        handles: OutputCollectionHandles,
    ) -> Result<(), ControllerError> {
        let name = handles.schema.name();
        if self.output_batch_handles.contains_key(&name) {
            return Err(ControllerError::duplicate_output_stream(&name));
        }
        self.output_batch_handles.insert(name, handles);

        Ok(())
    }
}

impl CircuitCatalog for Catalog {
    /// Look up an input stream handle by name.
    fn input_collection_handle(&self, name: &str) -> Option<&InputCollectionHandle> {
        self.input_collection_handles
            .get(&canonical_identifier(name))
    }

    /// Look up output stream handles by name.
    fn output_handles(&self, name: &str) -> Option<&OutputCollectionHandles> {
        self.output_batch_handles.get(&canonical_identifier(name))
    }
}

pub struct InputCollectionHandle {
    pub schema: Relation,
    pub handle: Box<dyn DeCollectionHandle>,
}

impl InputCollectionHandle {
    pub fn new<H>(schema: Relation, handle: H) -> Self
    where
        H: DeCollectionHandle + 'static,
    {
        Self {
            schema,
            handle: Box::new(handle),
        }
    }
}

/// A set of stream handles associated with each output collection.
pub struct OutputCollectionHandles {
    pub schema: Relation,

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
    pub schema: Relation,
    pub delta: Option<Box<dyn SerCollectionHandle>>,
    pub snapshot: Option<Box<dyn SerCollectionHandle>>,
}
