use std::any::Any;
use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use anyhow::Result as AnyResult;
#[cfg(feature = "with-avro")]
use apache_avro::{schema::NamesRef, types::Value as AvroValue, Schema as AvroSchema};
use arrow::record_batch::RecordBatch;
use dbsp::circuit::NodeId;
use dbsp::operator::StagedBuffers;
use dyn_clone::DynClone;
use feldera_types::format::csv::CsvParserConfig;
use feldera_types::format::json::JsonFlavor;
use feldera_types::program_schema::{Relation, SqlIdentifier};
use feldera_types::serde_with_context::SqlSerdeConfig;
use serde_arrow::ArrayBuilder;

use crate::errors::controller::ControllerError;
use crate::format::InputBuffer;

/// Descriptor that specifies the format in which records are received
/// or into which they should be encoded before sending.
#[derive(Clone)]
pub enum RecordFormat {
    // TODO: Support different JSON encodings:
    // * Map - the default encoding
    // * Array - allow the subset and the order of columns to be configurable
    // * Raw - Only applicable to single-column tables.  Input records contain
    // raw encoding of this column only.  This is particularly useful for
    // tables that store raw JSON or binary data to be parsed using SQL.
    Json(JsonFlavor),
    Csv(CsvParserConfig),
    Parquet(SqlSerdeConfig),
    #[cfg(feature = "with-avro")]
    Avro,
    Raw,
}

/// An input handle that deserializes and buffers records.
///
/// A trait for a type that wraps a [`ZSetHandle`](`dbsp::ZSetHandle`) or an
/// [`MapHandle`](`dbsp::MapHandle`) and collects serialized relational data for
/// the associated input stream.  The client passes a byte array with a
/// serialized data record (e.g., in JSON or CSV format) to
/// [`insert`](`Self::insert`), [`delete`](`Self::delete`), and
/// [`update`](`Self::update`) methods. The record gets deserialized into the
/// strongly typed representation expected by the input stream.
///
/// Instances of this trait are created by calling
/// [`DeCollectionHandle::configure_deserializer`].
/// The data format accepted by the handle is determined
/// by the `record_format` argument passed to this method.
///
/// The input handle internally buffers the deserialized records. Use the
/// `InputBuffer` supertrait to push them to the circuit or extract them for
/// later use.
pub trait DeCollectionStream: Send + Sync + InputBuffer {
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
    /// `Catalog::register_input_map`.
    fn update(&mut self, data: &[u8]) -> AnyResult<()>;

    /// Reserve space for at least `reservation` more updates in the
    /// internal input buffer.
    ///
    /// Reservations are not required but can be used when the number
    /// of inputs is known ahead of time to reduce reallocations.
    fn reserve(&mut self, reservation: usize);

    /// Removes any updates beyond the first `len`.
    fn truncate(&mut self, len: usize);

    /// Stages all of the `buffers`, which must have been obtained from a
    /// [Parser] for this stream, into a [StagedBuffers] that may later be used
    /// to push the collected data into the circuit.  See [StagedBuffers] for
    /// more information.
    ///
    /// [Parser]: crate::format::Parser
    fn stage(&self, buffers: Vec<Box<dyn InputBuffer>>) -> Box<dyn StagedBuffers>;

    /// Create a new deserializer with the same configuration connected to the
    /// same input stream. The new deserializer has an independent buffer that
    /// is initially empty.
    fn fork(&self) -> Box<dyn DeCollectionStream>;
}

/// Like `DeCollectionStream`, but deserializes Arrow-encoded records before pushing them to a
/// stream.
pub trait ArrowStream: InputBuffer + Send + Sync {
    fn insert(&mut self, data: &RecordBatch) -> AnyResult<()>;

    fn delete(&mut self, data: &RecordBatch) -> AnyResult<()>;

    /// Insert records in `data` with polarities from the `polarities` array.
    ///
    /// `polarities` must be the same length as `data`.
    fn insert_with_polarities(&mut self, data: &RecordBatch, polarities: &[bool]) -> AnyResult<()>;

    /// Create a new deserializer with the same configuration connected to
    /// the same input stream.
    fn fork(&self) -> Box<dyn ArrowStream>;

    /// Stages all of the `buffers`, which must have been obtained from a
    /// [Parser] for this stream, into a [StagedBuffers] that may later be used
    /// to push the collected data into the circuit.  See [StagedBuffers] for
    /// more information.
    ///
    /// [Parser]: crate::format::Parser
    fn stage(&self, buffers: Vec<Box<dyn InputBuffer>>) -> Box<dyn StagedBuffers>;
}

/// Like `DeCollectionStream`, but deserializes Avro-encoded records before pushing them to a
/// stream.
#[cfg(feature = "with-avro")]
pub trait AvroStream: InputBuffer + Send + Sync {
    fn insert(&mut self, data: &AvroValue, schema: &AvroSchema, n_bytes: usize) -> AnyResult<()>;

    fn delete(&mut self, data: &AvroValue, schema: &AvroSchema, n_bytes: usize) -> AnyResult<()>;

    /// Create a new deserializer with the same configuration connected to
    /// the same input stream.
    fn fork(&self) -> Box<dyn AvroStream>;

    /// Stages all of the `buffers`, which must have been obtained from a
    /// [Parser] for this stream, into a [StagedBuffers] that may later be used
    /// to push the collected data into the circuit.  See [StagedBuffers] for
    /// more information.
    ///
    /// [Parser]: crate::format::Parser
    fn stage(&self, buffers: Vec<Box<dyn InputBuffer>>) -> Box<dyn StagedBuffers>;
}

/// A handle to an input collection that can be used to feed serialized data
/// to the collection.
pub trait DeCollectionHandle: Send + Sync {
    /// Create a [`DeCollectionStream`] object to parse input data encoded
    /// using the format specified in `RecordFormat`.
    fn configure_deserializer(
        &self,
        record_format: RecordFormat,
    ) -> Result<Box<dyn DeCollectionStream>, ControllerError>;

    /// Create an `ArrowStream` object to parse Arrow-encoded input data.
    fn configure_arrow_deserializer(
        &self,
        config: SqlSerdeConfig,
    ) -> Result<Box<dyn ArrowStream>, ControllerError>;

    /// Create an `AvroStream` object to parse Avro-encoded input data.
    #[cfg(feature = "with-avro")]
    fn configure_avro_deserializer(&self) -> Result<Box<dyn AvroStream>, ControllerError>;

    fn fork(&self) -> Box<dyn DeCollectionHandle>;
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
    ) -> Result<Box<dyn SerCursor + Send + 'a>, ControllerError>;

    /// Returns all batches in this reader.
    ///
    /// A reader can wrap a single batch or a spine or a spine snapshot. This method extracts
    /// all batches from the reader.
    fn batches(&self) -> Vec<Arc<dyn SerBatch>>;

    fn snapshot(&self) -> Arc<dyn SerBatchReader>;
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

pub trait SyncSerBatchReader: SerBatchReader + Send + Sync {}

/// A type-erased `Batch`.
pub trait SerBatch: SyncSerBatchReader {
    /// Convert to `Arc<Any>`, which can then be downcast to a reference
    /// to a concrete batch type.
    fn as_any(self: Arc<Self>) -> Arc<dyn Any + Sync + Send>;

    /// Merge `self` with all batches in `other`.
    fn merge(self: Arc<Self>, other: Vec<Arc<dyn SerBatch>>) -> Arc<dyn SerBatch>;

    fn as_batch_reader(&self) -> &dyn SerBatchReader;

    /// Convert batch into a trace with identical contents.
    fn into_trace(self: Arc<Self>) -> Box<dyn SerTrace>;
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
pub trait SerCursor: Send {
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

    /// Convert key to JSON. Used for error reporting to generate a human-readable
    /// representation of the key.
    fn key_to_json(&mut self) -> AnyResult<serde_json::Value>;

    /// Like `serialize_key`, but only serializes the specified fields of the key.
    fn serialize_key_fields(
        &mut self,
        fields: &HashSet<String>,
        dst: &mut Vec<u8>,
    ) -> AnyResult<()>;

    /// Serialize current key into arrow format. Panics if invalid.
    fn serialize_key_to_arrow(&mut self, dst: &mut ArrayBuilder) -> AnyResult<()>;

    /// Serialize current key into arrow format, adding additional metadata columns.
    /// `metadata` must be a struct or a map.
    fn serialize_key_to_arrow_with_metadata(
        &mut self,
        metadata: &dyn erased_serde::Serialize,
        dst: &mut ArrayBuilder,
    ) -> AnyResult<()>;

    /// Serialize current value into arrow format. Panics if invalid.
    fn serialize_val_to_arrow(&mut self, dst: &mut ArrayBuilder) -> AnyResult<()>;

    /// Serialize current value into arrow format, adding additional metadata columns.
    /// `metadata` must be a struct or a map.
    fn serialize_val_to_arrow_with_metadata(
        &mut self,
        metadata: &dyn erased_serde::Serialize,
        dst: &mut ArrayBuilder,
    ) -> AnyResult<()>;

    #[cfg(feature = "with-avro")]
    /// Convert current key to an Avro value.
    fn key_to_avro(&mut self, schema: &AvroSchema, refs: &NamesRef<'_>) -> AnyResult<AvroValue>;

    /// Serialize the `(key, weight)` tuple.
    ///
    /// FIXME: This only exists to support the CSV serializer, which outputs
    /// key and weight in the same CSV record.
    fn serialize_key_weight(&mut self, dst: &mut Vec<u8>) -> AnyResult<()>;

    /// Serialize current value. Panics if invalid.
    fn serialize_val(&mut self, dst: &mut Vec<u8>) -> AnyResult<()>;

    /// Convert value to JSON. Used for error reporting to generate a human-readable
    /// representation of the value.
    fn val_to_json(&mut self) -> AnyResult<serde_json::Value>;

    #[cfg(feature = "with-avro")]
    /// Convert current value to Avro.
    fn val_to_avro(&mut self, schema: &AvroSchema, refs: &NamesRef<'_>) -> AnyResult<AvroValue>;

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

    fn count_keys(&mut self) -> usize {
        let mut count = 0;

        while self.key_valid() {
            count += 1;
            self.step_key()
        }

        count
    }
}

/// A handle to an output stream of a circuit that yields type-erased
/// read-only batches.
///
/// A trait for a type that wraps around an
/// [`OutputHandle`](`dbsp::OutputHandle`) and yields output batches produced by
/// the circuit as [`SerBatchReader`]s.
pub trait SerBatchReaderHandle: Send + Sync + DynClone {
    /// See [`OutputHandle::num_nonempty_mailboxes`](`dbsp::OutputHandle::num_nonempty_mailboxes`)
    fn num_nonempty_mailboxes(&self) -> usize;

    /// Like [`OutputHandle::take_from_worker`](`dbsp::OutputHandle::take_from_worker`),
    /// but returns output batch as a [`SyncSerBatchReader`] trait object.
    fn take_from_worker(&self, worker: usize) -> Option<Box<dyn SyncSerBatchReader>>;

    /// Like [`OutputHandle::take_from_all`](`dbsp::OutputHandle::take_from_all`),
    /// but returns output batches as [`SyncSerBatchReader`] trait objects.
    fn take_from_all(&self) -> Vec<Arc<dyn SyncSerBatchReader>>;

    /// Concatenate outputs from all workers into a single batch reader.
    fn concat(&self) -> Arc<dyn SyncSerBatchReader>;
}

dyn_clone::clone_trait_object!(SerBatchReaderHandle);

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

impl SerCursor for CursorWithPolarity<'_> {
    fn key_valid(&self) -> bool {
        self.cursor.key_valid()
    }

    fn val_valid(&self) -> bool {
        self.cursor.val_valid()
    }

    fn serialize_key(&mut self, dst: &mut Vec<u8>) -> AnyResult<()> {
        self.cursor.serialize_key(dst)
    }

    fn key_to_json(&mut self) -> AnyResult<serde_json::Value> {
        self.cursor.key_to_json()
    }

    fn serialize_key_fields(
        &mut self,
        fields: &HashSet<String>,
        dst: &mut Vec<u8>,
    ) -> AnyResult<()> {
        self.cursor.serialize_key_fields(fields, dst)
    }

    #[cfg(feature = "with-avro")]
    fn key_to_avro(&mut self, schema: &AvroSchema, refs: &NamesRef<'_>) -> AnyResult<AvroValue> {
        self.cursor.key_to_avro(schema, refs)
    }

    fn serialize_key_weight(&mut self, dst: &mut Vec<u8>) -> AnyResult<()> {
        self.cursor.serialize_key_weight(dst)
    }

    fn serialize_key_to_arrow(&mut self, dst: &mut ArrayBuilder) -> AnyResult<()> {
        self.cursor.serialize_key_to_arrow(dst)
    }

    fn serialize_key_to_arrow_with_metadata(
        &mut self,
        metadata: &dyn erased_serde::Serialize,
        dst: &mut ArrayBuilder,
    ) -> AnyResult<()> {
        self.cursor
            .serialize_key_to_arrow_with_metadata(metadata, dst)
    }

    fn serialize_val_to_arrow(&mut self, dst: &mut ArrayBuilder) -> AnyResult<()> {
        self.cursor.serialize_val_to_arrow(dst)
    }

    fn serialize_val_to_arrow_with_metadata(
        &mut self,
        metadata: &dyn erased_serde::Serialize,
        dst: &mut ArrayBuilder,
    ) -> AnyResult<()> {
        self.cursor
            .serialize_val_to_arrow_with_metadata(metadata, dst)
    }

    fn serialize_val(&mut self, dst: &mut Vec<u8>) -> AnyResult<()> {
        self.cursor.serialize_val(dst)
    }

    fn val_to_json(&mut self) -> AnyResult<serde_json::Value> {
        self.cursor.val_to_json()
    }

    #[cfg(feature = "with-avro")]
    fn val_to_avro(&mut self, schema: &AvroSchema, refs: &NamesRef<'_>) -> AnyResult<AvroValue> {
        self.cursor.val_to_avro(schema, refs)
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
pub trait CircuitCatalog: Send + Sync {
    /// Look up an input stream handle by name.
    fn input_collection_handle(&self, name: &SqlIdentifier) -> Option<&InputCollectionHandle>;

    fn output_iter(
        &self,
    ) -> Box<dyn Iterator<Item = (&SqlIdentifier, &OutputCollectionHandles)> + '_>;

    /// Look up output stream handles by name.
    fn output_handles(&self, name: &SqlIdentifier) -> Option<&OutputCollectionHandles>;

    fn output_handles_mut(&mut self, name: &SqlIdentifier) -> Option<&mut OutputCollectionHandles>;
}

pub struct InputCollectionHandle {
    pub schema: Relation,
    pub handle: Box<dyn DeCollectionHandle>,

    /// Node id of the input stream in the circuit.
    ///
    /// Used to check whether the input stream needs to be backfilled during bootstrapping,
    /// i.e., whether attached input connectors should be reset to their initial offsets or
    /// continue from the checkpointed offsets.
    pub node_id: NodeId,
}

impl InputCollectionHandle {
    pub fn new<H>(schema: Relation, handle: H, node_id: NodeId) -> Self
    where
        H: DeCollectionHandle + 'static,
    {
        Self {
            schema,
            handle: Box::new(handle),
            node_id,
        }
    }
}

/// A set of stream handles associated with each output collection.
#[derive(Clone)]
pub struct OutputCollectionHandles {
    pub key_schema: Option<Relation>,
    pub value_schema: Relation,

    pub index_of: Option<SqlIdentifier>,

    /// A handle to a snapshot of a materialized table/view.
    pub integrate_handle: Option<Arc<dyn SerBatchReaderHandle>>,

    /// A stream of changes to the collection.
    pub delta_handle: Box<dyn SerBatchReaderHandle>,
}
