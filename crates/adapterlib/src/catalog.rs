// The connector layer still builds record metadata as the enum `Variant`.
#![allow(deprecated)]

use std::any::Any;
#[cfg(any(feature = "with-avro", feature = "with-dynamodb"))]
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, Mutex};

use anyhow::Result as AnyResult;
#[cfg(feature = "with-avro")]
use apache_avro::{
    Schema as AvroSchema,
    schema::{Name as AvroName, NamesRef},
    types::Value as AvroValue,
};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
#[cfg(feature = "with-dynamodb")]
use aws_sdk_dynamodb::types::AttributeValue;
use dyn_clone::DynClone;
use feldera_types::circuit::{EnableCount, StagedBuffers};
use feldera_types::format::csv::CsvFormatConfig;
use feldera_types::format::json::JsonFlavor;
use feldera_types::program_schema::{Relation, SqlIdentifier};
use feldera_types::serde_with_context::SqlSerdeConfig;

use crate::ConnectorMetadata;
use crate::errors::controller::ControllerError;
use crate::format::InputBuffer;
use crate::postprocess::PostprocessorRegistry;
use crate::preprocess::PreprocessorRegistry;

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
    Csv(CsvFormatConfig),
    Parquet(SqlSerdeConfig),
    #[cfg(feature = "with-avro")]
    Avro,
    Raw(String),
    /// Output-only format for the DynamoDB connector.
    #[cfg(feature = "with-dynamodb")]
    DynamoDB,
}

/// An input handle that deserializes and buffers records.
///
/// A trait for a type that wraps an input stream of the engine's circuit, a Z-set
/// of records or a map from keys to records, and collects serialized relational
/// data for it.  The client passes a byte array with a
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
pub trait DeCollectionStream: Send + InputBuffer {
    /// Buffer a new insert update.
    ///
    /// `metadata` holds the attributes that the transport and the parser attached to the
    /// record, such as the Kafka topic and headers. Column DEFAULT expressions read them
    /// through `CONNECTOR_METADATA()`.
    ///
    /// Returns an error if deserialization fails, i.e., the serialized
    /// representation is corrupted or does not match the value type of
    /// the underlying input stream.
    fn insert(&mut self, data: &[u8], metadata: &Option<ConnectorMetadata>) -> AnyResult<()>;

    /// Buffer a new delete update.
    ///
    /// The `data` argument contains a serialized record whose
    /// type depends on the underlying input stream: a Z-set stream deletes by
    /// value, hence the serialized record must match the value type of the
    /// stream.  A map stream deletes by key, so the serialized record must
    /// match the key type of the stream.
    ///
    /// The record gets deserialized and pushed to the underlying input stream
    /// handle as a delete update.
    ///
    /// `metadata` holds the attributes that the transport and the parser attached to the
    /// record, such as the Kafka topic and headers. Column DEFAULT expressions read them
    /// through `CONNECTOR_METADATA()`.
    ///
    /// Returns an error if deserialization fails, i.e., the serialized
    /// representation is corrupted or does not match the value or key
    /// type of the underlying input stream.
    fn delete(&mut self, data: &[u8], metadata: &Option<ConnectorMetadata>) -> AnyResult<()>;

    /// Buffer a new update that will modify an existing record.
    ///
    /// `metadata` holds the attributes that the transport and the parser attached to the
    /// record, such as the Kafka topic and headers. Column DEFAULT expressions read them
    /// through `CONNECTOR_METADATA()`.
    ///
    /// This method can only be called on map streams and fails on other
    /// streams.  The serialized record must match the update type of the
    /// stream.
    fn update(&mut self, data: &[u8], metadata: &Option<ConnectorMetadata>) -> AnyResult<()>;

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
    /// Buffer a new batch of insert updates.
    ///
    /// `metadata` holds the attributes that the transport and the parser attached to the
    /// records of `data`, such as the Kafka topic and headers. Column DEFAULT expressions read
    /// them through `CONNECTOR_METADATA()`.
    fn insert(&mut self, data: &RecordBatch, metadata: &Option<ConnectorMetadata>)
    -> AnyResult<()>;

    /// Buffer a new batch of delete updates.
    ///
    /// `metadata` holds the attributes that the transport and the parser attached to the
    /// records of `data`, such as the Kafka topic and headers. Column DEFAULT expressions read
    /// them through `CONNECTOR_METADATA()`.
    fn delete(&mut self, data: &RecordBatch, metadata: &Option<ConnectorMetadata>)
    -> AnyResult<()>;

    /// Insert records in `data` with polarities from the `polarities` array.
    ///
    /// `polarities` must be the same length as `data`.
    fn insert_with_polarities(
        &mut self,
        data: &RecordBatch,
        polarities: &[bool],
        metadata: &Option<ConnectorMetadata>,
    ) -> AnyResult<()>;

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

#[cfg(feature = "with-avro")]
pub type AvroSchemaRefs = HashMap<AvroName, AvroSchema>;

/// Like `DeCollectionStream`, but deserializes Avro-encoded records before pushing them to a
/// stream.
#[cfg(feature = "with-avro")]
pub trait AvroStream: InputBuffer + Send {
    /// Installs the schema of the records that later calls push.
    ///
    /// The parser calls this before it pushes the first record, and again whenever the
    /// schema changes. A fork keeps the installed schema.
    ///
    /// # Arguments
    ///
    /// * `schema` - The schema of a record.
    /// * `refs` - The named schemas that `schema` references.
    fn configure_schema(&mut self, schema: &AvroSchema, refs: &AvroSchemaRefs) -> AnyResult<()>;

    /// Buffer a new insert update.
    ///
    /// Fails if no schema is installed or if `data` does not match it.
    ///
    /// # Arguments
    ///
    /// * `data` - The record, in the installed schema.
    /// * `n_bytes` - The size of the record in its message, for accounting.
    /// * `metadata` - The attributes that the transport and the parser attached to the
    ///   record, such as the Kafka topic and headers. Column DEFAULT expressions read them
    ///   through `CONNECTOR_METADATA()`.
    fn insert(
        &mut self,
        data: &AvroValue,
        n_bytes: usize,
        metadata: &Option<ConnectorMetadata>,
    ) -> AnyResult<()>;

    /// Buffer a new delete update, with the arguments of [`Self::insert`].
    fn delete(
        &mut self,
        data: &AvroValue,
        n_bytes: usize,
        metadata: &Option<ConnectorMetadata>,
    ) -> AnyResult<()>;

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
/// The batch hands out cursors that render its rows in a record format, so that the
/// output connectors can serialize its contents without knowing its key and value
/// types.
// The reason we need the `Sync` trait below is so that we can wrap batches
// in `Arc` and send the same batch to multiple output endpoint threads.
pub trait SerBatchReader: 'static + Send + Sync {
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

    /// Cuts the batch into at most `num_ranges` contiguous key ranges of about the same
    /// number of rows, whose cursors yield records in `record_format`, so that threads can
    /// encode the batch in parallel.
    ///
    /// Together the ranges hold every row of the batch once. Each range holds at least one
    /// key, so an empty batch has no ranges, and a batch with fewer keys than `num_ranges`
    /// has fewer ranges.
    fn split(
        self: Arc<Self>,
        num_ranges: usize,
        record_format: RecordFormat,
    ) -> AnyResult<Vec<Box<dyn SerKeyRange>>>;

    /// The number of rows that an ad-hoc query's scan of the batch puts in one Arrow batch.
    /// An engine whose rows convert to Arrow a column at a time takes larger batches.
    fn adhoc_batch_rows(&self) -> usize {
        256
    }
}

/// A contiguous range of the keys of a batch, which one thread reads on its own.
///
/// [`SerBatchReader::split`] cuts a batch into ranges.
pub trait SerKeyRange: Send + Sync {
    /// Creates a cursor over the rows of the range, in key order.
    fn cursor(&self) -> AnyResult<Box<dyn SerCursor + Send + '_>>;
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
            cursor.step_key().map_err(|_| std::fmt::Error)?;
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
pub trait SerBatch: SerBatchReader {
    /// Convert to `Arc<Any>`, which can then be downcast to a reference
    /// to a concrete batch type.
    fn as_any(self: Arc<Self>) -> Arc<dyn Any + Sync + Send>;

    /// Merge `self` with all batches in `other`.
    fn merge(self: Arc<Self>, other: Vec<Arc<dyn SerBatch>>) -> Arc<dyn SerBatch>;

    /// Concatenate `self.batches()` with `b.batches()` for all batches in `other`
    /// into a spine snapshot.
    fn concat(self: Arc<Self>, other: Vec<Arc<dyn SerBatch>>) -> Arc<dyn SerBatchReader>;

    fn as_batch_reader(&self) -> &dyn SerBatchReader;

    fn arc_as_batch_reader(self: Arc<Self>) -> Arc<dyn SerBatchReader>;

    /// Convert batch into a trace with identical contents.
    fn into_trace(self: Arc<Self>) -> Box<dyn SerTrace>;
}

/// A type-erased `Trace`.
pub trait SerTrace: SerBatchReader {
    /// Insert a batch into the trace.
    fn insert(&mut self, batch: Arc<dyn SerBatch>);

    fn insert_without_blocking(&mut self, batch: Arc<dyn SerBatch>) -> bool;

    fn backpressure_wait(&self);

    fn as_batch_reader(&self) -> &dyn SerBatchReader;
}

/// A wrapper around a `SerCursor` that makes an `IndexedZSet<K, V>` look like a `ZSet<V>`
/// by iterating over values only.
///
/// * Doesn't return values in order.
/// * Panics when any operations over values are attempted.
pub struct SerCursorFlattened<'a> {
    val_valid: bool,
    cursor: Box<dyn SerCursor + 'a>,
}

impl<'a> SerCursorFlattened<'a> {
    pub fn new(cursor: Box<dyn SerCursor + 'a>) -> Self {
        Self {
            cursor,
            val_valid: true,
        }
    }
}

impl<'a> SerCursor for SerCursorFlattened<'a> {
    fn key_valid(&self) -> bool {
        self.cursor.key_valid() && self.cursor.val_valid()
    }

    fn val_valid(&self) -> bool {
        self.val_valid
    }

    fn serialize_key(&mut self, dst: &mut Vec<u8>) -> AnyResult<()> {
        self.cursor.serialize_val(dst)
    }

    fn key_to_json(&mut self) -> AnyResult<serde_json::Value> {
        self.cursor.val_to_json()
    }

    #[cfg(feature = "with-dynamodb")]
    fn key_to_dynamodb_item(&mut self) -> AnyResult<HashMap<String, AttributeValue>> {
        self.cursor.val_to_dynamodb_item()
    }

    fn serialize_key_fields(
        &mut self,
        fields: &HashSet<String>,
        dst: &mut Vec<u8>,
    ) -> AnyResult<()> {
        self.cursor.serialize_val_fields(fields, dst)
    }

    fn serialize_val_fields(
        &mut self,
        _fields: &HashSet<String>,
        _dst: &mut Vec<u8>,
    ) -> AnyResult<()> {
        panic!("serialize_val_fields is not supported for flattened cursors");
    }

    #[cfg(feature = "with-avro")]
    fn serialize_key_avro(
        &mut self,
        schema: &AvroSchema,
        refs: &NamesRef<'_>,
        dst: &mut Vec<u8>,
    ) -> AnyResult<()> {
        self.cursor.serialize_val_avro(schema, refs, dst)
    }

    fn arrow_sink(&self, schema: SchemaRef) -> AnyResult<Box<dyn ArrowRowSink>> {
        self.cursor.arrow_sink(schema)
    }

    fn append_key_to_arrow(&mut self, sink: &mut dyn ArrowRowSink) -> AnyResult<()> {
        self.cursor.append_val_to_arrow(sink)
    }

    fn append_val_to_arrow(&mut self, _sink: &mut dyn ArrowRowSink) -> AnyResult<()> {
        panic!("append_val_to_arrow is not supported for flattened cursors");
    }

    fn serialize_key_weight(&mut self, dst: &mut Vec<u8>) -> AnyResult<()> {
        self.cursor.serialize_val_weight(dst)
    }

    fn serialize_val_weight(&mut self, _dst: &mut Vec<u8>) -> AnyResult<()> {
        panic!("serialize_val_weight is not supported for flattened cursors");
    }

    fn serialize_val(&mut self, _dst: &mut Vec<u8>) -> AnyResult<()> {
        panic!("serialize_val is not supported for flattened cursors");
    }

    fn val_to_json(&mut self) -> AnyResult<serde_json::Value> {
        panic!("val_to_json is not supported for flattened cursors");
    }

    #[cfg(feature = "with-dynamodb")]
    fn val_to_dynamodb_item(&mut self) -> AnyResult<HashMap<String, AttributeValue>> {
        panic!("val_to_dynamodb_item is not supported for flattened cursors");
    }

    #[cfg(feature = "with-avro")]
    fn serialize_val_avro(
        &mut self,
        _schema: &AvroSchema,
        _refs: &NamesRef<'_>,
        _dst: &mut Vec<u8>,
    ) -> AnyResult<()> {
        panic!("serialize_val_avro is not supported for flattened cursors");
    }

    fn weight(&mut self) -> i64 {
        self.cursor.weight()
    }

    fn step_key(&mut self) -> AnyResult<()> {
        debug_assert!(self.cursor.key_valid() && self.cursor.val_valid());
        self.cursor.step_val();
        while !self.cursor.val_valid() && self.cursor.key_valid() {
            self.cursor.step_key()?;
        }
        self.val_valid = true;
        Ok(())
    }

    fn step_val(&mut self) {
        self.val_valid = false;
    }

    fn rewind_keys(&mut self) -> AnyResult<()> {
        self.cursor.rewind_keys()?;
        self.val_valid = true;
        Ok(())
    }

    fn rewind_vals(&mut self) {
        self.val_valid = true;
    }
}

/// Rows that cursors append one at a time, which finish into an Arrow batch of the sink's
/// schema.
///
/// [`SerCursor::arrow_sink`] creates a sink; [`SerCursor::append_key_to_arrow`] and
/// [`SerCursor::append_val_to_arrow`] append the cursor's current row to it.
pub trait ArrowRowSink: Send {
    /// Returns the rows appended since the sink was created or last finished, as a batch,
    /// and leaves the sink empty.
    fn finish(&mut self) -> AnyResult<RecordBatch>;

    /// Returns the sink as `Any`, so that the engine that created it can reach its type.
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

/// Cursor that allows serializing the contents of a type-erased batch.
///
/// The cursor walks the keys of a batch and the values of each key, and renders the
/// current key or value in the record format that it was created with.
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

    /// Serialize current key to a DynamoDB item.
    #[cfg(feature = "with-dynamodb")]
    fn key_to_dynamodb_item(&mut self) -> AnyResult<HashMap<String, AttributeValue>>;

    /// Like `serialize_key`, but only serializes the specified fields of the key.
    fn serialize_key_fields(
        &mut self,
        fields: &HashSet<String>,
        dst: &mut Vec<u8>,
    ) -> AnyResult<()>;

    fn serialize_val_fields(
        &mut self,
        fields: &HashSet<String>,
        dst: &mut Vec<u8>,
    ) -> AnyResult<()>;

    /// Writes the current key to `dst` as a datum of the Avro `schema`. Panics if invalid.
    #[cfg(feature = "with-avro")]
    fn serialize_key_avro(
        &mut self,
        schema: &AvroSchema,
        refs: &NamesRef<'_>,
        dst: &mut Vec<u8>,
    ) -> AnyResult<()>;

    /// Creates a sink for rows of `schema`, the Arrow form of the key's or the value's
    /// columns, which [`Self::append_key_to_arrow`] and [`Self::append_val_to_arrow`] append
    /// to.
    fn arrow_sink(&self, schema: SchemaRef) -> AnyResult<Box<dyn ArrowRowSink>>;

    /// Appends the current key to `sink`, which a cursor of the same engine created. Panics
    /// if invalid.
    fn append_key_to_arrow(&mut self, sink: &mut dyn ArrowRowSink) -> AnyResult<()>;

    /// Appends the current value to `sink`, which a cursor of the same engine created.
    /// Panics if invalid.
    fn append_val_to_arrow(&mut self, sink: &mut dyn ArrowRowSink) -> AnyResult<()>;

    /// Serialize the `(key, weight)` tuple.
    ///
    /// FIXME: This only exists to support the CSV serializer, which outputs
    /// key and weight in the same CSV record.
    fn serialize_key_weight(&mut self, dst: &mut Vec<u8>) -> AnyResult<()>;

    fn serialize_val_weight(&mut self, dst: &mut Vec<u8>) -> AnyResult<()>;

    /// Serialize current value. Panics if invalid.
    fn serialize_val(&mut self, dst: &mut Vec<u8>) -> AnyResult<()>;

    /// Convert value to JSON. Used for error reporting to generate a human-readable
    /// representation of the value.
    fn val_to_json(&mut self) -> AnyResult<serde_json::Value>;

    /// Serialize current value to a DynamoDB item.
    #[cfg(feature = "with-dynamodb")]
    fn val_to_dynamodb_item(&mut self) -> AnyResult<HashMap<String, AttributeValue>>;

    /// Writes the current value to `dst` as a datum of the Avro `schema`. Panics if invalid.
    #[cfg(feature = "with-avro")]
    fn serialize_val_avro(
        &mut self,
        schema: &AvroSchema,
        refs: &NamesRef<'_>,
        dst: &mut Vec<u8>,
    ) -> AnyResult<()>;

    /// Returns the weight associated with the current key/value pair.
    fn weight(&mut self) -> i64;

    /// Advances the cursor to the next key.
    ///
    /// Fails if the cursor cannot read the next key, as when it reads a batch from storage.
    fn step_key(&mut self) -> AnyResult<()>;

    /// Advances the cursor to the next value.
    fn step_val(&mut self);

    /// Rewinds the cursor to the first key.
    ///
    /// Fails if the cursor cannot read the first key, as when it reads a batch from storage.
    fn rewind_keys(&mut self) -> AnyResult<()>;

    /// Rewinds the cursor to the first value for current key.
    fn rewind_vals(&mut self);

    fn count_keys(&mut self) -> AnyResult<usize> {
        let mut count = 0;

        while self.key_valid() {
            count += 1;
            self.step_key()?;
        }

        Ok(count)
    }
}

/// A handle to an output stream of a circuit that yields type-erased
/// read-only batches.
///
/// A trait for a type that wraps an output stream of the engine's circuit and
/// yields the output of each step as [`SerBatchReader`]s.
pub trait SerBatchReaderHandle: Send + Sync + DynClone {
    /// Takes the output of the last step once and returns it both whole and in parts.
    ///
    /// Without output in the last step, the whole reader is empty and there are no parts.
    fn take_readers(&self) -> SnapshotReaders;
}

/// One step's output of a collection, read two ways from one take.
#[derive(Clone)]
pub struct SnapshotReaders {
    /// Every row once: the reader for a host that reads the output as one batch.
    pub whole: Arc<dyn SerBatchReader>,

    /// Readers that together read every row once: the readers for a host that reads the
    /// parts in parallel. After a step there is one for each local worker, in worker
    /// order, so the ad-hoc scan of a multihost pipeline finds the part of a worker by
    /// the worker's index.
    pub partitions: Vec<Arc<dyn SerBatchReader>>,
}

dyn_clone::clone_trait_object!(SerBatchReaderHandle);

/// Cursor that iterates over deletions before insertions.
///
/// Most consumers don't understand Z-sets and expect a stream of upserts
/// instead, which means that the order of updates matters. For a table
/// with a primary key or unique constraint we must delete an existing record
/// before creating a new one with the same key.  The engine may not know about
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

    #[cfg(feature = "with-dynamodb")]
    fn key_to_dynamodb_item(&mut self) -> AnyResult<HashMap<String, AttributeValue>> {
        self.cursor.key_to_dynamodb_item()
    }

    fn serialize_key_fields(
        &mut self,
        fields: &HashSet<String>,
        dst: &mut Vec<u8>,
    ) -> AnyResult<()> {
        self.cursor.serialize_key_fields(fields, dst)
    }

    fn serialize_val_fields(
        &mut self,
        fields: &HashSet<String>,
        dst: &mut Vec<u8>,
    ) -> AnyResult<()> {
        self.cursor.serialize_val_fields(fields, dst)
    }

    #[cfg(feature = "with-avro")]
    fn serialize_key_avro(
        &mut self,
        schema: &AvroSchema,
        refs: &NamesRef<'_>,
        dst: &mut Vec<u8>,
    ) -> AnyResult<()> {
        self.cursor.serialize_key_avro(schema, refs, dst)
    }

    fn serialize_key_weight(&mut self, dst: &mut Vec<u8>) -> AnyResult<()> {
        self.cursor.serialize_key_weight(dst)
    }

    fn serialize_val_weight(&mut self, dst: &mut Vec<u8>) -> AnyResult<()> {
        self.cursor.serialize_val_weight(dst)
    }

    fn arrow_sink(&self, schema: SchemaRef) -> AnyResult<Box<dyn ArrowRowSink>> {
        self.cursor.arrow_sink(schema)
    }

    fn append_key_to_arrow(&mut self, sink: &mut dyn ArrowRowSink) -> AnyResult<()> {
        self.cursor.append_key_to_arrow(sink)
    }

    fn append_val_to_arrow(&mut self, sink: &mut dyn ArrowRowSink) -> AnyResult<()> {
        self.cursor.append_val_to_arrow(sink)
    }

    fn serialize_val(&mut self, dst: &mut Vec<u8>) -> AnyResult<()> {
        self.cursor.serialize_val(dst)
    }

    fn val_to_json(&mut self) -> AnyResult<serde_json::Value> {
        self.cursor.val_to_json()
    }

    #[cfg(feature = "with-dynamodb")]
    fn val_to_dynamodb_item(&mut self) -> AnyResult<HashMap<String, AttributeValue>> {
        self.cursor.val_to_dynamodb_item()
    }

    #[cfg(feature = "with-avro")]
    fn serialize_val_avro(
        &mut self,
        schema: &AvroSchema,
        refs: &NamesRef<'_>,
        dst: &mut Vec<u8>,
    ) -> AnyResult<()> {
        self.cursor.serialize_val_avro(schema, refs, dst)
    }

    fn weight(&mut self) -> i64 {
        self.cursor.weight()
    }

    fn step_key(&mut self) -> AnyResult<()> {
        self.cursor.step_key()?;
        if !self.cursor.key_valid() && !self.second_pass {
            self.cursor.rewind_keys()?;
            self.second_pass = true;
        }

        if self.cursor.key_valid() {
            self.advance_val();
        }
        Ok(())
    }

    fn step_val(&mut self) {
        self.cursor.step_val();
        self.advance_val();
    }

    fn rewind_keys(&mut self) -> AnyResult<()> {
        self.cursor.rewind_keys()?;
        self.second_pass = false;
        if self.cursor.key_valid() {
            self.advance_val();
        }
        Ok(())
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

    /// Iterate over the input stream handles and their names.
    fn input_iter(&self)
    -> Box<dyn Iterator<Item = (&SqlIdentifier, &InputCollectionHandle)> + '_>;

    fn output_iter(
        &self,
    ) -> Box<dyn Iterator<Item = (&SqlIdentifier, &OutputCollectionHandles)> + '_>;

    /// Look up output stream handles by name.
    fn output_handles(&self, name: &SqlIdentifier) -> Option<&OutputCollectionHandles>;

    /// Look up handles for an index of a stream.
    fn index_handles(
        &self,
        endpoint_name: &str,
        stream: &SqlIdentifier,
        index: &SqlIdentifier,
    ) -> Result<&OutputCollectionHandles, ControllerError>;

    fn output_handles_mut(&mut self, name: &SqlIdentifier) -> Option<&mut OutputCollectionHandles>;

    /// The registry used to insert new user-defined preprocessors
    fn preprocessor_registry(&self) -> Arc<Mutex<PreprocessorRegistry>>;

    /// The registry used to insert new user-defined postprocessors
    fn postprocessor_registry(&self) -> Arc<Mutex<PostprocessorRegistry>>;
}

#[doc(hidden)]
pub struct InputCollectionHandle {
    pub schema: Relation,
    pub handle: Box<dyn DeCollectionHandle>,

    /// The persistent id of the operator that produces the input stream, if it has one.
    ///
    /// A bootstrap names the operators that it rebuilds by persistent id, so this tells
    /// whether the table's state is rebuilt, and so whether its input connectors start over
    /// or continue from their checkpointed offsets. An operator that a checkpoint holds has
    /// a persistent id.
    pub persistent_id: Option<String>,
}

impl InputCollectionHandle {
    #[doc(hidden)]
    pub fn new<H>(schema: Relation, handle: H, persistent_id: Option<String>) -> Self
    where
        H: DeCollectionHandle + 'static,
    {
        Self {
            schema,
            handle: Box::new(handle),
            persistent_id,
        }
    }
}

/// A set of stream handles associated with each output collection.
#[derive(Clone)]
pub struct OutputCollectionHandles {
    /// Schema of the keys in the stream.
    ///
    /// Only set for indexed Z-sets.
    pub key_schema: Option<Relation>,

    /// Schema of the values in the stream.
    ///
    /// If the stream is an indexed Z-set, this is the schema of the values in the stream;
    /// if it is a Z-set, this is the schema of the keys in the stream.
    pub value_schema: Relation,

    /// Set when this stream is an index of another stream.
    pub index_of: Option<SqlIdentifier>,

    /// Set when the same stream is used to represent a view and its index.
    pub alias_as_index: Option<SqlIdentifier>,

    /// A handle to a snapshot of a materialized table/view.
    pub integrate_handle: Option<Arc<dyn SerBatchReaderHandle>>,

    /// A stream of changes to the collection.
    pub delta_handle: Box<dyn SerBatchReaderHandle>,

    /// Reference to the enable count of the accumulator used to collect updates to this stream.
    /// Incremented every time an output connector is attached to this stream; decremented when
    /// the output connector is detached.
    pub enable_count: EnableCount,
}

impl OutputCollectionHandles {
    /// Stream is an indexed Z-set.
    pub fn is_indexed(&self) -> bool {
        self.key_schema.is_some()
    }
}
