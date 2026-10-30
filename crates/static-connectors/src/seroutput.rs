//! `SerBatchReaderHandle` and related trait implementations backed by serde-based
//! serializers that convert strongly typed output records into byte streams.

use crate::catalog::{
    ArrowRowSink, SerBatchReader, SerBatchReaderHandle, SerTrace, SnapshotReaders,
};
#[cfg(feature = "with-avro")]
use crate::format::avro::serializer::{AvroSchemaSerializer, AvroSerializerError, avro_ser_config};
use crate::{
    ControllerError,
    catalog::{RecordFormat, SerBatch, SerCursor, SerKeyRange},
};
use anyhow::{Result as AnyResult, anyhow};
#[cfg(feature = "with-avro")]
use apache_avro::Schema as AvroSchema;
#[cfg(feature = "with-avro")]
use apache_avro::schema::NamesRef;
#[cfg(feature = "with-avro")]
use apache_avro::types::Value as AvroValue;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
#[cfg(feature = "with-dynamodb")]
use aws_sdk_dynamodb::types::AttributeValue;
use csv::{Writer as CsvWriter, WriterBuilder as CsvWriterBuilder};
use dbsp::{
    Batch, BatchReader, OutputHandle, Trace,
    dynamic::ClonableTrait,
    trace::{BatchReaderFactories, Cursor, TraceRole},
    typed_batch::{DynSpineSnapshot, SpineSnapshot},
};
use dbsp::{
    DBData,
    trace::{WithSnapshot, merge_batches},
};
use dbsp::{
    DBWeight,
    dynamic::{DowncastTrait, DynData},
};
use dbsp::{
    dynamic::Erase,
    typed_batch::{DynBatchReader, DynSpine, DynTrace, Spine, TypedBatch},
};
use erased_serde::{Serialize as ErasedSerialize, Serializer as ErasedSerializer};
use feldera_storage::tokio::TOKIO;
use feldera_types::format::json::json_serializer;
#[cfg(feature = "with-dynamodb")]
use feldera_types::serde_with_context::serde_config::{BinaryFormat, VariantFormat};
use feldera_types::serde_with_context::serialize::{
    SerializeFieldsWithContextWrapper, SerializeWithContextWrapper,
};
use feldera_types::{
    format::csv::CsvFormatConfig,
    serde_with_context::{SerializeWithContext, SqlSerdeConfig},
};
use serde::Serialize;
use serde_arrow::ArrayBuilder;
use serde_arrow::schema::SerdeArrowSchema;
#[cfg(feature = "with-dynamodb")]
use std::collections::HashMap;
use std::{any::Any, collections::HashSet, fmt::Debug, iter::once};
use std::{cell::RefCell, io, io::Write, marker::PhantomData, ops::DerefMut, sync::Arc};

pub trait ErasedSerializeWithContext {
    fn erased_serialize_with_context(
        &self,
        serializer: &mut dyn ErasedSerializer,
        context: &SqlSerdeConfig,
    ) -> erased_serde::Result<()>;

    fn erased_serialize_fields_with_context(
        &self,
        serializer: &mut dyn ErasedSerializer,
        context: &SqlSerdeConfig,
        fields: &HashSet<String>,
    ) -> erased_serde::Result<()>;
}

impl<T> ErasedSerializeWithContext for T
where
    T: SerializeWithContext<SqlSerdeConfig>,
{
    fn erased_serialize_with_context(
        &self,
        serializer: &mut dyn ErasedSerializer,
        context: &SqlSerdeConfig,
    ) -> erased_serde::Result<()> {
        let _ = self.serialize_with_context(serializer, context)?;
        Ok(())
    }

    fn erased_serialize_fields_with_context(
        &self,
        serializer: &mut dyn ErasedSerializer,
        context: &SqlSerdeConfig,
        fields: &HashSet<String>,
    ) -> erased_serde::Result<()> {
        let _ = self.serialize_fields_with_context(serializer, context, fields)?;
        Ok(())
    }
}

/// Implementation of the [`std::io::Write`] trait that allows swapping out
/// the underlying writer at runtime.
///
/// This is used in CSV serialization to serialize to a different buffer on
/// every invocation.
struct SwappableWrite<W> {
    writer: RefCell<Option<W>>,
}

impl<W> SwappableWrite<W> {
    fn new() -> Self {
        Self {
            writer: RefCell::new(None),
        }
    }

    fn swap(&self, mut writer: Option<W>) -> Option<W> {
        std::mem::swap(self.writer.borrow_mut().deref_mut(), &mut writer);
        writer
    }
}

impl<W> Write for SwappableWrite<W>
where
    W: Write,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.writer
            .borrow_mut()
            .as_mut()
            .map(|w| w.write(buf))
            .unwrap_or_else(|| Ok(0))
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer
            .borrow_mut()
            .as_mut()
            .map(|w| w.flush())
            .unwrap_or(Ok(()))
    }
}

/// The rows that the static engine's cursors append, in a serde_arrow builder.
struct ArrowBuilderSink(ArrayBuilder);

impl ArrowRowSink for ArrowBuilderSink {
    fn finish(&mut self) -> AnyResult<RecordBatch> {
        Ok(self.0.to_record_batch()?)
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Returns the builder of `sink`, which a cursor of the static engine created.
fn builder_of(sink: &mut dyn ArrowRowSink) -> AnyResult<&mut ArrayBuilder> {
    sink.as_any_mut()
        .downcast_mut::<ArrowBuilderSink>()
        .map(|sink| &mut sink.0)
        .ok_or_else(|| anyhow!("the Arrow row sink belongs to another engine"))
}

/// A serializer that encodes values to a byte array.
trait BytesSerializer: Send {
    fn serialize(&mut self, val: &dyn ErasedSerialize, buf: &mut Vec<u8>) -> AnyResult<()>;

    fn serialize_arrow(
        &mut self,
        _val: &dyn ErasedSerialize,
        _buf: &mut ArrayBuilder,
    ) -> AnyResult<()> {
        unimplemented!()
    }

    #[cfg(feature = "with-avro")]
    fn serialize_avro(
        &mut self,
        _val: &dyn ErasedSerialize,
        _schema: &AvroSchema,
        _refs: &NamesRef<'_>,
        _strict: bool,
    ) -> Result<AvroValue, AvroSerializerError> {
        unimplemented!()
    }
}

struct CsvSerializer {
    writer: CsvWriter<SwappableWrite<Vec<u8>>>,
}

impl CsvSerializer {
    fn create(config: CsvFormatConfig) -> Self {
        Self {
            writer: CsvWriterBuilder::new()
                .has_headers(false)
                .flexible(true)
                .delimiter(config.delimiter().0)
                .from_writer(SwappableWrite::new()),
        }
    }
}

impl BytesSerializer for CsvSerializer {
    fn serialize(&mut self, val: &dyn ErasedSerialize, buf: &mut Vec<u8>) -> AnyResult<()> {
        let owned_buf = std::mem::take(buf);
        self.writer.get_ref().swap(Some(owned_buf));
        let res = self.writer.serialize(val);
        let _ = self.writer.flush();
        *buf = self.writer.get_ref().swap(None).unwrap();
        Ok(res?)
    }
}

struct JsonSerializer;

impl JsonSerializer {
    fn create() -> Self {
        Self
    }
}

impl BytesSerializer for JsonSerializer {
    fn serialize(&mut self, val: &dyn ErasedSerialize, buf: &mut Vec<u8>) -> AnyResult<()> {
        val.serialize(&mut json_serializer(buf))?;
        Ok(())
    }
}

#[cfg(feature = "with-avro")]
pub struct AvroSerializer;

#[cfg(feature = "with-avro")]
impl AvroSerializer {
    fn create() -> Self {
        Self
    }
}

#[cfg(feature = "with-avro")]
impl BytesSerializer for AvroSerializer {
    fn serialize(&mut self, _val: &dyn ErasedSerialize, _buf: &mut Vec<u8>) -> AnyResult<()> {
        unimplemented!()
    }

    fn serialize_avro(
        &mut self,
        val: &dyn ErasedSerialize,
        schema: &AvroSchema,
        refs: &NamesRef<'_>,
        strict: bool,
    ) -> Result<AvroValue, AvroSerializerError> {
        val.serialize(AvroSchemaSerializer::new(schema, refs, strict))
    }
}

struct ParquetSerializer;

impl ParquetSerializer {
    fn create() -> Self {
        Self
    }
}

impl BytesSerializer for ParquetSerializer {
    fn serialize(&mut self, _val: &dyn ErasedSerialize, _buf: &mut Vec<u8>) -> AnyResult<()> {
        unimplemented!()
    }

    fn serialize_arrow(
        &mut self,
        val: &dyn ErasedSerialize,
        builder: &mut ArrayBuilder,
    ) -> AnyResult<()> {
        //let fields = Vec::<Field>::from_type::<T>(TracingOptions::default())?;
        builder.push(val)?;
        Ok(())
    }
}

pub struct SerCollectionHandleImpl<B, KD, VD> {
    handle: OutputHandle<B>,
    phantom: PhantomData<fn() -> (KD, VD)>,
}

impl<B: Clone, KD, VD> Clone for SerCollectionHandleImpl<B, KD, VD> {
    fn clone(&self) -> Self {
        Self {
            handle: self.handle.clone(),
            phantom: PhantomData,
        }
    }
}

impl<B, KD, VD> SerCollectionHandleImpl<B, KD, VD> {
    pub fn new(handle: OutputHandle<B>) -> Self {
        Self {
            handle,
            phantom: PhantomData,
        }
    }
}

impl<K, V, R, B, KD, VD> SerBatchReaderHandle
    for SerCollectionHandleImpl<TypedBatch<K, V, R, B>, KD, VD>
where
    B: DynBatchReader<Time = (), Key = DynData> + WithSnapshot + Send + Clone + Sync,
    B::Batch: DynBatchReader<Key = B::Key, Val = B::Val, R = B::R, Time = ()>,
    K: DBData + Erase<<B::Batch as DynBatchReader>::Key>,
    V: DBData + Erase<<B::Batch as DynBatchReader>::Val>,
    R: DBWeight + Erase<<B::Batch as DynBatchReader>::R> + Into<i64>,
    KD: From<K> + SerializeWithContext<SqlSerdeConfig> + 'static + Send + Debug,
    VD: From<V> + SerializeWithContext<SqlSerdeConfig> + 'static + Send + Debug,
{
    /// Each worker's batch moves into a snapshot of its own, and the whole shares the
    /// snapshots' batches, so no batch is copied. A snapshot of a batch taken by
    /// reference would clone it, on every step.
    fn take_readers(&self) -> SnapshotReaders {
        let snapshots = self
            .handle
            .take_from_all()
            .into_iter()
            .map(|batch| batch.into_dyn_snapshot())
            .collect::<Vec<_>>();
        let reader = |snapshot| {
            Arc::new(SerBatchImpl::<_, KD, VD>::new(SpineSnapshot::<
                TypedBatch<K, V, R, <TypedBatch<K, V, R, B> as BatchReader>::IntoBatch>,
            >::new(snapshot))) as Arc<dyn SerBatchReader>
        };
        let whole = reader(DynSpineSnapshot::concat(
            <TypedBatch<K, V, R, B> as BatchReader>::into_batch_factories(),
            &snapshots,
        ));
        let partitions = snapshots.into_iter().map(reader).collect();
        SnapshotReaders { whole, partitions }
    }
}

/// Appends `value` to `dst` as a datum of the Avro `schema`.
#[cfg(feature = "with-avro")]
fn write_avro_datum(schema: &AvroSchema, value: AvroValue, dst: &mut Vec<u8>) -> AnyResult<()> {
    let mut datum = apache_avro::to_avro_datum(schema, value)
        .map_err(|e| anyhow!("error serializing Avro value: {e}"))?;
    dst.append(&mut datum);
    Ok(())
}

#[cfg(feature = "with-dynamodb")]
fn to_dynamodb_item<T>(value: T, description: &str) -> AnyResult<HashMap<String, AttributeValue>>
where
    T: Serialize,
{
    let item = serde_dynamo::aws_sdk_dynamodb_1::to_item(value)
        .map_err(|e| anyhow!("Failed to serialize {description} to DynamoDB item: {e}"))?;
    item.into_iter()
        .map(|(k, v)| Ok((k, fix_number_attribute(v)?)))
        .collect()
}

/// The token used by `serde_json` (with `arbitrary_precision`) and by `fxp::Fixed` to serialize
/// decimal numbers as a single-field struct, which non-JSON serializers see as a map.
#[cfg(feature = "with-dynamodb")]
const SERDE_JSON_NUMBER_TOKEN: &str = "$serde_json::private::Number";

/// Recursively convert any `M({"$serde_json::private::Number": S(n)})` produced by
/// `serde_dynamo` back into the intended `N(n)`.  This arises because `SqlDecimal` and
/// `serde_json::Value::Number` (used by `VARIANT`) serialize using the same internal struct
/// format that `serde_json` uses for arbitrary-precision numbers.
///
/// `serde_dynamo` does not handle this case (see https://github.com/zenlist/serde_dynamo/issues/90);
/// remove this function if that is ever fixed.
#[cfg(feature = "with-dynamodb")]
fn fix_number_attribute(value: AttributeValue) -> AnyResult<AttributeValue> {
    match value {
        AttributeValue::M(mut map)
            if map.len() == 1 && map.contains_key(SERDE_JSON_NUMBER_TOKEN) =>
        {
            match map.remove(SERDE_JSON_NUMBER_TOKEN) {
                Some(AttributeValue::S(n)) => Ok(AttributeValue::N(n)),
                _ => Err(anyhow!("unable to serialize arbitrary-precision number")),
            }
        }
        AttributeValue::M(map) => Ok(AttributeValue::M(
            map.into_iter()
                .map(|(k, v)| Ok((k, fix_number_attribute(v)?)))
                .collect::<AnyResult<_>>()?,
        )),
        AttributeValue::L(list) => Ok(AttributeValue::L(
            list.into_iter()
                .map(fix_number_attribute)
                .collect::<AnyResult<_>>()?,
        )),
        other => Ok(other),
    }
}

/// [`SerBatch`] implementation that wraps a `BatchReader`.
#[repr(transparent)]
pub struct SerBatchImpl<B, KD, VD>
where
    B: BatchReader,
{
    batch: B,
    phantom: PhantomData<fn() -> (KD, VD)>,
}

impl<B, KD, VD> Clone for SerBatchImpl<B, KD, VD>
where
    B: Batch,
{
    fn clone(&self) -> Self {
        Self {
            batch: self.batch.clone(),
            phantom: PhantomData,
        }
    }
}

impl<B, KD, VD> SerBatchImpl<B, KD, VD>
where
    B: BatchReader,
{
    pub fn new(batch: B) -> Self {
        Self {
            batch,
            phantom: PhantomData,
        }
    }

    pub fn new_arc(batch: Arc<B>) -> Arc<Self> {
        unsafe { std::mem::transmute::<Arc<B>, Arc<Self>>(batch) }
    }
}

impl<B, KD, VD> SerBatchImpl<B, KD, VD>
where
    B: BatchReader<Time = (), DynK = DynData> + Send + Sync,
    B::R: Into<i64>,
    KD: From<B::Key> + SerializeWithContext<SqlSerdeConfig> + 'static + Send + Debug,
    VD: From<B::Val> + SerializeWithContext<SqlSerdeConfig> + 'static + Send + Debug,
{
    /// Creates a cursor that yields records in `record_format` and exposes the batch's
    /// keys, which a key range seeks to and stops at.
    fn keyed_cursor<'a>(
        &'a self,
        record_format: RecordFormat,
    ) -> Result<Box<dyn KeyedCursor + Send + 'a>, ControllerError> {
        Ok(match record_format {
            RecordFormat::Csv(config) => {
                Box::new(<SerCursorImpl<'a, CsvSerializer, B, KD, VD>>::new(
                    &self.batch,
                    SqlSerdeConfig::default(),
                    CsvSerializer::create(config),
                ))
            }
            RecordFormat::Json(json_flavor) => {
                let serde_config = SqlSerdeConfig::from(json_flavor);
                Box::new(<SerCursorImpl<'a, JsonSerializer, B, KD, VD>>::new(
                    &self.batch,
                    serde_config,
                    JsonSerializer::create(),
                ))
            }
            RecordFormat::Parquet(serde_config) => {
                Box::new(<SerCursorImpl<'a, ParquetSerializer, B, KD, VD>>::new(
                    &self.batch,
                    serde_config,
                    ParquetSerializer::create(),
                ))
            }
            #[cfg(feature = "with-avro")]
            RecordFormat::Avro => Box::new(<SerCursorImpl<'a, AvroSerializer, B, KD, VD>>::new(
                &self.batch,
                avro_ser_config(),
                AvroSerializer::create(),
            )),
            RecordFormat::Raw(_) => todo!(),
            #[cfg(feature = "with-dynamodb")]
            RecordFormat::DynamoDB => {
                let serde_config = SqlSerdeConfig::default()
                    .with_variant_format(VariantFormat::Json)
                    .with_binary_format(BinaryFormat::Bytes);
                Box::new(<SerCursorImpl<'a, JsonSerializer, B, KD, VD>>::new(
                    &self.batch,
                    serde_config,
                    JsonSerializer::create(),
                ))
            }
        })
    }
}

impl<B, KD, VD> SerBatchReader for SerBatchImpl<B, KD, VD>
where
    B: BatchReader<Time = (), DynK = DynData> + Send + Sync,
    B::R: Into<i64>,
    KD: From<B::Key> + SerializeWithContext<SqlSerdeConfig> + 'static + Send + Debug,
    VD: From<B::Val> + SerializeWithContext<SqlSerdeConfig> + 'static + Send + Debug,
{
    fn key_count(&self) -> usize {
        self.batch.inner().key_count_upper_bound()
    }

    fn len(&self) -> usize {
        self.batch.inner().len_upper_bound()
    }

    fn cursor<'a>(
        &'a self,
        record_format: RecordFormat,
    ) -> Result<Box<dyn SerCursor + Send + 'a>, ControllerError> {
        Ok(self.keyed_cursor(record_format)?)
    }

    fn batches(&self) -> Vec<Arc<dyn SerBatch>> {
        self.batch
            .batches()
            .into_iter()
            .map(|b| SerBatchImpl::<_, KD, VD>::new_arc(b) as Arc<dyn SerBatch>)
            .collect()
    }

    fn snapshot(&self) -> Arc<dyn SerBatchReader> {
        Arc::new(SerBatchImpl::<_, KD, VD>::new(TypedBatch::<
            B::Key,
            B::Val,
            B::R,
            _,
        >::new(
            self.batch.dyn_snapshot()
        )))
    }

    fn split(
        self: Arc<Self>,
        num_ranges: usize,
        record_format: RecordFormat,
    ) -> AnyResult<Vec<Box<dyn SerKeyRange>>> {
        let factories = self.batch.inner().factories();
        let mut bounds = factories.keys_factory().default_box();
        self.batch.inner().partition_keys(num_ranges, &mut *bounds);

        // Range `index` starts at bound `index - 1`, or at the first key, and ends before
        // bound `index`, or after the last key.
        let mut ranges: Vec<Box<dyn SerKeyRange>> = Vec::new();
        for index in 0..=bounds.len() {
            let start_key = {
                let mut cursor = self.keyed_cursor(record_format.clone())?;
                if index > 0 {
                    cursor.seek_key_exact(bounds.index(index - 1).as_data());
                }

                // The key that the cursor landed on; a range without one is empty.
                let Some(key) = cursor.get_key() else {
                    continue;
                };
                let mut start_key = factories.key_factory().default_box();
                key.clone_to(start_key.as_mut());
                start_key
            };
            let end_key = (index < bounds.len()).then(|| {
                let mut end_key = factories.key_factory().default_box();
                bounds.index(index).as_data().clone_to(end_key.as_mut());
                end_key
            });
            ranges.push(Box::new(KeyRange {
                batch: self.clone(),
                start_key,
                end_key,
                record_format: record_format.clone(),
            }));
        }
        Ok(ranges)
    }
}

/// The keys of a batch from `start_key`, which the batch holds, up to `end_key`, if any,
/// which the range does not include.
struct KeyRange<B, KD, VD>
where
    B: BatchReader,
{
    batch: Arc<SerBatchImpl<B, KD, VD>>,
    start_key: Box<DynData>,
    end_key: Option<Box<DynData>>,
    record_format: RecordFormat,
}

impl<B, KD, VD> SerKeyRange for KeyRange<B, KD, VD>
where
    B: BatchReader<Time = (), DynK = DynData> + Send + Sync,
    B::R: Into<i64>,
    KD: From<B::Key> + SerializeWithContext<SqlSerdeConfig> + 'static + Send + Debug,
    VD: From<B::Val> + SerializeWithContext<SqlSerdeConfig> + 'static + Send + Debug,
{
    fn cursor(&self) -> AnyResult<Box<dyn SerCursor + Send + '_>> {
        let mut cursor = self.batch.keyed_cursor(self.record_format.clone())?;

        // Not `seek_key_exact`, so that the cursor can single-step afterward.
        cursor.seek_key(self.start_key.as_data());

        Ok(Box::new(SplitCursor {
            cursor,
            start_key: self.start_key.clone(),
            end_key: self.end_key.clone(),
        }))
    }
}

/// A cursor over the keys of a [`KeyRange`].
struct SplitCursor<'a> {
    cursor: Box<dyn KeyedCursor + Send + 'a>,
    start_key: Box<DynData>,
    end_key: Option<Box<DynData>>,
}

impl SplitCursor<'_> {
    fn finished(&self) -> bool {
        if let Some(ref end_key) = self.end_key
            && let Some(current_key) = self.cursor.get_key()
        {
            return current_key >= end_key.as_data();
        }

        false
    }
}

impl SerCursor for SplitCursor<'_> {
    fn key_valid(&self) -> bool {
        self.cursor.key_valid() && !self.finished()
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

    fn arrow_sink(&self, schema: SchemaRef) -> AnyResult<Box<dyn ArrowRowSink>> {
        self.cursor.arrow_sink(schema)
    }

    fn append_key_to_arrow(&mut self, sink: &mut dyn ArrowRowSink) -> AnyResult<()> {
        self.cursor.append_key_to_arrow(sink)
    }

    fn append_val_to_arrow(&mut self, sink: &mut dyn ArrowRowSink) -> AnyResult<()> {
        self.cursor.append_val_to_arrow(sink)
    }

    fn serialize_key_weight(&mut self, dst: &mut Vec<u8>) -> AnyResult<()> {
        self.cursor.serialize_key_weight(dst)
    }

    fn serialize_val_weight(&mut self, dst: &mut Vec<u8>) -> AnyResult<()> {
        self.cursor.serialize_val_weight(dst)
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
        self.cursor.step_key()
    }

    fn step_val(&mut self) {
        self.cursor.step_val();
    }

    fn rewind_keys(&mut self) -> AnyResult<()> {
        self.cursor.rewind_keys()?;
        self.cursor.seek_key(self.start_key.as_data());
        Ok(())
    }

    fn rewind_vals(&mut self) {
        self.cursor.rewind_vals();
    }
}

/// A cursor of the static engine that exposes the keys of its batch.
trait KeyedCursor: SerCursor {
    /// Returns the current key, or `None` if the cursor has passed the last key.
    fn get_key(&self) -> Option<&DynData>;

    /// Moves the cursor to `key`, or to the first key after it, and returns whether the
    /// batch holds `key`.
    fn seek_key_exact(&mut self, key: &DynData) -> bool;

    /// Moves the cursor to the first key that is at least `key`.
    fn seek_key(&mut self, key: &DynData);
}

impl<B, KD, VD> SerBatch for SerBatchImpl<B, KD, VD>
where
    B: Batch<Time = (), DynK = DynData> + Send + Sync,
    B::R: Into<i64>,
    KD: From<B::Key> + SerializeWithContext<SqlSerdeConfig> + 'static + Send + Debug,
    VD: From<B::Val> + SerializeWithContext<SqlSerdeConfig> + 'static + Send + Debug,
{
    fn as_any(self: Arc<Self>) -> Arc<dyn Any + Sync + Send> {
        self
    }

    fn merge(self: Arc<Self>, other: Vec<Arc<dyn SerBatch>>) -> Arc<dyn SerBatch> {
        if other.is_empty() {
            return self;
        }

        let mut typed = Vec::with_capacity(other.len() + 1);

        typed.push(Arc::unwrap_or_clone(self).batch.into_inner());

        for batch in other.into_iter() {
            let any_batch = batch.as_any();
            let batch = any_batch.downcast::<Self>().unwrap();
            typed.push(Arc::unwrap_or_clone(batch).batch.into_inner())
        }

        Arc::new(Self::new(B::from_inner(merge_batches(
            &B::factories(),
            typed,
            &None,
            &None,
        ))))
    }

    fn concat(self: Arc<Self>, other: Vec<Arc<dyn SerBatch>>) -> Arc<dyn SerBatchReader> {
        let snapshots = once(self.batch.dyn_snapshot())
            .chain(other.into_iter().map(|batch| {
                batch
                    .as_any()
                    .downcast::<Self>()
                    .unwrap()
                    .batch
                    .dyn_snapshot()
            }))
            .collect::<Vec<_>>();

        Arc::new(SerBatchImpl::<_, KD, VD>::new(SpineSnapshot::<
            TypedBatch<B::Key, B::Val, B::R, B::IntoBatch>,
        >::new(
            DynSpineSnapshot::concat(B::into_batch_factories(), &snapshots),
        )))
    }

    fn into_trace(self: Arc<Self>) -> Box<dyn SerTrace> {
        let mut spine = TypedBatch::new(DynSpine::<B::Inner>::new(
            &B::factories(),
            Arc::new(String::from("SerTrace")),
            TraceRole::Integral,
        ));
        TOKIO.block_on(spine.insert(Arc::unwrap_or_clone(self).batch.into_inner()));
        Box::new(SerBatchImpl::<Spine<B>, KD, VD>::new(spine))
    }

    fn as_batch_reader(&self) -> &dyn SerBatchReader {
        self
    }

    fn arc_as_batch_reader(self: Arc<Self>) -> Arc<dyn SerBatchReader> {
        self
    }
}

impl<T, KD, VD> SerTrace for SerBatchImpl<T, KD, VD>
where
    T: Trace<Time = (), DynK = DynData> + Send + Sync,
    //TypedBatch<T::Key, T::Val, T::R, <T::InnerTrace as DynTrace>::Batch>: Send + Sync,
    T::R: Into<i64>,
    KD: From<T::Key> + SerializeWithContext<SqlSerdeConfig> + 'static + Send + Debug,
    VD: From<T::Val> + SerializeWithContext<SqlSerdeConfig> + 'static + Send + Debug,
{
    fn insert(&mut self, batch: Arc<dyn SerBatch>) {
        let batch = Arc::unwrap_or_clone(
            batch
                .as_any()
                .downcast::<SerBatchImpl<
                    TypedBatch<T::Key, T::Val, T::R, <T::InnerTrace as DynTrace>::Batch>,
                    KD,
                    VD,
                >>()
                .unwrap(),
        );
        TOKIO.block_on(self.batch.inner_mut().insert(batch.batch.into_inner()));
    }

    fn insert_without_blocking(&mut self, batch: Arc<dyn SerBatch>) -> bool {
        let batch = Arc::unwrap_or_clone(
            batch
                .as_any()
                .downcast::<SerBatchImpl<
                    TypedBatch<T::Key, T::Val, T::R, <T::InnerTrace as DynTrace>::Batch>,
                    KD,
                    VD,
                >>()
                .unwrap(),
        );
        self.batch
            .inner_mut()
            .insert_without_blocking(batch.batch.into_inner())
    }

    fn backpressure_wait(&self) {
        TOKIO.block_on(self.batch.inner().backpressure_wait());
    }

    fn as_batch_reader(&self) -> &dyn SerBatchReader {
        self
    }
}

/// [`SerCursor`] implementation that wraps a [`Cursor`].
struct SerCursorImpl<'a, Ser, B, KD, VD>
where
    B: BatchReader,
{
    cursor: <B::Inner as DynBatchReader>::Cursor<'a>,
    serde_config: SqlSerdeConfig,
    serializer: Ser,
    phantom: PhantomData<fn(KD, VD)>,
    key: Option<KD>,
    val: Option<VD>,
}

impl<'a, Ser, B, KD, VD> SerCursorImpl<'a, Ser, B, KD, VD>
where
    Ser: BytesSerializer,
    B: BatchReader<Time = (), DynK = DynData> + Send,
    B::R: Into<i64>,
    KD: From<B::Key> + SerializeWithContext<SqlSerdeConfig> + Send + Debug,
    VD: From<B::Val> + SerializeWithContext<SqlSerdeConfig> + Send + Debug,
{
    pub fn new(batch: &'a B, serde_config: SqlSerdeConfig, serializer: Ser) -> Self {
        let cursor = batch.inner().cursor();

        let mut result = Self {
            cursor,
            serde_config,
            serializer,
            key: None,
            val: None,
            phantom: PhantomData,
        };
        debug_assert!(!result.cursor.key_valid() || result.weight() != 0);

        result.update_key();
        result.update_val();
        result
    }

    fn update_key(&mut self) {
        if self.key_valid() {
            // Safety: `trait BatchReader` guarantees that the inner key type is `B::Key`.
            self.key = Some(KD::from(
                unsafe { self.cursor.key().downcast::<B::Key>() }.clone(),
            ));
        } else {
            self.key = None;
        }
    }

    fn update_val(&mut self) {
        if self.val_valid() {
            // Safety: `trait BatchReader` guarantees that the inner value type is `B::Val`.
            self.val = Some(VD::from(
                unsafe { self.cursor.val().downcast::<B::Val>() }.clone(),
            ));
        } else {
            self.val = None;
        }
    }
}

impl<'a, Ser, B, KD, VD> SerCursor for SerCursorImpl<'a, Ser, B, KD, VD>
where
    Ser: BytesSerializer,
    B: BatchReader<Time = (), DynK = DynData> + Send,
    B::R: Into<i64>,
    KD: From<B::Key> + SerializeWithContext<SqlSerdeConfig> + Send + Debug,
    VD: From<B::Val> + SerializeWithContext<SqlSerdeConfig> + Send + Debug,
    <B::Inner as DynBatchReader>::Cursor<'a>: Send,
{
    fn key_valid(&self) -> bool {
        self.cursor.key_valid()
    }

    fn val_valid(&self) -> bool {
        self.cursor.val_valid()
    }

    fn serialize_key(&mut self, dst: &mut Vec<u8>) -> AnyResult<()> {
        self.serializer.serialize(
            &SerializeWithContextWrapper::new(self.key.as_ref().unwrap(), &self.serde_config),
            dst,
        )
    }

    fn key_to_json(&mut self) -> AnyResult<serde_json::Value> {
        serde_json::to_value(SerializeWithContextWrapper::new(
            self.key.as_ref().unwrap(),
            &self.serde_config,
        ))
        .map_err(|e| anyhow!("Failed to serialize key to JSON: {}", e))
    }

    #[cfg(feature = "with-dynamodb")]
    fn key_to_dynamodb_item(&mut self) -> AnyResult<HashMap<String, AttributeValue>> {
        to_dynamodb_item(
            SerializeWithContextWrapper::new(self.key.as_ref().unwrap(), &self.serde_config),
            "key",
        )
    }

    fn serialize_key_fields(
        &mut self,
        fields: &HashSet<String>,
        dst: &mut Vec<u8>,
    ) -> AnyResult<()> {
        self.serializer.serialize(
            &SerializeFieldsWithContextWrapper::new(
                self.key.as_ref().unwrap(),
                &self.serde_config,
                fields,
            ),
            dst,
        )
    }

    fn serialize_val_fields(
        &mut self,
        fields: &HashSet<String>,
        dst: &mut Vec<u8>,
    ) -> AnyResult<()> {
        self.serializer.serialize(
            &SerializeFieldsWithContextWrapper::new(
                self.val.as_ref().unwrap(),
                &self.serde_config,
                fields,
            ),
            dst,
        )
    }

    #[cfg(feature = "with-avro")]
    fn serialize_key_avro(
        &mut self,
        schema: &AvroSchema,
        refs: &NamesRef<'_>,
        dst: &mut Vec<u8>,
    ) -> AnyResult<()> {
        let value = self.serializer.serialize_avro(
            &SerializeWithContextWrapper::new(self.key.as_ref().unwrap(), &self.serde_config),
            schema,
            refs,
            false,
        )?;
        write_avro_datum(schema, value, dst)
    }

    fn arrow_sink(&self, schema: SchemaRef) -> AnyResult<Box<dyn ArrowRowSink>> {
        let schema = SerdeArrowSchema::try_from(schema.fields().as_ref())
            .map_err(|e| anyhow!("Unable to convert schema to parquet/arrow: {e}"))?;
        let builder =
            ArrayBuilder::new(schema).map_err(|e| anyhow!("error creating array builder: {e}"))?;
        Ok(Box::new(ArrowBuilderSink(builder)))
    }

    fn append_key_to_arrow(&mut self, sink: &mut dyn ArrowRowSink) -> AnyResult<()> {
        self.serializer.serialize_arrow(
            &SerializeWithContextWrapper::new(self.key.as_ref().unwrap(), &self.serde_config),
            builder_of(sink)?,
        )
    }

    fn append_val_to_arrow(&mut self, sink: &mut dyn ArrowRowSink) -> AnyResult<()> {
        self.serializer.serialize_arrow(
            &SerializeWithContextWrapper::new(self.val.as_ref().unwrap(), &self.serde_config),
            builder_of(sink)?,
        )
    }

    fn serialize_key_weight(&mut self, dst: &mut Vec<u8>) -> AnyResult<()> {
        let w = self.weight();
        self.serializer.serialize(
            &SerializeWithContextWrapper::new(&(self.key.as_ref().unwrap(), w), &self.serde_config),
            dst,
        )
    }

    fn serialize_val_weight(&mut self, dst: &mut Vec<u8>) -> AnyResult<()> {
        let w = self.weight();
        self.serializer.serialize(
            &SerializeWithContextWrapper::new(&(self.val.as_ref().unwrap(), w), &self.serde_config),
            dst,
        )
    }

    fn serialize_val(&mut self, dst: &mut Vec<u8>) -> AnyResult<()> {
        self.serializer.serialize(
            &SerializeWithContextWrapper::new(&self.val.as_ref().unwrap(), &self.serde_config),
            dst,
        )
    }

    fn val_to_json(&mut self) -> AnyResult<serde_json::Value> {
        serde_json::to_value(SerializeWithContextWrapper::new(
            self.val.as_ref().unwrap(),
            &self.serde_config,
        ))
        .map_err(|e| anyhow!("Failed to serialize value to JSON: {}", e))
    }

    #[cfg(feature = "with-dynamodb")]
    fn val_to_dynamodb_item(&mut self) -> AnyResult<HashMap<String, AttributeValue>> {
        to_dynamodb_item(
            SerializeWithContextWrapper::new(self.val.as_ref().unwrap(), &self.serde_config),
            "value",
        )
    }

    #[cfg(feature = "with-avro")]
    fn serialize_val_avro(
        &mut self,
        schema: &AvroSchema,
        refs: &NamesRef<'_>,
        dst: &mut Vec<u8>,
    ) -> AnyResult<()> {
        let value = self.serializer.serialize_avro(
            &SerializeWithContextWrapper::new(self.val.as_ref().unwrap(), &self.serde_config),
            schema,
            refs,
            true,
        )?;
        write_avro_datum(schema, value, dst)
    }

    fn weight(&mut self) -> i64 {
        // Safety: `trait BatchReader` guarantees that the inner weight type of `B` is `B::R`.
        unsafe { self.cursor.weight().downcast::<B::R>().clone().into() }
    }

    fn step_key(&mut self) -> AnyResult<()> {
        self.cursor.step_key();
        debug_assert!(!self.cursor.key_valid() || self.weight() != 0);
        self.update_key();
        self.update_val();
        Ok(())
    }

    /// Advances the cursor to the next value.
    fn step_val(&mut self) {
        self.cursor.step_val();
        debug_assert!(!self.cursor.val_valid() || self.weight() != 0);

        self.update_val();
    }

    /// Rewinds the cursor to the first key.
    fn rewind_keys(&mut self) -> AnyResult<()> {
        self.cursor.rewind_keys();
        debug_assert!(!self.cursor.key_valid() || self.weight() != 0);
        self.update_key();
        self.update_val();
        Ok(())
    }

    /// Rewinds the cursor to the first value for current key.
    fn rewind_vals(&mut self) {
        self.cursor.rewind_vals();
        debug_assert!(!self.cursor.val_valid() || self.weight() != 0);
        self.update_val();
    }
}

impl<'a, Ser, B, KD, VD> KeyedCursor for SerCursorImpl<'a, Ser, B, KD, VD>
where
    Ser: BytesSerializer,
    B: BatchReader<Time = (), DynK = DynData> + Send,
    B::R: Into<i64>,
    KD: From<B::Key> + SerializeWithContext<SqlSerdeConfig> + Send + Debug,
    VD: From<B::Val> + SerializeWithContext<SqlSerdeConfig> + Send + Debug,
    <B::Inner as DynBatchReader>::Cursor<'a>: Send,
{
    fn get_key(&self) -> Option<&DynData> {
        self.cursor.get_key()
    }

    fn seek_key_exact(&mut self, key: &DynData) -> bool {
        let val = self.cursor.seek_key_exact(key, None);
        self.update_key();
        self.update_val();
        val
    }

    fn seek_key(&mut self, key: &DynData) {
        self.cursor.seek_key(key);
        self.update_key();
        self.update_val();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_output_keeps_float_text() {
        let mut buf = Vec::new();
        JsonSerializer::create()
            .serialize(&(1e20f64, 3.4e38f32, -0.0f64), &mut buf)
            .unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), "[1e20,3.4e38,-0.0]");
    }
}
