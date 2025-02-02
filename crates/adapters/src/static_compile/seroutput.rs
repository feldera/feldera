use crate::catalog::{SerBatchReader, SerBatchReaderHandle, SerTrace, SyncSerBatchReader};
#[cfg(feature = "with-avro")]
use crate::format::avro::serializer::{avro_ser_config, AvroSchemaSerializer, AvroSerializerError};
use crate::{
    catalog::{RecordFormat, SerBatch, SerCollectionHandle, SerCursor},
    ControllerError,
};
use anyhow::Result as AnyResult;
#[cfg(feature = "with-avro")]
use apache_avro::schema::NamesRef;
#[cfg(feature = "with-avro")]
use apache_avro::types::Value as AvroValue;
#[cfg(feature = "with-avro")]
use apache_avro::Schema as AvroSchema;
use csv::{Writer as CsvWriter, WriterBuilder as CsvWriterBuilder};
use dbsp::dynamic::DowncastTrait;
use dbsp::trace::merge_batches;
use dbsp::typed_batch::{DynBatchReader, DynSpine, DynTrace, Spine, TypedBatch};
use dbsp::{trace::Cursor, Batch, BatchReader, OutputHandle, Trace};
use feldera_types::serde_with_context::serialize::SerializeWithContextWrapper;
use feldera_types::{
    format::csv::CsvParserConfig,
    serde_with_context::{SerializationContext, SerializeWithContext, SqlSerdeConfig},
};
use serde_arrow::ArrayBuilder;
use std::{any::Any, collections::HashSet};
use std::{cell::RefCell, io, io::Write, marker::PhantomData, ops::DerefMut, sync::Arc};

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

/// A serializer that encodes values to a byte array.
trait BytesSerializer<C>: Send {
    fn serialize<T: SerializeWithContext<C>>(
        &mut self,
        val: &T,
        buf: &mut Vec<u8>,
    ) -> AnyResult<()>;

    fn serialize_fields<T: SerializeWithContext<C>>(
        &mut self,
        _val: &T,
        _fields: &HashSet<String>,
        _buf: &mut Vec<u8>,
    ) -> AnyResult<()> {
        unimplemented!()
    }

    fn serialize_arrow<T>(&mut self, _val: &T, _buf: &mut ArrayBuilder) -> AnyResult<()>
    where
        T: SerializeWithContext<C>,
    {
        unimplemented!()
    }

    #[cfg(feature = "with-avro")]
    fn serialize_avro<T>(
        &mut self,
        _val: &T,
        _schema: &AvroSchema,
        _refs: &NamesRef<'_>,
        _strict: bool,
    ) -> Result<AvroValue, AvroSerializerError>
    where
        T: SerializeWithContext<C>,
    {
        unimplemented!()
    }
}

struct CsvSerializer<C> {
    writer: CsvWriter<SwappableWrite<Vec<u8>>>,
    context: C,
}

impl<C> CsvSerializer<C>
where
    C: Send,
{
    fn create(context: C, config: CsvParserConfig) -> Self {
        Self {
            writer: CsvWriterBuilder::new()
                .has_headers(false)
                .flexible(true)
                .delimiter(config.delimiter().0)
                .from_writer(SwappableWrite::new()),
            context,
        }
    }
}

impl<C> BytesSerializer<C> for CsvSerializer<C>
where
    C: Send,
{
    fn serialize<T>(&mut self, val: &T, buf: &mut Vec<u8>) -> AnyResult<()>
    where
        T: SerializeWithContext<C>,
    {
        let owned_buf = std::mem::take(buf);
        self.writer.get_ref().swap(Some(owned_buf));
        let val_with_context = SerializationContext::new(&self.context, val);
        let res = self.writer.serialize(val_with_context);
        let _ = self.writer.flush();
        *buf = self.writer.get_ref().swap(None).unwrap();
        Ok(res?)
    }
}

struct JsonSerializer<C> {
    context: C,
}

impl<C> JsonSerializer<C>
where
    C: Send,
{
    fn create(context: C) -> Self {
        Self { context }
    }
}

impl<C> BytesSerializer<C> for JsonSerializer<C>
where
    C: Send,
{
    fn serialize<T>(&mut self, val: &T, buf: &mut Vec<u8>) -> AnyResult<()>
    where
        T: SerializeWithContext<C>,
    {
        val.serialize_with_context(&mut serde_json::Serializer::new(buf), &self.context)?;
        Ok(())
    }

    fn serialize_fields<T: SerializeWithContext<C>>(
        &mut self,
        val: &T,
        fields: &HashSet<String>,
        buf: &mut Vec<u8>,
    ) -> AnyResult<()> {
        val.serialize_fields_with_context(
            &mut serde_json::Serializer::new(buf),
            &self.context,
            fields,
        )?;
        Ok(())
    }
}

#[cfg(feature = "with-avro")]
pub struct AvroSerializer {
    config: SqlSerdeConfig,
}

#[cfg(feature = "with-avro")]
impl AvroSerializer {
    fn create() -> Self {
        Self {
            config: avro_ser_config(),
        }
    }
}

#[cfg(feature = "with-avro")]
impl BytesSerializer<SqlSerdeConfig> for AvroSerializer {
    fn serialize<T>(&mut self, _val: &T, _buf: &mut Vec<u8>) -> AnyResult<()>
    where
        T: SerializeWithContext<SqlSerdeConfig>,
    {
        unimplemented!()
    }

    fn serialize_avro<T>(
        &mut self,
        val: &T,
        schema: &AvroSchema,
        refs: &NamesRef<'_>,
        strict: bool,
    ) -> Result<AvroValue, AvroSerializerError>
    where
        T: SerializeWithContext<SqlSerdeConfig>,
    {
        val.serialize_with_context(
            AvroSchemaSerializer::new(schema, refs, strict),
            &self.config,
        )
    }
}

struct ParquetSerializer {
    context: SqlSerdeConfig,
}

impl ParquetSerializer {
    fn create(context: SqlSerdeConfig) -> Self {
        Self { context }
    }
}

impl BytesSerializer<SqlSerdeConfig> for ParquetSerializer {
    fn serialize<T>(&mut self, _val: &T, _buf: &mut Vec<u8>) -> AnyResult<()>
    where
        T: SerializeWithContext<SqlSerdeConfig>,
    {
        unimplemented!()
    }

    fn serialize_arrow<T>(&mut self, val: &T, builder: &mut ArrayBuilder) -> AnyResult<()>
    where
        T: SerializeWithContext<SqlSerdeConfig>,
    {
        //let fields = Vec::<Field>::from_type::<T>(TracingOptions::default())?;
        builder.push(SerializeWithContextWrapper::new(val, &self.context))?;
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

impl<B, KD, VD> SerBatchReaderHandle for SerCollectionHandleImpl<B, KD, VD>
where
    B: BatchReader<Time = ()> + Send + Sync + Clone,
    B::Inner: Send,
    B::R: Into<i64>,
    KD: From<B::Key> + SerializeWithContext<SqlSerdeConfig> + 'static + Send,
    VD: From<B::Val> + SerializeWithContext<SqlSerdeConfig> + 'static + Send,
{
    fn take_from_worker(&self, worker: usize) -> Option<Box<dyn SyncSerBatchReader>> {
        self.handle.take_from_worker(worker).map(|batch| {
            Box::new(<SerBatchImpl<B, KD, VD>>::new(batch)) as Box<dyn SyncSerBatchReader>
        })
    }

    fn num_nonempty_mailboxes(&self) -> usize {
        self.handle.num_nonempty_mailboxes()
    }

    fn take_from_all(&self) -> Vec<Arc<dyn SyncSerBatchReader>> {
        self.handle
            .take_from_all()
            .into_iter()
            .map(|batch| {
                Arc::new(<SerBatchImpl<B, KD, VD>>::new(batch)) as Arc<dyn SyncSerBatchReader>
            })
            .collect()
    }
}

impl<B, KD, VD> SerCollectionHandle for SerCollectionHandleImpl<B, KD, VD>
where
    B: Batch<Time = ()> + Send + Sync + Clone,
    B::InnerBatch: Send,
    B::R: Into<i64>,
    KD: From<B::Key> + SerializeWithContext<SqlSerdeConfig> + 'static + Send,
    VD: From<B::Val> + SerializeWithContext<SqlSerdeConfig> + 'static + Send,
{
    fn take_from_worker(&self, worker: usize) -> Option<Box<dyn SerBatch>> {
        self.handle
            .take_from_worker(worker)
            .map(|batch| Box::new(<SerBatchImpl<B, KD, VD>>::new(batch)) as Box<dyn SerBatch>)
    }

    fn num_nonempty_mailboxes(&self) -> usize {
        self.handle.num_nonempty_mailboxes()
    }

    fn take_from_all(&self) -> Vec<Arc<dyn SerBatch>> {
        self.handle
            .take_from_all()
            .into_iter()
            .map(|batch| Arc::new(<SerBatchImpl<B, KD, VD>>::new(batch)) as Arc<dyn SerBatch>)
            .collect()
    }

    fn consolidate(&self) -> Box<dyn SerBatch> {
        let batch = self.handle.consolidate();
        Box::new(<SerBatchImpl<B, KD, VD>>::new(batch))
    }
}

/// [`SerBatch`] implementation that wraps a `BatchReader`.
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
}

impl<B, KD, VD> SerBatchReader for SerBatchImpl<B, KD, VD>
where
    B: BatchReader<Time = ()>,
    B::R: Into<i64>,
    KD: From<B::Key> + SerializeWithContext<SqlSerdeConfig> + 'static + Send,
    VD: From<B::Val> + SerializeWithContext<SqlSerdeConfig> + 'static + Send,
{
    fn key_count(&self) -> usize {
        self.batch.inner().key_count()
    }

    fn len(&self) -> usize {
        self.batch.inner().len()
    }

    fn cursor<'a>(
        &'a self,
        record_format: RecordFormat,
    ) -> Result<Box<dyn SerCursor + Send + 'a>, ControllerError> {
        Ok(match record_format {
            RecordFormat::Csv(config) => {
                Box::new(<SerCursorImpl<'a, CsvSerializer<_>, B, KD, VD, _>>::new(
                    &self.batch,
                    CsvSerializer::create(SqlSerdeConfig::default(), config),
                ))
            }
            RecordFormat::Json(json_flavor) => {
                let config = SqlSerdeConfig::from(json_flavor);
                Box::new(<SerCursorImpl<
                    'a,
                    JsonSerializer<_>,
                    B,
                    KD,
                    VD,
                    SqlSerdeConfig,
                >>::new(
                    &self.batch, JsonSerializer::create(config)
                ))
            }
            RecordFormat::Parquet(serde_config) => Box::new(<SerCursorImpl<
                'a,
                ParquetSerializer,
                B,
                KD,
                VD,
                SqlSerdeConfig,
            >>::new(
                &self.batch,
                ParquetSerializer::create(serde_config),
            )),
            #[cfg(feature = "with-avro")]
            RecordFormat::Avro => {
                Box::new(
                    <SerCursorImpl<'a, AvroSerializer, B, KD, VD, SqlSerdeConfig>>::new(
                        &self.batch,
                        AvroSerializer::create(),
                    ),
                )
            }
            RecordFormat::Raw => todo!(),
        })
    }
}

impl<B, KD, VD> SyncSerBatchReader for SerBatchImpl<B, KD, VD>
where
    B: BatchReader<Time = ()> + Send + Sync,
    B::R: Into<i64>,
    KD: From<B::Key> + SerializeWithContext<SqlSerdeConfig> + 'static + Send,
    VD: From<B::Val> + SerializeWithContext<SqlSerdeConfig> + 'static + Send,
{
}

impl<B, KD, VD> SerBatch for SerBatchImpl<B, KD, VD>
where
    B: Batch<Time = ()> + Send + Sync,
    B::R: Into<i64>,
    KD: From<B::Key> + SerializeWithContext<SqlSerdeConfig> + 'static + Send,
    VD: From<B::Val> + SerializeWithContext<SqlSerdeConfig> + 'static + Send,
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

    fn into_trace(self: Arc<Self>) -> Box<dyn SerTrace> {
        let mut spine = TypedBatch::new(DynSpine::<B::Inner>::new(&B::factories()));
        spine.insert(Arc::unwrap_or_clone(self).batch.into_inner());
        Box::new(SerBatchImpl::<Spine<B>, KD, VD>::new(spine))
    }

    fn as_batch_reader(&self) -> &dyn SerBatchReader {
        self
    }
}

impl<T, KD, VD> SerTrace for SerBatchImpl<T, KD, VD>
where
    T: Trace<Time = ()>,
    //TypedBatch<T::Key, T::Val, T::R, <T::InnerTrace as DynTrace>::Batch>: Send + Sync,
    T::R: Into<i64>,
    KD: From<T::Key> + SerializeWithContext<SqlSerdeConfig> + 'static + Send,
    VD: From<T::Val> + SerializeWithContext<SqlSerdeConfig> + 'static + Send,
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
        self.batch.inner_mut().insert(batch.batch.into_inner());
    }

    fn as_batch_reader(&self) -> &dyn SerBatchReader {
        self
    }
}

/// [`SerCursor`] implementation that wraps a [`Cursor`].
struct SerCursorImpl<'a, Ser, B, KD, VD, C>
where
    B: BatchReader,
{
    cursor: <B::Inner as DynBatchReader>::Cursor<'a>,
    serializer: Ser,
    phantom: PhantomData<fn(KD, VD, C)>,
    key: Option<KD>,
    val: Option<VD>,
}

impl<'a, Ser, B, KD, VD, C> SerCursorImpl<'a, Ser, B, KD, VD, C>
where
    Ser: BytesSerializer<C>,
    B: BatchReader<Time = ()>,
    B::R: Into<i64>,
    KD: From<B::Key> + SerializeWithContext<C> + Send,
    VD: From<B::Val> + SerializeWithContext<C> + Send,
{
    pub fn new(batch: &'a B, serializer: Ser) -> Self {
        let cursor = batch.inner().cursor();

        let mut result = Self {
            cursor,
            serializer,
            key: None,
            val: None,
            phantom: PhantomData,
        };
        result.skip_zero_keys();
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

    fn skip_zero_keys(&mut self) {
        while self.key_valid() {
            self.skip_zero_vals();
            if self.val_valid() {
                return;
            }
            self.step_key()
        }
    }

    fn skip_zero_vals(&mut self) {
        while self.val_valid() {
            if self.weight() != 0 {
                return;
            }
            self.step_val();
        }
    }
}

impl<'a, Ser, B, KD, VD, C> SerCursor for SerCursorImpl<'a, Ser, B, KD, VD, C>
where
    Ser: BytesSerializer<C>,
    B: BatchReader<Time = ()>,
    B::R: Into<i64>,
    KD: From<B::Key> + SerializeWithContext<C> + Send,
    VD: From<B::Val> + SerializeWithContext<C> + Send,
    <B::Inner as DynBatchReader>::Cursor<'a>: Send,
{
    fn key_valid(&self) -> bool {
        self.cursor.key_valid()
    }

    fn val_valid(&self) -> bool {
        self.cursor.val_valid()
    }

    fn serialize_key(&mut self, dst: &mut Vec<u8>) -> AnyResult<()> {
        self.serializer.serialize(self.key.as_ref().unwrap(), dst)
    }

    fn serialize_key_fields(
        &mut self,
        fields: &HashSet<String>,
        dst: &mut Vec<u8>,
    ) -> AnyResult<()> {
        self.serializer
            .serialize_fields(self.key.as_ref().unwrap(), fields, dst)
    }

    fn serialize_key_to_arrow(&mut self, dst: &mut ArrayBuilder) -> AnyResult<()> {
        self.serializer
            .serialize_arrow(self.key.as_ref().unwrap(), dst)
    }

    #[cfg(feature = "with-avro")]
    fn key_to_avro(&mut self, schema: &AvroSchema, refs: &NamesRef<'_>) -> AnyResult<AvroValue> {
        Ok(self
            .serializer
            .serialize_avro(self.key.as_ref().unwrap(), schema, refs, false)?)
    }

    fn serialize_key_weight(&mut self, dst: &mut Vec<u8>) -> AnyResult<()> {
        let w = self.weight();
        self.serializer
            .serialize(&(self.key.as_ref().unwrap(), w), dst)
    }

    fn serialize_val(&mut self, dst: &mut Vec<u8>) -> AnyResult<()> {
        self.serializer.serialize(self.val.as_ref().unwrap(), dst)
    }

    #[cfg(feature = "with-avro")]
    fn val_to_avro(&mut self, schema: &AvroSchema, refs: &NamesRef<'_>) -> AnyResult<AvroValue> {
        Ok(self
            .serializer
            .serialize_avro(self.val.as_ref().unwrap(), schema, refs, true)?)
    }

    fn weight(&mut self) -> i64 {
        // Safety: `trait BatchReader` guarantees that the inner weight type of `B` is `B::R`.
        unsafe { self.cursor.weight().downcast::<B::R>().clone().into() }
    }

    fn step_key(&mut self) {
        self.cursor.step_key();
        self.skip_zero_keys();
        self.update_key();
        self.update_val();
    }

    /// Advances the cursor to the next value.
    fn step_val(&mut self) {
        self.cursor.step_val();
        self.skip_zero_vals();
        self.update_val();
    }

    /// Rewinds the cursor to the first key.
    fn rewind_keys(&mut self) {
        self.cursor.rewind_keys();
        self.skip_zero_keys();
        self.update_key();
        self.update_val();
    }

    /// Rewinds the cursor to the first value for current key.
    fn rewind_vals(&mut self) {
        self.cursor.rewind_vals();
        self.skip_zero_vals();
        self.update_val();
    }
}
