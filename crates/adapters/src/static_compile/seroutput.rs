use crate::catalog::{SerBatchReader, SerBatchReaderHandle, SerTrace, SyncSerBatchReader};
#[cfg(feature = "with-avro")]
use crate::format::avro::serializer::{avro_ser_config, AvroSchemaSerializer, AvroSerializerError};
use crate::{
    catalog::{RecordFormat, SerBatch, SerCursor},
    ControllerError,
};
use anyhow::{anyhow, Result as AnyResult};
#[cfg(feature = "with-avro")]
use apache_avro::schema::NamesRef;
#[cfg(feature = "with-avro")]
use apache_avro::types::Value as AvroValue;
#[cfg(feature = "with-avro")]
use apache_avro::Schema as AvroSchema;
use csv::{Writer as CsvWriter, WriterBuilder as CsvWriterBuilder};
use dbsp::{dynamic::DowncastTrait, DBWeight};
use dbsp::{
    dynamic::Erase,
    typed_batch::{DynBatchReader, DynSpine, DynTrace, Spine, TypedBatch},
};
use dbsp::{trace::Cursor, Batch, BatchReader, OutputHandle, Trace};
use dbsp::{
    trace::{merge_batches, WithSnapshot},
    DBData,
};
use erased_serde::{Serialize as ErasedSerialize, Serializer as ErasedSerializer};
use feldera_types::serde_with_context::serialize::{
    SerializeFieldsWithContextWrapper, SerializeWithContextWrapper,
};
use feldera_types::{
    format::csv::CsvParserConfig,
    serde_with_context::{SerializeWithContext, SqlSerdeConfig},
};
use serde::Serialize;
use serde_arrow::ArrayBuilder;
use std::{any::Any, collections::HashSet, fmt::Debug};
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

/// A wrapper type that adds the columns from E to records of type T.
/// Both `T` and `E` must be structs or maps.
#[derive(Serialize)]
struct ExtraColumns<'a, T: Serialize, E: Serialize + ?Sized> {
    #[serde(flatten)]
    value: &'a T,
    #[serde(flatten)]
    extra_columns: &'a E,
}

impl<C, T, E> SerializeWithContext<C> for ExtraColumns<'_, T, E>
where
    T: Serialize,
    E: Serialize + ?Sized,
{
    fn serialize_with_context<S>(&self, serializer: S, _context: &C) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serde::Serialize::serialize(self, serializer)
    }
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

    /// Serialize `val` to Arrow format, adding metadata columns from `metadata`.
    /// `metadata` must be a struct or a map.
    fn serialize_arrow_with_metadata(
        &mut self,
        _val: &dyn ErasedSerialize,
        _metadata: &dyn ErasedSerialize,
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
    fn create(config: CsvParserConfig) -> Self {
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
        val.serialize(&mut serde_json::Serializer::new(buf))?;
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

    fn serialize_arrow_with_metadata(
        &mut self,
        val: &dyn ErasedSerialize,
        metadata: &dyn ErasedSerialize,
        builder: &mut ArrayBuilder,
    ) -> AnyResult<()> {
        {
            let with_extras = ExtraColumns {
                value: &val,
                extra_columns: metadata,
            };

            builder.push(with_extras)?;
            Ok(())
        }
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
    B: DynBatchReader<Time = ()> + WithSnapshot + Send + Clone + Sync,
    B::Batch: DynBatchReader<Key = B::Key, Val = B::Val, R = B::R, Time = ()>,
    K: DBData + Erase<<B::Batch as DynBatchReader>::Key>,
    V: DBData + Erase<<B::Batch as DynBatchReader>::Val>,
    R: DBWeight + Erase<<B::Batch as DynBatchReader>::R> + Into<i64>,
    KD: From<K> + SerializeWithContext<SqlSerdeConfig> + 'static + Send + Debug,
    VD: From<V> + SerializeWithContext<SqlSerdeConfig> + 'static + Send + Debug,
{
    fn take_from_worker(&self, worker: usize) -> Option<Box<dyn SyncSerBatchReader>> {
        self.handle.take_from_worker(worker).map(|batch| {
            Box::new(<SerBatchImpl<TypedBatch<K, V, R, B>, KD, VD>>::new(batch))
                as Box<dyn SyncSerBatchReader>
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
                Arc::new(<SerBatchImpl<TypedBatch<K, V, R, B>, KD, VD>>::new(batch))
                    as Arc<dyn SyncSerBatchReader>
            })
            .collect()
    }

    fn concat(&self) -> Arc<dyn SyncSerBatchReader> {
        Arc::new(<SerBatchImpl<TypedBatch<K, V, R, _>, KD, VD>>::new(
            self.handle.concat(),
        )) as Arc<dyn SyncSerBatchReader>
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

impl<B, KD, VD> SerBatchReader for SerBatchImpl<B, KD, VD>
where
    B: BatchReader<Time = ()>,
    B::R: Into<i64>,
    KD: From<B::Key> + SerializeWithContext<SqlSerdeConfig> + 'static + Send + Debug,
    VD: From<B::Val> + SerializeWithContext<SqlSerdeConfig> + 'static + Send + Debug,
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
            RecordFormat::Raw => todo!(),
        })
    }

    fn batches(&self) -> Vec<Arc<dyn SerBatch>> {
        self.batch
            .batches()
            .into_iter()
            .map(|b| SerBatchImpl::<_, KD, VD>::new_arc(b) as Arc<dyn SerBatch>)
            .collect()
    }
}

impl<B, KD, VD> SyncSerBatchReader for SerBatchImpl<B, KD, VD>
where
    B: BatchReader<Time = ()> + Send + Sync,
    B::R: Into<i64>,
    KD: From<B::Key> + SerializeWithContext<SqlSerdeConfig> + 'static + Send + Debug,
    VD: From<B::Val> + SerializeWithContext<SqlSerdeConfig> + 'static + Send + Debug,
{
}

impl<B, KD, VD> SerBatch for SerBatchImpl<B, KD, VD>
where
    B: Batch<Time = ()> + Send + Sync,
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
        self.batch.inner_mut().insert(batch.batch.into_inner());
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
    B: BatchReader<Time = ()>,
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
    B: BatchReader<Time = ()>,
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

    fn serialize_key_to_arrow(&mut self, dst: &mut ArrayBuilder) -> AnyResult<()> {
        self.serializer.serialize_arrow(
            &SerializeWithContextWrapper::new(self.key.as_ref().unwrap(), &self.serde_config),
            dst,
        )
    }

    fn serialize_key_to_arrow_with_metadata(
        &mut self,
        metadata: &dyn ErasedSerialize,
        dst: &mut ArrayBuilder,
    ) -> AnyResult<()> {
        self.serializer.serialize_arrow_with_metadata(
            &SerializeWithContextWrapper::new(self.key.as_ref().unwrap(), &self.serde_config),
            metadata,
            dst,
        )
    }

    fn serialize_val_to_arrow(&mut self, dst: &mut ArrayBuilder) -> AnyResult<()> {
        self.serializer.serialize_arrow(
            &SerializeWithContextWrapper::new(self.val.as_ref().unwrap(), &self.serde_config),
            dst,
        )
    }

    fn serialize_val_to_arrow_with_metadata(
        &mut self,
        metadata: &dyn erased_serde::Serialize,
        dst: &mut ArrayBuilder,
    ) -> AnyResult<()> {
        self.serializer.serialize_arrow_with_metadata(
            &SerializeWithContextWrapper::new(self.val.as_ref().unwrap(), &self.serde_config),
            metadata,
            dst,
        )
    }

    #[cfg(feature = "with-avro")]
    fn key_to_avro(&mut self, schema: &AvroSchema, refs: &NamesRef<'_>) -> AnyResult<AvroValue> {
        Ok(self.serializer.serialize_avro(
            &SerializeWithContextWrapper::new(self.key.as_ref().unwrap(), &self.serde_config),
            schema,
            refs,
            false,
        )?)
    }

    fn serialize_key_weight(&mut self, dst: &mut Vec<u8>) -> AnyResult<()> {
        let w = self.weight();
        self.serializer.serialize(
            &SerializeWithContextWrapper::new(&(self.key.as_ref().unwrap(), w), &self.serde_config),
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

    #[cfg(feature = "with-avro")]
    fn val_to_avro(&mut self, schema: &AvroSchema, refs: &NamesRef<'_>) -> AnyResult<AvroValue> {
        // println!("val_to_avro {:?}", &self.val);

        Ok(self.serializer.serialize_avro(
            &SerializeWithContextWrapper::new(self.val.as_ref().unwrap(), &self.serde_config),
            schema,
            refs,
            true,
        )?)
    }

    fn weight(&mut self) -> i64 {
        // Safety: `trait BatchReader` guarantees that the inner weight type of `B` is `B::R`.
        unsafe { self.cursor.weight().downcast::<B::R>().clone().into() }
    }

    fn step_key(&mut self) {
        self.cursor.step_key();
        debug_assert!(!self.cursor.key_valid() || self.weight() != 0);
        self.update_key();
        self.update_val();
    }

    /// Advances the cursor to the next value.
    fn step_val(&mut self) {
        self.cursor.step_val();
        debug_assert!(!self.cursor.val_valid() || self.weight() != 0);

        self.update_val();
    }

    /// Rewinds the cursor to the first key.
    fn rewind_keys(&mut self) {
        self.cursor.rewind_keys();
        debug_assert!(!self.cursor.key_valid() || self.weight() != 0);
        self.update_key();
        self.update_val();
    }

    /// Rewinds the cursor to the first value for current key.
    fn rewind_vals(&mut self) {
        self.cursor.rewind_vals();
        debug_assert!(!self.cursor.val_valid() || self.weight() != 0);
        self.update_val();
    }
}
