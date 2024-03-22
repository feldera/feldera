use crate::catalog::{SerBatchReader, SerTrace};
use crate::{
    catalog::{RecordFormat, SerBatch, SerCollectionHandle, SerCursor},
    ControllerError,
};
use anyhow::Result as AnyResult;
use csv::{Writer as CsvWriter, WriterBuilder as CsvWriterBuilder};
use dbsp::dynamic::DowncastTrait;
use dbsp::trace::merge_batches;
use dbsp::typed_batch::{DynBatchReader, DynSpine, DynTrace, Spine, TypedBatch};
use dbsp::{trace::Cursor, Batch, BatchReader, OutputHandle, Trace};
use pipeline_types::serde_with_context::{
    SerializationContext, SerializeWithContext, SqlSerdeConfig,
};
use serde::Serialize;
use serde_arrow::ArrowBuilder;
use std::any::Any;
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
    fn create(context: C) -> Self;
    fn serialize<T: SerializeWithContext<C>>(
        &mut self,
        val: &T,
        buf: &mut Vec<u8>,
    ) -> AnyResult<()>;

    fn serialize_arrow<T>(&mut self, _val: &T, _buf: &mut ArrowBuilder) -> AnyResult<()>
    where
        T: Serialize,
    {
        unimplemented!()
    }
}

struct CsvSerializer<C> {
    writer: CsvWriter<SwappableWrite<Vec<u8>>>,
    context: C,
}

impl<C> BytesSerializer<C> for CsvSerializer<C>
where
    C: Send,
{
    fn create(context: C) -> Self {
        Self {
            writer: CsvWriterBuilder::new()
                .has_headers(false)
                .flexible(true)
                .from_writer(SwappableWrite::new()),
            context,
        }
    }

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

impl<C> BytesSerializer<C> for JsonSerializer<C>
where
    C: Send,
{
    fn create(context: C) -> Self {
        Self { context }
    }
    fn serialize<T>(&mut self, val: &T, buf: &mut Vec<u8>) -> AnyResult<()>
    where
        T: SerializeWithContext<C>,
    {
        val.serialize_with_context(&mut serde_json::Serializer::new(buf), &self.context)?;
        Ok(())
    }
}

struct ParquetSerializer {
    _context: SqlSerdeConfig,
}

impl BytesSerializer<SqlSerdeConfig> for ParquetSerializer {
    fn create(_context: SqlSerdeConfig) -> Self {
        Self { _context }
    }

    fn serialize<T>(&mut self, _val: &T, _buf: &mut Vec<u8>) -> AnyResult<()>
    where
        T: SerializeWithContext<SqlSerdeConfig>,
    {
        unimplemented!()
    }

    fn serialize_arrow<T>(&mut self, val: &T, builder: &mut ArrowBuilder) -> AnyResult<()>
    where
        T: Serialize,
    {
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

impl<B, KD, VD> SerCollectionHandle for SerCollectionHandleImpl<B, KD, VD>
where
    B: Batch<Time = ()> + Send + Sync + Clone,
    B::InnerBatch: Send,
    B::R: Into<i64>,
    KD: From<B::Key> + SerializeWithContext<SqlSerdeConfig> + 'static,
    VD: From<B::Val> + SerializeWithContext<SqlSerdeConfig> + 'static,
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

    fn fork(&self) -> Box<dyn SerCollectionHandle> {
        Box::new(self.clone())
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
    KD: From<B::Key> + SerializeWithContext<SqlSerdeConfig> + 'static,
    VD: From<B::Val> + SerializeWithContext<SqlSerdeConfig> + 'static,
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
    ) -> Result<Box<dyn SerCursor + 'a>, ControllerError> {
        Ok(match record_format {
            RecordFormat::Csv => {
                Box::new(<SerCursorImpl<'a, CsvSerializer<_>, B, KD, VD, _>>::new(
                    &self.batch,
                    SqlSerdeConfig::default(),
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
                >>::new(&self.batch, config))
            }
            RecordFormat::Parquet(schema) => Box::new(<SerCursorImpl<
                'a,
                ParquetSerializer,
                B,
                KD,
                VD,
                SqlSerdeConfig,
            >>::new(
                &self.batch,
                SqlSerdeConfig::from_schema(schema),
            )),
        })
    }
}

impl<B, KD, VD> SerBatch for SerBatchImpl<B, KD, VD>
where
    B: Batch<Time = ()> + Send + Sync,
    B::R: Into<i64>,
    KD: From<B::Key> + SerializeWithContext<SqlSerdeConfig> + 'static,
    VD: From<B::Val> + SerializeWithContext<SqlSerdeConfig> + 'static,
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
        ))))
    }

    fn into_trace(self: Arc<Self>, persistent_id: &str) -> Box<dyn SerTrace> {
        let mut spine = TypedBatch::new(DynSpine::<B::Inner>::new(&B::factories(), persistent_id));
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
    KD: From<T::Key> + SerializeWithContext<SqlSerdeConfig> + 'static,
    VD: From<T::Val> + SerializeWithContext<SqlSerdeConfig> + 'static,
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
    KD: From<B::Key> + SerializeWithContext<C>,
    VD: From<B::Val> + SerializeWithContext<C>,
{
    pub fn new(batch: &'a B, config: C) -> Self {
        let cursor = batch.inner().cursor();

        let mut result = Self {
            cursor,
            serializer: Ser::create(config),
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
    KD: From<B::Key> + SerializeWithContext<C>,
    VD: From<B::Val> + SerializeWithContext<C>,
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

    fn serialize_key_to_arrow(&mut self, dst: &mut ArrowBuilder) -> AnyResult<()> {
        self.serializer
            .serialize_arrow(self.key.as_ref().unwrap(), dst)
    }

    fn serialize_key_weight(&mut self, dst: &mut Vec<u8>) -> AnyResult<()> {
        let w = self.weight();
        self.serializer
            .serialize(&(self.key.as_ref().unwrap(), w), dst)
    }

    fn serialize_val(&mut self, dst: &mut Vec<u8>) -> AnyResult<()> {
        self.serializer.serialize(self.val.as_ref().unwrap(), dst)
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
