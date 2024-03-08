use crate::{
    catalog::{RecordFormat, SerBatch, SerCollectionHandle, SerCursor},
    ControllerError, SerializationContext, SerializeWithContext, SqlSerdeConfig,
};
use anyhow::Result as AnyResult;
use csv::{Writer as CsvWriter, WriterBuilder as CsvWriterBuilder};
use dbsp::dynamic::DowncastTrait;
use dbsp::typed_batch::DynBatchReader;
use dbsp::{trace::Cursor, Batch, BatchReader, OutputHandle};
use serde::Serialize;
use serde_arrow::ArrowBuilder;
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
    /// `Arc` is necessary for this type to satisfy `Send`.
    batch: Arc<B>,
    phantom: PhantomData<fn() -> (KD, VD)>,
}

impl<B, KD, VD> SerBatchImpl<B, KD, VD>
where
    B: BatchReader,
{
    pub fn new(batch: B) -> Self {
        Self {
            batch: Arc::new(batch),
            phantom: PhantomData,
        }
    }
}

impl<B, KD, VD> SerBatch for SerBatchImpl<B, KD, VD>
where
    B: BatchReader<Time = ()> + Clone + Send + Sync,
    B::R: Into<i64>,
    KD: From<B::Key> + SerializeWithContext<SqlSerdeConfig>,
    VD: From<B::Val> + SerializeWithContext<SqlSerdeConfig>,
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
    B: BatchReader<Time = ()> + Clone,
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
            // Safety: `trait BatchReader` guarantees that the inner key type is `B::Key`.
            self.val = Some(VD::from(
                unsafe { self.cursor.val().downcast::<B::Val>() }.clone(),
            ));
        } else {
            self.val = None;
        }
    }
}

impl<'a, Ser, B, KD, VD, C> SerCursor for SerCursorImpl<'a, Ser, B, KD, VD, C>
where
    Ser: BytesSerializer<C>,
    B: BatchReader<Time = ()> + Clone,
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
        self.update_key();
        self.update_val();
    }

    /// Advances the cursor to the next value.
    fn step_val(&mut self) {
        self.cursor.step_val();
        self.update_val();
    }

    /// Rewinds the cursor to the first key.
    fn rewind_keys(&mut self) {
        self.cursor.rewind_keys();
        self.update_key();
        self.update_val();
    }

    /// Rewinds the cursor to the first value for current key.
    fn rewind_vals(&mut self) {
        self.cursor.rewind_vals();
        self.update_val();
    }
}
