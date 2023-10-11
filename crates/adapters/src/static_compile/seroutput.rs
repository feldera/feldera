use crate::{
    catalog::{RecordFormat, SerBatch, SerCollectionHandle, SerCursor},
    ControllerError,
};
use anyhow::Result as AnyResult;
use csv::{Writer as CsvWriter, WriterBuilder as CsvWriterBuilder};
use dbsp::{
    trace::{Batch, BatchReader, Cursor, Deserializable},
    OutputHandle,
};
use serde::Serialize;
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
trait BytesSerializer: Send {
    fn create() -> Self;
    fn serialize<T: Serialize>(&mut self, val: &T, buf: &mut Vec<u8>) -> AnyResult<()>;
}

struct CsvSerializer {
    writer: CsvWriter<SwappableWrite<Vec<u8>>>,
}

impl BytesSerializer for CsvSerializer {
    fn create() -> Self {
        Self {
            writer: CsvWriterBuilder::new()
                .has_headers(false)
                .flexible(true)
                .from_writer(SwappableWrite::new()),
        }
    }

    fn serialize<T>(&mut self, val: &T, buf: &mut Vec<u8>) -> AnyResult<()>
    where
        T: Serialize,
    {
        let owned_buf = std::mem::take(buf);
        self.writer.get_ref().swap(Some(owned_buf));
        let res = self.writer.serialize(val);
        let _ = self.writer.flush();
        *buf = self.writer.get_ref().swap(None).unwrap();
        Ok(res?)
    }
}

struct JsonSerializer;

impl BytesSerializer for JsonSerializer {
    fn create() -> Self {
        Self
    }
    fn serialize<T>(&mut self, val: &T, buf: &mut Vec<u8>) -> AnyResult<()>
    where
        T: Serialize,
    {
        serde_json::to_writer(buf, val)?;
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
    B::R: Into<i64>,
    KD: From<B::Key> + Serialize + 'static,
    VD: From<B::Val> + Serialize + 'static,
    <<B as BatchReader>::Key as Deserializable>::ArchivedDeser: Ord,
    <<B as BatchReader>::Val as Deserializable>::ArchivedDeser: Ord,
{
    fn take_from_worker(&self, worker: usize) -> Option<Box<dyn SerBatch>> {
        self.handle
            .take_from_worker(worker)
            .map(|batch| Box::new(<SerBatchImpl<B, KD, VD>>::new(batch)) as Box<dyn SerBatch>)
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
    KD: From<B::Key> + Serialize,
    VD: From<B::Val> + Serialize,
{
    fn key_count(&self) -> usize {
        self.batch.key_count()
    }

    fn len(&self) -> usize {
        self.batch.len()
    }

    fn cursor<'a>(
        &'a self,
        record_format: RecordFormat,
    ) -> Result<Box<dyn SerCursor + 'a>, ControllerError> {
        Ok(match record_format {
            RecordFormat::Csv => Box::new(<SerCursorImpl<'a, CsvSerializer, B, KD, VD>>::new(
                &self.batch,
            )),
            // TODO: configurable serializers.
            RecordFormat::Json(_) => Box::new(<SerCursorImpl<'a, JsonSerializer, B, KD, VD>>::new(
                &self.batch,
            )),
        })
    }
}

/// [`SerCursor`] implementation that wraps a [`Cursor`].
struct SerCursorImpl<'a, Ser, B, KD, VD>
where
    B: BatchReader,
{
    cursor: B::Cursor<'a>,
    serializer: Ser,
    phantom: PhantomData<fn() -> (KD, VD)>,
    key: Option<KD>,
    val: Option<VD>,
}

impl<'a, Ser, B, KD, VD> SerCursorImpl<'a, Ser, B, KD, VD>
where
    Ser: BytesSerializer,
    B: BatchReader<Time = ()> + Clone,
    B::R: Into<i64>,
    KD: From<B::Key> + Serialize,
    VD: From<B::Val> + Serialize,
{
    pub fn new(batch: &'a B) -> Self {
        let cursor = batch.cursor();

        let mut result = Self {
            cursor,
            serializer: Ser::create(),
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
            self.key = Some(KD::from(self.cursor.key().clone()));
        } else {
            self.key = None;
        }
    }

    fn update_val(&mut self) {
        if self.val_valid() {
            self.val = Some(VD::from(self.cursor.val().clone()));
        } else {
            self.val = None;
        }
    }
}

impl<'a, Ser, B, KD, VD> SerCursor for SerCursorImpl<'a, Ser, B, KD, VD>
where
    Ser: BytesSerializer,
    B: BatchReader<Time = ()> + Clone,
    B::R: Into<i64>,
    KD: From<B::Key> + Serialize,
    VD: From<B::Val> + Serialize,
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

    fn serialize_key_weight(&mut self, dst: &mut Vec<u8>) -> AnyResult<()> {
        let w = self.weight();
        self.serializer
            .serialize(&(self.key.as_ref().unwrap(), w), dst)
    }

    fn serialize_val(&mut self, dst: &mut Vec<u8>) -> AnyResult<()> {
        self.serializer.serialize(self.val.as_ref().unwrap(), dst)
    }

    fn weight(&mut self) -> i64 {
        self.cursor.weight().into()
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
