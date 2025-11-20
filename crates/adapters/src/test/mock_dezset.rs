#[cfg(feature = "with-avro")]
use crate::{catalog::AvroStream, format::avro::from_avro_value};
use crate::{
    catalog::{ArrowStream, DeCollectionStream, RecordFormat},
    format::{raw::raw_serde_config, InputBuffer},
    static_compile::deinput::{
        fraction, fraction_take, CsvDeserializerFromBytes, DeserializerFromBytes,
        JsonDeserializerFromBytes, RawDeserializerFromBytes,
    },
    ControllerError, DeCollectionHandle,
};
use anyhow::Result as AnyResult;
#[cfg(feature = "with-avro")]
use apache_avro::{types::Value as AvroValue, Schema as AvroSchema};
use arrow::array::RecordBatch;
use dbsp::{operator::StagedBuffers, DBData};
use erased_serde::Deserializer as ErasedDeserializer;
#[cfg(feature = "with-avro")]
use feldera_adapterlib::catalog::AvroSchemaRefs;
use feldera_adapterlib::format::BufferSize;

use feldera_sqllib::Variant;
use feldera_types::serde_with_context::{DeserializeWithContext, SqlSerdeConfig};
use serde_arrow::Deserializer as ArrowDeserializer;
use std::{
    any::Any,
    fmt::Debug,
    hash::{DefaultHasher, Hash, Hasher},
    mem::swap,
    sync::{Arc, Mutex, MutexGuard},
};

use super::{wait, DEFAULT_TIMEOUT_MS};

#[derive(Clone, PartialEq, Eq, Debug, Hash, PartialOrd, Ord)]
pub enum MockUpdate<T, U> {
    Insert(T),
    Delete(T),
    Update(U),
}

impl<T: Debug, U: Debug> MockUpdate<T, U> {
    pub fn with_polarity(val: T, polarity: bool) -> Self {
        if polarity {
            Self::Insert(val)
        } else {
            Self::Delete(val)
        }
    }

    pub fn unwrap_insert(&self) -> &T {
        if let Self::Insert(val) = self {
            val
        } else {
            panic!("MockUpdate {self:?} is not an insert")
        }
    }

    pub fn unwrap_delete(&self) -> &T {
        if let Self::Delete(val) = self {
            val
        } else {
            panic!("MockUpdate {self:?} is not a delete")
        }
    }

    pub fn unwrap_update(&self) -> &U {
        if let Self::Update(val) = self {
            val
        } else {
            panic!("MockUpdate {self:?} is not an update")
        }
    }
}

/// Inner state of `MockDeZSet`.
pub struct MockDeZSetState<T, U> {
    /// Records flushed since the last `reset`.
    pub flushed: Vec<MockUpdate<T, U>>,
}

impl<T, U> Default for MockDeZSetState<T, U> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T, U> MockDeZSetState<T, U> {
    pub fn new() -> Self {
        Self {
            flushed: Vec::new(),
        }
    }

    /// Clear internal state.
    pub fn reset(&mut self) {
        self.flushed.clear();
    }
}

pub struct MockDeZSet<T, U>(Arc<Mutex<MockDeZSetState<T, U>>>);

impl<T, U> Default for MockDeZSet<T, U> {
    fn default() -> Self {
        Self::new()
    }
}

/// Mock implementation of `DeCollectionHandle`.
impl<T, U> Clone for MockDeZSet<T, U> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T, U> MockDeZSet<T, U> {
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(MockDeZSetState::new())))
    }

    pub fn reset(&self) {
        self.0.lock().unwrap().reset();
    }

    pub fn state(&self) -> MutexGuard<'_, MockDeZSetState<T, U>> {
        self.0.lock().unwrap()
    }
}

impl<T, U> DeCollectionHandle for MockDeZSet<T, U>
where
    T: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant>
        + Hash
        + Send
        + Sync
        + Debug
        + Clone
        + 'static,
    U: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant>
        + Hash
        + Send
        + Sync
        + Debug
        + Clone
        + 'static,
{
    fn configure_deserializer(
        &self,
        record_format: RecordFormat,
    ) -> Result<Box<dyn DeCollectionStream>, ControllerError> {
        match record_format {
            RecordFormat::Csv(delimiter) => Ok(Box::new(MockDeZSetStream::<
                CsvDeserializerFromBytes,
                T,
                U,
                _,
            >::new(
                self.clone(),
                (SqlSerdeConfig::default(), delimiter),
            ))),
            RecordFormat::Json(flavor) => Ok(Box::new(MockDeZSetStream::<
                JsonDeserializerFromBytes,
                T,
                U,
                _,
            >::new(
                self.clone(),
                SqlSerdeConfig::from(flavor),
            ))),
            RecordFormat::Parquet(_) => {
                todo!()
            }
            #[cfg(feature = "with-avro")]
            RecordFormat::Avro => {
                todo!()
            }
            RecordFormat::Raw => Ok(Box::new(MockDeZSetStream::<
                RawDeserializerFromBytes,
                T,
                U,
                _,
            >::new(
                self.clone(), raw_serde_config()
            ))),
        }
    }

    fn configure_arrow_deserializer(
        &self,
        config: SqlSerdeConfig,
    ) -> Result<Box<dyn ArrowStream>, ControllerError> {
        Ok(Box::new(MockZSetArrowStream::<T, U, _>::new(
            self.clone(),
            config,
        )))
    }

    #[cfg(feature = "with-avro")]
    fn configure_avro_deserializer(&self) -> Result<Box<dyn AvroStream>, ControllerError> {
        Ok(Box::new(MockAvroStream::new(self.clone())))
    }

    fn fork(&self) -> Box<dyn DeCollectionHandle> {
        Box::new(self.clone())
    }
}

#[derive(Clone)]
struct MockDeZSetStreamBuffer<T, U> {
    updates: Vec<MockUpdate<T, U>>,
    n_bytes: usize,
    handle: MockDeZSet<T, U>,
}

impl<T, U> MockDeZSetStreamBuffer<T, U> {
    fn new(handle: MockDeZSet<T, U>) -> Self {
        Self {
            updates: Vec::new(),
            n_bytes: 0,
            handle,
        }
    }
}

impl<T, U> InputBuffer for MockDeZSetStreamBuffer<T, U>
where
    T: Hash + Send + Sync + 'static,
    U: Hash + Send + Sync + 'static,
{
    fn flush(&mut self) {
        let mut state = self.handle.0.lock().unwrap();
        state.flushed.append(&mut self.updates);
        self.n_bytes = 0;
    }

    fn len(&self) -> BufferSize {
        BufferSize {
            records: self.updates.len(),
            bytes: self.n_bytes,
        }
    }

    fn hash(&self, hasher: &mut dyn Hasher) {
        for elem in &self.updates {
            let mut elem_hasher = DefaultHasher::new();
            elem.hash(&mut elem_hasher);
            hasher.write_u64(elem_hasher.finish())
        }
    }

    fn take_some(&mut self, n: usize) -> Option<Box<dyn InputBuffer>> {
        if !self.updates.is_empty() {
            let n = self.updates.len().min(n);
            Some(Box::new(MockDeZSetStreamBuffer {
                n_bytes: fraction_take(n, self.updates.len(), &mut self.n_bytes),
                updates: {
                    let mut some = self.updates.split_off(n);
                    swap(&mut some, &mut self.updates);
                    some
                },
                handle: self.handle.clone(),
            }))
        } else {
            None
        }
    }
}

struct MockDeZSetStreamStagedBuffers<T, U> {
    buffers: Vec<MockUpdate<T, U>>,
    handle: MockDeZSet<T, U>,
}

impl<T, U> MockDeZSetStreamStagedBuffers<T, U>
where
    T: 'static,
    U: 'static,
{
    fn new(buffers: Vec<Box<dyn InputBuffer>>, zset: &MockDeZSet<T, U>) -> Self {
        Self {
            buffers: buffers
                .into_iter()
                .flat_map(|buffer| {
                    (buffer as Box<dyn Any>)
                        .downcast::<MockDeZSetStreamBuffer<T, U>>()
                        .unwrap()
                        .updates
                })
                .collect(),
            handle: zset.clone(),
        }
    }
}

impl<T, U> StagedBuffers for MockDeZSetStreamStagedBuffers<T, U> {
    fn flush(&mut self) {
        self.handle
            .0
            .lock()
            .unwrap()
            .flushed
            .append(&mut self.buffers);
    }
}

#[derive(Clone)]
pub struct MockDeZSetStream<De, T, U, C>
where
    T: Send + Sync,
    U: Send + Sync,
{
    buffer: MockDeZSetStreamBuffer<T, U>,
    deserializer: De,
    config: C,
}

impl<De, T, U, C> MockDeZSetStream<De, T, U, C>
where
    De: DeserializerFromBytes<C>,
    T: Send + Sync,
    U: Send + Sync,
    C: Clone,
{
    pub fn new(handle: MockDeZSet<T, U>, config: C) -> Self {
        Self {
            buffer: MockDeZSetStreamBuffer::new(handle),
            deserializer: De::create(config.clone()),
            config,
        }
    }
}

impl<De, T, U, C> DeCollectionStream for MockDeZSetStream<De, T, U, C>
where
    T: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant> + Hash + Send + Sync + 'static,
    U: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant> + Hash + Send + Sync + 'static,
    De: DeserializerFromBytes<C> + Send + Sync + 'static,
    C: Clone + Send + Sync + 'static,
{
    fn insert(&mut self, data: &[u8], metadata: &Option<Variant>) -> AnyResult<()> {
        let val = DeserializerFromBytes::deserialize::<T>(&mut self.deserializer, data, metadata)?;
        self.buffer.updates.push(MockUpdate::Insert(val));
        self.buffer.n_bytes += data.len();
        Ok(())
    }

    fn delete(&mut self, data: &[u8], metadata: &Option<Variant>) -> AnyResult<()> {
        let val = DeserializerFromBytes::deserialize::<T>(&mut self.deserializer, data, metadata)?;
        self.buffer.updates.push(MockUpdate::Delete(val));
        self.buffer.n_bytes += data.len();
        Ok(())
    }

    fn update(&mut self, data: &[u8], metadata: &Option<Variant>) -> AnyResult<()> {
        let val = DeserializerFromBytes::deserialize::<U>(&mut self.deserializer, data, metadata)?;
        self.buffer.updates.push(MockUpdate::Update(val));
        self.buffer.n_bytes += data.len();
        Ok(())
    }

    fn reserve(&mut self, _reservation: usize) {}

    fn truncate(&mut self, len: usize) {
        if len < self.buffer.updates.len() {
            self.buffer.n_bytes = fraction(len, self.buffer.updates.len(), self.buffer.n_bytes);
            self.buffer.updates.truncate(len);
        }
    }

    fn fork(&self) -> Box<dyn DeCollectionStream> {
        Box::new(Self::new(self.buffer.handle.clone(), self.config.clone()))
    }

    fn stage(&self, buffers: Vec<Box<dyn InputBuffer>>) -> Box<dyn StagedBuffers> {
        Box::new(MockDeZSetStreamStagedBuffers::new(
            buffers,
            &self.buffer.handle,
        ))
    }
}

impl<De, T, U, C> InputBuffer for MockDeZSetStream<De, T, U, C>
where
    T: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant> + Hash + Send + Sync + 'static,
    U: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant> + Hash + Send + Sync + 'static,
    De: DeserializerFromBytes<C> + Send + Sync + 'static,
    C: Clone + Send + Sync + 'static,
{
    fn flush(&mut self) {
        self.buffer.flush()
    }

    fn take_some(&mut self, n: usize) -> Option<Box<dyn InputBuffer>> {
        self.buffer.take_some(n)
    }

    fn hash(&self, hasher: &mut dyn Hasher) {
        self.buffer.hash(hasher)
    }

    fn len(&self) -> BufferSize {
        self.buffer.len()
    }
}

#[derive(Clone)]
pub struct MockZSetArrowStream<T, U, C>
where
    T: Send + Sync,
    U: Send + Sync,
{
    buffer: MockDeZSetStreamBuffer<T, U>,
    config: C,
}

impl<T, U, C> MockZSetArrowStream<T, U, C>
where
    T: Send + Sync,
    U: Send + Sync,
    C: Clone,
{
    pub fn new(handle: MockDeZSet<T, U>, config: C) -> Self {
        Self {
            buffer: MockDeZSetStreamBuffer::new(handle),
            config,
        }
    }
}

impl<T, U, C> InputBuffer for MockZSetArrowStream<T, U, C>
where
    T: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant> + Hash + Send + Sync + 'static,
    U: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant> + Hash + Send + Sync + 'static,
    C: Clone + Send + Sync + 'static,
{
    fn flush(&mut self) {
        self.buffer.flush()
    }

    fn take_some(&mut self, n: usize) -> Option<Box<dyn InputBuffer>> {
        self.buffer.take_some(n)
    }

    fn hash(&self, hasher: &mut dyn Hasher) {
        self.buffer.hash(hasher)
    }

    fn len(&self) -> BufferSize {
        self.buffer.len()
    }
}

impl<T, U> ArrowStream for MockZSetArrowStream<T, U, SqlSerdeConfig>
where
    T: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant>
        + Hash
        + Send
        + Sync
        + Debug
        + Clone
        + 'static,
    U: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant>
        + Hash
        + Send
        + Sync
        + Debug
        + Clone
        + 'static,
{
    fn insert(&mut self, data: &RecordBatch, _metadata: &Option<Variant>) -> AnyResult<()> {
        let deserializer = ArrowDeserializer::from_record_batch(data)?;
        let deserializer =
            &mut <dyn ErasedDeserializer>::erase(deserializer) as &mut dyn ErasedDeserializer;

        let records = Vec::<T>::deserialize_with_context(deserializer, &self.config)?;
        self.buffer.updates.extend(
            records
                .into_iter()
                .map(|r| MockUpdate::<T, U>::with_polarity(r, true)),
        );
        self.buffer.n_bytes += data.get_array_memory_size();

        Ok(())
    }

    fn delete(&mut self, _data: &RecordBatch, _metadata: &Option<Variant>) -> AnyResult<()> {
        todo!()
    }

    fn insert_with_polarities(
        &mut self,
        _data: &arrow::array::RecordBatch,
        _polarities: &[bool],
        _metadata: &Option<Variant>,
    ) -> AnyResult<()> {
        todo!()
    }

    fn fork(&self) -> Box<dyn ArrowStream> {
        Box::new(self.clone())
    }

    fn stage(&self, buffers: Vec<Box<dyn InputBuffer>>) -> Box<dyn StagedBuffers> {
        Box::new(MockDeZSetStreamStagedBuffers::new(
            buffers,
            &self.buffer.handle,
        ))
    }
}

/// [`AvroStream`] implementation that collects deserialized records into a
/// [`MockDeZSet`].
#[cfg(feature = "with-avro")]
#[derive(Clone)]
pub struct MockAvroStream<T, U> {
    updates: Vec<MockUpdate<T, U>>,
    n_bytes: usize,
    handle: MockDeZSet<T, U>,
}

#[cfg(feature = "with-avro")]
impl<T, U> MockAvroStream<T, U> {
    fn new(handle: MockDeZSet<T, U>) -> Self {
        Self {
            updates: Vec::new(),
            n_bytes: 0,
            handle,
        }
    }
}

#[cfg(feature = "with-avro")]
impl<T, U> AvroStream for MockAvroStream<T, U>
where
    T: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant> + Hash + Send + Sync + 'static,
    U: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant> + Hash + Send + Sync + 'static,
{
    fn insert(
        &mut self,
        data: &AvroValue,
        schema: &AvroSchema,
        refs: &AvroSchemaRefs,
        n_bytes: usize,
        metadata: &Option<Variant>,
    ) -> AnyResult<()> {
        let v: T = from_avro_value(data, schema, refs, metadata)
            .map_err(|e| anyhow::anyhow!("error deserializing Avro record: {e}"))?;
        self.updates.push(MockUpdate::Insert(v));
        self.n_bytes += n_bytes;
        Ok(())
    }

    fn delete(
        &mut self,
        data: &AvroValue,
        schema: &AvroSchema,
        refs: &AvroSchemaRefs,
        n_bytes: usize,
        metadata: &Option<Variant>,
    ) -> AnyResult<()> {
        let v: T = from_avro_value(data, schema, refs, metadata)
            .map_err(|e| anyhow::anyhow!("error deserializing Avro record: {e}"))?;

        self.updates.push(MockUpdate::Delete(v));
        self.n_bytes += n_bytes;
        Ok(())
    }

    fn fork(&self) -> Box<dyn AvroStream> {
        Box::new(Self::new(self.handle.clone()))
    }

    fn stage(&self, buffers: Vec<Box<dyn InputBuffer>>) -> Box<dyn StagedBuffers> {
        Box::new(MockDeZSetStreamStagedBuffers::new(buffers, &self.handle))
    }
}

#[cfg(feature = "with-avro")]
impl<T, U> InputBuffer for MockAvroStream<T, U>
where
    T: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant> + Hash + Send + Sync + 'static,
    U: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant> + Hash + Send + Sync + 'static,
{
    fn flush(&mut self) {
        let mut state = self.handle.0.lock().unwrap();
        state.flushed.append(&mut self.updates);
    }

    fn take_some(&mut self, n: usize) -> Option<Box<dyn InputBuffer>> {
        if !self.updates.is_empty() {
            let n = self.updates.len().min(n);
            Some(Box::new(MockDeZSetStreamBuffer {
                n_bytes: fraction_take(n, self.updates.len(), &mut self.n_bytes),
                updates: {
                    let mut some = self.updates.split_off(n);
                    swap(&mut some, &mut self.updates);
                    some
                },
                handle: self.handle.clone(),
            }))
        } else {
            None
        }
    }

    fn len(&self) -> BufferSize {
        BufferSize {
            records: self.updates.len(),
            bytes: self.n_bytes,
        }
    }

    fn hash(&self, hasher: &mut dyn Hasher) {
        for elem in &self.updates {
            let mut elem_hasher = DefaultHasher::new();
            elem.hash(&mut elem_hasher);
            hasher.write_u64(elem_hasher.finish())
        }
    }
}

/// Wait to receive all records in `data` in the same order.
pub fn wait_for_output_ordered<T, F>(zset: &MockDeZSet<T, T>, data: &[Vec<T>], flush: F)
where
    T: DBData,
    F: Fn(),
{
    let num_records: usize = data.iter().map(Vec::len).sum();

    wait(
        || {
            flush();
            zset.state().flushed.len() == num_records
        },
        DEFAULT_TIMEOUT_MS,
    )
    .unwrap();

    let received = zset
        .state()
        .flushed
        .iter()
        .map(|u| u.unwrap_insert())
        .cloned()
        .collect::<Vec<_>>();

    let expected = data
        .iter()
        .flat_map(|data| data.iter())
        .cloned()
        .collect::<Vec<_>>();

    assert_eq!(&received, &expected);

    // for (i, val) in data.iter().flat_map(|data| data.iter()).enumerate() {
    //     assert_eq!(zset.state().flushed[i].unwrap_insert(), val);
    // }
}

/// Wait for `count` records to be received.
pub fn wait_for_output_count<T, F>(zset: &MockDeZSet<T, T>, count: usize, flush: F) -> Vec<T>
where
    T: DBData,
    F: Fn(),
{
    wait(
        || {
            flush();
            zset.state().flushed.len() == count
        },
        DEFAULT_TIMEOUT_MS,
    )
    .unwrap();

    zset.state()
        .flushed
        .iter()
        .map(|u| u.unwrap_insert())
        .cloned()
        .collect::<Vec<_>>()
}

/// Wait to receive all records in `data` in some order.
pub fn wait_for_output_unordered<T, F>(zset: &MockDeZSet<T, T>, data: &[Vec<T>], flush: F)
where
    T: DBData,
    F: Fn(),
{
    let num_records: usize = data.iter().map(Vec::len).sum();

    wait(
        || {
            flush();
            zset.state().flushed.len() == num_records
        },
        DEFAULT_TIMEOUT_MS,
    )
    .unwrap();

    let mut data_sorted = data
        .iter()
        .flat_map(|data| data.clone().into_iter())
        .collect::<Vec<_>>();
    data_sorted.sort();

    let mut zset_sorted = zset
        .state()
        .flushed
        .iter()
        .map(|upd| upd.unwrap_insert().clone())
        .collect::<Vec<_>>();
    zset_sorted.sort();

    assert_eq!(zset_sorted, data_sorted);
}
