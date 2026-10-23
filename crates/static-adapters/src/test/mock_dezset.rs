use crate::{
    ControllerError,
    catalog::{ArrowStream, DeCollectionHandle, DeCollectionStream, RecordFormat},
    deinput::{
        CsvDeserializerFromBytes, DeserializerFromBytes, JsonDeserializerFromBytes,
        RawDeserializerFromBytes, fraction, fraction_take,
    },
    format::raw::raw_serde_config,
    metadata::MetadataVariant,
};
#[cfg(feature = "with-avro")]
use crate::{catalog::AvroStream, deinput::InstalledAvroSchema, format::avro::from_avro_value};
use anyhow::{Result as AnyResult, bail};
#[cfg(feature = "with-avro")]
use apache_avro::{Schema as AvroSchema, types::Value as AvroValue};
use arrow::array::RecordBatch;
use dbsp::operator::StagedBuffers;
use dbsp_adapters::format::InputBuffer;
use erased_serde::Deserializer as ErasedDeserializer;
use feldera_adapterlib::ConnectorMetadata;
#[cfg(feature = "with-avro")]
use feldera_adapterlib::catalog::AvroSchemaRefs;
use feldera_adapterlib::format::{BufferSize, flatten_nested};

use feldera_sqllib::SqlVariant;
use feldera_types::serde_with_context::{DeserializeWithContext, SqlSerdeConfig};
use serde_arrow::Deserializer as ArrowDeserializer;
use std::{
    fmt::Debug,
    hash::{DefaultHasher, Hash, Hasher},
    mem::swap,
    sync::{Arc, Mutex, MutexGuard},
};

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
    T: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, SqlVariant>
        + Hash
        + Send
        + Sync
        + Debug
        + Clone
        + 'static,
    U: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, SqlVariant>
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
                unreachable!("Parquet input goes through configure_arrow_deserializer")
            }
            #[cfg(feature = "with-avro")]
            RecordFormat::Avro => {
                unreachable!("Avro input goes through configure_avro_deserializer")
            }
            RecordFormat::Raw(column_name) => Ok(Box::new(MockDeZSetStream::<
                RawDeserializerFromBytes,
                T,
                U,
                _,
            >::new(
                self.clone(),
                (raw_serde_config(), column_name.clone()),
            ))),
            #[cfg(feature = "with-dynamodb")]
            RecordFormat::DynamoDB => {
                unreachable!("DynamoDB is an output-only format")
            }
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
    fn flush(&mut self) -> AnyResult<()> {
        let mut state = self.handle.0.lock().unwrap();
        state.flushed.append(&mut self.updates);
        self.n_bytes = 0;
        Ok(())
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
            buffers: flatten_nested::<MockDeZSetStreamBuffer<T, U>>(buffers)
                .into_iter()
                .flat_map(|buffer| buffer.updates)
                .collect(),
            handle: zset.clone(),
        }
    }
}

impl<T, U> StagedBuffers for MockDeZSetStreamStagedBuffers<T, U>
where
    T: Send + Sync,
    U: Send + Sync,
{
    fn flush(&mut self) -> AnyResult<()> {
        self.handle
            .0
            .lock()
            .unwrap()
            .flushed
            .append(&mut self.buffers);
        Ok(())
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
    metadata: MetadataVariant,
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
            metadata: MetadataVariant::default(),
        }
    }
}

impl<De, T, U, C> DeCollectionStream for MockDeZSetStream<De, T, U, C>
where
    T: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, SqlVariant>
        + Hash
        + Send
        + Sync
        + 'static,
    U: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, SqlVariant>
        + Hash
        + Send
        + Sync
        + 'static,
    De: DeserializerFromBytes<C> + Send + Sync + 'static,
    C: Clone + Send + Sync + 'static,
{
    fn insert(&mut self, data: &[u8], metadata: &Option<ConnectorMetadata>) -> AnyResult<()> {
        let val = DeserializerFromBytes::deserialize::<T>(
            &mut self.deserializer,
            data,
            self.metadata.get(metadata),
        )?;
        self.buffer.updates.push(MockUpdate::Insert(val));
        self.buffer.n_bytes += data.len();
        Ok(())
    }

    fn delete(&mut self, data: &[u8], metadata: &Option<ConnectorMetadata>) -> AnyResult<()> {
        let val = DeserializerFromBytes::deserialize::<T>(
            &mut self.deserializer,
            data,
            self.metadata.get(metadata),
        )?;
        self.buffer.updates.push(MockUpdate::Delete(val));
        self.buffer.n_bytes += data.len();
        Ok(())
    }

    fn update(&mut self, data: &[u8], metadata: &Option<ConnectorMetadata>) -> AnyResult<()> {
        let val = DeserializerFromBytes::deserialize::<U>(
            &mut self.deserializer,
            data,
            self.metadata.get(metadata),
        )?;
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
    T: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, SqlVariant>
        + Hash
        + Send
        + Sync
        + 'static,
    U: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, SqlVariant>
        + Hash
        + Send
        + Sync
        + 'static,
    De: DeserializerFromBytes<C> + Send + Sync + 'static,
    C: Clone + Send + Sync + 'static,
{
    fn flush(&mut self) -> AnyResult<()> {
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
    metadata: MetadataVariant,
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
            metadata: MetadataVariant::default(),
        }
    }
}

impl<T, U, C> InputBuffer for MockZSetArrowStream<T, U, C>
where
    T: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, SqlVariant>
        + Hash
        + Send
        + Sync
        + 'static,
    U: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, SqlVariant>
        + Hash
        + Send
        + Sync
        + 'static,
    C: Clone + Send + Sync + 'static,
{
    fn flush(&mut self) -> AnyResult<()> {
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

impl<T, U> MockZSetArrowStream<T, U, SqlSerdeConfig>
where
    T: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, SqlVariant> + Send + Sync,
    U: Send + Sync,
{
    /// Deserialize the records in `data` and buffer the update that `update`
    /// builds for each of them.
    fn buffer_records<F>(
        &mut self,
        data: &RecordBatch,
        metadata: &Option<ConnectorMetadata>,
        update: F,
    ) -> AnyResult<()>
    where
        F: FnMut(T) -> MockUpdate<T, U>,
    {
        let deserializer = ArrowDeserializer::from_record_batch(data)?;
        let deserializer =
            &mut <dyn ErasedDeserializer>::erase(deserializer) as &mut dyn ErasedDeserializer;

        let records = Vec::<T>::deserialize_with_context_aux(
            deserializer,
            &self.config,
            self.metadata.get(metadata),
        )?;
        self.buffer.updates.extend(records.into_iter().map(update));
        self.buffer.n_bytes += data.get_array_memory_size();

        Ok(())
    }
}

impl<T, U> ArrowStream for MockZSetArrowStream<T, U, SqlSerdeConfig>
where
    T: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, SqlVariant>
        + Hash
        + Send
        + Sync
        + Debug
        + Clone
        + 'static,
    U: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, SqlVariant>
        + Hash
        + Send
        + Sync
        + Debug
        + Clone
        + 'static,
{
    fn insert(
        &mut self,
        data: &RecordBatch,
        metadata: &Option<ConnectorMetadata>,
    ) -> AnyResult<()> {
        self.buffer_records(data, metadata, |record| {
            MockUpdate::<T, U>::with_polarity(record, true)
        })
    }

    fn delete(
        &mut self,
        data: &RecordBatch,
        metadata: &Option<ConnectorMetadata>,
    ) -> AnyResult<()> {
        self.buffer_records(data, metadata, |record| {
            MockUpdate::<T, U>::with_polarity(record, false)
        })
    }

    fn insert_with_polarities(
        &mut self,
        data: &arrow::array::RecordBatch,
        polarities: &[bool],
        metadata: &Option<ConnectorMetadata>,
    ) -> AnyResult<()> {
        if polarities.len() != data.num_rows() {
            bail!(
                "insert_with_polarities: RecordBatch contains {} records, but 'polarities' array has length {}",
                data.num_rows(),
                polarities.len()
            );
        }

        let mut polarities = polarities.iter();
        self.buffer_records(data, metadata, move |record| {
            MockUpdate::<T, U>::with_polarity(record, *polarities.next().unwrap())
        })
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
    schema: InstalledAvroSchema,
    metadata: MetadataVariant,
}

#[cfg(feature = "with-avro")]
impl<T, U> MockAvroStream<T, U> {
    fn new(handle: MockDeZSet<T, U>) -> Self {
        Self {
            updates: Vec::new(),
            n_bytes: 0,
            handle,
            schema: InstalledAvroSchema::default(),
            metadata: MetadataVariant::default(),
        }
    }
}

#[cfg(feature = "with-avro")]
impl<T, U> AvroStream for MockAvroStream<T, U>
where
    T: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, SqlVariant>
        + Hash
        + Send
        + Sync
        + 'static,
    U: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, SqlVariant>
        + Hash
        + Send
        + Sync
        + 'static,
{
    fn configure_schema(&mut self, schema: &AvroSchema, refs: &AvroSchemaRefs) -> AnyResult<()> {
        self.schema.install(schema, refs);
        Ok(())
    }

    fn insert(
        &mut self,
        data: &AvroValue,
        n_bytes: usize,
        metadata: &Option<ConnectorMetadata>,
    ) -> AnyResult<()> {
        let (schema, refs) = self.schema.get()?;
        let v: T = from_avro_value(data, schema, refs, self.metadata.get(metadata))
            .map_err(|e| anyhow::anyhow!("error deserializing Avro record: {e}"))?;
        self.updates.push(MockUpdate::Insert(v));
        self.n_bytes += n_bytes;
        Ok(())
    }

    fn delete(
        &mut self,
        data: &AvroValue,
        n_bytes: usize,
        metadata: &Option<ConnectorMetadata>,
    ) -> AnyResult<()> {
        let (schema, refs) = self.schema.get()?;
        let v: T = from_avro_value(data, schema, refs, self.metadata.get(metadata))
            .map_err(|e| anyhow::anyhow!("error deserializing Avro record: {e}"))?;

        self.updates.push(MockUpdate::Delete(v));
        self.n_bytes += n_bytes;
        Ok(())
    }

    fn fork(&self) -> Box<dyn AvroStream> {
        Box::new(Self {
            schema: self.schema.clone(),
            ..Self::new(self.handle.clone())
        })
    }

    fn stage(&self, buffers: Vec<Box<dyn InputBuffer>>) -> Box<dyn StagedBuffers> {
        Box::new(MockDeZSetStreamStagedBuffers::new(buffers, &self.handle))
    }
}

#[cfg(feature = "with-avro")]
impl<T, U> InputBuffer for MockAvroStream<T, U>
where
    T: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, SqlVariant>
        + Hash
        + Send
        + Sync
        + 'static,
    U: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, SqlVariant>
        + Hash
        + Send
        + Sync
        + 'static,
{
    fn flush(&mut self) -> AnyResult<()> {
        let mut state = self.handle.0.lock().unwrap();
        state.flushed.append(&mut self.updates);
        Ok(())
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
