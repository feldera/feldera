#[cfg(feature = "with-avro")]
use crate::catalog::AvroStream;
#[cfg(feature = "with-avro")]
use crate::format::avro::from_avro_value;
use crate::format::csv::{csv_reader_builder, deserializer::ByteRecordDeserializer};
use crate::format::raw::{RawDeserializer, raw_serde_config};
use crate::{
    ControllerError, DeCollectionHandle,
    catalog::{DeCollectionStream, RecordFormat},
    format::byte_record_deserializer,
};
use crate::{catalog::ArrowStream, format::InputBuffer};
use anyhow::{Result as AnyResult, anyhow, bail};
#[cfg(feature = "with-avro")]
use apache_avro::{Schema as AvroSchema, types::Value as AvroValue};
use arrow::array::RecordBatch;
use dbsp::dynamic::Data;
use dbsp::operator::StagedBuffers;
use dbsp::{
    DBData, InputHandle, LazyMapHandle, MapHandle, ZSetHandle, ZWeight, algebra::HasOne,
    operator::Update, utils::Tup2,
};
use erased_serde::Deserializer as ErasedDeserializer;
#[cfg(feature = "with-avro")]
use feldera_adapterlib::catalog::AvroSchemaRefs;
use feldera_adapterlib::format::{BufferSize, flatten_nested};
use feldera_sqllib::Variant;
use feldera_types::format::csv::CsvFormatConfig;
use feldera_types::serde_with_context::{DeserializeWithContext, SqlSerdeConfig};
use serde_arrow::Deserializer as ArrowDeserializer;
use serde_json::de::SliceRead;
use std::hash::Hasher;
use std::iter::zip;
use std::{collections::VecDeque, marker::PhantomData, ops::Neg};

// The following functions are needed to force the erased deserializer implementation
// for the Arrow, CSV, JSON, and raw formats to get monomorphized in the adapters crate
// rather than in every crate that uses it.

#[inline(never)]
pub fn arrow_deserializer<'a>(
    data: &'a RecordBatch,
) -> Result<Box<dyn ErasedDeserializer<'a> + 'a>, serde_arrow::Error> {
    let deserializer = ArrowDeserializer::from_record_batch(data)?;

    Ok(Box::new(<dyn ErasedDeserializer<'a>>::erase(deserializer))
        as Box<dyn ErasedDeserializer<'a>>)
}

#[inline(never)]
pub fn csv_deserializer<'a>(
    deserializer: &'a mut ByteRecordDeserializer<'a>,
) -> Box<dyn ErasedDeserializer<'a> + 'a> {
    Box::new(<dyn ErasedDeserializer<'a>>::erase(deserializer))
}

#[inline(never)]
pub fn json_deserializer<'a>(
    deserializer: &'a mut serde_json::Deserializer<SliceRead<'a>>,
) -> Box<dyn ErasedDeserializer<'a> + 'a> {
    Box::new(<dyn ErasedDeserializer<'a>>::erase(deserializer))
}

#[inline(never)]
pub fn raw_deserializer<'a>(
    column_name: &'a str,
    data: &'a [u8],
) -> Box<dyn ErasedDeserializer<'a> + 'a> {
    let deserializer = RawDeserializer::new(column_name, data);
    Box::new(<dyn ErasedDeserializer<'a>>::erase(deserializer))
}

/// A deserializer that parses byte arrays into a strongly typed representation.
pub trait DeserializerFromBytes<C> {
    /// Create an instance of a deserializer.
    fn create(config: C) -> Self;

    /// Parse an object of type `T` from `data`.
    ///
    /// * `metadata` - optional record metadata passed as an aux argument to
    ///   `DeserializeWithContext::deserialize_with_context_aux`.
    fn deserialize<T>(&mut self, data: &[u8], metadata: &Option<Variant>) -> AnyResult<T>
    where
        T: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant>;
}

/// Deserializer for CSV-encoded data.
pub struct CsvDeserializerFromBytes {
    // CSV deserializer maintains some allocations across invocations,
    // so we keep an instance here.
    reader: csv::Reader<VecDeque<u8>>,
    // Byte record to read CSV records into.
    record: csv::ByteRecord,
    config: SqlSerdeConfig,
}

impl DeserializerFromBytes<(SqlSerdeConfig, CsvFormatConfig)> for CsvDeserializerFromBytes {
    fn create((serde_config, csv_config): (SqlSerdeConfig, CsvFormatConfig)) -> Self {
        CsvDeserializerFromBytes {
            // `csv_reader_builder` sets `has_headers(false)` — the adapter
            // layer handles header-row skipping itself.
            reader: csv_reader_builder(&csv_config).from_reader(VecDeque::new()),
            record: csv::ByteRecord::new(),
            config: serde_config,
        }
    }
    fn deserialize<T>(&mut self, data: &[u8], metadata: &Option<Variant>) -> AnyResult<T>
    where
        T: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant>,
    {
        // Push new data to reader.
        self.reader.get_mut().extend(data.iter());
        self.reader.read_byte_record(&mut self.record)?;

        let mut deserializer = byte_record_deserializer(&self.record, None);
        let deserializer = csv_deserializer(&mut deserializer);

        T::deserialize_with_context_aux(deserializer, &self.config, metadata)
            .map_err(|e| anyhow!(e.to_string()))
    }
}

// Deserializer for JSON-encoded data.
pub struct JsonDeserializerFromBytes {
    config: SqlSerdeConfig,
}

impl DeserializerFromBytes<SqlSerdeConfig> for JsonDeserializerFromBytes {
    fn create(config: SqlSerdeConfig) -> Self {
        JsonDeserializerFromBytes { config }
    }
    fn deserialize<T>(&mut self, data: &[u8], metadata: &Option<Variant>) -> AnyResult<T>
    where
        T: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant>,
    {
        let mut deserializer = serde_json::Deserializer::from_slice(data);
        let deserializer = json_deserializer(&mut deserializer);

        T::deserialize_with_context_aux(deserializer, &self.config, metadata)
            .map_err(|e| anyhow!(e.to_string()))
    }
}

pub struct RawDeserializerFromBytes {
    column_name: String,
    config: SqlSerdeConfig,
}

impl DeserializerFromBytes<(SqlSerdeConfig, String)> for RawDeserializerFromBytes {
    fn create((config, column_name): (SqlSerdeConfig, String)) -> Self {
        RawDeserializerFromBytes {
            column_name,
            config,
        }
    }
    fn deserialize<T>(&mut self, data: &[u8], metadata: &Option<Variant>) -> AnyResult<T>
    where
        T: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant>,
    {
        let deserializer = raw_deserializer(&self.column_name, data);

        T::deserialize_with_context_aux(deserializer, &self.config, metadata)
            .map_err(|e| anyhow!(e.to_string()))
    }
}

/// An input handle that allows pushing serialized data to an
/// [`InputHandle`].
pub trait DeScalarHandle: Send + Sync {
    /// Create a [`DeScalarStream`] object to parse input data encoded
    /// using the format specified in `RecordFormat`.
    fn configure_deserializer(
        &self,
        record_format: RecordFormat,
    ) -> Result<Box<dyn DeScalarStream>, ControllerError>;
}

/// An input handle that deserializes values before pushing them to
/// a stream.
///
/// Handles of this type are produced by the
/// [`DeScalarHandle::configure_deserializer`] method.
///
/// A trait for a type that wraps around an [`InputHandle`] and
/// pushes serialized data to an input stream.  The client of this trait
/// supplies values serialized using the format specified as an argument
/// to [`DeScalarHandle::configure_deserializer`].
///
/// Note: this trait is useful for feeding scalar values to the circuit.
/// Use [`DeCollectionStream`] for relational data.
pub trait DeScalarStream: Send {
    /// Deserialize an input value and push it to the circuit via
    /// [`InputHandle::set_for_worker`].
    ///
    /// Returns an error if deserialization fails, i.e., the serialized
    /// representation is corrupted or does not match the type of the underlying
    /// input stream.
    fn set_for_worker(&mut self, worker: usize, data: &[u8]) -> AnyResult<()>;

    /// Deserialize an input value and push it to the circuit via
    /// [`InputHandle::set_for_all`].
    ///
    /// Returns an error if deserialization fails, i.e., the serialized
    /// representation is corrupted or does not match the type of the underlying
    /// input stream.
    fn set_for_all(&mut self, data: &[u8]) -> AnyResult<()>;

    /// Invoke [`InputHandle::clear_for_all`].
    fn clear_for_all(&mut self);

    /// Create a new handle connected to the same input stream.
    fn fork(&self) -> Box<dyn DeScalarStream>;
}

#[derive(Clone)]
pub struct DeScalarHandleImpl<T, D, F> {
    handle: InputHandle<T>,
    map_func: F,
    phantom: PhantomData<fn(D)>,
}

impl<T, D, F> DeScalarHandleImpl<T, D, F> {
    pub fn new(handle: InputHandle<T>, map_func: F) -> Self {
        Self {
            handle,
            map_func,
            phantom: PhantomData,
        }
    }
}

impl<T, D, F> DeScalarHandle for DeScalarHandleImpl<T, D, F>
where
    T: Default + Send + Clone + 'static,
    D: Default
        + for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant>
        + Send
        + Clone
        + 'static,
    F: Fn(D) -> T + Send + Sync + Clone + 'static,
{
    fn configure_deserializer(
        &self,
        record_format: RecordFormat,
    ) -> Result<Box<dyn DeScalarStream>, ControllerError> {
        match record_format {
            RecordFormat::Csv(delimiter) => {
                let config = SqlSerdeConfig::default();
                Ok(Box::new(DeScalarStreamImpl::<
                    CsvDeserializerFromBytes,
                    T,
                    D,
                    _,
                    _,
                >::new(
                    self.handle.clone(),
                    self.map_func.clone(),
                    (config, delimiter),
                )))
            }
            RecordFormat::Json(flavor) => {
                let config = SqlSerdeConfig::from(flavor);
                Ok(Box::new(DeScalarStreamImpl::<
                    JsonDeserializerFromBytes,
                    T,
                    D,
                    _,
                    _,
                >::new(
                    self.handle.clone(),
                    self.map_func.clone(),
                    config,
                )))
            }
            RecordFormat::Raw(column_name) => {
                let config = raw_serde_config();
                Ok(Box::new(DeScalarStreamImpl::<
                    RawDeserializerFromBytes,
                    T,
                    D,
                    _,
                    _,
                >::new(
                    self.handle.clone(),
                    self.map_func.clone(),
                    (config, column_name.clone()),
                )))
            }
            RecordFormat::Parquet(_) => {
                todo!()
            }
            #[cfg(feature = "with-avro")]
            RecordFormat::Avro => {
                todo!()
            }
            #[cfg(feature = "with-dynamodb")]
            RecordFormat::DynamoDB => {
                unreachable!("DynamoDB is an output-only format")
            }
        }
    }
}

struct DeScalarStreamImpl<De, T, D, C, F> {
    handle: InputHandle<T>,
    deserializer: De,
    map_func: F,
    config: C,
    phantom: PhantomData<fn(D)>,
}

impl<De, T, D, C, F> DeScalarStreamImpl<De, T, D, C, F>
where
    De: DeserializerFromBytes<C> + Send,
    C: Clone,
{
    fn new(handle: InputHandle<T>, map_func: F, config: C) -> Self {
        Self {
            handle,
            deserializer: De::create(config.clone()),
            map_func,
            config,
            phantom: PhantomData,
        }
    }
}

impl<De, T, D, C, F> DeScalarStream for DeScalarStreamImpl<De, T, D, C, F>
where
    T: Default + Send + Clone + 'static,
    D: Default
        + for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant>
        + Send
        + Clone
        + 'static,
    De: DeserializerFromBytes<C> + Send + 'static,
    C: Clone + Send + 'static,
    F: Fn(D) -> T + Clone + Send + 'static,
{
    fn set_for_worker(&mut self, worker: usize, data: &[u8]) -> AnyResult<()> {
        let val = self.deserializer.deserialize(data, &None)?;
        self.handle.set_for_worker(worker, (self.map_func)(val));
        Ok(())
    }

    fn set_for_all(&mut self, data: &[u8]) -> AnyResult<()> {
        let val = self.deserializer.deserialize(data, &None)?;
        self.handle.set_for_all((self.map_func)(val));
        Ok(())
    }

    fn clear_for_all(&mut self) {
        self.handle.clear_for_all()
    }

    fn fork(&self) -> Box<dyn DeScalarStream> {
        Box::new(Self::new(
            self.handle.clone(),
            self.map_func.clone(),
            self.config.clone(),
        ))
    }
}

pub struct DeZSetHandle<K, D> {
    handle: ZSetHandle<K>,
    phantom: PhantomData<D>,
}

impl<K, D> Clone for DeZSetHandle<K, D> {
    fn clone(&self) -> Self {
        Self {
            handle: self.handle.clone(),
            phantom: PhantomData,
        }
    }
}

impl<K, D> DeZSetHandle<K, D> {
    pub fn new(handle: ZSetHandle<K>) -> Self {
        Self {
            handle,
            phantom: PhantomData,
        }
    }
}

impl<K, D> DeCollectionHandle for DeZSetHandle<K, D>
where
    K: DBData + From<D>,
    D: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant> + Send + Sync + 'static,
{
    fn configure_deserializer(
        &self,
        record_format: RecordFormat,
    ) -> Result<Box<dyn DeCollectionStream>, ControllerError> {
        match record_format {
            RecordFormat::Csv(config) => {
                Ok(Box::new(
                    DeZSetStream::<CsvDeserializerFromBytes, K, D, _>::new(
                        self.handle.clone(),
                        (SqlSerdeConfig::default(), config),
                    ),
                ))
            }
            RecordFormat::Json(flavor) => {
                let config = SqlSerdeConfig::from(flavor);
                Ok(Box::new(
                    DeZSetStream::<JsonDeserializerFromBytes, K, D, _>::new(
                        self.handle.clone(),
                        config,
                    ),
                ))
            }
            RecordFormat::Parquet(_) => {
                todo!()
            }
            #[cfg(feature = "with-avro")]
            RecordFormat::Avro => {
                todo!()
            }
            RecordFormat::Raw(column_name) => {
                let config = raw_serde_config();
                Ok(Box::new(
                    DeZSetStream::<RawDeserializerFromBytes, K, D, _>::new(
                        self.handle.clone(),
                        (config, column_name.clone()),
                    ),
                ))
            }
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
        Ok(Box::new(ArrowZSetStream::new(self.handle.clone(), config)))
    }

    #[cfg(feature = "with-avro")]
    fn configure_avro_deserializer(&self) -> Result<Box<dyn AvroStream>, ControllerError> {
        Ok(Box::new(AvroZSetStream::new(self.handle.clone())))
    }

    fn fork(&self) -> Box<dyn DeCollectionHandle> {
        Box::new(self.clone())
    }
}

struct DeZSetStreamBuffer<K> {
    // VecDeque is preferable to Vec here as it allows splitting off a small
    // prefix of a large buffer efficiently (see take_some).
    updates: VecDeque<Tup2<K, ZWeight>>,
    n_bytes: usize,
    handle: ZSetHandle<K>,
}

impl<K> DeZSetStreamBuffer<K> {
    fn new(handle: ZSetHandle<K>) -> Self {
        Self {
            updates: VecDeque::new(),
            n_bytes: 0,
            handle,
        }
    }
}

impl<K> InputBuffer for DeZSetStreamBuffer<K>
where
    K: DBData,
{
    fn flush(&mut self) {
        let updates = std::mem::take(&mut self.updates);
        self.handle.append(&mut updates.into());
        self.n_bytes = 0;
    }

    fn hash(&self, hasher: &mut dyn Hasher) {
        for update in &self.updates {
            hasher.write_u64(update.default_hash())
        }
    }

    fn len(&self) -> BufferSize {
        BufferSize {
            records: self.updates.len(),
            bytes: self.n_bytes,
        }
    }

    fn take_some(&mut self, n: usize) -> Option<Box<dyn InputBuffer>> {
        if !self.updates.is_empty() {
            let n = self.updates.len().min(n);
            Some(Box::new(Self {
                n_bytes: fraction_take(n, self.updates.len(), &mut self.n_bytes),
                // Draining a small prefix of a VecDeque is efficient as it doesn't require
                // shifting the rest of the vector.
                updates: self.updates.drain(0..n).collect(),
                handle: self.handle.clone(),
            }))
        } else {
            None
        }
    }
}

/// An input handle that wraps a [`MapHandle<K, R>`](`MapHandle`)
/// returned by
/// [`RootCircuit::add_input_zset`](`dbsp::RootCircuit::add_input_zset`).
///
/// The [`insert`](`Self::insert`) method of this handle deserializes value
/// `d` of type `D`, converts it to value `k: K` using the `From` trait and
/// buffers a `(k, +1)` update for the underlying `CollectionHandle`.
///
/// The [`delete`](`Self::delete`) method of this handle buffers a `(k, -1)`
/// update for the underlying `CollectionHandle`.
pub struct DeZSetStream<De, K, D, C> {
    buffer: DeZSetStreamBuffer<K>,
    deserializer: De,
    config: C,
    phantom: PhantomData<D>,
}

impl<De, K, D, C> DeZSetStream<De, K, D, C>
where
    De: DeserializerFromBytes<C>,
    C: Clone,
{
    pub fn new(handle: ZSetHandle<K>, config: C) -> Self {
        Self {
            buffer: DeZSetStreamBuffer::new(handle),
            deserializer: De::create(config.clone()),
            config,
            phantom: PhantomData,
        }
    }
}

impl<De, K, D, C> DeCollectionStream for DeZSetStream<De, K, D, C>
where
    De: DeserializerFromBytes<C> + Send + Sync + 'static,
    C: Clone + Send + Sync + 'static,
    K: DBData + From<D>,
    D: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant> + Send + Sync + 'static,
{
    fn insert(&mut self, data: &[u8], metadata: &Option<Variant>) -> AnyResult<()> {
        let key = <K as From<D>>::from(self.deserializer.deserialize::<D>(data, metadata)?);
        self.buffer.updates.push_back(Tup2(key, ZWeight::one()));
        self.buffer.n_bytes += data.len();
        Ok(())
    }

    fn delete(&mut self, data: &[u8], metadata: &Option<Variant>) -> AnyResult<()> {
        let key = <K as From<D>>::from(self.deserializer.deserialize::<D>(data, metadata)?);

        self.buffer
            .updates
            .push_back(Tup2(key, ZWeight::one().neg()));
        self.buffer.n_bytes += data.len();
        Ok(())
    }

    fn update(&mut self, _data: &[u8], _metadata: &Option<Variant>) -> AnyResult<()> {
        bail!("update operation is not supported on this stream")
    }

    fn reserve(&mut self, reservation: usize) {
        self.buffer.updates.reserve(reservation);
    }

    fn fork(&self) -> Box<dyn DeCollectionStream> {
        Box::new(Self::new(self.buffer.handle.clone(), self.config.clone()))
    }

    fn truncate(&mut self, len: usize) {
        if len < self.buffer.updates.len() {
            self.buffer.n_bytes = fraction(len, self.buffer.updates.len(), self.buffer.n_bytes);
            self.buffer.updates.truncate(len);
        }
    }

    fn stage(&self, buffers: Vec<Box<dyn InputBuffer>>) -> Box<dyn StagedBuffers> {
        Box::new(
            self.buffer.handle.stage(
                flatten_nested::<DeZSetStreamBuffer<K>>(buffers)
                    .into_iter()
                    .map(|buffer| buffer.updates),
            ),
        )
    }
}

/// Returns `(num/denom) * mul` as if in floating point, rounding down.  To
/// ensure that the result is representable, `denom` must be greater or equal to
/// `num`.
///
/// This function is meant for dividing `mul` into two pieces: one corresponding
/// to the share `num/denom`, the other corresponding to `1 - (num/denom)`.
/// Thus, for `denom == 0`, it returns 0, which works as well as any other
/// result in `0..=mul` for that purpose.
pub fn fraction(num: usize, denom: usize, mul: usize) -> usize {
    debug_assert!(num <= denom);
    if denom != 0 {
        ((num as u128 * mul as u128) / (denom as u128))
            .try_into()
            .unwrap()
    } else {
        0
    }
}

/// Divides `*mul` into two pieces: one corresponding to the share `num/denom`,
/// the other corresponding to `1 - (num/denom)`.  Returns the former share and
/// sets `*mul` to the latter.
pub fn fraction_take(num: usize, denom: usize, mul: &mut usize) -> usize {
    let head = fraction(num, denom, *mul);
    *mul -= head;
    head
}

impl<De, K, D, C> InputBuffer for DeZSetStream<De, K, D, C>
where
    De: DeserializerFromBytes<C> + Send + 'static,
    C: Clone + Send + 'static,
    K: DBData + From<D>,
    D: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant> + Send + 'static,
{
    fn flush(&mut self) {
        self.buffer.flush()
    }

    fn len(&self) -> BufferSize {
        self.buffer.len()
    }

    fn hash(&self, hasher: &mut dyn Hasher) {
        self.buffer.hash(hasher)
    }

    fn take_some(&mut self, n: usize) -> Option<Box<dyn InputBuffer>> {
        self.buffer.take_some(n)
    }
}

pub struct ArrowZSetStream<K, D, C> {
    buffer: DeZSetStreamBuffer<K>,
    config: C,
    phantom: PhantomData<D>,
}

impl<K, D, C> ArrowZSetStream<K, D, C> {
    pub fn new(handle: ZSetHandle<K>, config: C) -> Self {
        Self {
            buffer: DeZSetStreamBuffer::new(handle),
            config,
            phantom: PhantomData,
        }
    }
}

impl<K, D, C> ArrowStream for ArrowZSetStream<K, D, C>
where
    K: DBData + From<D>,
    D: for<'de> DeserializeWithContext<'de, C, Variant> + Send + Sync + 'static,
    C: Clone + Send + Sync + 'static,
{
    fn insert(&mut self, data: &RecordBatch, metadata: &Option<Variant>) -> AnyResult<()> {
        let deserializer = arrow_deserializer(data)?;

        let records = Vec::<D>::deserialize_with_context_aux(deserializer, &self.config, metadata)?;
        self.buffer
            .updates
            .extend(records.into_iter().map(|r| Tup2(K::from(r), 1)));
        self.buffer.n_bytes += data.get_array_memory_size();

        Ok(())
    }

    fn delete(&mut self, data: &RecordBatch, metadata: &Option<Variant>) -> AnyResult<()> {
        let deserializer = arrow_deserializer(data)?;

        let records = Vec::<D>::deserialize_with_context_aux(deserializer, &self.config, metadata)?;
        self.buffer
            .updates
            .extend(records.into_iter().map(|r| Tup2(K::from(r), -1)));
        self.buffer.n_bytes += data.get_array_memory_size();

        Ok(())
    }

    fn insert_with_polarities(
        &mut self,
        data: &RecordBatch,
        polarities: &[bool],
        metadata: &Option<Variant>,
    ) -> AnyResult<()> {
        if polarities.len() != data.num_rows() {
            // This should never happen. We could use an assertion here, but since the `polarities` array
            // is computed by datafusion, we'll throw an error just to be safe.
            bail!(
                "insert_with_polarities: RecordBatch contains {} records, but 'polarities' array has length {}",
                data.num_rows(),
                polarities.len()
            );
        }

        let deserializer = arrow_deserializer(data)?;

        let records = Vec::<D>::deserialize_with_context_aux(deserializer, &self.config, metadata)?;

        self.buffer.updates.extend(
            zip(records, polarities)
                .map(|(record, polarity)| Tup2(K::from(record), if *polarity { 1 } else { -1 })),
        );
        self.buffer.n_bytes += data.get_array_memory_size();

        Ok(())
    }

    fn fork(&self) -> Box<dyn ArrowStream> {
        Box::new(Self::new(self.buffer.handle.clone(), self.config.clone()))
    }

    fn stage(&self, buffers: Vec<Box<dyn InputBuffer>>) -> Box<dyn StagedBuffers> {
        Box::new(
            self.buffer.handle.stage(
                flatten_nested::<DeZSetStreamBuffer<K>>(buffers)
                    .into_iter()
                    .map(|buffer| buffer.updates),
            ),
        )
    }
}

impl<K, D, C> InputBuffer for ArrowZSetStream<K, D, C>
where
    K: DBData + From<D>,
    D: for<'de> DeserializeWithContext<'de, C, Variant> + Send + 'static,
    C: Clone + Send + 'static,
{
    fn flush(&mut self) {
        self.buffer.flush()
    }

    fn take_some(&mut self, n: usize) -> Option<Box<dyn InputBuffer>> {
        self.buffer.take_some(n)
    }

    fn len(&self) -> BufferSize {
        self.buffer.len()
    }

    fn hash(&self, hasher: &mut dyn Hasher) {
        self.buffer.hash(hasher)
    }
}

/// `AvroStream` implementation that collects deserialized records
/// into a Z-set.
#[cfg(feature = "with-avro")]
pub struct AvroZSetStream<K, D> {
    buffer: DeZSetStreamBuffer<K>,
    phantom: PhantomData<D>,
}

#[cfg(feature = "with-avro")]
impl<K, D> Clone for AvroZSetStream<K, D> {
    fn clone(&self) -> Self {
        Self::new(self.buffer.handle.clone())
    }
}

#[cfg(feature = "with-avro")]
impl<K, D> AvroZSetStream<K, D> {
    pub fn new(handle: ZSetHandle<K>) -> Self {
        Self {
            buffer: DeZSetStreamBuffer::new(handle),
            phantom: PhantomData,
        }
    }
}

#[cfg(feature = "with-avro")]
impl<K, D> AvroStream for AvroZSetStream<K, D>
where
    K: DBData + From<D>,
    D: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant> + Send + Sync + 'static,
{
    fn insert(
        &mut self,
        data: &AvroValue,
        schema: &AvroSchema,
        refs: &AvroSchemaRefs,
        n_bytes: usize,
        metadata: &Option<Variant>,
    ) -> AnyResult<()> {
        let v: D = from_avro_value(data, schema, refs, metadata)
            .map_err(|e| anyhow!("error deserializing Avro record: {e}"))?;

        self.buffer.updates.push_back(Tup2(K::from(v), 1));
        self.buffer.n_bytes += n_bytes;

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
        let v: D = from_avro_value(data, schema, refs, metadata)
            .map_err(|e| anyhow!("error deserializing Avro record: {e}"))?;

        self.buffer.updates.push_back(Tup2(K::from(v), -1));
        self.buffer.n_bytes += n_bytes;

        Ok(())
    }

    fn fork(&self) -> Box<dyn AvroStream> {
        Box::new(self.clone())
    }

    fn stage(&self, buffers: Vec<Box<dyn InputBuffer>>) -> Box<dyn StagedBuffers> {
        Box::new(
            self.buffer.handle.stage(
                flatten_nested::<DeZSetStreamBuffer<K>>(buffers)
                    .into_iter()
                    .map(|buffer| buffer.updates),
            ),
        )
    }
}

#[cfg(feature = "with-avro")]
impl<K, D> InputBuffer for AvroZSetStream<K, D>
where
    K: DBData + From<D>,
    D: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant> + Send + Sync + 'static,
{
    fn flush(&mut self) {
        self.buffer.flush()
    }

    fn take_some(&mut self, n: usize) -> Option<Box<dyn InputBuffer>> {
        self.buffer.take_some(n)
    }

    fn len(&self) -> BufferSize {
        self.buffer.len()
    }

    fn hash(&self, hasher: &mut dyn Hasher) {
        self.buffer.hash(hasher)
    }
}

pub struct DeMapHandle<K, KD, V, VD, U, UD, VF, UF>
where
    V: DBData,
    U: DBData,
{
    handle: MapHandle<K, V, U>,
    value_key_func: VF,
    update_key_func: UF,
    phantom: PhantomData<fn(KD, VD, UD)>,
}

impl<K, KD, V, VD, U, UD, VF, UF> Clone for DeMapHandle<K, KD, V, VD, U, UD, VF, UF>
where
    V: DBData,
    U: DBData,
    VF: Clone,
    UF: Clone,
{
    fn clone(&self) -> Self {
        Self {
            handle: self.handle.clone(),
            value_key_func: self.value_key_func.clone(),
            update_key_func: self.update_key_func.clone(),
            phantom: PhantomData,
        }
    }
}

impl<K, KD, V, VD, U, UD, VF, UF> DeMapHandle<K, KD, V, VD, U, UD, VF, UF>
where
    V: DBData,
    U: DBData,
{
    pub fn new(handle: MapHandle<K, V, U>, value_key_func: VF, update_key_func: UF) -> Self {
        Self {
            handle,
            value_key_func,
            update_key_func,
            phantom: PhantomData,
        }
    }
}

impl<K, KD, V, VD, U, UD, VF, UF> DeCollectionHandle for DeMapHandle<K, KD, V, VD, U, UD, VF, UF>
where
    K: DBData + From<KD>,
    KD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant> + Send + Sync + 'static,
    V: DBData + From<VD>,
    VD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant> + Send + Sync + 'static,
    U: DBData + From<UD>,
    UD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant> + Send + Sync + 'static,
    VF: Fn(&V) -> K + Clone + Send + Sync + 'static,
    UF: Fn(&U) -> K + Clone + Send + Sync + 'static,
{
    fn configure_deserializer(
        &self,
        record_format: RecordFormat,
    ) -> Result<Box<dyn DeCollectionStream>, ControllerError> {
        match record_format {
            RecordFormat::Csv(config) => Ok(Box::new(DeMapStream::<
                CsvDeserializerFromBytes,
                K,
                KD,
                V,
                VD,
                U,
                UD,
                VF,
                UF,
                _,
                MapHandle<K, V, U>,
            >::new(
                self.handle.clone(),
                self.value_key_func.clone(),
                self.update_key_func.clone(),
                (SqlSerdeConfig::default(), config),
            ))),
            RecordFormat::Json(flavor) => Ok(Box::new(DeMapStream::<
                JsonDeserializerFromBytes,
                K,
                KD,
                V,
                VD,
                U,
                UD,
                VF,
                UF,
                _,
                MapHandle<K, V, U>,
            >::new(
                self.handle.clone(),
                self.value_key_func.clone(),
                self.update_key_func.clone(),
                SqlSerdeConfig::from(flavor),
            ))),
            RecordFormat::Parquet(_) => {
                todo!()
            }
            #[cfg(feature = "with-avro")]
            RecordFormat::Avro => {
                todo!()
            }
            RecordFormat::Raw(column_name) => Ok(Box::new(DeMapStream::<
                RawDeserializerFromBytes,
                K,
                KD,
                V,
                VD,
                U,
                UD,
                VF,
                UF,
                _,
                MapHandle<K, V, U>,
            >::new(
                self.handle.clone(),
                self.value_key_func.clone(),
                self.update_key_func.clone(),
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
        Ok(Box::new(ArrowMapStream::new(
            self.handle.clone(),
            self.value_key_func.clone(),
            config,
        )))
    }

    #[cfg(feature = "with-avro")]
    fn configure_avro_deserializer(&self) -> Result<Box<dyn AvroStream>, ControllerError> {
        Ok(Box::new(AvroMapStream::new(
            self.handle.clone(),
            self.value_key_func.clone(),
        )))
    }

    fn fork(&self) -> Box<dyn DeCollectionHandle> {
        let clone: Self = self.clone();
        Box::new(clone)
    }
}

/// The lazy counterpart of [`DeMapHandle`].
///
/// It wraps a [`LazyMapHandle`], so the table takes writes and deletes and
/// refuses update records, and it needs no update type or update key function.
pub struct DeLazyMapHandle<K, KD, V, VD, VF>
where
    V: DBData,
{
    handle: LazyMapHandle<K, V>,
    value_key_func: VF,
    phantom: PhantomData<fn(KD, VD)>,
}

impl<K, KD, V, VD, VF> Clone for DeLazyMapHandle<K, KD, V, VD, VF>
where
    V: DBData,
    VF: Clone,
{
    fn clone(&self) -> Self {
        Self {
            handle: self.handle.clone(),
            value_key_func: self.value_key_func.clone(),
            phantom: PhantomData,
        }
    }
}

impl<K, KD, V, VD, VF> DeLazyMapHandle<K, KD, V, VD, VF>
where
    V: DBData,
{
    pub fn new(handle: LazyMapHandle<K, V>, value_key_func: VF) -> Self {
        Self {
            handle,
            value_key_func,
            phantom: PhantomData,
        }
    }
}

/// The streams are the eager map's, with the value type standing in for the
/// update type they never see: [`MapInput::TAKES_UPDATES`] is false for the
/// lazy handle, so an update record is refused before it is deserialized.
impl<K, KD, V, VD, VF> DeCollectionHandle for DeLazyMapHandle<K, KD, V, VD, VF>
where
    K: DBData + From<KD>,
    KD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant> + Send + Sync + 'static,
    V: DBData + From<VD>,
    VD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant> + Send + Sync + 'static,
    VF: Fn(&V) -> K + Clone + Send + Sync + 'static,
{
    fn configure_deserializer(
        &self,
        record_format: RecordFormat,
    ) -> Result<Box<dyn DeCollectionStream>, ControllerError> {
        match record_format {
            RecordFormat::Csv(config) => Ok(Box::new(DeMapStream::<
                CsvDeserializerFromBytes,
                K,
                KD,
                V,
                VD,
                V,
                VD,
                VF,
                VF,
                _,
                LazyMapHandle<K, V>,
            >::new(
                self.handle.clone(),
                self.value_key_func.clone(),
                self.value_key_func.clone(),
                (SqlSerdeConfig::default(), config),
            ))),
            RecordFormat::Json(flavor) => Ok(Box::new(DeMapStream::<
                JsonDeserializerFromBytes,
                K,
                KD,
                V,
                VD,
                V,
                VD,
                VF,
                VF,
                _,
                LazyMapHandle<K, V>,
            >::new(
                self.handle.clone(),
                self.value_key_func.clone(),
                self.value_key_func.clone(),
                SqlSerdeConfig::from(flavor),
            ))),
            RecordFormat::Parquet(_) => {
                todo!()
            }
            #[cfg(feature = "with-avro")]
            RecordFormat::Avro => {
                todo!()
            }
            RecordFormat::Raw(column_name) => Ok(Box::new(DeMapStream::<
                RawDeserializerFromBytes,
                K,
                KD,
                V,
                VD,
                V,
                VD,
                VF,
                VF,
                _,
                LazyMapHandle<K, V>,
            >::new(
                self.handle.clone(),
                self.value_key_func.clone(),
                self.value_key_func.clone(),
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
        Ok(Box::new(ArrowMapStream::<K, KD, V, VD, V, VF, _, _>::new(
            self.handle.clone(),
            self.value_key_func.clone(),
            config,
        )))
    }

    #[cfg(feature = "with-avro")]
    fn configure_avro_deserializer(&self) -> Result<Box<dyn AvroStream>, ControllerError> {
        Ok(Box::new(AvroMapStream::<K, KD, V, VD, V, VF, _>::new(
            self.handle.clone(),
            self.value_key_func.clone(),
        )))
    }

    fn fork(&self) -> Box<dyn DeCollectionHandle> {
        Box::new(self.clone())
    }
}

/// An input map as the deserializing streams see it.
///
/// The eager map takes an [`Update`] per record; the lazy map takes writes and
/// deletes and nothing else, since it resolves a key by keeping its last
/// command, which a patch cannot express.  The streams buffer whichever
/// command the map takes and hand the buffers on unchanged.
pub trait MapInput<K, V, U>: Clone + Send + Sync + 'static
where
    K: DBData,
    V: DBData,
    U: DBData,
{
    /// What the map takes per record.
    type Command: DBData;

    /// Whether the map takes an update, or only writes and deletes.
    const TAKES_UPDATES: bool;

    /// The command that writes `value` at its key.
    fn insert(value: V) -> Self::Command;

    /// The command that deletes whatever the key holds.
    fn delete() -> Self::Command;

    /// The command that patches the value at the key, or `None` from a map
    /// that cannot.
    fn update(update: U) -> Option<Self::Command>;

    /// Appends the commands to the map.
    fn append(&mut self, commands: &mut Vec<Tup2<K, Self::Command>>);

    /// Partitions the commands for the circuit, to be flushed later.
    fn stage(
        &self,
        buffers: impl IntoIterator<Item = VecDeque<Tup2<K, Self::Command>>>,
    ) -> Box<dyn StagedBuffers>;
}

impl<K, V, U> MapInput<K, V, U> for MapHandle<K, V, U>
where
    K: DBData,
    V: DBData,
    U: DBData,
{
    type Command = Update<V, U>;

    const TAKES_UPDATES: bool = true;

    fn insert(value: V) -> Update<V, U> {
        Update::Insert(value)
    }

    fn delete() -> Update<V, U> {
        Update::Delete
    }

    fn update(update: U) -> Option<Update<V, U>> {
        Some(Update::Update(update))
    }

    fn append(&mut self, commands: &mut Vec<Tup2<K, Update<V, U>>>) {
        MapHandle::append(self, commands)
    }

    fn stage(
        &self,
        buffers: impl IntoIterator<Item = VecDeque<Tup2<K, Update<V, U>>>>,
    ) -> Box<dyn StagedBuffers> {
        Box::new(MapHandle::stage(self, buffers))
    }
}

/// `U` is the update type the handle never carries; it is there so that the
/// lazy handle fits the streams written for the eager one.
impl<K, V, U> MapInput<K, V, U> for LazyMapHandle<K, V>
where
    K: DBData,
    V: DBData,
    U: DBData,
{
    type Command = Option<V>;

    const TAKES_UPDATES: bool = false;

    fn insert(value: V) -> Option<V> {
        Some(value)
    }

    fn delete() -> Option<V> {
        None
    }

    fn update(_update: U) -> Option<Option<V>> {
        None
    }

    fn append(&mut self, commands: &mut Vec<Tup2<K, Option<V>>>) {
        LazyMapHandle::append(self, commands)
    }

    fn stage(
        &self,
        buffers: impl IntoIterator<Item = VecDeque<Tup2<K, Option<V>>>>,
    ) -> Box<dyn StagedBuffers> {
        Box::new(LazyMapHandle::stage(self, buffers))
    }
}

struct DeMapStreamBuffer<K, V, U, H>
where
    K: DBData,
    V: DBData,
    U: DBData,
    H: MapInput<K, V, U>,
{
    updates: VecDeque<Tup2<K, H::Command>>,
    n_bytes: usize,
    handle: H,
}

impl<K, V, U, H> DeMapStreamBuffer<K, V, U, H>
where
    K: DBData,
    V: DBData,
    U: DBData,
    H: MapInput<K, V, U>,
{
    fn new(handle: H) -> Self {
        Self {
            updates: VecDeque::new(),
            n_bytes: 0,
            handle,
        }
    }
}

impl<K, V, U, H> InputBuffer for DeMapStreamBuffer<K, V, U, H>
where
    K: DBData,
    V: DBData,
    U: DBData,
    H: MapInput<K, V, U>,
{
    fn flush(&mut self) {
        let updates = std::mem::take(&mut self.updates);
        self.handle.append(&mut updates.into());
        self.n_bytes = 0;
    }

    fn len(&self) -> BufferSize {
        BufferSize {
            records: self.updates.len(),
            bytes: self.n_bytes,
        }
    }

    fn hash(&self, hasher: &mut dyn Hasher) {
        for update in &self.updates {
            hasher.write_u64(update.default_hash())
        }
    }

    fn take_some(&mut self, n: usize) -> Option<Box<dyn InputBuffer>> {
        if !self.updates.is_empty() {
            let n = self.updates.len().min(n);
            Some(Box::new(DeMapStreamBuffer {
                n_bytes: fraction_take(n, self.updates.len(), &mut self.n_bytes),
                updates: self.updates.drain(0..n).collect(),
                handle: self.handle.clone(),
            }))
        } else {
            None
        }
    }
}

/// An input handle that wraps a [`MapHandle<K, V>`](`MapHandle`)
/// returned by
/// [`RootCircuit::add_input_map`](`dbsp::RootCircuit::add_input_map`).
///
/// The [`insert`](`Self::insert`) method of this handle deserializes value
/// `v` type `V` and buffers a `(key_func(v), Some(v))` update for the
/// underlying `MapHandle`, where `key_func: F` extracts key of type `K`
/// from value of type `V`.
///
/// The [`delete`](`Self::delete`) method of this handle deserializes value
/// `k` type `K` and buffers a `(k, None)` update for the underlying
/// `MapHandle`.
pub struct DeMapStream<De, K, KD, V, VD, U, UD, VF, UF, C, H>
where
    K: DBData,
    V: DBData,
    U: DBData,
    H: MapInput<K, V, U>,
{
    buffer: DeMapStreamBuffer<K, V, U, H>,
    value_key_func: VF,
    update_key_func: UF,
    config: C,
    deserializer: De,
    phantom: PhantomData<fn(KD, VD, UD)>,
}

impl<De, K, KD, V, VD, U, UD, VF, UF, C, H> DeMapStream<De, K, KD, V, VD, U, UD, VF, UF, C, H>
where
    K: DBData,
    V: DBData,
    U: DBData,
    H: MapInput<K, V, U>,
    De: DeserializerFromBytes<C>,
    C: Clone,
{
    pub fn new(handle: H, value_key_func: VF, update_key_func: UF, config: C) -> Self {
        Self {
            buffer: DeMapStreamBuffer::new(handle),
            value_key_func,
            update_key_func,
            deserializer: De::create(config.clone()),
            config,
            phantom: PhantomData,
        }
    }
}

impl<De, K, KD, V, VD, U, UD, VF, UF, C, H> DeCollectionStream
    for DeMapStream<De, K, KD, V, VD, U, UD, VF, UF, C, H>
where
    De: DeserializerFromBytes<C> + Send + Sync + 'static,
    C: Clone + Send + Sync + 'static,
    K: DBData + From<KD>,
    KD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant> + Send + Sync + 'static,
    V: DBData + From<VD>,
    VD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant> + Send + Sync + 'static,
    U: DBData + From<UD>,
    H: MapInput<K, V, U>,
    UD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant> + Send + Sync + 'static,
    VF: Fn(&V) -> K + Clone + Send + Sync + 'static,
    UF: Fn(&U) -> K + Clone + Send + Sync + 'static,
{
    fn insert(&mut self, data: &[u8], metadata: &Option<Variant>) -> AnyResult<()> {
        let val = V::from(self.deserializer.deserialize::<VD>(data, metadata)?);
        let key = (self.value_key_func)(&val);

        self.buffer.updates.push_back(Tup2(key, H::insert(val)));
        self.buffer.n_bytes += data.len();
        Ok(())
    }

    fn delete(&mut self, data: &[u8], metadata: &Option<Variant>) -> AnyResult<()> {
        let key = K::from(self.deserializer.deserialize::<KD>(data, metadata)?);

        self.buffer.updates.push_back(Tup2(key, H::delete()));
        self.buffer.n_bytes += data.len();
        Ok(())
    }

    fn update(&mut self, data: &[u8], metadata: &Option<Variant>) -> AnyResult<()> {
        if !H::TAKES_UPDATES {
            bail!(
                "this table does not take update records: it keeps a key's last write, which a partial update cannot express; send the whole record as an insert"
            );
        }
        let upd = U::from(self.deserializer.deserialize::<UD>(data, metadata)?);
        let key = (self.update_key_func)(&upd);
        let Some(command) = H::update(upd) else {
            bail!("this table does not take update records");
        };

        self.buffer.updates.push_back(Tup2(key, command));
        self.buffer.n_bytes += data.len();
        Ok(())
    }

    fn reserve(&mut self, reservation: usize) {
        self.buffer.updates.reserve(reservation);
    }

    fn fork(&self) -> Box<dyn DeCollectionStream> {
        Box::new(Self::new(
            self.buffer.handle.clone(),
            self.value_key_func.clone(),
            self.update_key_func.clone(),
            self.config.clone(),
        ))
    }

    fn truncate(&mut self, len: usize) {
        if len < self.buffer.updates.len() {
            self.buffer.n_bytes = fraction(len, self.buffer.updates.len(), self.buffer.n_bytes);
            self.buffer.updates.truncate(len);
        }
    }

    fn stage(&self, buffers: Vec<Box<dyn InputBuffer>>) -> Box<dyn StagedBuffers> {
        self.buffer.handle.stage(
            flatten_nested::<DeMapStreamBuffer<K, V, U, H>>(buffers)
                .into_iter()
                .map(|buffer| buffer.updates),
        )
    }
}

impl<De, K, KD, V, VD, U, UD, VF, UF, C, H> InputBuffer
    for DeMapStream<De, K, KD, V, VD, U, UD, VF, UF, C, H>
where
    De: DeserializerFromBytes<C> + Send + Sync + 'static,
    C: Clone + Send + Sync + 'static,
    K: DBData + From<KD>,
    KD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant> + Send + Sync + 'static,
    V: DBData + From<VD>,
    VD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant> + Send + Sync + 'static,
    U: DBData + From<UD>,
    H: MapInput<K, V, U>,
    UD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant> + Send + Sync + 'static,
    VF: Fn(&V) -> K + Clone + Send + Sync + 'static,
    UF: Fn(&U) -> K + Clone + Send + Sync + 'static,
{
    fn flush(&mut self) {
        self.buffer.flush()
    }

    fn len(&self) -> BufferSize {
        self.buffer.len()
    }

    fn hash(&self, hasher: &mut dyn Hasher) {
        self.buffer.hash(hasher)
    }

    fn take_some(&mut self, n: usize) -> Option<Box<dyn InputBuffer>> {
        self.buffer.take_some(n)
    }
}

pub struct ArrowMapStream<K, KD, V, VD, U, VF, C, H>
where
    K: DBData,
    V: DBData,
    U: DBData,
    H: MapInput<K, V, U>,
{
    buffer: DeMapStreamBuffer<K, V, U, H>,
    value_key_func: VF,
    config: C,
    phantom: PhantomData<fn(KD, VD)>,
}

impl<K, KD, V, VD, U, VF, C, H> ArrowMapStream<K, KD, V, VD, U, VF, C, H>
where
    K: DBData,
    V: DBData,
    U: DBData,
    H: MapInput<K, V, U>,
    C: Clone,
{
    pub fn new(handle: H, value_key_func: VF, config: C) -> Self {
        Self {
            buffer: DeMapStreamBuffer::new(handle),
            value_key_func,
            config,
            phantom: PhantomData,
        }
    }
}

impl<K, KD, V, VD, U, VF, C, H> ArrowStream for ArrowMapStream<K, KD, V, VD, U, VF, C, H>
where
    C: Clone + Send + Sync + 'static,
    K: DBData + From<KD>,
    KD: for<'de> DeserializeWithContext<'de, C, Variant> + Send + Sync + 'static,
    V: DBData + From<VD>,
    VD: for<'de> DeserializeWithContext<'de, C, Variant> + Send + Sync + 'static,
    U: DBData,
    H: MapInput<K, V, U>,
    VF: Fn(&V) -> K + Clone + Send + Sync + 'static,
{
    fn insert(&mut self, data: &RecordBatch, metadata: &Option<Variant>) -> AnyResult<()> {
        let deserializer = arrow_deserializer(data)?;

        let records =
            Vec::<VD>::deserialize_with_context_aux(deserializer, &self.config, metadata)?;
        self.buffer.updates.extend(records.into_iter().map(|r| {
            let v = V::from(r);
            Tup2((self.value_key_func)(&v), H::insert(v))
        }));
        self.buffer.n_bytes += data.get_array_memory_size();

        Ok(())
    }

    fn delete(&mut self, data: &RecordBatch, metadata: &Option<Variant>) -> AnyResult<()> {
        let deserializer = arrow_deserializer(data)?;

        let records =
            Vec::<VD>::deserialize_with_context_aux(deserializer, &self.config, metadata)?;
        self.buffer.updates.extend(records.into_iter().map(|r| {
            let v = V::from(r);
            Tup2((self.value_key_func)(&v), H::delete())
        }));
        self.buffer.n_bytes += data.get_array_memory_size();

        Ok(())
    }

    fn fork(&self) -> Box<dyn ArrowStream> {
        Box::new(Self::new(
            self.buffer.handle.clone(),
            self.value_key_func.clone(),
            self.config.clone(),
        ))
    }

    fn insert_with_polarities(
        &mut self,
        data: &RecordBatch,
        polarities: &[bool],
        metadata: &Option<Variant>,
    ) -> AnyResult<()> {
        if polarities.len() != data.num_rows() {
            // This should never happen. We could use an assertion here, but since the `polarities` array
            // is computed by datafusion, we'll throw an error just to be safe.
            bail!(
                "insert_with_polarities: RecordBatch contains {} records, but 'polarities' array has length {}",
                data.num_rows(),
                polarities.len()
            );
        }

        let deserializer = arrow_deserializer(data)?;

        let records =
            Vec::<VD>::deserialize_with_context_aux(deserializer, &self.config, metadata)?;
        self.buffer
            .updates
            .extend(zip(records, polarities).map(|(record, polarity)| {
                let v = V::from(record);
                Tup2(
                    (self.value_key_func)(&v),
                    if *polarity { H::insert(v) } else { H::delete() },
                )
            }));
        self.buffer.n_bytes += data.get_array_memory_size();

        Ok(())
    }

    fn stage(&self, buffers: Vec<Box<dyn InputBuffer>>) -> Box<dyn StagedBuffers> {
        self.buffer.handle.stage(
            flatten_nested::<DeMapStreamBuffer<K, V, U, H>>(buffers)
                .into_iter()
                .map(|buffer| buffer.updates),
        )
    }
}

impl<K, KD, V, VD, U, VF, C, H> InputBuffer for ArrowMapStream<K, KD, V, VD, U, VF, C, H>
where
    C: Clone + Send + Sync + 'static,
    K: DBData + From<KD>,
    KD: for<'de> DeserializeWithContext<'de, C, Variant> + Send + Sync + 'static,
    V: DBData + From<VD>,
    VD: for<'de> DeserializeWithContext<'de, C, Variant> + Send + Sync + 'static,
    U: DBData,
    H: MapInput<K, V, U>,
    VF: Fn(&V) -> K + Clone + Send + Sync + 'static,
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

/// `AvroStream` implementation that collects deserialized records
/// into a map.
#[cfg(feature = "with-avro")]
pub struct AvroMapStream<K, KD, V, VD, U, VF, H>
where
    K: DBData,
    U: DBData,
    H: MapInput<K, V, U>,
    V: DBData,
{
    buffer: DeMapStreamBuffer<K, V, U, H>,
    value_key_func: VF,
    phantom: PhantomData<(KD, VD)>,
}

#[cfg(feature = "with-avro")]
impl<K, KD, V, VD, U, VF, H> Clone for AvroMapStream<K, KD, V, VD, U, VF, H>
where
    K: DBData,
    U: DBData,
    H: MapInput<K, V, U>,
    V: DBData,
    VF: Clone,
{
    fn clone(&self) -> Self {
        Self::new(self.buffer.handle.clone(), self.value_key_func.clone())
    }
}

#[cfg(feature = "with-avro")]
impl<K, KD, V, VD, U, VF, H> AvroMapStream<K, KD, V, VD, U, VF, H>
where
    K: DBData,
    U: DBData,
    H: MapInput<K, V, U>,
    V: DBData,
{
    pub fn new(handle: H, value_key_func: VF) -> Self {
        Self {
            buffer: DeMapStreamBuffer::new(handle),
            value_key_func,
            phantom: PhantomData,
        }
    }
}

#[cfg(feature = "with-avro")]
impl<K, KD, V, VD, U, VF, H> AvroStream for AvroMapStream<K, KD, V, VD, U, VF, H>
where
    K: DBData + From<KD>,
    KD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant> + Send + Sync + 'static,
    V: DBData + From<VD>,
    VD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant> + Send + Sync + 'static,
    U: DBData,
    H: MapInput<K, V, U>,
    VF: Fn(&V) -> K + Clone + Send + Sync + 'static,
{
    fn insert(
        &mut self,
        data: &AvroValue,
        schema: &AvroSchema,
        refs: &AvroSchemaRefs,
        n_bytes: usize,
        metadata: &Option<Variant>,
    ) -> AnyResult<()> {
        let v: VD = from_avro_value(data, schema, refs, metadata)
            .map_err(|e| anyhow!("error deserializing Avro record: {e}"))?;

        let val = V::from(v);
        self.buffer
            .updates
            .push_back(Tup2((self.value_key_func)(&val), H::insert(val)));
        self.buffer.n_bytes += n_bytes;

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
        let v: VD = from_avro_value(data, schema, refs, metadata)
            .map_err(|e| anyhow!("error deserializing Avro record: {e}"))?;

        let val = V::from(v);
        self.buffer
            .updates
            .push_back(Tup2((self.value_key_func)(&val), H::delete()));
        self.buffer.n_bytes += n_bytes;

        Ok(())
    }

    fn fork(&self) -> Box<dyn AvroStream> {
        Box::new(self.clone())
    }

    fn stage(&self, buffers: Vec<Box<dyn InputBuffer>>) -> Box<dyn StagedBuffers> {
        self.buffer.handle.stage(
            flatten_nested::<DeMapStreamBuffer<K, V, U, H>>(buffers)
                .into_iter()
                .map(|buffer| buffer.updates),
        )
    }
}

#[cfg(feature = "with-avro")]
impl<K, KD, V, VD, U, VF, H> InputBuffer for AvroMapStream<K, KD, V, VD, U, VF, H>
where
    K: DBData + From<KD>,
    KD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant> + Send + Sync + 'static,
    V: DBData + From<VD>,
    VD: for<'de> DeserializeWithContext<'de, SqlSerdeConfig, Variant> + Send + Sync + 'static,
    U: DBData,
    H: MapInput<K, V, U>,
    VF: Fn(&V) -> K + Clone + Send + Sync + 'static,
{
    fn flush(&mut self) {
        self.buffer.flush()
    }

    fn take_some(&mut self, n: usize) -> Option<Box<dyn InputBuffer>> {
        self.buffer.take_some(n)
    }

    fn len(&self) -> BufferSize {
        self.buffer.len()
    }

    fn hash(&self, hasher: &mut dyn Hasher) {
        self.buffer.hash(hasher)
    }
}

#[cfg(test)]
mod test {
    use crate::{
        DeCollectionHandle,
        static_compile::{
            DeLazyMapHandle, DeMapHandle, DeScalarHandle, DeScalarHandleImpl, DeZSetHandle,
            deinput::{RecordFormat, fraction},
        },
    };
    use csv::WriterBuilder as CsvWriterBuilder;
    use csv_core::{ReadRecordResult, Reader as CsvReader};
    use dbsp::{
        DBSPHandle, OrdIndexedZSet, OrdZSet, OutputHandle, Runtime, algebra::F32, utils::Tup2,
    };
    use feldera_macros::{HashRepr, IsNone, OrdRepr};
    use feldera_types::{deserialize_without_context, format::json::JsonFlavor};
    use serde_json::to_string as to_json_string;
    use size_of::SizeOf;
    use std::hash::Hash;

    #[test]
    fn test_fraction() {
        assert_eq!(fraction(1, 2, 47), 47 / 2);
        assert_eq!(fraction(usize::MAX / 4, usize::MAX / 4 * 4, 128), 32);
    }

    const NUM_WORKERS: usize = 4;

    #[derive(
        Clone,
        Debug,
        Default,
        Hash,
        PartialEq,
        Eq,
        PartialOrd,
        Ord,
        serde::Deserialize,
        serde::Serialize,
        SizeOf,
        rkyv::Archive,
        rkyv::Serialize,
        rkyv::Deserialize,
        IsNone,
        HashRepr,
        OrdRepr,
    )]
    #[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
    struct TestStruct {
        id: i64,
        s: String,
        b: bool,
        o: Option<F32>,
    }

    deserialize_without_context!(TestStruct);

    type InputHandles = (Box<dyn DeCollectionHandle>, Box<dyn DeCollectionHandle>);
    type OutputHandles = (
        OutputHandle<OrdZSet<TestStruct>>,
        OutputHandle<OrdIndexedZSet<i64, TestStruct>>,
    );

    // Test circuit for DeScalarHandle.
    fn descalar_test_circuit(
        workers: usize,
    ) -> (
        DBSPHandle,
        Box<dyn DeScalarHandle>,
        OutputHandle<TestStruct>,
    ) {
        let (dbsp, (input_handle, output_handle)) = Runtime::init_circuit(workers, |circuit| {
            let (input, input_handle) = circuit.add_input_stream::<TestStruct>();
            let output_handle = input.output();

            Ok((input_handle, output_handle))
        })
        .unwrap();

        let input_handle = DeScalarHandleImpl::new(input_handle, |x| x);

        (dbsp, Box::new(input_handle), output_handle)
    }

    #[test]
    fn test_scalar() {
        let (mut dbsp, input_handle, output_handle) = descalar_test_circuit(NUM_WORKERS);

        let inputs = [
            TestStruct {
                id: 1,
                s: "foo".to_string(),
                b: true,
                o: Some(F32::from(0.1)),
            },
            TestStruct {
                id: 2,
                s: "bar".to_string(),
                b: false,
                o: None,
            },
            TestStruct {
                id: 3,
                s: "".to_string(),
                b: false,
                o: None,
            },
        ];

        let mut input_stream = input_handle
            .configure_deserializer(RecordFormat::Json(JsonFlavor::Default))
            .unwrap();

        for input in inputs.iter() {
            let input_json = to_json_string(input).unwrap();
            input_stream.set_for_all(input_json.as_bytes()).unwrap();
            dbsp.transaction().unwrap();

            let outputs = output_handle.take_from_all();
            assert!(outputs.iter().all(|x| x == input));
        }

        let mut input_stream_clone = input_stream.fork();

        for (w, input) in inputs.iter().enumerate() {
            let input_json = to_json_string(input).unwrap();

            input_stream_clone
                .set_for_worker(w % NUM_WORKERS, input_json.as_bytes())
                .unwrap();
            dbsp.transaction().unwrap();

            let output = output_handle.take_from_worker(w % NUM_WORKERS).unwrap();
            assert_eq!(&output, input);
        }

        dbsp.kill().unwrap();
    }

    /// A lazy map takes writes and deletes through the streams written for
    /// the eager map, and refuses an update record before deserializing it.
    #[test]
    fn lazy_map_takes_writes_and_deletes_only() {
        let (mut dbsp, (handle, output)) = Runtime::init_circuit(NUM_WORKERS, |circuit| {
            let (map, handle) = circuit.add_lazy_input_map::<i64, TestStruct>();
            Ok((handle, map.output()))
        })
        .unwrap();
        let de_map = DeLazyMapHandle::<i64, i64, TestStruct, TestStruct, _>::new(
            handle,
            |row: &TestStruct| row.id,
        );
        let mut stream = de_map
            .configure_deserializer(RecordFormat::Json(JsonFlavor::Default))
            .unwrap();
        let rows = [
            TestStruct {
                id: 1,
                s: "foo".to_string(),
                b: true,
                o: Some(F32::from(0.1)),
            },
            TestStruct {
                id: 2,
                s: "bar".to_string(),
                b: false,
                o: None,
            },
        ];
        for row in &rows {
            stream
                .insert(to_json_string(row).unwrap().as_bytes(), &None)
                .unwrap();
        }
        // A delete names the key; the last command for a key wins, so key 1
        // ends up deleted.
        stream.delete(b"1", &None).unwrap();
        stream.flush();
        dbsp.transaction().unwrap();
        assert_eq!(
            output.consolidate(),
            <OrdIndexedZSet<i64, TestStruct>>::from_tuples(
                (),
                vec![Tup2(Tup2(2, rows[1].clone()), 1)]
            )
        );

        let refused = stream
            .update(br#"{"id": 2, "s": "changed"}"#, &None)
            .unwrap_err();
        assert!(
            refused.to_string().contains("does not take update records"),
            "{refused}"
        );
        dbsp.kill().unwrap();
    }

    // Test circuit for DeCollectionHandle handles.
    fn decollection_test_circuit(workers: usize) -> (DBSPHandle, InputHandles, OutputHandles) {
        let (dbsp, ((zset_input, zset_output), (map_input, map_output))) =
            Runtime::init_circuit(workers, |circuit| {
                let (zset, zset_handle) = circuit.add_input_zset::<TestStruct>();
                let (map, map_handle) =
                    circuit.add_input_map::<i64, TestStruct, TestStruct, _>(|v, u| *v = u.clone());

                let zset_output = zset.output();
                let map_output = map.output();

                Ok(((zset_handle, zset_output), (map_handle, map_output)))
            })
            .unwrap();

        let de_zset = DeZSetHandle::new(zset_input);
        let de_map: DeMapHandle<i64, i64, _, _, _, _, _, _> = DeMapHandle::new(
            map_input,
            |test_struct: &TestStruct| test_struct.id,
            |test_struct: &TestStruct| test_struct.id,
        );

        (
            dbsp,
            (Box::new(de_zset), Box::new(de_map)),
            (zset_output, map_output),
        )
    }

    // Feed `inputs` in CSV format.
    fn insert_csv(
        dbsp: &mut DBSPHandle,
        input_handles: &InputHandles,
        output_handles: &OutputHandles,
        inputs: &[TestStruct],
    ) {
        let mut zset_stream = input_handles
            .0
            .configure_deserializer(RecordFormat::Csv(Default::default()))
            .unwrap();
        let mut map_stream = input_handles
            .1
            .configure_deserializer(RecordFormat::Csv(Default::default()))
            .unwrap();

        let zset_output = &output_handles.0;
        let map_output = &output_handles.1;

        let zset = OrdZSet::from_keys(
            (),
            inputs
                .iter()
                .map(|v| Tup2(v.clone(), 1))
                .collect::<Vec<_>>(),
        );
        let map = <OrdIndexedZSet<i64, TestStruct>>::from_tuples(
            (),
            inputs
                .iter()
                .map(|v| Tup2(Tup2(v.id, v.clone()), 1))
                .collect::<Vec<_>>(),
        );

        zset_stream.reserve(inputs.len());
        map_stream.reserve(0);

        // Serialize `inputs` as CSV.
        let mut csv_bytes: Vec<u8> = Vec::new();
        let mut csv_writer = CsvWriterBuilder::new()
            .has_headers(false)
            .from_writer(&mut csv_bytes);
        for input in inputs.iter() {
            csv_writer.serialize(input).unwrap();
        }
        let _ = csv_writer.into_inner().unwrap();
        let csv_string = String::from_utf8(csv_bytes).unwrap();

        // println!("csv:\n{}", csv_string);

        // CSV reader iterates over CSV records.
        let mut csv_reader = CsvReader::new();
        let mut data = csv_string.as_bytes();
        let mut output = vec![0; data.len()];
        let mut ends = [0usize; 10];

        // Make sure that providing a record with incorrect number of columns doesn't
        // prevent subsequent records from parsing correctly (this works thanks to
        // `CsvReaderBuilder::flexible()`).
        zset_stream
            .insert(b"1,x,x,x,x,x,x,x,x\n", &None)
            .unwrap_err();
        loop {
            let (result, bytes_read, _, _) = csv_reader.read_record(data, &mut output, &mut ends);
            match result {
                ReadRecordResult::End => break,
                ReadRecordResult::Record => {
                    zset_stream.insert(&data[0..bytes_read], &None).unwrap();
                    map_stream.insert(&data[0..bytes_read], &None).unwrap();
                    data = &data[bytes_read..];
                }
                result => panic!("Unexpected result parsing CSV: {result:?}"),
            }
        }

        zset_stream.flush();
        map_stream.flush();

        dbsp.transaction().unwrap();

        assert_eq!(zset_output.consolidate(), zset);
        assert_eq!(map_output.consolidate(), map);
    }

    // Delete `inputs` in JSON format.
    fn delete_json(
        dbsp: &mut DBSPHandle,
        input_handles: &InputHandles,
        output_handles: &OutputHandles,
        inputs: &[TestStruct],
    ) {
        let mut zset_input = input_handles
            .0
            .configure_deserializer(RecordFormat::Json(JsonFlavor::Default))
            .unwrap();
        let mut map_input = input_handles
            .1
            .configure_deserializer(RecordFormat::Json(JsonFlavor::Default))
            .unwrap();

        let zset_output = &output_handles.0;
        let map_output = &output_handles.1;

        let zset = OrdZSet::from_keys(
            (),
            inputs
                .iter()
                .map(|v| Tup2(v.clone(), -1))
                .collect::<Vec<_>>(),
        );
        let map = <OrdIndexedZSet<i64, TestStruct>>::from_tuples(
            (),
            inputs
                .iter()
                .map(|v| Tup2(Tup2(v.id, v.clone()), -1))
                .collect::<Vec<_>>(),
        );

        for input in inputs.iter() {
            let id = input.id;
            let input = to_json_string(input).unwrap();

            zset_input.delete(input.as_bytes(), &None).unwrap();
            zset_input.flush();

            let input_id = to_json_string(&id).unwrap();
            map_input.delete(input_id.as_bytes(), &None).unwrap();
            map_input.flush();
        }

        dbsp.transaction().unwrap();

        assert_eq!(zset_output.consolidate(), zset);
        assert_eq!(map_output.consolidate(), map);
    }

    #[test]
    fn test_collection() {
        let (mut dbsp, input_handles, output_handles) = decollection_test_circuit(NUM_WORKERS);

        let inputs = vec![
            TestStruct {
                id: 1,
                s: "foo".to_string(),
                b: true,
                o: Some(F32::from(0.1)),
            },
            TestStruct {
                id: 2,
                s: "bar".to_string(),
                b: false,
                o: None,
            },
            TestStruct {
                id: 3,
                s: "".to_string(),
                b: false,
                o: None,
            },
        ];

        insert_csv(&mut dbsp, &input_handles, &output_handles, &inputs);
        delete_json(&mut dbsp, &input_handles, &output_handles, &inputs);

        dbsp.kill().unwrap();
    }
}
